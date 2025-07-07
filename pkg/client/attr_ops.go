package client

import (
	"context"
	"log"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/uttam-li/dfs/api/generated/common"
	"github.com/uttam-li/dfs/api/generated/master"
)

// Lookup resolves a filename within a directory to its inode number and attributes.
func (fs *Filesystem) Lookup(cancel <-chan struct{}, header *fuse.InHeader, name string, out *fuse.EntryOut) fuse.Status {
	if name == "" {
		return fuse.EINVAL
	}

	_, status := fs.validateDirectoryOperation(header.NodeId, "Lookup")
	if status != fuse.OK {
		return status
	}

	ctx, cancelCtx := context.WithTimeout(context.Background(), fs.config.RPCTimeout)
	defer cancelCtx()

	resp, err := fs.masterClient.Lookup(ctx, &master.LookupRequest{
		ParentIno: header.NodeId,
		Name:      name,
	})

	if err != nil {
		if fs.config.Debug {
			log.Printf("[ERROR] Lookup failed: %v", err)
		}
		return grpcToFuseStatus(err)
	}

	if resp.Inode == nil {
		return fuse.ENOENT
	}

	fs.cache.StoreInode(resp.Inode)
	out.NodeId = resp.Inode.Ino
	fs.inodeToFuseAttr(resp.Inode, &out.Attr)
	fs.setCacheTimeouts(resp.Inode, out)

	return fuse.OK
}

func (fs *Filesystem) setCacheTimeouts(inode *master.Inode, out *fuse.EntryOut) {
	entryTimeout := time.Duration(fs.config.AttrCacheTimeout)
	if (inode.Attributes.GetMode() & syscall.S_IFDIR) != 0 {
		entryTimeout = time.Duration(fs.config.DirCacheTimeout)
	}

	out.SetEntryTimeout(entryTimeout)
	out.SetAttrTimeout(time.Duration(fs.config.AttrCacheTimeout))
}

// setAttrTimeouts applies consistent cache timeout values to AttrOut responses.
func (fs *Filesystem) setAttrTimeouts(out *fuse.AttrOut) {
	timeoutNanos := fs.config.AttrCacheTimeout.Nanoseconds()
	out.AttrValid = uint64(timeoutNanos)
	out.AttrValidNsec = uint32(timeoutNanos % 1e9)
}

// Forget decrements the lookup count for an inode and removes it from cache when no longer referenced.
func (fs *Filesystem) Forget(nodeid, nlookup uint64) {
	if fs.config.Debug && nlookup > 1000 {
		log.Printf("[DEBUG] Forget: high nlookup count nodeid=%d nlookup=%d", nodeid, nlookup)
	}
	fs.cache.Invalidate(nodeid)
}

// GetAttr retrieves the attributes for a given inode.
func (fs *Filesystem) GetAttr(cancel <-chan struct{}, input *fuse.GetAttrIn, out *fuse.AttrOut) (code fuse.Status) {
	inode, err := fs.getInodeWithCache(input.NodeId)
	if err != nil {
		if fs.config.Debug {
			log.Printf("[ERROR] GetAttr: inode %d not found: %v", input.NodeId, err)
		}
		return grpcToFuseStatus(err)
	}

	fs.inodeToFuseAttr(inode, &out.Attr)
	fs.setAttrTimeouts(out)

	return fuse.OK
}

// SetAttr modifies the attributes of an inode based on the provided mask.
func (fs *Filesystem) SetAttr(cancel <-chan struct{}, input *fuse.SetAttrIn, out *fuse.AttrOut) (code fuse.Status) {
	inode, err := fs.getInodeWithCache(input.NodeId)
	if err != nil {
		if fs.config.Debug {
			log.Printf("[ERROR] SetAttr: inode %d not found: %v", input.NodeId, err)
		}
		return grpcToFuseStatus(err)
	}

	ctx, cancelCtx := context.WithTimeout(context.Background(), fs.config.RPCTimeout)
	defer cancelCtx()

	attrs := &common.FileAttributes{
		Mode:   inode.Attributes.Mode,
		Uid:    inode.Attributes.Uid,
		Gid:    inode.Attributes.Gid,
		Size:   inode.Attributes.Size,
		Atime:  inode.Attributes.Atime,
		Mtime:  inode.Attributes.Mtime,
		Ctime:  inode.Attributes.Ctime,
		Nlink:  inode.Attributes.Nlink,
		Blocks: inode.Attributes.Blocks,
	}

	validMask := fs.buildAttributeMask(input, attrs)

	resp, err := fs.masterClient.SetAttributes(ctx, &master.SetAttributesRequest{
		Ino:           input.NodeId,
		Attributes:    attrs,
		AttributeMask: validMask,
	})
	if err != nil {
		if fs.config.Debug {
			log.Printf("[ERROR] SetAttr: failed for inode %d: %v", input.NodeId, err)
		}
		return grpcToFuseStatus(err)
	}

	fs.cache.Invalidate(input.NodeId)

	updatedInode := &master.Inode{
		Ino:          input.NodeId,
		Attributes:   resp.NewAttributes,
		Xattrs:       inode.Xattrs,
		ChunkHandles: inode.ChunkHandles,
	}
	fs.cache.StoreInode(updatedInode)

	fs.populateAttrOut(resp.NewAttributes, input.NodeId, out)

	return fuse.OK
}

// buildAttributeMask creates a bitmask indicating which attributes should be updated.
func (fs *Filesystem) buildAttributeMask(input *fuse.SetAttrIn, attrs *common.FileAttributes) uint32 {
	validMask := uint32(0)

	if input.Valid&fuse.FATTR_MODE != 0 {
		attrs.Mode = input.Mode
		validMask |= 1 << 0
	}

	if input.Valid&fuse.FATTR_UID != 0 {
		attrs.Uid = input.Uid
		validMask |= 1 << 1
	}

	if input.Valid&fuse.FATTR_GID != 0 {
		attrs.Gid = input.Gid
		validMask |= 1 << 2
	}

	if input.Valid&fuse.FATTR_SIZE != 0 {
		attrs.Size = input.Size
		validMask |= 1 << 3
		if fs.config.Debug {
			log.Printf("[DEBUG] SetAttr: truncating inode %d to size %d", input.NodeId, input.Size)
		}
	}

	if input.Valid&fuse.FATTR_ATIME != 0 {
		attrs.Atime = int64(input.Atime)
		validMask |= 1 << 4
	}

	if input.Valid&fuse.FATTR_MTIME != 0 {
		attrs.Mtime = int64(input.Mtime)
		validMask |= 1 << 5
	}

	return validMask
}

// populateAttrOut converts master attributes to FUSE AttrOut format.
func (fs *Filesystem) populateAttrOut(attrs *common.FileAttributes, ino uint64, out *fuse.AttrOut) {
	out.Attr.Ino = ino
	out.Attr.Mode = attrs.Mode
	out.Attr.Size = attrs.Size
	out.Attr.Nlink = attrs.Nlink
	out.Attr.Uid = attrs.Uid
	out.Attr.Gid = attrs.Gid
	out.Attr.Atime = uint64(attrs.Atime)
	out.Attr.Mtime = uint64(attrs.Mtime)
	out.Attr.Ctime = uint64(attrs.Ctime)
	out.Attr.Blksize = uint32(fs.config.BlockSize)
	if out.Attr.Size > 0 {
		out.Attr.Blocks = (out.Attr.Size + uint64(fs.config.BlockSize) - 1) / uint64(fs.config.BlockSize)
	}
}

// Access checks whether the calling process can access the file in the requested mode.
func (fs *Filesystem) Access(cancel <-chan struct{}, input *fuse.AccessIn) (code fuse.Status) {
	inode, err := fs.getInodeWithCache(input.NodeId)
	if err != nil {
		if fs.config.Debug {
			log.Printf("[ERROR] Access: inode %d not found: %v", input.NodeId, err)
		}
		return grpcToFuseStatus(err)
	}

	if input.Mask == 0 {
		return fuse.OK
	}

	mode := inode.Attributes.Mode
	uid := inode.Attributes.Uid
	gid := inode.Attributes.Gid

	var permBits uint32
	if input.InHeader.Uid == uid {
		permBits = (mode >> 6) & 0x7
	} else if input.InHeader.Gid == gid {
		permBits = (mode >> 3) & 0x7
	} else {
		permBits = mode & 0x7
	}

	const (
		R_OK = 4
		W_OK = 2
		X_OK = 1
	)

	requestedPerms := uint32(0)
	if input.Mask&R_OK != 0 {
		requestedPerms |= 0x4
	}
	if input.Mask&W_OK != 0 {
		requestedPerms |= 0x2
	}
	if input.Mask&X_OK != 0 {
		requestedPerms |= 0x1
	}

	if (permBits & requestedPerms) != requestedPerms {
		if fs.config.Debug {
			log.Printf("[DEBUG] Access denied: inode=%d uid=%d/%d gid=%d/%d mode=0%o requested=0x%x allowed=0x%x",
				input.NodeId, input.InHeader.Uid, uid, input.InHeader.Gid, gid, mode, requestedPerms, permBits)
		}
		return fuse.EACCES
	}

	return fuse.OK
}

// StatFs returns filesystem statistics.
func (fs *Filesystem) StatFs(cancel <-chan struct{}, input *fuse.InHeader, out *fuse.StatfsOut) fuse.Status {
	ctx, cancelCtx := context.WithTimeout(context.Background(), fs.config.RPCTimeout)
	defer cancelCtx()

	stats, err := fs.masterClient.GetSystemStats(ctx, &master.GetSystemStatsRequest{})
	if err != nil {
		if fs.config.Debug {
			log.Printf("[ERROR] StatFs failed: %v", err)
		}
		return grpcToFuseStatus(err)
	}

	blockSize := uint64(fs.config.BlockSize)

	out.Bsize = fs.config.BlockSize
	out.Blocks = stats.Stats.TotalSpace / blockSize
	out.Bfree = stats.Stats.AvailableSpace / blockSize
	out.Bavail = stats.Stats.AvailableSpace / blockSize
	out.Files = stats.Stats.TotalFiles
	out.Ffree = stats.Stats.TotalFiles
	out.NameLen = fs.config.MaxNameLength
	out.Frsize = out.Bsize

	return fuse.OK
}

// Statx returns extended file attributes.
func (fs *Filesystem) Statx(cancel <-chan struct{}, input *fuse.StatxIn, out *fuse.StatxOut) (code fuse.Status) {
	ctx, cancelCtx := context.WithTimeout(context.Background(), fs.config.RPCTimeout)
	defer cancelCtx()

	resp, err := fs.masterClient.Statx(ctx, &master.StatxRequest{
		Ino:   input.NodeId,
		Mask:  0xffffffff, // Request all fields
		Flags: 0,          // No special flags
	})

	if err != nil {
		if fs.config.Debug {
			log.Printf("[ERROR] Statx: failed for inode %d: %v", input.NodeId, err)
		}
		return grpcToFuseStatus(err)
	}

	fs.statxToFuseStatx(resp.Attributes, resp.Mask, out)

	return fuse.OK
}
