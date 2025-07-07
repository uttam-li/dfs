package client

import (
	"context"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/uttam-li/dfs/api/generated/common"
	pb "github.com/uttam-li/dfs/api/generated/master"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// getInodeWithCache tries cache first, then fetches from master
func (fs *Filesystem) getInodeWithCache(ino uint64) (*pb.Inode, error) {
	if inode, exists := fs.cache.GetInode(ino); exists {
		return inode, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), fs.config.RPCTimeout)
	defer cancel()

	resp, err := fs.masterClient.GetAttributes(ctx, &pb.GetAttributesRequest{Inode: ino})
	if err != nil {
		return nil, err
	}

	fs.cache.StoreInode(resp.Inode)
	return resp.Inode, nil
}

// inodeToFuseAttr converts protobuf inode to FUSE attributes
func (fs *Filesystem) inodeToFuseAttr(inode *pb.Inode, attr *fuse.Attr) {
	attr.Ino = inode.Ino
	attr.Mode = inode.Attributes.Mode
	attr.Size = inode.Attributes.Size
	attr.Nlink = inode.Attributes.Nlink
	attr.Uid = inode.Attributes.Uid
	attr.Gid = inode.Attributes.Gid
	attr.Atime = uint64(inode.Attributes.Atime)
	attr.Mtime = uint64(inode.Attributes.Mtime)
	attr.Ctime = uint64(inode.Attributes.Ctime)

	attr.Blksize = uint32(fs.config.BlockSize)
	if attr.Size > 0 {
		attr.Blocks = (attr.Size + uint64(fs.config.BlockSize) - 1) / uint64(fs.config.BlockSize)
	}
}

// grpcToFuseStatus converts gRPC errors to FUSE status codes
func grpcToFuseStatus(err error) fuse.Status {
	if err == nil {
		return fuse.OK
	}

	// Handle context errors efficiently
	switch err {
	case context.Canceled:
		return fuse.EINTR
	case context.DeadlineExceeded:
		return fuse.Status(syscall.ETIMEDOUT)
	}

	st, ok := status.FromError(err)
	if !ok {
		return fuse.EIO
	}

	switch st.Code() {
	case codes.OK:
		return fuse.OK
	case codes.NotFound:
		// Check if this is an extended attribute error (optimized)
		if st.Message() == "attribute not found" {
			return fuse.Status(syscall.ENODATA)
		}
		return fuse.ENOENT
	case codes.PermissionDenied:
		return fuse.EACCES
	case codes.InvalidArgument:
		return fuse.EINVAL
	case codes.AlreadyExists:
		return fuse.Status(syscall.EEXIST)
	case codes.DeadlineExceeded, codes.Canceled:
		return fuse.Status(syscall.ETIMEDOUT)
	case codes.Unavailable:
		return fuse.EAGAIN
	case codes.ResourceExhausted:
		return fuse.Status(syscall.ENOSPC)
	case codes.FailedPrecondition:
		return fuse.Status(syscall.ESTALE)
	case codes.DataLoss, codes.Internal:
		return fuse.EIO
	default:
		return fuse.EIO
	}
}

// validateInodeOperation checks if an inode exists and matches the expected file type
func (fs *Filesystem) validateInodeOperation(ino uint64, expectedType uint32, operation string) (*pb.Inode, fuse.Status) {
	inode, err := fs.getInodeWithCache(ino)
	if err != nil {
		return nil, grpcToFuseStatus(err)
	}

	fileType := inode.Attributes.GetMode() & syscall.S_IFMT
	if fileType != expectedType {
		if expectedType == syscall.S_IFDIR {
			return nil, fuse.ENOTDIR
		}
		return nil, fuse.EISDIR
	}

	return inode, fuse.OK
}

// validateDirectoryOperation checks if an inode exists and is a directory
func (fs *Filesystem) validateDirectoryOperation(ino uint64, operation string) (*pb.Inode, fuse.Status) {
	return fs.validateInodeOperation(ino, syscall.S_IFDIR, operation)
}

// validateFileOperation checks if an inode exists and is a regular file
func (fs *Filesystem) validateFileOperation(ino uint64, operation string) (*pb.Inode, fuse.Status) {
	return fs.validateInodeOperation(ino, syscall.S_IFREG, operation)
}

// statxToFuseStatx converts protobuf statx attributes to FUSE statx format
func (fs *Filesystem) statxToFuseStatx(attrs *pb.StatxAttributes, mask uint32, out *fuse.StatxOut) {
	// Set the mask indicating which fields are valid
	out.Mask = mask

	// Basic file attributes - convert types as needed
	out.Ino = attrs.Ino
	out.Mode = uint16(attrs.Mode) // Convert uint32 to uint16
	out.Uid = attrs.Uid
	out.Gid = attrs.Gid
	out.Size = attrs.Size
	out.Nlink = attrs.Nlink
	out.Blocks = attrs.Blocks
	out.Blksize = attrs.Blksize

	// Device information
	out.RdevMajor = attrs.RdevMajor
	out.RdevMinor = attrs.RdevMinor
	out.DevMajor = attrs.DevMajor
	out.DevMinor = attrs.DevMinor

	// Timestamps - use the SxTime type properly with proper type conversion
	if attrs.Atime != nil {
		out.Atime.Sec = uint64(attrs.Atime.Sec)
		out.Atime.Nsec = attrs.Atime.Nsec
	}
	if attrs.Mtime != nil {
		out.Mtime.Sec = uint64(attrs.Mtime.Sec)
		out.Mtime.Nsec = attrs.Mtime.Nsec
	}
	if attrs.Ctime != nil {
		out.Ctime.Sec = uint64(attrs.Ctime.Sec)
		out.Ctime.Nsec = attrs.Ctime.Nsec
	}
	if attrs.Btime != nil {
		out.Btime.Sec = uint64(attrs.Btime.Sec)
		out.Btime.Nsec = attrs.Btime.Nsec
	}
}

// allocateChunkWithLease gets chunk handle and lease with cache optimization for writes
func (fs *Filesystem) allocateChunkWithLease(cancel <-chan struct{}, fileIno uint64, chunkIndex uint64) (string, []*common.ChunkLocation, error) {
	
	// Check cache first for existing chunk handle and locations
	if handle, version, exists := fs.cache.GetChunkHandle(fileIno, chunkIndex); exists {
		if locations, locExists := fs.cache.GetChunkLocation(fileIno, chunkIndex); locExists {
			// Convert cached locations to ChunkLocation format efficiently
			chunkLocations := make([]*common.ChunkLocation, len(locations))
			for i, addr := range locations {
				chunkLocations[i] = &common.ChunkLocation{
					Address:   addr,
					Version:   version,
					IsPrimary: i == 0, // First is primary by convention
				}
			}
			return handle, chunkLocations, nil
		}
	}

	ctx, cancelCtx := context.WithTimeout(context.Background(), fs.config.RPCTimeout)
	defer cancelCtx()

	resp, err := fs.masterClient.AllocateChunk(ctx, &pb.AllocateChunkRequest{
		FileIno:    fileIno,
		ChunkIndex: chunkIndex,
	})
	if err != nil {
		return "", nil, err
	}

	addresses := make([]string, len(resp.Locations))
	for i, loc := range resp.Locations {
		addresses[i] = loc.Address
	}
	fs.cache.StoreChunkLocation(fileIno, chunkIndex, resp.ChunkHandle, addresses, resp.Locations[0].Version)

	return resp.ChunkHandle, resp.Locations, nil
}

// hasWritePermission checks if the user has write permission efficiently
func (fs *Filesystem) hasWritePermission(inode *pb.Inode, uid, gid uint32) bool {
	attrs := inode.Attributes
	mode := attrs.Mode

	// Owner check - most common case first
	if uid == attrs.Uid {
		return (mode>>6)&0x2 != 0 // Owner write bit
	}

	// Group check
	if gid == attrs.Gid {
		return (mode>>3)&0x2 != 0 // Group write bit
	}

	// Others check
	return mode&0x2 != 0 // Others write bit
}
