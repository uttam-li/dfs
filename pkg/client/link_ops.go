package client

import (
	"context"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/uttam-li/dfs/api/generated/common"
	"github.com/uttam-li/dfs/api/generated/master"
)

// Mknod creates a new file system node (file, directory, or symlink) with
// the specified name, mode, and device number in the parent directory.
func (fs *Filesystem) Mknod(cancel <-chan struct{}, input *fuse.MknodIn, name string, out *fuse.EntryOut) (code fuse.Status) {
	if name == "" {
		return fuse.EINVAL
	}

	_, status := fs.validateDirectoryOperation(input.InHeader.NodeId, "Mknod")
	if status != fuse.OK {
		return status
	}

	entryType, status := fs.getEntryTypeFromMode(input.Mode)
	if status != fuse.OK {
		return status
	}

	ctx, cancelCtx := context.WithTimeout(context.Background(), fs.config.RPCTimeout)
	defer cancelCtx()

	resp, err := fs.masterClient.CreateEntry(ctx, &master.CreateEntryRequest{
		ParentIno: input.InHeader.NodeId,
		Name:      name,
		Type:      entryType,
		Attributes: &common.FileAttributes{
			Mode:  input.Mode,
			Uid:   input.InHeader.Uid,
			Gid:   input.InHeader.Gid,
			Nlink: 1,
		},
	})

	if err != nil {
		return grpcToFuseStatus(err)
	}

	fs.populateEntryOut(resp.Inode, out)
	return fuse.OK
}

// Unlink removes a file from the parent directory.
func (fs *Filesystem) Unlink(cancel <-chan struct{}, header *fuse.InHeader, name string) (code fuse.Status) {
	if name == "" {
		return fuse.EINVAL
	}

	_, status := fs.validateDirectoryOperation(header.NodeId, "Unlink")
	if status != fuse.OK {
		return status
	}

	ctx, cancelCtx := context.WithTimeout(context.Background(), fs.config.RPCTimeout)
	defer cancelCtx()

	_, err := fs.masterClient.DeleteEntry(ctx, &master.DeleteEntryRequest{
		ParentIno: header.NodeId,
		Name:      name,
		Recursive: false,
	})
	if err != nil {
		return grpcToFuseStatus(err)
	}

	return fuse.OK
}

// Rename moves an entry from one directory to another or changes its name.
func (fs *Filesystem) Rename(cancel <-chan struct{}, input *fuse.RenameIn, oldName string, newName string) (code fuse.Status) {
	if oldName == "" || newName == "" {
		return fuse.EINVAL
	}

	_, status := fs.validateDirectoryOperation(input.InHeader.NodeId, "Rename")
	if status != fuse.OK {
		return status
	}

	_, status = fs.validateDirectoryOperation(input.Newdir, "Rename")
	if status != fuse.OK {
		return status
	}

	ctx, cancelCtx := context.WithTimeout(context.Background(), fs.config.RPCTimeout)
	defer cancelCtx()

	_, err := fs.masterClient.MoveEntry(ctx, &master.MoveEntryRequest{
		OldParentIno: input.InHeader.NodeId,
		OldName:      oldName,
		NewParentIno: input.Newdir,
		NewName:      newName,
	})
	if err != nil {
		return grpcToFuseStatus(err)
	}

	return fuse.OK
}

// TODO:
// Link creates a hard link to an existing file. Currently not supported in GFS
// as it would require changes to inode structure and link counting.
func (fs *Filesystem) Link(cancel <-chan struct{}, input *fuse.LinkIn, filename string, out *fuse.EntryOut) (code fuse.Status) {
	if filename == "" {
		return fuse.EINVAL
	}

	_, status := fs.validateDirectoryOperation(input.InHeader.NodeId, "Link")
	if status != fuse.OK {
		return status
	}

	return fuse.ENOTSUP
}

// Symlink creates a symbolic link pointing to the specified target.
func (fs *Filesystem) Symlink(cancel <-chan struct{}, header *fuse.InHeader, pointedTo string, linkName string, out *fuse.EntryOut) (code fuse.Status) {
	if linkName == "" || pointedTo == "" {
		return fuse.EINVAL
	}

	_, status := fs.validateDirectoryOperation(header.NodeId, "Symlink")
	if status != fuse.OK {
		return status
	}

	ctx, cancelCtx := context.WithTimeout(context.Background(), fs.config.RPCTimeout)
	defer cancelCtx()

	resp, err := fs.masterClient.CreateEntry(ctx, &master.CreateEntryRequest{
		ParentIno:     header.NodeId,
		Name:          linkName,
		Type:          master.CreateEntryRequest_SYMLINK,
		SymlinkTarget: pointedTo,
		Attributes: &common.FileAttributes{
			Mode:  syscall.S_IFLNK | 0777,
			Uid:   header.Uid,
			Gid:   header.Gid,
			Nlink: 1,
			Size:  uint64(len(pointedTo)),
		},
	})

	if err != nil {
		return grpcToFuseStatus(err)
	}

	fs.populateEntryOut(resp.Inode, out)
	return fuse.OK
}

// Readlink returns the target path of a symbolic link.
func (fs *Filesystem) Readlink(cancel <-chan struct{}, header *fuse.InHeader) (out []byte, code fuse.Status) {
	inode, err := fs.getInodeWithCache(header.NodeId)
	if err != nil {
		return nil, grpcToFuseStatus(err)
	}

	if (inode.Attributes.GetMode() & syscall.S_IFMT) != syscall.S_IFLNK {
		return nil, fuse.EINVAL
	}

	if inode.Xattrs == nil {
		return nil, fuse.ENOENT
	}

	target, exists := inode.Xattrs["symlink_target"]
	if !exists {
		return nil, fuse.ENOENT
	}

	return target, fuse.OK
}

// Helper functions

// getEntryTypeFromMode determines the entry type based on file mode.
func (fs *Filesystem) getEntryTypeFromMode(mode uint32) (master.CreateEntryRequest_EntryType, fuse.Status) {
	fileType := mode & syscall.S_IFMT

	switch fileType {
	case syscall.S_IFREG:
		return master.CreateEntryRequest_FILE, fuse.OK
	case syscall.S_IFDIR:
		return master.CreateEntryRequest_DIRECTORY, fuse.OK
	case syscall.S_IFLNK:
		return master.CreateEntryRequest_SYMLINK, fuse.OK
	default:
		return 0, fuse.ENOTSUP
	}
}

// populateEntryOut fills the EntryOut structure with inode information.
func (fs *Filesystem) populateEntryOut(inode *master.Inode, out *fuse.EntryOut) {
	fs.cache.StoreInode(inode)
	out.NodeId = inode.Ino
	fs.inodeToFuseAttr(inode, &out.Attr)
	fs.setCacheTimeouts(inode, out)
}
