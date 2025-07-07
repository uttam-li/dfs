package client

import (
	"context"
	"log"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/uttam-li/dfs/api/generated/common"
	"github.com/uttam-li/dfs/api/generated/master"
)

// Mkdir creates a new directory
func (fs *Filesystem) Mkdir(cancel <-chan struct{}, input *fuse.MkdirIn, name string, out *fuse.EntryOut) (code fuse.Status) {
	if name == "" {
		return fuse.EINVAL
	}

	// Validate parent directory
	_, status := fs.validateDirectoryOperation(input.InHeader.NodeId, "Mkdir")
	if status != fuse.OK {
		return status
	}

	ctx, cancelCtx := context.WithTimeout(context.Background(), fs.config.RPCTimeout)
	defer cancelCtx()

	resp, err := fs.masterClient.CreateEntry(ctx, &master.CreateEntryRequest{
		ParentIno: input.InHeader.NodeId,
		Name:      name,
		Type:      master.CreateEntryRequest_DIRECTORY,
		Attributes: &common.FileAttributes{
			Mode:  input.Mode | syscall.S_IFDIR,
			Uid:   input.InHeader.Uid,
			Gid:   input.InHeader.Gid,
			Nlink: 2, // . and parent link
		},
	})

	if err != nil {
		return grpcToFuseStatus(err)
	}

	fs.cache.StoreInode(resp.Inode)
	out.NodeId = resp.Inode.Ino
	fs.inodeToFuseAttr(resp.Inode, &out.Attr)
	fs.setCacheTimeouts(resp.Inode, out)

	return fuse.OK
}

func (fs *Filesystem) Rmdir(cancel <-chan struct{}, header *fuse.InHeader, name string) (code fuse.Status) {

	if name == "" {
		return fuse.EINVAL
	}

	_, status := fs.validateDirectoryOperation(header.NodeId, "Rmdir")
	if status != fuse.OK {
		return status
	}

	ctx, cancelCtx := context.WithTimeout(context.Background(), fs.config.RPCTimeout)
	defer cancelCtx()

	_, err := fs.masterClient.DeleteEntry(ctx, &master.DeleteEntryRequest{
		ParentIno: header.NodeId,
		Name:      name,
		Recursive: false, // Directory must be empty
	})
	if err != nil {
		if fs.config.Debug {
			log.Printf("[ERROR] Rmdir failed: %v", err)
		}
		return grpcToFuseStatus(err)
	}

	return fuse.OK
}

func (fs *Filesystem) OpenDir(cancel <-chan struct{}, input *fuse.OpenIn, out *fuse.OpenOut) (status fuse.Status) {

	_, status = fs.validateDirectoryOperation(input.NodeId, "OpenDir")
	if status != fuse.OK {
		return status
	}

	fs.fdMutex.Lock()
	fh := fs.nextFd
	fs.nextFd++
	fs.fdMutex.Unlock()

	out.Fh = fh
	out.OpenFlags = 0 // No special flags needed for directories

	return fuse.OK
}

func (fs *Filesystem) ReadDir(cancel <-chan struct{}, input *fuse.ReadIn, out *fuse.DirEntryList) fuse.Status {

	_, status := fs.validateDirectoryOperation(input.NodeId, "ReadDir")
	if status != fuse.OK {
		return status
	}

	ctx, cancelCtx := context.WithTimeout(context.Background(), fs.config.RPCTimeout)
	defer cancelCtx()

	resp, err := fs.masterClient.ListDirectory(ctx, &master.ListDirectoryRequest{
		DirIno:     input.NodeId,
		MaxEntries: 1000,
	})
	if err != nil {
		if fs.config.Debug {
			log.Printf("[ERROR] ReadDir failed: %v", err)
		}
		return grpcToFuseStatus(err)
	}

	for i, entry := range resp.Entries {
		if uint64(i) < input.Offset {
			continue
		}

		entryAdded := out.AddDirEntry(fuse.DirEntry{
			Mode: entry.Attributes.Mode,
			Name: entry.Name,
			Ino:  entry.Ino,
		})
		if !entryAdded {
			// Buffer is full
			break
		}
	}

	return fuse.OK
}

func (fs *Filesystem) ReadDirPlus(cancel <-chan struct{}, input *fuse.ReadIn, out *fuse.DirEntryList) fuse.Status {

	_, status := fs.validateDirectoryOperation(input.NodeId, "ReadDirPlus")
	if status != fuse.OK {
		return status
	}

	ctx, cancelCtx := context.WithTimeout(context.Background(), fs.config.RPCTimeout)
	defer cancelCtx()

	resp, err := fs.masterClient.ListDirectory(ctx, &master.ListDirectoryRequest{
		DirIno:     input.NodeId,
		MaxEntries: 1000,
	})
	if err != nil {
		if fs.config.Debug {
			log.Printf("[ERROR] ReadDirPlus failed: %v", err)
		}
		return grpcToFuseStatus(err)
	}

	for i, entry := range resp.Entries {
		// Skip entries before offset
		if uint64(i) < input.Offset {
			continue
		}

		// Create a synthetic inode for caching
		inode := &master.Inode{
			Ino:        entry.Ino,
			Attributes: entry.Attributes,
		}
		fs.cache.StoreInode(inode)

		// Create DirEntry with full attributes
		dirEntry := fuse.DirEntry{
			Mode: entry.Attributes.Mode,
			Name: entry.Name,
			Ino:  entry.Ino,
		}

		entryOut := out.AddDirLookupEntry(dirEntry)
		if entryOut == nil {
			// Buffer is full
			break
		}

		entryOut.NodeId = entry.Ino
		fs.inodeToFuseAttr(inode, &entryOut.Attr)
		fs.setCacheTimeouts(inode, entryOut)
	}

	return fuse.OK
}

func (fs *Filesystem) ReleaseDir(input *fuse.ReleaseIn) {
	// Nothing special needed for directory release in DFS
	// File descriptor cleanup is automatic
}

func (fs *Filesystem) FsyncDir(cancel <-chan struct{}, input *fuse.FsyncIn) (code fuse.Status) {
	// TODO: Implement
	return fuse.ENOSYS
}
