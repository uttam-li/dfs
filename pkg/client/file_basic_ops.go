package client

import (
	"context"
	"fmt"
	"log"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/uttam-li/dfs/api/generated/common"
	"github.com/uttam-li/dfs/api/generated/master"
)

// getInodeFromFh safely retrieves the inode number for a given file handle.
func (fs *Filesystem) getInodeFromFh(fh uint64) (uint64, fuse.Status) {
	fs.openFilesMu.RLock()
	openFile, exists := fs.openFiles[fh]
	fs.openFilesMu.RUnlock()

	if !exists {
		if fs.config.Debug {
			log.Printf("[ERROR] getInodeFromFh: file handle %d not found in openFiles", fh)
		}
		return 0, fuse.EBADF
	}

	openFile.mu.RLock()
	inode := openFile.Inode
	openFile.LastAccess = time.Now()
	openFile.mu.RUnlock()

	return inode, fuse.OK
}

// checkWritePermissions verifies if the user has write access to the file.
func (fs *Filesystem) checkWritePermissions(inode *master.Inode, uid, gid uint32) bool {
	mode := inode.Attributes.Mode
	fileUid := inode.Attributes.Uid
	fileGid := inode.Attributes.Gid

	var permBits uint32
	if uid == fileUid {
		permBits = (mode >> 6) & 0x7
	} else if gid == fileGid {
		permBits = (mode >> 3) & 0x7
	} else {
		permBits = mode & 0x7
	}

	return (permBits & 0x2) != 0
}

// allocateFileDescriptor generates a new file descriptor and increments the counter.
func (fs *Filesystem) allocateFileDescriptor() uint64 {
	fs.fdMutex.Lock()
	fh := fs.nextFd
	fs.nextFd++
	fs.fdMutex.Unlock()
	return fh
}

// trackOpenFile records an open file in the filesystem's tracking table.
func (fs *Filesystem) trackOpenFile(fh, inode uint64, flags uint32) {
	isWrite := (flags & (syscall.O_WRONLY | syscall.O_RDWR)) != 0
	openFileInfo := &OpenFileInfo{
		Inode:      inode,
		OpenFlags:  flags,
		RefCount:   1,
		IsWrite:    isWrite,
		OpenTime:   time.Now(),
		LastAccess: time.Now(),
	}

	fs.openFilesMu.Lock()
	fs.openFiles[fh] = openFileInfo
	fs.openFilesMu.Unlock()
}

// Create creates a new file in the specified directory and opens it.
func (fs *Filesystem) Create(cancel <-chan struct{}, input *fuse.CreateIn, name string, out *fuse.CreateOut) (code fuse.Status) {
	if name == "" {
		return fuse.EINVAL
	}

	_, status := fs.validateDirectoryOperation(input.InHeader.NodeId, "Create")
	if status != fuse.OK {
		return status
	}

	ctx, cancelCtx := context.WithTimeout(context.Background(), fs.config.RPCTimeout)
	defer cancelCtx()

	fileMode := input.Mode | syscall.S_IFREG
	if (fileMode & 0600) == 0 {
		fileMode = 0644 | syscall.S_IFREG
	}

	resp, err := fs.masterClient.CreateEntry(ctx, &master.CreateEntryRequest{
		ParentIno: input.InHeader.NodeId,
		Name:      name,
		Type:      master.CreateEntryRequest_FILE,
		Attributes: &common.FileAttributes{
			Mode:  fileMode,
			Uid:   input.InHeader.Uid,
			Gid:   input.InHeader.Gid,
			Nlink: 1,
		},
	})

	if err != nil {
		if fs.config.Debug {
			log.Printf("[ERROR] Create failed: %v", err)
		}
		return grpcToFuseStatus(err)
	}

	fs.cache.StoreInode(resp.Inode)
	fh := fs.allocateFileDescriptor()
	fs.trackOpenFile(fh, resp.Inode.Ino, input.Flags)

	out.NodeId = resp.Inode.Ino
	out.Fh = fh
	out.OpenFlags = 0
	fs.inodeToFuseAttr(resp.Inode, &out.Attr)
	fs.setCacheTimeouts(resp.Inode, &out.EntryOut)

	return fuse.OK
}

// Open opens an existing file for reading or writing.
func (fs *Filesystem) Open(cancel <-chan struct{}, input *fuse.OpenIn, out *fuse.OpenOut) (status fuse.Status) {
	inode, status := fs.validateFileOperation(input.NodeId, "Open")
	if status != fuse.OK {
		return status
	}

	if input.Flags&(syscall.O_WRONLY|syscall.O_RDWR) != 0 {
		if !fs.checkWritePermissions(inode, input.InHeader.Uid, input.InHeader.Gid) {
			if fs.config.Debug {
				log.Printf("[DEBUG] Open write permission denied: inode=%d uid=%d/%d gid=%d/%d mode=0%o flags=0x%x",
					input.NodeId, input.InHeader.Uid, inode.Attributes.Uid,
					input.InHeader.Gid, inode.Attributes.Gid, inode.Attributes.Mode, input.Flags)
			}
			return fuse.EACCES
		}
	}

	fh := fs.allocateFileDescriptor()
	fs.trackOpenFile(fh, input.NodeId, input.Flags)

	out.Fh = fh
	out.OpenFlags = 0

	return fuse.OK
}

// flushBufferForHandle flushes any pending write buffer for the given file handle.
func (fs *Filesystem) flushBufferForHandle(cancel <-chan struct{}, fh uint64) error {
	fs.writeBuffersMu.Lock()
	buffer, exists := fs.writeBuffers[fh]
	if exists {
		delete(fs.writeBuffers, fh)
	}
	fs.writeBuffersMu.Unlock()

	if !exists || len(buffer.Data) == 0 {
		return nil
	}

	buffer.mu.Lock()
	defer buffer.mu.Unlock()

	for retries := 0; retries < 3; retries++ {
		cachedInode, status := fs.validateFileOperation(buffer.NodeId, "Release")
		if status == fuse.OK {
			status = fs.flushBuffer(cancel, buffer, cachedInode)
		}
		if status != fuse.EINTR {
			if status != fuse.OK {
				return fmt.Errorf("failed to flush buffer: status=%v", status)
			}
			break
		}
	}
	return nil
}

// cleanupOpenFile removes a file handle from the tracking table.
func (fs *Filesystem) cleanupOpenFile(fh uint64) {
	fs.openFilesMu.Lock()
	defer fs.openFilesMu.Unlock()

	openFile, exists := fs.openFiles[fh]
	if !exists {
		return
	}

	openFile.mu.Lock()
	openFile.RefCount--
	if openFile.RefCount <= 0 {
		delete(fs.openFiles, fh)
	}
	openFile.mu.Unlock()
}

// Release closes a file handle and flushes any pending writes.
func (fs *Filesystem) Release(cancel <-chan struct{}, input *fuse.ReleaseIn) {
	inode, status := fs.getInodeFromFh(input.Fh)
	if status != fuse.OK {
		if fs.config.Debug {
			log.Printf("[ERROR] Release: failed to get inode for fh=%d: %v", input.Fh, status)
		}
		inode = input.NodeId
	}

	if err := fs.flushBufferForHandle(cancel, input.Fh); err != nil {
		if fs.config.Debug {
			log.Printf("[ERROR] Release: %v", err)
		}
	}

	if err := fs.syncFileToChunkServers(cancel, inode); err != nil {
		if fs.config.Debug {
			log.Printf("[ERROR] Release: failed to sync file %d: %v", inode, err)
		}
	}

	fs.cleanupOpenFile(input.Fh)
}

// Flush forces any pending writes for a file handle to be sent to storage.
func (fs *Filesystem) Flush(cancel <-chan struct{}, input *fuse.FlushIn) fuse.Status {
	inode, status := fs.getInodeFromFh(input.Fh)
	if status != fuse.OK {
		if fs.config.Debug {
			log.Printf("[ERROR] Flush: failed to get inode for fh=%d: %v", input.Fh, status)
		}
		inode = input.NodeId
	}

	fs.cache.Invalidate(inode)

	fs.writeBuffersMu.Lock()
	buffer, exists := fs.writeBuffers[input.Fh]
	fs.writeBuffersMu.Unlock()

	if !exists {
		return fuse.OK
	}

	cachedInode, status := fs.validateFileOperation(inode, "Flush")
	if status != fuse.OK {
		return status
	}

	fs.writeBuffersMu.Lock()
	status = fs.flushBuffer(cancel, buffer, cachedInode)
	fs.writeBuffersMu.Unlock()

	if status != fuse.OK {
		if fs.config.Debug {
			log.Printf("[ERROR] Flush: failed to flush buffer for fh=%d: %v", input.Fh, status)
		}
		return status
	}

	return fuse.OK
}

// Fsync synchronizes file data to storage.
func (fs *Filesystem) Fsync(cancel <-chan struct{}, input *fuse.FsyncIn) (code fuse.Status) {
	_, status := fs.getInodeFromFh(input.Fh)
	if status != fuse.OK {
		if fs.config.Debug {
			log.Printf("[ERROR] Fsync: failed to get inode for fh=%d: %v", input.Fh, status)
		}
		return status
	}

	return fuse.OK
}

// Fallocate pre-allocates space for a file.
func (fs *Filesystem) Fallocate(cancel <-chan struct{}, input *fuse.FallocateIn) (code fuse.Status) {
	if input.Mode != 0 {
		return fuse.ENOTSUP
	}

	inode, status := fs.validateFileOperation(input.NodeId, "Fallocate")
	if status != fuse.OK {
		return status
	}

	endOffset := input.Offset + input.Length
	if endOffset <= inode.Attributes.Size {
		return fuse.OK
	}

	chunkSize := uint64(fs.config.ChunkSize)
	endChunkIndex := (endOffset - 1) / chunkSize

	for chunkIndex := uint64(0); chunkIndex <= endChunkIndex; chunkIndex++ {
		_, _, err := fs.allocateChunkWithLease(cancel, input.NodeId, chunkIndex)
		if err != nil {
			if fs.config.Debug {
				log.Printf("[ERROR] Fallocate: failed to allocate chunk %d: %v", chunkIndex, err)
			}
			return grpcToFuseStatus(err)
		}
	}

	return fuse.OK
}

// Lseek changes the file position for a file handle.
func (fs *Filesystem) Lseek(cancel <-chan struct{}, in *fuse.LseekIn, out *fuse.LseekOut) fuse.Status {
	inode, status := fs.validateFileOperation(in.NodeId, "Lseek")
	if status != fuse.OK {
		return status
	}

	var newOffset uint64

	switch in.Whence {
	case 0: // SEEK_SET
		newOffset = in.Offset
	case 1: // SEEK_CUR
		newOffset = in.Offset
	case 2: // SEEK_END
		newOffset = inode.Attributes.Size + in.Offset
	default:
		return fuse.EINVAL
	}

	out.Offset = newOffset
	return fuse.OK
}

// CopyFileRange copies data between two files.
func (fs *Filesystem) CopyFileRange(cancel <-chan struct{}, input *fuse.CopyFileRangeIn) (written uint32, code fuse.Status) {
	readIn := &fuse.ReadIn{
		InHeader: fuse.InHeader{NodeId: input.NodeId},
		Offset:   input.OffIn,
		Size:     uint32(input.Len),
	}

	readResult, status := fs.Read(cancel, readIn, make([]byte, input.Len))
	if status != fuse.OK {
		return 0, status
	}

	data, status := readResult.Bytes(make([]byte, input.Len))
	if status != fuse.OK {
		return 0, status
	}

	writeIn := &fuse.WriteIn{
		InHeader: fuse.InHeader{NodeId: input.NodeIdOut},
		Offset:   input.OffOut,
	}

	return fs.Write(cancel, writeIn, data)
}

// syncFileToChunkServers forces synchronization of all chunks for a file to ensure
// data is persisted to disk. This is critical for applications that create swap
// files and then rename them (like Krita, Nvim, etc.)
func (fs *Filesystem) syncFileToChunkServers(cancel <-chan struct{}, fileIno uint64) error {
	fs.writeBuffersMu.Lock()
	defer fs.writeBuffersMu.Unlock()

	buffersFound := 0
	for fh, buffer := range fs.writeBuffers {
		if buffer.NodeId == fileIno && len(buffer.Data) > 0 {
			buffersFound++
			buffer.mu.Lock()
			cachedInode, status := fs.validateFileOperation(fileIno, "SyncFile")
			if status == fuse.OK {
				status = fs.flushBuffer(cancel, buffer, cachedInode)
			}
			if status != fuse.OK {
				buffer.mu.Unlock()
				return fmt.Errorf("failed to flush buffer for fh=%d: status=%v", fh, status)
			}
			buffer.mu.Unlock()
		}
	}

	return nil
}
