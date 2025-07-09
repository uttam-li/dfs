package service

import (
	"fmt"
	"log"
	"syscall"
	"time"

	"github.com/google/btree"
	"github.com/uttam-li/dfs/api/generated/common"
	commonTypes "github.com/uttam-li/dfs/pkg/common"
	"github.com/uttam-li/dfs/pkg/master/types"
)

const (
	// Retry parameters for race condition handling
	maxMoveRetries = 3
	retryDelay     = 5 * time.Millisecond
)

// CreateEntry creates a new directory entry with the specified attributes.
// It supports creating files, directories, and symbolic links in the filesystem.
func (ms *MasterService) CreateEntry(parentIno uint64, name string, entryType uint32, attrs *common.FileAttributes, symlinkTarget string) (*types.Inode, error) {
	if err := ms.validateParentDirectory(parentIno); err != nil {
		return nil, err
	}

	if err := ms.checkEntryExists(parentIno, name); err != nil {
		return nil, err
	}

	newInode := ms.createNewInode(entryType, attrs, symlinkTarget)

	newItem := &types.BTreeItem{
		Key: types.BTreeKey{
			ParentIno: parentIno,
			Name:      name,
		},
		Inode: newInode,
	}

	if err := ms.insertEntryWithPersistence(newItem, parentIno, name, entryType, attrs, symlinkTarget); err != nil {
		return nil, err
	}

	return newInode, nil
}

// DeleteEntry removes a directory entry and optionally all its children recursively.
// Returns the deleted inode number and a list of freed chunk handles.
func (ms *MasterService) DeleteEntry(parentIno uint64, name string, recursive bool) (uint64, []string, error) {
	item, err := ms.findEntry(parentIno, name)
	if err != nil {
		return 0, nil, err
	}

	deletedIno := item.Inode.Ino
	isDirectory := item.Inode.IsDir()

	var freedChunks []string
	for _, chunkHandle := range item.Inode.Chunks {
		freedChunks = append(freedChunks, chunkHandle.String())
	}

	if isDirectory {
		childChunks, err := ms.handleDirectoryDeletion(deletedIno, recursive)
		if err != nil {
			return 0, nil, err
		}
		freedChunks = append(freedChunks, childChunks...)
	}

	ms.metadataLock.Lock()
	defer ms.metadataLock.Unlock()

	searchKey := &types.BTreeItem{
		Key: types.BTreeKey{ParentIno: parentIno, Name: name},
	}

	if currentItem := ms.master.Metadata.Get(searchKey); currentItem == nil {
		return 0, nil, fmt.Errorf("entry was already deleted: %s", name)
	}

	ms.master.Metadata.Delete(item)
	ms.processChunkDeletions(item.Inode, deletedIno)
	ms.markInodeForGC(deletedIno, item.Inode)
	ms.logDeletionOperation(deletedIno, parentIno, name, isDirectory)

	return deletedIno, freedChunks, nil
}

// deleteInodeRecursive is a helper function for recursive deletion
func (ms *MasterService) deleteInodeRecursive(item *types.BTreeItem) ([]string, error) {
	var freedChunks []string

	if item.Inode.IsDir() {
		children := ms.findChildren(item.Inode.Ino)
		for _, child := range children {
			childFreedChunks, _ := ms.deleteInodeRecursive(child)
			freedChunks = append(freedChunks, childFreedChunks...)
			ms.master.Metadata.Delete(child)

			if ms.gcManager != nil {
				ms.gcManager.MarkInodeForDeletion(child.Inode.Ino, child.Inode)
			}
		}
	}

	// Process chunks from this inode
	for _, chunkHandle := range item.Inode.Chunks {
		freedChunks = append(freedChunks, chunkHandle.String())
	}

	// Use the shared chunk deletion logic
	ms.processChunkDeletions(item.Inode, item.Inode.Ino)

	return freedChunks, nil
}

// MoveEntry atomically moves or renames an entry from one location to another.
// Handles race conditions with recently created files through retry logic.
func (ms *MasterService) MoveEntry(oldParentIno uint64, oldName string, newParentIno uint64, newName string) (*types.Inode, error) {
	ms.metadataLock.Lock()
	defer ms.metadataLock.Unlock()

	oldItem, err := ms.findEntryWithRetry(oldParentIno, oldName)
	if err != nil {
		return nil, err
	}

	if err := ms.validateNewParent(newParentIno); err != nil {
		return nil, err
	}

	newItem := &types.BTreeItem{
		Key: types.BTreeKey{
			ParentIno: newParentIno,
			Name:      newName,
		},
		Inode: oldItem.Inode,
	}

	if err := ms.performAtomicMove(oldItem, newItem, newParentIno, newName); err != nil {
		return nil, err
	}

	if err := ms.persistMoveOperation(oldParentIno, newParentIno, oldItem.Inode.Ino, oldName, newName, oldItem, newItem); err != nil {
		return nil, err
	}

	return newItem.Inode, nil
}

// Helper methods for entry operations

// validateParentDirectory checks if the parent directory exists and is actually a directory
func (ms *MasterService) validateParentDirectory(parentIno uint64) error {
	ms.metadataLock.RLock()
	defer ms.metadataLock.RUnlock()

	found := false
	ms.master.Metadata.Ascend(func(item btree.Item) bool {
		btreeItem := item.(*types.BTreeItem)
		if btreeItem.Inode != nil && btreeItem.Inode.Ino == parentIno {
			if !btreeItem.Inode.IsDir() {
				return false // Parent is not a directory
			}
			found = true
			return false
		}
		return true
	})

	if !found {
		return fmt.Errorf("parent directory not found or is not a directory: %d", parentIno)
	}
	return nil
}

// checkEntryExists verifies that an entry with the given name doesn't already exist
func (ms *MasterService) checkEntryExists(parentIno uint64, name string) error {
	ms.metadataLock.RLock()
	defer ms.metadataLock.RUnlock()

	searchKey := &types.BTreeItem{
		Key: types.BTreeKey{
			ParentIno: parentIno,
			Name:      name,
		},
	}

	if existing := ms.master.Metadata.Get(searchKey); existing != nil {
		return fmt.Errorf("entry already exists: %s", name)
	}
	return nil
}

// createNewInode creates a new inode with the specified attributes
func (ms *MasterService) createNewInode(entryType uint32, attrs *common.FileAttributes, symlinkTarget string) *types.Inode {
	newIno := ms.master.NextIno
	ms.master.NextIno++
	now := uint64(time.Now().Unix())

	// Set the file type in mode based on entryType
	mode := attrs.Mode
	switch entryType {
	case 0: // FILE
		mode |= 0x8000 // S_IFREG equivalent
	case 1: // DIRECTORY
		mode |= 0x4000 // S_IFDIR equivalent
	case 2: // SYMLINK
		mode |= 0xa000 // S_IFLNK equivalent
	}

	newInode := &types.Inode{
		Ino:    newIno,
		Size:   attrs.Size,
		Mtime:  now,
		Nlink:  1,
		Xattr:  make(map[string][]byte),
		Chunks: make(map[uint64]commonTypes.ChunkHandle),
	}
	newInode.MakeAttr(mode, attrs.Uid, attrs.Gid)

	// For symlinks, store the target in extended attributes
	if entryType == 2 && symlinkTarget != "" {
		newInode.Xattr["symlink_target"] = []byte(symlinkTarget)
	}

	return newInode
}

// insertEntryWithPersistence inserts the entry and logs the operation for persistence
func (ms *MasterService) insertEntryWithPersistence(item *types.BTreeItem, parentIno uint64, name string, entryType uint32, attrs *common.FileAttributes, symlinkTarget string) error {
	ms.metadataLock.Lock()
	defer ms.metadataLock.Unlock()

	ms.master.Metadata.ReplaceOrInsert(item)

	// Log operation for persistence
	if ms.persistenceManager != nil {
		err := ms.persistenceManager.LogCreateInode(item.Inode.Ino, parentIno, name, attrs, entryType == 1, symlinkTarget)
		if err != nil {
			log.Printf("Failed to log create entry operation: %v", err)
			return err
		}
	}

	return nil
}

// findEntry locates an entry by parent inode and name
func (ms *MasterService) findEntry(parentIno uint64, name string) (*types.BTreeItem, error) {
	ms.metadataLock.RLock()
	defer ms.metadataLock.RUnlock()

	searchKey := &types.BTreeItem{
		Key: types.BTreeKey{ParentIno: parentIno, Name: name},
	}

	item := ms.master.Metadata.Get(searchKey)
	if item == nil {
		return nil, fmt.Errorf("entry not found: %s", name)
	}

	return item.(*types.BTreeItem), nil
}

// handleDirectoryDeletion processes deletion of a directory and its contents
func (ms *MasterService) handleDirectoryDeletion(deletedIno uint64, recursive bool) ([]string, error) {
	var freedChunks []string

	children := ms.findChildren(deletedIno)
	if len(children) > 0 && !recursive {
		return nil, fmt.Errorf("directory not empty and recursive flag not set")
	}

	if len(children) > 0 && recursive {
		for _, child := range children {
			childFreedChunks, _ := ms.deleteInodeRecursive(child)
			freedChunks = append(freedChunks, childFreedChunks...)
		}
	}

	return freedChunks, nil
}

// processChunkDeletions handles deletion of chunks associated with an inode
func (ms *MasterService) processChunkDeletions(inode *types.Inode, inodeNum uint64) {
	for chunkIndex, chunkHandle := range inode.Chunks {
		if ms.persistenceManager != nil {
			err := ms.persistenceManager.LogDeleteChunk(chunkHandle.String(), inodeNum, chunkIndex)
			if err != nil {
				log.Printf("Failed to log chunk deletion for %s: %v", chunkHandle.String(), err)
			}
		}
	}

	ms.removeChunkMetadataAndScheduleDeletion(inode.Chunks)
}

// markInodeForGC marks an inode for garbage collection
func (ms *MasterService) markInodeForGC(inodeNum uint64, inode *types.Inode) {
	if ms.gcManager != nil {
		ms.gcManager.MarkInodeForDeletion(inodeNum, inode)
	}
}

// logDeletionOperation logs the deletion operation for persistence
func (ms *MasterService) logDeletionOperation(deletedIno, parentIno uint64, name string, isDirectory bool) {
	if ms.persistenceManager != nil {
		err := ms.persistenceManager.LogDeleteInode(deletedIno, parentIno, name)
		if err != nil {
			log.Printf("Failed to log delete entry operation for %s: %v", name, err)
		}
	}
}

// findEntryWithRetry attempts to find an entry with retry logic for race conditions
func (ms *MasterService) findEntryWithRetry(parentIno uint64, name string) (*types.BTreeItem, error) {
	var lastErr error

	for attempt := 0; attempt < maxMoveRetries; attempt++ {
		searchKey := &types.BTreeItem{
			Key: types.BTreeKey{ParentIno: parentIno, Name: name},
		}

		if item := ms.master.Metadata.Get(searchKey); item != nil {
			return item.(*types.BTreeItem), nil
		}

		lastErr = fmt.Errorf("source entry not found: %s", name)
		if attempt < maxMoveRetries-1 {
			time.Sleep(retryDelay)
		}
	}

	return nil, lastErr
}

// validateNewParent ensures the new parent directory exists and is valid
func (ms *MasterService) validateNewParent(newParentIno uint64) error {
	found := false
	ms.master.Metadata.Ascend(func(item btree.Item) bool {
		btreeItem := item.(*types.BTreeItem)
		if btreeItem.Inode != nil && btreeItem.Inode.Ino == newParentIno {
			if !btreeItem.Inode.IsDir() {
				return false // New parent is not a directory
			}
			found = true
			return false
		}
		return true
	})

	if !found {
		return fmt.Errorf("new parent directory not found or is not a directory: %d", newParentIno)
	}
	return nil
}

// performAtomicMove executes the atomic move operation
func (ms *MasterService) performAtomicMove(oldItem, newItem *types.BTreeItem, newParentIno uint64, newName string) error {
	var deletedInode *types.Inode
	
	// Check if destination already exists and prepare for replacement
	if existingItem := ms.master.Metadata.Get(newItem); existingItem != nil {
		existingBTreeItem := existingItem.(*types.BTreeItem)
		deletedInode = existingBTreeItem.Inode
		
		ms.master.Metadata.Delete(existingBTreeItem)
		log.Printf("[INFO] MoveEntry: replacing existing file %s (inode %d) with %s (inode %d)", 
			newName, deletedInode.Ino, oldItem.Key.Name, newItem.Inode.Ino)
	}

	ms.master.Metadata.Delete(oldItem)
	ms.master.Metadata.ReplaceOrInsert(newItem)

	if deletedInode != nil {
		go ms.scheduleReplacedInodeCleanup(deletedInode)
	}

	return nil
}

// persistMoveOperation logs the move operation for persistence
func (ms *MasterService) persistMoveOperation(oldParentIno, newParentIno, inodeNum uint64, oldName, newName string, oldItem, newItem *types.BTreeItem) error {
	if ms.persistenceManager != nil {
		err := ms.persistenceManager.LogRename(oldParentIno, newParentIno, inodeNum, oldName, newName)
		if err != nil {
			log.Printf("Failed to log move entry operation: %v", err)
			return err
		}
	}
	return nil
}

// findChildren returns all child entries of a directory
func (ms *MasterService) findChildren(parentIno uint64) []*types.BTreeItem {
	var children []*types.BTreeItem
	ms.master.Metadata.Ascend(func(item btree.Item) bool {
		btreeItem := item.(*types.BTreeItem)
		if btreeItem.Key.ParentIno == parentIno {
			children = append(children, btreeItem)
		}
		return true
	})
	return children
}

// scheduleReplacedInodeCleanup handles cleanup of an inode that was replaced during rename
func (ms *MasterService) scheduleReplacedInodeCleanup(deletedInode *types.Inode) {
	if deletedInode == nil {
		return
	}
	
	log.Printf("[INFO] scheduleReplacedInodeCleanup: cleaning up replaced inode %d", deletedInode.Ino)
	
	mode := deletedInode.GetMode()
	if (mode & syscall.S_IFREG) != 0 { // Regular file
		ms.processChunkDeletions(deletedInode, deletedInode.Ino)
	}

	if deletedInode.IsDir() {
		log.Printf("[WARNING] scheduleReplacedInodeCleanup: attempted to replace directory inode %d", deletedInode.Ino)
	}
}
