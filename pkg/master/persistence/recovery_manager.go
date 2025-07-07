package persistence

import (
	"context"
	"fmt"
	"log"
	"syscall"

	"github.com/google/btree"
	"github.com/uttam-li/dfs/api/generated/persistence"
	commonTypes "github.com/uttam-li/dfs/pkg/common"
	"github.com/uttam-li/dfs/pkg/master/types"
)

// RecoveryManager handles replaying operation logs
type RecoveryManager struct {
	logManager *LogManager
	master     *types.Master
}

// NewRecoveryManager creates a new recovery manager
func NewRecoveryManager(logManager *LogManager, master *types.Master) *RecoveryManager {
	return &RecoveryManager{
		logManager: logManager,
		master:     master,
	}
}

// Recover performs recovery from operation logs
func (rm *RecoveryManager) Recover(ctx context.Context) error {
	log.Printf("Starting recovery process...")

	// Set up recovery callback
	rm.logManager.SetRecoveryCallback(rm.processLogEntry)

	// For now, start from beginning (sequence 0)
	// In a full implementation, we would:
	// 1. Find the latest checkpoint
	// 2. Load the checkpoint data
	// 3. Replay logs from the checkpoint onwards

	err := rm.logManager.ReplayLogsFromSequence(0, rm.processLogEntry)
	if err != nil {
		return fmt.Errorf("failed to replay logs: %w", err)
	}

	log.Printf("Recovery completed successfully")
	return nil
}

// processLogEntry processes a single log entry during recovery
func (rm *RecoveryManager) processLogEntry(entry *persistence.LogEntry) error {
	switch entry.Type {
	case persistence.LogEntryType_LOG_ENTRY_CREATE_INODE:
		return rm.processCreateInode(entry.GetCreateInode())
	case persistence.LogEntryType_LOG_ENTRY_DELETE_INODE:
		return rm.processDeleteInode(entry.GetDeleteInode())
	case persistence.LogEntryType_LOG_ENTRY_UPDATE_INODE:
		return rm.processUpdateInode(entry.GetUpdateInode())
	case persistence.LogEntryType_LOG_ENTRY_CREATE_CHUNK:
		return rm.processCreateChunk(entry.GetCreateChunk())
	case persistence.LogEntryType_LOG_ENTRY_DELETE_CHUNK:
		return rm.processDeleteChunk(entry.GetDeleteChunk())
	case persistence.LogEntryType_LOG_ENTRY_UPDATE_CHUNK:
		return rm.processUpdateChunk(entry.GetUpdateChunk())
	case persistence.LogEntryType_LOG_ENTRY_MKDIR:
		return rm.processMkdir(entry.GetMkdir())
	case persistence.LogEntryType_LOG_ENTRY_RMDIR:
		return rm.processRmdir(entry.GetRmdir())
	case persistence.LogEntryType_LOG_ENTRY_RENAME:
		return rm.processRename(entry.GetRename())
	case persistence.LogEntryType_LOG_ENTRY_SYMLINK:
		return rm.processSymlink(entry.GetSymlink())
	case persistence.LogEntryType_LOG_ENTRY_UNLINK:
		return rm.processUnlink(entry.GetUnlink())
	case persistence.LogEntryType_LOG_ENTRY_SETATTR:
		return rm.processSetattr(entry.GetSetattr())
	case persistence.LogEntryType_LOG_ENTRY_SETXATTR:
		return rm.processSetxattr(entry.GetSetxattr())
	case persistence.LogEntryType_LOG_ENTRY_REMOVEXATTR:
		return rm.processRemovexattr(entry.GetRemovexattr())
	default:
		log.Printf("Unknown log entry type: %v", entry.Type)
		return nil
	}
}

// processCreateInode recreates an inode from a log entry
func (rm *RecoveryManager) processCreateInode(entry *persistence.CreateInodeLogEntry) error {
	if entry == nil {
		return fmt.Errorf("create inode entry is nil")
	}

	inode := &types.Inode{
		Ino:    entry.InodeId,
		Size:   entry.Attributes.Size,
		Mtime:  uint64(entry.Attributes.Mtime),
		Nlink:  uint16(entry.Attributes.Nlink),
		Xattr:  make(map[string][]byte),
		Chunks: make(map[uint64]commonTypes.ChunkHandle),
	}

	// Set file mode based on whether it's a directory
	if entry.IsDirectory {
		inode.MakeAttr(syscall.S_IFDIR|uint32(entry.Attributes.Mode), entry.Attributes.Uid, entry.Attributes.Gid)
	} else if entry.Target != "" {
		// Symlink
		inode.MakeAttr(syscall.S_IFLNK|uint32(entry.Attributes.Mode), entry.Attributes.Uid, entry.Attributes.Gid)
	} else {
		// Regular file
		inode.MakeAttr(syscall.S_IFREG|uint32(entry.Attributes.Mode), entry.Attributes.Uid, entry.Attributes.Gid)
	}

	// Insert into btree
	btreeItem := &types.BTreeItem{
		Key: types.BTreeKey{
			ParentIno: entry.ParentId,
			Name:      entry.Name,
		},
		Inode: inode,
	}

	rm.master.Metadata.ReplaceOrInsert(btreeItem)

	// Update next inode number
	if entry.InodeId >= rm.master.NextIno {
		rm.master.NextIno = entry.InodeId + 1
	}

	return nil
}

// processDeleteInode removes an inode from recovery
func (rm *RecoveryManager) processDeleteInode(entry *persistence.DeleteInodeLogEntry) error {
	if entry == nil {
		return fmt.Errorf("delete inode entry is nil")
	}

	key := &types.BTreeItem{
		Key: types.BTreeKey{
			ParentIno: entry.ParentId,
			Name:      entry.Name,
		},
	}

	rm.master.Metadata.Delete(key)
	return nil
}

// processUpdateInode updates an inode's attributes
func (rm *RecoveryManager) processUpdateInode(entry *persistence.UpdateInodeLogEntry) error {
	if entry == nil {
		return fmt.Errorf("update inode entry is nil")
	}

	// Find the inode in the btree
	var foundItem *types.BTreeItem
	rm.master.Metadata.Ascend(func(item btree.Item) bool {
		btreeItem := item.(*types.BTreeItem)
		if btreeItem.Inode != nil && btreeItem.Inode.Ino == entry.InodeId {
			foundItem = btreeItem
			return false
		}
		return true
	})

	if foundItem == nil {
		return fmt.Errorf("inode %d not found for update", entry.InodeId)
	}

	// Update attributes based on field mask
	for _, field := range entry.FieldMask {
		switch field {
		case "mode":
			foundItem.Inode.MakeAttr(entry.Attributes.Mode, foundItem.Inode.GetUID(), foundItem.Inode.GetGID())
		case "uid":
			foundItem.Inode.MakeAttr(foundItem.Inode.GetMode(), entry.Attributes.Uid, foundItem.Inode.GetGID())
		case "gid":
			foundItem.Inode.MakeAttr(foundItem.Inode.GetMode(), foundItem.Inode.GetUID(), entry.Attributes.Gid)
		case "size":
			foundItem.Inode.Size = entry.Attributes.Size
		case "mtime":
			foundItem.Inode.Mtime = uint64(entry.Attributes.Mtime)
		}
	}

	return nil
}

// processCreateChunk recreates chunk metadata
func (rm *RecoveryManager) processCreateChunk(entry *persistence.CreateChunkLogEntry) error {
	if entry == nil {
		return fmt.Errorf("create chunk entry is nil")
	}

	// Parse chunk handle from string
	chunkHandle, err := commonTypes.ParseChunkHandle(entry.ChunkHandle)
	if err != nil {
		return fmt.Errorf("invalid chunk handle in log entry: %w", err)
	}

	// Convert location strings
	locations := make([]string, len(entry.Locations))
	for i, loc := range entry.Locations {
		locations[i] = loc.Address
	}

	chunkMetadata := &types.ChunkMetadata{
		Version:   uint16(entry.Version),
		Size:      uint32(entry.Size),
		Locations: locations,
	}

	rm.master.ChunkMetadata[chunkHandle] = chunkMetadata

	// Update inode chunk mapping
	rm.master.Metadata.Ascend(func(item btree.Item) bool {
		btreeItem := item.(*types.BTreeItem)
		if btreeItem.Inode != nil && btreeItem.Inode.Ino == entry.InodeId {
			btreeItem.Inode.Chunks[entry.ChunkIndex] = chunkHandle
			return false
		}
		return true
	})

	return nil
}

// processDeleteChunk removes chunk metadata
func (rm *RecoveryManager) processDeleteChunk(entry *persistence.DeleteChunkLogEntry) error {
	if entry == nil {
		return fmt.Errorf("delete chunk entry is nil")
	}

	// Parse chunk handle from string
	chunkHandle, err := commonTypes.ParseChunkHandle(entry.ChunkHandle)
	if err != nil {
		return fmt.Errorf("invalid chunk handle in log entry: %w", err)
	}

	delete(rm.master.ChunkMetadata, chunkHandle)

	// Remove from inode chunk mapping
	rm.master.Metadata.Ascend(func(item btree.Item) bool {
		btreeItem := item.(*types.BTreeItem)
		if btreeItem.Inode != nil && btreeItem.Inode.Ino == entry.InodeId {
			delete(btreeItem.Inode.Chunks, entry.ChunkIndex)
			return false
		}
		return true
	})

	return nil
}

// processUpdateChunk updates chunk metadata
func (rm *RecoveryManager) processUpdateChunk(entry *persistence.UpdateChunkLogEntry) error {
	if entry == nil {
		return fmt.Errorf("update chunk entry is nil")
	}

	// Parse chunk handle from string
	chunkHandle, err := commonTypes.ParseChunkHandle(entry.ChunkHandle)
	if err != nil {
		return fmt.Errorf("invalid chunk handle in log entry: %w", err)
	}

	chunkMetadata, exists := rm.master.ChunkMetadata[chunkHandle]
	if !exists {
		return fmt.Errorf("chunk %s not found for update", entry.ChunkHandle)
	}

	// Update chunk metadata
	chunkMetadata.Version = uint16(entry.Version)
	chunkMetadata.Size = uint32(entry.Size)

	// Update locations
	locations := make([]string, len(entry.Locations))
	for i, loc := range entry.Locations {
		locations[i] = loc.Address
	}
	chunkMetadata.Locations = locations

	return nil
}

// Helper methods for directory operations
func (rm *RecoveryManager) processMkdir(entry *persistence.MkdirLogEntry) error {
	return rm.processCreateInode(&persistence.CreateInodeLogEntry{
		InodeId:     entry.InodeId,
		ParentId:    entry.ParentId,
		Name:        entry.Name,
		Attributes:  entry.Attributes,
		IsDirectory: true,
	})
}

func (rm *RecoveryManager) processRmdir(entry *persistence.RmdirLogEntry) error {
	return rm.processDeleteInode(&persistence.DeleteInodeLogEntry{
		InodeId:  entry.InodeId,
		ParentId: entry.ParentId,
		Name:     entry.Name,
	})
}

func (rm *RecoveryManager) processRename(entry *persistence.RenameLogEntry) error {
	// For rename, we need to update the btree key
	var foundItem *types.BTreeItem
	oldKey := types.BTreeKey{
		ParentIno: entry.OldParentId,
		Name:      entry.OldName,
	}

	// Find and remove old entry
	rm.master.Metadata.Ascend(func(item btree.Item) bool {
		btreeItem := item.(*types.BTreeItem)
		if btreeItem.Key == oldKey {
			foundItem = btreeItem
			return false
		}
		return true
	})

	if foundItem != nil {
		rm.master.Metadata.Delete(foundItem)

		// Insert with new key
		foundItem.Key = types.BTreeKey{
			ParentIno: entry.NewParentId,
			Name:      entry.NewName,
		}
		rm.master.Metadata.ReplaceOrInsert(foundItem)
	}

	return nil
}

func (rm *RecoveryManager) processSymlink(entry *persistence.SymlinkLogEntry) error {
	return rm.processCreateInode(&persistence.CreateInodeLogEntry{
		InodeId:     entry.InodeId,
		ParentId:    entry.ParentId,
		Name:        entry.Name,
		Attributes:  entry.Attributes,
		IsDirectory: false,
		Target:      entry.Target,
	})
}

func (rm *RecoveryManager) processUnlink(entry *persistence.UnlinkLogEntry) error {
	return rm.processDeleteInode(&persistence.DeleteInodeLogEntry{
		InodeId:  entry.InodeId,
		ParentId: entry.ParentId,
		Name:     entry.Name,
	})
}

func (rm *RecoveryManager) processSetattr(entry *persistence.SetattrLogEntry) error {
	return rm.processUpdateInode(&persistence.UpdateInodeLogEntry{
		InodeId:    entry.InodeId,
		Attributes: entry.Attributes,
		FieldMask:  entry.FieldMask,
	})
}

func (rm *RecoveryManager) processSetxattr(entry *persistence.SetxattrLogEntry) error {
	// Find the inode and set extended attribute
	rm.master.Metadata.Ascend(func(item btree.Item) bool {
		btreeItem := item.(*types.BTreeItem)
		if btreeItem.Inode != nil && btreeItem.Inode.Ino == entry.InodeId {
			btreeItem.Inode.Xattr[entry.Name] = entry.Value
			return false
		}
		return true
	})
	return nil
}

func (rm *RecoveryManager) processRemovexattr(entry *persistence.RemovexattrLogEntry) error {
	// Find the inode and remove extended attribute
	rm.master.Metadata.Ascend(func(item btree.Item) bool {
		btreeItem := item.(*types.BTreeItem)
		if btreeItem.Inode != nil && btreeItem.Inode.Ino == entry.InodeId {
			delete(btreeItem.Inode.Xattr, entry.Name)
			return false
		}
		return true
	})
	return nil
}

// ReplayLog replays a single log entry (placeholder)
func (rm *RecoveryManager) ReplayLog(entry []byte) error {
	// Parse log entry and apply the operation
	// This would depend on the log entry format
	return nil
}

// LoadCheckpoint loads the latest checkpoint (placeholder)
func (rm *RecoveryManager) LoadCheckpoint() error {
	// Load checkpoint data and restore master state
	return nil
}
