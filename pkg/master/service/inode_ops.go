package service

import (
	"fmt"
	"log"
	"time"

	"github.com/google/btree"
	"github.com/uttam-li/dfs/api/generated/common"
	"github.com/uttam-li/dfs/pkg/master/types"
)

// findInodeByID finds an inode by its ID in the btree
func (ms *MasterService) findInodeByID(ino uint64) (*types.BTreeItem, error) {
	var foundItem *types.BTreeItem

	ms.master.Metadata.Ascend(func(item btree.Item) bool {
		btreeItem := item.(*types.BTreeItem)
		if btreeItem.Inode != nil && btreeItem.Inode.Ino == ino {
			foundItem = btreeItem
			return false // Stop iteration
		}
		return true // Continue iteration
	})

	if foundItem == nil {
		return nil, fmt.Errorf("inode not found: %d", ino)
	}

	return foundItem, nil
}

// GetAttributes retrieves the attributes of an inode
func (ms *MasterService) GetAttributes(ino uint64, attributeMask uint32) (*types.Inode, error) {
	ms.metadataLock.RLock()
	defer ms.metadataLock.RUnlock()

	btreeItem, err := ms.findInodeByID(ino)
	if err != nil {
		return nil, err
	}

	return btreeItem.Inode, nil
}

// Statx retrieves extended attributes of an inode
func (ms *MasterService) Statx(ino uint64, mask uint32, flags uint32) (*types.Inode, uint32, error) {
	ms.metadataLock.RLock()
	defer ms.metadataLock.RUnlock()

	btreeItem, err := ms.findInodeByID(ino)
	if err != nil {
		return nil, 0, err
	}

	// For now, we support all standard fields
	// In a real implementation, you might optimize based on the requested mask
	supportedMask := mask // Return the same mask indicating we have all requested fields

	return btreeItem.Inode, supportedMask, nil
}

// SetAttributes updates the attributes of an inode
func (ms *MasterService) SetAttributes(ino uint64, attrs *common.FileAttributes, attributeMask uint32) (*common.FileAttributes, error) {
	ms.metadataLock.Lock()
	defer ms.metadataLock.Unlock()

	// Use the common method to find the inode
	foundItem, err := ms.findInodeByID(ino)
	if err != nil {
		return nil, err
	}

	now := uint64(time.Now().Unix())

	// Update attributes based on the attribute mask
	if attributeMask&(1<<0) != 0 { // MODE
		foundItem.Inode.MakeAttr(attrs.Mode, foundItem.Inode.GetUID(), foundItem.Inode.GetGID())
	}
	if attributeMask&(1<<1) != 0 { // UID
		foundItem.Inode.MakeAttr(foundItem.Inode.GetMode(), attrs.Uid, foundItem.Inode.GetGID())
	}
	if attributeMask&(1<<2) != 0 { // GID
		foundItem.Inode.MakeAttr(foundItem.Inode.GetMode(), foundItem.Inode.GetUID(), attrs.Gid)
	}
	if attributeMask&(1<<3) != 0 { // SIZE
		foundItem.Inode.Size = attrs.Size
		// TODO: Handle chunk truncation/extension
	}
	if attributeMask&(1<<4) != 0 { // ATIME
		// Update access time in extended metadata if needed
	}
	if attributeMask&(1<<5) != 0 { // MTIME
		foundItem.Inode.Mtime = uint64(attrs.Mtime)
	}

	// Always update ctime when attributes change
	foundItem.Inode.Mtime = now // Using mtime field for simplicity

	// Log the inode update for recovery
	if ms.persistenceManager != nil {
		updatedAttrs := &common.FileAttributes{
			Mode:   foundItem.Inode.GetMode(),
			Uid:    foundItem.Inode.GetUID(),
			Gid:    foundItem.Inode.GetGID(),
			Size:   foundItem.Inode.Size,
			Atime:  int64(now),
			Mtime:  int64(foundItem.Inode.Mtime),
			Ctime:  int64(now),
			Nlink:  uint32(foundItem.Inode.Nlink),
			Blocks: (foundItem.Inode.Size + 4095) / 4096, // Assume 4KB blocks
		}

		// Determine which fields changed based on attribute mask
		var fieldMask []string
		if attributeMask&(1<<0) != 0 { // MODE
			fieldMask = append(fieldMask, "mode")
		}
		if attributeMask&(1<<1) != 0 { // UID
			fieldMask = append(fieldMask, "uid")
		}
		if attributeMask&(1<<2) != 0 { // GID
			fieldMask = append(fieldMask, "gid")
		}
		if attributeMask&(1<<3) != 0 { // SIZE
			fieldMask = append(fieldMask, "size")
		}
		if attributeMask&(1<<4) != 0 { // ATIME
			fieldMask = append(fieldMask, "atime")
		}
		if attributeMask&(1<<5) != 0 { // MTIME
			fieldMask = append(fieldMask, "mtime")
		}

		err := ms.persistenceManager.LogUpdateInode(ino, updatedAttrs, fieldMask)
		if err != nil {
			log.Printf("Failed to log inode update for ino=%d: %v", ino, err)
		}
	}

	// Return the updated attributes
	return &common.FileAttributes{
		Mode:   foundItem.Inode.GetMode(),
		Uid:    foundItem.Inode.GetUID(),
		Gid:    foundItem.Inode.GetGID(),
		Size:   foundItem.Inode.Size,
		Atime:  int64(now),
		Mtime:  int64(foundItem.Inode.Mtime),
		Ctime:  int64(now),
		Nlink:  uint32(foundItem.Inode.Nlink),
		Blocks: (foundItem.Inode.Size + 4095) / 4096, // Assume 4KB blocks
	}, nil
}
