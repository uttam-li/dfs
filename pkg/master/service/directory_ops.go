package service

import (
	"syscall"

	"github.com/google/btree"
	"github.com/uttam-li/dfs/api/generated/common"
	"github.com/uttam-li/dfs/api/generated/master"
	"github.com/uttam-li/dfs/pkg/master/types"
)

// ListDirectory lists entries in a directory
func (ms *MasterService) ListDirectory(dirIno uint64, continuationToken string, maxEntries uint32) ([]*master.ListDirectoryResponse_DirectoryEntry, string, bool, error) {
	ms.metadataLock.RLock()
	defer ms.metadataLock.RUnlock()

	var entries []*master.ListDirectoryResponse_DirectoryEntry
	var nextToken string
	var hasMore bool
	var count uint32

	// If maxEntries is 0, set a reasonable default
	if maxEntries == 0 {
		maxEntries = 1000
	}

	startName := continuationToken

	ms.master.Metadata.Ascend(func(item btree.Item) bool {
		btreeItem := item.(*types.BTreeItem)
		if btreeItem.Key.ParentIno == dirIno {
			// Skip until we reach the continuation point
			if startName != "" && btreeItem.Key.Name <= startName {
				return true
			}

			if count >= maxEntries {
				hasMore = true
				nextToken = btreeItem.Key.Name
				return false // Stop iteration
			}

			// Determine entry type
			entryType := uint32(0) // FILE
			if btreeItem.Inode.IsDir() {
				entryType = 1 // DIRECTORY
			} else if btreeItem.Inode.GetMode()&syscall.S_IFLNK != 0 {
				entryType = 2 // SYMLINK
			}

			entry := &master.ListDirectoryResponse_DirectoryEntry{
				Name: btreeItem.Key.Name,
				Ino:  btreeItem.Inode.Ino,
				Type: entryType,
				Attributes: &common.FileAttributes{
					Mode:   btreeItem.Inode.GetMode(),
					Uid:    btreeItem.Inode.GetUID(),
					Gid:    btreeItem.Inode.GetGID(),
					Size:   btreeItem.Inode.Size,
					Atime:  int64(btreeItem.Inode.Mtime),
					Mtime:  int64(btreeItem.Inode.Mtime),
					Ctime:  int64(btreeItem.Inode.Mtime),
					Nlink:  uint32(btreeItem.Inode.Nlink),
					Blocks: (btreeItem.Inode.Size + 4095) / 4096,
				},
			}

			entries = append(entries, entry)
			count++
		}
		return true
	})

	return entries, nextToken, hasMore, nil
}
