package service

import (
	"fmt"

	"github.com/uttam-li/dfs/pkg/master/types"
)

// Lookup finds a child entry in a directory by name and returns its inode.
// Returns an error if the entry does not exist or is invalid.
func (ms *MasterService) Lookup(parentIno uint64, name string) (*types.Inode, error) {
	ms.metadataLock.RLock()
	defer ms.metadataLock.RUnlock()

	searchKey := &types.BTreeItem{
		Key: types.BTreeKey{
			ParentIno: parentIno,
			Name:      name,
		},
	}

	result := ms.master.Metadata.Get(searchKey)
	if result == nil {
		return nil, fmt.Errorf("entry not found: %s", name)
	}

	item := result.(*types.BTreeItem)
	if item.Inode == nil {
		return nil, fmt.Errorf("invalid entry: missing inode data")
	}

	return item.Inode, nil
}
