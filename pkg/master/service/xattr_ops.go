package service

import (
	"fmt"
	"time"
)

// GetXAttr retrieves an extended attribute value
func (ms *MasterService) GetXAttr(ino uint64, name string) ([]byte, error) {
	ms.metadataLock.RLock()
	defer ms.metadataLock.RUnlock()

	foundItem, err := ms.findInodeByID(ino)
	if err != nil {
		return nil, err
	}

	if foundItem.Inode.Xattr == nil {
		return nil, fmt.Errorf("attribute not found: %s", name)
	}

	value, exists := foundItem.Inode.Xattr[name]
	if !exists {
		return nil, fmt.Errorf("attribute not found: %s", name)
	}

	return value, nil
}

// SetXAttr sets an extended attribute value
func (ms *MasterService) SetXAttr(ino uint64, name string, value []byte, flags uint32) error {
	ms.metadataLock.Lock()
	defer ms.metadataLock.Unlock()

	foundItem, err := ms.findInodeByID(ino)
	if err != nil {
		return err
	}

	// Initialize xattr map if nil
	if foundItem.Inode.Xattr == nil {
		foundItem.Inode.Xattr = make(map[string][]byte)
	}

	// Check flags
	_, exists := foundItem.Inode.Xattr[name]

	// XATTR_CREATE flag: fail if attribute already exists
	if flags&1 != 0 && exists {
		return fmt.Errorf("attribute already exists: %s", name)
	}

	// XATTR_REPLACE flag: fail if attribute doesn't exist
	if flags&2 != 0 && !exists {
		return fmt.Errorf("attribute not found: %s", name)
	}

	// Set the attribute
	foundItem.Inode.Xattr[name] = value

	// Update modification time
	foundItem.Inode.Mtime = uint64(time.Now().Unix())

	// Log the xattr operation for recovery
	if ms.persistenceManager != nil {
		err := ms.persistenceManager.LogSetxattr(ino, name, value, flags)
		if err != nil {
			return fmt.Errorf("failed to log setxattr operation: %w", err)
		}
	}

	return nil
}

// ListXAttr lists all extended attribute names
func (ms *MasterService) ListXAttr(ino uint64) ([]string, error) {
	ms.metadataLock.RLock()
	defer ms.metadataLock.RUnlock()

	foundItem, err := ms.findInodeByID(ino)
	if err != nil {
		return nil, err
	}

	if foundItem.Inode.Xattr == nil {
		return []string{}, nil
	}

	names := make([]string, 0, len(foundItem.Inode.Xattr))
	for name := range foundItem.Inode.Xattr {
		names = append(names, name)
	}

	return names, nil
}

// RemoveXAttr removes an extended attribute
func (ms *MasterService) RemoveXAttr(ino uint64, name string) error {
	ms.metadataLock.Lock()
	defer ms.metadataLock.Unlock()

	foundItem, err := ms.findInodeByID(ino)
	if err != nil {
		return err
	}

	if foundItem.Inode.Xattr == nil {
		return fmt.Errorf("attribute not found: %s", name)
	}

	_, exists := foundItem.Inode.Xattr[name]
	if !exists {
		return fmt.Errorf("attribute not found: %s", name)
	}

	delete(foundItem.Inode.Xattr, name)

	// Update modification time
	foundItem.Inode.Mtime = uint64(time.Now().Unix())

	// Log the xattr removal operation for recovery
	if ms.persistenceManager != nil {
		err := ms.persistenceManager.LogRemovexattr(ino, name)
		if err != nil {
			return fmt.Errorf("failed to log removexattr operation: %w", err)
		}
	}

	return nil
}
