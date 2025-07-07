package types

import (
	"sync"
	"syscall"

	"github.com/google/btree"
	"github.com/uttam-li/dfs/pkg/common"
)

const (
	MODE_SHIFT = 32 // Mode gets high 32 bits
	UID_SHIFT  = 16 // UID gets middle 16 bits
	GID_SHIFT  = 0  // GID gets low 16 bits

	// Updated masks to match the shifts
	MODE_MASK = uint64(0xFFFFFFFF) << MODE_SHIFT // 32 bits for mode including file type
	UID_MASK  = uint64(0xFFFF) << UID_SHIFT      // 16 bits for UID (max 65535)
	GID_MASK  = uint64(0xFFFF) << GID_SHIFT      // 16 bits for GID (max 65535)
)

type Master struct {
	mu sync.RWMutex

	Metadata      btree.BTree
	ChunkMetadata map[common.ChunkHandle]*ChunkMetadata

	NextIno uint64
	Root    uint64
}

func (m *Master) GetAndIncrementNextIno() uint64 {
	ino := m.NextIno
	m.NextIno++
	return ino
}

type ChunkMetadata struct {
	Version   uint16
	Size      uint32
	Locations []string
}

type BTreeKey struct {
	ParentIno uint64
	Name      string
}

type BTreeItem struct {
	Key   BTreeKey
	Inode *Inode
}

// Less implements btree.Item interface. It compares two BTreeItems,
// first by ParentIno, then by Name.
func (i *BTreeItem) Less(than btree.Item) bool {
	other := than.(*BTreeItem)
	if i.Key.ParentIno != other.Key.ParentIno {
		return i.Key.ParentIno < other.Key.ParentIno
	}
	return i.Key.Name < other.Key.Name
}

// IsDir returns true if the item is a directory
func (i *BTreeItem) IsDir() bool {
	if i.Inode == nil {
		return false
	}
	mode := i.Inode.GetMode()
	return (mode & syscall.S_IFDIR) != 0
}

type Inode struct {
	Attr   uint64
	Ino    uint64
	Size   uint64
	Mtime  uint64
	Chunks map[uint64]common.ChunkHandle
	Xattr  map[string][]byte
	Nlink  uint16
}

func (ino *Inode) GetMode() uint32 {
	return uint32((ino.Attr & MODE_MASK) >> MODE_SHIFT)
}

func (ino *Inode) GetUID() uint32 {
	return uint32((ino.Attr & UID_MASK) >> UID_SHIFT)
}

func (ino *Inode) GetGID() uint32 {
	return uint32((ino.Attr & GID_MASK) >> GID_SHIFT)
}

func (ino *Inode) MakeAttr(mode uint32, uid uint32, gid uint32) {
	modeVal := uint64(mode) & 0xFFFFFFFF // 32 bits for mode
	uidVal := uint64(uid) & 0xFFFF       // 16 bits for UID
	gidVal := uint64(gid) & 0xFFFF       // 16 bits for GID

	ino.Attr = (modeVal << MODE_SHIFT) | (uidVal << UID_SHIFT) | (gidVal << GID_SHIFT)
}

// IsDir returns true if the inode represents a directory
func (ino *Inode) IsDir() bool {
	mode := ino.GetMode()
	return (mode & syscall.S_IFDIR) != 0
}

// ServerVersionMap tracks version information for chunks on a server
type ServerVersionMap struct {
	Versions map[common.ChunkHandle]uint64
	mu       sync.RWMutex
}

// NewServerVersionMap creates a new server version map
func NewServerVersionMap() *ServerVersionMap {
	return &ServerVersionMap{
		Versions: make(map[common.ChunkHandle]uint64),
	}
}

// GetVersion gets the version for a chunk
func (svm *ServerVersionMap) GetVersion(handle common.ChunkHandle) (uint64, bool) {
	svm.mu.RLock()
	defer svm.mu.RUnlock()
	version, exists := svm.Versions[handle]
	return version, exists
}

// SetVersion sets the version for a chunk
func (svm *ServerVersionMap) SetVersion(handle common.ChunkHandle, version uint64) {
	svm.mu.Lock()
	defer svm.mu.Unlock()
	svm.Versions[handle] = version
}

// RemoveChunk removes a chunk from the version map
func (svm *ServerVersionMap) RemoveChunk(handle common.ChunkHandle) {
	svm.mu.Lock()
	defer svm.mu.Unlock()
	delete(svm.Versions, handle)
}
