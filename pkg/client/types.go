package client

import (
	"sync"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/uttam-li/dfs/api/generated/master"
	"github.com/uttam-li/dfs/pkg/client/cache"
	"github.com/uttam-li/dfs/pkg/common"
	"google.golang.org/grpc"
)

// WriteBuffer holds buffered write data for efficient small writes
type WriteBuffer struct {
	mu          sync.RWMutex
	FileHandle  uint64
	NodeId      uint64
	StartOffset uint64
	Data        []byte
	LastWrite   time.Time
	Dirty       bool
}

// PendingWrite represents a write operation that needs to be serialized using GFS protocol
type PendingWrite struct {
	WriteID      string
	ChunkHandle  string
	ChunkIndex   uint64
	Offset       uint64
	Data         []byte
	ChunkVersion uint32
	PrimaryAddr  string
	ReplicaAddrs []string
	Timestamp    time.Time
}

// WriteSerializationJob represents a batch of writes to be serialized
type WriteSerializationJob struct {
	FileHandle  uint64
	NodeID      uint64
	Writes      []*PendingWrite
	CachedInode *master.Inode
}

// OpenFileInfo tracks the state of an open file handle
type OpenFileInfo struct {
	Inode      uint64       
	OpenFlags  uint32       // The flags used when opening (O_RDONLY, O_WRONLY, O_RDWR, etc.)
	RefCount   int          // Reference count for this file handle
	IsWrite    bool         // True if opened for writing (O_WRONLY or O_RDWR)
	OpenTime   time.Time    
	LastAccess time.Time    
	mu         sync.RWMutex
}

// ChunkReadRequest represents a read request for a specific chunk
type ChunkReadRequest struct {
	ChunkIndex uint64
	Offset     uint64
	Size       uint32
}

// ChunkReadResult represents the result of a chunk read operation
type ChunkReadResult struct {
	ChunkIndex uint64
	Data       []byte
	Err        error
}

// Filesystem represents a DFS filesystem client
type Filesystem struct {
	fuse.RawFileSystem

	// gRPC connections
	masterConn     *grpc.ClientConn
	masterClient   master.MasterServiceClient
	connectionPool *ChunkServerConnectionPool

	// Cache
	cache      *cache.Cache
	cacheLease *cache.ChunkLeaseCache

	// Configuration
	config *common.ClientConfig

	// Write buffering for small writes
	writeBuffers   map[uint64]*WriteBuffer // fh -> buffer
	writeBuffersMu sync.RWMutex

	// Open file tracking for proper file handle to inode mapping
	openFiles   map[uint64]*OpenFileInfo // fh -> open file info
	openFilesMu sync.RWMutex             // Protects openFiles map

	// State management
	nextFd uint64
	state  common.ServiceState

	// Synchronization
	mu      sync.RWMutex
	fdMutex sync.RWMutex
}
