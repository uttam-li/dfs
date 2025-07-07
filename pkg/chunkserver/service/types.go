package service

import (
	"context"
	"sync"
	"time"

	"github.com/uttam-li/dfs/api/generated/chunkserver"
	"github.com/uttam-li/dfs/api/generated/master"
	"github.com/uttam-li/dfs/pkg/common"
	"google.golang.org/grpc"
)

// ChunkServerConnection represents a connection to another chunkserver
type ChunkServerConnection struct {
	conn   *grpc.ClientConn
	client chunkserver.ChunkServerServiceClient
	addr   string
}

// WriteBuffer holds buffered write data for the two-phase write protocol
type WriteBuffer struct {
	Handle      string
	Offset      uint64
	Data        []byte
	Version     uint32
	CreatedTime time.Time
}

// ChunkMetadata holds metadata for a chunk stored on this server
type ChunkMetadata struct {
	Handle       string
	Version      uint32
	Size         uint64
	Checksum     []byte
	CreatedTime  time.Time
	LastAccessed time.Time
	LastModified time.Time
	FilePath     string
}

// ChunkStorage manages chunk files on disk
type ChunkStorage struct {
	storageRoot string
	chunks      map[string]*ChunkMetadata
	mu          sync.RWMutex
}

// ChunkServerService manages chunk server operations
type ChunkServerService struct {
	config  *common.ChunkServerConfig
	storage *ChunkStorage

	// Master connection
	masterConn        *grpc.ClientConn
	masterClient      master.MasterServiceClient
	serverID          string
	registered        bool
	heartbeatInterval time.Duration

	chunkServerConns map[string]*ChunkServerConnection // address -> connection
	chunkServerMu    sync.RWMutex

	writeBuffers   map[string]*WriteBuffer // writeID -> buffer
	writeBuffersMu sync.RWMutex

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	state  common.ServiceState
	mu     sync.RWMutex
}
