package client

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/uttam-li/dfs/api/generated/master"
	"github.com/uttam-li/dfs/pkg/client/cache"
	"github.com/uttam-li/dfs/pkg/common"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// NewFilesystem creates a new DFS filesystem client with the provided configuration.
func NewFilesystem(config *common.ClientConfig) (*Filesystem, error) {
	if config == nil {
		return nil, fmt.Errorf("config cannot be nil")
	}

	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	fs := &Filesystem{
		config:       config,
		nextFd:       1,
		state:        common.ServiceStateUnknown,
		writeBuffers: make(map[uint64]*WriteBuffer),
		openFiles:    make(map[uint64]*OpenFileInfo),
	}

	fs.initializeCaches(config)
	fs.initializeConnectionPool()

	if err := fs.connectToMaster(); err != nil {
		return nil, fmt.Errorf("failed to connect to master: %w", err)
	}

	go fs.startBufferCleanupRoutine()

	fs.state = common.ServiceStateRunning
	log.Printf("DFS filesystem client created successfully")
	return fs, nil
}

// initializeCaches sets up the metadata and chunk lease caches.
func (fs *Filesystem) initializeCaches(config *common.ClientConfig) {
	fs.cache = cache.NewCache(config.MetadataCacheTTL, config.ChunkLocCacheTTL, config.MaxMetadataEntries)
	fs.cacheLease = cache.NewChunkLeaseCache()
}

// initializeConnectionPool creates a high-performance connection pool for chunk servers.
func (fs *Filesystem) initializeConnectionPool() {
	poolConfig := ConnectionPoolConfig{
		MaxIdleConns:        20,
		IdleTimeout:         2 * time.Minute,
		ConnectTimeout:      3 * time.Second,
		MaxLifetime:         10 * time.Minute,
		HealthCheckInterval: 60 * time.Second,
	}
	fs.connectionPool = NewChunkServerConnectionPool(poolConfig)
}

// connectToMaster establishes a gRPC connection to the master server.
func (fs *Filesystem) connectToMaster() error {
	conn, err := grpc.NewClient(
		fs.config.MasterAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallSendMsgSize(common.DefaultMaxGRPCMessageSize),
			grpc.MaxCallRecvMsgSize(common.DefaultMaxGRPCMessageSize),
		),
	)
	if err != nil {
		return fmt.Errorf("failed to connect to master server: %w", err)
	}

	fs.masterConn = conn
	fs.masterClient = master.NewMasterServiceClient(conn)

	if fs.config.Debug {
		log.Printf("Connected to master server at: %s", fs.config.MasterAddr)
	}

	return nil
}

// Start initializes the filesystem client for operation.
func (fs *Filesystem) Start(ctx context.Context) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	if fs.state == common.ServiceStateRunning {
		return nil
	}

	fs.state = common.ServiceStateRunning
	return nil
}

// Close gracefully shuts down the filesystem client and releases all resources.
func (fs *Filesystem) Close() error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	if fs.state == common.ServiceStateStopped {
		return nil
	}

	fs.state = common.ServiceStateStopping

	if fs.config.Debug {
		log.Printf("Shutting down client connections...")
	}

	if fs.connectionPool != nil {
		fs.connectionPool.Close()
	}

	if fs.masterConn != nil {
		if err := fs.masterConn.Close(); err != nil {
			log.Printf("Error closing master connection: %v", err)
			return err
		}
	}

	fs.state = common.ServiceStateStopped
	if fs.config.Debug {
		log.Printf("Filesystem client stopped successfully")
	}

	return nil
}

// GetState returns the current operational state of the filesystem client.
func (fs *Filesystem) GetState() common.ServiceState {
	fs.mu.RLock()
	defer fs.mu.RUnlock()
	return fs.state
}

// IsRunning returns true if the filesystem client is currently operational.
func (fs *Filesystem) IsRunning() bool {
	return fs.GetState() == common.ServiceStateRunning
}

func (fs *Filesystem) cleanupStaleWriteBuffers() {
	const maxBufferAge = 5 * time.Second

	fs.writeBuffersMu.Lock()
	defer fs.writeBuffersMu.Unlock()

	now := time.Now()
	var staleFhs []uint64

	for fh, buffer := range fs.writeBuffers {
		buffer.mu.Lock()
		isStale := now.Sub(buffer.LastWrite) > maxBufferAge
		hasData := len(buffer.Data) > 0
		buffer.mu.Unlock()

		if isStale {
			if hasData && fs.config.Debug {
				log.Printf("[DEBUG] Found stale buffer for fh=%d with %d bytes", fh, len(buffer.Data))
			}
			staleFhs = append(staleFhs, fh)
		}
	}

	for _, fh := range staleFhs {
		delete(fs.writeBuffers, fh)
	}
}

func (fs *Filesystem) startBufferCleanupRoutine() {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		<-ticker.C
		fs.cleanupStaleWriteBuffers()

		fs.mu.RLock()
		isRunning := fs.state == common.ServiceStateRunning
		fs.mu.RUnlock()

		if !isRunning {
			return
		}
	}
}
