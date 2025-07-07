package service

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/uttam-li/dfs/api/generated/chunkserver"
	"github.com/uttam-li/dfs/pkg/common"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// NewChunkServerService creates a new chunk server service
func NewChunkServerService(config *common.ChunkServerConfig) (*ChunkServerService, error) {
	if config == nil {
		return nil, fmt.Errorf("config cannot be nil")
	}

	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	cs := &ChunkServerService{
		config: config,
		storage: &ChunkStorage{
			storageRoot: config.StorageRoot,
			chunks:      make(map[string]*ChunkMetadata),
		},
		serverID:          config.ID.String(),
		registered:        false,
		heartbeatInterval: config.HeartbeatInterval,
		chunkServerConns:  make(map[string]*ChunkServerConnection),
		writeBuffers:      make(map[string]*WriteBuffer),
		ctx:               ctx,
		cancel:            cancel,
		state:             common.ServiceStateUnknown,
	}
	log.Printf("Chunk server service created successfully with ID: %s", cs.serverID)
	return cs, nil
}

// Start starts the chunk server service
func (cs *ChunkServerService) Start(ctx context.Context) error {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	if cs.state == common.ServiceStateRunning {
		return fmt.Errorf("service is already running")
	}

	if cs.state == common.ServiceStateStarting {
		return fmt.Errorf("service is already starting")
	}

	cs.state = common.ServiceStateStarting

	if err := cs.initialize(); err != nil {
		cs.state = common.ServiceStateError
		return fmt.Errorf("failed to initialize: %w", err)
	}

	if err := cs.connectToMaster(); err != nil {
		cs.state = common.ServiceStateError
		return fmt.Errorf("failed to connect to master: %w", err)
	}

	if err := cs.registerWithMaster(); err != nil {
		cs.state = common.ServiceStateError
		return fmt.Errorf("failed to register with master: %w", err)
	}

	cs.wg.Add(1)
	go cs.heartbeatRoutine()
	cs.startBufferCleanupRoutine()
	cs.startGracefulShutdownCleanup()

	cs.state = common.ServiceStateRunning
	log.Printf("Chunk server service started successfully")
	return nil
}

// Stop gracefully stops the chunk server service
func (cs *ChunkServerService) Stop() error {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	if cs.state == common.ServiceStateStopped || cs.state == common.ServiceStateStopping {
		return nil
	}

	cs.state = common.ServiceStateStopping
	log.Printf("Stopping chunk server service...")

	cs.cancel()
	cs.wg.Wait()
	cs.cleanupAllWriteBuffers()
	cs.closeChunkServerConnections()
	cs.disconnectFromMaster()

	cs.state = common.ServiceStateStopped
	return nil
}

// GetState returns the current state of the service
func (cs *ChunkServerService) GetState() common.ServiceState {
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	return cs.state
}

// IsRunning returns true if the service is running
func (cs *ChunkServerService) IsRunning() bool {
	return cs.GetState() == common.ServiceStateRunning
}

// GetConfig returns the service configuration
func (cs *ChunkServerService) GetConfig() *common.ChunkServerConfig {
	return cs.config
}

// WaitForShutdown blocks until the service is stopped
func (cs *ChunkServerService) WaitForShutdown() {
	cs.wg.Wait()
}

// SignalShutdown signals the service to stop
func (cs *ChunkServerService) SignalShutdown() {
	cs.cancel()
}

// getChunkServerConnection gets or creates a connection to another chunk server
func (cs *ChunkServerService) getChunkServerConnection(address string) (*ChunkServerConnection, error) {
	if address == "" {
		return nil, fmt.Errorf("address cannot be empty")
	}

	cs.chunkServerMu.RLock()
	if conn, exists := cs.chunkServerConns[address]; exists {
		cs.chunkServerMu.RUnlock()
		return conn, nil
	}
	cs.chunkServerMu.RUnlock()

	cs.chunkServerMu.Lock()
	defer cs.chunkServerMu.Unlock()

	if conn, exists := cs.chunkServerConns[address]; exists {
		return conn, nil
	}

	grpcConn, err := grpc.NewClient(address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallSendMsgSize(common.DefaultMaxGRPCMessageSize),
			grpc.MaxCallRecvMsgSize(common.DefaultMaxGRPCMessageSize),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to chunk server %s: %w", address, err)
	}

	conn := &ChunkServerConnection{
		conn:   grpcConn,
		client: chunkserver.NewChunkServerServiceClient(grpcConn),
		addr:   address,
	}

	cs.chunkServerConns[address] = conn
	return conn, nil
}

// closeChunkServerConnections closes all chunk server connections
func (cs *ChunkServerService) closeChunkServerConnections() {
	cs.chunkServerMu.Lock()
	defer cs.chunkServerMu.Unlock()

	for addr, conn := range cs.chunkServerConns {
		if err := conn.conn.Close(); err != nil {
			log.Printf("Failed to close connection to chunk server %s: %v", addr, err)
		}
	}
	cs.chunkServerConns = make(map[string]*ChunkServerConnection)
}

// startGracefulShutdownCleanup starts a background cleanup routine for interrupted operations
func (cs *ChunkServerService) startGracefulShutdownCleanup() {
	cs.wg.Add(1)
	go func() {
		defer cs.wg.Done()
		<-cs.ctx.Done()
		cs.cleanupAllWriteBuffers()
		time.Sleep(100 * time.Millisecond)
	}()
}
