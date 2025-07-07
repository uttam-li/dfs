package service

import (
	"context"
	"fmt"
	"log"
	"maps"
	"sync"
	"syscall"
	"time"

	"github.com/google/btree"
	"github.com/google/uuid"
	"github.com/uttam-li/dfs/pkg/common"
	"github.com/uttam-li/dfs/pkg/master/persistence"
	"github.com/uttam-li/dfs/pkg/master/types"
)

// MasterService manages the distributed file system master operations
type MasterService struct {
	// Locks for thread-safe operations
	metadataLock    sync.RWMutex
	chunkServerLock sync.RWMutex

	// Configuration and core data
	config *common.MasterConfig
	master *types.Master

	// ChunkServer tracking
	chunkServers map[uuid.UUID]*ChunkServerInfo

	// Essential service managers only
	chunkServerClientManager *ChunkServerClientManager
	heartbeatManager         *HeartbeatManager
	persistenceManager       *persistence.LogManager
	recoveryManager          *persistence.RecoveryManager
	replicationManager       *ReplicationManager
	leaseManager             *LeaseManager
	gcManager                *GCManager

	// Service lifecycle
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	state  common.ServiceState
	mu     sync.RWMutex
}

// ChunkServerInfo contains information about a chunk server
type ChunkServerInfo struct {
	Address    string
	LastSeen   time.Time
	TotalSpace uint64
	UsedSpace  uint64
}

// NewMasterService creates a new master service instance
func NewMasterService(config *common.MasterConfig) (*MasterService, error) {
	if config == nil {
		return nil, fmt.Errorf("config cannot be nil")
	}

	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	master := &types.Master{
		Metadata:      *btree.New(32),
		ChunkMetadata: make(map[common.ChunkHandle]*types.ChunkMetadata),
		NextIno:       config.RootInode + 1,
		Root:          config.RootInode,
	}

	ms := &MasterService{
		config:       config,
		master:       master,
		chunkServers: make(map[uuid.UUID]*ChunkServerInfo),
		ctx:          ctx,
		cancel:       cancel,
		state:        common.ServiceStateUnknown,
	}

	rootInode := &types.Inode{
		Ino:    config.RootInode,
		Size:   4096,
		Mtime:  uint64(time.Now().Unix()),
		Nlink:  2,
		Xattr:  make(map[string][]byte),
		Chunks: make(map[uint64]common.ChunkHandle),
	}

	rootInode.MakeAttr(syscall.S_IFDIR|0755, 0, 0)

	master.Metadata.ReplaceOrInsert(&types.BTreeItem{
		Key: types.BTreeKey{
			ParentIno: config.RootInode,
			Name:      ".",
		},
		Inode: rootInode,
	})

	// Initialize service managers
	if err := ms.initializeManagers(); err != nil {
		cancel()
		return nil, fmt.Errorf("failed to initialize managers: %w", err)
	}

	log.Printf("Master service created successfully")
	return ms, nil
}

// initializeManagers initializes all the internal service managers
func (ms *MasterService) initializeManagers() error {
	// Initialize chunk server client manager
	ms.chunkServerClientManager = NewChunkServerClientManager()

	// Initialize heartbeat manager
	ms.heartbeatManager = NewHeartbeatManager(
		ms.config.HeartbeatTimeout,
		ms.config.ReplicationFactor,
	)
	ms.heartbeatManager.SetMasterService(ms)

	// Initialize persistence manager
	var err error
	ms.persistenceManager, err = persistence.NewLogManager(ms.config)
	if err != nil {
		return fmt.Errorf("failed to create persistence manager: %w", err)
	}

	// Initialize recovery manager
	ms.recoveryManager = persistence.NewRecoveryManager(
		ms.persistenceManager,
		ms.master,
	)

	// Initialize lease manager
	ms.leaseManager = NewLeaseManager(ms.config.LeaseTimeout)

	// Initialize replication manager
	ms.replicationManager = NewReplicationManager(ms)

	// Initialize garbage collection manager
	ms.gcManager = NewGCManager(ms)

	return nil
}

// Start starts the master service and all its managers
func (ms *MasterService) Start(ctx context.Context) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	if ms.state == common.ServiceStateRunning {
		return fmt.Errorf("service is already running")
	}

	if ms.state == common.ServiceStateStarting {
		return fmt.Errorf("service is already starting")
	}

	ms.state = common.ServiceStateStarting
	log.Printf("Starting master service...")

	// Start all managers in sequence
	startTimeout := time.NewTimer(ms.config.StartupTimeout)
	defer startTimeout.Stop()

	startCtx, startCancel := context.WithCancel(ctx)
	defer startCancel()

	// Channel to receive startup results
	startResult := make(chan error, 1)

	go func() {
		defer func() {
			if r := recover(); r != nil {
				startResult <- fmt.Errorf("panic during startup: %v", r)
			}
		}()

		if err := ms.startManagers(startCtx); err != nil {
			startResult <- err
			return
		}

		startResult <- nil
	}()

	select {
	case err := <-startResult:
		if err != nil {
			ms.state = common.ServiceStateError
			return fmt.Errorf("failed to start managers: %w", err)
		}
	case <-startTimeout.C:
		startCancel()
		ms.state = common.ServiceStateError
		return fmt.Errorf("startup timeout exceeded")
	case <-ctx.Done():
		startCancel()
		ms.state = common.ServiceStateStopped
		return ctx.Err()
	}

	ms.state = common.ServiceStateRunning
	log.Printf("Master service started successfully")
	return nil
}

// startManagers starts all internal managers
func (ms *MasterService) startManagers(ctx context.Context) error {
	// Start persistence manager with the master service's main context
	if err := ms.persistenceManager.Start(ms.ctx); err != nil {
		return fmt.Errorf("failed to start persistence manager: %w", err)
	}

	// Start chunk server client manager
	if err := ms.chunkServerClientManager.Start(ms.ctx); err != nil {
		return fmt.Errorf("failed to start chunk server client manager: %w", err)
	}

	// Perform recovery if needed (this can use the startup context)
	if err := ms.recoveryManager.Recover(ctx); err != nil {
		return fmt.Errorf("failed to recover from logs: %w", err)
	}

	// Start heartbeat manager with the master service's main context
	if err := ms.heartbeatManager.Start(ms.ctx); err != nil {
		return fmt.Errorf("failed to start heartbeat manager: %w", err)
	}

	// Start lease manager
	if err := ms.leaseManager.Start(ms.ctx); err != nil {
		return fmt.Errorf("failed to start lease manager: %w", err)
	}

	// Start replication manager
	if err := ms.replicationManager.Start(ms.ctx); err != nil {
		return fmt.Errorf("failed to start replication manager: %w", err)
	}

	// Start garbage collection manager
	if err := ms.gcManager.Start(ms.ctx); err != nil {
		return fmt.Errorf("failed to start garbage collection manager: %w", err)
	}

	return nil
}

// Stop gracefully stops the master service
func (ms *MasterService) Stop() error {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	if ms.state == common.ServiceStateStopped || ms.state == common.ServiceStateStopping {
		return nil
	}

	ms.state = common.ServiceStateStopping
	log.Printf("Stopping master service...")

	// Cancel context to signal all managers to stop
	ms.cancel()

	// Create timeout for graceful shutdown
	shutdownTimeout := time.NewTimer(ms.config.ShutdownTimeout)
	defer shutdownTimeout.Stop()

	// Channel to receive shutdown completion
	shutdownDone := make(chan struct{})

	go func() {
		defer close(shutdownDone)
		ms.stopManagers()
		ms.wg.Wait()
	}()

	select {
	case <-shutdownDone:
		log.Printf("Master service stopped successfully")
	case <-shutdownTimeout.C:
		log.Printf("Graceful shutdown timeout exceeded, forcing stop")
	}

	ms.state = common.ServiceStateStopped
	return nil
}

// stopManagers stops all internal managers
func (ms *MasterService) stopManagers() {
	// Stop managers in reverse order of initialization
	if ms.gcManager != nil {
		if err := ms.gcManager.Stop(); err != nil {
			log.Printf("Error stopping garbage collection manager: %v", err)
		}
	}

	if ms.replicationManager != nil {
		if err := ms.replicationManager.Stop(); err != nil {
			log.Printf("Error stopping replication manager: %v", err)
		}
	}

	if ms.leaseManager != nil {
		if err := ms.leaseManager.Stop(); err != nil {
			log.Printf("Error stopping lease manager: %v", err)
		}
	}

	if ms.heartbeatManager != nil {
		if err := ms.heartbeatManager.Stop(); err != nil {
			log.Printf("Error stopping heartbeat manager: %v", err)
		}
	}

	if ms.persistenceManager != nil {
		if err := ms.persistenceManager.Stop(); err != nil {
			log.Printf("Error stopping persistence manager: %v", err)
		}
	}

	if ms.chunkServerClientManager != nil {
		if err := ms.chunkServerClientManager.Stop(); err != nil {
			log.Printf("Error stopping chunk server client manager: %v", err)
		}
	}
}

// GetState returns the current state of the service
func (ms *MasterService) GetState() common.ServiceState {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	return ms.state
}

// IsRunning returns true if the service is running
func (ms *MasterService) IsRunning() bool {
	return ms.GetState() == common.ServiceStateRunning
}

// GetConfig returns the service configuration
func (ms *MasterService) GetConfig() *common.MasterConfig {
	return ms.config
}

// GetChunkServers returns a copy of the chunk servers map
func (ms *MasterService) GetChunkServers() map[uuid.UUID]*ChunkServerInfo {
	ms.chunkServerLock.RLock()
	defer ms.chunkServerLock.RUnlock()

	chunkServers := make(map[uuid.UUID]*ChunkServerInfo, len(ms.chunkServers))
	maps.Copy(chunkServers, ms.chunkServers)
	return chunkServers
}

// selectChunkServerAddresses selects available chunk server addresses for new chunks
func (ms *MasterService) selectChunkServerAddresses(count int) []string {
	ms.chunkServerLock.RLock()
	defer ms.chunkServerLock.RUnlock()

	var selected []string
	for _, serverInfo := range ms.chunkServers {
		if len(selected) >= count {
			break
		}
		// Basic health check - server seen recently
		if time.Since(serverInfo.LastSeen) < ms.config.HeartbeatTimeout {
			selected = append(selected, serverInfo.Address)
		}
	}
	return selected
}

// getServerIdByAddress finds the server ID for a given address (for compatibility)
func (ms *MasterService) getServerIdByAddress(address string) string {
	ms.chunkServerLock.RLock()
	defer ms.chunkServerLock.RUnlock()

	for serverUUID, serverInfo := range ms.chunkServers {
		if serverInfo.Address == address {
			return serverUUID.String()
		}
	}
	// Return empty string if not found - this maintains compatibility
	return ""
}

// GetGCManager returns the garbage collection manager
func (ms *MasterService) GetGCManager() *GCManager {
	return ms.gcManager
}

// MarkInodeForDeletion marks an inode for lazy deletion via the GC manager
func (ms *MasterService) MarkInodeForDeletion(ino uint64, inode *types.Inode) {
	if ms.gcManager != nil {
		ms.gcManager.MarkInodeForDeletion(ino, inode)
	}
}

// MarkChunkStale marks chunk replicas as stale via the GC manager
func (ms *MasterService) MarkChunkStale(handle common.ChunkHandle, staleServers []string) {
	if ms.gcManager != nil {
		// Use MarkChunkUnreferenced instead since MarkChunkStale doesn't exist
		ms.gcManager.MarkChunkUnreferenced(handle, 0, staleServers)
	}
}

// GetGCState returns garbage collection manager state
func (ms *MasterService) GetGCState() common.ServiceState {
	if ms.gcManager != nil {
		return ms.gcManager.GetState()
	}
	return common.ServiceStateUnknown
}

// GetLeaseManager returns the lease manager
func (ms *MasterService) GetLeaseManager() *LeaseManager {
	return ms.leaseManager
}

// GrantChunkLease grants a lease for a chunk with proper integration
func (ms *MasterService) GrantChunkLease(chunkHandle common.ChunkHandle, requestingServer string, replicas []string, version uint64) (*LeaseInfo, error) {
	if ms.leaseManager == nil {
		return nil, fmt.Errorf("lease manager not available")
	}

	req := LeaseRequest{
		ChunkHandle:      chunkHandle,
		RequestingServer: requestingServer,
		Replicas:         replicas,
		Version:          version,
		Metadata:         make(map[string]interface{}),
	}

	return ms.leaseManager.GrantLease(req)
}

// RevokeChunkLeasesForServer revokes all leases held by a specific server
func (ms *MasterService) RevokeChunkLeasesForServer(serverAddr, reason string) int {
	if ms.leaseManager == nil {
		return 0
	}

	return ms.leaseManager.RevokeAllLeasesForServer(serverAddr, reason)
}

// GetLeaseStats returns lease management statistics
func (ms *MasterService) GetLeaseStats() *LeaseStats {
	if ms.leaseManager != nil {
		return ms.leaseManager.GetLeaseStats()
	}
	return nil
}

// GetReplicationStats returns replication statistics
func (ms *MasterService) GetReplicationStats() map[string]interface{} {
	stats := make(map[string]interface{})

	if ms.replicationManager != nil && ms.replicationManager.IsRunning() {
		serverLoad := ms.replicationManager.GetServerLoadInfo()

		// Server load summary
		serverStats := make(map[string]interface{})
		totalServers := len(serverLoad)
		highLoadServers := 0
		avgDiskUsage := 0.0

		for _, load := range serverLoad {
			if load.TotalSpace > 0 {
				diskUsage := float64(load.UsedSpace) / float64(load.TotalSpace)
				if diskUsage > 0.85 {
					highLoadServers++
				}
				avgDiskUsage += diskUsage
			}
		}

		if totalServers > 0 {
			avgDiskUsage /= float64(totalServers)
		}

		serverStats["total_servers"] = totalServers
		serverStats["high_load_servers"] = highLoadServers
		serverStats["avg_disk_usage"] = avgDiskUsage
		stats["server_stats"] = serverStats
	}

	return stats
}
