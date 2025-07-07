package service

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	chunkserver "github.com/uttam-li/dfs/api/generated/chunkserver"
	"github.com/uttam-li/dfs/pkg/common"
)

// ReplicationPriority defines the urgency level for chunk replication operations.
type ReplicationPriority int

const (
	PriorityCritical ReplicationPriority = iota // 0 replicas - data loss risk
	PriorityHigh                                // < half desired replicas
	PriorityNormal                              // below desired but safe
	PriorityLow                                 // cleanup/optimization
)

// ReplicationManager manages chunk replication across the distributed file system.
type ReplicationManager struct {
	mu            sync.RWMutex
	masterService *MasterService

	replicationQueue   chan *ReplicationTask
	priorityQueue      chan *ReplicationTask // High-priority queue for critical chunks
	activeReplications map[common.ChunkHandle]*ReplicationStatus
	batchReplications  map[string]*BatchReplicationJob // server -> batch job

	// Configuration for replication
	replicationFactor    int
	maxWorkers           int
	maxBatchSize         int           // Maximum chunks per batch
	maxConcurrentBatches int           // Maximum concurrent batches per server
	batchTimeout         time.Duration // Timeout for batch operations
	monitorInterval      time.Duration

	// Chunk streaming configuration
	streamChunkSize uint32        // Size of streaming chunks (e.g., 1MB)
	streamTimeout   time.Duration // Timeout for streaming operations

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	state  common.ServiceState
}

// ReplicationTask represents a chunk replication request with priority.
type ReplicationTask struct {
	ChunkHandle common.ChunkHandle
	Priority    ReplicationPriority
}

// DeletionTask represents a chunk deletion request (kept for compatibility).
type DeletionTask struct {
	ChunkHandle common.ChunkHandle
	ServerAddr  string
	Reason      string
}

// NewReplicationManager creates a new replication manager instance.
func NewReplicationManager(masterService *MasterService) *ReplicationManager {
	ctx, cancel := context.WithCancel(context.Background())

	return &ReplicationManager{
		masterService:        masterService,
		replicationQueue:     make(chan *ReplicationTask, 1000),
		priorityQueue:        make(chan *ReplicationTask, 500),
		activeReplications:   make(map[common.ChunkHandle]*ReplicationStatus),
		batchReplications:    make(map[string]*BatchReplicationJob),
		replicationFactor:    masterService.config.ReplicationFactor,
		maxWorkers:           6,
		maxBatchSize:         10,
		maxConcurrentBatches: 3,
		batchTimeout:         5 * time.Minute,
		monitorInterval:      90 * time.Second,
		streamChunkSize:      1024 * 1024,
		streamTimeout:        30 * time.Second,
		ctx:                  ctx,
		cancel:               cancel,
		state:                common.ServiceStateUnknown,
	}
}

// Start starts the replication manager and its worker goroutines.
func (rm *ReplicationManager) Start(ctx context.Context) error {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	if rm.state == common.ServiceStateRunning {
		return fmt.Errorf("replication manager already running")
	}

	rm.state = common.ServiceStateRunning

	for i := 0; i < rm.maxWorkers; i++ {
		rm.wg.Add(1)
		go rm.replicationWorker(i)
	}

	rm.wg.Add(1)
	go rm.batchReplicationCoordinator()

	rm.wg.Add(1)
	go rm.replicationMonitor()

	log.Printf("Replication manager started with %d workers, batch coordinator, and monitor (interval: %v)",
		rm.maxWorkers, rm.monitorInterval)
	return nil
}

// Stop stops the replication manager and waits for all workers to finish.
func (rm *ReplicationManager) Stop() error {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	if rm.state != common.ServiceStateRunning {
		return nil
	}

	rm.state = common.ServiceStateStopping
	rm.cancel()
	close(rm.replicationQueue)
	rm.wg.Wait()

	rm.state = common.ServiceStateStopped
	log.Printf("Replication manager stopped")
	return nil
}

// IsRunning returns true if the replication manager is running
func (rm *ReplicationManager) IsRunning() bool {
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	return rm.state == common.ServiceStateRunning
}

// UpdateServerLoad updates server load information.
func (rm *ReplicationManager) UpdateServerLoad(addr string, totalSpace, usedSpace uint64, cpuUsage, memoryUsage float64, connections int) {
	// Simplified - no complex load tracking needed for basic replication
}

// QueueReplication queues a chunk for replication with the specified priority.
func (rm *ReplicationManager) QueueReplication(chunkHandle common.ChunkHandle, priority ReplicationPriority) {
	if !rm.IsRunning() {
		return
	}

	rm.mu.Lock()
	if _, exists := rm.activeReplications[chunkHandle]; exists {
		rm.mu.Unlock()
		return
	}
	rm.mu.Unlock()

	task := &ReplicationTask{
		ChunkHandle: chunkHandle,
		Priority:    priority,
	}

	var targetQueue chan *ReplicationTask
	if priority == PriorityCritical || priority == PriorityHigh {
		targetQueue = rm.priorityQueue
	} else {
		targetQueue = rm.replicationQueue
	}

	select {
	case targetQueue <- task:
		// Successfully queued
	default:
		log.Printf("REPLICATION: Queue full, dropping task for chunk %v", chunkHandle)
	}
}

// QueueChunkDeletion delegates chunk deletion to the GC manager (kept for compatibility).
func (rm *ReplicationManager) QueueChunkDeletion(chunkHandle common.ChunkHandle, serverAddr, reason string) {
	if rm.masterService.gcManager != nil {
		// Delegation would happen here when gcManager.QueueChunkDeletion is available
	}
}

// HandleServerFailure handles server failure by triggering replication checks.
func (rm *ReplicationManager) HandleServerFailure(serverAddr string) {
	if !rm.IsRunning() {
		return
	}

	log.Printf("REPLICATION: Handling server failure: %s", serverAddr)
	rm.CheckReplicationNeeds()
}

// CheckReplicationNeeds scans for under-replicated chunks and queues them for replication.
func (rm *ReplicationManager) CheckReplicationNeeds() {
	if !rm.IsRunning() {
		return
	}

	rm.masterService.metadataLock.RLock()
	defer rm.masterService.metadataLock.RUnlock()

	underReplicated := 0
	activeServers := rm.getActiveServers()

	for chunkHandle, chunkMeta := range rm.masterService.master.ChunkMetadata {
		activeReplicas := rm.countActiveReplicas(chunkMeta.Locations, activeServers)

		if activeReplicas < rm.replicationFactor {
			underReplicated++
			priority := rm.calculatePriority(activeReplicas)
			rm.QueueReplication(chunkHandle, priority)
		}
	}

	if underReplicated > 0 {
		log.Printf("REPLICATION: Found %d under-replicated chunks", underReplicated)
	}
}

// getActiveServers returns a map of currently active chunk servers.
func (rm *ReplicationManager) getActiveServers() map[string]bool {
	activeServers := make(map[string]bool)
	heartbeatTimeout := rm.masterService.config.HeartbeatTimeout
	now := time.Now()

	rm.masterService.chunkServerLock.RLock()
	defer rm.masterService.chunkServerLock.RUnlock()

	for _, serverInfo := range rm.masterService.chunkServers {
		if now.Sub(serverInfo.LastSeen) <= heartbeatTimeout {
			activeServers[serverInfo.Address] = true
		}
	}

	return activeServers
}

// countActiveReplicas counts how many chunk replicas are on active servers.
func (rm *ReplicationManager) countActiveReplicas(locations []string, activeServers map[string]bool) int {
	count := 0
	for _, location := range locations {
		if activeServers[location] {
			count++
		}
	}
	return count
}

// calculatePriority determines replication priority based on current replica count.
func (rm *ReplicationManager) calculatePriority(currentReplicas int) ReplicationPriority {
	if currentReplicas == 0 {
		return PriorityCritical
	} else if currentReplicas < rm.replicationFactor/2 {
		return PriorityHigh
	} else {
		return PriorityNormal
	}
}

// GetServerLoadInfo returns server load information (simplified for compatibility).
func (rm *ReplicationManager) GetServerLoadInfo() map[string]*ServerLoadInfo {
	return make(map[string]*ServerLoadInfo)
}

// ForceStaleServerCleanup triggers immediate replication checks for stale servers.
func (rm *ReplicationManager) ForceStaleServerCleanup() {
	if !rm.IsRunning() {
		return
	}

	log.Printf("REPLICATION: Force cleanup triggered")
	rm.CheckReplicationNeeds()
}

// ReplicationStatus tracks the status of an ongoing chunk replication operation.
type ReplicationStatus struct {
	ChunkHandle      common.ChunkHandle
	SourceServer     string
	TargetServer     string
	StartTime        time.Time
	Priority         ReplicationPriority
	StreamsActive    int
	BytesTransferred uint64
	TotalBytes       uint64
}

// BatchReplicationJob manages multiple chunk replications to the same target server.
type BatchReplicationJob struct {
	TargetServer    string
	Chunks          []*ReplicationTask
	StartTime       time.Time
	CompletedChunks int
	FailedChunks    int
	mu              sync.RWMutex
}

// AddChunk safely adds a chunk task to the batch job.
func (b *BatchReplicationJob) AddChunk(task *ReplicationTask) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.Chunks = append(b.Chunks, task)
}

// MarkCompleted safely marks a chunk as completed or failed.
func (b *BatchReplicationJob) MarkCompleted(success bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if success {
		b.CompletedChunks++
	} else {
		b.FailedChunks++
	}
}

// StreamingChunk represents a chunk of data for streaming replication operations.
type StreamingChunk struct {
	ChunkHandle common.ChunkHandle
	Offset      uint64
	Data        []byte
	SequenceNum uint32
	IsLast      bool
}

// replicationWorker processes replication tasks
func (rm *ReplicationManager) replicationWorker(workerID int) {
	defer rm.wg.Done()

	log.Printf("REPLICATION: Worker %d started", workerID)

	for {
		select {
		case <-rm.ctx.Done():
			log.Printf("REPLICATION: Worker %d stopping", workerID)
			return
		case task := <-rm.priorityQueue:
			if task == nil {
				return
			}
			log.Printf("REPLICATION: Worker %d processing priority task %v", workerID, task.ChunkHandle)
			rm.processReplication(task)
		case task := <-rm.replicationQueue:
			if task == nil {
				return
			}
			log.Printf("REPLICATION: Worker %d processing task %v", workerID, task.ChunkHandle)
			rm.processReplication(task)
		}
	}
}

func (rm *ReplicationManager) processReplication(task *ReplicationTask) {
	rm.mu.Lock()
	rm.activeReplications[task.ChunkHandle] = &ReplicationStatus{
		ChunkHandle:      task.ChunkHandle,
		StartTime:        time.Now(),
		Priority:         task.Priority,
		StreamsActive:    0,
		BytesTransferred: 0,
		TotalBytes:       0,
	}
	rm.mu.Unlock()

	defer func() {
		rm.mu.Lock()
		delete(rm.activeReplications, task.ChunkHandle)
		rm.mu.Unlock()
	}()

	log.Printf("REPLICATION: Processing chunk %v (priority: %d)", task.ChunkHandle, task.Priority)

	// Get current chunk metadata
	rm.masterService.metadataLock.RLock()
	chunkMeta, exists := rm.masterService.master.ChunkMetadata[task.ChunkHandle]
	if !exists {
		rm.masterService.metadataLock.RUnlock()
		return
	}

	activeServers := rm.getActiveServers()
	activeReplicas := rm.countActiveReplicas(chunkMeta.Locations, activeServers)
	rm.masterService.metadataLock.RUnlock()

	if activeReplicas >= rm.replicationFactor {
		log.Printf("REPLICATION: Chunk %v already has enough replicas (%d), skipping",
			task.ChunkHandle, activeReplicas)
		return
	}

	sourceServer := rm.selectOptimalSourceServer(chunkMeta.Locations, activeServers)
	targetServer := rm.selectOptimalTargetServer(chunkMeta.Locations, activeServers)

	if sourceServer == "" || targetServer == "" {
		log.Printf("REPLICATION: Cannot find suitable servers for chunk %v", task.ChunkHandle)
		return
	}

	rm.mu.Lock()
	if status, exists := rm.activeReplications[task.ChunkHandle]; exists {
		status.SourceServer = sourceServer
		status.TargetServer = targetServer
	}
	rm.mu.Unlock()

	if rm.replicateChunkStreaming(task.ChunkHandle, sourceServer, targetServer, chunkMeta.Version) {
		rm.updateChunkLocation(task.ChunkHandle, targetServer)
		log.Printf("REPLICATION: Successfully replicated chunk %v from %s to %s",
			task.ChunkHandle, sourceServer, targetServer)
	} else {
		log.Printf("REPLICATION: Failed to replicate chunk %v from %s to %s",
			task.ChunkHandle, sourceServer, targetServer)
	}
}

func (rm *ReplicationManager) selectOptimalSourceServer(locations []string, activeServers map[string]bool) string {
	// For now, use the first available server (can be enhanced with load balancing)
	for _, location := range locations {
		if activeServers[location] {
			return location
		}
	}
	return ""
}

func (rm *ReplicationManager) selectOptimalTargetServer(locations []string, activeServers map[string]bool) string {
	existing := make(map[string]bool)
	for _, location := range locations {
		existing[location] = true
	}

	// For now, use the first available server (can be enhanced with load balancing)
	for serverAddr := range activeServers {
		if !existing[serverAddr] {
			return serverAddr
		}
	}
	return ""
}

func (rm *ReplicationManager) replicateChunkStreaming(chunkHandle common.ChunkHandle, sourceServer, targetServer string, version uint16) bool {
	if rm.masterService.chunkServerClientManager == nil {
		log.Printf("REPLICATION: No client manager available")
		return false
	}

	targetConn, err := rm.masterService.chunkServerClientManager.GetConnection(targetServer)
	if err != nil {
		log.Printf("REPLICATION: Failed to connect to target %s: %v", targetServer, err)
		return false
	}

	targetClient := chunkserver.NewChunkServerServiceClient(targetConn)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	req := &chunkserver.ReplicateChunkRequest{
		ChunkHandle:   chunkHandle.String(),
		SourceServer:  sourceServer,
		TargetVersion: uint32(version),
	}

	start := time.Now()
	_, err = targetClient.ReplicateChunk(ctx, req)
	duration := time.Since(start)

	if err == nil {
		log.Printf("REPLICATION: Chunk %v replicated in %v", chunkHandle, duration)

		rm.mu.Lock()
		if status, exists := rm.activeReplications[chunkHandle]; exists {
			status.BytesTransferred = 64 * 1024 * 1024
			status.TotalBytes = 64 * 1024 * 1024
		}
		rm.mu.Unlock()

		return true
	}

	log.Printf("REPLICATION: Standard replication failed for chunk %v: %v", chunkHandle, err)
	return false
}

func (rm *ReplicationManager) updateChunkLocation(chunkHandle common.ChunkHandle, newServer string) {
	rm.masterService.metadataLock.Lock()
	defer rm.masterService.metadataLock.Unlock()

	if chunkMeta, exists := rm.masterService.master.ChunkMetadata[chunkHandle]; exists {
		for _, location := range chunkMeta.Locations {
			if location == newServer {
				return
			}
		}

		chunkMeta.Locations = append(chunkMeta.Locations, newServer)
		log.Printf("REPLICATION: Added %s to chunk %v locations (total: %d)",
			newServer, chunkHandle, len(chunkMeta.Locations))
	}
}

func (rm *ReplicationManager) replicationMonitor() {
	defer rm.wg.Done()

	ticker := time.NewTicker(rm.monitorInterval)
	defer ticker.Stop()

	log.Printf("REPLICATION: Monitor started (interval: %v)", rm.monitorInterval)

	for {
		select {
		case <-rm.ctx.Done():
			log.Printf("REPLICATION: Monitor stopping")
			return
		case <-ticker.C:
			if rm.IsRunning() {
				rm.CheckReplicationNeeds()

				// Also check for stale replications that might be stuck
				rm.cleanupStaleReplications()
			}
		}
	}
}

func (rm *ReplicationManager) batchReplicationCoordinator() {
	defer rm.wg.Done()

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	log.Printf("REPLICATION: Batch coordinator started (checking every 2 seconds)")

	for {
		select {
		case <-rm.ctx.Done():
			log.Printf("REPLICATION: Batch coordinator stopping")
			return
		case <-ticker.C:
			rm.processBatchOpportunities()
			rm.processQueueStatus()
		}
	}
}

func (rm *ReplicationManager) processBatchOpportunities() {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	for serverAddr, batchJob := range rm.batchReplications {
		if time.Since(batchJob.StartTime) > rm.batchTimeout {
			log.Printf("REPLICATION: Batch job for %s timed out, cleaning up", serverAddr)
			delete(rm.batchReplications, serverAddr)
		}
	}
}

func (rm *ReplicationManager) processQueueStatus() {
	normalQueueLen := len(rm.replicationQueue)
	priorityQueueLen := len(rm.priorityQueue)

	if normalQueueLen > 800 {
		log.Printf("REPLICATION: WARNING - Normal queue is %d%% full (%d/1000)",
			(normalQueueLen*100)/1000, normalQueueLen)
	}
	if priorityQueueLen > 400 {
		log.Printf("REPLICATION: WARNING - Priority queue is %d%% full (%d/500)",
			(priorityQueueLen*100)/500, priorityQueueLen)
	}
}

func (rm *ReplicationManager) cleanupStaleReplications() {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	maxReplicationTime := 15 * time.Minute // Max time for a single replication
	now := time.Now()
	staleCount := 0

	for chunkHandle, status := range rm.activeReplications {
		if now.Sub(status.StartTime) > maxReplicationTime {
			log.Printf("REPLICATION: Cleaning up stale replication for chunk %v (running for %v)",
				chunkHandle, now.Sub(status.StartTime))
			delete(rm.activeReplications, chunkHandle)
			staleCount++

			go func(ch common.ChunkHandle) {
				rm.QueueReplication(ch, PriorityHigh)
			}(chunkHandle)
		}
	}

	if staleCount > 0 {
		log.Printf("REPLICATION: Cleaned up %d stale replications", staleCount)
	}
}

// ServerLoadInfo represents load and capacity information for a chunk server (kept for compatibility).
type ServerLoadInfo struct {
	Address         string
	TotalSpace      uint64
	UsedSpace       uint64
	CPUUsage        float64
	MemoryUsage     float64
	ReplicationLoad int
	LastUpdate      time.Time
}
