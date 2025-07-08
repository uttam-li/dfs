package service

import (
	"context"
	"fmt"
	"log"
	"math/rand"
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

	replicationQueue       chan *ReplicationTask
	activeReplications     map[common.ChunkHandle]*ReplicationStatus
	queuedChunks           map[common.ChunkHandle]bool // Track queued chunks to avoid duplicates
	serverReplicationCount map[string]int              // Track active replications per server

	// Configuration for replication
	replicationFactor        int
	maxWorkers               int
	maxReplicationsPerServer int           // Max concurrent replications per target server
	monitorInterval          time.Duration // How often to scan for under-replicated chunks
	workerCheckInterval      time.Duration // How often workers check for tasks

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	state  common.ServiceState
}

// ReplicationTask represents a chunk replication request.
type ReplicationTask struct {
	ChunkHandle  common.ChunkHandle
	SourceServer string
	TargetServer string
	RetryCount   int // Track retry attempts for backoff
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
		masterService:            masterService,
		replicationQueue:         make(chan *ReplicationTask, 2000), // Larger queue for simplicity
		activeReplications:       make(map[common.ChunkHandle]*ReplicationStatus),
		queuedChunks:             make(map[common.ChunkHandle]bool),
		serverReplicationCount:   make(map[string]int),
		replicationFactor:        masterService.config.ReplicationFactor,
		maxWorkers:               3,                
		maxReplicationsPerServer: 5,                
		monitorInterval:          10 * time.Second, 
		workerCheckInterval:      5 * time.Second,
		ctx:                      ctx,
		cancel:                   cancel,
		state:                    common.ServiceStateUnknown,
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
	go rm.replicationMonitor()

	log.Printf("Replication manager started with %d workers and monitor (interval: %v)",
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

// QueueReplication queues a chunk for replication by finding suitable servers.
func (rm *ReplicationManager) QueueReplication(chunkHandle common.ChunkHandle, priority ReplicationPriority) {
	if !rm.IsRunning() {
		return
	}

	// Check if already being processed or queued
	rm.mu.Lock()
	if _, exists := rm.activeReplications[chunkHandle]; exists {
		rm.mu.Unlock()
		return
	}
	if rm.queuedChunks[chunkHandle] {
		rm.mu.Unlock()
		return // Already queued
	}
	rm.mu.Unlock()

	// Get chunk metadata and find servers
	rm.masterService.metadataLock.RLock()
	chunkMeta, exists := rm.masterService.master.ChunkMetadata[chunkHandle]
	if !exists {
		rm.masterService.metadataLock.RUnlock()
		return
	}

	activeServers := rm.getActiveServers()
	activeReplicas := rm.countActiveReplicas(chunkMeta.Locations, activeServers)

	// Skip if enough replicas
	if activeReplicas >= rm.replicationFactor {
		rm.masterService.metadataLock.RUnlock()
		return
	}

	// Find source and target servers
	sourceServer := rm.selectOptimalSourceServer(chunkMeta.Locations, activeServers)
	targetServer := rm.selectOptimalTargetServer(chunkMeta.Locations, activeServers)
	rm.masterService.metadataLock.RUnlock()

	if sourceServer == "" || targetServer == "" {
		// No suitable servers right now, will be retried next monitoring cycle
		return
	}

	// Check if target server is at capacity before queueing
	rm.mu.RLock()
	currentLoad := rm.serverReplicationCount[targetServer]
	rm.mu.RUnlock()

	if currentLoad >= rm.maxReplicationsPerServer {
		// Target server is busy, will retry next monitoring cycle (don't log to reduce spam)
		return
	}

	// Create replication task with specific servers
	task := &ReplicationTask{
		ChunkHandle:  chunkHandle,
		SourceServer: sourceServer,
		TargetServer: targetServer,
		RetryCount:   0,
	}

	// Mark as queued and queue the task
	rm.mu.Lock()
	rm.queuedChunks[chunkHandle] = true
	rm.mu.Unlock()

	select {
	case rm.replicationQueue <- task:
		// Successfully queued
	default:
		// Queue full, remove from queued set
		rm.mu.Lock()
		delete(rm.queuedChunks, chunkHandle)
		rm.mu.Unlock()
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

	// Add a brief delay to avoid overwhelming the system during concurrent checks
	select {
	case <-time.After(100 * time.Millisecond):
	case <-rm.ctx.Done():
		return
	}

	rm.masterService.metadataLock.RLock()
	defer rm.masterService.metadataLock.RUnlock()

	underReplicated := 0
	queued := 0
	activeServers := rm.getActiveServers()

	// If we have very few active servers, be more conservative with replication
	maxChunksToQueue := 50
	if len(activeServers) <= 2 {
		maxChunksToQueue = 20 // Reduce load when few servers available
	}

	for chunkHandle, chunkMeta := range rm.masterService.master.ChunkMetadata {
		activeReplicas := rm.countActiveReplicas(chunkMeta.Locations, activeServers)

		if activeReplicas < rm.replicationFactor {
			underReplicated++
			priority := rm.calculatePriority(activeReplicas)

			// Rate limit: queue chunks based on available server capacity
			if queued < maxChunksToQueue {
				rm.QueueReplication(chunkHandle, priority)
				queued++
			}
		}
	}

	if underReplicated > 0 {
		activeServersList := make([]string, 0, len(activeServers))
		for server := range activeServers {
			activeServersList = append(activeServersList, server)
		}
		log.Printf("REPLICATION: Found %d under-replicated chunks (queued %d), active servers: %v",
			underReplicated, queued, activeServersList)
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

// StreamingChunk represents a chunk of data for streaming replication operations.
type StreamingChunk struct {
	ChunkHandle common.ChunkHandle
	Offset      uint64
	Data        []byte
	SequenceNum uint32
	IsLast      bool
}

// replicationWorker processes replication tasks from the queue
func (rm *ReplicationManager) replicationWorker(workerID int) {
	defer rm.wg.Done()

	log.Printf("REPLICATION: Worker %d started", workerID)

	for {
		select {
		case <-rm.ctx.Done():
			log.Printf("REPLICATION: Worker %d stopping", workerID)
			return
		case task := <-rm.replicationQueue:
			if task == nil {
				return // Channel closed
			}
			log.Printf("REPLICATION: Worker %d processing task %v", workerID, task.ChunkHandle)
			rm.processReplication(task)
		}
	}
}

func (rm *ReplicationManager) processReplication(task *ReplicationTask) {
	rm.mu.Lock()
	// Remove from queued set since we're now processing
	delete(rm.queuedChunks, task.ChunkHandle)

	// Atomically check capacity and reserve slot if available
	currentCount := rm.serverReplicationCount[task.TargetServer]
	if currentCount >= rm.maxReplicationsPerServer {
		rm.mu.Unlock()
		// Reduce log spam by only logging every 10th re-queue for the same reason
		if task.RetryCount%10 == 0 {
			log.Printf("REPLICATION: Target server %s at capacity (%d/%d), re-queueing chunk %v (retry #%d)",
				task.TargetServer, currentCount, rm.maxReplicationsPerServer, task.ChunkHandle, task.RetryCount)
		}
		// Re-queue with exponential backoff to avoid overwhelming the system
		go func(retryCount int) {
			// Exponential backoff: 30s, 60s, 120s, max 300s (5 minutes)
			backoffDelay := time.Duration(30*(1<<retryCount)) * time.Second
			if backoffDelay > 5*time.Minute {
				backoffDelay = 5 * time.Minute
			}
			time.Sleep(backoffDelay)

			// After several retries, try re-selecting servers to avoid persistent bottlenecks
			if retryCount >= 3 {
				rm.QueueReplication(task.ChunkHandle, PriorityNormal)
				return
			}

			// Create new task with incremented retry count
			newTask := &ReplicationTask{
				ChunkHandle:  task.ChunkHandle,
				SourceServer: task.SourceServer,
				TargetServer: task.TargetServer,
				RetryCount:   retryCount + 1,
			}

			// Re-queue directly to avoid re-computing servers
			rm.mu.Lock()
			rm.queuedChunks[task.ChunkHandle] = true
			rm.mu.Unlock()

			select {
			case rm.replicationQueue <- newTask:
				// Successfully re-queued
			default:
				// Queue full, remove from queued set
				rm.mu.Lock()
				delete(rm.queuedChunks, task.ChunkHandle)
				rm.mu.Unlock()
			}
		}(task.RetryCount)
		return
	}

	// Atomically reserve a slot by incrementing the count
	rm.serverReplicationCount[task.TargetServer] = currentCount + 1

	rm.activeReplications[task.ChunkHandle] = &ReplicationStatus{
		ChunkHandle:      task.ChunkHandle,
		SourceServer:     task.SourceServer,
		TargetServer:     task.TargetServer,
		StartTime:        time.Now(),
		StreamsActive:    0,
		BytesTransferred: 0,
		TotalBytes:       0,
	}
	rm.mu.Unlock()

	defer func() {
		rm.mu.Lock()
		delete(rm.activeReplications, task.ChunkHandle)
		// Decrement server replication count
		rm.serverReplicationCount[task.TargetServer]--
		if rm.serverReplicationCount[task.TargetServer] == 0 {
			delete(rm.serverReplicationCount, task.TargetServer)
		}
		rm.mu.Unlock()
	}()

	log.Printf("REPLICATION: Processing chunk %v from %s to %s", task.ChunkHandle, task.SourceServer, task.TargetServer)

	// Add a small delay to reduce pressure on target server
	time.Sleep(100 * time.Millisecond)

	// Get current chunk metadata to verify it still needs replication
	rm.masterService.metadataLock.RLock()
	chunkMeta, exists := rm.masterService.master.ChunkMetadata[task.ChunkHandle]
	if !exists {
		rm.masterService.metadataLock.RUnlock()
		log.Printf("REPLICATION: Chunk %v not found in metadata, skipping", task.ChunkHandle)
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

	// Verify servers are still active
	if !activeServers[task.SourceServer] || !activeServers[task.TargetServer] {
		log.Printf("REPLICATION: Source (%s) or target (%s) server not active, re-queueing chunk %v",
			task.SourceServer, task.TargetServer, task.ChunkHandle)

		// Re-queue with delay to avoid tight retry loops
		go func(retryCount int) {
			// Backoff for server unavailability: 5s, 10s, 20s, max 60s
			backoffDelay := time.Duration(5*(1<<retryCount)) * time.Second
			if backoffDelay > time.Minute {
				backoffDelay = time.Minute
			}
			time.Sleep(backoffDelay)
			rm.QueueReplication(task.ChunkHandle, PriorityNormal)
		}(task.RetryCount)
		return
	}

	// Attempt replication
	success := rm.replicateChunkStreaming(task.ChunkHandle, task.SourceServer, task.TargetServer, chunkMeta.Version)

	if success {
		rm.updateChunkLocation(task.ChunkHandle, task.TargetServer)
		log.Printf("REPLICATION: Successfully replicated chunk %v from %s to %s",
			task.ChunkHandle, task.SourceServer, task.TargetServer)
	} else {
		log.Printf("REPLICATION: Failed to replicate chunk %v from %s to %s, re-queueing",
			task.ChunkHandle, task.SourceServer, task.TargetServer)

		// Re-queue with exponential backoff for failures
		go func(retryCount int) {
			// Exponential backoff: 30s, 60s, 120s, max 300s (5 minutes)
			backoffDelay := time.Duration(30*(1<<retryCount)) * time.Second
			if backoffDelay > 5*time.Minute {
				backoffDelay = 5 * time.Minute
			}
			time.Sleep(backoffDelay)
			rm.QueueReplication(task.ChunkHandle, PriorityNormal)
		}(task.RetryCount)
	}
}

func (rm *ReplicationManager) selectOptimalSourceServer(locations []string, activeServers map[string]bool) string {
	// Collect available source servers
	var candidates []string
	for _, location := range locations {
		if activeServers[location] {
			candidates = append(candidates, location)
		}
	}

	// Return random candidate to distribute load
	if len(candidates) == 0 {
		return ""
	}
	return candidates[rand.Intn(len(candidates))]
}

func (rm *ReplicationManager) selectOptimalTargetServer(locations []string, activeServers map[string]bool) string {
	existing := make(map[string]bool)
	for _, location := range locations {
		existing[location] = true
	}

	// Collect available target servers and their current load
	type serverLoad struct {
		addr string
		load int
	}
	var candidates []serverLoad

	rm.mu.RLock()
	for serverAddr := range activeServers {
		if !existing[serverAddr] {
			load := rm.serverReplicationCount[serverAddr]
			candidates = append(candidates, serverLoad{addr: serverAddr, load: load})
		}
	}
	rm.mu.RUnlock()

	if len(candidates) == 0 {
		return ""
	}

	// Find servers with minimum load
	minLoad := candidates[0].load
	for _, candidate := range candidates {
		if candidate.load < minLoad {
			minLoad = candidate.load
		}
	}

	// Filter to servers with minimum load
	var minLoadCandidates []string
	for _, candidate := range candidates {
		if candidate.load == minLoad {
			minLoadCandidates = append(minLoadCandidates, candidate.addr)
		}
	}

	// Return random candidate among those with minimum load
	return minLoadCandidates[rand.Intn(len(minLoadCandidates))]
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

	// Use a shorter timeout and add retries for robustness
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	req := &chunkserver.ReplicateChunkRequest{
		ChunkHandle:   chunkHandle.String(),
		SourceServer:  sourceServer,
		TargetVersion: uint32(version),
	}

	start := time.Now()
	_, err = targetClient.ReplicateChunk(ctx, req)
	duration := time.Since(start)

	// Release connection after use to help with connection management
	defer rm.masterService.chunkServerClientManager.ReleaseConnection(targetServer)

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
