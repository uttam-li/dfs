package service

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/uttam-li/dfs/pkg/common"
)

type HeartbeatManager struct {
	mu            sync.RWMutex
	masterService *MasterService

	// Server tracking
	serverHeartbeats map[uuid.UUID]*ServerHeartbeat

	// Configuration
	heartbeatTimeout time.Duration
	checkInterval    time.Duration

	// Service lifecycle
	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	running bool
}

// ServerHeartbeat tracks basic heartbeat information
type ServerHeartbeat struct {
	ServerID   uuid.UUID
	Address    string
	LastSeen   time.Time
	ChunkCount int
	IsAlive    bool
}

// NewHeartbeatManager creates a simple heartbeat manager
func NewHeartbeatManager(timeout time.Duration, replicationFactor int) *HeartbeatManager {
	// Check interval should be much more frequent than timeout for quick detection
	checkInterval := max(timeout/6, 5*time.Second)

	return &HeartbeatManager{
		serverHeartbeats: make(map[uuid.UUID]*ServerHeartbeat),
		heartbeatTimeout: timeout,
		checkInterval:    checkInterval,
	}
}

// SetMasterService sets the master service reference
func (hm *HeartbeatManager) SetMasterService(ms *MasterService) {
	hm.masterService = ms
}

// Start starts the heartbeat monitor
func (hm *HeartbeatManager) Start(ctx context.Context) error {
	hm.mu.Lock()
	defer hm.mu.Unlock()

	if hm.running {
		return nil
	}

	hm.ctx, hm.cancel = context.WithCancel(ctx)
	hm.running = true

	// Start monitoring goroutine
	hm.wg.Add(1)
	go hm.monitor()

	log.Printf("Heartbeat manager started (timeout: %v, check interval: %v)", hm.heartbeatTimeout, hm.checkInterval)
	return nil
}

// Stop stops the heartbeat manager
func (hm *HeartbeatManager) Stop() error {
	hm.mu.Lock()
	defer hm.mu.Unlock()

	if !hm.running {
		return nil
	}

	hm.running = false
	if hm.cancel != nil {
		hm.cancel()
	}

	hm.wg.Wait()
	log.Printf("Heartbeat manager stopped")
	return nil
}

// RecordHeartbeat records a heartbeat from a chunkserver and logs processing time
func (hm *HeartbeatManager) RecordHeartbeat(serverID uuid.UUID, address string, chunkCount int, processingStart time.Time) {
	hm.mu.Lock()
	defer hm.mu.Unlock()

	now := time.Now()
	processingTime := now.Sub(processingStart)

	// Update or create heartbeat record
	if hb, exists := hm.serverHeartbeats[serverID]; exists {
		wasAlive := hb.IsAlive
		hb.LastSeen = now
		hb.ChunkCount = chunkCount
		hb.IsAlive = true

		if !wasAlive {
			log.Printf("HEARTBEAT: Server %s (%v) recovered - reported %d chunks",
				address, serverID, chunkCount)
		}
	} else {
		hm.serverHeartbeats[serverID] = &ServerHeartbeat{
			ServerID:   serverID,
			Address:    address,
			LastSeen:   now,
			ChunkCount: chunkCount,
			IsAlive:    true,
		}
		log.Printf("HEARTBEAT: New server %s (%v) registered - reported %d chunks",
			address, serverID, chunkCount)
	}

	log.Printf("HEARTBEAT: Server %s (%v) - %d chunks, processed in %v",
		address, serverID, chunkCount, processingTime)
}

// ProcessChunkList compares received chunk list with master metadata and notifies GC service
func (hm *HeartbeatManager) ProcessChunkList(serverAddr string, reportedChunks []string) (orphanedChunks []string) {
	if hm.masterService == nil {
		return nil
	}

	hm.masterService.metadataLock.RLock()
	defer hm.masterService.metadataLock.RUnlock()

	for _, chunkHandleStr := range reportedChunks {
		chunkHandle, err := common.ParseChunkHandle(chunkHandleStr)
		if err != nil {
			log.Printf("HEARTBEAT: Invalid chunk handle from server %s: %s", serverAddr, chunkHandleStr)
			continue
		}

		// Check if chunk exists in master metadata
		if _, exists := hm.masterService.master.ChunkMetadata[chunkHandle]; !exists {
			orphanedChunks = append(orphanedChunks, chunkHandleStr)
			log.Printf("HEARTBEAT: Orphaned chunk detected - server %s has chunk %s not in master metadata",
				serverAddr, chunkHandleStr)

			// Notify GC service to clean up orphaned chunk
			if hm.masterService.gcManager != nil {
				go hm.masterService.gcManager.MarkChunkUnreferenced(chunkHandle, 0, []string{serverAddr})
			}
		}
	}

	if len(orphanedChunks) > 0 {
		log.Printf("HEARTBEAT: Found %d orphaned chunks on server %s", len(orphanedChunks), serverAddr)
	}

	return orphanedChunks
}

// monitor periodically checks for missed heartbeats
func (hm *HeartbeatManager) monitor() {
	defer hm.wg.Done()

	ticker := time.NewTicker(hm.checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-hm.ctx.Done():
			return
		case <-ticker.C:
			hm.checkMissedHeartbeats()
		}
	}
}

// checkMissedHeartbeats detects servers that missed heartbeats and handles failures
func (hm *HeartbeatManager) checkMissedHeartbeats() {
	hm.mu.Lock()
	now := time.Now()
	var failedServers []uuid.UUID

	for serverID, hb := range hm.serverHeartbeats {
		timeSinceLastSeen := now.Sub(hb.LastSeen)

		if hb.IsAlive && timeSinceLastSeen > hm.heartbeatTimeout {
			hb.IsAlive = false
			failedServers = append(failedServers, serverID)
			log.Printf("HEARTBEAT: Server %s (%v) missed heartbeat - marking as failed (last seen: %v, timeout: %v, elapsed: %v)",
				hb.Address, serverID, hb.LastSeen.Format("15:04:05"), hm.heartbeatTimeout, timeSinceLastSeen)
		} else if hb.IsAlive && timeSinceLastSeen > hm.heartbeatTimeout/2 {
			log.Printf("HEARTBEAT: Server %s (%v) approaching timeout (last seen: %v, elapsed: %v)",
				hb.Address, serverID, hb.LastSeen.Format("15:04:05"), timeSinceLastSeen)
		}
	}
	hm.mu.Unlock()

	// Handle failed servers
	for _, serverID := range failedServers {
		hm.handleServerFailure(serverID)
	}

	if len(failedServers) > 0 {
		// Trigger comprehensive stale server cleanup in replication manager
		if hm.masterService.replicationManager != nil {
			go hm.masterService.replicationManager.ForceStaleServerCleanup()
		}
	}
}

// handleServerFailure handles a server that has failed
func (hm *HeartbeatManager) handleServerFailure(serverID uuid.UUID) {
	if hm.masterService == nil {
		return
	}

	log.Printf("HEARTBEAT: Handling server failure: %v", serverID)

	// Get server info before removal
	hm.masterService.chunkServerLock.Lock()
	serverInfo, exists := hm.masterService.chunkServers[serverID]
	if !exists {
		hm.masterService.chunkServerLock.Unlock()
		return
	}

	serverAddress := serverInfo.Address
	// Remove from master's server list
	delete(hm.masterService.chunkServers, serverID)
	hm.masterService.chunkServerLock.Unlock()

	log.Printf("HEARTBEAT: Removed failed server %s (%v)", serverAddress, serverID)

	// Update chunk metadata to remove this server from all chunk locations
	removedChunkCount := hm.removeServerFromChunkMetadata(serverAddress)

	log.Printf("HEARTBEAT: Removed failed server %s from %d chunk locations",
		serverAddress, removedChunkCount)

	// Notify replication manager about server failure
	if hm.masterService.replicationManager != nil {
		hm.masterService.replicationManager.HandleServerFailure(serverAddress)
		go hm.masterService.replicationManager.CheckReplicationNeeds()
	}

	// Revoke leases held by the failed server
	if hm.masterService.leaseManager != nil {
		revokedCount := hm.masterService.RevokeChunkLeasesForServer(serverAddress, "server failure")
		if revokedCount > 0 {
			log.Printf("HEARTBEAT: Revoked %d leases for failed server %s", revokedCount, serverAddress)
		}
	}
}

// removeServerFromChunkMetadata removes the failed server from all chunk location metadata
func (hm *HeartbeatManager) removeServerFromChunkMetadata(failedServerAddr string) int {
	hm.masterService.metadataLock.Lock()
	defer hm.masterService.metadataLock.Unlock()

	removedCount := 0
	for chunkHandle, chunkMeta := range hm.masterService.master.ChunkMetadata {
		// Remove failed server from locations
		var newLocations []string
		serverFound := false
		for _, location := range chunkMeta.Locations {
			if location == failedServerAddr {
				serverFound = true
				removedCount++
			} else {
				newLocations = append(newLocations, location)
			}
		}

		if serverFound {
			chunkMeta.Locations = newLocations
			log.Printf("HEARTBEAT: Updated chunk %v locations: removed %s (now has %d replicas)",
				chunkHandle, failedServerAddr, len(newLocations))

			// Log critical chunks with no replicas
			if len(newLocations) == 0 {
				log.Printf("HEARTBEAT: CRITICAL - Chunk %v has NO replicas left!", chunkHandle)
			}
		}
	}

	return removedCount
}

// DetectOrphanedChunks detects chunks present on chunkserver but not in master metadata
// This method is called by the heartbeat service during chunk list processing
func (hm *HeartbeatManager) DetectOrphanedChunks(serverAddr string, reportedChunks []string) []string {
	return hm.ProcessChunkList(serverAddr, reportedChunks)
}

// GetAliveServers returns list of currently alive servers
func (hm *HeartbeatManager) GetAliveServers() []uuid.UUID {
	hm.mu.RLock()
	defer hm.mu.RUnlock()

	var aliveServers []uuid.UUID
	for serverID, hb := range hm.serverHeartbeats {
		if hb.IsAlive {
			aliveServers = append(aliveServers, serverID)
		}
	}
	return aliveServers
}

// GetServerStats returns basic heartbeat statistics for monitoring
func (hm *HeartbeatManager) GetServerStats() (alive, dead int) {
	hm.mu.RLock()
	defer hm.mu.RUnlock()

	for _, hb := range hm.serverHeartbeats {
		if hb.IsAlive {
			alive++
		} else {
			dead++
		}
	}
	return alive, dead
}

// IsRunning returns true if heartbeat manager is running
func (hm *HeartbeatManager) IsRunning() bool {
	hm.mu.RLock()
	defer hm.mu.RUnlock()
	return hm.running
}
