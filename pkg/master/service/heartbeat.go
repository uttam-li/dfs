package service

import (
	"fmt"
	"log"
	"slices"
	"time"

	"github.com/google/uuid"
	"github.com/uttam-li/dfs/pkg/common"
)

// RegisterChunkServer registers a new chunk server with the master and handles chunk reconciliation
func (ms *MasterService) RegisterChunkServer(serverID, address string, totalSpace uint64, existingChunks []string) (int64, []string, string, error) {
	// Check state
	if ms.GetState() != common.ServiceStateRunning {
		return 0, nil, "", fmt.Errorf("master service is not ready yet (state: %v), please retry", ms.GetState())
	}

	// Parse or generate server ID
	serverUUID, err := uuid.Parse(serverID)
	if err != nil {
		serverUUID = uuid.New()
		log.Printf("Generated new server ID: %v for address: %s", serverUUID, address)
	}

	// Convert existing chunks to proper format and analyze them
	validChunks := 0
	invalidChunks := 0
	chunksToDelete := make([]string, 0)

	for _, chunkStr := range existingChunks {
		if chunkHandle, err := common.ParseChunkHandle(chunkStr); err == nil {
			// Check if this chunk should exist in metadata
			ms.metadataLock.RLock()
			if chunkMeta, exists := ms.master.ChunkMetadata[chunkHandle]; exists {
				// Check if this server should have this chunk
				shouldHave := slices.Contains(chunkMeta.Locations, address)

				if !shouldHave {
					// Server has chunk it shouldn't have
					log.Printf("Server %s has unexpected chunk %v during registration", address, chunkHandle)
					chunksToDelete = append(chunksToDelete, chunkStr)
					invalidChunks++
				}
			} else {
				// Chunk doesn't exist in metadata - orphaned
				log.Printf("Server %s has orphaned chunk %v during registration", address, chunkHandle)
				chunksToDelete = append(chunksToDelete, chunkStr)
				invalidChunks++

				// Mark for GC
				if ms.gcManager != nil {
					go ms.gcManager.MarkChunkUnreferenced(chunkHandle, 0, []string{address})
				}
			}
			ms.metadataLock.RUnlock()
		}
	}

	// Create server info
	serverInfo := &ChunkServerInfo{
		Address:    address,
		LastSeen:   time.Now(),
		TotalSpace: totalSpace,
		UsedSpace:  0,
	}

	// Register server
	ms.chunkServerLock.Lock()
	ms.chunkServers[serverUUID] = serverInfo
	ms.chunkServerLock.Unlock()

	// Record with heartbeat manager
	ms.heartbeatManager.RecordHeartbeat(serverUUID, address, len(existingChunks), time.Now())

	// Update replication manager with this new server
	if ms.replicationManager != nil && ms.replicationManager.IsRunning() {
		ms.replicationManager.UpdateServerLoad(address, totalSpace, 0, 0, 0, 0)

		// // Trigger replication check to see if this new server can help with under-replicated chunks
		// go ms.replicationManager.CheckReplicationNeeds()
	}

	// Return registration response
	heartbeatInterval := int64(ms.config.HeartbeatTimeout.Seconds() / 3)

	log.Printf("Registered chunk server %v at %s: %d total chunks, %d valid, %d invalid/orphaned",
		serverUUID, address, len(existingChunks), validChunks, invalidChunks)

	return heartbeatInterval, chunksToDelete, serverUUID.String(), nil
}

// SendHeartbeat processes a heartbeat from a chunk server with clean responsibility separation
func (ms *MasterService) SendHeartbeat(serverID string, totalSpace, usedSpace, availableSpace uint64, pendingDeletes, activeConnections uint32, cpuUsage, memoryUsage float64, chunkReports []ChunkReportInfo) ([]string, []ReplicationOrder, int64, error) {
	start := time.Now()

	// Parse server ID
	serverUUID, err := uuid.Parse(serverID)
	if err != nil {
		return nil, nil, 0, fmt.Errorf("invalid server ID: %s", serverID)
	}

	// Update server info quickly
	ms.chunkServerLock.Lock()
	serverInfo, exists := ms.chunkServers[serverUUID]
	if !exists {
		ms.chunkServerLock.Unlock()
		return nil, nil, 0, fmt.Errorf("chunk server not registered: %s", serverID)
	}

	// Update basic server stats
	serverInfo.LastSeen = time.Now()
	serverInfo.TotalSpace = totalSpace
	serverInfo.UsedSpace = usedSpace
	ms.chunkServerLock.Unlock()

	// Extract chunk handles from reports for heartbeat processing
	reportedChunkHandles := make([]string, 0, len(chunkReports))
	for _, report := range chunkReports {
		reportedChunkHandles = append(reportedChunkHandles, report.ChunkHandle)
	} // Record heartbeat and process chunk list (heartbeat service responsibility)
	orphanedChunks := ms.heartbeatManager.ProcessChunkList(serverInfo.Address, reportedChunkHandles)

	// Instead of immediately asking chunkserver to delete chunks,
	// queue them for GC processing to avoid blocking heartbeat
	var chunksToDelete []string
	if len(orphanedChunks) > 0 {
		log.Printf("HEARTBEAT: Found %d orphaned chunks on server %s, queuing for GC processing",
			len(orphanedChunks), serverInfo.Address)

		// Queue orphaned chunks for async deletion via GC manager
		if ms.gcManager != nil {
			for _, chunkHandleStr := range orphanedChunks {
				if chunkHandle, err := common.ParseChunkHandle(chunkHandleStr); err == nil {
					go ms.gcManager.MarkChunkUnreferenced(chunkHandle, 0, []string{serverInfo.Address})
				}
			}
		}

		// For now, still return the list to chunkserver for immediate cleanup
		// This provides dual protection: immediate cleanup + GC-based cleanup
		chunksToDelete = orphanedChunks
	}

	// Record heartbeat with processing time
	ms.heartbeatManager.RecordHeartbeat(serverUUID, serverInfo.Address, len(chunkReports), start)

	// Update replication manager with server load information
	if ms.replicationManager != nil && ms.replicationManager.IsRunning() {
		ms.replicationManager.UpdateServerLoad(
			serverInfo.Address,
			totalSpace,
			usedSpace,
			cpuUsage,
			memoryUsage,
			int(activeConnections),
		)
	}

	// Get replication orders from replication manager
	replicationOrders := ms.getReplicationOrders(serverInfo.Address)

	// Return response
	nextHeartbeatInterval := int64(ms.config.HeartbeatTimeout.Seconds() / 3)

	return chunksToDelete, replicationOrders, nextHeartbeatInterval, nil
}

// getReplicationOrders gets replication orders for the requesting server
func (ms *MasterService) getReplicationOrders(serverAddr string) []ReplicationOrder {
	var orders []ReplicationOrder

	// Get pending replication tasks that could use this server as a target
	// This is a simplified implementation - in practice, you'd want more sophisticated logic

	if ms.replicationManager != nil && ms.replicationManager.IsRunning() {
		// Check if this server is suitable for receiving replications
		serverLoad := ms.replicationManager.GetServerLoadInfo()
		if loadInfo, exists := serverLoad[serverAddr]; exists {
			diskUsage := float64(loadInfo.UsedSpace) / float64(loadInfo.TotalSpace)

			// Don't give replication orders to heavily loaded servers
			if diskUsage < 0.85 && loadInfo.ReplicationLoad < 2 {
				// This server can accept replication tasks
				// For now, return empty - the replication manager handles scheduling internally
			}
		}
	}

	return orders
}

// ChunkReportInfo represents information about a chunk from heartbeat
type ChunkReportInfo struct {
	ChunkHandle string
	Version     uint32
	Size        uint64
	Checksum    []byte
}

// ReplicationOrder represents a chunk replication order
type ReplicationOrder struct {
	ChunkHandle   string
	TargetServers []string
	Priority      uint32
}
