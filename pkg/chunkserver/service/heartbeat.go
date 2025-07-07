package service

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/uttam-li/dfs/api/generated/master"
)

// heartbeatRoutine sends periodic heartbeats to master
func (cs *ChunkServerService) heartbeatRoutine() {
	defer cs.wg.Done()

	ticker := time.NewTicker(cs.heartbeatInterval)
	defer ticker.Stop()

	consecutiveFailures := 0
	const maxConsecutiveFailures = 5

	for {
		select {
		case <-cs.ctx.Done():
			return
		case <-ticker.C:
			if err := cs.sendHeartbeat(); err != nil {
				consecutiveFailures++
				log.Printf("Failed to send heartbeat (%d/%d consecutive failures): %v",
					consecutiveFailures, maxConsecutiveFailures, err)

				if consecutiveFailures >= maxConsecutiveFailures {
					log.Printf("Too many consecutive heartbeat failures, attempting to reconnect to master")
					cs.attemptMasterReconnection()
					consecutiveFailures = 0
				}
			} else {
				consecutiveFailures = 0
			}
		}
	}
}

// sendHeartbeat sends a heartbeat to the master
func (cs *ChunkServerService) sendHeartbeat() error {
	startTime := time.Now()

	// Collect server stats
	totalSpace, usedSpace, availableSpace, _, err := cs.GetServerStats()
	if err != nil {
		return fmt.Errorf("failed to get server stats: %w", err)
	}

	// Collect chunk reports (including CPU and memory usage for better monitoring)
	chunkReports := make([]*master.SendHeartbeatRequest_ChunkReport, 0)
	cs.storage.mu.RLock()
	for handle, metadata := range cs.storage.chunks {
		chunkReports = append(chunkReports, &master.SendHeartbeatRequest_ChunkReport{
			ChunkHandle: handle,
			Version:     metadata.Version,
			Size:        metadata.Size,
			Checksum:    metadata.Checksum,
		})
	}
	cs.storage.mu.RUnlock()

	// Create heartbeat request with enhanced stats
	req := &master.SendHeartbeatRequest{
		ServerId: cs.serverID,
		Stats: &master.SendHeartbeatRequest_ServerStats{
			TotalSpace:        totalSpace,
			UsedSpace:         usedSpace,
			AvailableSpace:    availableSpace,
			PendingDeletes:    0,   // TODO: implement pending deletes tracking
			ActiveConnections: 0,   // TODO: track active client connections
			CpuUsage:          0.0, // TODO: implement CPU usage monitoring
			MemoryUsage:       0.0, // TODO: implement memory usage monitoring
		},
		ChunkReports: chunkReports,
	}

	// Send heartbeat with timeout to avoid hanging
	ctx, cancel := context.WithTimeout(cs.ctx, 10*time.Second)
	defer cancel()

	resp, err := cs.masterClient.SendHeartbeat(ctx, req)
	if err != nil {
		return fmt.Errorf("heartbeat failed: %w", err)
	}

	// Process response - queue chunks for deletion asynchronously
	if len(resp.ChunksToDelete) > 0 {
		log.Printf("Master requested deletion of %d chunks", len(resp.ChunksToDelete))
		// Queue deletions asynchronously to avoid blocking heartbeat
		go cs.processChunkDeletions(resp.ChunksToDelete)
	}

	// Process replication orders if any
	if len(resp.ReplicationOrders) > 0 {
		log.Printf("Received %d replication orders from master", len(resp.ReplicationOrders))
		for _, order := range resp.ReplicationOrders {
			log.Printf("Replication order for chunk %s to servers %v (priority: %d)",
				order.ChunkHandle, order.TargetServers, order.Priority)
			// TODO: Implement chunk replication to target servers
		}
	}

	// Update heartbeat interval if master suggests a different interval
	if resp.NextHeartbeatInterval > 0 {
		newInterval := time.Duration(resp.NextHeartbeatInterval) * time.Second
		if newInterval != cs.heartbeatInterval {
			cs.heartbeatInterval = newInterval
			log.Printf("Heartbeat interval updated to: %v as requested by master", cs.heartbeatInterval)
		}
	}

	roundTripTime := time.Since(startTime)
	log.Printf("[HEARTBEAT] Successfully sent heartbeat: chunks_count=%d, round_trip_time=%v",
		len(chunkReports), roundTripTime)

	return nil
}

// processChunkDeletions handles chunk deletions asynchronously to avoid blocking heartbeat
func (cs *ChunkServerService) processChunkDeletions(chunkHandles []string) {
	if len(chunkHandles) == 0 {
		return
	}

	log.Printf("DELETION: Processing %d chunk deletions asynchronously", len(chunkHandles))

	for _, chunkHandle := range chunkHandles {
		// Check if context is cancelled to avoid processing deletions during shutdown
		select {
		case <-cs.ctx.Done():
			log.Printf("DELETION: Stopping chunk deletion processing due to context cancellation")
			return
		default:
		}

		if err := cs.DeleteChunk(chunkHandle); err != nil {
			log.Printf("DELETION: Failed to delete chunk %s as requested by master: %v", chunkHandle, err)
		} else {
			log.Printf("DELETION: Successfully deleted chunk %s as requested by master", chunkHandle)
		}

		// Small delay to avoid overwhelming the system
		time.Sleep(10 * time.Millisecond)
	}

	log.Printf("DELETION: Completed processing %d chunk deletions", len(chunkHandles))
}
