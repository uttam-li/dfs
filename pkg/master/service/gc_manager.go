package service

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/google/btree"
	chunkserver "github.com/uttam-li/dfs/api/generated/chunkserver"
	"github.com/uttam-li/dfs/pkg/common"
	"github.com/uttam-li/dfs/pkg/master/types"
)

// GCManager manages garbage collection of deleted files and unreferenced chunks
type GCManager struct {
	mu     sync.RWMutex
	master *MasterService

	// Configuration
	gcInterval               time.Duration
	orphanedChunkGracePeriod time.Duration
	maxChunksPerScan         int
	throttleDelay            time.Duration

	// Tracking structures
	deletedInodes      map[uint64]*DeletedInode                  // Inodes marked for deletion
	unreferencedChunks map[common.ChunkHandle]*UnreferencedChunk // Chunks to be garbage collected
	scanProgress       *ScanProgress

	// Service state
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	state  common.ServiceState

	// Timers and tickers
	gcTicker *time.Ticker
}

// DeletedInode represents an inode that has been marked for deletion
type DeletedInode struct {
	Ino         uint64
	DeletedAt   time.Time
	Size        uint64
	ChunkCount  int
	IsDirectory bool
}

// UnreferencedChunk represents a chunk that may need to be garbage collected
type UnreferencedChunk struct {
	Handle      common.ChunkHandle
	LastSeen    time.Time
	Size        uint32
	Locations   []string
	MarkedForGC bool
	GCScheduled time.Time
}

// ScanProgress tracks the progress of garbage collection scans
type ScanProgress struct {
	mu            sync.RWMutex
	LastScanStart time.Time
	LastScanEnd   time.Time
	TotalInodes   uint64
	ScannedInodes uint64
	TotalChunks   uint64
	ScannedChunks uint64
	IsScanning    bool
}

// NewGCManager creates a new garbage collection manager
func NewGCManager(masterService *MasterService) *GCManager {
	config := masterService.GetConfig()

	return &GCManager{
		master:                   masterService,
		gcInterval:               config.GCInterval,
		orphanedChunkGracePeriod: config.OrphanedChunkGracePeriod,
		maxChunksPerScan:         1000,                   // Process up to 1000 chunks per scan cycle
		throttleDelay:            100 * time.Millisecond, // Small delay between chunk operations
		deletedInodes:            make(map[uint64]*DeletedInode),
		unreferencedChunks:       make(map[common.ChunkHandle]*UnreferencedChunk),
		scanProgress:             &ScanProgress{},
		state:                    common.ServiceStateUnknown,
	}
}

// Start starts the garbage collection manager
func (gc *GCManager) Start(ctx context.Context) error {
	gc.mu.Lock()
	defer gc.mu.Unlock()

	if gc.state == common.ServiceStateRunning {
		return fmt.Errorf("GC manager is already running")
	}

	gc.ctx, gc.cancel = context.WithCancel(ctx)
	gc.state = common.ServiceStateStarting

	// Start the main GC loop
	gc.gcTicker = time.NewTicker(gc.gcInterval)
	gc.wg.Add(1)
	go gc.gcLoop()

	gc.state = common.ServiceStateRunning
	log.Printf("Garbage collection manager started with interval: %v", gc.gcInterval)
	return nil
}

// Stop stops the garbage collection manager
func (gc *GCManager) Stop() error {
	gc.mu.Lock()
	defer gc.mu.Unlock()

	if gc.state == common.ServiceStateStopped || gc.state == common.ServiceStateStopping {
		return nil
	}

	gc.state = common.ServiceStateStopping
	log.Printf("Stopping garbage collection manager...")

	if gc.cancel != nil {
		gc.cancel()
	}

	if gc.gcTicker != nil {
		gc.gcTicker.Stop()
	}

	// Wait for all goroutines to finish with timeout
	done := make(chan struct{})
	go func() {
		defer close(done)
		gc.wg.Wait()
	}()

	select {
	case <-done:
		log.Printf("Garbage collection manager stopped successfully")
	case <-time.After(10 * time.Second):
		log.Printf("Garbage collection manager stop timeout exceeded")
	}

	gc.state = common.ServiceStateStopped
	return nil
}

// GetState returns the current state of the GC manager
func (gc *GCManager) GetState() common.ServiceState {
	gc.mu.RLock()
	defer gc.mu.RUnlock()
	return gc.state
}

// MarkInodeForDeletion marks an inode for lazy deletion
func (gc *GCManager) MarkInodeForDeletion(ino uint64, inode *types.Inode) {
	gc.mu.Lock()
	defer gc.mu.Unlock()

	deletedInode := &DeletedInode{
		Ino:       ino,
		DeletedAt: time.Now(),
	}

	// Handle case where inode is nil (already deleted from metadata)
	if inode != nil {
		deletedInode.Size = inode.Size
		deletedInode.ChunkCount = len(inode.Chunks)
		deletedInode.IsDirectory = (inode.GetMode() & 0040000) != 0 // S_IFDIR
	} else {
		// Inode is nil, which means it was already deleted from metadata
		// We can't retrieve the metadata anymore, so use default values
		deletedInode.Size = 0
		deletedInode.ChunkCount = 0
		deletedInode.IsDirectory = false
		log.Printf("Warning: Marking inode %d for deletion without metadata (inode was nil)", ino)
	}

	gc.deletedInodes[ino] = deletedInode
	log.Printf("Marked inode %d for deletion (size: %d, chunks: %d)", ino, deletedInode.Size, deletedInode.ChunkCount)
}

// MarkChunkUnreferenced marks a chunk as potentially unreferenced
func (gc *GCManager) MarkChunkUnreferenced(handle common.ChunkHandle, size uint32, locations []string) {
	gc.mu.Lock()
	defer gc.mu.Unlock()

	unreferencedChunk := &UnreferencedChunk{
		Handle:      handle,
		LastSeen:    time.Now(),
		Size:        size,
		Locations:   make([]string, len(locations)),
		MarkedForGC: false,
	}
	copy(unreferencedChunk.Locations, locations)

	gc.unreferencedChunks[handle] = unreferencedChunk
	log.Printf("Marked chunk %s as potentially unreferenced (size: %d)", handle.String(), size)
}

// GetScanProgress returns the current scan progress
func (gc *GCManager) GetScanProgress() *ScanProgress {
	gc.scanProgress.mu.RLock()
	defer gc.scanProgress.mu.RUnlock()

	// Return a copy to avoid race conditions
	return &ScanProgress{
		LastScanStart: gc.scanProgress.LastScanStart,
		LastScanEnd:   gc.scanProgress.LastScanEnd,
		TotalInodes:   gc.scanProgress.TotalInodes,
		ScannedInodes: gc.scanProgress.ScannedInodes,
		TotalChunks:   gc.scanProgress.TotalChunks,
		ScannedChunks: gc.scanProgress.ScannedChunks,
		IsScanning:    gc.scanProgress.IsScanning,
	}
}

// GetGCStats returns garbage collection statistics
func (gc *GCManager) GetGCStats() *GCStats {
	gc.mu.RLock()
	defer gc.mu.RUnlock()

	var totalDeletedSize uint64
	var totalUnreferencedSize uint64

	for _, deleted := range gc.deletedInodes {
		totalDeletedSize += deleted.Size
	}

	for _, unreferenced := range gc.unreferencedChunks {
		totalUnreferencedSize += uint64(unreferenced.Size)
	}

	return &GCStats{
		DeletedInodes:         len(gc.deletedInodes),
		UnreferencedChunks:    len(gc.unreferencedChunks),
		TotalDeletedSize:      totalDeletedSize,
		TotalUnreferencedSize: totalUnreferencedSize,
		LastScanTime:          gc.scanProgress.LastScanEnd,
		IsScanning:            gc.scanProgress.IsScanning,
	}
}

// GCStats contains garbage collection statistics
type GCStats struct {
	DeletedInodes         int
	UnreferencedChunks    int
	TotalDeletedSize      uint64
	TotalUnreferencedSize uint64
	LastScanTime          time.Time
	IsScanning            bool
}

// gcLoop is the main garbage collection loop
func (gc *GCManager) gcLoop() {
	defer gc.wg.Done()

	log.Printf("Starting garbage collection loop")

	for {
		select {
		case <-gc.ctx.Done():
			log.Printf("Garbage collection loop stopped")
			return
		case <-gc.gcTicker.C:
			gc.performGarbageCollection()
		}
	}
}

// performGarbageCollection performs a full garbage collection cycle
func (gc *GCManager) performGarbageCollection() {
	log.Printf("Starting garbage collection cycle")
	startTime := time.Now()

	// Update scan progress
	gc.scanProgress.mu.Lock()
	gc.scanProgress.LastScanStart = startTime
	gc.scanProgress.IsScanning = true
	gc.scanProgress.mu.Unlock()

	// Phase 1: Scan for unreferenced chunks
	gc.scanForUnreferencedChunks()

	// Phase 2: Process deleted inodes
	gc.processDeletedInodes()

	// Phase 3: Delete old unreferenced chunks
	gc.deleteOldUnreferencedChunks()

	// Update scan progress
	gc.scanProgress.mu.Lock()
	gc.scanProgress.LastScanEnd = time.Now()
	gc.scanProgress.IsScanning = false
	gc.scanProgress.mu.Unlock()

	duration := time.Since(startTime)
	log.Printf("Garbage collection cycle completed in %v", duration)
}

// scanForUnreferencedChunks scans the metadata to find unreferenced chunks
func (gc *GCManager) scanForUnreferencedChunks() {
	log.Printf("Scanning for unreferenced chunks")

	// Get all chunks from chunk metadata
	gc.master.metadataLock.RLock()
	allChunks := make(map[common.ChunkHandle]bool)
	for handle := range gc.master.master.ChunkMetadata {
		allChunks[handle] = false // false means not referenced yet
	}
	gc.master.metadataLock.RUnlock()

	// Update total chunks count
	gc.scanProgress.mu.Lock()
	gc.scanProgress.TotalChunks = uint64(len(allChunks))
	gc.scanProgress.ScannedChunks = 0
	gc.scanProgress.mu.Unlock()

	// Scan all inodes to find referenced chunks
	referencedChunks := make(map[common.ChunkHandle]bool)

	gc.master.metadataLock.RLock()
	gc.master.master.Metadata.Ascend(func(item btree.Item) bool {
		if btreeItem, ok := item.(*types.BTreeItem); ok && btreeItem.Inode != nil {
			// Mark all chunks in this inode as referenced
			for _, chunkHandle := range btreeItem.Inode.Chunks {
				referencedChunks[chunkHandle] = true
			}
		}
		return true // continue iteration
	})
	gc.master.metadataLock.RUnlock()

	// Find unreferenced chunks
	for handle := range allChunks {
		select {
		case <-gc.ctx.Done():
			return
		default:
		}

		if !referencedChunks[handle] {
			// This chunk is not referenced by any inode
			gc.master.metadataLock.RLock()
			chunkMeta, exists := gc.master.master.ChunkMetadata[handle]
			gc.master.metadataLock.RUnlock()

			if exists {
				gc.MarkChunkUnreferenced(handle, chunkMeta.Size, chunkMeta.Locations)
			}
		}

		// Update progress
		gc.scanProgress.mu.Lock()
		gc.scanProgress.ScannedChunks++
		gc.scanProgress.mu.Unlock()

		// Throttle to avoid overwhelming the system
		time.Sleep(gc.throttleDelay)
	}

	log.Printf("Unreferenced chunk scan completed, found %d unreferenced chunks", len(gc.unreferencedChunks))
}

// processDeletedInodes processes inodes that have been marked for deletion
func (gc *GCManager) processDeletedInodes() {
	gc.mu.Lock()
	defer gc.mu.Unlock()

	processedCount := 0
	for ino, deletedInode := range gc.deletedInodes {
		select {
		case <-gc.ctx.Done():
			return
		default:
		}

		// Check if enough time has passed since deletion
		if time.Since(deletedInode.DeletedAt) >= gc.orphanedChunkGracePeriod {
			log.Printf("Processing deleted inode %d", ino)
			delete(gc.deletedInodes, ino)
			processedCount++
		}

		// Throttle processing
		time.Sleep(gc.throttleDelay)
	}

	if processedCount > 0 {
		log.Printf("Processed %d deleted inodes", processedCount)
	}
}

// deleteOldUnreferencedChunks deletes chunks that have been unreferenced for too long
func (gc *GCManager) deleteOldUnreferencedChunks() {
	gc.mu.Lock()
	defer gc.mu.Unlock()

	processedCount := 0
	deletedCount := 0

	for handle, unreferenced := range gc.unreferencedChunks {
		select {
		case <-gc.ctx.Done():
			return
		default:
		}

		// Double-check with replication manager that this chunk is actually unreferenced
		// and not part of an active replication
		if gc.master.replicationManager != nil && gc.master.replicationManager.IsRunning() {
			// Check if chunk is currently being replicated
			rm := gc.master.replicationManager
			rm.mu.RLock()
			_, isActiveReplication := rm.activeReplications[handle]
			rm.mu.RUnlock()

			if isActiveReplication {
				log.Printf("Skipping GC of chunk %s - active replication/deletion in progress", handle.String())
				continue
			}
		}

		// Check if enough time has passed since marking as unreferenced
		if time.Since(unreferenced.LastSeen) >= gc.orphanedChunkGracePeriod {
			if !unreferenced.MarkedForGC {
				// Mark for deletion
				unreferenced.MarkedForGC = true
				unreferenced.GCScheduled = time.Now()
				log.Printf("Scheduled chunk %s for deletion", handle.String())
			} else if time.Since(unreferenced.GCScheduled) >= time.Minute {
				// Actually delete the chunk
				if gc.deleteChunkFromServers(handle, unreferenced.Locations) {
					// Log chunk deletion for recovery
					if gc.master.persistenceManager != nil {
						// For GC, we might not have easy access to inode/chunk index
						// but we can use 0 as placeholder or enhance data structure
						err := gc.master.persistenceManager.LogDeleteChunk(handle.String(), 0, 0)
						if err != nil {
							log.Printf("Failed to log GC chunk deletion for %s: %v", handle.String(), err)
						}
					}

					delete(gc.unreferencedChunks, handle)
					deletedCount++
					log.Printf("Deleted unreferenced chunk %s", handle.String())
				}
			}
		}
		processedCount++

		// Throttle processing
		time.Sleep(gc.throttleDelay)
	}

	if deletedCount > 0 {
		log.Printf("Deleted %d unreferenced chunks (processed %d total)", deletedCount, processedCount)
	}
}

// deleteChunkFromServers attempts to delete a chunk from all its replica locations
func (gc *GCManager) deleteChunkFromServers(handle common.ChunkHandle, locations []string) bool {
	if len(locations) == 0 {
		return true // Consider it deleted if no locations
	}

	successCount := 0
	for _, location := range locations {
		if gc.deleteChunkFromServer(handle, location) {
			successCount++
		}
	}

	// Consider successful if we deleted from majority of replicas
	return successCount > len(locations)/2
}

// deleteChunkFromServer deletes a chunk from a specific server
func (gc *GCManager) deleteChunkFromServer(handle common.ChunkHandle, serverAddr string) bool {
	log.Printf("GC: Deleting chunk %s from server %s", handle.String(), serverAddr)

	// Get chunk server client manager for gRPC calls
	if gc.master.chunkServerClientManager == nil {
		log.Printf("GC: No chunk server client manager available")
		return false
	}

	// Create context with timeout for deletion request
	ctx, cancel := context.WithTimeout(gc.ctx, 30*time.Second)
	defer cancel()

	// Get or create connection to the chunk server
	conn, err := gc.master.chunkServerClientManager.GetConnection(serverAddr)
	if err != nil {
		log.Printf("GC: Failed to get connection for server %s: %v", serverAddr, err)
		return false
	}
	defer gc.master.chunkServerClientManager.ReleaseConnection(serverAddr)

	// Create chunk server client
	client := chunkserver.NewChunkServerServiceClient(conn)

	// Send deletion request
	_, err = client.DeleteChunks(ctx, &chunkserver.DeleteChunksRequest{
		ChunkHandles: []string{handle.String()},
	})

	if err != nil {
		log.Printf("GC: Failed to delete chunk %s from server %s: %v", handle.String(), serverAddr, err)
		return false
	}

	log.Printf("GC: Successfully deleted chunk %s from server %s", handle.String(), serverAddr)
	return true
}
