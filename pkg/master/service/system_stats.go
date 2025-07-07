package service

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/google/btree"
	"github.com/uttam-li/dfs/api/generated/master"
	commonTypes "github.com/uttam-li/dfs/pkg/common"
	mastTypes "github.com/uttam-li/dfs/pkg/master/types"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	// Default filesystem constants
	DefaultBlockSize = 64 * 1024 * 1024 // 64MB chunks
	DefaultNameLen   = 255              // Maximum filename length
)

// SystemStatsCalculator handles filesystem statistics calculations
type SystemStatsCalculator struct {
	master       *mastTypes.Master
	chunkServers map[string]*ChunkServerInfo
	config       *commonTypes.MasterConfig
	mu           sync.RWMutex
}

// SystemStats represents calculated filesystem statistics
type SystemStats struct {
	TotalSpace     uint64
	UsedSpace      uint64
	AvailableSpace uint64
	TotalInodes    uint64
	UsedInodes     uint64
	FreeInodes     uint64
	TotalChunks    uint64
	BlockSize      uint32
}

// GetSystemStats retrieves comprehensive filesystem statistics
func (ms *MasterService) GetSystemStats() (*master.GetSystemStatsResponse, error) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	// Calculate statistics with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	calculator := &SystemStatsCalculator{
		master:       ms.master,
		chunkServers: ms.convertChunkServers(),
		config:       ms.config,
	}

	stats, err := calculator.calculateStats(ctx)
	if err != nil {
		log.Printf("[ERROR] Failed to calculate system stats: %v", err)
		return nil, status.Errorf(codes.Internal, "Failed to calculate system statistics: %v", err)
	}

	// Build successful response
	response := &master.GetSystemStatsResponse{
		Stats: &master.GetSystemStatsResponse_SystemStats{
			TotalChunkServers:     uint32(len(ms.convertChunkServers())),
			ActiveChunkServers:    uint32(ms.getActiveChunkServerCount()),
			TotalInodes:           stats.TotalInodes,
			UsedInodes:            stats.UsedInodes,
			FreeInodes:            stats.FreeInodes,
			TotalChunks:           stats.TotalChunks,
			TotalFiles:            ms.getTotalFileCount(),
			TotalDirectories:      ms.getTotalDirectoryCount(),
			TotalSpace:            stats.TotalSpace,
			UsedSpace:             stats.UsedSpace,
			AvailableSpace:        stats.AvailableSpace,
			ReplicationFactor:     3.0, // Default replication factor
			UnderReplicatedChunks: ms.getUnderReplicatedChunkCount(),
			OverReplicatedChunks:  ms.getOverReplicatedChunkCount(),
		},
	}

	if ms.config.Debug {
		log.Printf("[DEBUG] System stats calculated: total_space=%d, used_space=%d, available_space=%d, total_inodes=%d, used_inodes=%d",
			stats.TotalSpace, stats.UsedSpace, stats.AvailableSpace, stats.TotalInodes, stats.UsedInodes)
	}

	return response, nil
}

// convertChunkServers converts the UUID-keyed map to string-keyed map for calculator
func (ms *MasterService) convertChunkServers() map[string]*ChunkServerInfo {
	result := make(map[string]*ChunkServerInfo)
	chunkServers := ms.GetChunkServers()

	for uuid, info := range chunkServers {
		result[uuid.String()] = info
	}

	return result
}

// calculateStats performs the actual statistics calculation
func (calc *SystemStatsCalculator) calculateStats(ctx context.Context) (*SystemStats, error) {
	calc.mu.Lock()
	defer calc.mu.Unlock()

	// Check for context cancellation
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	stats := &SystemStats{
		BlockSize: DefaultBlockSize,
	}

	// Calculate storage statistics from chunk servers
	if err := calc.calculateStorageStats(stats); err != nil {
		return nil, fmt.Errorf("failed to calculate storage stats: %w", err)
	}

	// Calculate inode statistics from metadata
	if err := calc.calculateInodeStats(stats); err != nil {
		return nil, fmt.Errorf("failed to calculate inode stats: %w", err)
	}

	// Calculate chunk statistics
	if err := calc.calculateChunkStats(stats); err != nil {
		return nil, fmt.Errorf("failed to calculate chunk stats: %w", err)
	}

	// Validate calculated statistics
	if err := calc.validateStats(stats); err != nil {
		return nil, fmt.Errorf("invalid statistics calculated: %w", err)
	}

	return stats, nil
}

// calculateStorageStats calculates storage-related statistics
func (calc *SystemStatsCalculator) calculateStorageStats(stats *SystemStats) error {
	var totalSpace, usedSpace uint64

	// Aggregate from all chunk servers
	for serverID, serverInfo := range calc.chunkServers {
		if serverInfo == nil {
			log.Printf("[WARN] Nil server info for server %s", serverID)
			continue
		}

		totalSpace += serverInfo.TotalSpace
		usedSpace += serverInfo.UsedSpace

		// Validate server data
		if serverInfo.UsedSpace > serverInfo.TotalSpace {
			log.Printf("[WARN] Server %s reports invalid space usage: used=%d > total=%d",
				serverID, serverInfo.UsedSpace, serverInfo.TotalSpace)
		}
	}

	stats.TotalSpace = totalSpace
	stats.UsedSpace = usedSpace

	// Calculate available space (ensure no underflow)
	if totalSpace >= usedSpace {
		stats.AvailableSpace = totalSpace - usedSpace
	} else {
		log.Printf("[WARN] Used space (%d) exceeds total space (%d), setting available to 0", usedSpace, totalSpace)
		stats.AvailableSpace = 0
	}

	return nil
}

// calculateInodeStats calculates inode-related statistics
func (calc *SystemStatsCalculator) calculateInodeStats(stats *SystemStats) error {
	if calc.master == nil {
		return fmt.Errorf("master reference is nil")
	}

	// Count used inodes by traversing the B-tree
	usedInodes := uint64(0)
	calc.master.Metadata.Ascend(func(item btree.Item) bool {
		if item != nil {
			usedInodes++
		}
		return true // Continue iteration
	})

	stats.UsedInodes = usedInodes

	// Calculate total possible inodes (configurable limit)
	// Use a reasonable default based on system capacity
	maxInodes := calc.config.MaxInodes
	if maxInodes == 0 {
		// Default: 1 inode per 4KB of total space
		maxInodes = stats.TotalSpace / 4096
		if maxInodes == 0 {
			maxInodes = 1000000 // Minimum 1M inodes
		}
	}

	stats.TotalInodes = maxInodes

	// Calculate free inodes (ensure no underflow)
	if maxInodes >= usedInodes {
		stats.FreeInodes = maxInodes - usedInodes
	} else {
		log.Printf("[WARN] Used inodes (%d) exceeds max inodes (%d), setting free to 0", usedInodes, maxInodes)
		stats.FreeInodes = 0
	}

	return nil
}

// calculateChunkStats calculates chunk-related statistics
func (calc *SystemStatsCalculator) calculateChunkStats(stats *SystemStats) error {
	if calc.master == nil || calc.master.ChunkMetadata == nil {
		return fmt.Errorf("chunk metadata is not available")
	}

	stats.TotalChunks = uint64(len(calc.master.ChunkMetadata))
	return nil
}

// validateStats validates the calculated statistics for consistency
func (calc *SystemStatsCalculator) validateStats(stats *SystemStats) error {
	// Validate storage statistics
	if stats.UsedSpace > stats.TotalSpace {
		return fmt.Errorf("used space (%d) cannot exceed total space (%d)", stats.UsedSpace, stats.TotalSpace)
	}

	if stats.AvailableSpace > stats.TotalSpace {
		return fmt.Errorf("available space (%d) cannot exceed total space (%d)", stats.AvailableSpace, stats.TotalSpace)
	}

	if stats.UsedSpace+stats.AvailableSpace > stats.TotalSpace {
		return fmt.Errorf("used + available space (%d) cannot exceed total space (%d)",
			stats.UsedSpace+stats.AvailableSpace, stats.TotalSpace)
	}

	// Validate inode statistics
	if stats.UsedInodes > stats.TotalInodes {
		return fmt.Errorf("used inodes (%d) cannot exceed total inodes (%d)", stats.UsedInodes, stats.TotalInodes)
	}

	if stats.FreeInodes > stats.TotalInodes {
		return fmt.Errorf("free inodes (%d) cannot exceed total inodes (%d)", stats.FreeInodes, stats.TotalInodes)
	}

	if stats.UsedInodes+stats.FreeInodes > stats.TotalInodes {
		return fmt.Errorf("used + free inodes (%d) cannot exceed total inodes (%d)",
			stats.UsedInodes+stats.FreeInodes, stats.TotalInodes)
	}

	// Validate block size
	if stats.BlockSize == 0 {
		return fmt.Errorf("block size cannot be zero")
	}

	return nil
}

// Helper methods for system statistics

// getActiveChunkServerCount returns the number of active chunk servers
func (ms *MasterService) getActiveChunkServerCount() int {
	count := 0
	chunkServers := ms.GetChunkServers()

	for _, serverInfo := range chunkServers {
		if serverInfo != nil {
			// Consider a server active if it has reported recently
			// You can add more sophisticated logic here based on last heartbeat
			count++
		}
	}

	return count
}

// getTotalFileCount returns the total number of files in the filesystem
func (ms *MasterService) getTotalFileCount() uint64 {
	if ms.master == nil {
		return 0
	}

	fileCount := uint64(0)
	ms.master.Metadata.Ascend(func(item btree.Item) bool {
		if btreeItem, ok := item.(*mastTypes.BTreeItem); ok && btreeItem.Inode != nil {
			if !btreeItem.Inode.IsDir() {
				fileCount++
			}
		}
		return true
	})

	return fileCount
}

// getTotalDirectoryCount returns the total number of directories in the filesystem
func (ms *MasterService) getTotalDirectoryCount() uint64 {
	if ms.master == nil {
		return 0
	}

	dirCount := uint64(0)
	ms.master.Metadata.Ascend(func(item btree.Item) bool {
		if btreeItem, ok := item.(*mastTypes.BTreeItem); ok && btreeItem.Inode != nil {
			if btreeItem.Inode.IsDir() {
				dirCount++
			}
		}
		return true
	})

	return dirCount
}

// getUnderReplicatedChunkCount returns the number of under-replicated chunks
func (ms *MasterService) getUnderReplicatedChunkCount() uint32 {
	if ms.master == nil || ms.master.ChunkMetadata == nil {
		return 0
	}

	count := uint32(0)
	defaultReplicas := 3 // Default replication factor

	for _, chunkMeta := range ms.master.ChunkMetadata {
		if chunkMeta != nil && len(chunkMeta.Locations) < defaultReplicas {
			count++
		}
	}

	return count
}

// getOverReplicatedChunkCount returns the number of over-replicated chunks
func (ms *MasterService) getOverReplicatedChunkCount() uint32 {
	if ms.master == nil || ms.master.ChunkMetadata == nil {
		return 0
	}

	count := uint32(0)
	defaultReplicas := 3 // Default replication factor

	for _, chunkMeta := range ms.master.ChunkMetadata {
		if chunkMeta != nil && len(chunkMeta.Locations) > defaultReplicas {
			count++
		}
	}

	return count
}
