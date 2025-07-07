package service

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
)

// initialize initializes the chunk server resources
func (cs *ChunkServerService) initialize() error {
	if err := os.MkdirAll(cs.storage.storageRoot, 0755); err != nil {
		return fmt.Errorf("failed to create storage directory: %w", err)
	}

	// Scan for existing chunks in storage directory
	if err := cs.scanExistingChunks(); err != nil {
		log.Printf("[WARN] Failed to scan existing chunks: %v", err)
		// Don't fail initialization, just log the warning
	}

	log.Printf("Chunk server initialized with storage root: %s", cs.config.StorageRoot)
	log.Printf("Found %d existing chunks", len(cs.storage.chunks))
	return nil
}

// scanExistingChunks scans the storage directory for existing chunk files
func (cs *ChunkServerService) scanExistingChunks() error {
	entries, err := os.ReadDir(cs.storage.storageRoot)
	if err != nil {
		return fmt.Errorf("failed to read storage directory: %w", err)
	}

	cs.storage.mu.Lock()
	defer cs.storage.mu.Unlock()

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		handle := strings.TrimSuffix(entry.Name(), ".chunk")
		filePath := filepath.Join(cs.storage.storageRoot, entry.Name())

		fileInfo, err := entry.Info()
		if err != nil {
			log.Printf("failed to get file info for %s: %v", handle, err)
			continue
		}

		// Create metadata for existing chunk
		metadata := &ChunkMetadata{
			Handle:       handle,
			Version:      1, // Default version, should be managed by master
			Size:         uint64(fileInfo.Size()),
			Checksum:     cs.computeChecksum(filePath),
			CreatedTime:  fileInfo.ModTime(),
			LastAccessed: fileInfo.ModTime(),
			LastModified: fileInfo.ModTime(),
			FilePath:     filePath,
		}

		cs.storage.chunks[handle] = metadata
		log.Printf("[INFO] Discovered existing chunk: %s (%d bytes)", handle, metadata.Size)
	}

	return nil
}

// VerifyChecksum verifies the checksum of a chunk
func (cs *ChunkServerService) VerifyChecksum(handle string) (bool, []byte, []byte, error) {
	cs.storage.mu.RLock()
	metadata, exists := cs.storage.chunks[handle]
	cs.storage.mu.RUnlock()

	if !exists {
		return false, nil, nil, fmt.Errorf("chunk not found: %s", handle)
	}

	storedChecksum := metadata.Checksum
	computedChecksum := cs.computeChecksum(metadata.FilePath)

	if computedChecksum == nil {
		return false, storedChecksum, nil, fmt.Errorf("failed to compute checksum")
	}

	isValid := true
	if storedChecksum != nil {
		isValid = string(storedChecksum) == string(computedChecksum)
	}

	return isValid, storedChecksum, computedChecksum, nil
}

// IsChunkSparse checks if a chunk has sparse regions (holes)
func (cs *ChunkServerService) IsChunkSparse(handle string) (bool, uint64, uint64, error) {
	cs.storage.mu.RLock()
	metadata, exists := cs.storage.chunks[handle]
	cs.storage.mu.RUnlock()

	if !exists {
		return false, 0, 0, fmt.Errorf("chunk not found: %s", handle)
	}

	// Get actual file size on disk
	fileInfo, err := os.Stat(metadata.FilePath)
	if err != nil {
		return false, 0, 0, fmt.Errorf("failed to stat chunk file: %w", err)
	}

	actualSize := uint64(fileInfo.Size())
	logicalSize := metadata.Size

	isSparse := logicalSize > actualSize

	return isSparse, logicalSize, actualSize, nil
}

// GetChunkSpaceUsage returns detailed space usage information for debugging
func (cs *ChunkServerService) GetChunkSpaceUsage() map[string]map[string]uint64 {
	cs.storage.mu.RLock()
	defer cs.storage.mu.RUnlock()

	usage := make(map[string]map[string]uint64)

	for handle, metadata := range cs.storage.chunks {
		chunkUsage := make(map[string]uint64)
		chunkUsage["logical_size"] = metadata.Size

		// Get actual file size
		if fileInfo, err := os.Stat(metadata.FilePath); err == nil {
			chunkUsage["actual_size"] = uint64(fileInfo.Size())
			chunkUsage["is_sparse"] = 0
			if metadata.Size > uint64(fileInfo.Size()) {
				chunkUsage["is_sparse"] = 1
			}
		} else {
			chunkUsage["actual_size"] = 0
			chunkUsage["is_sparse"] = 0
		}

		usage[handle] = chunkUsage
	}

	return usage
}