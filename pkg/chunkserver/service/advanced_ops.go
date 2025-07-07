package service

import (
	"context"
	"crypto/md5"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/uttam-li/dfs/api/generated/chunkserver"
)

// WriteOperation represents a single write operation.
type WriteOperation struct {
	Offset uint64
	Data   []byte
}

// ChunkReadRequest represents a chunk read request.
type ChunkReadRequest struct {
	Handle string
	Offset uint64
	Length uint32
}

// ChunkReadResponse represents a chunk read response.
type ChunkReadResponse struct {
	Handle string
	Data   []byte
	Error  error
}

// checkShutdown checks if the service is shutting down.
func (cs *ChunkServerService) checkShutdown() error {
	select {
	case <-cs.ctx.Done():
		return fmt.Errorf("operation cancelled: service is shutting down")
	default:
		return nil
	}
}

// ensureChunkMetadata ensures chunk metadata exists and handles version validation.
func (cs *ChunkServerService) ensureChunkMetadata(handle string, version uint32) error {
	cs.storage.mu.Lock()
	defer cs.storage.mu.Unlock()

	metadata, exists := cs.storage.chunks[handle]
	if !exists {
		chunkPath := filepath.Join(cs.storage.storageRoot, handle+".chunk")
		metadata = &ChunkMetadata{
			Handle:       handle,
			Version:      version,
			Size:         0,
			Checksum:     nil,
			CreatedTime:  time.Now(),
			LastAccessed: time.Now(),
			LastModified: time.Now(),
			FilePath:     chunkPath,
		}
		cs.storage.chunks[handle] = metadata
		return nil
	}

	if metadata.Version < version {
		metadata.Version = version
	} else if metadata.Version > version {
		return fmt.Errorf("version conflict: chunk has version %d, write requests version %d",
			metadata.Version, version)
	}

	return nil
}

// filterOtherReplicas returns replicas excluding this server
func (cs *ChunkServerService) filterOtherReplicas(replicas []string) []string {
	var otherReplicas []string
	serverAddr := cs.getServerAddress()

	for _, replica := range replicas {
		if replica != serverAddr {
			otherReplicas = append(otherReplicas, replica)
		}
	}

	return otherReplicas
}

// forwardWritesToReplicas forwards writes to replica servers concurrently
func (cs *ChunkServerService) forwardWritesToReplicas(replicas []string, handle string, writeIDs []string) error {
	var wg sync.WaitGroup
	errs := make(chan error, len(replicas))

	for _, replica := range replicas {
		wg.Add(1)
		go func(targetReplica string) {
			defer wg.Done()
			if err := cs.forwardSerializeWrites(targetReplica, handle, writeIDs); err != nil {
				log.Printf("[ERROR] SerializeWrites: Failed to apply writes to replica %s: %v", targetReplica, err)
				errs <- fmt.Errorf("failed to apply writes to replica %s: %w", targetReplica, err)
			}
		}(replica)
	}

	wg.Wait()
	close(errs)

	for err := range errs {
		if err != nil {
			return err
		}
	}

	return nil
}

// BufferData stores write data in memory until commit time with proper version handling.
func (cs *ChunkServerService) BufferData(writeID, handle string, offset uint64, data []byte, version uint32) error {
	if err := cs.checkShutdown(); err != nil {
		return err
	}

	cs.writeBuffersMu.Lock()
	defer cs.writeBuffersMu.Unlock()

	if err := cs.ensureChunkMetadata(handle, version); err != nil {
		return err
	}

	dataCopy := make([]byte, len(data))
	copy(dataCopy, data)

	buffer := &WriteBuffer{
		Handle:      handle,
		Offset:      offset,
		Data:        dataCopy,
		Version:     version,
		CreatedTime: time.Now(),
	}

	cs.writeBuffers[writeID] = buffer
	return nil
}

// SerializeWrites tells replicas to apply writes in order (lease holder function).
func (cs *ChunkServerService) SerializeWrites(handle string, writeIDs []string, replicas []string) error {
	if err := cs.checkShutdown(); err != nil {
		return err
	}

	appliedWrites := make(map[string]bool)

	for _, writeID := range writeIDs {
		if err := cs.checkShutdown(); err != nil {
			cs.cleanupFailedWriteBuffers(writeIDs, appliedWrites)
			return err
		}

		if err := cs.ApplyWrite(context.Background(), writeID); err != nil {
			log.Printf("[ERROR] SerializeWrites: Primary failed to apply write %s: %v", writeID, err)
			cs.cleanupFailedWriteBuffers(writeIDs, appliedWrites)
			return fmt.Errorf("primary failed to apply write %s: %w", writeID, err)
		}
		appliedWrites[writeID] = true
	}

	otherReplicas := cs.filterOtherReplicas(replicas)
	if len(otherReplicas) == 0 {
		return nil
	}

	return cs.forwardWritesToReplicas(otherReplicas, handle, writeIDs)
}

// ApplyWrites applies a sequence of writes atomically to a chunk.
// Creates the chunk file lazily if it doesn't exist yet.
func (cs *ChunkServerService) ApplyWrites(handle string, writes []WriteOperation) error {
	if err := cs.checkShutdown(); err != nil {
		return err
	}

	cs.storage.mu.Lock()
	defer cs.storage.mu.Unlock()

	metadata, exists := cs.storage.chunks[handle]
	if !exists {
		return fmt.Errorf("chunk metadata not found: %s (should have been created during BufferData)", handle)
	}

	if err := os.MkdirAll(cs.storage.storageRoot, 0755); err != nil {
		return fmt.Errorf("failed to create storage directory: %w", err)
	}

	file, err := os.OpenFile(metadata.FilePath, os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return fmt.Errorf("failed to open/create chunk file: %w", err)
	}
	defer file.Close()

	var maxOffset uint64
	for _, write := range writes {
		if _, err := file.Seek(int64(write.Offset), 0); err != nil {
			return fmt.Errorf("failed to seek to offset %d: %w", write.Offset, err)
		}

		if _, err := file.Write(write.Data); err != nil {
			return fmt.Errorf("failed to write data at offset %d: %w", write.Offset, err)
		}

		endOffset := write.Offset + uint64(len(write.Data))
		if endOffset > maxOffset {
			maxOffset = endOffset
		}
	}

	if err := file.Sync(); err != nil {
		return fmt.Errorf("failed to sync chunk file: %w", err)
	}

	if maxOffset > metadata.Size {
		metadata.Size = maxOffset
	}
	metadata.LastModified = time.Now()
	metadata.Checksum = cs.computeChecksum(metadata.FilePath)

	return nil
}

// ValidateChunkVersion validates that a chunk has the expected version.
// Returns success for non-existent chunks (they will be created lazily with the expected version).
func (cs *ChunkServerService) ValidateChunkVersion(handle string, expectedVersion uint32) error {
	cs.storage.mu.RLock()
	metadata, exists := cs.storage.chunks[handle]
	cs.storage.mu.RUnlock()

	if !exists {
		return nil
	}

	if metadata.Version != expectedVersion {
		return fmt.Errorf("version mismatch for chunk %s: expected %d, got %d",
			handle, expectedVersion, metadata.Version)
	}

	return nil
}

// UpdateChunkVersion updates the version of a chunk.
func (cs *ChunkServerService) UpdateChunkVersion(handle string, newVersion uint32) error {
	cs.storage.mu.Lock()
	defer cs.storage.mu.Unlock()

	metadata, exists := cs.storage.chunks[handle]
	if !exists {
		return fmt.Errorf("chunk not found: %s", handle)
	}

	metadata.Version = newVersion
	metadata.LastModified = time.Now()
	return nil
}

// getServerAddress returns the server address for this chunkserver
func (cs *ChunkServerService) getServerAddress() string {
	return fmt.Sprintf("localhost:%d", cs.config.GRPCPort)
}

// forwardSerializeWrites forwards a SerializeWrites request to a secondary replica.
func (cs *ChunkServerService) forwardSerializeWrites(replicaAddress, handle string, writeIDs []string) error {
	conn, err := cs.getChunkServerConnection(replicaAddress)
	if err != nil {
		return fmt.Errorf("failed to get connection to replica %s: %w", replicaAddress, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	resp, err := conn.client.SerializeWrites(ctx, &chunkserver.SerializeWritesRequest{
		ChunkHandle:    handle,
		WriteIds:       writeIDs,
		ReplicaServers: []string{}, // Empty to avoid infinite recursion
	})

	if err != nil {
		return fmt.Errorf("gRPC call failed: %w", err)
	}

	if !resp.Success {
		return fmt.Errorf("serialize writes failed on replica: %s", resp.ErrorMessage)
	}

	return nil
}

// ComputeDataChecksum computes checksum for arbitrary data.
func (cs *ChunkServerService) ComputeDataChecksum(data []byte) []byte {
	hash := md5.New()
	hash.Write(data)
	return hash.Sum(nil)
}

// ApplyWrite applies a buffered write to disk with proper version management.
func (cs *ChunkServerService) ApplyWrite(ctx context.Context, writeID string) error {
	cs.writeBuffersMu.RLock()
	buffer, exists := cs.writeBuffers[writeID]
	cs.writeBuffersMu.RUnlock()

	if !exists {
		return fmt.Errorf("write buffer %s not found", writeID)
	}

	cs.storage.mu.Lock()
	defer cs.storage.mu.Unlock()

	metadata, exists := cs.storage.chunks[buffer.Handle]
	if !exists {
		return fmt.Errorf("chunk %s not found", buffer.Handle)
	}

	if metadata.Version != buffer.Version {
		return fmt.Errorf("version mismatch: metadata version %d != buffer version %d",
			metadata.Version, buffer.Version)
	}

	file, err := os.OpenFile(metadata.FilePath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return fmt.Errorf("failed to open chunk file: %w", err)
	}
	defer file.Close()

	if _, err := file.Seek(int64(buffer.Offset), 0); err != nil {
		return fmt.Errorf("failed to seek in file: %w", err)
	}

	if _, err = file.Write(buffer.Data); err != nil {
		return fmt.Errorf("failed to write data: %w", err)
	}

	if err := file.Sync(); err != nil {
		return fmt.Errorf("failed to sync file: %w", err)
	}

	if err := cs.updateChunkChecksum(buffer.Handle); err != nil {
		return fmt.Errorf("failed to update checksum: %w", err)
	}

	if fileInfo, err := file.Stat(); err == nil {
		metadata.Size = uint64(fileInfo.Size())
	}

	delete(cs.writeBuffers, writeID)
	return nil
}

// updateChunkChecksum updates the checksum for a chunk
func (cs *ChunkServerService) updateChunkChecksum(handle string) error {
	metadata, exists := cs.storage.chunks[handle]
	if !exists {
		return fmt.Errorf("chunk not found: %s", handle)
	}

	metadata.Checksum = cs.computeChecksum(metadata.FilePath)
	return nil
}

// cleanupStaleWriteBuffers removes write buffers that are older than the specified timeout.
// This prevents memory leaks from failed or abandoned write operations.
func (cs *ChunkServerService) cleanupStaleWriteBuffers(timeout time.Duration) int {
	cs.writeBuffersMu.Lock()
	defer cs.writeBuffersMu.Unlock()

	now := time.Now()
	cleanedCount := 0

	for writeID, buffer := range cs.writeBuffers {
		if now.Sub(buffer.CreatedTime) > timeout {
			delete(cs.writeBuffers, writeID)
			cleanedCount++
		}
	}

	return cleanedCount
}

// startBufferCleanupRoutine starts a background goroutine that periodically cleans up stale write buffers.
// This implements GFS-style automatic memory management where each component is responsible for its own cleanup.
func (cs *ChunkServerService) startBufferCleanupRoutine() {
	cs.wg.Add(1)
	go func() {
		defer cs.wg.Done()

		cleanupInterval := 5 * time.Minute
		bufferTimeout := 30 * time.Minute

		ticker := time.NewTicker(cleanupInterval)
		defer ticker.Stop()

		for {
			select {
			case <-cs.ctx.Done():
				return
			case <-ticker.C:
				cs.cleanupStaleWriteBuffers(bufferTimeout)
			}
		}
	}()
}

// cleanupAllWriteBuffers removes all write buffers (used during shutdown).
// This ensures proper cleanup when the chunkserver stops.
func (cs *ChunkServerService) cleanupAllWriteBuffers() {
	cs.writeBuffersMu.Lock()
	defer cs.writeBuffersMu.Unlock()

	bufferCount := len(cs.writeBuffers)
	if bufferCount > 0 {
		cs.writeBuffers = make(map[string]*WriteBuffer)
	}
}

// cleanupFailedWriteBuffers cleans up buffers for writes that failed during serialization.
// This provides GFS-style robustness by ensuring no orphaned buffers remain after failures.
func (cs *ChunkServerService) cleanupFailedWriteBuffers(writeIDs []string, appliedWrites map[string]bool) {
	cs.writeBuffersMu.Lock()
	defer cs.writeBuffersMu.Unlock()

	for _, writeID := range writeIDs {
		if !appliedWrites[writeID] {
			delete(cs.writeBuffers, writeID)
		}
	}
}
