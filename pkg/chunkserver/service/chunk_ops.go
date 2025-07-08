package service

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/uttam-li/dfs/api/generated/chunkserver"
)

// CreateChunk creates a new chunk on this server.
func (cs *ChunkServerService) CreateChunk(handle string, version uint32, initialSize uint64) error {
	if handle == "" {
		return fmt.Errorf("chunk handle cannot be empty")
	}

	cs.storage.mu.Lock()
	defer cs.storage.mu.Unlock()

	if _, exists := cs.storage.chunks[handle]; exists {
		return fmt.Errorf("chunk already exists: %s", handle)
	}

	chunkPath := filepath.Join(cs.storage.storageRoot, handle+".chunk")

	if err := os.MkdirAll(cs.storage.storageRoot, 0755); err != nil {
		return fmt.Errorf("failed to create storage directory: %w", err)
	}

	file, err := os.Create(chunkPath)
	if err != nil {
		return fmt.Errorf("failed to create chunk file: %w", err)
	}
	defer file.Close()

	if initialSize > 0 {
		if err := file.Truncate(int64(initialSize)); err != nil {
			return fmt.Errorf("failed to truncate chunk file: %w", err)
		}
	}

	now := time.Now()
	metadata := &ChunkMetadata{
		Handle:       handle,
		Version:      version,
		Size:         initialSize,
		Checksum:     nil,
		CreatedTime:  now,
		LastAccessed: now,
		LastModified: now,
		FilePath:     chunkPath,
	}

	cs.storage.chunks[handle] = metadata
	return nil
}

// ReadChunk reads data from a chunk at specified offset and length.
func (cs *ChunkServerService) ReadChunk(handle string, offset uint64, length uint32) ([]byte, error) {
	if handle == "" {
		return nil, fmt.Errorf("chunk handle cannot be empty")
	}

	cs.storage.mu.RLock()
	metadata, exists := cs.storage.chunks[handle]
	cs.storage.mu.RUnlock()

	if !exists {
		return []byte{}, nil
	}

	if _, err := os.Stat(metadata.FilePath); os.IsNotExist(err) {
		return []byte{}, nil
	}

	file, err := os.Open(metadata.FilePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open chunk file: %w", err)
	}
	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to get file stats: %w", err)
	}

	if uint64(fileInfo.Size()) <= offset {
		return []byte{}, nil
	}

	if offset+uint64(length) > uint64(fileInfo.Size()) {
		length = uint32(uint64(fileInfo.Size()) - offset)
	}

	data := make([]byte, length)

	if _, err := file.Seek(int64(offset), io.SeekStart); err != nil {
		return nil, fmt.Errorf("failed to seek to offset: %w", err)
	}

	bytesRead, err := io.ReadFull(file, data)
	if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
		return nil, fmt.Errorf("failed to read chunk data: %w", err)
	}

	return data[:bytesRead], nil
}

// WriteChunk writes data to a chunk (creates chunk lazily if it doesn't exist).
func (cs *ChunkServerService) WriteChunk(handle string, offset uint64, data []byte) error {
	if handle == "" {
		return fmt.Errorf("chunk handle cannot be empty")
	}

	select {
	case <-cs.ctx.Done():
		return fmt.Errorf("write operation cancelled: service is shutting down")
	default:
	}

	cs.storage.mu.Lock()
	metadata, exists := cs.storage.chunks[handle]

	if !exists {
		chunkPath := filepath.Join(cs.storage.storageRoot, handle+".chunk")
		now := time.Now()
		metadata = &ChunkMetadata{
			Handle:       handle,
			Version:      1,
			Size:         0,
			Checksum:     nil,
			CreatedTime:  now,
			LastAccessed: now,
			LastModified: now,
			FilePath:     chunkPath,
		}
		cs.storage.chunks[handle] = metadata
	}
	cs.storage.mu.Unlock()

	file, err := os.OpenFile(metadata.FilePath, os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return fmt.Errorf("failed to open chunk file for writing: %w", err)
	}
	defer func() {
		if closeErr := file.Close(); closeErr != nil {
			log.Printf("Failed to close chunk file %s: %v", handle, closeErr)
		}
	}()

	if _, err := file.Seek(int64(offset), 0); err != nil {
		return fmt.Errorf("failed to seek in chunk file: %w", err)
	}

	bytesWritten, err := file.Write(data)
	if err != nil {
		return fmt.Errorf("failed to write chunk data: %w", err)
	}

	// Only sync periodically or for final writes to reduce disk I/O bottleneck
	// During replication, we'll sync at the end in ReplicateChunkFromSource
	// For now, just ensure data is written but don't force expensive sync on every write
	
	// Update metadata quickly (short lock hold time)
	cs.storage.mu.Lock()
	newSize := offset + uint64(bytesWritten)
	if newSize > metadata.Size {
		metadata.Size = newSize
	}
	metadata.LastModified = time.Now()
	cs.storage.mu.Unlock()

	return nil
}

// DeleteChunk removes a chunk from this server.
func (cs *ChunkServerService) DeleteChunk(handle string) error {
	if handle == "" {
		return fmt.Errorf("chunk handle cannot be empty")
	}

	cs.storage.mu.Lock()
	defer cs.storage.mu.Unlock()

	metadata, exists := cs.storage.chunks[handle]
	if !exists {
		return fmt.Errorf("chunk not found: %s", handle)
	}

	if err := os.Remove(metadata.FilePath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to remove chunk file: %w", err)
	}

	delete(cs.storage.chunks, handle)
	return nil
}

// GetChunkInfo returns information about a chunk.
func (cs *ChunkServerService) GetChunkInfo(handle string) (*ChunkMetadata, error) {
	if handle == "" {
		return nil, fmt.Errorf("chunk handle cannot be empty")
	}

	cs.storage.mu.RLock()
	defer cs.storage.mu.RUnlock()

	metadata, exists := cs.storage.chunks[handle]
	if !exists {
		return nil, fmt.Errorf("chunk not found: %s", handle)
	}

	return &ChunkMetadata{
		Handle:       metadata.Handle,
		Version:      metadata.Version,
		Size:         metadata.Size,
		Checksum:     append([]byte(nil), metadata.Checksum...),
		CreatedTime:  metadata.CreatedTime,
		LastAccessed: metadata.LastAccessed,
		LastModified: metadata.LastModified,
		FilePath:     metadata.FilePath,
	}, nil
}

// GetChunkInfoForReplication returns detailed chunk information for replication purposes.
func (cs *ChunkServerService) GetChunkInfoForReplication(handle string) (exists bool, size uint64, version uint32, checksum []byte, err error) {
	if handle == "" {
		return false, 0, 0, nil, fmt.Errorf("chunk handle cannot be empty")
	}

	cs.storage.mu.RLock()
	metadata, exists := cs.storage.chunks[handle]
	cs.storage.mu.RUnlock()

	if !exists {
		return false, 0, 0, nil, nil
	}

	checksumCopy := make([]byte, len(metadata.Checksum))
	copy(checksumCopy, metadata.Checksum)

	return true, metadata.Size, metadata.Version, checksumCopy, nil
}

// ListChunks returns all chunks stored on this server.
func (cs *ChunkServerService) ListChunks() []*ChunkMetadata {
	cs.storage.mu.RLock()
	defer cs.storage.mu.RUnlock()

	chunks := make([]*ChunkMetadata, 0, len(cs.storage.chunks))
	for _, metadata := range cs.storage.chunks {
		chunks = append(chunks, &ChunkMetadata{
			Handle:       metadata.Handle,
			Version:      metadata.Version,
			Size:         metadata.Size,
			Checksum:     append([]byte(nil), metadata.Checksum...),
			CreatedTime:  metadata.CreatedTime,
			LastAccessed: metadata.LastAccessed,
			LastModified: metadata.LastModified,
			FilePath:     metadata.FilePath,
		})
	}

	return chunks
}

// GetServerStats returns server statistics.
func (cs *ChunkServerService) GetServerStats() (totalSpace, usedSpace, availableSpace uint64, chunkCount uint32, err error) {
	chunkCount = uint32(len(cs.storage.chunks))

	cs.storage.mu.RLock()
	for _, metadata := range cs.storage.chunks {
		usedSpace += metadata.Size
	}
	cs.storage.mu.RUnlock()

	totalSpace = 1024 * 1024 * 1024 * 1024 // 1TB
	availableSpace = totalSpace - usedSpace

	return totalSpace, usedSpace, availableSpace, chunkCount, nil
}

// ChunkExists checks if a chunk exists on this server.
func (cs *ChunkServerService) ChunkExists(handle string) bool {
	if handle == "" {
		return false
	}

	cs.storage.mu.RLock()
	_, exists := cs.storage.chunks[handle]
	cs.storage.mu.RUnlock()
	return exists
}

// ReplicateChunkFromSource replicates a chunk from another chunkserver using streaming approach.
func (cs *ChunkServerService) ReplicateChunkFromSource(ctx context.Context, handle, sourceServer string, targetVersion uint32) error {
	if handle == "" {
		return fmt.Errorf("chunk handle cannot be empty")
	}
	if sourceServer == "" {
		return fmt.Errorf("source server cannot be empty")
	}

	conn, err := cs.getChunkServerConnection(sourceServer)
	if err != nil {
		return fmt.Errorf("failed to connect to source server %s: %w", sourceServer, err)
	}

	client := chunkserver.NewChunkServerServiceClient(conn.conn)

	totalSize, err := cs.getSourceChunkInfo(ctx, client, handle)
	if err != nil {
		return fmt.Errorf("failed to get source chunk size: %w", err)
	}

	if err := cs.CreateChunk(handle, targetVersion, totalSize); err != nil {
		return fmt.Errorf("failed to create local chunk: %w", err)
	}

	if err := cs.streamCopyChunk(ctx, client, handle, totalSize); err != nil {
		if delErr := cs.DeleteChunk(handle); delErr != nil {
			log.Printf("Failed to cleanup chunk %s after replication failure: %v", handle, delErr)
		}
		return fmt.Errorf("failed to stream copy chunk: %w", err)
	}

	// Finalize the chunk: sync to disk and compute checksum
	if err := cs.finalizeChunk(handle); err != nil {
		log.Printf("Failed to finalize chunk %s after replication: %v", handle, err)
		// Don't fail the replication for finalization errors, but log them
	}

	return nil
}

// getSourceChunkInfo retrieves chunk information including size from the source server.
func (cs *ChunkServerService) getSourceChunkInfo(ctx context.Context, client chunkserver.ChunkServerServiceClient, handle string) (uint64, error) {
	infoResp, err := client.GetChunkInfo(ctx, &chunkserver.GetChunkInfoRequest{
		ChunkHandle: handle,
	})
	if err == nil && infoResp.Exists {
		return infoResp.Size, nil
	}

	// Fallback to binary search if GetChunkInfo is not available
	const maxChunkSize = 64 * 1024 * 1024 // 64MB max chunk size
	low, high := uint64(0), uint64(maxChunkSize)

	for low < high {
		mid := (low + high + 1) / 2

		_, err := client.ReadChunk(ctx, &chunkserver.ReadChunkRequest{
			ChunkHandle: handle,
			Offset:      mid,
			Length:      1,
		})

		if err != nil || mid == 0 {
			high = mid - 1
		} else {
			low = mid
		}

		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		default:
		}
	}

	return low + 1, nil
}

// streamCopyChunk performs streaming copy with optimal buffer management.
func (cs *ChunkServerService) streamCopyChunk(ctx context.Context, client chunkserver.ChunkServerServiceClient, handle string, totalSize uint64) error {
	bufferSize := uint32(256 * 1024) // Start with 256KB
	if totalSize > 16*1024*1024 {    // If chunk > 16MB, use 1MB buffer
		bufferSize = 1024 * 1024
	} else if totalSize < 64*1024 { // If chunk < 64KB, use smaller buffer
		bufferSize = 32 * 1024
	}

	offset := uint64(0)

	for offset < totalSize {
		remaining := totalSize - offset
		currentReadSize := bufferSize
		if remaining < uint64(bufferSize) {
			currentReadSize = uint32(remaining)
		}

		resp, err := client.ReadChunk(ctx, &chunkserver.ReadChunkRequest{
			ChunkHandle: handle,
			Offset:      offset,
			Length:      currentReadSize,
		})
		if err != nil {
			return fmt.Errorf("failed to read chunk data at offset %d: %w", offset, err)
		}

		if len(resp.Data) == 0 {
			break
		}

		if err := cs.WriteChunk(handle, offset, resp.Data); err != nil {
			return fmt.Errorf("failed to write chunk data at offset %d: %w", offset, err)
		}

		offset += uint64(len(resp.Data))

		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
	}

	return nil
}

// finalizeChunk syncs the chunk to disk and computes its checksum.
// This should be called after all writes to a chunk are complete.
func (cs *ChunkServerService) finalizeChunk(handle string) error {
	cs.storage.mu.RLock()
	metadata, exists := cs.storage.chunks[handle]
	if !exists {
		cs.storage.mu.RUnlock()
		return fmt.Errorf("chunk not found: %s", handle)
	}
	filePath := metadata.FilePath
	cs.storage.mu.RUnlock()

	// Sync the file to disk
	file, err := os.OpenFile(filePath, os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open chunk file for sync: %w", err)
	}
	
	if err := file.Sync(); err != nil {
		file.Close()
		return fmt.Errorf("failed to sync chunk file: %w", err)
	}
	file.Close()

	// Compute and update checksum
	cs.storage.mu.Lock()
	if metadata, exists := cs.storage.chunks[handle]; exists {
		metadata.Checksum = cs.computeChecksum(filePath)
	}
	cs.storage.mu.Unlock()

	return nil
}
