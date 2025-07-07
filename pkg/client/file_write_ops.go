package client

import (
	"context"
	"crypto/rand"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/uttam-li/dfs/api/generated/chunkserver"
	"github.com/uttam-li/dfs/api/generated/common"
	"github.com/uttam-li/dfs/api/generated/master"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	FlushThreshold       = 8 * 1024 * 1024   // 8MB
	MaxBufferSize        = 16 * 1024 * 1024  // 16MB per file handle
	MaxTotalBufferMemory = 128 * 1024 * 1024 // 128MB total across all buffers
)

// generateWriteID creates a unique identifier for a write operation
func generateWriteID() string {
	b := make([]byte, 16)
	rand.Read(b)
	return fmt.Sprintf("write_%x", b)
}

// ChunkWriteJob represents a chunk write operation
type ChunkWriteJob struct {
	ChunkIndex uint64
	Offset     uint64
	Data       []byte
	ChunkSize  uint64
}

// ChunkWriteResult represents the result of a chunk write operation
type ChunkWriteResult struct {
	ChunkIndex uint64
	Written    uint32
	Status     fuse.Status
	ErrorMsg   string
}

// Write handles file write operations following GFS protocol
func (fs *Filesystem) Write(cancel <-chan struct{}, input *fuse.WriteIn, data []byte) (written uint32, code fuse.Status) {
	start := time.Now()

	if fs.config.Debug {
		defer func() {
			log.Printf("[DEBUG] Write: inode=%d fh=%d offset=%d size=%d written=%d duration=%v status=%v",
				input.NodeId, input.Fh, input.Offset, len(data), written, time.Since(start), code)
		}()
	}

	if len(data) == 0 {
		return 0, fuse.OK
	}

	inode, status := fs.getInodeFromFh(input.Fh)
	if status != fuse.OK {
		return 0, status
	}

	cachedInode, status := fs.validateFileOperation(inode, "Write")
	if status != fuse.OK {
		return 0, status
	}

	if !fs.hasWritePermission(cachedInode, input.InHeader.Uid, input.InHeader.Gid) {
		return 0, fuse.EACCES
	}

	return fs.bufferWrite(cancel, input.Fh, inode, data, input.Offset, cachedInode)
}

// bufferWrite implements GFS-style write buffering with immediate return
func (fs *Filesystem) bufferWrite(cancel <-chan struct{}, fh uint64, inode uint64, data []byte, offset uint64, cachedInode *master.Inode) (uint32, fuse.Status) {
	fs.writeBuffersMu.Lock()
	defer fs.writeBuffersMu.Unlock()

	BufferSize := fs.config.BufferSize
	if BufferSize > MaxBufferSize {
		BufferSize = MaxBufferSize
	}

	// Simple memory pressure check
	totalMemory := fs.calculateTotalBufferMemory()
	if totalMemory > MaxTotalBufferMemory {
		fs.flushLargestBufferUnsafe()
	}

	buffer, exists := fs.writeBuffers[fh]
	if !exists {
		buffer = &WriteBuffer{
			FileHandle:  fh,
			NodeId:      inode,
			StartOffset: offset,
			Data:        make([]byte, 0, BufferSize),
			LastWrite:   time.Now(),
			Dirty:       false,
		}
		fs.writeBuffers[fh] = buffer
	}

	buffer.mu.Lock()
	defer buffer.mu.Unlock()

	isNonSequential := (buffer.StartOffset + uint64(len(buffer.Data))) != offset
	wouldExceedBuffer := len(buffer.Data)+len(data) > BufferSize

	if isNonSequential || wouldExceedBuffer {
		// Flush existing buffer if it has data
		if len(buffer.Data) > 0 && buffer.Dirty {
			fs.flushBufferToChunkservers(buffer, cachedInode)
		}

		buffer.Data = buffer.Data[:0]
		buffer.StartOffset = offset
		buffer.Dirty = false
	}

	buffer.Data = append(buffer.Data, data...)
	buffer.LastWrite = time.Now()
	buffer.Dirty = true

	// Check if we should flush based on threshold
	if len(buffer.Data) >= FlushThreshold {
		fs.flushBufferToChunkservers(buffer, cachedInode)
	}

	return uint32(len(data)), fuse.OK
}

func (fs *Filesystem) flushBufferToChunkservers(buffer *WriteBuffer, cachedInode *master.Inode) {
	if buffer == nil || len(buffer.Data) == 0 || !buffer.Dirty {
		return
	}

	bufferCopy := &WriteBuffer{
		FileHandle:  buffer.FileHandle,
		NodeId:      buffer.NodeId,
		StartOffset: buffer.StartOffset,
		Data:        make([]byte, len(buffer.Data)),
		LastWrite:   buffer.LastWrite,
		Dirty:       true,
	}
	copy(bufferCopy.Data, buffer.Data)

	buffer.Data = buffer.Data[:0]
	buffer.Dirty = false

	fs.processBufferWrites(bufferCopy, cachedInode)
}

func (fs *Filesystem) processBufferWrites(buffer *WriteBuffer, cachedInode *master.Inode) {
	chunkSize := uint64(fs.config.ChunkSize)
	var pendingWrites []*PendingWrite

	offset := buffer.StartOffset
	data := buffer.Data

	// Split buffer data into chunk-sized writes
	for len(data) > 0 {
		chunkIndex := offset / chunkSize
		chunkOffset := offset % chunkSize

		bytesToWrite := min(chunkSize-chunkOffset, uint64(len(data)))

		writeID := generateWriteID()
		chunkData := make([]byte, bytesToWrite)
		copy(chunkData, data[:bytesToWrite])

		chunkHandle, primaryAddr, replicaAddrs, version, err := fs.ensureChunkExistsWithRetry(buffer.NodeId, chunkIndex, 2)
		if err != nil {
			log.Printf("[ERROR] Failed to ensure chunk %d exists for inode %d: %v", chunkIndex, buffer.NodeId, err)
			// Continue with next chunk instead of failing entirely
			data = data[bytesToWrite:]
			offset += bytesToWrite
			continue
		}

		pendingWrite := &PendingWrite{
			WriteID:      writeID,
			ChunkHandle:  chunkHandle,
			ChunkIndex:   chunkIndex,
			Offset:       chunkOffset,
			Data:         chunkData,
			ChunkVersion: version,
			PrimaryAddr:  primaryAddr,
			ReplicaAddrs: replicaAddrs,
			Timestamp:    time.Now(),
		}

		pendingWrites = append(pendingWrites, pendingWrite)

		data = data[bytesToWrite:]
		offset += bytesToWrite
	}

	if len(pendingWrites) > 0 {
		fs.executeWriteProtocol(&WriteSerializationJob{
			FileHandle:  buffer.FileHandle,
			NodeID:      buffer.NodeId,
			Writes:      pendingWrites,
			CachedInode: cachedInode,
		})
	}
}

func (fs *Filesystem) ensureChunkExists(inode uint64, chunkIndex uint64) (string, string, []string, uint32, error) {
	ctx, cancel := context.WithTimeout(context.Background(), fs.config.RPCTimeout)
	defer cancel()

	// First check if chunk already exists
	resp, err := fs.masterClient.GetAttributes(ctx, &master.GetAttributesRequest{
		Inode: inode,
	})
	if err != nil {
		return "", "", nil, 0, fmt.Errorf("failed to get file attributes: %w", err)
	}

	if resp.Inode == nil {
		return "", "", nil, 0, fmt.Errorf("file not found")
	}

	var chunkHandle string

	if int(chunkIndex) >= len(resp.Inode.ChunkHandles) || resp.Inode.ChunkHandles[chunkIndex] == "" {
		// Allocate new chunk with lease
		handle, _, err := fs.allocateChunkWithLease(nil, inode, chunkIndex)
		if err != nil {
			return "", "", nil, 0, fmt.Errorf("failed to allocate chunk: %w", err)
		}
		chunkHandle = handle
	} else {
		chunkHandle = resp.Inode.ChunkHandles[chunkIndex]
	}

	// Get chunk locations and lease information
	chunkResp, err := fs.masterClient.GetChunkLocations(ctx, &master.GetChunkLocationsRequest{
		ChunkHandles: []string{chunkHandle},
	})
	if err != nil {
		return "", "", nil, 0, fmt.Errorf("failed to get chunk locations: %w", err)
	}

	if len(chunkResp.Chunks) == 0 || len(chunkResp.Chunks[0].Locations) == 0 {
		return "", "", nil, 0, fmt.Errorf("no chunk locations found for %s", chunkHandle)
	}

	chunkInfo := chunkResp.Chunks[0]

	leaseResp, err := fs.masterClient.GetLeaseInfo(ctx, &master.GetLeaseInfoRequest{
		ChunkHandle: chunkHandle,
	})
	if err != nil {
		return "", "", nil, 0, fmt.Errorf("failed to get lease info: %w", err)
	}

	primaryAddr := leaseResp.Primary
	if primaryAddr == "" {
		return "", "", nil, 0, fmt.Errorf("no primary found for chunk %s", chunkHandle)
	}

	// Collect replica addresses (excluding primary)
	var replicaAddrs []string
	for _, location := range chunkInfo.Locations {
		if location.Address != primaryAddr {
			replicaAddrs = append(replicaAddrs, location.Address)
		}
	}

	return chunkHandle, primaryAddr, replicaAddrs, chunkInfo.Version, nil
}

func (fs *Filesystem) ensureChunkExistsWithRetry(inode uint64, chunkIndex uint64, maxRetries int) (string, string, []string, uint32, error) {
	var lastErr error

	for attempt := 0; attempt <= maxRetries; attempt++ {
		if attempt > 0 {
			time.Sleep(time.Duration(attempt) * time.Second) // Exponential backoff
		}

		handle, primary, replicas, version, err := fs.ensureChunkExists(inode, chunkIndex)
		if err == nil {
			return handle, primary, replicas, version, nil
		}

		lastErr = err
	}

	return "", "", nil, 0, fmt.Errorf("chunk allocation failed after %d attempts: %w", maxRetries+1, lastErr)
}

func (fs *Filesystem) executeWriteProtocol(job *WriteSerializationJob) {
	if len(job.Writes) == 0 {
		return
	}

	// Phase 1: Data Flow - Send data to all replicas (primary + secondaries)
	dataFlowSuccess := fs.executeDataFlowPhase(job.Writes)
	if !dataFlowSuccess {
		log.Printf("[WARN] Data flow phase failed - proceeding anyway")
	}

	// Phase 2: Control Flow - Send write commands to primary only
	controlFlowSuccess := fs.executeControlFlowPhase(job.Writes)
	if !controlFlowSuccess {
		log.Printf("[ERROR] Control flow phase failed")
		return
	}

	// Phase 3: Update metadata after successful writes
	if controlFlowSuccess {
		fs.updateMetadataAfterWrite(job)
	}
}

func (fs *Filesystem) executeDataFlowPhase(writes []*PendingWrite) bool {
	var wg sync.WaitGroup
	successCount := int64(0)
	totalReplicas := int64(0)

	for _, write := range writes {
		allReplicas := append([]string{write.PrimaryAddr}, write.ReplicaAddrs...)
		totalReplicas += int64(len(allReplicas))

		// Send to all replicas in parallel
		for _, replicaAddr := range allReplicas {
			wg.Add(1)
			go func(addr string, w *PendingWrite) {
				defer wg.Done()
				if fs.sendDataToReplica(addr, w) {
					atomic.AddInt64(&successCount, 1)
				}
			}(replicaAddr, write)
		}
	}

	wg.Wait()

	successRate := float64(successCount) / float64(totalReplicas)
	success := successRate >= 0.3

	if !success {
		log.Printf("[WARN] Data flow failed - insufficient replicas succeeded")
		log.Printf("[WARN] This may indicate chunkservers don't implement BufferData RPC yet")
	}

	return success
}

func (fs *Filesystem) sendDataToReplica(address string, write *PendingWrite) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, err := grpc.NewClient(address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallSendMsgSize(128*1024*1024), // 128MB
			grpc.MaxCallRecvMsgSize(128*1024*1024), // 128MB
		),
	)
	if err != nil {
		return false
	}
	defer conn.Close()

	client := chunkserver.NewChunkServerServiceClient(conn)

	req := &chunkserver.BufferDataRequest{
		WriteId:     write.WriteID,
		ChunkHandle: write.ChunkHandle,
		Offset:      write.Offset,
		Data:        write.Data,
		Version:     write.ChunkVersion,
	}

	resp, err := client.BufferData(ctx, req)
	if err != nil {
		return false
	}

	if !resp.Success {
		return false
	}

	return true
}

func (fs *Filesystem) executeControlFlowPhase(writes []*PendingWrite) bool {
	chunkWrites := make(map[string][]*PendingWrite)
	for _, write := range writes {
		key := write.ChunkHandle + "|" + write.PrimaryAddr
		chunkWrites[key] = append(chunkWrites[key], write)
	}

	successCount := 0
	for key, chunkWriteList := range chunkWrites {
		parts := splitString(key, "|")
		if len(parts) != 2 {
			continue
		}
		chunkHandle := parts[0]
		primaryAddr := parts[1]

		if fs.sendSerializeCommandToPrimary(primaryAddr, chunkHandle, chunkWriteList) {
			successCount++
		}
	}

	success := successCount == len(chunkWrites)
	return success
}

func (fs *Filesystem) sendSerializeCommandToPrimary(primaryAddr string, chunkHandle string, writes []*PendingWrite) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	conn, err := grpc.NewClient(primaryAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallSendMsgSize(128*1024*1024), // 128MB
			grpc.MaxCallRecvMsgSize(128*1024*1024), // 128MB
		),
	)
	if err != nil {
		return false
	}
	defer conn.Close()

	client := chunkserver.NewChunkServerServiceClient(conn)

	// Collect write IDs and replica addresses
	var writeIDs []string
	var replicaAddrs []string
	for _, write := range writes {
		writeIDs = append(writeIDs, write.WriteID)
		for _, addr := range write.ReplicaAddrs {
			if !contains(replicaAddrs, addr) {
				replicaAddrs = append(replicaAddrs, addr)
			}
		}
	}

	req := &chunkserver.SerializeWritesRequest{
		ChunkHandle:    chunkHandle,
		WriteIds:       writeIDs,
		ReplicaServers: replicaAddrs,
	}

	resp, err := client.SerializeWrites(ctx, req)
	if err != nil {
		return false
	}

	if !resp.Success {
		return false
	}

	return true
}

// updateMetadataAfterWrite updates file size metadata after successful writes
func (fs *Filesystem) updateMetadataAfterWrite(job *WriteSerializationJob) {
	if len(job.Writes) == 0 {
		return
	}

	maxOffset := uint64(0)
	for _, write := range job.Writes {
		writeEnd := write.Offset + uint64(len(write.Data)) + write.ChunkIndex*uint64(fs.config.ChunkSize)
		if writeEnd > maxOffset {
			maxOffset = writeEnd
		}
	}

	// Update file size if it grew
	if maxOffset > job.CachedInode.Attributes.Size {
		err := fs.updateFileSize(nil, job.NodeID, maxOffset, job.CachedInode)
		if err != nil {
			log.Printf("[WARN] Failed to update file size: %v", err)
		}

		fs.cache.Invalidate(job.NodeID)
	}
}

// updateFileSize updates the file size metadata in the master server
func (fs *Filesystem) updateFileSize(cancel <-chan struct{}, inode uint64, newSize uint64, cachedInode *master.Inode) error {
	ctx, cancelCtx := context.WithTimeout(context.Background(), fs.config.RPCTimeout)
	defer cancelCtx()

	attrs := &common.FileAttributes{
		Size: newSize,
	}

	// Only update size attribute (bit 3 for SIZE according to FUSE)
	validMask := uint32(1 << 3)

	_, err := fs.masterClient.SetAttributes(ctx, &master.SetAttributesRequest{
		Ino:           inode,
		Attributes:    attrs,
		AttributeMask: validMask,
	})

	return err
}

// Helper functions
func splitString(s, sep string) []string {
	var result []string
	start := 0
	for i := 0; i <= len(s)-len(sep); i++ {
		if s[i:i+len(sep)] == sep {
			result = append(result, s[start:i])
			start = i + len(sep)
			i += len(sep) - 1
		}
	}
	result = append(result, s[start:])
	return result
}

func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

func (fs *Filesystem) flushBuffer(cancel <-chan struct{}, buffer *WriteBuffer, cachedInode *master.Inode) fuse.Status {
	if buffer == nil || len(buffer.Data) == 0 || !buffer.Dirty {
		return fuse.OK
	}

	fs.flushBufferToChunkservers(buffer, cachedInode)

	return fuse.OK
}

// calculateTotalBufferMemory calculates total memory used by all write buffers
func (fs *Filesystem) calculateTotalBufferMemory() int {
	total := 0
	for _, buffer := range fs.writeBuffers {
		if buffer != nil {
			total += len(buffer.Data)
		}
	}
	return total
}

// flushLargestBufferUnsafe flushes the largest buffer to free memory
// WARNING: Must be called with writeBuffersMu already locked
func (fs *Filesystem) flushLargestBufferUnsafe() {
	if len(fs.writeBuffers) == 0 {
		return
	}

	var largestBuffer *WriteBuffer
	maxSize := 0

	for _, buffer := range fs.writeBuffers {
		if buffer != nil && len(buffer.Data) > maxSize && buffer.Dirty {
			maxSize = len(buffer.Data)
			largestBuffer = buffer
		}
	}

	if largestBuffer != nil && maxSize > 0 {
		cachedInode, status := fs.validateFileOperation(largestBuffer.NodeId, "MemoryFlush")
		if status == fuse.OK {
			fs.flushBufferToChunkservers(largestBuffer, cachedInode)
		}
	}
}
