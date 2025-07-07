package client

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"
	chunkserver "github.com/uttam-li/dfs/api/generated/chunkserver"
	"github.com/uttam-li/dfs/api/generated/common"
	"github.com/uttam-li/dfs/api/generated/master"
)

// chunkRead represents a single chunk read operation.
type chunkRead struct {
	index  uint64 // chunk index
	offset uint64 // offset within this chunk
	length uint32 // bytes to read from this chunk
	bufPos uint64 // position in output buffer
}

// Read reads data from a file. It ensures read-after-write consistency by
// checking write buffers first. For reads not in the buffer, it fetches data
// from chunkservers, handling both single and concurrent multi-chunk reads
// with replica failover.
func (fs *Filesystem) Read(cancel <-chan struct{}, input *fuse.ReadIn, buf []byte) (fuse.ReadResult, fuse.Status) {
	ino, status := fs.getInodeFromFh(input.Fh)
	if status != fuse.OK {
		return nil, status
	}

	// Check write buffers for read-after-write consistency.
	if data, found := fs.checkWriteBuffers(ino, input.Offset, input.Size); found {
		return fuse.ReadResultData(data), fuse.OK
	}

	if _, status = fs.validateFileOperation(ino, "Read"); status != fuse.OK {
		return nil, status
	}

	return fs.readChunks(cancel, ino, input.Offset, input.Size, buf)
}

// readChunks plans and executes read operations across one or more chunks.
// It calculates which chunks are needed and dispatches to specialized functions
// for single or multiple chunk reads.
func (fs *Filesystem) readChunks(cancel <-chan struct{}, ino uint64, offset uint64, size uint32, buf []byte) (fuse.ReadResult, fuse.Status) {
	chunkSize := uint64(fs.config.ChunkSize)
	startChunk := offset / chunkSize
	endChunk := (offset + uint64(size) - 1) / chunkSize

	var reads []chunkRead
	currentOffset := offset
	bytesRemaining := uint64(size)

	for chunkIndex := startChunk; chunkIndex <= endChunk && bytesRemaining > 0; chunkIndex++ {
		chunkOffset := currentOffset % chunkSize
		bytesToRead := bytesRemaining
		if bytesToRead > chunkSize-chunkOffset {
			bytesToRead = chunkSize - chunkOffset
		}

		reads = append(reads, chunkRead{
			index:  chunkIndex,
			offset: chunkOffset,
			length: uint32(bytesToRead),
			bufPos: currentOffset - offset,
		})

		currentOffset += bytesToRead
		bytesRemaining -= bytesToRead
	}

	// Optimize for the common case of reading from a single chunk.
	if len(reads) == 1 {
		return fs.readSingleChunk(cancel, ino, reads[0], buf)
	}

	return fs.readMultipleChunks(cancel, ino, reads, buf)
}

// readSingleChunk reads data from a single file chunk. It fetches chunk
// metadata, handles sparse chunks, and reads from an available replica.
func (fs *Filesystem) readSingleChunk(cancel <-chan struct{}, ino uint64, read chunkRead, buf []byte) (fuse.ReadResult, fuse.Status) {
	locations, handle, status := fs.getChunkInfo(ino, read.index)
	if status != fuse.OK {
		return nil, status
	}

	// A chunk with no locations is sparse; return zeros.
	if len(locations) == 0 {
		for i := uint32(0); i < read.length; i++ {
			buf[i] = 0
		}
		return fuse.ReadResultData(buf[:read.length]), fuse.OK
	}

	data, err := fs.readFromReplicas(cancel, handle, read.offset, read.length, locations)
	if err != nil {
		return nil, fuse.EIO
	}

	copy(buf, data)
	return fuse.ReadResultData(buf[:len(data)]), fuse.OK
}

// readMultipleChunks reads data from multiple file chunks concurrently. It
// launches a goroutine for each chunk read, collects the results, and assembles
// them into the final buffer.
func (fs *Filesystem) readMultipleChunks(cancel <-chan struct{}, ino uint64, reads []chunkRead, buf []byte) (fuse.ReadResult, fuse.Status) {
	type result struct {
		read chunkRead
		data []byte
		err  error
	}

	resultChan := make(chan result, len(reads))
	var wg sync.WaitGroup

	for _, read := range reads {
		wg.Add(1)
		go func(r chunkRead) {
			defer wg.Done()

			locations, handle, status := fs.getChunkInfo(ino, r.index)
			if status != fuse.OK {
				resultChan <- result{read: r, err: fmt.Errorf("getChunkInfo failed for chunk %d: %v", r.index, status)}
				return
			}

			// A chunk with no locations is sparse; return a zero-filled buffer.
			if len(locations) == 0 {
				data := make([]byte, r.length)
				resultChan <- result{read: r, data: data}
				return
			}

			data, err := fs.readFromReplicas(cancel, handle, r.offset, r.length, locations)
			if err != nil {
				err = fmt.Errorf("readFromReplicas failed for chunk %d: %w", r.index, err)
			}
			resultChan <- result{read: r, data: data, err: err}
		}(read)
	}

	go func() {
		wg.Wait()
		close(resultChan)
	}()

	totalRead := 0
	for res := range resultChan {
		if res.err != nil {
			return nil, fuse.EIO
		}
		copy(buf[res.read.bufPos:], res.data)
		totalRead += len(res.data)
	}

	return fuse.ReadResultData(buf[:totalRead]), fuse.OK
}

// readFromReplicas attempts to read data from one of the chunk's replicas,
// trying each location in order until one succeeds. It uses a short timeout
// and handles cancellation.
func (fs *Filesystem) readFromReplicas(cancel <-chan struct{}, handle string, offset uint64, length uint32, locations []*common.ChunkLocation) ([]byte, error) {
	ctx, cancelCtx := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancelCtx()

	var lastErr error
	for _, location := range locations {
		select {
		case <-cancel:
			return nil, fmt.Errorf("read operation cancelled")
		case <-ctx.Done():
			return nil, fmt.Errorf("read timed out: %w", lastErr)
		default:
		}

		conn, err := fs.connectionPool.GetConnection(location.Address)
		if err != nil {
			lastErr = fmt.Errorf("failed to connect to %s: %w", location.Address, err)
			continue // Try next replica.
		}

		resp, err := conn.client.ReadChunk(ctx, &chunkserver.ReadChunkRequest{
			ChunkHandle: handle,
			Offset:      offset,
			Length:      length,
		})
		fs.connectionPool.ReleaseConnection(conn)

		if err != nil {
			lastErr = fmt.Errorf("failed to read from %s: %w", location.Address, err)
			continue // Fast failover to next replica.
		}

		if resp != nil && len(resp.Data) > 0 {
			// Ensure we don't return more data than requested.
			if uint32(len(resp.Data)) > length {
				return resp.Data[:length], nil
			}
			return resp.Data, nil
		}
	}

	if lastErr != nil {
		return nil, fmt.Errorf("failed to read from any replica: %w", lastErr)
	}
	return nil, fmt.Errorf("failed to read from any replica (all replicas returned empty data)")
}

// checkWriteBuffers checks for pending writes that overlap with a read request
// to ensure read-after-write consistency.
func (fs *Filesystem) checkWriteBuffers(fileIno uint64, readOffset uint64, readSize uint32) ([]byte, bool) {
	fs.writeBuffersMu.RLock()
	defer fs.writeBuffersMu.RUnlock()

	readEnd := readOffset + uint64(readSize)

	for _, buffer := range fs.writeBuffers {
		if buffer.NodeId != fileIno {
			continue
		}

		buffer.mu.Lock()
		bufferEnd := buffer.StartOffset + uint64(len(buffer.Data))

		// Check if the write buffer completely covers the read request.
		if buffer.StartOffset <= readOffset && bufferEnd >= readEnd {
			startIdx := readOffset - buffer.StartOffset
			endIdx := startIdx + uint64(readSize)
			if endIdx <= uint64(len(buffer.Data)) {
				result := make([]byte, readSize)
				copy(result, buffer.Data[startIdx:endIdx])
				buffer.mu.Unlock()
				return result, true
			}
		}
		buffer.mu.Unlock()
	}

	return nil, false
}

// getChunkInfo retrieves chunk locations and a handle from the master.
// It returns an empty location slice for sparse chunks.
func (fs *Filesystem) getChunkInfo(ino uint64, chunkIndex uint64) ([]*common.ChunkLocation, string, fuse.Status) {
	ctx, cancelCtx := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelCtx()

	resp, err := fs.masterClient.GetAttributes(ctx, &master.GetAttributesRequest{
		Inode: ino,
	})
	if err != nil {
		return nil, "", grpcToFuseStatus(err)
	}

	// If the chunk index is out of bounds, it's a sparse chunk.
	if resp.Inode == nil || int(chunkIndex) >= len(resp.Inode.ChunkHandles) {
		return nil, "", fuse.OK
	}

	handle := resp.Inode.ChunkHandles[chunkIndex]
	// An empty handle also signifies a sparse chunk.
	if handle == "" {
		return nil, "", fuse.OK
	}

	chunkResp, err := fs.masterClient.GetChunkLocations(ctx, &master.GetChunkLocationsRequest{
		ChunkHandles: []string{handle},
	})
	if err != nil {
		return nil, "", grpcToFuseStatus(err)
	}

	if len(chunkResp.Chunks) == 0 {
		return nil, handle, fuse.OK
	}

	return chunkResp.Chunks[0].Locations, handle, fuse.OK
}
