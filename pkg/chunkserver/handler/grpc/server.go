package grpc

import (
	"context"
	"log"
	"time"

	pb "github.com/uttam-li/dfs/api/generated/chunkserver"
	"github.com/uttam-li/dfs/pkg/chunkserver/service"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ChunkServerGRPCHandler handles gRPC requests for chunk server operations
type ChunkServerGRPCHandler struct {
	pb.UnimplementedChunkServerServiceServer
	service *service.ChunkServerService
}

// NewChunkServerGRPCHandler creates and registers a new gRPC handler for the chunk server service
func NewChunkServerGRPCHandler(service *service.ChunkServerService, grpc *grpc.Server) {
	grpcHandler := &ChunkServerGRPCHandler{
		service: service,
	}

	pb.RegisterChunkServerServiceServer(grpc, grpcHandler)
}

// ReadChunk reads data from a chunk
func (h *ChunkServerGRPCHandler) ReadChunk(ctx context.Context, req *pb.ReadChunkRequest) (*pb.ReadChunkResponse, error) {
	if req == nil || req.ChunkHandle == "" {
		return nil, status.Errorf(codes.InvalidArgument, "invalid request")
	}

	if !h.service.IsRunning() {
		return nil, status.Errorf(codes.Unavailable, "server unavailable")
	}

	data, err := h.service.ReadChunk(req.ChunkHandle, req.Offset, req.Length)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "read failed")
	}

	return &pb.ReadChunkResponse{
		Data:         data,
		IsFinalChunk: true,
	}, nil
}

// CreateChunk creates a new chunk
func (h *ChunkServerGRPCHandler) CreateChunk(ctx context.Context, req *pb.CreateChunkRequest) (*pb.CreateChunkResponse, error) {
	if req == nil || req.ChunkHandle == "" {
		return nil, status.Errorf(codes.InvalidArgument, "invalid request")
	}

	if !h.service.IsRunning() {
		return nil, status.Errorf(codes.Unavailable, "server unavailable")
	}

	err := h.service.CreateChunk(req.ChunkHandle, req.Version, req.InitialSize)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "creation failed")
	}

	return &pb.CreateChunkResponse{
		AllocatedSize: req.InitialSize,
	}, nil
}

// DeleteChunks deletes multiple chunks and returns deletion results
func (h *ChunkServerGRPCHandler) DeleteChunks(ctx context.Context, req *pb.DeleteChunksRequest) (*pb.DeleteChunksResponse, error) {
	if req == nil {
		return nil, status.Errorf(codes.InvalidArgument, "request cannot be nil")
	}

	if !h.service.IsRunning() {
		return nil, status.Errorf(codes.Unavailable, "chunk server is not running")
	}

	results := make([]*pb.DeleteChunksResponse_DeleteResult, 0, len(req.ChunkHandles))

	for _, handle := range req.ChunkHandles {
		result := &pb.DeleteChunksResponse_DeleteResult{
			ChunkHandle: handle,
			FreedSpace:  0,
		}

		if chunkInfo, err := h.service.GetChunkInfo(handle); err == nil {
			result.FreedSpace = chunkInfo.Size
		}

		if err := h.service.DeleteChunk(handle); err != nil {
			log.Printf("Failed to delete chunk %s: %v", handle, err)
			result.FreedSpace = 0
		}

		results = append(results, result)
	}

	return &pb.DeleteChunksResponse{Results: results}, nil
}

func (h *ChunkServerGRPCHandler) ReplicateChunk(ctx context.Context, req *pb.ReplicateChunkRequest) (*pb.ReplicateChunkResponse, error) {
	if req == nil {
		return nil, status.Errorf(codes.InvalidArgument, "request cannot be nil")
	}

	if req.ChunkHandle == "" {
		return nil, status.Errorf(codes.InvalidArgument, "chunk handle cannot be empty")
	}

	if req.SourceServer == "" {
		return nil, status.Errorf(codes.InvalidArgument, "source server cannot be empty")
	}

	if !h.service.IsRunning() {
		return nil, status.Errorf(codes.Unavailable, "chunk server is not running")
	}

	log.Printf("[INFO] Starting replication of chunk %s from server %s (target version %d)",
		req.ChunkHandle, req.SourceServer, req.TargetVersion)

	// Check if chunk already exists locally
	if h.service.ChunkExists(req.ChunkHandle) {
		chunkInfo, err := h.service.GetChunkInfo(req.ChunkHandle)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "failed to get existing chunk info: %v", err)
		}

		return &pb.ReplicateChunkResponse{
			ChunkSize: chunkInfo.Size,
			Checksum:  chunkInfo.Checksum,
		}, nil
	}

	err := h.service.ReplicateChunkFromSource(ctx, req.ChunkHandle, req.SourceServer, req.TargetVersion)
	if err != nil {
		log.Printf("Failed to replicate chunk %s from %s: %v", req.ChunkHandle, req.SourceServer, err)
		return nil, status.Errorf(codes.Internal, "replication failed: %v", err)
	}

	chunkInfo, err := h.service.GetChunkInfo(req.ChunkHandle)
	if err != nil {
		log.Printf("Failed to get replicated chunk info for %s: %v", req.ChunkHandle, err)
		return nil, status.Errorf(codes.Internal, "failed to get replicated chunk info: %v", err)
	}

	log.Printf("[INFO] Successfully replicated chunk %s from server %s (size: %d bytes)",
		req.ChunkHandle, req.SourceServer, chunkInfo.Size)

	return &pb.ReplicateChunkResponse{
		ChunkSize: chunkInfo.Size,
		Checksum:  chunkInfo.Checksum,
	}, nil
}

func (h *ChunkServerGRPCHandler) HealthCheck(ctx context.Context, req *pb.HealthCheckRequest) (*pb.HealthCheckResponse, error) {
	isHealthy := h.service.IsRunning()

	var issues []string
	if !isHealthy {
		issues = append(issues, "service is not running")
	}

	// Create health information
	health := &pb.ServerHealth{
		CpuUsage:          10.5,                                                    // Mock data
		MemoryUsage:       25.0,                                                    // Mock data
		DiskUsage:         30.0,                                                    // Mock data
		ActiveConnections: 5,                                                       // Mock data
		UptimeSeconds:     int64(time.Since(time.Now().Add(-time.Hour)).Seconds()), // Mock uptime
	}

	response := &pb.HealthCheckResponse{
		IsHealthy: isHealthy,
		Issues:    issues,
		Health:    health,
	}

	return response, nil
}

func (h *ChunkServerGRPCHandler) BufferData(ctx context.Context, req *pb.BufferDataRequest) (*pb.BufferDataResponse, error) {
	if req == nil {
		return nil, status.Errorf(codes.InvalidArgument, "request cannot be nil")
	}

	if req.WriteId == "" || req.ChunkHandle == "" {
		return nil, status.Errorf(codes.InvalidArgument, "write_id and chunk_handle cannot be empty")
	}

	if !h.service.IsRunning() {
		return nil, status.Errorf(codes.Unavailable, "chunk server is not running")
	}

	err := h.service.BufferData(req.WriteId, req.ChunkHandle, req.Offset, req.Data, req.Version)
	if err != nil {
		log.Printf("[ERROR] BufferData failed: %v", err)
		return &pb.BufferDataResponse{
			Success:      false,
			ErrorMessage: err.Error(),
		}, nil
	}

	return &pb.BufferDataResponse{
		Success: true,
	}, nil
}

func (h *ChunkServerGRPCHandler) SerializeWrites(ctx context.Context, req *pb.SerializeWritesRequest) (*pb.SerializeWritesResponse, error) {
	if req == nil {
		return nil, status.Errorf(codes.InvalidArgument, "request cannot be nil")
	}

	if req.ChunkHandle == "" || len(req.WriteIds) == 0 {
		return nil, status.Errorf(codes.InvalidArgument, "chunk_handle and write_ids cannot be empty")
	}

	if !h.service.IsRunning() {
		return nil, status.Errorf(codes.Unavailable, "chunk server is not running")
	}

	err := h.service.SerializeWrites(req.ChunkHandle, req.WriteIds, req.ReplicaServers)
	if err != nil {
		log.Printf("[ERROR] SerializeWrites failed: %v", err)
		return &pb.SerializeWritesResponse{
			Success:      false,
			ErrorMessage: err.Error(),
		}, nil
	}

	return &pb.SerializeWritesResponse{
		Success:       true,
		WritesApplied: uint32(len(req.WriteIds)),
	}, nil
}

func (h *ChunkServerGRPCHandler) ApplyWrites(ctx context.Context, req *pb.ApplyWritesRequest) (*pb.ApplyWritesResponse, error) {
	if req == nil {
		return nil, status.Errorf(codes.InvalidArgument, "request cannot be nil")
	}

	if req.ChunkHandle == "" || len(req.WriteOperations) == 0 {
		return nil, status.Errorf(codes.InvalidArgument, "chunk_handle and write_operations cannot be empty")
	}

	if !h.service.IsRunning() {
		return nil, status.Errorf(codes.Unavailable, "chunk server is not running")
	}

	// Convert protobuf writes to service writes
	writes := make([]service.WriteOperation, len(req.WriteOperations))
	for i, op := range req.WriteOperations {
		writes[i] = service.WriteOperation{
			Offset: op.Offset,
			Data:   op.Data,
		}
	}

	err := h.service.ApplyWrites(req.ChunkHandle, writes)
	if err != nil {
		log.Printf("[ERROR] ApplyWrites failed: %v", err)
		return &pb.ApplyWritesResponse{
			Success:      false,
			ErrorMessage: err.Error(),
		}, nil
	}

	// Get updated chunk info
	chunkInfo, err := h.service.GetChunkInfo(req.ChunkHandle)
	if err != nil {
		log.Printf("[WARN] Failed to get chunk info after apply writes: %v", err)
	}

	response := &pb.ApplyWritesResponse{
		Success: true,
	}

	if chunkInfo != nil {
		response.NewChunkSize = chunkInfo.Size
		response.Checksum = chunkInfo.Checksum
	}

	return response, nil
}

// GetChunkInfo returns information about a chunk.
func (h *ChunkServerGRPCHandler) GetChunkInfo(ctx context.Context, req *pb.GetChunkInfoRequest) (*pb.GetChunkInfoResponse, error) {
	if req == nil || req.ChunkHandle == "" {
		return nil, status.Errorf(codes.InvalidArgument, "invalid request: chunk handle cannot be empty")
	}

	if !h.service.IsRunning() {
		return nil, status.Errorf(codes.Unavailable, "chunk server is not running")
	}

	// Check if chunk exists and get its info
	exists, size, version, checksum, err := h.service.GetChunkInfoForReplication(req.ChunkHandle)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get chunk info: %v", err)
	}

	response := &pb.GetChunkInfoResponse{
		Exists:   exists,
		Size:     size,
		Version:  version,
		Checksum: checksum,
	}

	// Add timestamps if chunk exists
	if exists {
		if chunkInfo, err := h.service.GetChunkInfo(req.ChunkHandle); err == nil {
			response.CreatedTime = chunkInfo.CreatedTime.Unix()
			response.LastModified = chunkInfo.LastModified.Unix()
		}
	}

	return response, nil
}
