package grpc

import (
	"context"
	"fmt"
	"log"

	"github.com/uttam-li/dfs/api/generated/master"
	commonTypes "github.com/uttam-li/dfs/pkg/common"
	"github.com/uttam-li/dfs/pkg/master/service"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// MasterGRPCHandler handles gRPC requests for the master service
type MasterGRPCHandler struct {
	master.UnimplementedMasterServiceServer
	service *service.MasterService
}

// validateRequest performs common request validation
func (h *MasterGRPCHandler) validateRequest(req interface{}) error {
	if req == nil {
		return status.Errorf(codes.InvalidArgument, "request cannot be nil")
	}
	return nil
}

// validateServiceRunning checks if the master service is running
func (h *MasterGRPCHandler) validateServiceRunning() error {
	if !h.service.IsRunning() {
		return status.Errorf(codes.Unavailable, "master service is not running")
	}
	return nil
}

// handleContextError converts context errors to appropriate gRPC status codes
func handleContextError(ctx context.Context, err error) error {
	if ctx.Err() == context.Canceled {
		return status.Errorf(codes.Canceled, "request cancelled")
	}
	if ctx.Err() == context.DeadlineExceeded {
		return status.Errorf(codes.DeadlineExceeded, "request timeout")
	}
	return err
}

// NewMasterGRPCHandler creates and registers a new gRPC handler for the master service
func NewMasterGRPCHandler(grpc *grpc.Server, masterService *service.MasterService) {
	grpcHandler := &MasterGRPCHandler{
		service: masterService,
	}

	master.RegisterMasterServiceServer(grpc, grpcHandler)
}

// Lookup finds an entry in a directory
func (h *MasterGRPCHandler) Lookup(ctx context.Context, req *master.LookupRequest) (*master.LookupResponse, error) {
	if err := h.validateRequest(req); err != nil {
		return nil, err
	}

	if req.Name == "" {
		return nil, status.Errorf(codes.InvalidArgument, "name cannot be empty")
	}

	if err := h.validateServiceRunning(); err != nil {
		return nil, err
	}

	inode, err := h.service.Lookup(req.ParentIno, req.Name)
	if err != nil {
		if handleErr := handleContextError(ctx, err); handleErr != err {
			return nil, handleErr
		}
		return nil, status.Errorf(codes.NotFound, "entry not found: %s", req.Name)
	}

	return &master.LookupResponse{
		Inode: convertInoForGrpc(inode),
	}, nil
}

// GetAttributes retrieves file attributes for the specified inode
func (h *MasterGRPCHandler) GetAttributes(ctx context.Context, req *master.GetAttributesRequest) (*master.GetAttributesResponse, error) {
	if err := h.validateRequest(req); err != nil {
		return nil, err
	}

	if req.Inode == 0 {
		return nil, status.Errorf(codes.InvalidArgument, "inode cannot be zero")
	}

	if err := h.validateServiceRunning(); err != nil {
		return nil, err
	}

	inode, err := h.service.GetAttributes(req.Inode, req.AttributeMask)
	if err != nil {
		if handleErr := handleContextError(ctx, err); handleErr != err {
			return nil, handleErr
		}
		return nil, status.Errorf(codes.NotFound, "inode not found: %d", req.Inode)
	}

	return &master.GetAttributesResponse{
		Inode: convertInoForGrpc(inode),
	}, nil
}

func (h *MasterGRPCHandler) SetAttributes(ctx context.Context, req *master.SetAttributesRequest) (*master.SetAttributesResponse, error) {
	if err := h.validateRequest(req); err != nil {
		return nil, err
	}

	if req.Ino == 0 {
		return nil, status.Errorf(codes.InvalidArgument, "inode cannot be zero")
	}

	if req.Attributes == nil {
		return nil, status.Errorf(codes.InvalidArgument, "attributes cannot be nil")
	}

	if err := h.validateServiceRunning(); err != nil {
		return nil, err
	}

	newAttrs, err := h.service.SetAttributes(req.Ino, req.Attributes, req.AttributeMask)
	if err != nil {
		if handleErr := handleContextError(ctx, err); handleErr != err {
			return nil, handleErr
		}
		return nil, status.Errorf(codes.NotFound, "inode not found: %d", req.Ino)
	}

	return &master.SetAttributesResponse{
		NewAttributes: newAttrs,
	}, nil
}

func (h *MasterGRPCHandler) CreateEntry(ctx context.Context, req *master.CreateEntryRequest) (*master.CreateEntryResponse, error) {
	if err := h.validateRequest(req); err != nil {
		return nil, err
	}

	if req.ParentIno == 0 {
		return nil, status.Errorf(codes.InvalidArgument, "parent inode cannot be zero")
	}

	if req.Name == "" {
		return nil, status.Errorf(codes.InvalidArgument, "name cannot be empty")
	}

	if req.Attributes == nil {
		return nil, status.Errorf(codes.InvalidArgument, "attributes cannot be nil")
	}

	if err := h.validateServiceRunning(); err != nil {
		return nil, err
	}

	inode, err := h.service.CreateEntry(req.ParentIno, req.Name, uint32(req.Type), req.Attributes, req.SymlinkTarget)
	if err != nil {
		if handleErr := handleContextError(ctx, err); handleErr != err {
			return nil, handleErr
		}
		return nil, status.Errorf(codes.AlreadyExists, "entry already exists: %s", req.Name)
	}

	return &master.CreateEntryResponse{
		Inode: convertInoForGrpc(inode),
	}, nil
}

func (h *MasterGRPCHandler) DeleteEntry(ctx context.Context, req *master.DeleteEntryRequest) (*master.DeleteEntryResponse, error) {
	if err := h.validateRequest(req); err != nil {
		return nil, err
	}

	if req.ParentIno == 0 {
		return nil, status.Errorf(codes.InvalidArgument, "parent inode cannot be zero")
	}

	if req.Name == "" {
		return nil, status.Errorf(codes.InvalidArgument, "name cannot be empty")
	}

	if err := h.validateServiceRunning(); err != nil {
		return nil, err
	}

	deletedIno, freedChunks, err := h.service.DeleteEntry(req.ParentIno, req.Name, req.Recursive)
	if err != nil {
		if handleErr := handleContextError(ctx, err); handleErr != err {
			return nil, handleErr
		}
		return nil, status.Errorf(codes.NotFound, "entry not found: %s", req.Name)
	}

	return &master.DeleteEntryResponse{
		DeletedIno:  deletedIno,
		FreedChunks: freedChunks,
	}, nil
}

func (h *MasterGRPCHandler) ListDirectory(ctx context.Context, req *master.ListDirectoryRequest) (*master.ListDirectoryResponse, error) {
	if err := h.validateRequest(req); err != nil {
		return nil, err
	}

	if req.DirIno == 0 {
		return nil, status.Errorf(codes.InvalidArgument, "directory inode cannot be zero")
	}

	if err := h.validateServiceRunning(); err != nil {
		return nil, err
	}

	entries, nextToken, hasMore, err := h.service.ListDirectory(req.DirIno, req.ContinuationToken, req.MaxEntries)
	if err != nil {
		if handleErr := handleContextError(ctx, err); handleErr != err {
			return nil, handleErr
		}
		return nil, status.Errorf(codes.NotFound, "directory not found: %d", req.DirIno)
	}

	return &master.ListDirectoryResponse{
		Entries:               entries,
		NextContinuationToken: nextToken,
		HasMore:               hasMore,
	}, nil
}

func (h *MasterGRPCHandler) MoveEntry(ctx context.Context, req *master.MoveEntryRequest) (*master.MoveEntryResponse, error) {
	if err := h.validateRequest(req); err != nil {
		return nil, err
	}

	if req.OldParentIno == 0 || req.NewParentIno == 0 {
		return nil, status.Errorf(codes.InvalidArgument, "parent inodes cannot be zero")
	}

	if req.OldName == "" || req.NewName == "" {
		return nil, status.Errorf(codes.InvalidArgument, "names cannot be empty")
	}

	if err := h.validateServiceRunning(); err != nil {
		return nil, err
	}

	updatedInode, err := h.service.MoveEntry(req.OldParentIno, req.OldName, req.NewParentIno, req.NewName)
	if err != nil {
		if handleErr := handleContextError(ctx, err); handleErr != err {
			return nil, handleErr
		}
		return nil, status.Errorf(codes.NotFound, "source entry not found: %s", req.OldName)
	}

	return &master.MoveEntryResponse{
		UpdatedInode: convertInoForGrpc(updatedInode),
	}, nil
}

func (h *MasterGRPCHandler) AllocateChunk(ctx context.Context, req *master.AllocateChunkRequest) (*master.AllocateChunkResponse, error) {
	if err := h.validateRequest(req); err != nil {
		return nil, err
	}

	if req.FileIno == 0 {
		return nil, status.Errorf(codes.InvalidArgument, "file inode cannot be zero")
	}

	if err := h.validateServiceRunning(); err != nil {
		return nil, err
	}

	chunkHandle, locations, leaseExpiry, err := h.service.AllocateChunk(req.FileIno, req.ChunkIndex)
	if err != nil {
		if handleErr := handleContextError(ctx, err); handleErr != err {
			return nil, handleErr
		}
		return nil, status.Errorf(codes.ResourceExhausted, "no available chunk servers")
	}

	return &master.AllocateChunkResponse{
		ChunkHandle: chunkHandle,
		Locations:   locations,
		LeaseExpiry: leaseExpiry,
	}, nil
}

func (h *MasterGRPCHandler) GetChunkLocations(ctx context.Context, req *master.GetChunkLocationsRequest) (*master.GetChunkLocationsResponse, error) {
	if err := h.validateRequest(req); err != nil {
		return nil, err
	}

	if len(req.ChunkHandles) == 0 {
		return nil, status.Errorf(codes.InvalidArgument, "chunk handles cannot be empty")
	}

	if err := h.validateServiceRunning(); err != nil {
		return nil, err
	}

	chunkInfos, err := h.service.GetChunkLocations(req.ChunkHandles)
	if err != nil {
		if handleErr := handleContextError(ctx, err); handleErr != err {
			return nil, handleErr
		}
		return nil, status.Errorf(codes.Internal, "failed to get chunk locations")
	}

	var protoChunks []*master.GetChunkLocationsResponse_ChunkLocationInfo
	for _, chunkInfo := range chunkInfos {
		protoChunk := &master.GetChunkLocationsResponse_ChunkLocationInfo{
			ChunkHandle: chunkInfo.ChunkHandle,
			Locations:   chunkInfo.Locations,
			Version:     chunkInfo.Version,
			Size:        chunkInfo.Size,
		}
		protoChunks = append(protoChunks, protoChunk)
	}

	return &master.GetChunkLocationsResponse{
		Chunks: protoChunks,
	}, nil
}

func (h *MasterGRPCHandler) RegisterChunkServer(ctx context.Context, req *master.RegisterChunkServerRequest) (*master.RegisterChunkServerResponse, error) {
	if err := h.validateRequest(req); err != nil {
		return nil, err
	}

	if req.Address == "" {
		return nil, status.Errorf(codes.InvalidArgument, "address cannot be empty")
	}

	if err := h.validateServiceRunning(); err != nil {
		return nil, err
	}

	heartbeatInterval, chunksToDelete, assignedServerID, err := h.service.RegisterChunkServer(req.ServerId, req.Address, req.TotalSpace, req.ExistingChunks)
	if err != nil {
		log.Printf("[ERROR] RegisterChunkServer: failed for server=%s address=%s: %v", req.ServerId, req.Address, err)
		if handleErr := handleContextError(ctx, err); handleErr != err {
			return nil, handleErr
		}
		return nil, status.Errorf(codes.Internal, "failed to register chunk server")
	}

	log.Printf("[INFO] RegisterChunkServer: registered server=%s address=%s assigned_id=%s",
		req.ServerId, req.Address, assignedServerID)

	return &master.RegisterChunkServerResponse{
		HeartbeatInterval: heartbeatInterval,
		ChunksToDelete:    chunksToDelete,
		AssignedServerId:  assignedServerID,
	}, nil
}

func (h *MasterGRPCHandler) SendHeartbeat(ctx context.Context, req *master.SendHeartbeatRequest) (*master.SendHeartbeatResponse, error) {
	if err := h.validateRequest(req); err != nil {
		return nil, err
	}

	if req.ServerId == "" {
		return nil, status.Errorf(codes.InvalidArgument, "server ID cannot be empty")
	}

	if req.Stats == nil {
		return nil, status.Errorf(codes.InvalidArgument, "stats cannot be nil")
	}

	if err := h.validateServiceRunning(); err != nil {
		return nil, err
	}

	var chunkReports []service.ChunkReportInfo
	for _, report := range req.ChunkReports {
		chunkReports = append(chunkReports, service.ChunkReportInfo{
			ChunkHandle: report.ChunkHandle,
			Version:     report.Version,
			Size:        report.Size,
			Checksum:    report.Checksum,
		})
	}

	chunksToDelete, replicationOrders, nextHeartbeatInterval, err := h.service.SendHeartbeat(
		req.ServerId,
		req.Stats.TotalSpace,
		req.Stats.UsedSpace,
		req.Stats.AvailableSpace,
		req.Stats.PendingDeletes,
		req.Stats.ActiveConnections,
		req.Stats.CpuUsage,
		req.Stats.MemoryUsage,
		chunkReports,
	)
	if err != nil {
		if handleErr := handleContextError(ctx, err); handleErr != err {
			return nil, handleErr
		}
		return nil, status.Errorf(codes.NotFound, "chunk server not registered: %s", req.ServerId)
	}

	var protoReplicationOrders []*master.SendHeartbeatResponse_ChunkReplicationOrder
	for _, order := range replicationOrders {
		protoOrder := &master.SendHeartbeatResponse_ChunkReplicationOrder{
			ChunkHandle:   order.ChunkHandle,
			TargetServers: order.TargetServers,
			Priority:      order.Priority,
		}
		protoReplicationOrders = append(protoReplicationOrders, protoOrder)
	}

	return &master.SendHeartbeatResponse{
		ChunksToDelete:        chunksToDelete,
		ReplicationOrders:     protoReplicationOrders,
		NextHeartbeatInterval: nextHeartbeatInterval,
	}, nil
}

func (h *MasterGRPCHandler) GetSystemStats(ctx context.Context, req *master.GetSystemStatsRequest) (*master.GetSystemStatsResponse, error) {
	if err := h.validateRequest(req); err != nil {
		return nil, err
	}

	if err := h.validateServiceRunning(); err != nil {
		return nil, err
	}

	stats, err := h.service.GetSystemStats()
	if err != nil {
		if handleErr := handleContextError(ctx, err); handleErr != err {
			return nil, handleErr
		}
		return nil, status.Errorf(codes.Internal, "Failed to get system statistics: %v", err)
	}

	return stats, nil
}

func (h *MasterGRPCHandler) Statx(ctx context.Context, req *master.StatxRequest) (*master.StatxResponse, error) {
	if err := h.validateRequest(req); err != nil {
		return nil, err
	}

	if req.Ino == 0 {
		return nil, status.Errorf(codes.InvalidArgument, "inode cannot be zero")
	}

	if err := h.validateServiceRunning(); err != nil {
		return nil, err
	}

	inode, supportedMask, err := h.service.Statx(req.Ino, req.Mask, req.Flags)
	if err != nil {
		if handleErr := handleContextError(ctx, err); handleErr != err {
			return nil, handleErr
		}
		return nil, status.Errorf(codes.NotFound, "inode not found: %d", req.Ino)
	}

	statxAttrs := convertInodeToStatxAttrs(inode)

	return &master.StatxResponse{
		Attributes: statxAttrs,
		Mask:       supportedMask,
	}, nil
}

// GetXAttr retrieves an extended attribute value
func (h *MasterGRPCHandler) GetXAttr(ctx context.Context, req *master.GetXAttrRequest) (*master.GetXAttrResponse, error) {
	if err := h.validateRequest(req); err != nil {
		return nil, err
	}

	if req.Ino == 0 {
		return nil, status.Errorf(codes.InvalidArgument, "inode cannot be zero")
	}

	if req.Name == "" {
		return nil, status.Errorf(codes.InvalidArgument, "attribute name cannot be empty")
	}

	if err := h.validateServiceRunning(); err != nil {
		return nil, err
	}

	value, err := h.service.GetXAttr(req.Ino, req.Name)
	if err != nil {
		if err.Error() == fmt.Sprintf("attribute not found: %s", req.Name) {
			return nil, status.Errorf(codes.NotFound, "attribute not found: %s", req.Name)
		}
		return nil, status.Errorf(codes.Internal, "failed to get attribute: %v", err)
	}

	return &master.GetXAttrResponse{
		Value: value,
	}, nil
}

// SetXAttr sets an extended attribute value
func (h *MasterGRPCHandler) SetXAttr(ctx context.Context, req *master.SetXAttrRequest) (*master.SetXAttrResponse, error) {
	if err := h.validateRequest(req); err != nil {
		return nil, err
	}

	if req.Ino == 0 {
		return nil, status.Errorf(codes.InvalidArgument, "inode cannot be zero")
	}

	if req.Name == "" {
		return nil, status.Errorf(codes.InvalidArgument, "attribute name cannot be empty")
	}

	if err := h.validateServiceRunning(); err != nil {
		return nil, err
	}

	err := h.service.SetXAttr(req.Ino, req.Name, req.Value, req.Flags)
	if err != nil {
		if err.Error() == "attribute already exists: "+req.Name {
			return nil, status.Errorf(codes.AlreadyExists, "attribute already exists: %s", req.Name)
		} else if err.Error() == "attribute not found: "+req.Name {
			return nil, status.Errorf(codes.NotFound, "attribute not found: %s", req.Name)
		}
		return nil, status.Errorf(codes.Internal, "failed to set attribute: %v", err)
	}

	return &master.SetXAttrResponse{}, nil
}

// ListXAttr lists all extended attribute names
func (h *MasterGRPCHandler) ListXAttr(ctx context.Context, req *master.ListXAttrRequest) (*master.ListXAttrResponse, error) {
	if err := h.validateRequest(req); err != nil {
		return nil, err
	}

	if req.Ino == 0 {
		return nil, status.Errorf(codes.InvalidArgument, "inode cannot be zero")
	}

	if err := h.validateServiceRunning(); err != nil {
		return nil, err
	}

	names, err := h.service.ListXAttr(req.Ino)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "inode not found: %d", req.Ino)
	}

	return &master.ListXAttrResponse{
		Names: names,
	}, nil
}

// RemoveXAttr removes an extended attribute
func (h *MasterGRPCHandler) RemoveXAttr(ctx context.Context, req *master.RemoveXAttrRequest) (*master.RemoveXAttrResponse, error) {
	if err := h.validateRequest(req); err != nil {
		return nil, err
	}

	if req.Ino == 0 {
		return nil, status.Errorf(codes.InvalidArgument, "inode cannot be zero")
	}

	if req.Name == "" {
		return nil, status.Errorf(codes.InvalidArgument, "attribute name cannot be empty")
	}

	if err := h.validateServiceRunning(); err != nil {
		return nil, err
	}

	err := h.service.RemoveXAttr(req.Ino, req.Name)
	if err != nil {
		if err.Error() == fmt.Sprintf("attribute not found: %s", req.Name) {
			return nil, status.Errorf(codes.NotFound, "attribute not found: %s", req.Name)
		}
		return nil, status.Errorf(codes.Internal, "failed to remove attribute: %v", err)
	}

	return &master.RemoveXAttrResponse{}, nil
}

// Lease Management Handlers

func (h *MasterGRPCHandler) GrantLease(ctx context.Context, req *master.GrantLeaseRequest) (*master.GrantLeaseResponse, error) {
	if err := h.validateRequest(req); err != nil {
		return nil, err
	}

	if req.ChunkHandle == "" {
		return nil, status.Errorf(codes.InvalidArgument, "chunk handle cannot be empty")
	}

	if req.RequestingServer == "" {
		return nil, status.Errorf(codes.InvalidArgument, "requesting server cannot be empty")
	}

	if err := h.validateServiceRunning(); err != nil {
		return nil, err
	}

	chunkHandle, err := commonTypes.ParseChunkHandle(req.ChunkHandle)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid chunk handle: %v", err)
	}

	lease, err := h.service.GrantChunkLease(chunkHandle, req.RequestingServer, req.Replicas, req.Version)
	if err != nil {
		log.Printf("[ERROR] GrantLease: failed to grant lease for chunk %s: %v", req.ChunkHandle, err)
		return nil, status.Errorf(codes.Internal, "failed to grant lease: %v", err)
	}

	return &master.GrantLeaseResponse{
		LeaseId:        lease.LeaseID,
		Primary:        lease.Primary,
		ExpireTime:     lease.ExpireTime.Unix(),
		RenewableAfter: lease.RenewableAfter.Unix(),
	}, nil
}

func (h *MasterGRPCHandler) RenewLease(ctx context.Context, req *master.RenewLeaseRequest) (*master.RenewLeaseResponse, error) {
	if err := h.validateRequest(req); err != nil {
		return nil, err
	}

	if req.ChunkHandle == "" {
		return nil, status.Errorf(codes.InvalidArgument, "chunk handle cannot be empty")
	}

	if req.LeaseId == "" {
		return nil, status.Errorf(codes.InvalidArgument, "lease ID cannot be empty")
	}

	if err := h.validateServiceRunning(); err != nil {
		return nil, err
	}

	chunkHandle, err := commonTypes.ParseChunkHandle(req.ChunkHandle)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid chunk handle: %v", err)
	}

	renewalReq := service.LeaseRenewalRequest{
		ChunkHandle: chunkHandle,
		LeaseID:     req.LeaseId,
		Primary:     req.Primary,
		Version:     req.Version,
	}

	lease, err := h.service.GetLeaseManager().RenewLease(renewalReq)
	if err != nil {
		log.Printf("[ERROR] RenewLease: failed to renew lease %s for chunk %s: %v", req.LeaseId, req.ChunkHandle, err)
		return nil, status.Errorf(codes.Internal, "failed to renew lease: %v", err)
	}

	return &master.RenewLeaseResponse{
		LeaseId:        lease.LeaseID,
		ExpireTime:     lease.ExpireTime.Unix(),
		RenewableAfter: lease.RenewableAfter.Unix(),
		RenewalCount:   int32(lease.RenewalCount),
	}, nil
}

func (h *MasterGRPCHandler) RevokeLease(ctx context.Context, req *master.RevokeLeaseRequest) (*master.RevokeLeaseResponse, error) {
	if err := h.validateRequest(req); err != nil {
		return nil, err
	}

	if req.ChunkHandle == "" {
		return nil, status.Errorf(codes.InvalidArgument, "chunk handle cannot be empty")
	}

	if err := h.validateServiceRunning(); err != nil {
		return nil, err
	}

	chunkHandle, err := commonTypes.ParseChunkHandle(req.ChunkHandle)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid chunk handle: %v", err)
	}

	err = h.service.GetLeaseManager().RevokeLease(chunkHandle, req.Reason)
	if err != nil {
		log.Printf("[ERROR] RevokeLease: failed to revoke lease for chunk %s: %v", req.ChunkHandle, err)
		return &master.RevokeLeaseResponse{
			Success: false,
			Message: err.Error(),
		}, nil
	}

	return &master.RevokeLeaseResponse{
		Success: true,
		Message: "Lease revoked successfully",
	}, nil
}

func (h *MasterGRPCHandler) GetLeaseInfo(ctx context.Context, req *master.GetLeaseInfoRequest) (*master.GetLeaseInfoResponse, error) {
	if err := h.validateRequest(req); err != nil {
		return nil, err
	}

	if req.ChunkHandle == "" {
		return nil, status.Errorf(codes.InvalidArgument, "chunk handle cannot be empty")
	}

	if err := h.validateServiceRunning(); err != nil {
		return nil, err
	}

	chunkHandle, err := commonTypes.ParseChunkHandle(req.ChunkHandle)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid chunk handle: %v", err)
	}

	lease := h.service.GetLeaseManager().GetLease(chunkHandle)
	if lease == nil {
		return nil, status.Errorf(codes.NotFound, "no lease found for chunk %s", req.ChunkHandle)
	}

	return &master.GetLeaseInfoResponse{
		LeaseId:        lease.LeaseID,
		Primary:        lease.Primary,
		Replicas:       lease.Replicas,
		Version:        lease.Version,
		GrantedAt:      lease.GrantedAt.Unix(),
		ExpireTime:     lease.ExpireTime.Unix(),
		RenewableAfter: lease.RenewableAfter.Unix(),
		State:          string(lease.State),
		RenewalCount:   int32(lease.RenewalCount),
	}, nil
}
