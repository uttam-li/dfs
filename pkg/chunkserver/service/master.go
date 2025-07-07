package service

import (
	"fmt"
	"log"
	"time"

	"github.com/uttam-li/dfs/api/generated/master"
	"github.com/uttam-li/dfs/pkg/common"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	defaultTotalSpace = 1024 * 1024 * 1024 * 1024 // 1TB
)

// connectToMaster establishes a gRPC connection to the master server.
// Returns an error if the connection cannot be established.
func (cs *ChunkServerService) connectToMaster() error {
	var err error
	cs.masterConn, err = grpc.NewClient(cs.config.MasterAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallSendMsgSize(common.DefaultMaxGRPCMessageSize),
			grpc.MaxCallRecvMsgSize(common.DefaultMaxGRPCMessageSize),
		),
	)
	if err != nil {
		return fmt.Errorf("failed to connect to master at %s: %w", cs.config.MasterAddr, err)
	}

	cs.masterClient = master.NewMasterServiceClient(cs.masterConn)
	log.Printf("Connected to master server at %s", cs.config.MasterAddr)
	return nil
}

// registerWithMaster registers this chunk server with the master and provides
// information about existing chunks. Returns an error if registration fails.
func (cs *ChunkServerService) registerWithMaster() error {
	existingChunks := cs.collectExistingChunks()

	serverAddr := fmt.Sprintf("localhost:%d", cs.config.GRPCPort)

	req := &master.RegisterChunkServerRequest{
		ServerId:       cs.serverID,
		Address:        serverAddr,
		TotalSpace:     defaultTotalSpace,
		ExistingChunks: existingChunks,
	}

	resp, err := cs.masterClient.RegisterChunkServer(cs.ctx, req)
	if err != nil {
		return fmt.Errorf("failed to register with master: %w", err)
	}

	cs.registered = true
	cs.heartbeatInterval = time.Duration(resp.HeartbeatInterval) * time.Second
	cs.serverID = resp.AssignedServerId

	if err := cs.cleanupRequestedChunks(resp.ChunksToDelete); err != nil {
		log.Printf("Error during chunk cleanup: %v", err)
	}

	return nil
}

// disconnectFromMaster closes the connection to the master server and resets
// connection-related state.
func (cs *ChunkServerService) disconnectFromMaster() {
	if cs.masterConn != nil {
		cs.masterConn.Close()
		cs.masterConn = nil
		cs.masterClient = nil
		cs.registered = false
		log.Printf("Disconnected from master server")
	}
}

// attemptMasterReconnection attempts to reconnect to the master server.
// Logs errors if reconnection or re-registration fails.
func (cs *ChunkServerService) attemptMasterReconnection() {
	log.Printf("Attempting to reconnect to master server")

	// Close existing connection
	cs.disconnectFromMaster()

	if err := cs.connectToMaster(); err != nil {
		log.Printf("Failed to reconnect to master: %v", err)
		return
	}

	if err := cs.registerWithMaster(); err != nil {
		log.Printf("Failed to re-register with master: %v", err)
		return
	}

	log.Printf("Successfull reconnected to master")
}

// collectExistingChunks returns a list of all chunk handles currently stored.
func (cs *ChunkServerService) collectExistingChunks() []string {
	existingChunks := make([]string, 0)
	cs.storage.mu.RLock()
	for handle := range cs.storage.chunks {
		existingChunks = append(existingChunks, handle)
	}
	cs.storage.mu.RUnlock()
	return existingChunks
}

// cleanupRequestedChunks deletes chunks that the master has requested to be removed.
func (cs *ChunkServerService) cleanupRequestedChunks(chunksToDelete []string) error {
	for _, chunkHandle := range chunksToDelete {
		if err := cs.DeleteChunk(chunkHandle); err != nil {
			log.Printf("Failed to delete requested chunk %s: %v", chunkHandle, err)
		}
	}
	return nil
}
