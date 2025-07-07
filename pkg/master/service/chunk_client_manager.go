package service

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/uttam-li/dfs/pkg/common"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// ConnectionInfo holds metadata about a gRPC connection
type ConnectionInfo struct {
	conn      *grpc.ClientConn
	createdAt time.Time
	lastUsed  time.Time
	inUse     bool
}

// ChunkServerClientManager manages gRPC connections to chunk servers
type ChunkServerClientManager struct {
	chunkServerClients   map[string]*ConnectionInfo
	chunkServerClientsMu sync.RWMutex

	// Connection pool settings
	maxIdleTime time.Duration
	maxLifetime time.Duration

	// Service lifecycle
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
	running  bool
	runningM sync.RWMutex
}

// NewChunkServerClientManager creates a new chunk server client manager
func NewChunkServerClientManager() *ChunkServerClientManager {
	return &ChunkServerClientManager{
		chunkServerClients: make(map[string]*ConnectionInfo),
		maxIdleTime:        5 * time.Minute,  // Close connections idle for 5 minutes
		maxLifetime:        30 * time.Minute, // Maximum connection lifetime
	}
}

// Start starts the chunk server client manager
func (cm *ChunkServerClientManager) Start(ctx context.Context) error {
	cm.runningM.Lock()
	defer cm.runningM.Unlock()

	if cm.running {
		return nil
	}

	cm.ctx, cm.cancel = context.WithCancel(ctx)
	cm.running = true

	// Start connection cleanup routine
	cm.wg.Add(1)
	go cm.cleanupRoutine()

	log.Printf("Chunk server client manager started")
	return nil
}

// Stop stops the chunk server client manager
func (cm *ChunkServerClientManager) Stop() error {
	cm.runningM.Lock()
	defer cm.runningM.Unlock()

	if !cm.running {
		return nil
	}

	cm.cancel()
	cm.wg.Wait()

	// Close all existing connections
	cm.chunkServerClientsMu.Lock()
	for addr, connInfo := range cm.chunkServerClients {
		if err := connInfo.conn.Close(); err != nil {
			log.Printf("Error closing connection to %s: %v", addr, err)
		}
	}
	cm.chunkServerClients = make(map[string]*ConnectionInfo)
	cm.chunkServerClientsMu.Unlock()

	cm.running = false

	log.Printf("Chunk server client manager stopped")
	return nil
}

// GetConnection gets or creates a connection to a chunk server
func (cm *ChunkServerClientManager) GetConnection(address string) (*grpc.ClientConn, error) {
	cm.chunkServerClientsMu.RLock()
	if connInfo, exists := cm.chunkServerClients[address]; exists {
		// Check if connection is still valid and not too old
		now := time.Now()
		if !connInfo.inUse &&
			now.Sub(connInfo.lastUsed) < cm.maxIdleTime &&
			now.Sub(connInfo.createdAt) < cm.maxLifetime {
			connInfo.inUse = true
			connInfo.lastUsed = now
			cm.chunkServerClientsMu.RUnlock()
			return connInfo.conn, nil
		}
	}
	cm.chunkServerClientsMu.RUnlock()

	// Create new connection
	cm.chunkServerClientsMu.Lock()
	defer cm.chunkServerClientsMu.Unlock()

	// Double-check after acquiring write lock
	if connInfo, exists := cm.chunkServerClients[address]; exists {
		now := time.Now()
		if !connInfo.inUse &&
			now.Sub(connInfo.lastUsed) < cm.maxIdleTime &&
			now.Sub(connInfo.createdAt) < cm.maxLifetime {
			connInfo.inUse = true
			connInfo.lastUsed = now
			return connInfo.conn, nil
		} else {
			// Close old connection
			connInfo.conn.Close()
			delete(cm.chunkServerClients, address)
		}
	}

	conn, err := grpc.NewClient(address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallSendMsgSize(common.DefaultMaxGRPCMessageSize),
			grpc.MaxCallRecvMsgSize(common.DefaultMaxGRPCMessageSize),
		),
	)
	if err != nil {
		return nil, err
	}

	now := time.Now()
	connInfo := &ConnectionInfo{
		conn:      conn,
		createdAt: now,
		lastUsed:  now,
		inUse:     true,
	}
	cm.chunkServerClients[address] = connInfo

	log.Printf("Created new connection to chunk server: %s", address)
	return conn, nil
}

// ReleaseConnection marks a connection as no longer in use
func (cm *ChunkServerClientManager) ReleaseConnection(address string) {
	cm.chunkServerClientsMu.Lock()
	defer cm.chunkServerClientsMu.Unlock()

	if connInfo, exists := cm.chunkServerClients[address]; exists {
		connInfo.inUse = false
		connInfo.lastUsed = time.Now()
	}
}

// RemoveConnection removes a connection to a chunk server
func (cm *ChunkServerClientManager) RemoveConnection(address string) {
	cm.chunkServerClientsMu.Lock()
	defer cm.chunkServerClientsMu.Unlock()

	if connInfo, exists := cm.chunkServerClients[address]; exists {
		connInfo.conn.Close()
		delete(cm.chunkServerClients, address)
		log.Printf("Removed connection to chunk server: %s", address)
	}
}

// cleanupRoutine periodically cleans up expired connections
func (cm *ChunkServerClientManager) cleanupRoutine() {
	defer cm.wg.Done()

	ticker := time.NewTicker(1 * time.Minute) // Check every minute
	defer ticker.Stop()

	for {
		select {
		case <-cm.ctx.Done():
			return
		case <-ticker.C:
			cm.performCleanup()
		}
	}
}

// performCleanup removes expired and idle connections
func (cm *ChunkServerClientManager) performCleanup() {
	cm.chunkServerClientsMu.Lock()
	defer cm.chunkServerClientsMu.Unlock()

	now := time.Now()
	toRemove := make([]string, 0)

	for address, connInfo := range cm.chunkServerClients {
		// Skip connections that are currently in use
		if connInfo.inUse {
			continue
		}

		// Remove connections that are too old or have been idle too long
		if now.Sub(connInfo.createdAt) > cm.maxLifetime ||
			now.Sub(connInfo.lastUsed) > cm.maxIdleTime {
			toRemove = append(toRemove, address)
		}
	}

	for _, address := range toRemove {
		if connInfo, exists := cm.chunkServerClients[address]; exists {
			connInfo.conn.Close()
			delete(cm.chunkServerClients, address)
			log.Printf("Cleaned up expired connection to chunk server: %s", address)
		}
	}
}

// IsRunning returns true if the client manager is running
func (cm *ChunkServerClientManager) IsRunning() bool {
	cm.runningM.RLock()
	defer cm.runningM.RUnlock()
	return cm.running
}

// GetStats returns statistics about the connection manager
func (cm *ChunkServerClientManager) GetStats() map[string]int {
	cm.chunkServerClientsMu.RLock()
	defer cm.chunkServerClientsMu.RUnlock()

	stats := make(map[string]int)
	totalConns := 0
	activeConns := 0
	idleConns := 0

	for _, connInfo := range cm.chunkServerClients {
		totalConns++
		if connInfo.inUse {
			activeConns++
		} else {
			idleConns++
		}
	}

	stats["total_connections"] = totalConns
	stats["active_connections"] = activeConns
	stats["idle_connections"] = idleConns
	stats["servers"] = len(cm.chunkServerClients)

	return stats
}
