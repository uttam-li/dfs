package client

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	chunkserver "github.com/uttam-li/dfs/api/generated/chunkserver"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

// ConnectionPoolConfig holds configuration for the connection pool
type ConnectionPoolConfig struct {
	MaxIdleConns        int           // Maximum number of idle connections per server
	IdleTimeout         time.Duration // Timeout for idle connections
	ConnectTimeout      time.Duration // Timeout for establishing connections
	MaxLifetime         time.Duration // Maximum lifetime for a connection
	HealthCheckInterval time.Duration // Interval for health checks
}

// PooledConnection wraps a gRPC connection with metadata
type PooledConnection struct {
	conn      *grpc.ClientConn
	client    chunkserver.ChunkServerServiceClient
	createdAt time.Time
	lastUsed  time.Time
	inUse     bool
	healthy   bool
	mu        sync.RWMutex
}

// ChunkServerConnectionPool manages connections to chunk servers
type ChunkServerConnectionPool struct {
	config      ConnectionPoolConfig
	connections map[string][]*PooledConnection // address -> connections
	mu          sync.RWMutex

	// Background maintenance
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewChunkServerConnectionPool creates a new connection pool
func NewChunkServerConnectionPool(config ConnectionPoolConfig) *ChunkServerConnectionPool {
	ctx, cancel := context.WithCancel(context.Background())

	if config.MaxIdleConns == 0 {
		config.MaxIdleConns = 5
	}
	if config.IdleTimeout == 0 {
		config.IdleTimeout = 2 * time.Minute
	}
	if config.ConnectTimeout == 0 {
		config.ConnectTimeout = 5 * time.Second
	}
	if config.MaxLifetime == 0 {
		config.MaxLifetime = 15 * time.Minute
	}
	if config.HealthCheckInterval == 0 {
		config.HealthCheckInterval = 15 * time.Second
	}

	pool := &ChunkServerConnectionPool{
		config:      config,
		connections: make(map[string][]*PooledConnection),
		ctx:         ctx,
		cancel:      cancel,
	}

	// Start background maintenance
	pool.wg.Add(1)
	go pool.maintenanceRoutine()

	return pool
}

// GetConnection gets a connection from the pool or creates a new one
func (p *ChunkServerConnectionPool) GetConnection(address string) (*PooledConnection, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Look for an available connection
	if conns, exists := p.connections[address]; exists {
		for i, conn := range conns {
			conn.mu.Lock()
			if !conn.inUse && conn.healthy {
				conn.inUse = true
				conn.lastUsed = time.Now()
				conn.mu.Unlock()
				return conn, nil
			}
			conn.mu.Unlock()

			// Remove unhealthy connections
			if !conn.healthy {
				conn.conn.Close()
				p.connections[address] = append(conns[:i], conns[i+1:]...)
			}
		}
	}

	// Create new connection
	return p.createConnection(address)
}

// ReleaseConnection returns a connection to the pool
func (p *ChunkServerConnectionPool) ReleaseConnection(conn *PooledConnection) {
	if conn == nil {
		return
	}

	conn.mu.Lock()
	conn.inUse = false
	conn.lastUsed = time.Now()
	conn.mu.Unlock()
}

// createConnection creates a new connection to a chunk server
func (p *ChunkServerConnectionPool) createConnection(address string) (*PooledConnection, error) {
	ctx, cancel := context.WithTimeout(p.ctx, p.config.ConnectTimeout)
	defer cancel()

	// Use high-performance gRPC dial options
	dialOpts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                10 * time.Second, 
			Timeout:             3 * time.Second,  
			PermitWithoutStream: true,            
		}),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallSendMsgSize(128*1024*1024), // 128MB for large chunks
			grpc.MaxCallRecvMsgSize(128*1024*1024), // 128MB for large chunks
		),
		grpc.WithWriteBufferSize(64 * 1024),     
		grpc.WithReadBufferSize(64 * 1024),          
		grpc.WithInitialWindowSize(1024 * 1024),     
		grpc.WithInitialConnWindowSize(1024 * 1024), 
	}

	grpcConn, err := grpc.NewClient(address, dialOpts...)

	if err != nil {
		return nil, fmt.Errorf("failed to connect to chunk server %s: %w", address, err)
	}

	client := chunkserver.NewChunkServerServiceClient(grpcConn)

	// Test the connection
	healthCtx, healthCancel := context.WithTimeout(ctx, 5*time.Second)
	defer healthCancel()

	_, err = client.HealthCheck(healthCtx, &chunkserver.HealthCheckRequest{})
	if err != nil {
		grpcConn.Close()
		return nil, fmt.Errorf("health check failed for chunk server %s: %w", address, err)
	}

	conn := &PooledConnection{
		conn:      grpcConn,
		client:    client,
		createdAt: time.Now(),
		lastUsed:  time.Now(),
		inUse:     true,
		healthy:   true,
	}

	if p.connections[address] == nil {
		p.connections[address] = make([]*PooledConnection, 0)
	}
	p.connections[address] = append(p.connections[address], conn)

	log.Printf("Created new connection to chunk server: %s", address)
	return conn, nil
}

// maintenanceRoutine performs background maintenance on the connection pool
func (p *ChunkServerConnectionPool) maintenanceRoutine() {
	defer p.wg.Done()

	ticker := time.NewTicker(p.config.HealthCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-p.ctx.Done():
			return
		case <-ticker.C:
			p.performMaintenance()
		}
	}
}

// performMaintenance cleans up expired and unhealthy connections
func (p *ChunkServerConnectionPool) performMaintenance() {
	p.mu.Lock()
	defer p.mu.Unlock()

	now := time.Now()

	for address, conns := range p.connections {
		activeConns := make([]*PooledConnection, 0)
		idleCount := 0

		for _, conn := range conns {
			conn.mu.RLock()
			shouldRemove := false

			// Check if connection is too old
			if now.Sub(conn.createdAt) > p.config.MaxLifetime {
				shouldRemove = true
			}

			// Check if idle connection has timed out
			if !conn.inUse && now.Sub(conn.lastUsed) > p.config.IdleTimeout {
				shouldRemove = true
			}

			// Check if we have too many idle connections
			if !conn.inUse {
				idleCount++
				if idleCount > p.config.MaxIdleConns {
					shouldRemove = true
				}
			}

			conn.mu.RUnlock()

			if shouldRemove && !conn.inUse {
				conn.conn.Close()
				log.Printf("Closed expired connection to chunk server: %s", address)
			} else {
				activeConns = append(activeConns, conn)

				// Perform health check on idle connections
				if !conn.inUse {
					go p.healthCheckConnection(conn, address)
				}
			}
		}

		if len(activeConns) == 0 {
			delete(p.connections, address)
		} else {
			p.connections[address] = activeConns
		}
	}
}

// healthCheckConnection performs a health check on a connection
func (p *ChunkServerConnectionPool) healthCheckConnection(conn *PooledConnection, address string) {
	ctx, cancel := context.WithTimeout(p.ctx, 5*time.Second)
	defer cancel()

	_, err := conn.client.HealthCheck(ctx, &chunkserver.HealthCheckRequest{})

	conn.mu.Lock()
	if err != nil {
		conn.healthy = false
		log.Printf("Health check failed for chunk server %s: %v", address, err)
	} else {
		conn.healthy = true
	}
	conn.mu.Unlock()
}

// Close closes all connections in the pool
func (p *ChunkServerConnectionPool) Close() {
	p.cancel()
	p.wg.Wait()

	p.mu.Lock()
	defer p.mu.Unlock()

	for address, conns := range p.connections {
		for _, conn := range conns {
			conn.conn.Close()
		}
		log.Printf("Closed all connections to chunk server: %s", address)
	}

	p.connections = make(map[string][]*PooledConnection)
}

// GetStats returns statistics about the connection pool
func (p *ChunkServerConnectionPool) GetStats() map[string]int {
	p.mu.RLock()
	defer p.mu.RUnlock()

	stats := make(map[string]int)
	totalConns := 0
	activeConns := 0
	idleConns := 0

	for _, conns := range p.connections {
		totalConns += len(conns)
		for _, conn := range conns {
			conn.mu.RLock()
			if conn.inUse {
				activeConns++
			} else {
				idleConns++
			}
			conn.mu.RUnlock()
		}
	}

	stats["total_connections"] = totalConns
	stats["active_connections"] = activeConns
	stats["idle_connections"] = idleConns
	stats["servers"] = len(p.connections)

	return stats
}

// GetClient returns the gRPC client from a pooled connection
func (conn *PooledConnection) GetClient() chunkserver.ChunkServerServiceClient {
	return conn.client
}

// IsHealthy returns true if the connection is healthy
func (conn *PooledConnection) IsHealthy() bool {
	conn.mu.RLock()
	defer conn.mu.RUnlock()
	return conn.healthy
}

// MarkUnhealthy marks the connection as unhealthy
func (conn *PooledConnection) MarkUnhealthy() {
	conn.mu.Lock()
	defer conn.mu.Unlock()
	conn.healthy = false
}
