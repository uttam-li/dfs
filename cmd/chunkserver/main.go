package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/uttam-li/dfs/pkg/chunkserver/handler/grpc"
	"github.com/uttam-li/dfs/pkg/chunkserver/service"
	"github.com/uttam-li/dfs/pkg/common"
	grpcServer "google.golang.org/grpc"
)

// ChunkServerApplication manages the lifecycle of a chunk server, including its gRPC server and internal service.
type ChunkServerApplication struct {
	config     *common.ChunkServerConfig
	service    *service.ChunkServerService
	grpcServer *grpcServer.Server
	listener   net.Listener

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	mu     sync.Mutex

	running bool
}

func main() {
	log.SetFlags(log.Ltime | log.Lshortfile)

	// Parse command line arguments
	var (
		port        = flag.Int("port", 0, "gRPC port for the chunkserver (required)")
		storageRoot = flag.String("storage", "", "Storage root directory (required, will be created under .storage/)")
		masterAddr  = flag.String("master", "localhost:8000", "Master server address")
		help        = flag.Bool("help", false, "Show help message")
	)
	flag.Parse()

	if *help {
		fmt.Println("ChunkServer - Distributed File System Chunk Server")
		fmt.Println("")
		fmt.Println("Usage:")
		fmt.Printf("  %s -port <port> -storage <storage_name> [-master <master_addr>]\n", os.Args[0])
		fmt.Println("")
		fmt.Println("Arguments:")
		fmt.Println("  -port     gRPC port for the chunkserver (required)")
		fmt.Println("  -storage  Storage identifier (e.g., 'chunk_1', 'chunk_2')")
		fmt.Println("            Creates directory storage/<storage_name>/")
		fmt.Println("  -master   Master server address (default: localhost:8000)")
		fmt.Println("  -help     Show this help message")
		fmt.Println("")
		fmt.Println("Examples:")
		fmt.Println("  make chunkserver PORT=8081 STORAGE=chunk_1")
		fmt.Println("  make chunkserver PORT=8082 STORAGE=chunk_2")
		os.Exit(0)
	}

	// Validate required arguments
	if *port == 0 {
		log.Fatalf("Port is required. Use -port <port_number> or run with -help for usage")
	}

	if *storageRoot == "" {
		log.Fatalf("Storage root is required. Use -storage <storage_name> or run with -help for usage")
	}

	// Validate port range
	if *port <= 0 || *port > 65535 {
		log.Fatalf("Invalid port: %d. Port must be between 1 and 65535", *port)
	}

	// Create storage path: .storage/<storage_name>
	storagePath := fmt.Sprintf("storage/%s", *storageRoot)

	// Load configuration with overrides
	config := common.LoadChunkServerConfig()
	config.GRPCPort = *port
	config.StorageRoot = storagePath
	config.MasterAddr = *masterAddr

	if err := config.Validate(); err != nil {
		log.Fatalf("Invalid configuration: %v", err)
	}

	log.Printf("Starting ChunkServer with:")
	log.Printf("  Port: %d", config.GRPCPort)
	log.Printf("  Storage: %s", config.StorageRoot)
	log.Printf("  Master: %s", config.MasterAddr)

	// Create chunk server application
	app, err := NewChunkServerApplication(config)
	if err != nil {
		log.Fatalf("Failed to create chunk server: %v", err)
	}

	// Setup graceful shutdown
	app.setupGracefulShutdown()

	// Start the application
	if err := app.Start(); err != nil {
		log.Fatalf("Failed to start chunk server: %v", err)
	}

	log.Printf("ChunkServer started successfully on port: %d", app.config.GRPCPort)
	log.Printf("Storage directory: %s", app.config.StorageRoot)
	log.Println("Press Ctrl+C to stop chunk server")

	// Keep the main goroutine alive
	select {}
}

// NewChunkServerApplication creates a new ChunkServerApplication with the provided configuration.
func NewChunkServerApplication(config *common.ChunkServerConfig) (*ChunkServerApplication, error) {
	if config == nil {
		return nil, fmt.Errorf("config cannot be nil")
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &ChunkServerApplication{
		config: config,
		ctx:    ctx,
		cancel: cancel,
	}, nil
}

// Start launches the chunk server service and the gRPC server.
func (app *ChunkServerApplication) Start() error {
	app.mu.Lock()
	defer app.mu.Unlock()

	if app.running {
		return fmt.Errorf("chunk server already running")
	}

	log.Printf("Starting chunk server...")

	// Create and start service
	if err := app.createService(); err != nil {
		return fmt.Errorf("failed to create service: %w", err)
	}

	// Start gRPC server
	if err := app.startGRPCServer(); err != nil {
		return fmt.Errorf("failed to start gRPC server: %w", err)
	}

	app.running = true
	return nil
}

// createService creates and starts the chunk server service
func (app *ChunkServerApplication) createService() error {
	var err error
	app.service, err = service.NewChunkServerService(app.config)
	if err != nil {
		return fmt.Errorf("failed to create chunk server service: %w", err)
	}

	// Start the service with timeout
	startCtx, startCancel := context.WithTimeout(app.ctx, app.config.StartupTimeout)
	defer startCancel()

	if err := app.service.Start(startCtx); err != nil {
		return fmt.Errorf("failed to start chunk server service: %w", err)
	}

	return nil
}

// startGRPCServer starts the gRPC server
func (app *ChunkServerApplication) startGRPCServer() error {
	address := fmt.Sprintf(":%d", app.config.GRPCPort)

	lis, err := net.Listen("tcp", address)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", address, err)
	}
	app.listener = lis

	// Create gRPC server with high-performance options
	serverOpts := []grpcServer.ServerOption{
		grpcServer.MaxRecvMsgSize(128 * 1024 * 1024), // 128MB
		grpcServer.MaxSendMsgSize(128 * 1024 * 1024), // 128MB
		grpcServer.MaxConcurrentStreams(2000),        // High concurrency
		grpcServer.WriteBufferSize(32 * 1024),        // 32KB write buffer
		grpcServer.ReadBufferSize(32 * 1024),         // 32KB read buffer
	}

	app.grpcServer = grpcServer.NewServer(serverOpts...)
	grpc.NewChunkServerGRPCHandler(app.service, app.grpcServer)

	// Start gRPC server in goroutine
	app.wg.Add(1)
	go func() {
		defer app.wg.Done()
		log.Printf("gRPC server listening on %s", address)
		if err := app.grpcServer.Serve(app.listener); err != nil {
			log.Printf("gRPC server error: %v", err)
		}
	}()

	// Give the server a moment to start
	time.Sleep(100 * time.Millisecond)

	return nil
}

// setupGracefulShutdown configures system signal handlers for clean application shutdown.
func (app *ChunkServerApplication) setupGracefulShutdown() {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigCh
		log.Println("Shutdown signal received")

		if err := app.Stop(); err != nil {
			log.Printf("Error during shutdown: %v", err)
		}

		log.Println("Chunk server shutdown completed")
		os.Exit(0)
	}()
}

// Stop gracefully shuts down the chunk server, stopping the gRPC server, service, and network listener.
func (app *ChunkServerApplication) Stop() error {
	app.mu.Lock()
	defer app.mu.Unlock()

	if !app.running {
		return nil
	}

	log.Println("Stopping chunk server...")

	// Cancel context to signal shutdown
	app.cancel()

	// Stop gRPC server gracefully
	if app.grpcServer != nil {
		done := make(chan struct{})
		go func() {
			app.grpcServer.GracefulStop()
			close(done)
		}()

		// Wait for graceful stop or force stop after timeout
		select {
		case <-done:
			log.Println("gRPC server stopped gracefully")
		case <-time.After(app.config.ShutdownTimeout / 2):
			log.Println("Force stopping gRPC server")
			app.grpcServer.Stop()
		}
	}

	// Stop chunk server service
	if app.service != nil {
		if err := app.service.Stop(); err != nil {
			log.Printf("Error stopping chunk server service: %v", err)
		}
	}

	// Close the network listener to free the port
	if app.listener != nil {
		if err := app.listener.Close(); err != nil {
			log.Printf("Error closing listener: %v", err)
		}
	}

	// Wait for all goroutines to finish
	done := make(chan struct{})
	go func() {
		app.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		log.Println("All goroutines stopped")
	case <-time.After(app.config.ShutdownTimeout):
		log.Println("Timeout waiting for goroutines to stop")
	}

	app.running = false
	return nil
}

// IsRunning reports whether the chunk server is currently running.
func (app *ChunkServerApplication) IsRunning() bool {
	app.mu.Lock()
	defer app.mu.Unlock()
	return app.running
}

// GetService returns the underlying ChunkServerService instance.
func (app *ChunkServerApplication) GetService() *service.ChunkServerService {
	return app.service
}
