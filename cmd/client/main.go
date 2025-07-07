package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/uttam-li/dfs/pkg/client"
	"github.com/uttam-li/dfs/pkg/common"
)

type ClientApplication struct {
	config     *common.ClientConfig
	filesystem *client.Filesystem
	fuseServer *fuse.Server

	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	mu      sync.Mutex
	running bool
}

func main() {
	log.SetFlags(log.Ltime | log.Lshortfile)

	// Load base configuration from environment variables
	config := common.LoadClientConfig()

	// Prompt user for interactive configuration
	promptUserConfig(config)

	if err := config.Validate(); err != nil {
		log.Fatalf("Invalid configuration: %v", err)
	}

	app, err := NewClientApplication(config)
	if err != nil {
		log.Fatalf("Failed to create client application: %v", err)
	}

	app.setupGracefulShutdown()

	if err := app.Start(); err != nil {
		log.Fatalf("Failed to start client: %v", err)
	}

	log.Println("Press Ctrl+C to stop client")

	// Keep the main goroutine alive
	select {}
}

// NewClientApplication creates a new ClientApplication with the provided configuration.
func NewClientApplication(config *common.ClientConfig) (*ClientApplication, error) {
	if config == nil {
		return nil, fmt.Errorf("config cannot be nil")
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &ClientApplication{
		config: config,
		ctx:    ctx,
		cancel: cancel,
	}, nil
}

// Start initializes the filesystem and mounts the FUSE server.
func (app *ClientApplication) Start() error {
	app.mu.Lock()
	defer app.mu.Unlock()

	if app.running {
		return fmt.Errorf("client is already running")
	}

	log.Printf("Starting DFS client...")

	// 1. Create the filesystem
	var err error
	app.filesystem, err = client.NewFilesystem(app.config)
	if err != nil {
		return fmt.Errorf("failed to create filesystem: %w", err)
	}

	// 2. Mount the FUSE filesystem
	// Ensure mount point exists
	if err := os.MkdirAll(app.config.MountPoint, 0755); err != nil {
		return fmt.Errorf("failed to create mount point: %w", err)
	}

	opts := &fuse.MountOptions{
		AllowOther:    app.config.AllowOthers,
		Debug:         app.config.Debug,
		MaxBackground: 500,
		MaxWrite:      int(app.config.ChunkSize),
		MaxReadAhead:  int(app.config.ChunkSize),
		FsName:        "dfs",
		Name:          "dfs",
	}

	server, err := fuse.NewServer(app.filesystem, app.config.MountPoint, opts)
	if err != nil {
		return fmt.Errorf("failed to create FUSE server: %w", err)
	}
	app.fuseServer = server

	app.wg.Add(1)
	go func() {
		defer app.wg.Done()
		// Serve blocks until the filesystem is unmounted
		app.fuseServer.Serve()
	}()

	// Give the server a moment to start and mount the filesystem
	time.Sleep(100 * time.Millisecond)

	app.fuseServer.WaitMount()

	app.running = true
	return nil
}

// setupGracefulShutdown configures signal handling for a clean shutdown.
func (app *ClientApplication) setupGracefulShutdown() {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigCh
		log.Println("Shutdown signal received")

		if err := app.Stop(); err != nil {
			log.Printf("Error during shutdown: %v", err)
		}

		log.Println("DFS client shutdown completed")
		os.Exit(0)
	}()
}

// Stop gracefully unmounts the filesystem and shuts down the client.
func (app *ClientApplication) Stop() error {
	app.mu.Lock()
	defer app.mu.Unlock()

	if !app.running {
		return nil
	}

	log.Println("Stopping DFS client...")

	// Cancel context to signal shutdown
	app.cancel()

	// Unmount FUSE filesystem
	if app.fuseServer != nil {
		if err := app.fuseServer.Unmount(); err != nil {
			log.Printf("Error unmounting FUSE server: %v", err)
		}
	}

	// Stop filesystem
	if app.filesystem != nil {
		if err := app.filesystem.Close(); err != nil {
			log.Printf("Error stopping filesystem: %v", err)
		}
	}

	app.wg.Wait()

	app.running = false
	return nil
}

// promptUserConfig prompts the user for configuration values and updates the config object.
func promptUserConfig(config *common.ClientConfig) {
	reader := bufio.NewReader(os.Stdin)

	fmt.Printf("Enter master server address (default: %s): ", config.MasterAddr)
	addr, _ := reader.ReadString('\n')
	addr = strings.TrimSpace(addr)
	if addr != "" {
		config.MasterAddr = addr
	}

	fmt.Printf("Enter mount point (default: %s): ", config.MountPoint)
	mountPoint, _ := reader.ReadString('\n')
	mountPoint = strings.TrimSpace(mountPoint)
	if mountPoint != "" {
		config.MountPoint = mountPoint
	}

	fmt.Printf("Configuration will use:")
	fmt.Printf("  Master Address: %s\n", config.MasterAddr)
	fmt.Printf("  Mount Point: %s\n", config.MountPoint)
	fmt.Printf("  Debug Mode: %v\n", config.Debug)
	fmt.Println()
}
