package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/uttam-li/dfs/pkg/common"
	"github.com/uttam-li/dfs/pkg/master/handler/grpc"
	"github.com/uttam-li/dfs/pkg/master/service"

	grpcServer "google.golang.org/grpc"
)

type MasterServer struct {
	config     *common.MasterConfig
	service    *service.MasterService
	grpcServer *grpcServer.Server
	listener   net.Listener

	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	mu      sync.Mutex
	running bool
}

func main() {
	log.SetFlags(log.Ltime | log.Lshortfile)

	config := common.LoadMasterConfig()
	if err := config.Validate(); err != nil {
		log.Fatalf("Invalid configuration: %v", err)
	}

	server, err := NewMasterServer(config)
	if err != nil {
		log.Fatalf("Failed to create master server: %v", err)
	}

	server.setupGracefulShutdown()

	if err := server.Start(); err != nil {
		log.Fatalf("Failed to start master server: %v", err)
	}

	log.Printf("Master server started successfully on port: %s", server.config.Port)
	log.Println("Press Ctrl+C to stop master server")

	<-server.ctx.Done()
}

// NewMasterServer creates a new MasterServer with the provided configuration.
func NewMasterServer(config *common.MasterConfig) (*MasterServer, error) {
	if config == nil {
		return nil, fmt.Errorf("config cannot be nil")
	}

	ctx, cancel := context.WithCancel(context.Background())

	server := &MasterServer{
		config: config,
		ctx:    ctx,
		cancel: cancel,
	}

	return server, nil
}

// setupGracefulShutdown configures signal handling for clean shutdown.
func (s *MasterServer) setupGracefulShutdown() {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigCh
		log.Println("Shutdown signal received")

		if err := s.Stop(); err != nil {
			log.Printf("Error during shutdown: %v", err)
		}

		log.Printf("Master server shutdown completed")
	}()
}

// Start initializes the master service and starts the gRPC server.
func (s *MasterServer) Start() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.running {
		return fmt.Errorf("server is already running")
	}

	var err error
	s.service, err = service.NewMasterService(s.config)
	if err != nil {
		return fmt.Errorf("failed to create master service: %w", err)
	}

	startCtx, startCancel := context.WithTimeout(s.ctx, s.config.StartupTimeout)
	defer startCancel()

	if err := s.service.Start(startCtx); err != nil {
		return fmt.Errorf("failed to start master service: %w", err)
	}

	port := fmt.Sprintf(":%s", s.config.Port)

	lis, err := net.Listen("tcp", port)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", port, err)
	}
	s.listener = lis

	s.grpcServer = grpcServer.NewServer(
		grpcServer.MaxRecvMsgSize(common.DefaultMaxGRPCMessageSize),
		grpcServer.MaxSendMsgSize(common.DefaultMaxGRPCMessageSize),
	)
	grpc.NewMasterGRPCHandler(s.grpcServer, s.service)

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		log.Printf("gRPC server listening on %s", port)
		if err := s.grpcServer.Serve(s.listener); err != nil {
			log.Printf("gRPC server error: %v", err)
		}
	}()

	time.Sleep(100 * time.Millisecond)

	s.running = true
	return nil
}

// Stop gracefully shuts down the master server and all its components.
func (s *MasterServer) Stop() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.running {
		return nil
	}

	log.Println("Stopping master server...")

	s.cancel()

	if s.grpcServer != nil {
		done := make(chan struct{})
		go func() {
			s.grpcServer.GracefulStop()
			close(done)
		}()

		select {
		case <-done:
		case <-time.After(s.config.ShutdownTimeout / 2):
			s.grpcServer.Stop()
		}
	}

	if s.listener != nil {
		if err := s.listener.Close(); err != nil {
			log.Printf("Error closing listener: %v", err)
		}
	}

	if s.service != nil {
		if err := s.service.Stop(); err != nil {
			log.Printf("Error stopping master service: %v", err)
		}
	}

	s.wg.Wait()

	s.running = false
	return nil
}
