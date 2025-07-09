package common

import "time"

// Default configuration values
const (
	// Common defaults
	DefaultDebug = true

	// Master service defaults
	DefaultMasterPort               = "8000"
	DefaultReplicationFactor        = 3
	DefaultChunkSize                = 64 * 1024 * 1024 // 64MB
	DefaultMaxInodes                = 1000000          // 1 Million
	DefaultHeartbeatInterval        = 30 * time.Second
	DefaultHeartbeatTimeout         = 60 * time.Second
	DefaultLeaseTimeout             = 30 * time.Minute
	DefaultCheckpointInterval       = 2 * time.Minute
	DefaultGCInterval               = 5 * time.Minute
	DefaultOrphanedChunkGracePeriod = 24 * time.Hour
	DefaultLogDir                   = "./logs"
	DefaultRootInode                = 1

	// ChunkServer defaults
	DefaultChunkServerPort      = 8081
	DefaultStorageRoot          = "./storage"
	DefaultChunkServerChunkSize = 64 * 1024 * 1024 // 64MB
	DefaultMasterAddr           = "localhost:8000"

	// Client defaults
	DefaultBlockSize     = 64 * 1024 // 64KB
	DefaultMaxNameLength = 255
	DefaultMountPoint    = "./mnt"
	DefaultAllowOthers   = false

	// Client cache defaults
	DefaultBufferSize         = 20 * 1024 * 1024
	DefaultMetadataCacheTTL   = 5 * time.Minute
	DefaultChunkLocCacheTTL   = 10 * time.Minute
	DefaultMaxMetadataEntries = 10000
	DefaultChunkTimeout       = 8 * time.Second
	DefaultMaxRetries         = 2
	DefaultRPCTimeout         = 5 * time.Second
	DefaultConnIdleTimeout    = 1 * time.Minute

	// Attr Cache
	DefaultAttrCacheTimeout = 5 * time.Minute
	DefaultDirCacheTimeout  = 5 * time.Minute

	// Client write defauclts
	DefaultWriteBatchSize = 1024 * 1024 // 1MB

	// gRPC Settings
	DefaultMaxGRPCMessageSize = 128 * 1024 * 1024 // 128MB for large chunk transfers

	// Log and persistence defaults
	DefaultMaxLogFileSize  = 64 * 1024 * 1024 // 64MB
	DefaultLogRotationSize = 32 * 1024 * 1024 // 32MB
	DefaultLogBufferSize   = 1024 * 1024      // 1MB
	DefaultCheckpointDir   = "./checkpoints"
	DefaultRecoveryTimeout = 15 * time.Second
	DefaultLogSyncInterval = 2 * time.Second
	DefaultLogVersion      = 1
	LogFileMagic           = 0x4746534C // "GFSL" - GFS Log

	// Service lifecycle
	DefaultShutdownTimeout = 15 * time.Second
	DefaultStartupTimeout  = 5 * time.Second
)

// Service states
type ServiceState int

const (
	ServiceStateUnknown ServiceState = iota
	ServiceStateStarting
	ServiceStateRunning
	ServiceStateStopping
	ServiceStateStopped
	ServiceStateError
)
