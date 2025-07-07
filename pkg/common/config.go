package common

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/google/uuid"
)

// CommonConfig contains shared configuration options
type CommonConfig struct {
	Debug bool `json:"debug"`
}

// ClientConfig contains configuration for the DFS client
type ClientConfig struct {
	CommonConfig

	// Connection Settings
	MasterAddr string `json:"master_addr"`
	MountPoint string `json:"mount_point"`

	// FUSE Settings
	AllowOthers bool `json:"allow_others"`

	// Filesystem Settings
	BlockSize     uint32 `json:"block_size"`
	ChunkSize     uint32 `json:"chunk_size"`
	MaxNameLength uint32 `json:"max_name_length"`

	// Cache Settings
	BufferSize         int
	MetadataCacheTTL   time.Duration
	ChunkLocCacheTTL   time.Duration
	MaxMetadataEntries int
	ChunkTimeout       time.Duration
	MaxRetries         int
	RPCTimeout         time.Duration
	ConnIdleTimeout    time.Duration

	// Attr Cache
	AttrCacheTimeout time.Duration
	DirCacheTimeout  time.Duration

	// Write Settings
	WriteBatchSize int // 1MB default, tunable
	WriteBuffers   map[uint64]*WriteBufferState
}

type WriteBufferState struct {
	Ino         uint64
	ChunkIndex  uint64
	ChunkOffset uint64
	Data        []byte
	LastWrite   time.Time
}

// MasterConfig contains configuration for the master server
type MasterConfig struct {
	CommonConfig

	// Server Settings
	Port string `json:"port"`

	// Replication Settings
	ReplicationFactor int    `json:"replication_factor"`
	ChunkSize         int64  `json:"chunk_size"`
	MaxInodes         uint64 `json:"max_inodes"`

	// Timing Settings
	HeartbeatInterval        time.Duration `json:"heartbeat_interval"`
	HeartbeatTimeout         time.Duration `json:"heartbeat_timeout"`
	LeaseTimeout             time.Duration `json:"lease_timeout"`
	CheckpointInterval       time.Duration `json:"checkpoint_interval"`
	GCInterval               time.Duration `json:"gc_interval"`
	OrphanedChunkGracePeriod time.Duration `json:"orphaned_chunk_grace_period"`

	// Storage Settings
	LogDir        string `json:"log_dir"`
	CheckpointDir string `json:"checkpoint_dir"`
	RootInode     uint64 `json:"root_inode"`

	// Log and Recovery Settings
	MaxLogFileSize     int64         `json:"max_log_file_size"`
	LogBufferSize      int           `json:"log_buffer_size"`
	LogSyncInterval    time.Duration `json:"log_sync_interval"`
	RecoveryTimeout    time.Duration `json:"recovery_timeout"`
	EnableLogRecovery  bool          `json:"enable_log_recovery"`
	LogRetentionPeriod time.Duration `json:"log_retention_period"`

	// Service Lifecycle
	ShutdownTimeout time.Duration `json:"shutdown_timeout"`
	StartupTimeout  time.Duration `json:"startup_timeout"`
}

// ChunkServerConfig contains configuration for chunk servers
type ChunkServerConfig struct {
	CommonConfig

	// Identity
	ID uuid.UUID `json:"id"`

	// Network Settings
	GRPCPort          int           `json:"grpc_port"`
	MasterAddr        string        `json:"master_addr"`
	HeartbeatInterval time.Duration `json:"heartbeat_interval"`
	HeartbeatTimeout  time.Duration `json:"heartbeat_timeout"`

	// Storage Settings
	StorageRoot string `json:"storage_root"`
	ChunkSize   int32  `json:"chunk_size"`

	// Service Lifecycle
	ShutdownTimeout time.Duration `json:"shutdown_timeout"`
	StartupTimeout  time.Duration `json:"startup_timeout"`
}

// LoadMasterConfig loads master configuration from environment variables with defaults
func LoadMasterConfig() *MasterConfig {
	config := &MasterConfig{
		CommonConfig: CommonConfig{
			Debug: getBoolEnv("DFS_DEBUG", DefaultDebug),
		},
		Port:                     getStringEnv("DFS_MASTER_PORT", DefaultMasterPort),
		ReplicationFactor:        getIntEnv("DFS_REPLICATION_FACTOR", DefaultReplicationFactor),
		ChunkSize:                int64(getIntEnv("DFS_CHUNK_SIZE", DefaultChunkSize)),
		MaxInodes:                uint64(getIntEnv("MAX_INODES", DefaultMaxInodes)),
		HeartbeatInterval:        getDurationEnv("DFS_HEARTBEAT_INTERVAL", DefaultHeartbeatInterval),
		HeartbeatTimeout:         getDurationEnv("DFS_HEARTBEAT_TIMEOUT", DefaultHeartbeatTimeout),
		LeaseTimeout:             getDurationEnv("DFS_LEASE_TIMEOUT", DefaultLeaseTimeout),
		CheckpointInterval:       getDurationEnv("DFS_CHECKPOINT_INTERVAL", DefaultCheckpointInterval),
		GCInterval:               getDurationEnv("DFS_GC_INTERVAL", DefaultGCInterval),
		OrphanedChunkGracePeriod: getDurationEnv("DFS_ORPHANED_CHUNK_GRACE_PERIOD", DefaultOrphanedChunkGracePeriod),
		LogDir:                   getStringEnv("DFS_LOG_DIR", DefaultLogDir),
		CheckpointDir:            getStringEnv("DFS_CHECKPOINT_DIR", DefaultCheckpointDir),
		RootInode:                uint64(getIntEnv("DFS_ROOT_INODE", DefaultRootInode)),

		// Log and Recovery Settings
		MaxLogFileSize:     int64(getIntEnv("DFS_MAX_LOG_FILE_SIZE", DefaultMaxLogFileSize)),
		LogBufferSize:      getIntEnv("DFS_LOG_BUFFER_SIZE", DefaultLogBufferSize),
		LogSyncInterval:    getDurationEnv("DFS_LOG_SYNC_INTERVAL", DefaultLogSyncInterval),
		RecoveryTimeout:    getDurationEnv("DFS_RECOVERY_TIMEOUT", DefaultRecoveryTimeout),
		EnableLogRecovery:  getBoolEnv("DFS_ENABLE_LOG_RECOVERY", true),
		LogRetentionPeriod: getDurationEnv("DFS_LOG_RETENTION_PERIOD", 7*24*time.Hour), // 7 days

		ShutdownTimeout: getDurationEnv("DFS_SHUTDOWN_TIMEOUT", DefaultShutdownTimeout),
		StartupTimeout:  getDurationEnv("DFS_STARTUP_TIMEOUT", DefaultStartupTimeout),
	}

	return config
}

// LoadChunkServerConfig loads chunk server configuration from environment variables with defaults
func LoadChunkServerConfig() *ChunkServerConfig {
	config := &ChunkServerConfig{
		CommonConfig: CommonConfig{
			Debug: getBoolEnv("DFS_DEBUG", DefaultDebug),
		},
		ID:                uuid.New(),
		GRPCPort:          getIntEnv("DFS_CHUNKSERVER_PORT", DefaultChunkServerPort),
		MasterAddr:        getStringEnv("DFS_MASTER_ADDR", DefaultMasterAddr),
		HeartbeatInterval: getDurationEnv("DFS_HEARTBEAT_INTERVAL", DefaultHeartbeatInterval),
		HeartbeatTimeout:  getDurationEnv("DFS_HEARTBEAT_TIMEOUT", DefaultHeartbeatTimeout),
		StorageRoot:       getStringEnv("DFS_STORAGE_ROOT", DefaultStorageRoot),
		ChunkSize:         int32(getIntEnv("DFS_CHUNK_SIZE", DefaultChunkServerChunkSize)),
		ShutdownTimeout:   getDurationEnv("DFS_SHUTDOWN_TIMEOUT", DefaultShutdownTimeout),
		StartupTimeout:    getDurationEnv("DFS_STARTUP_TIMEOUT", DefaultStartupTimeout),
	}

	return config
}

// LoadClientConfig loads client configuration from environment variables with defaults
func LoadClientConfig() *ClientConfig {
	config := &ClientConfig{
		CommonConfig: CommonConfig{
			Debug: getBoolEnv("DFS_DEBUG", DefaultDebug),
		},
		MasterAddr:    getStringEnv("DFS_MASTER_ADDR", DefaultMasterAddr),
		MountPoint:    getStringEnv("DFS_MOUNT_POINT", DefaultMountPoint),
		AllowOthers:   getBoolEnv("DFS_ALLOW_OTHERS", DefaultAllowOthers),
		BlockSize:     uint32(getIntEnv("DFS_BLOCK_SIZE", DefaultBlockSize)),
		ChunkSize:     uint32(getIntEnv("DFS_CHUNK_SIZE", DefaultChunkSize)),
		MaxNameLength: uint32(getIntEnv("DFS_MAX_NAME_LENGTH", DefaultMaxNameLength)),

		// Cache Settings
		BufferSize:         getIntEnv("DFS_CACHE_SIZE", DefaultBufferSize),
		MetadataCacheTTL:   getDurationEnv("DFS_METADATA_CACHE_TTL", DefaultMetadataCacheTTL),
		ChunkLocCacheTTL:   getDurationEnv("DFS_CHUNK_LOC_CACHE_TTL", DefaultChunkLocCacheTTL),
		MaxMetadataEntries: getIntEnv("DFS_MAX_METADATA_ENTRIES", DefaultMaxMetadataEntries),
		ChunkTimeout:       getDurationEnv("DFS_CHUNK_TIMEOUT", DefaultChunkTimeout),
		MaxRetries:         getIntEnv("DFS_MAX_RETRIES", DefaultMaxRetries),
		RPCTimeout:         getDurationEnv("DFS_RPC_TIMEOUT", DefaultRPCTimeout),
		ConnIdleTimeout:    getDurationEnv("DFS_CONN_IDLE_TIMEOUT", DefaultConnIdleTimeout),

		//Attr Cache
		AttrCacheTimeout: getDurationEnv("ATTR_CACHE_TIMEOUT", DefaultAttrCacheTimeout),
		DirCacheTimeout:  getDurationEnv("DIR_CACHE_TIMEOUT", DefaultDirCacheTimeout),

		// Write Settings
		WriteBatchSize: getIntEnv("DFS_WRITE_BATCH_SIZE", DefaultWriteBatchSize),
		WriteBuffers:   make(map[uint64]*WriteBufferState),
	}

	return config
}

// Validate validates the master configuration
func (c *MasterConfig) Validate() error {
	if c.Port == "" {
		return fmt.Errorf("port cannot be empty")
	}
	if c.ReplicationFactor < 1 {
		return fmt.Errorf("replication factor must be at least 1")
	}
	if c.ChunkSize <= 0 {
		return fmt.Errorf("chunk size must be positive")
	}
	if c.HeartbeatInterval <= 0 {
		return fmt.Errorf("heartbeat interval must be positive")
	}
	if c.HeartbeatTimeout <= c.HeartbeatInterval {
		return fmt.Errorf("heartbeat timeout must be greater than heartbeat interval")
	}
	if c.GCInterval <= 0 {
		return fmt.Errorf("GC interval must be positive")
	}
	if c.OrphanedChunkGracePeriod <= 0 {
		return fmt.Errorf("orphaned chunk grace period must be positive")
	}
	return nil
}

// Validate validates the chunk server configuration
func (c *ChunkServerConfig) Validate() error {
	if c.GRPCPort <= 0 || c.GRPCPort > 65535 {
		return fmt.Errorf("grpc port must be between 1 and 65535")
	}
	if c.MasterAddr == "" {
		return fmt.Errorf("master address cannot be empty")
	}
	if c.StorageRoot == "" {
		return fmt.Errorf("storage root cannot be empty")
	}
	if c.ChunkSize <= 0 {
		return fmt.Errorf("chunk size must be positive")
	}
	return nil
}

// Validate validates the client configuration
func (c *ClientConfig) Validate() error {
	if c.MasterAddr == "" {
		return fmt.Errorf("master address cannot be empty")
	}
	if c.MountPoint == "" {
		return fmt.Errorf("mount point cannot be empty")
	}
	if c.ChunkSize == 0 {
		return fmt.Errorf("chunk size must be positive")
	}
	if c.BlockSize == 0 {
		return fmt.Errorf("block size must be positive")
	}
	return nil
}

// Helper functions for environment variable parsing
func getStringEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getIntEnv(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		if intValue, err := strconv.Atoi(value); err == nil {
			return intValue
		}
	}
	return defaultValue
}

func getBoolEnv(key string, defaultValue bool) bool {
	if value := os.Getenv(key); value != "" {
		if boolValue, err := strconv.ParseBool(value); err == nil {
			return boolValue
		}
	}
	return defaultValue
}

func getDurationEnv(key string, defaultValue time.Duration) time.Duration {
	if value := os.Getenv(key); value != "" {
		if duration, err := time.ParseDuration(value); err == nil {
			return duration
		}
	}
	return defaultValue
}
