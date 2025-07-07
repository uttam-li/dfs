package persistence

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/uttam-li/dfs/api/generated/common"
	"github.com/uttam-li/dfs/api/generated/persistence"
	commonConfig "github.com/uttam-li/dfs/pkg/common"
	"google.golang.org/protobuf/proto"
)

const (
	// Log file constants
	LogFileVersion = 1
	LogFileExt     = ".log"

	// Recovery constants
	MaxRecoveryRetries = 3
	RecoveryBatchSize  = 1000
)

var (
	ErrInvalidLogEntry   = fmt.Errorf("invalid log entry")
	ErrChecksumMismatch  = fmt.Errorf("checksum mismatch")
	ErrInvalidLogVersion = fmt.Errorf("invalid log version")
	ErrCorruptedLog      = fmt.Errorf("log file is corrupted")
	ErrInvalidMagic      = fmt.Errorf("invalid log file magic number")
	ErrLogNotStarted     = fmt.Errorf("log manager not started")
	ErrLogAlreadyStarted = fmt.Errorf("log manager already started")
)

// LogManager handles writing and reading operation logs with recovery capabilities
type LogManager struct {
	config        *commonConfig.MasterConfig
	logDir        string
	checkpointDir string
	nodeID        string

	// Current log file
	currentLog *os.File
	writer     *bufio.Writer
	seqNum     uint64
	logSize    int64

	// Buffering
	logBuffer  []*persistence.LogEntry
	bufferSize int
	lastSync   time.Time

	// Synchronization
	mutex       sync.RWMutex
	bufferMutex sync.Mutex

	// Service lifecycle
	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	running bool

	// Callbacks
	recoveryCallback func(*persistence.LogEntry) error

	// Metrics
	totalEntries      uint64
	totalBytes        uint64
	lastCheckpointSeq uint64
}

// NewLogManager creates a new log manager with the given configuration
func NewLogManager(config *commonConfig.MasterConfig) (*LogManager, error) {
	if config == nil {
		return nil, fmt.Errorf("config cannot be nil")
	}

	// Generate or load node ID
	nodeIDBytes := make([]byte, 16)
	if _, err := rand.Read(nodeIDBytes); err != nil {
		return nil, fmt.Errorf("failed to generate node ID: %w", err)
	}
	nodeID := fmt.Sprintf("%x", nodeIDBytes)

	// Create directories
	if err := os.MkdirAll(config.LogDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create log directory: %w", err)
	}
	if err := os.MkdirAll(config.CheckpointDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create checkpoint directory: %w", err)
	}

	return &LogManager{
		config:        config,
		logDir:        config.LogDir,
		checkpointDir: config.CheckpointDir,
		nodeID:        nodeID,
		logBuffer:     make([]*persistence.LogEntry, 0, config.LogBufferSize),
		bufferSize:    config.LogBufferSize,
		lastSync:      time.Now(),
	}, nil
}

// Start starts the log manager
func (lm *LogManager) Start(ctx context.Context) error {
	lm.mutex.Lock()
	defer lm.mutex.Unlock()

	if lm.running {
		return ErrLogAlreadyStarted
	}

	lm.ctx, lm.cancel = context.WithCancel(ctx)

	// Initialize log file
	if err := lm.initializeLogFile(); err != nil {
		return fmt.Errorf("failed to initialize log file: %w", err)
	}

	// Start background routines
	lm.wg.Add(2)
	go lm.syncRoutine()
	go lm.checkpointRoutine()

	lm.running = true
	log.Printf("Log manager started with node ID: %s", lm.nodeID)
	return nil
}

// Stop stops the log manager gracefully
func (lm *LogManager) Stop() error {
	lm.mutex.Lock()
	defer lm.mutex.Unlock()

	if !lm.running {
		return nil
	}

	// Cancel context and wait for routines
	lm.cancel()
	lm.wg.Wait()

	// Flush remaining entries
	if err := lm.flushBuffer(); err != nil {
		log.Printf("Failed to flush buffer during shutdown: %v", err)
	}

	// Close current log file
	if lm.writer != nil {
		lm.writer.Flush()
	}
	if lm.currentLog != nil {
		lm.currentLog.Close()
	}

	lm.running = false
	log.Printf("Log manager stopped")
	return nil
}

// WriteLogEntry writes a log entry to the buffer
func (lm *LogManager) WriteLogEntry(entry *persistence.LogEntry) error {
	if entry == nil {
		return fmt.Errorf("log entry cannot be nil")
	}

	lm.mutex.RLock()
	if !lm.running {
		lm.mutex.RUnlock()
		return ErrLogNotStarted
	}
	lm.mutex.RUnlock()

	// Set sequence number and timestamp
	lm.bufferMutex.Lock()
	lm.seqNum++
	entry.SequenceNumber = lm.seqNum
	entry.Timestamp = time.Now().UnixNano()

	// Calculate checksum
	data, err := proto.Marshal(entry)
	if err != nil {
		lm.bufferMutex.Unlock()
		return fmt.Errorf("failed to marshal log entry: %w", err)
	}
	entry.Checksum = crc32.ChecksumIEEE(data)

	// Add to buffer
	lm.logBuffer = append(lm.logBuffer, entry)

	// Check if buffer is full or sync interval reached
	shouldFlush := len(lm.logBuffer) >= lm.bufferSize ||
		time.Since(lm.lastSync) >= lm.config.LogSyncInterval

	lm.bufferMutex.Unlock()

	if shouldFlush {
		return lm.flushBuffer()
	}

	return nil
}

// flushBuffer writes buffered entries to disk
func (lm *LogManager) flushBuffer() error {
	lm.bufferMutex.Lock()
	defer lm.bufferMutex.Unlock()

	if len(lm.logBuffer) == 0 {
		return nil
	}

	// Write entries to file
	for _, entry := range lm.logBuffer {
		if err := lm.writeEntryToFile(entry); err != nil {
			return fmt.Errorf("failed to write entry to file: %w", err)
		}
	}

	// Flush writer
	if err := lm.writer.Flush(); err != nil {
		return fmt.Errorf("failed to flush writer: %w", err)
	}

	// Sync to disk
	if err := lm.currentLog.Sync(); err != nil {
		return fmt.Errorf("failed to sync log file: %w", err)
	}

	// Clear buffer
	lm.logBuffer = lm.logBuffer[:0]
	lm.lastSync = time.Now()

	return nil
}

// writeEntryToFile writes a single entry to the current log file
func (lm *LogManager) writeEntryToFile(entry *persistence.LogEntry) error {
	data, err := proto.Marshal(entry)
	if err != nil {
		return fmt.Errorf("failed to marshal entry: %w", err)
	}

	// Write entry size (4 bytes)
	size := uint32(len(data))
	if err := binary.Write(lm.writer, binary.LittleEndian, size); err != nil {
		return fmt.Errorf("failed to write entry size: %w", err)
	}

	// Write entry data
	if _, err := lm.writer.Write(data); err != nil {
		return fmt.Errorf("failed to write entry data: %w", err)
	}

	lm.logSize += int64(4 + len(data))
	lm.totalEntries++
	lm.totalBytes += uint64(4 + len(data))

	// Check if log rotation is needed
	if lm.logSize >= lm.config.MaxLogFileSize {
		return lm.rotateLogFile()
	}

	return nil
}

// initializeLogFile finds an existing log file to reuse or creates a new one
func (lm *LogManager) initializeLogFile() error {
	// First, try to find the most recent log file that's not full
	logFiles, err := lm.GetLogFileList()
	if err != nil {
		log.Printf("Warning: failed to get existing log files: %v", err)
	} else if len(logFiles) > 0 {
		// Try to reuse the most recent log file
		mostRecentLog := logFiles[len(logFiles)-1]
		if lm.tryReuseLogFile(mostRecentLog) {
			return nil
		}
	}

	// No suitable existing log file found, create a new one
	return lm.createNewLogFile()
}

// tryReuseLogFile attempts to reuse an existing log file if it's not full
func (lm *LogManager) tryReuseLogFile(logFile string) bool {
	// Check file size first
	fileInfo, err := os.Stat(logFile)
	if err != nil {
		log.Printf("Failed to stat log file %s: %v", logFile, err)
		return false
	}

	// If file is already at or near max size, don't reuse it
	if fileInfo.Size() >= lm.config.MaxLogFileSize {
		log.Printf("Log file %s is full (%d bytes), creating new one", logFile, fileInfo.Size())
		return false
	}

	// Try to open the file for appending
	file, err := os.OpenFile(logFile, os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		log.Printf("Failed to open log file %s for appending: %v", logFile, err)
		return false
	}

	// Validate the log file structure
	if !lm.validateLogFileForReuse(logFile) {
		file.Close()
		return false
	}

	// Update sequence number by reading the last entry
	lastSeq, err := lm.getLastSequenceNumber(logFile)
	if err != nil {
		log.Printf("Failed to get last sequence number from %s: %v", logFile, err)
		file.Close()
		return false
	}

	lm.currentLog = file
	lm.writer = bufio.NewWriter(file)
	lm.logSize = fileInfo.Size()
	lm.seqNum = lastSeq

	log.Printf("Reusing existing log file: %s (size: %d bytes, last seq: %d)",
		filepath.Base(logFile), fileInfo.Size(), lastSeq)
	return true
}

// validateLogFileForReuse checks if a log file has a valid structure for reuse
func (lm *LogManager) validateLogFileForReuse(logFile string) bool {
	file, err := os.Open(logFile)
	if err != nil {
		return false
	}
	defer file.Close()

	reader := bufio.NewReader(file)

	// Read and validate header
	var headerSize uint32
	if err := binary.Read(reader, binary.LittleEndian, &headerSize); err != nil {
		return false
	}

	if headerSize == 0 || headerSize > 1024*1024 {
		return false
	}

	headerData := make([]byte, headerSize)
	if _, err := io.ReadFull(reader, headerData); err != nil {
		return false
	}

	var header persistence.LogFileHeader
	if err := proto.Unmarshal(headerData, &header); err != nil {
		return false
	}

	return header.Magic == commonConfig.LogFileMagic
}

// getLastSequenceNumber reads the last sequence number from a log file
func (lm *LogManager) getLastSequenceNumber(logFile string) (uint64, error) {
	file, err := os.Open(logFile)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	reader := bufio.NewReader(file)

	// Skip header
	var headerSize uint32
	if err := binary.Read(reader, binary.LittleEndian, &headerSize); err != nil {
		return 0, err
	}

	if _, err := reader.Discard(int(headerSize)); err != nil {
		return 0, err
	}

	var lastSeq uint64 = 0

	// Read through all entries to find the last sequence number
	for {
		var entrySize uint32
		if err := binary.Read(reader, binary.LittleEndian, &entrySize); err != nil {
			if err == io.EOF {
				break
			}
			return 0, err
		}

		entryData := make([]byte, entrySize)
		if _, err := io.ReadFull(reader, entryData); err != nil {
			return 0, err
		}

		var entry persistence.LogEntry
		if err := proto.Unmarshal(entryData, &entry); err != nil {
			return 0, err
		}

		if entry.SequenceNumber > lastSeq {
			lastSeq = entry.SequenceNumber
		}
	}

	return lastSeq, nil
}

// createNewLogFile creates a completely new log file
func (lm *LogManager) createNewLogFile() error {
	timestamp := time.Now().Unix()
	logFileName := filepath.Join(lm.logDir, fmt.Sprintf("master_%d%s", timestamp, LogFileExt))

	file, err := os.OpenFile(logFileName, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0644)
	if err != nil {
		return fmt.Errorf("failed to create log file: %w", err)
	}

	lm.currentLog = file
	lm.writer = bufio.NewWriter(file)
	lm.logSize = 0

	// Write log file header
	header := &persistence.LogFileHeader{
		Magic:         commonConfig.LogFileMagic,
		Version:       LogFileVersion,
		CreatedAt:     time.Now().UnixNano(),
		NodeId:        lm.nodeID,
		StartSequence: lm.seqNum,
	}

	headerData, err := proto.Marshal(header)
	if err != nil {
		return fmt.Errorf("failed to marshal header: %w", err)
	}

	// Write header size and data
	headerSize := uint32(len(headerData))
	if err := binary.Write(lm.writer, binary.LittleEndian, headerSize); err != nil {
		return fmt.Errorf("failed to write header size: %w", err)
	}

	if _, err := lm.writer.Write(headerData); err != nil {
		return fmt.Errorf("failed to write header: %w", err)
	}

	lm.logSize = int64(4 + len(headerData))

	log.Printf("Created new log file: %s", logFileName)
	return nil
}

// rotateLogFile creates a new log file when the current one is full
func (lm *LogManager) rotateLogFile() error {
	// Close current file
	if lm.writer != nil {
		lm.writer.Flush()
	}
	if lm.currentLog != nil {
		lm.currentLog.Close()
	}

	// Create new log file (always create new for rotation)
	return lm.createNewLogFile()
}

// syncRoutine periodically syncs buffered entries
func (lm *LogManager) syncRoutine() {
	defer lm.wg.Done()

	ticker := time.NewTicker(lm.config.LogSyncInterval)
	defer ticker.Stop()

	for {
		select {
		case <-lm.ctx.Done():
			return
		case <-ticker.C:
			if err := lm.flushBuffer(); err != nil {
				log.Printf("Failed to sync log buffer: %v", err)
			}
		}
	}
}

// checkpointRoutine periodically creates checkpoints
func (lm *LogManager) checkpointRoutine() {
	defer lm.wg.Done()

	ticker := time.NewTicker(lm.config.CheckpointInterval)
	defer ticker.Stop()

	for {
		select {
		case <-lm.ctx.Done():
			return
		case <-ticker.C:
			if err := lm.CreateCheckpoint(); err != nil {
				log.Printf("Failed to create checkpoint: %v", err)
			}
		}
	}
}

// CreateCheckpoint creates a checkpoint of the current state
func (lm *LogManager) CreateCheckpoint() error {
	lm.mutex.Lock()
	defer lm.mutex.Unlock()

	// First, flush any pending entries
	if err := lm.flushBuffer(); err != nil {
		return fmt.Errorf("failed to flush buffer before checkpoint: %w", err)
	}

	timestamp := time.Now()
	checkpointFile := filepath.Join(lm.checkpointDir,
		fmt.Sprintf("checkpoint_%d.pb", timestamp.Unix()))

	// Create checkpoint metadata
	metadata := &persistence.CheckpointMetadata{
		Timestamp:      timestamp.UnixNano(),
		SequenceNumber: lm.seqNum,
		LogFilePath:    lm.currentLog.Name(),
		LogFileOffset:  uint64(lm.logSize),
		Checksum:       "", // Will be calculated after writing
		Metadata: map[string]string{
			"node_id":       lm.nodeID,
			"total_entries": fmt.Sprintf("%d", lm.totalEntries),
			"total_bytes":   fmt.Sprintf("%d", lm.totalBytes),
		},
	}

	// Write checkpoint file
	checkpointData, err := proto.Marshal(metadata)
	if err != nil {
		return fmt.Errorf("failed to marshal checkpoint metadata: %w", err)
	}

	if err := os.WriteFile(checkpointFile, checkpointData, 0644); err != nil {
		return fmt.Errorf("failed to write checkpoint file: %w", err)
	}

	// Calculate and update checksum
	checksum := fmt.Sprintf("%x", crc32.ChecksumIEEE(checkpointData))
	metadata.Checksum = checksum

	// Rewrite with checksum
	checkpointData, _ = proto.Marshal(metadata)
	if err := os.WriteFile(checkpointFile, checkpointData, 0644); err != nil {
		return fmt.Errorf("failed to update checkpoint with checksum: %w", err)
	}

	lm.lastCheckpointSeq = lm.seqNum
	log.Printf("Created checkpoint at sequence %d: %s", lm.seqNum, checkpointFile)

	return nil
}

// SetRecoveryCallback sets the callback function for processing recovered entries
func (lm *LogManager) SetRecoveryCallback(callback func(*persistence.LogEntry) error) {
	lm.recoveryCallback = callback
}

// LogCreateInode logs the creation of a new inode
func (lm *LogManager) LogCreateInode(inodeID, parentID uint64, name string, attrs *common.FileAttributes, isDirectory bool, target string) error {
	entry := &persistence.LogEntry{
		Type:        persistence.LogEntryType_LOG_ENTRY_CREATE_INODE,
		OperationId: fmt.Sprintf("create_inode_%d_%d_%s", inodeID, parentID, name),
		Data: &persistence.LogEntry_CreateInode{
			CreateInode: &persistence.CreateInodeLogEntry{
				InodeId:     inodeID,
				ParentId:    parentID,
				Name:        name,
				Attributes:  attrs,
				IsDirectory: isDirectory,
				Target:      target,
			},
		},
	}
	return lm.WriteLogEntry(entry)
}

// LogDeleteInode logs the deletion of an inode
func (lm *LogManager) LogDeleteInode(inodeID, parentID uint64, name string) error {
	entry := &persistence.LogEntry{
		Type:        persistence.LogEntryType_LOG_ENTRY_DELETE_INODE,
		OperationId: fmt.Sprintf("delete_inode_%d_%d_%s", inodeID, parentID, name),
		Data: &persistence.LogEntry_DeleteInode{
			DeleteInode: &persistence.DeleteInodeLogEntry{
				InodeId:  inodeID,
				ParentId: parentID,
				Name:     name,
			},
		},
	}
	return lm.WriteLogEntry(entry)
}

// LogUpdateInode logs the update of an inode's attributes
func (lm *LogManager) LogUpdateInode(inodeID uint64, attrs *common.FileAttributes, fieldMask []string) error {
	entry := &persistence.LogEntry{
		Type:        persistence.LogEntryType_LOG_ENTRY_UPDATE_INODE,
		OperationId: fmt.Sprintf("update_inode_%d_%d", inodeID, time.Now().UnixNano()),
		Data: &persistence.LogEntry_UpdateInode{
			UpdateInode: &persistence.UpdateInodeLogEntry{
				InodeId:    inodeID,
				Attributes: attrs,
				FieldMask:  fieldMask,
			},
		},
	}
	return lm.WriteLogEntry(entry)
}

// LogCreateChunk logs the creation of a new chunk
func (lm *LogManager) LogCreateChunk(chunkHandle string, inodeID, chunkIndex uint64, version uint32, locations []*common.ChunkLocation, size int64, checksum string) error {
	entry := &persistence.LogEntry{
		Type:        persistence.LogEntryType_LOG_ENTRY_CREATE_CHUNK,
		OperationId: fmt.Sprintf("create_chunk_%s_%d_%d", chunkHandle, inodeID, chunkIndex),
		Data: &persistence.LogEntry_CreateChunk{
			CreateChunk: &persistence.CreateChunkLogEntry{
				ChunkHandle: chunkHandle,
				InodeId:     inodeID,
				ChunkIndex:  chunkIndex,
				Version:     version,
				Locations:   locations,
				Size:        size,
				Checksum:    checksum,
			},
		},
	}
	return lm.WriteLogEntry(entry)
}

// LogDeleteChunk logs the deletion of a chunk
func (lm *LogManager) LogDeleteChunk(chunkHandle string, inodeID, chunkIndex uint64) error {
	entry := &persistence.LogEntry{
		Type:        persistence.LogEntryType_LOG_ENTRY_DELETE_CHUNK,
		OperationId: fmt.Sprintf("delete_chunk_%s_%d_%d", chunkHandle, inodeID, chunkIndex),
		Data: &persistence.LogEntry_DeleteChunk{
			DeleteChunk: &persistence.DeleteChunkLogEntry{
				ChunkHandle: chunkHandle,
				InodeId:     inodeID,
				ChunkIndex:  chunkIndex,
			},
		},
	}
	return lm.WriteLogEntry(entry)
}

// LogUpdateChunk logs the update of a chunk
func (lm *LogManager) LogUpdateChunk(chunkHandle string, version uint32, locations []*common.ChunkLocation, size int64, checksum string) error {
	entry := &persistence.LogEntry{
		Type:        persistence.LogEntryType_LOG_ENTRY_UPDATE_CHUNK,
		OperationId: fmt.Sprintf("update_chunk_%s_%d", chunkHandle, time.Now().UnixNano()),
		Data: &persistence.LogEntry_UpdateChunk{
			UpdateChunk: &persistence.UpdateChunkLogEntry{
				ChunkHandle: chunkHandle,
				Version:     version,
				Locations:   locations,
				Size:        size,
				Checksum:    checksum,
			},
		},
	}
	return lm.WriteLogEntry(entry)
}

// LogLeaseGrant logs the granting of a lease
func (lm *LogManager) LogLeaseGrant(chunkHandle string, primaryServer string, replicaServers []string, leaseExpiry int64, version uint32) error {
	entry := &persistence.LogEntry{
		Type:        persistence.LogEntryType_LOG_ENTRY_LEASE_GRANT,
		OperationId: fmt.Sprintf("lease_grant_%s_%s", chunkHandle, primaryServer),
		Data: &persistence.LogEntry_LeaseGrant{
			LeaseGrant: &persistence.LeaseGrantLogEntry{
				ChunkHandle:    chunkHandle,
				PrimaryServer:  primaryServer,
				ReplicaServers: replicaServers,
				LeaseExpiry:    leaseExpiry,
				Version:        version,
			},
		},
	}
	return lm.WriteLogEntry(entry)
}

// LogLeaseRevoke logs the revocation of a lease
func (lm *LogManager) LogLeaseRevoke(chunkHandle string, primaryServer string, version uint32) error {
	entry := &persistence.LogEntry{
		Type:        persistence.LogEntryType_LOG_ENTRY_LEASE_REVOKE,
		OperationId: fmt.Sprintf("lease_revoke_%s_%s", chunkHandle, primaryServer),
		Data: &persistence.LogEntry_LeaseRevoke{
			LeaseRevoke: &persistence.LeaseRevokeLogEntry{
				ChunkHandle:   chunkHandle,
				PrimaryServer: primaryServer,
				Version:       version,
			},
		},
	}
	return lm.WriteLogEntry(entry)
}

// LogMkdir logs directory creation
func (lm *LogManager) LogMkdir(parentID, inodeID uint64, name string, attrs *common.FileAttributes) error {
	entry := &persistence.LogEntry{
		Type:        persistence.LogEntryType_LOG_ENTRY_MKDIR,
		OperationId: fmt.Sprintf("mkdir_%d_%d_%s", parentID, inodeID, name),
		Data: &persistence.LogEntry_Mkdir{
			Mkdir: &persistence.MkdirLogEntry{
				ParentId:   parentID,
				Name:       name,
				InodeId:    inodeID,
				Attributes: attrs,
			},
		},
	}
	return lm.WriteLogEntry(entry)
}

// LogRmdir logs directory removal
func (lm *LogManager) LogRmdir(parentID, inodeID uint64, name string) error {
	entry := &persistence.LogEntry{
		Type:        persistence.LogEntryType_LOG_ENTRY_RMDIR,
		OperationId: fmt.Sprintf("rmdir_%d_%d_%s", parentID, inodeID, name),
		Data: &persistence.LogEntry_Rmdir{
			Rmdir: &persistence.RmdirLogEntry{
				ParentId: parentID,
				Name:     name,
				InodeId:  inodeID,
			},
		},
	}
	return lm.WriteLogEntry(entry)
}

// LogRename logs file/directory rename
func (lm *LogManager) LogRename(oldParentID, newParentID, inodeID uint64, oldName, newName string) error {
	entry := &persistence.LogEntry{
		Type:        persistence.LogEntryType_LOG_ENTRY_RENAME,
		OperationId: fmt.Sprintf("rename_%d_%s_%d_%s", oldParentID, oldName, newParentID, newName),
		Data: &persistence.LogEntry_Rename{
			Rename: &persistence.RenameLogEntry{
				OldParentId: oldParentID,
				OldName:     oldName,
				NewParentId: newParentID,
				NewName:     newName,
				InodeId:     inodeID,
			},
		},
	}
	return lm.WriteLogEntry(entry)
}

// LogSymlink logs symlink creation
func (lm *LogManager) LogSymlink(parentID, inodeID uint64, name, target string, attrs *common.FileAttributes) error {
	entry := &persistence.LogEntry{
		Type:        persistence.LogEntryType_LOG_ENTRY_SYMLINK,
		OperationId: fmt.Sprintf("symlink_%d_%d_%s", parentID, inodeID, name),
		Data: &persistence.LogEntry_Symlink{
			Symlink: &persistence.SymlinkLogEntry{
				ParentId:   parentID,
				Name:       name,
				Target:     target,
				InodeId:    inodeID,
				Attributes: attrs,
			},
		},
	}
	return lm.WriteLogEntry(entry)
}

// LogUnlink logs file unlink
func (lm *LogManager) LogUnlink(parentID, inodeID uint64, name string) error {
	entry := &persistence.LogEntry{
		Type:        persistence.LogEntryType_LOG_ENTRY_UNLINK,
		OperationId: fmt.Sprintf("unlink_%d_%d_%s", parentID, inodeID, name),
		Data: &persistence.LogEntry_Unlink{
			Unlink: &persistence.UnlinkLogEntry{
				ParentId: parentID,
				Name:     name,
				InodeId:  inodeID,
			},
		},
	}
	return lm.WriteLogEntry(entry)
}

// LogSetattr logs attribute setting
func (lm *LogManager) LogSetattr(inodeID uint64, attrs *common.FileAttributes, fieldMask []string) error {
	entry := &persistence.LogEntry{
		Type:        persistence.LogEntryType_LOG_ENTRY_SETATTR,
		OperationId: fmt.Sprintf("setattr_%d_%d", inodeID, time.Now().UnixNano()),
		Data: &persistence.LogEntry_Setattr{
			Setattr: &persistence.SetattrLogEntry{
				InodeId:    inodeID,
				Attributes: attrs,
				FieldMask:  fieldMask,
			},
		},
	}
	return lm.WriteLogEntry(entry)
}

// LogSetxattr logs extended attribute setting
func (lm *LogManager) LogSetxattr(inodeID uint64, name string, value []byte, flags uint32) error {
	entry := &persistence.LogEntry{
		Type:        persistence.LogEntryType_LOG_ENTRY_SETXATTR,
		OperationId: fmt.Sprintf("setxattr_%d_%s_%d", inodeID, name, time.Now().UnixNano()),
		Data: &persistence.LogEntry_Setxattr{
			Setxattr: &persistence.SetxattrLogEntry{
				InodeId: inodeID,
				Name:    name,
				Value:   value,
				Flags:   flags,
			},
		},
	}
	return lm.WriteLogEntry(entry)
}

// LogRemovexattr logs extended attribute removal
func (lm *LogManager) LogRemovexattr(inodeID uint64, name string) error {
	entry := &persistence.LogEntry{
		Type:        persistence.LogEntryType_LOG_ENTRY_REMOVEXATTR,
		OperationId: fmt.Sprintf("removexattr_%d_%s_%d", inodeID, name, time.Now().UnixNano()),
		Data: &persistence.LogEntry_Removexattr{
			Removexattr: &persistence.RemovexattrLogEntry{
				InodeId: inodeID,
				Name:    name,
			},
		},
	}
	return lm.WriteLogEntry(entry)
}

// CleanOldLogs removes old log files based on retention policy
func (lm *LogManager) CleanOldLogs() error {
	lm.mutex.Lock()
	defer lm.mutex.Unlock()

	files, err := os.ReadDir(lm.logDir)
	if err != nil {
		return fmt.Errorf("failed to read log directory: %w", err)
	}

	cutoffTime := time.Now().Add(-lm.config.LogRetentionPeriod)

	for _, file := range files {
		if !strings.HasSuffix(file.Name(), LogFileExt) {
			continue
		}

		filePath := filepath.Join(lm.logDir, file.Name())
		fileInfo, err := file.Info()
		if err != nil {
			log.Printf("Failed to get file info for %s: %v", filePath, err)
			continue
		}

		if fileInfo.ModTime().Before(cutoffTime) {
			if err := os.Remove(filePath); err != nil {
				log.Printf("Failed to remove old log file %s: %v", filePath, err)
			} else {
				log.Printf("Removed old log file: %s", filePath)
			}
		}
	}

	return nil
}

// ForceSync forces a sync of buffered entries to disk
func (lm *LogManager) ForceSync() error {
	return lm.flushBuffer()
}

// GetLogFileList returns a list of all log files
func (lm *LogManager) GetLogFileList() ([]string, error) {
	files, err := os.ReadDir(lm.logDir)
	if err != nil {
		return nil, fmt.Errorf("failed to read log directory: %w", err)
	}

	var logFiles []string
	for _, file := range files {
		if strings.HasSuffix(file.Name(), LogFileExt) {
			logFiles = append(logFiles, filepath.Join(lm.logDir, file.Name()))
		}
	}

	sort.Strings(logFiles)
	return logFiles, nil
}

// ValidateLogFile validates the integrity of a log file
func (lm *LogManager) ValidateLogFile(logFile string) error {
	file, err := os.Open(logFile)
	if err != nil {
		return fmt.Errorf("failed to open log file: %w", err)
	}
	defer file.Close()

	// Check if file is empty
	fileInfo, err := file.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat log file: %w", err)
	}

	if fileInfo.Size() == 0 {
		return fmt.Errorf("log file is empty")
	}

	reader := bufio.NewReader(file)
	entriesValidated := 0

	// Read header size first
	var headerSize uint32
	if err := binary.Read(reader, binary.LittleEndian, &headerSize); err != nil {
		if err == io.EOF {
			return fmt.Errorf("unexpected EOF while reading header size")
		}
		return fmt.Errorf("failed to read header size: %w", err)
	}

	// Validate header size is reasonable
	if headerSize == 0 || headerSize > 1024*1024 { // Max 1MB header
		return fmt.Errorf("invalid header size: %d", headerSize)
	}

	// Read and validate header
	headerData := make([]byte, headerSize)
	if _, err := io.ReadFull(reader, headerData); err != nil {
		return fmt.Errorf("failed to read header data: %w", err)
	}

	var header persistence.LogFileHeader
	if err := proto.Unmarshal(headerData, &header); err != nil {
		return fmt.Errorf("invalid log file header: %w", err)
	}

	// Validate header magic
	if header.Magic != commonConfig.LogFileMagic {
		return fmt.Errorf("invalid log file magic")
	}

	// Validate each entry
	for {
		var entrySize uint32
		if err := binary.Read(reader, binary.LittleEndian, &entrySize); err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("failed to read entry size: %w", err)
		}

		entryData := make([]byte, entrySize)
		if _, err := io.ReadFull(reader, entryData); err != nil {
			return fmt.Errorf("failed to read entry data: %w", err)
		}

		var entry persistence.LogEntry
		if err := proto.Unmarshal(entryData, &entry); err != nil {
			return fmt.Errorf("invalid log entry at position %d: %w", entriesValidated, err)
		}

		// Validate checksum
		tempEntry := persistence.LogEntry{
			SequenceNumber: entry.SequenceNumber,
			Timestamp:      entry.Timestamp,
			Type:           entry.Type,
			OperationId:    entry.OperationId,
			Checksum:       0, // Reset for validation
			Data:           entry.Data,
		}
		tempData, _ := proto.Marshal(&tempEntry)
		expectedChecksum := crc32.ChecksumIEEE(tempData)

		if entry.Checksum != expectedChecksum {
			return fmt.Errorf("checksum mismatch at entry %d", entriesValidated)
		}

		entriesValidated++
	}

	log.Printf("Log file %s validated successfully: %d entries", logFile, entriesValidated)
	return nil
}

// ReplayLogsFromSequence replays log entries starting from a specific sequence number
func (lm *LogManager) ReplayLogsFromSequence(startSeq uint64, callback func(*persistence.LogEntry) error) error {
	if callback == nil {
		return fmt.Errorf("callback cannot be nil")
	}

	logFiles, err := lm.GetLogFileList()
	if err != nil {
		return fmt.Errorf("failed to get log files: %w", err)
	}

	if len(logFiles) == 0 {
		log.Printf("No log files found for replay")
		return nil
	}

	entriesReplayed := 0
	filesProcessed := 0
	for _, logFile := range logFiles {
		entries, err := lm.replayLogFileFromSequence(logFile, startSeq, callback)
		if err != nil {
			// Only log actual errors, not empty/new files or EOF conditions
			if err != io.EOF && !strings.Contains(err.Error(), "EOF") && !strings.Contains(err.Error(), "empty") {
				log.Printf("Failed to replay log file %s: %v", logFile, err)
			}
			continue
		}
		if entries > 0 {
			log.Printf("Replayed %d entries from log file: %s", entries, filepath.Base(logFile))
		}
		entriesReplayed += entries
		filesProcessed++
	}

	log.Printf("Replay completed: %d entries from %d log files (processed %d files)",
		entriesReplayed, len(logFiles), filesProcessed)
	return nil
}

// replayLogFileFromSequence replays entries from a log file starting from a specific sequence
func (lm *LogManager) replayLogFileFromSequence(logFile string, minSeq uint64, callback func(*persistence.LogEntry) error) (int, error) {
	file, err := os.Open(logFile)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	// Check if file is empty
	fileInfo, err := file.Stat()
	if err != nil {
		return 0, fmt.Errorf("failed to stat log file: %w", err)
	}

	if fileInfo.Size() == 0 {
		// Empty file, nothing to replay
		return 0, nil
	}

	reader := bufio.NewReader(file)

	// Read header size
	var headerSize uint32
	if err := binary.Read(reader, binary.LittleEndian, &headerSize); err != nil {
		if err == io.EOF {
			// File only contains partial data or just created, nothing to replay
			return 0, nil
		}
		return 0, fmt.Errorf("failed to read header size: %w", err)
	}

	// Validate header size is reasonable
	if headerSize == 0 || headerSize > 1024*1024 { // Max 1MB header
		return 0, fmt.Errorf("invalid header size: %d", headerSize)
	}

	// Skip header data
	headerData := make([]byte, headerSize)
	if _, err := io.ReadFull(reader, headerData); err != nil {
		if err == io.EOF {
			// Incomplete header, file might be corrupted or still being written
			return 0, nil
		}
		return 0, fmt.Errorf("failed to read header data: %w", err)
	}

	// Validate header
	var header persistence.LogFileHeader
	if err := proto.Unmarshal(headerData, &header); err != nil {
		log.Printf("Warning: corrupted header in log file %s, skipping: %v", logFile, err)
		return 0, nil
	}

	// Validate header magic
	if header.Magic != commonConfig.LogFileMagic {
		log.Printf("Warning: invalid magic in log file %s, skipping", logFile)
		return 0, nil
	}

	entriesReplayed := 0
	for {
		// Read entry size
		var entrySize uint32
		if err := binary.Read(reader, binary.LittleEndian, &entrySize); err != nil {
			if err == io.EOF {
				break
			}
			return entriesReplayed, fmt.Errorf("failed to read entry size: %w", err)
		}

		// Read entry data
		entryData := make([]byte, entrySize)
		if _, err := io.ReadFull(reader, entryData); err != nil {
			return entriesReplayed, fmt.Errorf("failed to read entry data: %w", err)
		}

		// Unmarshal entry
		var entry persistence.LogEntry
		if err := proto.Unmarshal(entryData, &entry); err != nil {
			return entriesReplayed, fmt.Errorf("failed to unmarshal entry: %w", err)
		}

		if entry.SequenceNumber <= minSeq {
			continue
		}

		if err := callback(&entry); err != nil {
			log.Printf("Failed to replay entry %d: %v", entry.SequenceNumber, err)
			continue
		}

		entriesReplayed++
	}

	return entriesReplayed, nil
}
