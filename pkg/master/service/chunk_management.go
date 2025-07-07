package service

import (
	"fmt"
	"log"
	"time"

	"github.com/google/btree"
	"github.com/uttam-li/dfs/api/generated/common"
	commonTypes "github.com/uttam-li/dfs/pkg/common"
	"github.com/uttam-li/dfs/pkg/master/types"
)

// AllocateChunk allocates a new chunk for a file or returns existing chunk
func (ms *MasterService) AllocateChunk(fileIno uint64, chunkIndex uint64) (string, []*common.ChunkLocation, int64, error) {
	ms.metadataLock.Lock()
	defer ms.metadataLock.Unlock()

	// Find the file inode
	var targetInode *types.Inode
	ms.master.Metadata.Ascend(func(item btree.Item) bool {
		btreeItem := item.(*types.BTreeItem)
		if btreeItem.Inode != nil && btreeItem.Inode.Ino == fileIno && !btreeItem.Inode.IsDir() {
			targetInode = btreeItem.Inode
			return false
		}
		return true
	})

	if targetInode == nil {
		return "", nil, 0, fmt.Errorf("file not found: %d", fileIno)
	}

	// Check if chunk already exists
	if existingHandle, exists := targetInode.Chunks[chunkIndex]; exists {
		// Return existing chunk info
		return ms.getExistingChunkInfo(existingHandle)
	}

	// Create new chunk
	return ms.createNewChunkSimple(fileIno, chunkIndex, targetInode)
}

// getExistingChunkInfo returns info for an existing chunk
func (ms *MasterService) getExistingChunkInfo(chunkHandle commonTypes.ChunkHandle) (string, []*common.ChunkLocation, int64, error) {
	chunkMeta, exists := ms.master.ChunkMetadata[chunkHandle]
	if !exists {
		// Create minimal metadata for existing chunk
		selectedAddresses := ms.selectChunkServerAddresses(int(ms.config.ReplicationFactor))
		if len(selectedAddresses) == 0 {
			return "", nil, 0, fmt.Errorf("no available chunk servers")
		}

		chunkMeta = &types.ChunkMetadata{
			Version:   1,
			Size:      0,
			Locations: selectedAddresses,
		}
		ms.master.ChunkMetadata[chunkHandle] = chunkMeta
	}

	// Build location info
	var locations []*common.ChunkLocation

	// Get current lease information
	lease := ms.leaseManager.GetLease(chunkHandle)
	var leaseExpiry int64
	var primaryServer string

	if lease != nil && lease.State == LeaseStateActive {
		leaseExpiry = lease.ExpireTime.Unix()
		primaryServer = lease.Primary
	} else {
		// No active lease - potentially grant a new one or use fallback
		leaseExpiry = time.Now().Add(ms.config.LeaseTimeout).Unix()
		if len(chunkMeta.Locations) > 0 {
			primaryServer = chunkMeta.Locations[0] // Default to first location as primary
		}
	}

	for _, address := range chunkMeta.Locations {
		location := &common.ChunkLocation{
			ServerId:    ms.getServerIdByAddress(address), // Get server ID for compatibility
			Address:     address,
			Version:     uint32(chunkMeta.Version),
			IsPrimary:   address == primaryServer, // Set based on current lease
			LeaseExpiry: leaseExpiry,
		}
		locations = append(locations, location)
	}

	return chunkHandle.String(), locations, leaseExpiry, nil
}

// createNewChunkSimple creates a new chunk with lease management integration
func (ms *MasterService) createNewChunkSimple(fileIno uint64, chunkIndex uint64, targetInode *types.Inode) (string, []*common.ChunkLocation, int64, error) {
	// Generate new chunk handle
	chunkHandle := commonTypes.NewChunkHandle()

	// Select server addresses
	selectedAddresses := ms.selectChunkServerAddresses(int(ms.config.ReplicationFactor))
	if len(selectedAddresses) == 0 {
		return "", nil, 0, fmt.Errorf("no available chunk servers")
	}

	// Create chunk metadata
	chunkMeta := &types.ChunkMetadata{
		Version:   1,
		Size:      0,
		Locations: selectedAddresses,
	}
	ms.master.ChunkMetadata[chunkHandle] = chunkMeta

	// Update inode chunks map
	targetInode.Chunks[chunkIndex] = chunkHandle

	// Grant lease for the new chunk to the primary server (first in the list)
	primaryServer := selectedAddresses[0]
	replicas := selectedAddresses[1:] // Remaining servers are replicas

	if _, err := ms.GrantChunkLease(chunkHandle, primaryServer, replicas, uint64(chunkMeta.Version)); err != nil {
		log.Printf("Warning: Failed to grant lease for new chunk %s: %v", chunkHandle.String(), err)
		// Continue without lease - chunk can still be allocated
	} else {
		// log.Printf("Granted lease %s for new chunk %s to primary %s", lease.LeaseID, chunkHandle.String(), primaryServer)
	}

	// Build location response
	var locations []*common.ChunkLocation

	// Get lease information for the chunk
	lease := ms.leaseManager.GetLease(chunkHandle)
	var leaseExpiry int64
	if lease != nil {
		leaseExpiry = lease.ExpireTime.Unix()
	} else {
		// Fallback to default lease timeout if no lease found
		leaseExpiry = time.Now().Add(ms.config.LeaseTimeout).Unix()
	}

	for i, address := range selectedAddresses {
		location := &common.ChunkLocation{
			ServerId:    ms.getServerIdByAddress(address), // Get server ID for compatibility
			Address:     address,
			Version:     uint32(chunkMeta.Version),
			IsPrimary:   i == 0, // First is primary
			LeaseExpiry: leaseExpiry,
		}
		locations = append(locations, location)
	}

	// Log the chunk creation for recovery
	if ms.persistenceManager != nil {
		err := ms.persistenceManager.LogCreateChunk(
			chunkHandle.String(),
			fileIno,
			chunkIndex,
			uint32(chunkMeta.Version),
			locations,
			0,  // Initial size is 0
			"", // No checksum initially
		)
		if err != nil {
			log.Printf("Failed to log chunk creation for %s: %v", chunkHandle.String(), err)
		}
	}

	return chunkHandle.String(), locations, leaseExpiry, nil
}

// ReportChunkCorruption handles reports of chunk corruption (simplified)
func (ms *MasterService) ReportChunkCorruption(chunkHandle, reportingServerAddress string) (bool, []string, error) {
	ms.metadataLock.Lock()
	defer ms.metadataLock.Unlock()

	ch, err := commonTypes.ParseChunkHandle(chunkHandle)
	if err != nil {
		return false, nil, fmt.Errorf("invalid chunk handle: %s", chunkHandle)
	}

	chunkMeta, exists := ms.master.ChunkMetadata[ch]
	if !exists {
		return false, nil, fmt.Errorf("chunk not found: %s", chunkHandle)
	}

	// Remove the corrupted server by address
	var newLocations []string
	for _, address := range chunkMeta.Locations {
		if address != reportingServerAddress {
			newLocations = append(newLocations, address)
		}
	}

	if len(newLocations) == 0 {
		// All replicas corrupted - delete chunk metadata

		// Log chunk deletion for recovery
		if ms.persistenceManager != nil {
			// Need to find the inode and chunk index for this chunk handle
			var inodeID, chunkIndex uint64
			ms.master.Metadata.Ascend(func(item btree.Item) bool {
				btreeItem := item.(*types.BTreeItem)
				if btreeItem.Inode != nil {
					for index, handle := range btreeItem.Inode.Chunks {
						if handle == ch {
							inodeID = btreeItem.Inode.Ino
							chunkIndex = index
							return false // Found it, stop searching
						}
					}
				}
				return true
			})

			if inodeID != 0 {
				err := ms.persistenceManager.LogDeleteChunk(chunkHandle, inodeID, chunkIndex)
				if err != nil {
					log.Printf("Failed to log chunk deletion for %s: %v", chunkHandle, err)
				}
			}
		}

		delete(ms.master.ChunkMetadata, ch)
		return true, nil, nil
	}

	// Update locations
	chunkMeta.Locations = newLocations
	return false, newLocations, nil
}

// ChunkLocationInfo represents chunk location information
type ChunkLocationInfo struct {
	ChunkHandle string
	Locations   []*common.ChunkLocation
	Version     uint32
	Size        uint64
}

// GetChunkLocations retrieves the locations of chunks (simplified)
func (ms *MasterService) GetChunkLocations(chunkHandles []string) ([]*ChunkLocationInfo, error) {
	ms.metadataLock.RLock()
	defer ms.metadataLock.RUnlock()

	chunkInfos := make([]*ChunkLocationInfo, 0, len(chunkHandles))

	for _, chunkHandle := range chunkHandles {
		ch, err := commonTypes.ParseChunkHandle(chunkHandle)
		if err != nil {
			// Return empty info for invalid handles
			chunkInfos = append(chunkInfos, &ChunkLocationInfo{
				ChunkHandle: chunkHandle,
				Locations:   []*common.ChunkLocation{},
				Version:     0,
				Size:        0,
			})
			continue
		}

		chunkMeta, exists := ms.master.ChunkMetadata[ch]
		if !exists {
			// Return empty info for non-existent chunks
			chunkInfos = append(chunkInfos, &ChunkLocationInfo{
				ChunkHandle: chunkHandle,
				Locations:   []*common.ChunkLocation{},
				Version:     0,
				Size:        0,
			})
			continue
		}

		// Build location info quickly
		var locations []*common.ChunkLocation
		for i, address := range chunkMeta.Locations {
			location := &common.ChunkLocation{
				ServerId:    ms.getServerIdByAddress(address), // Get server ID for compatibility
				Address:     address,
				Version:     uint32(chunkMeta.Version),
				IsPrimary:   i == 0, // First is primary
				LeaseExpiry: 0,      // Don't calculate lease info here for speed
			}
			locations = append(locations, location)
		}

		chunkInfo := &ChunkLocationInfo{
			ChunkHandle: chunkHandle,
			Locations:   locations,
			Version:     uint32(chunkMeta.Version),
			Size:        uint64(chunkMeta.Size),
		}
		chunkInfos = append(chunkInfos, chunkInfo)
	}

	return chunkInfos, nil
}

// removeChunkMetadataAndScheduleDeletion removes chunk metadata from master and schedules deletion on chunk servers
func (ms *MasterService) removeChunkMetadataAndScheduleDeletion(chunks map[uint64]commonTypes.ChunkHandle) {
	if len(chunks) == 0 {
		return
	}

	log.Printf("Removing metadata and scheduling deletion for %d chunks", len(chunks))

	for chunkIndex, chunkHandle := range chunks {
		// Get chunk metadata before removing it
		if chunkMeta, exists := ms.master.ChunkMetadata[chunkHandle]; exists {
			// Schedule deletion on all chunk servers that have this chunk
			if ms.replicationManager != nil && ms.replicationManager.IsRunning() {
				for _, serverAddr := range chunkMeta.Locations {
					log.Printf("Scheduling deletion of chunk %v from server %s (inode deletion)",
						chunkHandle, serverAddr)
					ms.replicationManager.QueueChunkDeletion(chunkHandle, serverAddr, "inode deletion")
				}
			}

			// Remove chunk metadata from master
			delete(ms.master.ChunkMetadata, chunkHandle)
			log.Printf("Removed chunk metadata for chunk %v (index: %d)", chunkHandle, chunkIndex)
		} else {
			log.Printf("Chunk metadata not found for chunk %v (index: %d) - may already be deleted",
				chunkHandle, chunkIndex)
		}
	}
}
