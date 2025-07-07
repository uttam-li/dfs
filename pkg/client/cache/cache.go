package cache

import (
	"fmt"
	"sync"
	"time"

	"github.com/uttam-li/dfs/api/generated/master"
)

type ChunkLocationCacheEntry struct {
    Handle     string   // Chunk handle (UUID)
    Locations  []string // Server addresses storing the chunk
    Version    uint32   // Version number
    Expiration time.Time
}

// MetadataEntry represents cached metadata for an inode
type MetadataEntry struct {
    Inode      *master.Inode
    Expiration time.Time
}

// Cache represents a client-side cache
type Cache struct {
    chunkLocationCache map[string]ChunkLocationCacheEntry // Maps "ino:chunkIndex" -> locations
    metadataCache      map[uint64]*MetadataEntry          // Limited inode metadata cache
    mutex              sync.RWMutex

    // Configuration
    metadataTTL time.Duration
    chunkLocTTL time.Duration
    maxEntries  int

    // Statistics for monitoring
    stats CacheStats
}

type CacheStats struct {
    MetadataHits   uint64
    MetadataMisses uint64
    ChunkHits      uint64
    ChunkMisses    uint64
    Evictions      uint64
}

// NewCache creates a new client-side cache
func NewCache(metadataTTL, chunkLocTTL time.Duration, maxEntries int) *Cache {
    return &Cache{
        chunkLocationCache: make(map[string]ChunkLocationCacheEntry),
        metadataCache:      make(map[uint64]*MetadataEntry),
        metadataTTL:        metadataTTL,
        chunkLocTTL:        chunkLocTTL,
        maxEntries:         maxEntries,
    }
}

// GetInode retrieves an inode from the metadata cache
func (c *Cache) GetInode(ino uint64) (*master.Inode, bool) {
    c.mutex.RLock()
    defer c.mutex.RUnlock()

    entry, exists := c.metadataCache[ino]
    if !exists {
        c.stats.MetadataMisses++
        return nil, false
    }

    // Check if entry has expired
    if time.Now().After(entry.Expiration) {
        c.stats.MetadataMisses++
        return nil, false
    }

    c.stats.MetadataHits++
    
    return entry.Inode, true
}

// StoreInode stores an inode in the metadata cache
func (c *Cache) StoreInode(inode *master.Inode) {
    if inode == nil {
        return
    }

    c.mutex.Lock()
    defer c.mutex.Unlock()

    // Check if we need to evict entries due to size limit
    if len(c.metadataCache) >= c.maxEntries {
        c.evictOldestMetadata()
    }

    c.metadataCache[inode.Ino] = &MetadataEntry{
        Inode:      inode,
        Expiration: time.Now().Add(c.metadataTTL),
    }
}

// GetChunkLocation retrieves chunk locations from cache
func (c *Cache) GetChunkLocation(ino, chunkIndex uint64) ([]string, bool) {
    key := fmt.Sprintf("%d:%d", ino, chunkIndex)
    
    c.mutex.RLock()
    defer c.mutex.RUnlock()

    entry, exists := c.chunkLocationCache[key]
    if !exists {
        c.stats.ChunkMisses++
        return nil, false
    }

    // Check if entry has expired
    if time.Now().After(entry.Expiration) {
        c.stats.ChunkMisses++
        return nil, false
    }

    c.stats.ChunkHits++
    
    // Return a copy of the slice to prevent external modifications
    locations := make([]string, len(entry.Locations))
    copy(locations, entry.Locations)
    return locations, true
}

// StoreChunkLocation stores chunk location information in cache
func (c *Cache) StoreChunkLocation(ino, chunkIndex uint64, handle string, locations []string, version uint32) {
    if handle == "" || len(locations) == 0 {
        return
    }

    key := fmt.Sprintf("%d:%d", ino, chunkIndex)
    
    c.mutex.Lock()
    defer c.mutex.Unlock()

    // Check if we need to evict entries due to size limit
    totalEntries := len(c.metadataCache) + len(c.chunkLocationCache)
    if totalEntries >= c.maxEntries {
        c.evictOldestChunkLocation()
    }

    // Store a copy of the locations slice
    locationsCopy := make([]string, len(locations))
    copy(locationsCopy, locations)

    c.chunkLocationCache[key] = ChunkLocationCacheEntry{
        Handle:     handle,
        Locations:  locationsCopy,
        Version:    version,
        Expiration: time.Now().Add(c.chunkLocTTL),
    }
}

// GetChunkHandle retrieves chunk handle and version from cache
func (c *Cache) GetChunkHandle(ino uint64, chunkIndex uint64) (string, uint32, bool) {
    key := fmt.Sprintf("%d:%d", ino, chunkIndex)
    
    c.mutex.RLock()
    defer c.mutex.RUnlock()

    entry, exists := c.chunkLocationCache[key]
    if !exists {
        return "", 0, false
    }

    // Check if entry has expired
    if time.Now().After(entry.Expiration) {
        return "", 0, false
    }

    return entry.Handle, entry.Version, true
}

// Invalidate removes all cached entries for a specific inode
func (c *Cache) Invalidate(ino uint64) {
    c.mutex.Lock()
    defer c.mutex.Unlock()

    // Remove metadata entry
    delete(c.metadataCache, ino)

    // Remove all chunk location entries for this inode
    prefix := fmt.Sprintf("%d:", ino)
    for key := range c.chunkLocationCache {
        if len(key) > len(prefix) && key[:len(prefix)] == prefix {
            delete(c.chunkLocationCache, key)
        }
    }
}

// GetStats returns cache statistics
func (c *Cache) GetStats() CacheStats {
    c.mutex.RLock()
    defer c.mutex.RUnlock()
    return c.stats
}

// evictOldestMetadata removes the oldest metadata entry (LRU approximation)
func (c *Cache) evictOldestMetadata() {
    var oldestIno uint64
    var oldestTime time.Time
    first := true

    for ino, entry := range c.metadataCache {
        if first || entry.Expiration.Before(oldestTime) {
            oldestIno = ino
            oldestTime = entry.Expiration
            first = false
        }
    }

    if !first {
        delete(c.metadataCache, oldestIno)
        c.stats.Evictions++
    }
}

// evictOldestChunkLocation removes the oldest chunk location entry
func (c *Cache) evictOldestChunkLocation() {
    var oldestKey string
    var oldestTime time.Time
    first := true

    for key, entry := range c.chunkLocationCache {
        if first || entry.Expiration.Before(oldestTime) {
            oldestKey = key
            oldestTime = entry.Expiration
            first = false
        }
    }

    if !first {
        delete(c.chunkLocationCache, oldestKey)
        c.stats.Evictions++
    }
}

// StartJanitor starts a background goroutine to clean expired entries
func (c *Cache) StartJanitor(interval time.Duration) {
    go func() {
        ticker := time.NewTicker(interval)
        defer ticker.Stop()

        for range ticker.C {
            c.cleanExpired()
        }
    }()
}

// cleanExpired removes all expired entries from the cache
func (c *Cache) cleanExpired() {
    now := time.Now()

    c.mutex.Lock()
    defer c.mutex.Unlock()

    // Clean expired metadata entries
    for ino, entry := range c.metadataCache {
        if now.After(entry.Expiration) {
            delete(c.metadataCache, ino)
        }
    }

    // Clean expired chunk location entries
    for key, entry := range c.chunkLocationCache {
        if now.After(entry.Expiration) {
            delete(c.chunkLocationCache, key)
        }
    }
}