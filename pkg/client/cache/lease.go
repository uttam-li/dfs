package cache

import (
	"sync"
	"time"
)

type ChunkLeaseCache struct {
	leases map[string]*CachedLease // key: chunkHandle
	mutex  sync.RWMutex
}

type CachedLease struct {
	ChunkHandle string
	Primary     string
	Replicas    []string
	Version     uint32
	ExpireTime  time.Time
}

func NewChunkLeaseCache() *ChunkLeaseCache {
	return &ChunkLeaseCache{
		leases: make(map[string]*CachedLease),
	}
}

func (clc *ChunkLeaseCache) GetLease(chunkHandle string) (*CachedLease, bool) {
	clc.mutex.RLock()
	defer clc.mutex.RUnlock()

	lease, exists := clc.leases[chunkHandle]
	if !exists {
		return nil, false
	}

	// Check if lease is still valid (with 10 second buffer)
	if time.Now().Add(10 * time.Second).After(lease.ExpireTime) {
		return nil, false
	}

	return lease, true
}

func (clc *ChunkLeaseCache) StoreLease(chunkHandle, primary string, replicas []string, version uint32, duration time.Duration) {
	clc.mutex.Lock()
	defer clc.mutex.Unlock()

	clc.leases[chunkHandle] = &CachedLease{
		ChunkHandle: chunkHandle,
		Primary:     primary,
		Replicas:    replicas,
		Version:     version,
		ExpireTime:  time.Now().Add(duration),
	}
}

func (clc *ChunkLeaseCache) InvalidateLease(chunkHandle string) {
	clc.mutex.Lock()
	defer clc.mutex.Unlock()

	delete(clc.leases, chunkHandle)
}
