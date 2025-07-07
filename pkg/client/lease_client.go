package client

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/uttam-li/dfs/api/generated/master"
	"github.com/uttam-li/dfs/pkg/common"
)

// LeaseClient manages chunk leases from the client side
type LeaseClient struct {
	mu           sync.RWMutex
	masterClient master.MasterServiceClient
	activeLeases map[common.ChunkHandle]*ClientLeaseInfo

	// Configuration
	renewalThreshold time.Duration // How early to renew before expiry

	// Service lifecycle
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// Background renewal
	renewalTicker *time.Ticker
}

// ClientLeaseInfo tracks lease information on the client side
type ClientLeaseInfo struct {
	ChunkHandle    common.ChunkHandle
	LeaseID        string
	Primary        string
	Replicas       []string
	Version        uint64
	ExpireTime     time.Time
	RenewableAfter time.Time
	RenewalCount   int
	AutoRenew      bool      // Whether to automatically renew this lease
	LastUsed       time.Time // Last time this lease was used
}

// NewLeaseClient creates a new client-side lease manager
func NewLeaseClient(masterClient master.MasterServiceClient) *LeaseClient {
	return &LeaseClient{
		masterClient:     masterClient,
		activeLeases:     make(map[common.ChunkHandle]*ClientLeaseInfo),
		renewalThreshold: 30 * time.Second, // Renew 30 seconds before expiry
	}
}

// Start starts the lease client background renewal process
func (lc *LeaseClient) Start(ctx context.Context) error {
	lc.mu.Lock()
	defer lc.mu.Unlock()

	lc.ctx, lc.cancel = context.WithCancel(ctx)

	// Start renewal ticker
	lc.renewalTicker = time.NewTicker(10 * time.Second) // Check every 10 seconds
	lc.wg.Add(1)
	go lc.renewalLoop()

	log.Printf("Lease client started")
	return nil
}

// Stop stops the lease client
func (lc *LeaseClient) Stop() error {
	lc.mu.Lock()
	defer lc.mu.Unlock()

	if lc.cancel != nil {
		lc.cancel()
	}

	if lc.renewalTicker != nil {
		lc.renewalTicker.Stop()
	}

	lc.wg.Wait()
	log.Printf("Lease client stopped")
	return nil
}

// RequestLease requests a new lease for a chunk
func (lc *LeaseClient) RequestLease(chunkHandle common.ChunkHandle, requestingServer string, replicas []string, version uint64) (*ClientLeaseInfo, error) {
	req := &master.GrantLeaseRequest{
		ChunkHandle:      chunkHandle.String(),
		RequestingServer: requestingServer,
		Replicas:         replicas,
		Version:          version,
	}

	ctx, cancel := context.WithTimeout(lc.ctx, 10*time.Second)
	defer cancel()

	resp, err := lc.masterClient.GrantLease(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("failed to request lease: %w", err)
	}

	leaseInfo := &ClientLeaseInfo{
		ChunkHandle:    chunkHandle,
		LeaseID:        resp.LeaseId,
		Primary:        resp.Primary,
		Replicas:       replicas,
		Version:        version,
		ExpireTime:     time.Unix(resp.ExpireTime, 0),
		RenewableAfter: time.Unix(resp.RenewableAfter, 0),
		RenewalCount:   0,
		AutoRenew:      true,
		LastUsed:       time.Now(),
	}

	lc.mu.Lock()
	lc.activeLeases[chunkHandle] = leaseInfo
	lc.mu.Unlock()

	log.Printf("Obtained lease %s for chunk %s (expires: %v)",
		resp.LeaseId, chunkHandle.String(), leaseInfo.ExpireTime)

	return leaseInfo, nil
}

// GetLease retrieves lease information for a chunk
func (lc *LeaseClient) GetLease(chunkHandle common.ChunkHandle) *ClientLeaseInfo {
	lc.mu.RLock()
	defer lc.mu.RUnlock()

	lease, exists := lc.activeLeases[chunkHandle]
	if !exists {
		return nil
	}

	// Check if lease is still valid
	if time.Now().Before(lease.ExpireTime) {
		lease.LastUsed = time.Now()
		return lease
	}

	// Lease has expired
	return nil
}

// RenewLease manually renews a lease
func (lc *LeaseClient) RenewLease(chunkHandle common.ChunkHandle) error {
	lc.mu.RLock()
	lease, exists := lc.activeLeases[chunkHandle]
	if !exists {
		lc.mu.RUnlock()
		return fmt.Errorf("no lease found for chunk %s", chunkHandle.String())
	}

	// Copy lease info to avoid holding lock during RPC
	leaseID := lease.LeaseID
	primary := lease.Primary
	version := lease.Version
	lc.mu.RUnlock()

	req := &master.RenewLeaseRequest{
		ChunkHandle: chunkHandle.String(),
		LeaseId:     leaseID,
		Primary:     primary,
		Version:     version,
	}

	ctx, cancel := context.WithTimeout(lc.ctx, 5*time.Second)
	defer cancel()

	resp, err := lc.masterClient.RenewLease(ctx, req)
	if err != nil {
		return fmt.Errorf("failed to renew lease: %w", err)
	}

	// Update lease information
	lc.mu.Lock()
	if currentLease, exists := lc.activeLeases[chunkHandle]; exists && currentLease.LeaseID == leaseID {
		currentLease.ExpireTime = time.Unix(resp.ExpireTime, 0)
		currentLease.RenewableAfter = time.Unix(resp.RenewableAfter, 0)
		currentLease.RenewalCount = int(resp.RenewalCount)
		currentLease.LastUsed = time.Now()
	}
	lc.mu.Unlock()

	log.Printf("Renewed lease %s for chunk %s (expires: %v, renewal: #%d)",
		leaseID, chunkHandle.String(), time.Unix(resp.ExpireTime, 0), resp.RenewalCount)

	return nil
}

// RevokeLease revokes a lease
func (lc *LeaseClient) RevokeLease(chunkHandle common.ChunkHandle, reason string) error {
	req := &master.RevokeLeaseRequest{
		ChunkHandle: chunkHandle.String(),
		Reason:      reason,
	}

	ctx, cancel := context.WithTimeout(lc.ctx, 5*time.Second)
	defer cancel()

	resp, err := lc.masterClient.RevokeLease(ctx, req)
	if err != nil {
		return fmt.Errorf("failed to revoke lease: %w", err)
	}

	if !resp.Success {
		return fmt.Errorf("lease revocation failed: %s", resp.Message)
	}

	// Remove from active leases
	lc.mu.Lock()
	delete(lc.activeLeases, chunkHandle)
	lc.mu.Unlock()

	log.Printf("Revoked lease for chunk %s (reason: %s)", chunkHandle.String(), reason)
	return nil
}

// SetAutoRenew enables or disables auto-renewal for a lease
func (lc *LeaseClient) SetAutoRenew(chunkHandle common.ChunkHandle, autoRenew bool) {
	lc.mu.Lock()
	defer lc.mu.Unlock()

	if lease, exists := lc.activeLeases[chunkHandle]; exists {
		lease.AutoRenew = autoRenew
	}
}

// renewalLoop runs the background lease renewal process
func (lc *LeaseClient) renewalLoop() {
	defer lc.wg.Done()

	log.Printf("Started lease renewal background process")

	for {
		select {
		case <-lc.ctx.Done():
			log.Printf("Lease renewal loop stopped")
			return
		case <-lc.renewalTicker.C:
			lc.processLeaseRenewals()
		}
	}
}

// processLeaseRenewals checks and renews leases that need renewal
func (lc *LeaseClient) processLeaseRenewals() {
	lc.mu.RLock()
	leasesToRenew := make([]common.ChunkHandle, 0)
	leasesToCleanup := make([]common.ChunkHandle, 0)

	now := time.Now()
	for handle, lease := range lc.activeLeases {
		if lease.AutoRenew {
			// Check if lease needs renewal
			timeUntilExpiry := lease.ExpireTime.Sub(now)
			if timeUntilExpiry <= lc.renewalThreshold && now.After(lease.RenewableAfter) {
				leasesToRenew = append(leasesToRenew, handle)
			}
		}

		// Check if lease has expired and should be cleaned up
		if now.After(lease.ExpireTime) {
			leasesToCleanup = append(leasesToCleanup, handle)
		}
	}
	lc.mu.RUnlock()

	// Renew leases that need renewal
	for _, handle := range leasesToRenew {
		if err := lc.RenewLease(handle); err != nil {
			log.Printf("Failed to auto-renew lease for chunk %s: %v", handle.String(), err)
		}
	}

	// Clean up expired leases
	if len(leasesToCleanup) > 0 {
		lc.mu.Lock()
		for _, handle := range leasesToCleanup {
			delete(lc.activeLeases, handle)
			log.Printf("Cleaned up expired lease for chunk %s", handle.String())
		}
		lc.mu.Unlock()
	}
}

// GetActiveLeases returns a copy of all active leases
func (lc *LeaseClient) GetActiveLeases() map[common.ChunkHandle]*ClientLeaseInfo {
	lc.mu.RLock()
	defer lc.mu.RUnlock()

	leases := make(map[common.ChunkHandle]*ClientLeaseInfo, len(lc.activeLeases))
	for handle, lease := range lc.activeLeases {
		// Create a copy to avoid race conditions
		leases[handle] = &ClientLeaseInfo{
			ChunkHandle:    lease.ChunkHandle,
			LeaseID:        lease.LeaseID,
			Primary:        lease.Primary,
			Replicas:       append([]string{}, lease.Replicas...), // Copy slice
			Version:        lease.Version,
			ExpireTime:     lease.ExpireTime,
			RenewableAfter: lease.RenewableAfter,
			RenewalCount:   lease.RenewalCount,
			AutoRenew:      lease.AutoRenew,
			LastUsed:       lease.LastUsed,
		}
	}

	return leases
}

// GetLeaseStats returns statistics about client-side lease management
func (lc *LeaseClient) GetLeaseStats() *ClientLeaseStats {
	lc.mu.RLock()
	defer lc.mu.RUnlock()

	stats := &ClientLeaseStats{
		ActiveLeases: len(lc.activeLeases),
	}

	now := time.Now()
	for _, lease := range lc.activeLeases {
		if lease.AutoRenew {
			stats.AutoRenewLeases++
		}

		if now.After(lease.RenewableAfter) {
			stats.RenewableLeases++
		}

		stats.TotalRenewals += lease.RenewalCount
	}

	return stats
}

// ClientLeaseStats contains statistics about client-side lease management
type ClientLeaseStats struct {
	ActiveLeases    int
	AutoRenewLeases int
	RenewableLeases int
	TotalRenewals   int
}
