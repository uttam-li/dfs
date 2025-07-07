package service

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/uttam-li/dfs/pkg/common"
)

const (
	// Lease states
	LeaseStateActive   LeaseState = "active"
	LeaseStateExpiring LeaseState = "expiring"
	LeaseStateExpired  LeaseState = "expired"
	LeaseStateRevoked  LeaseState = "revoked"

	// Default lease renewal window (lease will be renewable in the last 1/3 of its lifetime)
	DefaultLeaseRenewalRatio = 0.33

	// Default grace period for lease handoff
 	DefaultLeaseHandoffGracePeriod = 2 * time.Second
)

type LeaseState string

// LeaseManager manages leases for chunks with robust state management
type LeaseManager struct {
	mu           sync.RWMutex
	leases       map[common.ChunkHandle]*LeaseInfo
	leaseTimeout time.Duration

	// Lease renewal and handoff configuration
	renewalWindow      time.Duration
	handoffGracePeriod time.Duration

	// Tracking and metrics
	leaseStats       *LeaseStats
	expiryCallbacks  map[common.ChunkHandle][]LeaseExpiryCallback
	handoffCallbacks map[common.ChunkHandle][]LeaseHandoffCallback

	// Service lifecycle
	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	state   common.ServiceState
	stateMu sync.RWMutex

	// Background monitoring
	monitorTicker *time.Ticker
}

// LeaseInfo represents comprehensive lease information for a chunk
type LeaseInfo struct {
	ChunkHandle    common.ChunkHandle
	LeaseID        string                 // Unique lease identifier
	Primary        string                 // Primary chunkserver address
	Replicas       []string               // Replica chunkserver addresses
	Version        uint64                 // Chunk version
	GrantedAt      time.Time              // When the lease was granted
	ExpireTime     time.Time              // When the lease expires
	RenewableAfter time.Time              // When the lease becomes renewable
	State          LeaseState             // Current lease state
	RenewalCount   int                    // Number of times lease has been renewed
	LastActivity   time.Time              // Last activity on this lease
	Metadata       map[string]interface{} // Additional metadata
}

// LeaseStats tracks lease management statistics
type LeaseStats struct {
	mu                   sync.RWMutex
	TotalLeases          int64
	ActiveLeases         int64
	ExpiredLeases        int64
	RevokedLeases        int64
	RenewalRequests      int64
	SuccessfulRenewals   int64
	FailedRenewals       int64
	HandoffRequests      int64
	SuccessfulHandoffs   int64
	FailedHandoffs       int64
	AverageLeaseLifetime time.Duration
}

// LeaseRequest represents a request for a new lease
type LeaseRequest struct {
	ChunkHandle      common.ChunkHandle
	RequestingServer string
	Replicas         []string
	Version          uint64
	Metadata         map[string]interface{}
}

// LeaseRenewalRequest represents a lease renewal request
type LeaseRenewalRequest struct {
	ChunkHandle common.ChunkHandle
	LeaseID     string
	Primary     string
	Version     uint64
}

// LeaseHandoffRequest represents a lease handoff request
type LeaseHandoffRequest struct {
	ChunkHandle    common.ChunkHandle
	LeaseID        string
	CurrentPrimary string
	NewPrimary     string
	Version        uint64
	Reason         string
}

// Callback types
type LeaseExpiryCallback func(lease *LeaseInfo)
type LeaseHandoffCallback func(oldLease, newLease *LeaseInfo)

// NewLeaseManager creates a new robust lease manager
func NewLeaseManager(timeout time.Duration) *LeaseManager {
	renewalWindow := time.Duration(float64(timeout) * DefaultLeaseRenewalRatio)

	return &LeaseManager{
		leases:             make(map[common.ChunkHandle]*LeaseInfo),
		leaseTimeout:       timeout,
		renewalWindow:      renewalWindow,
		handoffGracePeriod: DefaultLeaseHandoffGracePeriod,
		leaseStats:         &LeaseStats{},
		expiryCallbacks:    make(map[common.ChunkHandle][]LeaseExpiryCallback),
		handoffCallbacks:   make(map[common.ChunkHandle][]LeaseHandoffCallback),
		state:              common.ServiceStateUnknown,
	}
}

// Start starts the lease manager with enhanced monitoring
func (lm *LeaseManager) Start(ctx context.Context) error {
	lm.stateMu.Lock()
	defer lm.stateMu.Unlock()

	if lm.state == common.ServiceStateRunning {
		return fmt.Errorf("lease manager is already running")
	}

	if lm.state == common.ServiceStateStarting {
		return fmt.Errorf("lease manager is already starting")
	}

	lm.state = common.ServiceStateStarting
	lm.ctx, lm.cancel = context.WithCancel(ctx)

	// Start background monitoring with shorter intervals for better precision
	monitorInterval := lm.leaseTimeout / 10
	if monitorInterval > 30*time.Second {
		monitorInterval = 30 * time.Second
	} else if monitorInterval < 1*time.Second {
		monitorInterval = 1 * time.Second
	}

	lm.monitorTicker = time.NewTicker(monitorInterval)
	lm.wg.Add(1)
	go lm.monitorLeases()

	lm.state = common.ServiceStateRunning
	log.Printf("Lease manager started with timeout: %v, monitor interval: %v", lm.leaseTimeout, monitorInterval)
	return nil
}

// Stop stops the lease manager gracefully
func (lm *LeaseManager) Stop() error {
	lm.stateMu.Lock()
	defer lm.stateMu.Unlock()

	if lm.state == common.ServiceStateStopped || lm.state == common.ServiceStateStopping {
		return nil
	}

	lm.state = common.ServiceStateStopping
	log.Printf("Stopping lease manager...")

	if lm.cancel != nil {
		lm.cancel()
	}

	if lm.monitorTicker != nil {
		lm.monitorTicker.Stop()
	}

	// Wait for background goroutines to finish with timeout
	done := make(chan struct{})
	go func() {
		defer close(done)
		lm.wg.Wait()
	}()

	select {
	case <-done:
		log.Printf("Lease manager stopped successfully")
	case <-time.After(10 * time.Second):
		log.Printf("Lease manager stop timeout exceeded")
	}

	lm.state = common.ServiceStateStopped
	return nil
}

// GetState returns the current state of the lease manager
func (lm *LeaseManager) GetState() common.ServiceState {
	lm.stateMu.RLock()
	defer lm.stateMu.RUnlock()
	return lm.state
}

// GrantLease grants a new lease with comprehensive validation
func (lm *LeaseManager) GrantLease(req LeaseRequest) (*LeaseInfo, error) {
	if lm.GetState() != common.ServiceStateRunning {
		return nil, fmt.Errorf("lease manager is not running")
	}

	// Validate request
	if req.RequestingServer == "" {
		return nil, fmt.Errorf("requesting server cannot be empty")
	}

	lm.mu.Lock()
	defer lm.mu.Unlock()

	// Check if lease already exists
	if existingLease, exists := lm.leases[req.ChunkHandle]; exists {
		if existingLease.State == LeaseStateActive && time.Now().Before(existingLease.ExpireTime) {
			return nil, fmt.Errorf("active lease already exists for chunk %s (primary: %s, expires: %v)",
				req.ChunkHandle.String(), existingLease.Primary, existingLease.ExpireTime)
		}
		// Clean up expired/revoked lease
		delete(lm.leases, req.ChunkHandle)
	}

	now := time.Now()
	leaseID := uuid.New().String()

	lease := &LeaseInfo{
		ChunkHandle:    req.ChunkHandle,
		LeaseID:        leaseID,
		Primary:        req.RequestingServer,
		Replicas:       make([]string, len(req.Replicas)),
		Version:        req.Version,
		GrantedAt:      now,
		ExpireTime:     now.Add(lm.leaseTimeout),
		RenewableAfter: now.Add(lm.leaseTimeout - lm.renewalWindow),
		State:          LeaseStateActive,
		RenewalCount:   0,
		LastActivity:   now,
		Metadata:       make(map[string]any),
	}

	copy(lease.Replicas, req.Replicas)
	if req.Metadata != nil {
		for k, v := range req.Metadata {
			lease.Metadata[k] = v
		}
	}

	lm.leases[req.ChunkHandle] = lease

	// Update statistics
	lm.leaseStats.mu.Lock()
	lm.leaseStats.TotalLeases++
	lm.leaseStats.ActiveLeases++
	lm.leaseStats.mu.Unlock()

	log.Printf("Granted lease %s for chunk %s to primary %s (expires: %v)",
		leaseID, req.ChunkHandle.String(), req.RequestingServer, lease.ExpireTime)

	return lease, nil
}

// RenewLease renews an existing lease
func (lm *LeaseManager) RenewLease(req LeaseRenewalRequest) (*LeaseInfo, error) {
	if lm.GetState() != common.ServiceStateRunning {
		return nil, fmt.Errorf("lease manager is not running")
	}

	lm.mu.Lock()
	defer lm.mu.Unlock()

	lm.leaseStats.mu.Lock()
	lm.leaseStats.RenewalRequests++
	lm.leaseStats.mu.Unlock()

	lease, exists := lm.leases[req.ChunkHandle]
	if !exists {
		lm.leaseStats.mu.Lock()
		lm.leaseStats.FailedRenewals++
		lm.leaseStats.mu.Unlock()
		return nil, fmt.Errorf("lease not found for chunk %s", req.ChunkHandle.String())
	}

	// Validate lease ownership
	if lease.LeaseID != req.LeaseID {
		lm.leaseStats.mu.Lock()
		lm.leaseStats.FailedRenewals++
		lm.leaseStats.mu.Unlock()
		return nil, fmt.Errorf("lease ID mismatch for chunk %s", req.ChunkHandle.String())
	}

	if lease.Primary != req.Primary {
		lm.leaseStats.mu.Lock()
		lm.leaseStats.FailedRenewals++
		lm.leaseStats.mu.Unlock()
		return nil, fmt.Errorf("primary mismatch for chunk %s lease %s", req.ChunkHandle.String(), req.LeaseID)
	}

	now := time.Now()

	// Check if lease is renewable
	if now.Before(lease.RenewableAfter) {
		lm.leaseStats.mu.Lock()
		lm.leaseStats.FailedRenewals++
		lm.leaseStats.mu.Unlock()
		return nil, fmt.Errorf("lease %s for chunk %s is not yet renewable (renewable after: %v)",
			req.LeaseID, req.ChunkHandle.String(), lease.RenewableAfter)
	}

	// Check if lease is expired
	if lease.State == LeaseStateExpired || now.After(lease.ExpireTime) {
		lm.leaseStats.mu.Lock()
		lm.leaseStats.FailedRenewals++
		lm.leaseStats.mu.Unlock()
		return nil, fmt.Errorf("lease %s for chunk %s has expired", req.LeaseID, req.ChunkHandle.String())
	}

	// Renew the lease
	lease.ExpireTime = now.Add(lm.leaseTimeout)
	lease.RenewableAfter = now.Add(lm.leaseTimeout - lm.renewalWindow)
	lease.RenewalCount++
	lease.LastActivity = now
	lease.Version = req.Version

	lm.leaseStats.mu.Lock()
	lm.leaseStats.SuccessfulRenewals++
	lm.leaseStats.mu.Unlock()

	log.Printf("Renewed lease %s for chunk %s (renewal #%d, expires: %v)",
		lease.LeaseID, req.ChunkHandle.String(), lease.RenewalCount, lease.ExpireTime)

	return lease, nil
}

// RequestLeaseHandoff handles lease handoff between chunkservers
func (lm *LeaseManager) RequestLeaseHandoff(req LeaseHandoffRequest) (*LeaseInfo, error) {
	if lm.GetState() != common.ServiceStateRunning {
		return nil, fmt.Errorf("lease manager is not running")
	}

	if req.CurrentPrimary == req.NewPrimary {
		return nil, fmt.Errorf("current and new primary cannot be the same")
	}

	lm.mu.Lock()
	defer lm.mu.Unlock()

	lm.leaseStats.mu.Lock()
	lm.leaseStats.HandoffRequests++
	lm.leaseStats.mu.Unlock()

	oldLease, exists := lm.leases[req.ChunkHandle]
	if !exists {
		lm.leaseStats.mu.Lock()
		lm.leaseStats.FailedHandoffs++
		lm.leaseStats.mu.Unlock()
		return nil, fmt.Errorf("lease not found for chunk %s", req.ChunkHandle.String())
	}

	// Validate current lease
	if oldLease.LeaseID != req.LeaseID || oldLease.Primary != req.CurrentPrimary {
		lm.leaseStats.mu.Lock()
		lm.leaseStats.FailedHandoffs++
		lm.leaseStats.mu.Unlock()
		return nil, fmt.Errorf("lease validation failed for handoff request")
	}

	if oldLease.State != LeaseStateActive {
		lm.leaseStats.mu.Lock()
		lm.leaseStats.FailedHandoffs++
		lm.leaseStats.mu.Unlock()
		return nil, fmt.Errorf("cannot handoff non-active lease")
	}

	now := time.Now()

	// Create new lease for handoff
	newLeaseID := uuid.New().String()
	newLease := &LeaseInfo{
		ChunkHandle:    req.ChunkHandle,
		LeaseID:        newLeaseID,
		Primary:        req.NewPrimary,
		Replicas:       make([]string, len(oldLease.Replicas)),
		Version:        req.Version,
		GrantedAt:      now,
		ExpireTime:     now.Add(lm.leaseTimeout),
		RenewableAfter: now.Add(lm.leaseTimeout - lm.renewalWindow),
		State:          LeaseStateActive,
		RenewalCount:   0,
		LastActivity:   now,
		Metadata:       make(map[string]interface{}),
	}

	copy(newLease.Replicas, oldLease.Replicas)
	for k, v := range oldLease.Metadata {
		newLease.Metadata[k] = v
	}
	newLease.Metadata["handoff_reason"] = req.Reason
	newLease.Metadata["previous_primary"] = req.CurrentPrimary
	newLease.Metadata["previous_lease_id"] = req.LeaseID

	// Mark old lease as revoked
	oldLease.State = LeaseStateRevoked

	// Replace with new lease
	lm.leases[req.ChunkHandle] = newLease

	// Execute handoff callbacks
	if callbacks, exists := lm.handoffCallbacks[req.ChunkHandle]; exists {
		for _, callback := range callbacks {
			go callback(oldLease, newLease)
		}
	}

	lm.leaseStats.mu.Lock()
	lm.leaseStats.SuccessfulHandoffs++
	lm.leaseStats.mu.Unlock()

	log.Printf("Completed lease handoff for chunk %s: %s->%s (old lease: %s, new lease: %s, reason: %s)",
		req.ChunkHandle.String(), req.CurrentPrimary, req.NewPrimary, req.LeaseID, newLeaseID, req.Reason)

	return newLease, nil
}

// GetLease retrieves the current lease for a chunk
func (lm *LeaseManager) GetLease(handle common.ChunkHandle) *LeaseInfo {
	lm.mu.RLock()
	defer lm.mu.RUnlock()

	lease, exists := lm.leases[handle]
	if !exists {
		return nil
	}

	// Check if lease is still valid
	now := time.Now()
	if lease.State == LeaseStateActive && now.Before(lease.ExpireTime) {
		// Update last activity
		lease.LastActivity = now
		return lease
	}

	// Lease is expired or invalid
	return nil
}

// RevokeLease revokes a lease immediately
func (lm *LeaseManager) RevokeLease(handle common.ChunkHandle, reason string) error {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	lease, exists := lm.leases[handle]
	if !exists {
		return fmt.Errorf("lease not found for chunk %s", handle.String())
	}

	if lease.State != LeaseStateActive {
		return fmt.Errorf("cannot revoke non-active lease for chunk %s", handle.String())
	}

	lease.State = LeaseStateRevoked
	lease.Metadata["revocation_reason"] = reason
	lease.Metadata["revoked_at"] = time.Now()

	lm.leaseStats.mu.Lock()
	lm.leaseStats.RevokedLeases++
	lm.leaseStats.ActiveLeases--
	lm.leaseStats.mu.Unlock()

	log.Printf("Revoked lease %s for chunk %s (reason: %s)", lease.LeaseID, handle.String(), reason)

	// Execute expiry callbacks for revoked leases
	if callbacks, exists := lm.expiryCallbacks[handle]; exists {
		for _, callback := range callbacks {
			go callback(lease)
		}
	}

	return nil
}

// RevokeAllLeasesForServer revokes all leases held by a specific server
func (lm *LeaseManager) RevokeAllLeasesForServer(serverAddr string, reason string) int {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	revokedCount := 0
	for handle, lease := range lm.leases {
		if lease.Primary == serverAddr && lease.State == LeaseStateActive {
			lease.State = LeaseStateRevoked
			lease.Metadata["revocation_reason"] = reason
			lease.Metadata["revoked_at"] = time.Now()
			revokedCount++

			// Execute expiry callbacks
			if callbacks, exists := lm.expiryCallbacks[handle]; exists {
				for _, callback := range callbacks {
					go callback(lease)
				}
			}

			log.Printf("Revoked lease %s for chunk %s from server %s (reason: %s)",
				lease.LeaseID, handle.String(), serverAddr, reason)
		}
	}

	if revokedCount > 0 {
		lm.leaseStats.mu.Lock()
		lm.leaseStats.RevokedLeases += int64(revokedCount)
		lm.leaseStats.ActiveLeases -= int64(revokedCount)
		lm.leaseStats.mu.Unlock()

		log.Printf("Revoked %d leases for server %s", revokedCount, serverAddr)
	}

	return revokedCount
}

// IsLeaseRenewable checks if a lease can be renewed
func (lm *LeaseManager) IsLeaseRenewable(handle common.ChunkHandle, leaseID string) bool {
	lm.mu.RLock()
	defer lm.mu.RUnlock()

	lease, exists := lm.leases[handle]
	if !exists || lease.LeaseID != leaseID {
		return false
	}

	now := time.Now()
	return lease.State == LeaseStateActive &&
		now.After(lease.RenewableAfter) &&
		now.Before(lease.ExpireTime)
}

// GetLeaseStats returns current lease statistics
func (lm *LeaseManager) GetLeaseStats() *LeaseStats {
	lm.leaseStats.mu.RLock()
	defer lm.leaseStats.mu.RUnlock()

	// Return a copy to avoid race conditions
	return &LeaseStats{
		TotalLeases:          lm.leaseStats.TotalLeases,
		ActiveLeases:         lm.leaseStats.ActiveLeases,
		ExpiredLeases:        lm.leaseStats.ExpiredLeases,
		RevokedLeases:        lm.leaseStats.RevokedLeases,
		RenewalRequests:      lm.leaseStats.RenewalRequests,
		SuccessfulRenewals:   lm.leaseStats.SuccessfulRenewals,
		FailedRenewals:       lm.leaseStats.FailedRenewals,
		HandoffRequests:      lm.leaseStats.HandoffRequests,
		SuccessfulHandoffs:   lm.leaseStats.SuccessfulHandoffs,
		FailedHandoffs:       lm.leaseStats.FailedHandoffs,
		AverageLeaseLifetime: lm.leaseStats.AverageLeaseLifetime,
	}
}

// RegisterExpiryCallback registers a callback to be called when a lease expires
func (lm *LeaseManager) RegisterExpiryCallback(handle common.ChunkHandle, callback LeaseExpiryCallback) {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	if lm.expiryCallbacks[handle] == nil {
		lm.expiryCallbacks[handle] = make([]LeaseExpiryCallback, 0)
	}
	lm.expiryCallbacks[handle] = append(lm.expiryCallbacks[handle], callback)
}

// RegisterHandoffCallback registers a callback to be called when a lease is handed off
func (lm *LeaseManager) RegisterHandoffCallback(handle common.ChunkHandle, callback LeaseHandoffCallback) {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	if lm.handoffCallbacks[handle] == nil {
		lm.handoffCallbacks[handle] = make([]LeaseHandoffCallback, 0)
	}
	lm.handoffCallbacks[handle] = append(lm.handoffCallbacks[handle], callback)
}

// monitorLeases runs the background lease monitoring
func (lm *LeaseManager) monitorLeases() {
	defer lm.wg.Done()

	log.Printf("Started lease monitoring goroutine")

	for {
		select {
		case <-lm.ctx.Done():
			log.Printf("Lease monitoring goroutine stopped")
			return
		case <-lm.monitorTicker.C:
			lm.processLeaseStates()
		}
	}
}

// processLeaseStates processes lease state transitions
func (lm *LeaseManager) processLeaseStates() {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	now := time.Now()
	expiredLeases := make([]*LeaseInfo, 0)
	expiringLeases := make([]*LeaseInfo, 0)

	for handle, lease := range lm.leases {
		switch lease.State {
		case LeaseStateActive:
			if now.After(lease.ExpireTime) {
				// Lease has expired
				lease.State = LeaseStateExpired
				expiredLeases = append(expiredLeases, lease)
			} else if now.After(lease.RenewableAfter) && lease.State != LeaseStateExpiring {
				// Lease is entering renewal window
				lease.State = LeaseStateExpiring
				expiringLeases = append(expiringLeases, lease)
			}
		case LeaseStateExpired, LeaseStateRevoked:
			// Clean up old expired/revoked leases after grace period
			if now.After(lease.ExpireTime.Add(lm.handoffGracePeriod)) {
				delete(lm.leases, handle)
				delete(lm.expiryCallbacks, handle)
				delete(lm.handoffCallbacks, handle)
			}
		}
	}

	// Update statistics and execute callbacks outside the main lock
	if len(expiredLeases) > 0 {
		lm.leaseStats.mu.Lock()
		lm.leaseStats.ExpiredLeases += int64(len(expiredLeases))
		lm.leaseStats.ActiveLeases -= int64(len(expiredLeases))
		lm.leaseStats.mu.Unlock()

		for _, lease := range expiredLeases {
			log.Printf("Lease %s for chunk %s expired (primary: %s)",
				lease.LeaseID, lease.ChunkHandle.String(), lease.Primary)

			// Execute expiry callbacks
			if callbacks, exists := lm.expiryCallbacks[lease.ChunkHandle]; exists {
				for _, callback := range callbacks {
					go callback(lease)
				}
			}
		}
	}

	if len(expiringLeases) > 0 {
		for _, lease := range expiringLeases {
			log.Printf("Lease %s for chunk %s entering renewal window (primary: %s, expires: %v)",
				lease.LeaseID, lease.ChunkHandle.String(), lease.Primary, lease.ExpireTime)
		}
	}
}

// IsRunning returns true if the lease manager is running
func (lm *LeaseManager) IsRunning() bool {
	return lm.GetState() == common.ServiceStateRunning
}
