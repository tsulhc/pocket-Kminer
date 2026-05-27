package miner

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/pokt-network/pocket-relay-miner/logging"
	redisutil "github.com/pokt-network/pocket-relay-miner/transport/redis"
)

// Supplier Claiming Default Timing Constants
//
// These defaults are used when no YAML config is provided. They are tuned for
// production reliability with high supplier counts (500+).
//
// Timing relationships:
//   - ClaimTTL = 90s: Time before a claim expires if not renewed
//   - RenewRate = 10s: Claims are renewed 9x before TTL expires (90s / 10s)
//   - InstanceTTL = 90s: Instance registration expires after 90s without heartbeat
//   - InstanceHeartbeatRate = 10s: Instance heartbeats every 10s
//   - RebalanceInterval = 30s: Check for rebalancing every 30s
//
// Failover timing:
//   - If a miner crashes, its claims expire after ClaimTTL (90s)
//   - Other miners detect orphaned claims in the next scan cycle (~30s)
//   - Maximum failover time: ClaimTTL + RebalanceInterval = ~120s
//
// These can be overridden via supplier_claiming config in miner YAML.
const (
	// ClaimTTL is how long a supplier claim is valid before it expires.
	// If a miner crashes, other miners can reclaim after this duration.
	// Set to 90s (up from 30s) to give the sequential renewal loop sufficient
	// headroom with high supplier counts (500+) where renewal can take 4-8s.
	ClaimTTL = 90 * time.Second

	// RenewRate is how often to renew claims.
	// Claims are renewed 9x before TTL expires (90s / 10s = 9x safety margin).
	RenewRate = 10 * time.Second

	// InstanceTTL is how long an instance registration is valid.
	// Matches ClaimTTL for consistency.
	InstanceTTL = 90 * time.Second

	// InstanceHeartbeatRate is how often to heartbeat instance registration.
	// Matches RenewRate for consistency.
	InstanceHeartbeatRate = 10 * time.Second

	// RebalanceInterval is how often to check for orphaned suppliers.
	// In single-primary mode, miners do not redistribute healthy claims; standby
	// miners only pick up claims that expire or are otherwise missing.
	RebalanceInterval = 30 * time.Second
)

// SupplierClaimerConfig contains configuration for the SupplierClaimer.
type SupplierClaimerConfig struct {
	// ClaimTTL is how long a supplier claim is valid before expiring.
	// Default: 30s
	ClaimTTL time.Duration

	// RenewRate is how often to renew claims.
	// Default: 10s (should be < ClaimTTL/2)
	RenewRate time.Duration

	// InstanceTTL is how long an instance registration is valid.
	// Default: 30s
	InstanceTTL time.Duration

	// InstanceHeartbeatRate is how often to heartbeat instance registration.
	// Default: 10s
	InstanceHeartbeatRate time.Duration

	// RebalanceInterval is how often to check for orphaned suppliers.
	// Default: 30s
	RebalanceInterval time.Duration
}

// SupplierClaimer manages distributed supplier claiming using Redis-based leases.
// It uses a single-primary model: the first healthy miner claims every supplier
// it can, and standby miners only claim suppliers whose leases expire.
//
// Key features:
// - Lease-based claiming with automatic renewal
// - Classic failover through automatic reclaim of orphaned suppliers
// - Instance registration with heartbeat
type SupplierClaimer struct {
	logger      logging.Logger
	redisClient *redisutil.Client
	instanceID  string
	config      SupplierClaimerConfig

	// Claimed suppliers (this instance owns these)
	// Maps supplier address to the time it was claimed, enabling newest-first
	// release ordering during rebalancing to minimize session handoff churn.
	claimed   map[string]time.Time
	claimedMu sync.RWMutex

	// All configured suppliers (from KeyManager)
	allSuppliers   []string
	allSuppliersMu sync.RWMutex

	// Callbacks
	onClaimFn   func(ctx context.Context, supplier string) error
	onReleaseFn func(ctx context.Context, supplier string) error

	// Lifecycle
	ctx      context.Context
	cancelFn context.CancelFunc
	wg       sync.WaitGroup
}

// NewSupplierClaimer creates a new supplier claimer.
// Uses the provided config values. Zero values fall back to the package-level
// constants (ClaimTTL=90s, RenewRate=10s, etc.).
func NewSupplierClaimer(
	logger logging.Logger,
	redisClient *redisutil.Client,
	instanceID string,
	cfg SupplierClaimerConfig,
) *SupplierClaimer {
	// Apply defaults for any zero-value fields
	if cfg.ClaimTTL <= 0 {
		cfg.ClaimTTL = ClaimTTL
	}
	if cfg.RenewRate <= 0 {
		cfg.RenewRate = RenewRate
	}
	if cfg.InstanceTTL <= 0 {
		cfg.InstanceTTL = cfg.ClaimTTL // Match ClaimTTL
	}
	if cfg.InstanceHeartbeatRate <= 0 {
		cfg.InstanceHeartbeatRate = cfg.RenewRate // Match RenewRate
	}
	if cfg.RebalanceInterval <= 0 {
		cfg.RebalanceInterval = RebalanceInterval
	}

	componentLogger := logging.ForComponent(logger, logging.ComponentSupplierClaimer)
	componentLogger.Info().
		Dur("claim_ttl", cfg.ClaimTTL).
		Dur("renew_rate", cfg.RenewRate).
		Dur("orphan_scan_interval", cfg.RebalanceInterval).
		Msg("supplier claimer timing configuration")

	return &SupplierClaimer{
		logger:      componentLogger,
		redisClient: redisClient,
		instanceID:  instanceID,
		config:      cfg,
		claimed:     make(map[string]time.Time),
	}
}

// SetCallbacks sets the callbacks for claim and release events.
// onClaimFn is called when a supplier is successfully claimed (should start lifecycle).
// onReleaseFn is called when a supplier is released (should drain and stop lifecycle).
func (c *SupplierClaimer) SetCallbacks(
	onClaimFn func(ctx context.Context, supplier string) error,
	onReleaseFn func(ctx context.Context, supplier string) error,
) {
	c.onClaimFn = onClaimFn
	c.onReleaseFn = onReleaseFn
}

// Start initializes the claimer and begins the claim, renew, and orphan scan loops.
func (c *SupplierClaimer) Start(ctx context.Context, suppliers []string) error {
	c.ctx, c.cancelFn = context.WithCancel(ctx)
	suppliers = append([]string(nil), suppliers...)
	sort.Strings(suppliers)

	c.allSuppliersMu.Lock()
	c.allSuppliers = suppliers
	c.allSuppliersMu.Unlock()

	// Log configuration
	c.logger.Info().
		Dur("claim_ttl", c.config.ClaimTTL).
		Dur("renew_rate", c.config.RenewRate).
		Dur("instance_ttl", c.config.InstanceTTL).
		Dur("orphan_scan_interval", c.config.RebalanceInterval).
		Msg("supplier claimer configuration")

	// Register this instance
	if err := c.registerInstance(ctx); err != nil {
		return fmt.Errorf("failed to register instance: %w", err)
	}

	// Initial claim of suppliers
	if err := c.initialClaim(ctx); err != nil {
		c.logger.Warn().Err(err).Msg("initial claim had errors (will retry in background)")
	}

	// Start background goroutines
	c.wg.Add(3)
	go c.instanceHeartbeatLoop()
	go c.renewLoop()
	go c.orphanScanLoop()

	c.logger.Info().
		Int("suppliers", len(suppliers)).
		Int("claimed", c.ClaimedCount()).
		Msg("supplier claimer started")

	return nil
}

// Stop gracefully shuts down the claimer and releases all claims.
func (c *SupplierClaimer) Stop(ctx context.Context) error {
	if c.cancelFn != nil {
		c.cancelFn()
	}

	// Wait for goroutines to finish
	c.wg.Wait()

	// Release all claims
	c.claimedMu.Lock()
	claimed := make([]string, 0, len(c.claimed))
	for supplier := range c.claimed {
		claimed = append(claimed, supplier)
	}
	c.claimedMu.Unlock()

	for _, supplier := range claimed {
		if err := c.Release(ctx, supplier); err != nil {
			c.logger.Warn().Err(err).Str("supplier", supplier).Msg("failed to release claim on shutdown")
		}
	}

	// Unregister this instance
	if err := c.unregisterInstance(ctx); err != nil {
		c.logger.Warn().Err(err).Msg("failed to unregister instance on shutdown")
	}

	c.logger.Info().Msg("supplier claimer stopped")

	return nil
}

// TryClaim attempts to claim a supplier using Redis SET NX with TTL.
// Returns true if the claim was successful, false if already claimed by another instance.
func (c *SupplierClaimer) TryClaim(ctx context.Context, supplier string) bool {
	claimKey := c.redisClient.KB().MinerClaimKey(supplier)

	// Use SET NX (only set if not exists) with TTL
	success, err := c.redisClient.SetNX(ctx, claimKey, c.instanceID, c.config.ClaimTTL).Result()
	if err != nil {
		c.logger.Error().
			Err(err).
			Str("supplier", supplier).
			Str("claim_key", claimKey).
			Msg("failed to claim supplier")
		return false
	}

	if !success {
		// Already claimed - check if it's by us (renewal case) or another instance
		owner, err := c.redisClient.Get(ctx, claimKey).Result()
		if err == nil && owner == c.instanceID {
			// We already own it, just renew - check result to ensure it worked
			renewed, expireErr := c.redisClient.Expire(ctx, claimKey, c.config.ClaimTTL).Result()
			if expireErr != nil {
				c.logger.Warn().
					Err(expireErr).
					Str("supplier", supplier).
					Msg("failed to renew existing claim")
				return false
			}
			if !renewed {
				c.logger.Warn().
					Str("supplier", supplier).
					Msg("claim key disappeared during renewal in TryClaim")
				return false
			}
			return true
		}
		c.logger.Debug().
			Str("supplier", supplier).
			Str("owner", owner).
			Msg("supplier already claimed by another instance")
		return false
	}

	// Successfully claimed — record timestamp for newest-first release ordering
	c.claimedMu.Lock()
	c.claimed[supplier] = time.Now()
	c.claimedMu.Unlock()

	c.logger.Debug().
		Str("supplier", supplier).
		Str("claim_key", claimKey).
		Dur("ttl", c.config.ClaimTTL).
		Msg("claimed supplier")

	supplierClaimedTotal.WithLabelValues(supplier, c.instanceID).Inc()

	// Invoke claim callback
	if c.onClaimFn != nil {
		if err := c.onClaimFn(ctx, supplier); err != nil {
			c.logger.Error().
				Err(err).
				Str("supplier", supplier).
				Msg("claim callback failed")
			// Release the claim since we couldn't start lifecycle
			_ = c.Release(ctx, supplier)
			return false
		}
	}

	return true
}

// Release releases a supplier claim.
// The release callback is invoked BEFORE the Redis claim key is deleted. If the
// callback returns an error, the claim key is kept and the supplier stays
// claimed by this instance.
func (c *SupplierClaimer) Release(ctx context.Context, supplier string) error {
	// Invoke release callback FIRST. If it returns an error, we abort and keep
	// the Redis claim key alive to prevent orphan-reclaim thrashing.
	if c.onReleaseFn != nil {
		if err := c.onReleaseFn(ctx, supplier); err != nil {
			c.logger.Debug().
				Err(err).
				Str("supplier", supplier).
				Msg("release callback failed, keeping claim")
			return err
		}
	}

	claimKey := c.redisClient.KB().MinerClaimKey(supplier)

	// Only delete if we own it (atomic check-and-delete)
	// Use Lua script to ensure atomicity
	script := redis.NewScript(`
		if redis.call("get", KEYS[1]) == ARGV[1] then
			return redis.call("del", KEYS[1])
		else
			return 0
		end
	`)

	result, err := script.Run(ctx, c.redisClient, []string{claimKey}, c.instanceID).Int64()
	if err != nil {
		return fmt.Errorf("failed to release claim: %w", err)
	}

	if result == 0 {
		// We didn't own it, but still update local state
		c.logger.Debug().Str("supplier", supplier).Msg("claim was not owned by us")
	}

	c.claimedMu.Lock()
	delete(c.claimed, supplier)
	c.claimedMu.Unlock()

	c.logger.Debug().
		Str("supplier", supplier).
		Msg("released supplier claim")

	supplierReleasedTotal.WithLabelValues(supplier, c.instanceID).Inc()

	return nil
}

// IsClaimed returns true if the supplier is claimed by this instance.
func (c *SupplierClaimer) IsClaimed(supplier string) bool {
	c.claimedMu.RLock()
	defer c.claimedMu.RUnlock()
	_, ok := c.claimed[supplier]
	return ok
}

// ClaimedCount returns the number of suppliers claimed by this instance.
func (c *SupplierClaimer) ClaimedCount() int {
	c.claimedMu.RLock()
	defer c.claimedMu.RUnlock()
	return len(c.claimed)
}

// ClaimedSuppliers returns a copy of the claimed suppliers set.
func (c *SupplierClaimer) ClaimedSuppliers() []string {
	c.claimedMu.RLock()
	defer c.claimedMu.RUnlock()
	suppliers := make([]string, 0, len(c.claimed))
	for supplier := range c.claimed {
		suppliers = append(suppliers, supplier)
	}
	return suppliers
}

// registerInstance registers this miner instance in Redis.
func (c *SupplierClaimer) registerInstance(ctx context.Context) error {
	instanceKey := c.redisClient.KB().MinerInstanceKey(c.instanceID)
	activeSetKey := c.redisClient.KB().MinerActiveSetKey()

	// Set instance key with TTL
	if err := c.redisClient.Set(ctx, instanceKey, time.Now().UnixNano(), c.config.InstanceTTL).Err(); err != nil {
		return fmt.Errorf("failed to set instance key: %w", err)
	}

	// Add to active set
	if err := c.redisClient.SAdd(ctx, activeSetKey, c.instanceID).Err(); err != nil {
		return fmt.Errorf("failed to add to active set: %w", err)
	}

	c.logger.Debug().
		Msg("registered miner instance")

	return nil
}

// unregisterInstance removes this miner instance from Redis.
func (c *SupplierClaimer) unregisterInstance(ctx context.Context) error {
	instanceKey := c.redisClient.KB().MinerInstanceKey(c.instanceID)
	activeSetKey := c.redisClient.KB().MinerActiveSetKey()

	// Remove from active set
	c.redisClient.SRem(ctx, activeSetKey, c.instanceID)

	// Delete instance key
	c.redisClient.Del(ctx, instanceKey)

	c.logger.Debug().
		Msg("unregistered miner instance")

	return nil
}

// initialClaim attempts to claim every configured supplier on startup. Existing
// claims owned by another miner are left untouched, which makes additional
// miners passive standbys until the primary's leases expire.
func (c *SupplierClaimer) initialClaim(ctx context.Context) error {
	c.allSuppliersMu.RLock()
	suppliers := make([]string, len(c.allSuppliers))
	copy(suppliers, c.allSuppliers)
	c.allSuppliersMu.RUnlock()

	skipped := 0
	for _, supplier := range suppliers {
		if !c.TryClaim(ctx, supplier) {
			skipped++
		}
	}

	c.logger.Info().
		Int("configured", len(suppliers)).
		Int("claimed", c.ClaimedCount()).
		Int("skipped", skipped).
		Msg("initial claim complete")

	return nil
}

// instanceHeartbeatLoop periodically renews instance registration.
func (c *SupplierClaimer) instanceHeartbeatLoop() {
	defer c.wg.Done()

	ticker := time.NewTicker(c.config.InstanceHeartbeatRate)
	defer ticker.Stop()

	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			if err := c.registerInstance(c.ctx); err != nil {
				c.logger.Warn().Err(err).Msg("failed to heartbeat instance")
			}

			// Clean up stale instances from active set
			c.cleanupStaleInstances()
		}
	}
}

// cleanupStaleInstances removes instances whose keys have expired.
func (c *SupplierClaimer) cleanupStaleInstances() {
	activeSetKey := c.redisClient.KB().MinerActiveSetKey()

	// Get all instances in the set
	instances, err := c.redisClient.SMembers(c.ctx, activeSetKey).Result()
	if err != nil {
		return
	}

	for _, instanceID := range instances {
		instanceKey := c.redisClient.KB().MinerInstanceKey(instanceID)

		// Check if instance key exists
		exists, err := c.redisClient.Exists(c.ctx, instanceKey).Result()
		if err != nil {
			continue
		}

		if exists == 0 {
			// Instance key expired, remove from set
			c.redisClient.SRem(c.ctx, activeSetKey, instanceID)
			c.logger.Debug().
				Str("stale_instance", instanceID).
				Msg("removed stale instance from active set")
		}
	}
}

// renewLoop periodically renews all claimed supplier leases.
func (c *SupplierClaimer) renewLoop() {
	defer c.wg.Done()

	ticker := time.NewTicker(c.config.RenewRate)
	defer ticker.Stop()

	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			c.renewAllClaims()
		}
	}
}

// renewAllClaims renews all claimed supplier leases.
func (c *SupplierClaimer) renewAllClaims() {
	c.claimedMu.RLock()
	claimed := make([]string, 0, len(c.claimed))
	for supplier := range c.claimed {
		claimed = append(claimed, supplier)
	}
	c.claimedMu.RUnlock()

	if len(claimed) == 0 {
		return
	}

	c.logger.Debug().
		Int("claim_count", len(claimed)).
		Dur("ttl", c.config.ClaimTTL).
		Msg("renewing claims")

	for _, supplier := range claimed {
		claimKey := c.redisClient.KB().MinerClaimKey(supplier)

		// Renew only if we still own it (check-and-renew)
		owner, err := c.redisClient.Get(c.ctx, claimKey).Result()
		if err != nil {
			if err == redis.Nil {
				// Claim expired, try to reclaim
				c.logger.Warn().
					Str("supplier", supplier).
					Str("claim_key", claimKey).
					Msg("claim expired, attempting reclaim")
				c.claimedMu.Lock()
				delete(c.claimed, supplier)
				c.claimedMu.Unlock()
				c.TryClaim(c.ctx, supplier)
			} else {
				c.logger.Warn().
					Err(err).
					Str("supplier", supplier).
					Msg("failed to get claim owner")
			}
			continue
		}

		if owner != c.instanceID {
			// Someone else claimed it
			c.logger.Warn().
				Str("supplier", supplier).
				Str("owner", owner).
				Str("expected", c.instanceID).
				Msg("claim stolen by another instance")
			c.claimedMu.Lock()
			delete(c.claimed, supplier)
			c.claimedMu.Unlock()
			continue
		}

		// Renew the lease - check BOTH error AND result
		// Expire returns (bool, error) - bool is false if key doesn't exist
		renewed, err := c.redisClient.Expire(c.ctx, claimKey, c.config.ClaimTTL).Result()
		if err != nil {
			c.logger.Warn().Err(err).Str("supplier", supplier).Msg("failed to renew claim")
		} else if !renewed {
			// Key didn't exist when we tried to renew - it expired between GET and EXPIRE
			c.logger.Warn().
				Str("supplier", supplier).
				Str("claim_key", claimKey).
				Msg("claim key disappeared during renewal (race condition)")
			c.claimedMu.Lock()
			delete(c.claimed, supplier)
			c.claimedMu.Unlock()
			// Try to reclaim
			c.TryClaim(c.ctx, supplier)
		}
	}
}

// orphanScanLoop periodically checks for supplier claims that are missing or
// expired. It intentionally does not release healthy claims for fair-share
// balancing; this keeps miner failover classic and avoids active/active churn.
func (c *SupplierClaimer) orphanScanLoop() {
	defer c.wg.Done()

	ticker := time.NewTicker(c.config.RebalanceInterval)
	defer ticker.Stop()

	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			c.recoverUnclaimedSuppliers()
		}
	}
}

// recoverUnclaimedSuppliers claims suppliers whose Redis claim is missing. In
// normal operation this is a no-op for standby miners until the primary dies and
// its claim leases expire.
func (c *SupplierClaimer) recoverUnclaimedSuppliers() {
	currentCount := c.ClaimedCount()
	c.allSuppliersMu.RLock()
	totalSuppliers := len(c.allSuppliers)
	c.allSuppliersMu.RUnlock()

	supplierClaimedGauge.WithLabelValues(c.instanceID).Set(float64(currentCount))
	supplierFairShareGauge.WithLabelValues(c.instanceID).Set(float64(totalSuppliers))

	c.logger.Debug().
		Int("configured", totalSuppliers).
		Int("current", currentCount).
		Msg("orphan supplier scan")

	c.claimOrphaned()
}

// claimOrphaned scans all configured suppliers and claims any that have no active
// claim key in Redis. This catches suppliers that fell through the cracks — e.g.,
// when a primary miner dies and its supplier claim leases expire.
func (c *SupplierClaimer) claimOrphaned() {
	c.allSuppliersMu.RLock()
	suppliers := make([]string, len(c.allSuppliers))
	copy(suppliers, c.allSuppliers)
	c.allSuppliersMu.RUnlock()

	if len(suppliers) == 0 {
		return
	}

	orphaned := 0
	claimed := 0
	for _, supplier := range suppliers {
		if c.IsClaimed(supplier) {
			continue
		}

		claimKey := c.redisClient.KB().MinerClaimKey(supplier)
		exists, err := c.redisClient.Exists(c.ctx, claimKey).Result()
		if err != nil {
			c.logger.Warn().Err(err).Str("supplier", supplier).
				Msg("failed to check claim key for orphan detection")
			continue
		}

		if exists == 0 {
			orphaned++
			c.logger.Debug().Str("supplier", supplier).
				Msg("detected orphaned supplier (no claim key), attempting to claim")
			if c.TryClaim(c.ctx, supplier) {
				claimed++
			}
		}
	}

	if orphaned > 0 {
		c.logger.Info().
			Int("orphaned_detected", orphaned).
			Int("orphaned_claimed", claimed).
			Int("total_suppliers", len(suppliers)).
			Msg("orphaned supplier scan complete")
	}
}

// UpdateSuppliers updates the list of configured suppliers.
// Called when KeyManager detects a config change.
func (c *SupplierClaimer) UpdateSuppliers(suppliers []string) {
	suppliers = append([]string(nil), suppliers...)
	sort.Strings(suppliers)

	c.allSuppliersMu.RLock()
	unchanged := len(c.allSuppliers) == len(suppliers)
	if unchanged {
		for i := range suppliers {
			if c.allSuppliers[i] != suppliers[i] {
				unchanged = false
				break
			}
		}
	}
	c.allSuppliersMu.RUnlock()

	if unchanged {
		c.logger.Debug().Int("suppliers", len(suppliers)).Msg("supplier list unchanged")
		return
	}

	c.allSuppliersMu.Lock()
	c.allSuppliers = suppliers
	c.allSuppliersMu.Unlock()

	c.logger.Info().
		Int("suppliers", len(suppliers)).
		Msg("updated supplier list")

	// Re-scan immediately so a primary can pick up newly configured suppliers.
	if c.ctx != nil {
		c.recoverUnclaimedSuppliers()
	}
}
