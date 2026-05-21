//go:build test

package miner

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	redisutil "github.com/pokt-network/pocket-relay-miner/transport/redis"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/suite"
)

// SupplierClaimerTestSuite tests the SupplierClaimer functionality.
// Uses miniredis for real Redis operations (Rule #1: no mocks).
type SupplierClaimerTestSuite struct {
	suite.Suite
	miniRedis   *miniredis.Miniredis
	redisClient *redisutil.Client
	ctx         context.Context
}

func (s *SupplierClaimerTestSuite) SetupSuite() {
	mr, err := miniredis.Run()
	s.Require().NoError(err, "failed to create miniredis")
	s.miniRedis = mr
	s.ctx = context.Background()

	redisURL := fmt.Sprintf("redis://%s", mr.Addr())
	client, err := redisutil.NewClient(s.ctx, redisutil.ClientConfig{
		URL: redisURL,
	})
	s.Require().NoError(err, "failed to create Redis client")
	s.redisClient = client
}

func (s *SupplierClaimerTestSuite) SetupTest() {
	s.miniRedis.FlushAll()
}

func (s *SupplierClaimerTestSuite) TearDownSuite() {
	if s.miniRedis != nil {
		s.miniRedis.Close()
	}
	if s.redisClient != nil {
		s.redisClient.Close()
	}
}

// createTestClaimer creates a SupplierClaimer for testing.
func (s *SupplierClaimerTestSuite) createTestClaimer(instanceID string) *SupplierClaimer {
	logger := zerolog.Nop()
	return NewSupplierClaimer(logger, s.redisClient, instanceID, SupplierClaimerConfig{})
}

// TestInitialClaimClaimsEveryAvailableSupplier verifies the single-primary
// behavior: active miner count does not cap this instance at a fair share.
func (s *SupplierClaimerTestSuite) TestInitialClaimClaimsEveryAvailableSupplier() {
	claimer := s.createTestClaimer("primary-instance")
	claimer.ctx, claimer.cancelFn = context.WithCancel(s.ctx)
	defer claimer.cancelFn()

	err := claimer.registerInstance(s.ctx)
	s.Require().NoError(err)

	standby := s.createTestClaimer("standby-instance")
	err = standby.registerInstance(s.ctx)
	s.Require().NoError(err)

	claimer.allSuppliers = []string{"supplierA", "supplierB", "supplierC", "supplierD"}
	err = claimer.initialClaim(s.ctx)
	s.Require().NoError(err)
	s.Require().Equal(4, claimer.ClaimedCount())

	for _, supplier := range claimer.allSuppliers {
		owner, err := s.redisClient.Get(s.ctx, s.redisClient.KB().MinerClaimKey(supplier)).Result()
		s.Require().NoError(err)
		s.Require().Equal("primary-instance", owner)
	}
}

// TestStandbyClaimsOnlyExpiredSupplier verifies classic failover: a standby does
// not steal healthy claims, but can claim a supplier after the primary lease is gone.
func (s *SupplierClaimerTestSuite) TestStandbyClaimsOnlyExpiredSupplier() {
	primary := s.createTestClaimer("primary-instance")
	primary.ctx, primary.cancelFn = context.WithCancel(s.ctx)
	defer primary.cancelFn()
	s.Require().NoError(primary.registerInstance(s.ctx))

	suppliers := []string{"supplierA", "supplierB"}
	primary.allSuppliers = suppliers
	s.Require().NoError(primary.initialClaim(s.ctx))
	s.Require().Equal(2, primary.ClaimedCount())

	standby := s.createTestClaimer("standby-instance")
	standby.ctx, standby.cancelFn = context.WithCancel(s.ctx)
	defer standby.cancelFn()
	s.Require().NoError(standby.registerInstance(s.ctx))
	standby.allSuppliers = suppliers
	s.Require().NoError(standby.initialClaim(s.ctx))
	s.Require().Equal(0, standby.ClaimedCount())

	claimKey := s.redisClient.KB().MinerClaimKey("supplierA")
	s.Require().NoError(s.redisClient.Del(s.ctx, claimKey).Err())
	standby.recoverUnclaimedSuppliers()

	s.Require().True(standby.IsClaimed("supplierA"))
	s.Require().False(standby.IsClaimed("supplierB"))
	owner, err := s.redisClient.Get(s.ctx, claimKey).Result()
	s.Require().NoError(err)
	s.Require().Equal("standby-instance", owner)
}

// TestClaimedMapTimestamp verifies that after TryClaim succeeds, the claimed map
// entry contains a non-zero time.Time value.
func (s *SupplierClaimerTestSuite) TestClaimedMapTimestamp() {
	claimer := s.createTestClaimer("test-instance-3")
	claimer.ctx, claimer.cancelFn = context.WithCancel(s.ctx)
	defer claimer.cancelFn()

	err := claimer.registerInstance(s.ctx)
	s.Require().NoError(err)

	claimer.allSuppliers = []string{"supplierX"}

	before := time.Now()
	ok := claimer.TryClaim(s.ctx, "supplierX")
	s.Require().True(ok, "TryClaim should succeed")
	after := time.Now()

	// Verify the claimed map stores a valid timestamp
	claimer.claimedMu.RLock()
	claimedAt, exists := claimer.claimed["supplierX"]
	claimer.claimedMu.RUnlock()

	s.Require().True(exists, "supplierX should be in claimed map")
	s.Require().False(claimedAt.IsZero(), "claimed timestamp should not be zero")
	s.Require().True(claimedAt.After(before) || claimedAt.Equal(before),
		"claimed timestamp should be >= before time")
	s.Require().True(claimedAt.Before(after) || claimedAt.Equal(after),
		"claimed timestamp should be <= after time")
}

func TestSupplierClaimerTestSuite(t *testing.T) {
	suite.Run(t, new(SupplierClaimerTestSuite))
}
