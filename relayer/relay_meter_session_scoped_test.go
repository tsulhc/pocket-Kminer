//go:build test

package relayer

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/stretchr/testify/require"

	"github.com/pokt-network/pocket-relay-miner/logging"
	redisutil "github.com/pokt-network/pocket-relay-miner/transport/redis"
	sharedtypes "github.com/pokt-network/poktroll/x/shared/types"
)

type epochMeterParams struct {
	mu          sync.Mutex
	latest      *sharedtypes.Params
	byHeight    map[int64]*sharedtypes.Params
	heights     []int64
	latestCalls int
}

func (c *epochMeterParams) GetLatestSharedParams(context.Context) (*sharedtypes.Params, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.latestCalls++
	return c.latest, nil
}

func (c *epochMeterParams) GetSharedParams(_ context.Context, height int64) (*sharedtypes.Params, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.heights = append(c.heights, height)
	if p, ok := c.byHeight[height]; ok {
		return p, nil
	}
	return c.latest, nil
}

func (c *epochMeterParams) queriedHeights() []int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]int64(nil), c.heights...)
}

type fixedMeterCUProvider struct{ cupr uint64 }

func (p fixedMeterCUProvider) GetServiceComputeUnits(context.Context, string, int64) uint64 {
	return p.cupr
}

func newSessionScopedMeter(t *testing.T, params *epochMeterParams, provider ServiceComputeUnitsProvider) *RelayMeter {
	t.Helper()
	mr, err := miniredis.Run()
	require.NoError(t, err)
	t.Cleanup(mr.Close)
	ctx := context.Background()
	rdb, err := redisutil.NewClient(ctx, redisutil.ClientConfig{URL: fmt.Sprintf("redis://%s", mr.Addr())})
	require.NoError(t, err)
	t.Cleanup(func() { _ = rdb.Close() })
	app := &fakeAppClient{addr: "pokt1app_scoped"}
	app.stakeUpokt.Store(1_000_000)
	meter := NewRelayMeter(logging.NewLoggerFromConfig(logging.DefaultConfig()), rdb, app, nil,
		&fakeSessionClient{numSuppliers: 1}, nil, params, nil, nil,
		RelayMeterConfig{RedisKeyPrefix: "ha"})
	meter.SetServiceComputeUnitsProvider(provider)
	require.NoError(t, meter.Start(ctx))
	t.Cleanup(func() { _ = meter.Close() })
	return meter
}

func TestRelayMeter_GetRelayCostUsesSessionStartEpoch(t *testing.T) {
	params := &epochMeterParams{
		latest:   &sharedtypes.Params{ComputeUnitsToTokensMultiplier: 10, ComputeUnitCostGranularity: 1},
		byHeight: map[int64]*sharedtypes.Params{91: {ComputeUnitsToTokensMultiplier: 100, ComputeUnitCostGranularity: 1}},
	}
	meter := newSessionScopedMeter(t, params, fixedMeterCUProvider{cupr: 7})
	cost, err := meter.getRelayCost(context.Background(), "seda", 91)
	require.NoError(t, err)
	require.Equal(t, int64(700), cost)
	require.Equal(t, []int64{91}, params.queriedHeights())
}

func TestRelayMeter_CalculateMaxStakeUsesEndedSessionParams(t *testing.T) {
	oldParams := &sharedtypes.Params{
		NumBlocksPerSession:         10,
		ClaimWindowOpenOffsetBlocks: 2, ClaimWindowCloseOffsetBlocks: 2,
		ProofWindowOpenOffsetBlocks: 3, ProofWindowCloseOffsetBlocks: 3,
	}
	newParams := &sharedtypes.Params{
		NumBlocksPerSession:         10,
		ClaimWindowOpenOffsetBlocks: 10, ClaimWindowCloseOffsetBlocks: 10,
		ProofWindowOpenOffsetBlocks: 10, ProofWindowCloseOffsetBlocks: 10,
	}
	params := &epochMeterParams{latest: newParams, byHeight: map[int64]*sharedtypes.Params{100: oldParams}}
	meter := newSessionScopedMeter(t, params, nil)
	maxStake, _, _, err := meter.calculateMaxStake(context.Background(), "pokt1app_scoped", "seda", 100, 110)
	require.NoError(t, err)
	require.Equal(t, int64(500_000), maxStake)
	require.Equal(t, []int64{100}, params.queriedHeights())
}
