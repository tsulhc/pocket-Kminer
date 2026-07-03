//go:build test

package miner

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/stretchr/testify/require"

	"github.com/pokt-network/pocket-relay-miner/logging"
	redisutil "github.com/pokt-network/pocket-relay-miner/transport/redis"
)

func newWatchdogTestManager(t *testing.T, handler func(string)) (*SupplierManager, *redisutil.Client) {
	t.Helper()

	mr, err := miniredis.Run()
	require.NoError(t, err)
	t.Cleanup(mr.Close)

	ctx := context.Background()
	redisClient, err := redisutil.NewClient(ctx, redisutil.ClientConfig{URL: fmt.Sprintf("redis://%s", mr.Addr())})
	require.NoError(t, err)
	t.Cleanup(func() { _ = redisClient.Close() })

	m := &SupplierManager{
		logger: logging.NewLoggerFromConfig(logging.DefaultConfig()),
		config: SupplierManagerConfig{
			RedisClient:                       redisClient,
			MinerID:                           "miner-a",
			LeaderOwnerMismatchMaxConsecutive: 1,
			LeaderOwnerMismatchHandler:        handler,
		},
		suppliers: map[string]*SupplierState{
			"pokt1supplier": {},
		},
	}

	return m, redisClient
}

func TestCheckLeaderOwnerConsistency_NoLeaderTriggersHandler(t *testing.T) {
	reasons := make(chan string, 1)
	m, _ := newWatchdogTestManager(t, func(reason string) { reasons <- reason })

	m.checkLeaderOwnerConsistency(context.Background())

	select {
	case gotReason := <-reasons:
		require.Equal(t, "no_leader", gotReason)
	case <-time.After(time.Second):
		t.Fatal("handler was not called")
	}
}

func TestCheckLeaderOwnerConsistency_MismatchTriggersHandler(t *testing.T) {
	reasons := make(chan string, 1)
	m, redisClient := newWatchdogTestManager(t, func(reason string) { reasons <- reason })
	require.NoError(t, redisClient.Set(context.Background(), redisClient.KB().GlobalLeaderKey(), "miner-b", 0).Err())

	m.checkLeaderOwnerConsistency(context.Background())

	select {
	case gotReason := <-reasons:
		require.Equal(t, "mismatch", gotReason)
	case <-time.After(time.Second):
		t.Fatal("handler was not called")
	}
}

func TestCheckLeaderOwnerConsistency_OKDoesNotTriggerHandler(t *testing.T) {
	called := false
	m, redisClient := newWatchdogTestManager(t, func(string) { called = true })
	m.leaderOwnerMismatchConsecutive.Store(3)
	require.NoError(t, redisClient.Set(context.Background(), redisClient.KB().GlobalLeaderKey(), "miner-a", 0).Err())

	m.checkLeaderOwnerConsistency(context.Background())

	require.False(t, called)
	require.Equal(t, int32(0), m.leaderOwnerMismatchConsecutive.Load())
}

func TestCheckLeaderOwnerConsistency_ProductionMinerPrefixMatchesGlobalLeader(t *testing.T) {
	called := false
	m, redisClient := newWatchdogTestManager(t, func(string) { called = true })
	m.config.MinerID = "miner-pocket-relayminer-pg-1-343031"
	m.leaderOwnerMismatchConsecutive.Store(3)
	require.NoError(t, redisClient.Set(context.Background(), redisClient.KB().GlobalLeaderKey(), "pocket-relayminer-pg-1-343031", 0).Err())

	m.checkLeaderOwnerConsistency(context.Background())

	require.False(t, called)
	require.Equal(t, int32(0), m.leaderOwnerMismatchConsecutive.Load())
}
