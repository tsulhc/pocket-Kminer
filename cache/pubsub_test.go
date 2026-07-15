//go:build test

package cache

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/stretchr/testify/require"

	"github.com/pokt-network/pocket-relay-miner/logging"
	redisutil "github.com/pokt-network/pocket-relay-miner/transport/redis"
)

func newPubSubTestRedis(t *testing.T) *redisutil.Client {
	t.Helper()
	mr, err := miniredis.Run()
	require.NoError(t, err)
	t.Cleanup(mr.Close)

	client, err := redisutil.NewClient(context.Background(), redisutil.ClientConfig{
		URL: fmt.Sprintf("redis://%s", mr.Addr()),
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func TestSubscribeToInvalidations_ActiveOnReturn(t *testing.T) {
	redisClient := newPubSubTestRedis(t)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	logger := logging.NewLoggerFromConfig(logging.DefaultConfig())
	require.NoError(t, SubscribeToInvalidations(ctx, redisClient, logger, "service", func(context.Context, string) error {
		return nil
	}))

	channel := redisClient.KB().EventChannel("service", "invalidate")
	counts, err := redisClient.PubSubNumSub(context.Background(), channel).Result()
	require.NoError(t, err)
	require.GreaterOrEqual(t, counts[channel], int64(1))
}

func TestSubscribeToInvalidations_DeliversMessagePublishedImmediately(t *testing.T) {
	redisClient := newPubSubTestRedis(t)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	var handled atomic.Int64
	logger := logging.NewLoggerFromConfig(logging.DefaultConfig())
	require.NoError(t, SubscribeToInvalidations(ctx, redisClient, logger, "application", func(context.Context, string) error {
		handled.Add(1)
		return nil
	}))

	require.NoError(t, PublishInvalidation(ctx, redisClient, logger, "application", "x"))
	require.Eventually(t, func() bool {
		return handled.Load() == 1
	}, time.Second, time.Millisecond)
}

func TestSubscribeToInvalidations_ContextCanceledWhileWaiting(t *testing.T) {
	redisClient := newPubSubTestRedis(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	logger := logging.NewLoggerFromConfig(logging.DefaultConfig())
	err := SubscribeToInvalidations(ctx, redisClient, logger, "service", func(context.Context, string) error {
		return nil
	})
	require.ErrorIs(t, err, context.Canceled)
}
