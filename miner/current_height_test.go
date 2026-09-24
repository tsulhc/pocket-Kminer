package miner

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type blockingCurrentHeightProvider struct{}

func (blockingCurrentHeightProvider) CurrentHeight(ctx context.Context) (int64, error) {
	<-ctx.Done()
	return 0, ctx.Err()
}

func TestQueryCurrentHeightWithTimeoutBoundsBlockedProvider(t *testing.T) {
	start := time.Now()
	height, err := queryCurrentHeightWithTimeout(
		context.Background(),
		blockingCurrentHeightProvider{},
		10*time.Millisecond,
	)

	require.Zero(t, height)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Less(t, time.Since(start), time.Second)
}

func TestQueryCurrentHeightWithTimeoutHonorsParentCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	height, err := queryCurrentHeightWithTimeout(
		ctx,
		blockingCurrentHeightProvider{},
		time.Second,
	)

	require.Zero(t, height)
	require.ErrorIs(t, err, context.Canceled)
}
