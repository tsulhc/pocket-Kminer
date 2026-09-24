package miner

import (
	"context"
	"time"
)

// currentHeightQueryTimeout bounds direct chain-height probes so a stuck RPC
// endpoint cannot block lifecycle timing or suppress the block-event fast path.
const currentHeightQueryTimeout = 5 * time.Second

func queryCurrentHeight(ctx context.Context, provider currentHeightProvider) (int64, error) {
	return queryCurrentHeightWithTimeout(ctx, provider, currentHeightQueryTimeout)
}

func queryCurrentHeightWithTimeout(
	ctx context.Context,
	provider currentHeightProvider,
	timeout time.Duration,
) (int64, error) {
	if timeout <= 0 {
		timeout = currentHeightQueryTimeout
	}
	queryCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return provider.CurrentHeight(queryCtx)
}
