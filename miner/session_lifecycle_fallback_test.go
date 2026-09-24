//go:build test

package miner

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hashicorp/go-version"
	"github.com/stretchr/testify/require"

	localclient "github.com/pokt-network/pocket-relay-miner/client"
	"github.com/pokt-network/pocket-relay-miner/logging"
	"github.com/pokt-network/poktroll/pkg/client"
)

type fallbackHeightBlockClient struct {
	lastHeight    int64
	currentHeight int64
}

func (b *fallbackHeightBlockClient) LastBlock(context.Context) client.Block {
	return &mockBlock{height: b.lastHeight}
}

func (b *fallbackHeightBlockClient) CurrentHeight(context.Context) (int64, error) {
	return b.currentHeight, nil
}

func (b *fallbackHeightBlockClient) CommittedBlocksSequence(context.Context) client.BlockReplayObservable {
	return nil
}

func (b *fallbackHeightBlockClient) Close() {}

func (b *fallbackHeightBlockClient) GetChainVersion() *version.Version { return nil }

func TestSessionLifecycleCurrentChainHeight_PrefersRPCProvider(t *testing.T) {
	m := &SessionLifecycleManager{
		blockClient: &fallbackHeightBlockClient{lastHeight: 100, currentHeight: 120},
	}

	height, err := m.currentChainHeight(context.Background())
	require.NoError(t, err)
	require.Equal(t, int64(120), height)
}

func TestSessionLifecycleCurrentChainHeight_FallsBackToLastBlock(t *testing.T) {
	m := &SessionLifecycleManager{
		blockClient: &mockBlockClient{currentHeight: 100},
	}

	height, err := m.currentChainHeight(context.Background())
	require.NoError(t, err)
	require.Equal(t, int64(100), height)
}

type fallbackProbeBlockClient struct {
	fallbackHeightBlockClient
	currentHeightFn func(context.Context) (int64, error)
}

func (b *fallbackProbeBlockClient) CurrentHeight(ctx context.Context) (int64, error) {
	return b.currentHeightFn(ctx)
}

func TestProcessLifecycleHeightRejectsDuplicateAndOutOfOrderHeights(t *testing.T) {
	var transitionMu sync.Mutex
	var lastHeight atomic.Int64
	var processed []int64
	var previousHeights []int64

	for _, height := range []int64{100, 100, 99, 101} {
		processLifecycleHeight(&transitionMu, &lastHeight, height, func(previousHeight int64) {
			processed = append(processed, height)
			previousHeights = append(previousHeights, previousHeight)
		})
	}

	require.Equal(t, []int64{100, 101}, processed)
	require.Equal(t, []int64{0, 100}, previousHeights)
	require.Equal(t, int64(101), lastHeight.Load())
}

func TestSessionLifecycleFallbackRunsBeforeAnyBlockEventAndPreservesEventAge(t *testing.T) {
	var heightQueries atomic.Int64
	blockClient := &fallbackProbeBlockClient{
		currentHeightFn: func(context.Context) (int64, error) {
			if heightQueries.Add(1) == 1 {
				return 0, errors.New("temporary RPC failure")
			}
			return 120, nil
		},
	}
	m := &SessionLifecycleManager{
		logger:      logging.NewLoggerFromConfig(logging.DefaultConfig()),
		blockClient: blockClient,
	}

	var lastHeight atomic.Int64 // No block event has been received or processed.
	var lastEventTimeNano atomic.Int64
	staleEventTime := time.Now().Add(-time.Hour).UnixNano()
	lastEventTimeNano.Store(staleEventTime)
	var transitionMu sync.Mutex
	var processed []int64
	processedMu := sync.Mutex{}
	advanced := make(chan struct{}, 1)
	onHeight := func(height int64) {
		processLifecycleHeight(&transitionMu, &lastHeight, height, func(int64) {
			processedMu.Lock()
			processed = append(processed, height)
			processedMu.Unlock()
			advanced <- struct{}{}
		})
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		m.runBlockEventFallbackWithInterval(ctx, &lastHeight, &lastEventTimeNano, onHeight, time.Millisecond)
		close(done)
	}()

	select {
	case <-advanced:
	case <-time.After(time.Second):
		cancel()
		t.Fatal("RPC fallback did not advance height without an initial block event")
	}
	require.Eventually(t, func() bool { return heightQueries.Load() >= 3 }, time.Second, time.Millisecond,
		"stale-event fallback should continue querying at its bounded interval")
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("RPC fallback did not stop after cancellation")
	}

	processedMu.Lock()
	require.Equal(t, []int64{120}, processed)
	processedMu.Unlock()
	require.Equal(t, int64(120), lastHeight.Load())
	require.Equal(t, staleEventTime, lastEventTimeNano.Load(), "RPC fallback must not refresh real-event freshness")
}

func TestSessionLifecycleFallbackAndCallbackWaitRecoverFromSameSilentFeed(t *testing.T) {
	const targetHeight = 120
	var currentHeight atomic.Int64
	currentHeight.Store(100)
	blockClient := &callbackHeightBlockClient{}
	blockClient.lastHeight.Store(100)
	blockClient.currentHeightFn = func(context.Context) (int64, error) {
		return currentHeight.Load(), nil
	}
	subscribed := make(chan struct{}, 1)
	blockClient.subscribeFn = func(context.Context, int) <-chan *localclient.SimpleBlock {
		subscribed <- struct{}{}
		return make(chan *localclient.SimpleBlock) // open, silent event feed
	}
	canonicalBlock := &mockBlock{height: targetHeight, hash: []byte("divergent-height-canonical-hash")}
	blockClient.getBlockAtHeight = func(context.Context, int64) (client.Block, error) {
		return canonicalBlock, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	callback := newCallbackHeightTestCallback(blockClient)
	type callbackResult struct {
		block client.Block
		err   error
	}
	callbackDone := make(chan callbackResult, 1)
	go func() {
		block, err := callback.waitForBlockWithInterval(ctx, targetHeight, time.Millisecond)
		callbackDone <- callbackResult{block: block, err: err}
	}()
	select {
	case <-subscribed:
	case <-time.After(time.Second):
		t.Fatal("callback did not subscribe to the silent event feed")
	}

	m := &SessionLifecycleManager{
		logger:      logging.NewLoggerFromConfig(logging.DefaultConfig()),
		blockClient: blockClient,
	}
	var lastHeight atomic.Int64
	lastHeight.Store(100)
	var lastEventTimeNano atomic.Int64
	lastEventTimeNano.Store(time.Now().Add(-time.Hour).UnixNano())
	var transitionMu sync.Mutex
	transitionHeight := make(chan int64, 1)
	fallbackDone := make(chan struct{})
	go func() {
		m.runBlockEventFallbackWithInterval(ctx, &lastHeight, &lastEventTimeNano, func(height int64) {
			processLifecycleHeight(&transitionMu, &lastHeight, height, func(int64) {
				transitionHeight <- height
			})
		}, time.Millisecond)
		close(fallbackDone)
	}()

	currentHeight.Store(targetHeight)
	select {
	case result := <-callbackDone:
		require.NoError(t, result.err)
		require.Same(t, canonicalBlock, result.block)
	case <-time.After(time.Second):
		t.Fatal("callback remained blocked on the open-but-silent subscription")
	}
	select {
	case height := <-transitionHeight:
		require.Equal(t, int64(targetHeight), height)
	case <-time.After(time.Second):
		t.Fatal("lifecycle RPC fallback did not observe the same chain-height advance")
	}
	cancel()
	select {
	case <-fallbackDone:
	case <-time.After(time.Second):
		t.Fatal("lifecycle RPC fallback did not stop after cancellation")
	}
	require.Equal(t, []int64{targetHeight}, blockClient.requestedBlockHeights())
}
