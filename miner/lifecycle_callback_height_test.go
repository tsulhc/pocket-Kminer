package miner

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pokt-network/pocket-relay-miner/client"
	pocktclient "github.com/pokt-network/poktroll/pkg/client"
	"github.com/stretchr/testify/require"
)

type callbackHeightBlockClient struct {
	pocktclient.BlockClient

	lastHeight       atomic.Int64
	currentHeightFn  func(context.Context) (int64, error)
	subscribeFn      func(context.Context, int) <-chan *client.SimpleBlock
	getBlockAtHeight func(context.Context, int64) (pocktclient.Block, error)
	getBlockMu       sync.Mutex
	requestedHeights []int64
}

func (c *callbackHeightBlockClient) LastBlock(context.Context) pocktclient.Block {
	return &mockBlock{height: c.lastHeight.Load()}
}

func (c *callbackHeightBlockClient) CurrentHeight(ctx context.Context) (int64, error) {
	return c.currentHeightFn(ctx)
}

func (c *callbackHeightBlockClient) Subscribe(ctx context.Context, bufferSize int) <-chan *client.SimpleBlock {
	return c.subscribeFn(ctx, bufferSize)
}

func (c *callbackHeightBlockClient) GetBlockAtHeight(ctx context.Context, height int64) (pocktclient.Block, error) {
	c.getBlockMu.Lock()
	c.requestedHeights = append(c.requestedHeights, height)
	c.getBlockMu.Unlock()
	return c.getBlockAtHeight(ctx, height)
}

func (c *callbackHeightBlockClient) requestedBlockHeights() []int64 {
	c.getBlockMu.Lock()
	defer c.getBlockMu.Unlock()
	return append([]int64(nil), c.requestedHeights...)
}

type callbackLastOnlyBlockClient struct {
	pocktclient.BlockClient
	lastBlockFn      func(context.Context) pocktclient.Block
	getBlockAtHeight func(context.Context, int64) (pocktclient.Block, error)
	getBlockMu       sync.Mutex
	requestedHeights []int64
}

func (c *callbackLastOnlyBlockClient) LastBlock(ctx context.Context) pocktclient.Block {
	return c.lastBlockFn(ctx)
}

func (c *callbackLastOnlyBlockClient) GetBlockAtHeight(ctx context.Context, height int64) (pocktclient.Block, error) {
	c.getBlockMu.Lock()
	c.requestedHeights = append(c.requestedHeights, height)
	c.getBlockMu.Unlock()
	return c.getBlockAtHeight(ctx, height)
}

func (c *callbackLastOnlyBlockClient) requestedBlockHeights() []int64 {
	c.getBlockMu.Lock()
	defer c.getBlockMu.Unlock()
	return append([]int64(nil), c.requestedHeights...)
}

func newCallbackHeightTestCallback(blockClient pocktclient.BlockClient) *LifecycleCallback {
	callback := createTestLifecycleCallback(nil)
	callback.blockClient = blockClient
	return callback
}

func TestLifecycleCallbackCurrentHeightForTiming(t *testing.T) {
	t.Run("prefers RPC height over stale event height", func(t *testing.T) {
		blockClient := &callbackHeightBlockClient{currentHeightFn: func(context.Context) (int64, error) {
			return 120, nil
		}}
		blockClient.lastHeight.Store(100)
		callback := newCallbackHeightTestCallback(blockClient)

		require.Equal(t, int64(120), callback.currentHeightForTiming(context.Background()))
	})

	t.Run("falls back to LastBlock when CurrentHeight is unavailable", func(t *testing.T) {
		blockClient := &mockBlockClient{currentHeight: 100}
		callback := newCallbackHeightTestCallback(blockClient)

		require.Equal(t, int64(100), callback.currentHeightForTiming(context.Background()))
	})

	t.Run("falls back to LastBlock when CurrentHeight fails", func(t *testing.T) {
		blockClient := &callbackHeightBlockClient{
			currentHeightFn: func(context.Context) (int64, error) {
				return 0, errors.New("RPC unavailable")
			},
		}
		blockClient.lastHeight.Store(100)
		callback := newCallbackHeightTestCallback(blockClient)

		require.Equal(t, int64(100), callback.currentHeightForTiming(context.Background()))
	})
}

func TestLifecycleCallbackWaitForBlockUsesCurrentHeightWithSilentSubscription(t *testing.T) {
	const targetHeight = 120
	blockClient := &callbackHeightBlockClient{}
	blockClient.lastHeight.Store(100)
	var currentHeight atomic.Int64
	currentHeight.Store(100)
	blockClient.currentHeightFn = func(context.Context) (int64, error) {
		return currentHeight.Load(), nil
	}
	subscribed := make(chan context.Context, 1)
	blockEvents := make(chan *client.SimpleBlock)
	blockClient.subscribeFn = func(ctx context.Context, _ int) <-chan *client.SimpleBlock {
		subscribed <- ctx
		return blockEvents // intentionally remains open and silent
	}
	canonicalBlock := &mockBlock{height: targetHeight, hash: []byte("canonical-target-hash")}
	blockClient.getBlockAtHeight = func(_ context.Context, _ int64) (pocktclient.Block, error) {
		return canonicalBlock, nil
	}
	callback := newCallbackHeightTestCallback(blockClient)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	type result struct {
		block pocktclient.Block
		err   error
	}
	resultCh := make(chan result, 1)
	go func() {
		block, err := callback.waitForBlockWithInterval(ctx, targetHeight, time.Millisecond)
		resultCh <- result{block: block, err: err}
	}()

	var subscriptionCtx context.Context
	select {
	case subscriptionCtx = <-subscribed:
	case <-time.After(time.Second):
		t.Fatal("waitForBlock did not subscribe")
	}
	currentHeight.Store(targetHeight + 2)

	select {
	case result := <-resultCh:
		require.NoError(t, result.err)
		require.Same(t, canonicalBlock, result.block)
		require.Equal(t, []byte("canonical-target-hash"), result.block.Hash())
	case <-time.After(time.Second):
		t.Fatal("waitForBlock did not recover from the silent subscription")
	}
	require.Equal(t, []int64{targetHeight}, blockClient.requestedBlockHeights())
	select {
	case <-subscriptionCtx.Done():
	case <-time.After(time.Second):
		t.Fatal("temporary block subscription was not canceled after the wait")
	}
}

func TestLifecycleCallbackWaitForBlockReturnsImmediatelyWhenCurrentHeightReached(t *testing.T) {
	const targetHeight = 120
	blockClient := &callbackHeightBlockClient{currentHeightFn: func(context.Context) (int64, error) {
		return targetHeight + 1, nil
	}}
	blockClient.lastHeight.Store(100)
	var subscribeCalls atomic.Int64
	blockClient.subscribeFn = func(context.Context, int) <-chan *client.SimpleBlock {
		subscribeCalls.Add(1)
		return make(chan *client.SimpleBlock)
	}
	canonicalBlock := &mockBlock{height: targetHeight, hash: []byte("canonical-immediate-hash")}
	blockClient.getBlockAtHeight = func(_ context.Context, height int64) (pocktclient.Block, error) {
		if height != targetHeight {
			t.Errorf("requested block height %d, want exact target %d", height, targetHeight)
		}
		return canonicalBlock, nil
	}
	callback := newCallbackHeightTestCallback(blockClient)

	block, err := callback.waitForBlockWithInterval(context.Background(), targetHeight, time.Hour)
	require.NoError(t, err)
	require.Same(t, canonicalBlock, block)
	require.Equal(t, int64(0), subscribeCalls.Load(), "an already-reached RPC height should not subscribe")
	require.Equal(t, []int64{targetHeight}, blockClient.requestedBlockHeights())
}

func TestLifecycleCallbackWaitForBlockKeepsEventFastPath(t *testing.T) {
	const targetHeight = 120
	blockClient := &callbackHeightBlockClient{currentHeightFn: func(context.Context) (int64, error) {
		return 100, nil
	}}
	blockClient.lastHeight.Store(100)
	blockEvents := make(chan *client.SimpleBlock, 1)
	blockClient.subscribeFn = func(context.Context, int) <-chan *client.SimpleBlock {
		return blockEvents
	}
	canonicalBlock := &mockBlock{height: targetHeight, hash: []byte("canonical-event-hash")}
	blockClient.getBlockAtHeight = func(_ context.Context, _ int64) (pocktclient.Block, error) {
		return canonicalBlock, nil
	}
	callback := newCallbackHeightTestCallback(blockClient)
	blockEvents <- client.NewSimpleBlock(targetHeight, []byte("event-hash"), time.Now())

	block, err := callback.waitForBlockWithInterval(context.Background(), targetHeight, time.Hour)
	require.NoError(t, err)
	require.Same(t, canonicalBlock, block)
	require.Equal(t, []int64{targetHeight}, blockClient.requestedBlockHeights())
}

func TestLifecycleCallbackWaitForBlockClosedSubscriptionUsesCurrentHeight(t *testing.T) {
	const targetHeight = 120
	blockClient := &callbackHeightBlockClient{}
	blockClient.lastHeight.Store(100)
	var currentHeight atomic.Int64
	currentHeight.Store(100)
	blockClient.currentHeightFn = func(context.Context) (int64, error) {
		return currentHeight.Load(), nil
	}
	subscribed := make(chan context.Context, 1)
	blockClient.subscribeFn = func(ctx context.Context, _ int) <-chan *client.SimpleBlock {
		subscribed <- ctx
		closedEvents := make(chan *client.SimpleBlock)
		close(closedEvents)
		return closedEvents
	}
	canonicalBlock := &mockBlock{height: targetHeight, hash: []byte("canonical-fallback-hash")}
	blockClient.getBlockAtHeight = func(_ context.Context, _ int64) (pocktclient.Block, error) {
		return canonicalBlock, nil
	}
	callback := newCallbackHeightTestCallback(blockClient)

	type result struct {
		block pocktclient.Block
		err   error
	}
	resultCh := make(chan result, 1)
	go func() {
		block, err := callback.waitForBlockWithInterval(context.Background(), targetHeight, time.Millisecond)
		resultCh <- result{block: block, err: err}
	}()

	var subscriptionCtx context.Context
	select {
	case subscriptionCtx = <-subscribed:
	case <-time.After(time.Second):
		t.Fatal("waitForBlock did not subscribe before fallback")
	}
	select {
	case <-subscriptionCtx.Done():
	case <-time.After(time.Second):
		t.Fatal("closed subscription context was not canceled before polling")
	}
	currentHeight.Store(targetHeight)

	select {
	case result := <-resultCh:
		require.NoError(t, result.err)
		require.Same(t, canonicalBlock, result.block)
	case <-time.After(time.Second):
		t.Fatal("polling did not observe current height after subscription closure")
	}
	require.Equal(t, []int64{targetHeight}, blockClient.requestedBlockHeights())
}

func TestLifecycleCallbackWaitForBlockRetriesTransientCurrentHeightError(t *testing.T) {
	const targetHeight = 120
	blockClient := &callbackHeightBlockClient{}
	blockClient.lastHeight.Store(100)
	var heightQueries atomic.Int64
	blockClient.currentHeightFn = func(context.Context) (int64, error) {
		if heightQueries.Add(1) == 1 {
			return 0, errors.New("temporary RPC failure")
		}
		return targetHeight, nil
	}
	blockEvents := make(chan *client.SimpleBlock)
	blockClient.subscribeFn = func(context.Context, int) <-chan *client.SimpleBlock {
		return blockEvents
	}
	canonicalBlock := &mockBlock{height: targetHeight, hash: []byte("canonical-retry-hash")}
	blockClient.getBlockAtHeight = func(context.Context, int64) (pocktclient.Block, error) {
		return canonicalBlock, nil
	}
	callback := newCallbackHeightTestCallback(blockClient)

	block, err := callback.waitForBlockWithInterval(context.Background(), targetHeight, time.Millisecond)
	require.NoError(t, err)
	require.Same(t, canonicalBlock, block)
	require.GreaterOrEqual(t, heightQueries.Load(), int64(2))
	require.Equal(t, []int64{targetHeight}, blockClient.requestedBlockHeights())
}

func TestLifecycleCallbackWaitForBlockUnsupportedSubscriptionFallsBackToLastBlock(t *testing.T) {
	const targetHeight = 120
	var lastBlockCalls atomic.Int64
	blockClient := &callbackLastOnlyBlockClient{
		lastBlockFn: func(context.Context) pocktclient.Block {
			if lastBlockCalls.Add(1) == 1 {
				return &mockBlock{height: 100}
			}
			return &mockBlock{height: targetHeight}
		},
	}
	canonicalBlock := &mockBlock{height: targetHeight, hash: []byte("canonical-last-block-hash")}
	blockClient.getBlockAtHeight = func(context.Context, int64) (pocktclient.Block, error) {
		return canonicalBlock, nil
	}
	callback := newCallbackHeightTestCallback(blockClient)

	block, err := callback.waitForBlockWithInterval(context.Background(), targetHeight, time.Millisecond)
	require.NoError(t, err)
	require.Same(t, canonicalBlock, block)
	require.GreaterOrEqual(t, lastBlockCalls.Load(), int64(2))
	require.Equal(t, []int64{targetHeight}, blockClient.requestedBlockHeights())
}

func TestLifecycleCallbackWaitForBlockHonorsCancellation(t *testing.T) {
	blockClient := &callbackHeightBlockClient{currentHeightFn: func(context.Context) (int64, error) {
		return 100, nil
	}}
	blockClient.lastHeight.Store(100)
	subscribed := make(chan context.Context, 1)
	blockClient.subscribeFn = func(ctx context.Context, _ int) <-chan *client.SimpleBlock {
		subscribed <- ctx
		return make(chan *client.SimpleBlock)
	}
	callback := newCallbackHeightTestCallback(blockClient)
	ctx, cancel := context.WithCancel(context.Background())
	resultCh := make(chan error, 1)
	go func() {
		_, err := callback.waitForBlockWithInterval(ctx, 120, time.Hour)
		resultCh <- err
	}()

	var subscriptionCtx context.Context
	select {
	case subscriptionCtx = <-subscribed:
	case <-time.After(time.Second):
		t.Fatal("waitForBlock did not subscribe")
	}
	cancel()
	select {
	case err := <-resultCh:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("waitForBlock did not return after cancellation")
	}
	select {
	case <-subscriptionCtx.Done():
	case <-time.After(time.Second):
		t.Fatal("temporary subscription remained active after cancellation")
	}
}
