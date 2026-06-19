//go:build test

package miner

import (
	"context"
	"sync"
	"testing"
	"time"

	localclient "github.com/pokt-network/pocket-relay-miner/client"
	"github.com/stretchr/testify/require"
)

func blk(height int64) *localclient.SimpleBlock {
	return localclient.NewSimpleBlock(height, nil, time.Time{})
}

func dropNewestSend(ch chan *localclient.SimpleBlock, b *localclient.SimpleBlock) {
	select {
	case ch <- b:
	default:
	}
}

func naiveBlockLoop(ctx context.Context, ch <-chan *localclient.SimpleBlock, onHeight func(int64)) {
	last := int64(0)
	for {
		select {
		case <-ctx.Done():
			return
		case b, ok := <-ch:
			if !ok {
				return
			}
			h := b.Height()
			if h <= last {
				continue
			}
			last = h
			onHeight(h)
		}
	}
}

func TestNaiveBlockLoop_StrandedOnStaleHead(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch := make(chan *localclient.SimpleBlock, 2)
	gate := make(chan struct{})
	entered := make(chan struct{}, 1)

	var mu sync.Mutex
	var processed []int64
	onHeight := func(h int64) {
		mu.Lock()
		processed = append(processed, h)
		mu.Unlock()
		select {
		case entered <- struct{}{}:
		default:
		}
		<-gate
	}

	go naiveBlockLoop(ctx, ch, onHeight)

	ch <- blk(1)
	<-entered

	for h := int64(2); h <= 100; h++ {
		dropNewestSend(ch, blk(h))
	}
	close(gate)

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(processed) == 3
	}, 2*time.Second, 5*time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, []int64{1, 2, 3}, processed)
	require.NotContains(t, processed, int64(100))
}

func TestCoalescingBlockLoop_ReachesHeadUnderSlowProcessor(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch := make(chan *localclient.SimpleBlock, 2)
	gate := make(chan struct{})
	entered := make(chan struct{}, 1)

	var mu sync.Mutex
	var processed []int64
	onHeight := func(h int64) {
		mu.Lock()
		processed = append(processed, h)
		mu.Unlock()
		select {
		case entered <- struct{}{}:
		default:
		}
		<-gate
	}

	go runCoalescingBlockLoop(ctx, ch, onHeight)

	ch <- blk(1)
	<-entered

	for h := int64(2); h <= 100; h++ {
		dropNewestSend(ch, blk(h))
		require.Eventually(t, func() bool { return len(ch) == 0 }, time.Second, time.Millisecond)
	}
	close(gate)

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(processed) > 0 && processed[len(processed)-1] == 100
	}, 2*time.Second, 5*time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	for i := 1; i < len(processed); i++ {
		require.Greater(t, processed[i], processed[i-1])
	}
	require.Less(t, len(processed), 100)
}

func TestCoalescingBlockLoop_NeverBlocksProducer(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch := make(chan *localclient.SimpleBlock, 4)
	wedge := make(chan struct{})

	var seen int64
	go runCoalescingBlockLoop(ctx, ch, func(h int64) {
		if h > seen {
			seen = h
		}
		<-wedge
	})

	const burst = 5000
	done := make(chan struct{})
	go func() {
		for h := int64(1); h <= burst; h++ {
			ch <- blk(h)
		}
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("producer blocked")
	}
}

func TestCoalescingBlockLoop_ProcessesFinalHeightOnClose(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch := make(chan *localclient.SimpleBlock, 64)

	var mu sync.Mutex
	var processed []int64
	done := make(chan struct{})
	go func() {
		runCoalescingBlockLoop(ctx, ch, func(h int64) {
			mu.Lock()
			processed = append(processed, h)
			mu.Unlock()
		})
		close(done)
	}()

	for h := int64(1); h <= 100; h++ {
		ch <- blk(h)
	}
	close(ch)

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("loop did not exit after channel close")
	}

	mu.Lock()
	defer mu.Unlock()
	require.NotEmpty(t, processed)
	require.Equal(t, int64(100), processed[len(processed)-1])
	for i := 1; i < len(processed); i++ {
		require.Greater(t, processed[i], processed[i-1])
	}
}
