//go:build test

package miner

import (
	"context"
	"testing"

	"github.com/hashicorp/go-version"
	"github.com/stretchr/testify/require"

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
