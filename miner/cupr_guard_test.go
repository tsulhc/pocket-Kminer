//go:build test

package miner

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

type cuprGuardQueryClient struct {
	cupr uint64
	err  error
}

func (c cuprGuardQueryClient) GetServiceComputeUnitsPerRelayAtHeight(context.Context, string, int64) (uint64, error) {
	return c.cupr, c.err
}

func TestIsClaimCUPRConsistent(t *testing.T) {
	require.True(t, isClaimCUPRConsistent(1783*6312, 1783, 6312))
	require.False(t, isClaimCUPRConsistent(1783*6276, 1783, 6312))
	require.True(t, isClaimCUPRConsistent(1783*6276, 1783, 0))
}

func TestEvaluateClaimCUPRGuard_UsesSessionStartHeight(t *testing.T) {
	allowed, cupr, err := evaluateClaimCUPRGuard(context.Background(), cuprGuardQueryClient{cupr: 6312}, "svc", 100, 1783*6312, 1783)
	require.NoError(t, err)
	require.True(t, allowed)
	require.Equal(t, uint64(6312), cupr)
}

func TestEvaluateClaimCUPRGuard_QueryErrorFailsOpen(t *testing.T) {
	allowed, cupr, err := evaluateClaimCUPRGuard(context.Background(), cuprGuardQueryClient{err: errors.New("node unavailable")}, "svc", 100, 1, 1)
	require.Error(t, err)
	require.True(t, allowed)
	require.Zero(t, cupr)
}
