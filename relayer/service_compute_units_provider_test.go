//go:build test

package relayer

import (
	"context"
	"errors"
	"testing"

	sharedtypes "github.com/pokt-network/poktroll/x/shared/types"
	"github.com/stretchr/testify/require"

	"github.com/pokt-network/pocket-relay-miner/logging"
)

type fakeServiceCache struct {
	svc       *sharedtypes.Service
	err       error
	atHeight  uint64
	heightErr error
}

func (f *fakeServiceCache) Get(_ context.Context, _ string, _ ...bool) (*sharedtypes.Service, error) {
	return f.svc, f.err
}

func (f *fakeServiceCache) GetServiceComputeUnitsPerRelayAtHeight(_ context.Context, _ string, _ int64) (uint64, error) {
	return f.atHeight, f.heightErr
}

func culog() logging.Logger {
	return logging.NewLoggerFromConfig(logging.DefaultConfig())
}

func TestServiceCacheComputeUnitsProvider_UsesSessionStartCUPR(t *testing.T) {
	fc := &fakeServiceCache{
		svc:      &sharedtypes.Service{Id: "seda", ComputeUnitsPerRelay: 6312},
		atHeight: 6276,
	}
	p := NewServiceCacheComputeUnitsProvider(culog(), fc, fc)

	require.Equal(t, uint64(6276), p.GetServiceComputeUnits(context.Background(), "seda", 100))
}

func TestServiceCacheComputeUnitsProvider_FallsBackToLiveOnAtHeightError(t *testing.T) {
	fc := &fakeServiceCache{
		svc:       &sharedtypes.Service{Id: "seda", ComputeUnitsPerRelay: 6312},
		heightErr: errors.New("unimplemented"),
	}
	p := NewServiceCacheComputeUnitsProvider(culog(), fc, fc)

	require.Equal(t, uint64(6312), p.GetServiceComputeUnits(context.Background(), "seda", 100))
}

func TestServiceCacheComputeUnitsProvider_DefaultsToOneOnLiveError(t *testing.T) {
	fc := &fakeServiceCache{err: errors.New("service not found"), heightErr: errors.New("unimplemented")}
	p := NewServiceCacheComputeUnitsProvider(culog(), fc, fc)

	require.Equal(t, uint64(1), p.GetServiceComputeUnits(context.Background(), "unknown", 100))
}

func TestServiceCacheComputeUnitsProvider_DefaultsToOneOnZero(t *testing.T) {
	fc := &fakeServiceCache{svc: &sharedtypes.Service{Id: "seda", ComputeUnitsPerRelay: 0}, heightErr: errors.New("unimplemented")}
	p := NewServiceCacheComputeUnitsProvider(culog(), fc, fc)

	require.Equal(t, uint64(1), p.GetServiceComputeUnits(context.Background(), "seda", 100))
}
