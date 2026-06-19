//go:build test

package miner

import (
	"testing"
	"time"

	"github.com/alitto/pond/v2"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/pokt-network/pocket-relay-miner/logging"
)

func TestRecordTransitionQueueDepth_ReflectsWedgedSubpool(t *testing.T) {
	const sup = "pokt1qdepthtest000000000000000000000000000"

	pool := pond.NewPool(1)
	t.Cleanup(func() { pool.Stop() })

	m := &SessionLifecycleManager{
		logger:            logging.NewLoggerFromConfig(logging.DefaultConfig()),
		config:            SessionLifecycleConfig{SupplierAddress: sup},
		transitionSubpool: pool,
	}

	m.recordTransitionQueueDepth()
	require.Equal(t, float64(0), testutil.ToFloat64(sessionTransitionQueueDepth.WithLabelValues(sup)))

	release := make(chan struct{})
	started := make(chan struct{})
	pool.Submit(func() {
		close(started)
		<-release
	})
	<-started

	const queued = 5
	for i := 0; i < queued; i++ {
		pool.Submit(func() { <-release })
	}

	require.Eventually(t, func() bool { return pool.WaitingTasks() == uint64(queued) }, 2*time.Second, time.Millisecond)
	m.recordTransitionQueueDepth()
	require.Equal(t, float64(queued), testutil.ToFloat64(sessionTransitionQueueDepth.WithLabelValues(sup)))

	close(release)
	require.Eventually(t, func() bool { return pool.WaitingTasks() == 0 }, 2*time.Second, time.Millisecond)
	m.recordTransitionQueueDepth()
	require.Equal(t, float64(0), testutil.ToFloat64(sessionTransitionQueueDepth.WithLabelValues(sup)))
}
