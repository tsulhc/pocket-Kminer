//go:build test

package miner

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

func TestRelayMetricsDoNotCreateSessionLabelSeries(t *testing.T) {
	supplier := "supplier-cardinality-test"
	service := "service-cardinality-test"

	RecordRelayAddedToSMST(supplier, service, "session-one")
	RecordRelayAddedToSMST(supplier, service, "session-two")
	RecordRelayFailedSMST(supplier, service, "session-one", "transient_error")

	added := metricWithLabels(t, relaysAddedToSMST, map[string]string{
		"supplier":   supplier,
		"service_id": service,
	})
	require.Len(t, added.Label, 2)
	require.Equal(t, float64(2), added.GetCounter().GetValue())

	failed := metricWithLabels(t, relaysFailedSMST, map[string]string{
		"supplier":   supplier,
		"service_id": service,
		"reason":     "transient_error",
	})
	require.Len(t, failed.Label, 3)
	require.Equal(t, float64(1), failed.GetCounter().GetValue())

	RecordClaimLeafStats(supplier, service, "session-one", 3, 4)
	RecordClaimLeafStats(supplier, service, "session-two", 5, 6)
	claimLeaves := metricWithLabels(t, claimNumLeaves, map[string]string{
		"supplier":   supplier,
		"service_id": service,
	})
	require.Len(t, claimLeaves.Label, 2)
	require.Equal(t, float64(5), claimLeaves.GetGauge().GetValue())

	SetClaimScheduledHeight(supplier, service, "session-one", 100)
	SetProofScheduledHeight(supplier, service, "session-two", 200)
	claimHeight := metricWithLabels(t, claimScheduledHeight, map[string]string{
		"supplier":   supplier,
		"service_id": service,
	})
	require.Len(t, claimHeight.Label, 2)
	require.Equal(t, float64(100), claimHeight.GetGauge().GetValue())
	proofHeight := metricWithLabels(t, proofScheduledHeight, map[string]string{
		"supplier":   supplier,
		"service_id": service,
	})
	require.Len(t, proofHeight.Label, 2)
	require.Equal(t, float64(200), proofHeight.GetGauge().GetValue())

	require.NotPanics(t, func() {
		ClearSessionMetrics(supplier, "session-one", service)
	})
}

func TestBlockResultRetryDoesNotCreateHeightSeries(t *testing.T) {
	RecordBlockResultsRetry(1, 1)
	RecordBlockResultsRetry(2, 1)

	metric := metricWithLabels(t, blockResultsRetriesTotal, map[string]string{})
	require.Empty(t, metric.Label)
}

func TestDedupMetricsDoNotCreateSessionLabelSeries(t *testing.T) {
	dedupRedisCacheHits.WithLabelValues().Inc()
	dedupMisses.WithLabelValues().Inc()
	dedupMarked.WithLabelValues().Inc()
	dedupErrors.WithLabelValues("redis_check").Inc()

	for _, collector := range []prometheus.Collector{
		dedupRedisCacheHits,
		dedupMisses,
		dedupMarked,
	} {
		metric := metricWithLabels(t, collector, map[string]string{})
		require.Empty(t, metric.Label)
	}

	metric := metricWithLabels(t, dedupErrors, map[string]string{"operation": "redis_check"})
	require.Len(t, metric.Label, 1)
}

func metricWithLabels(t *testing.T, collector prometheus.Collector, want map[string]string) *dto.Metric {
	t.Helper()

	metrics := make(chan prometheus.Metric)
	go func() {
		collector.Collect(metrics)
		close(metrics)
	}()

	for collected := range metrics {
		metric := &dto.Metric{}
		require.NoError(t, collected.Write(metric))
		if len(metric.Label) != len(want) {
			continue
		}

		matched := true
		for _, label := range metric.Label {
			if want[label.GetName()] != label.GetValue() {
				matched = false
				break
			}
		}
		if matched {
			return metric
		}
	}

	t.Fatalf("metric labels not found: %v", want)
	return nil
}
