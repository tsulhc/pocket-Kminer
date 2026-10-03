package relay

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestAssignSuppliersToPool_MoreWorkersThanSuppliers verifies that when there
// are more workers than suppliers, the pool matches concurrency and suppliers
// are handed out round-robin (so the first suppliers absorb the remainder).
func TestAssignSuppliersToPool_MoreWorkersThanSuppliers(t *testing.T) {
	suppliers := []string{"pokt1a", "pokt1b", "pokt1c"}

	assigned := assignSuppliersToPool(suppliers, 10)

	// poolSize = max(concurrency=10, len(suppliers)=3) = 10.
	require.Len(t, assigned, 10)

	// Exact round-robin order index-by-index (suppliers[i%3]).
	require.Equal(t, []string{
		"pokt1a", "pokt1b", "pokt1c",
		"pokt1a", "pokt1b", "pokt1c",
		"pokt1a", "pokt1b", "pokt1c",
		"pokt1a",
	}, assigned)

	// Distribution: a=4 (i=0,3,6,9), b=3 (i=1,4,7), c=3 (i=2,5,8).
	counts := map[string]int{}
	for _, s := range assigned {
		counts[s]++
	}
	require.Equal(t, 4, counts["pokt1a"])
	require.Equal(t, 3, counts["pokt1b"])
	require.Equal(t, 3, counts["pokt1c"])
}

// TestAssignSuppliersToPool_MoreSuppliersThanWorkers verifies that when there
// are more suppliers than workers, the pool grows to len(suppliers) so every
// supplier gets exactly one connection (order preserved).
func TestAssignSuppliersToPool_MoreSuppliersThanWorkers(t *testing.T) {
	suppliers := []string{
		"pokt1-00", "pokt1-01", "pokt1-02", "pokt1-03", "pokt1-04",
		"pokt1-05", "pokt1-06", "pokt1-07", "pokt1-08", "pokt1-09",
		"pokt1-10", "pokt1-11", "pokt1-12", "pokt1-13", "pokt1-14",
	}

	assigned := assignSuppliersToPool(suppliers, 5)

	// poolSize = max(concurrency=5, len(suppliers)=15) = 15.
	require.Len(t, assigned, 15)

	// Order preserved and every supplier present exactly once.
	require.Equal(t, suppliers, assigned)

	counts := map[string]int{}
	for _, s := range assigned {
		counts[s]++
	}
	for _, s := range suppliers {
		require.Equal(t, 1, counts[s], "supplier %s should appear exactly once", s)
	}
}

// TestAssignSuppliersToPool_SingleSupplier verifies the fixed-supplier case
// (no --all-suppliers): every pooled connection targets the same supplier.
func TestAssignSuppliersToPool_SingleSupplier(t *testing.T) {
	assigned := assignSuppliersToPool([]string{"pokt1only"}, 4)

	// poolSize = max(concurrency=4, len(suppliers)=1) = 4.
	require.Equal(t, []string{"pokt1only", "pokt1only", "pokt1only", "pokt1only"}, assigned)
}

// TestAssignSuppliersToPool_Empty verifies that an empty supplier list yields
// nil; the caller validates non-emptiness before building the pool.
func TestAssignSuppliersToPool_Empty(t *testing.T) {
	require.Nil(t, assignSuppliersToPool(nil, 4))
	require.Nil(t, assignSuppliersToPool([]string{}, 4))
}

// TestAssignSuppliersToPool_ZeroConcurrency verifies the pool never collapses
// below the supplier count: with concurrency 0, poolSize falls back to
// len(suppliers) so each supplier still gets one connection.
func TestAssignSuppliersToPool_ZeroConcurrency(t *testing.T) {
	suppliers := []string{"pokt1a", "pokt1b", "pokt1c"}

	assigned := assignSuppliersToPool(suppliers, 0)

	// poolSize = max(concurrency=0, len(suppliers)=3) = 3.
	require.Equal(t, suppliers, assigned)
}
