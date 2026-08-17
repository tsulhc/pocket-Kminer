//go:build test

package miner

import "testing"

func TestIsClaimCUPRConsistent_FailsOpenUntilHeightAwareGuardIsWired(t *testing.T) {
	tests := []struct {
		name    string
		smstSum uint64
		smstCnt uint64
		cupr    uint64
	}{
		{name: "uniform CUPR", smstSum: 1783 * 6312, smstCnt: 1783, cupr: 6312},
		{name: "mixed weights", smstSum: 11190188, smstCnt: 1783, cupr: 6312},
		{name: "old tree against changed live CUPR", smstSum: 1783 * 6276, smstCnt: 1783, cupr: 6312},
		{name: "unknown CUPR", smstSum: 1783 * 6276, smstCnt: 1783, cupr: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if !isClaimCUPRConsistent(tt.smstSum, tt.smstCnt, tt.cupr) {
				t.Fatalf("legacy live-CUPR guard must fail open on poktroll v0.1.35")
			}
		})
	}
}
