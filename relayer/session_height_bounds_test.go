//go:build test

package relayer

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSessionHeightsPlausible(t *testing.T) {
	const arrival = int64(1_000_000)
	tests := []struct {
		name                string
		start, end, arrival int64
		want                bool
	}{
		{"active", arrival - 10, arrival + 10, arrival, true},
		{"grace", arrival - 30, arrival - 5, arrival, true},
		{"zero start", 0, 20, arrival, false},
		{"reversed", 20, 10, arrival, false},
		{"too long", arrival, arrival + maxPlausibleSessionLengthBlocks + 1, arrival, false},
		{"far future", arrival + maxSessionLookaheadBlocks + 1, arrival + maxSessionLookaheadBlocks + 11, arrival, false},
		{"far past", arrival - maxSessionLookbackBlocks - 20, arrival - maxSessionLookbackBlocks - 10, arrival, false},
		{"boot sane", 100, 120, 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, sessionHeightsPlausible(tt.start, tt.end, tt.arrival))
		})
	}
}
