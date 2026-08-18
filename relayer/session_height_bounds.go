package relayer

const (
	maxPlausibleSessionLengthBlocks = int64(10_000)
	maxSessionLookbackBlocks        = int64(10_000)
	maxSessionLookaheadBlocks       = int64(10_000)
)

// sessionHeightsPlausible rejects structurally invalid or obviously unrelated
// session heights before they can drive at-height chain queries.
func sessionHeightsPlausible(startHeight, endHeight, arrivalHeight int64) bool {
	if startHeight <= 0 || endHeight <= startHeight {
		return false
	}
	if endHeight-startHeight > maxPlausibleSessionLengthBlocks {
		return false
	}
	if arrivalHeight <= 0 {
		return true
	}
	return startHeight <= arrivalHeight+maxSessionLookaheadBlocks &&
		endHeight >= arrivalHeight-maxSessionLookbackBlocks
}
