package miner

import "context"

// ClaimCUPRQueryClient resolves the service CUPR effective at a block height.
type ClaimCUPRQueryClient interface {
	GetServiceComputeUnitsPerRelayAtHeight(ctx context.Context, serviceID string, blockHeight int64) (uint64, error)
}

// isClaimCUPRConsistent reports whether the SMST sum matches the protocol
// invariant num_relays * CUPR. Unknown CUPR (zero) fails open.
func isClaimCUPRConsistent(smstSum, smstCount, cupr uint64) bool {
	if cupr == 0 {
		return true
	}
	return smstSum == smstCount*cupr
}

func evaluateClaimCUPRGuard(
	ctx context.Context,
	client ClaimCUPRQueryClient,
	serviceID string,
	sessionStartHeight int64,
	smstSum, smstCount uint64,
) (allowed bool, cupr uint64, err error) {
	if client == nil || sessionStartHeight <= 0 {
		return true, 0, nil
	}
	cupr, err = client.GetServiceComputeUnitsPerRelayAtHeight(ctx, serviceID, sessionStartHeight)
	if err != nil {
		return true, 0, err
	}
	return isClaimCUPRConsistent(smstSum, smstCount, cupr), cupr, nil
}
