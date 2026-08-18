package cache

import (
	"context"
	"fmt"
)

type serviceCUPRAtHeightQueryClient interface {
	GetServiceComputeUnitsPerRelayAtHeight(ctx context.Context, serviceID string, blockHeight int64) (uint64, error)
}

// GetServiceComputeUnitsPerRelayAtHeight exposes the query layer's immutable
// height-aware CUPR lookup without changing NewServiceCache or its existing HA
// wiring. Older query clients fail explicitly and the relayer falls back live.
func (c *serviceCache) GetServiceComputeUnitsPerRelayAtHeight(
	ctx context.Context,
	serviceID string,
	blockHeight int64,
) (uint64, error) {
	queryClient, ok := c.queryClient.(serviceCUPRAtHeightQueryClient)
	if !ok {
		return 0, fmt.Errorf("service query client does not support CUPR-at-height")
	}
	return queryClient.GetServiceComputeUnitsPerRelayAtHeight(ctx, serviceID, blockHeight)
}
