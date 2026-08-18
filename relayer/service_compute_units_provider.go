package relayer

import (
	"context"
	"time"

	"github.com/pokt-network/pocket-relay-miner/logging"
)

// serviceComputeUnitsLookupTimeout bounds a CUPR lookup on the relay hot path.
const serviceComputeUnitsLookupTimeout = 5 * time.Second

// ServiceCUPRAtHeightQueryClient resolves the compute_units_per_relay that was
// effective for a service at a block height.
type ServiceCUPRAtHeightQueryClient interface {
	GetServiceComputeUnitsPerRelayAtHeight(ctx context.Context, serviceID string, blockHeight int64) (uint64, error)
}

// serviceCacheComputeUnitsProvider pins CUPR to the relay's session start when
// the backing cache exposes a height-aware query, and degrades to the refreshed
// live cache when connected to a pre-v0.1.35 node or on query failure.
type serviceCacheComputeUnitsProvider struct {
	logger      logging.Logger
	cache       ServiceCache
	queryClient ServiceCUPRAtHeightQueryClient
}

// NewServiceCacheComputeUnitsProvider keeps the existing fork wiring: the cache
// itself exposes the optional at-height query through a narrow interface.
func NewServiceCacheComputeUnitsProvider(logger logging.Logger, cache ServiceCache, queryClient ServiceCUPRAtHeightQueryClient) ServiceComputeUnitsProvider {
	return &serviceCacheComputeUnitsProvider{
		logger:      logging.ForComponent(logger, logging.ComponentRelayProcessor),
		cache:       cache,
		queryClient: queryClient,
	}
}

// GetServiceComputeUnits returns CUPR at sessionStartHeight when available.
// Errors deliberately fail open to the live cache so a v0.1.34 node continues
// serving unchanged while the query layer periodically re-probes v0.1.35 support.
func (p *serviceCacheComputeUnitsProvider) GetServiceComputeUnits(
	ctx context.Context,
	serviceID string,
	sessionStartHeight int64,
) uint64 {
	if p.cache == nil {
		return 1
	}

	if sessionStartHeight > 0 && p.queryClient != nil {
		queryCtx, cancel := context.WithTimeout(ctx, serviceComputeUnitsLookupTimeout)
		computeUnits, err := p.queryClient.GetServiceComputeUnitsPerRelayAtHeight(queryCtx, serviceID, sessionStartHeight)
		cancel()
		if err == nil {
			if computeUnits == 0 {
				return 1
			}
			return computeUnits
		}

		p.logger.Debug().
			Err(err).
			Str(logging.FieldServiceID, serviceID).
			Int64("session_start_height", sessionStartHeight).
			Msg("session-start CUPR unavailable, falling back to live service cache")
	}

	return p.liveComputeUnits(ctx, serviceID)
}

func (p *serviceCacheComputeUnitsProvider) liveComputeUnits(ctx context.Context, serviceID string) uint64 {
	lookupCtx, cancel := context.WithTimeout(ctx, serviceComputeUnitsLookupTimeout)
	defer cancel()

	service, err := p.cache.Get(lookupCtx, serviceID)
	if err != nil {
		p.logger.Debug().
			Err(err).
			Str(logging.FieldServiceID, serviceID).
			Msg("service compute units cache miss, using default of 1")
		return 1
	}

	computeUnits := service.GetComputeUnitsPerRelay()
	if computeUnits == 0 {
		return 1
	}
	return computeUnits
}
