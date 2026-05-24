# Architecture Direction

This fork has two architecture states that must not be confused.

## Current RC

The current release candidate keeps compatibility with the existing runtime and
configuration model:

- Relayers are stateless and can scale horizontally behind PATH or a load balancer.
- Relayers validate relays, sign responses, and publish mined relays to Redis Streams.
- Miners consume Redis Streams, update Redis-backed SMST/session state, and submit
  claims/proofs to the chain.
- Supplier ownership uses Redis leases. A healthy primary can own all suppliers;
  standby miners reclaim only missing or expired claims.
- Redis is revenue-critical in this state and must be persisted, monitored, and
  bounded. Relay streams are queues and must not grow indefinitely.

## Target Design

The target design keeps relayer availability as the highest priority while moving
the miner hot path away from shared remote SMST mutation:

- Relayers remain stateless and continue serving traffic even if miners are down.
- Relay queues are durable, bounded, and partitioned by supplier/session ownership.
- Exactly one miner writer owns a supplier/session shard at a time.
- SMST state lives in miner-local memory for hot-path mutation.
- A durable local WAL plus checkpoints protects SMST state across restarts.
- Redis remains for coordination, cache invalidation, lightweight metadata, and
  transitional compatibility, not every relay's SMST mutation.

## Non-Negotiable Invariants

- ACK a relay only after the miner has durably persisted enough state to rebuild
  the claim/proof path.
- Do not allow two miners to mutate the same supplier/session SMST concurrently.
- Keep relayer request handling independent from miner availability where possible.
- Bound queues with explicit retention, ACK/delete semantics, metrics, and alerts.
- Preserve protocol requirements: claims need the SMST root, and proofs may need
  the real relay leaf and closest proof path until the proof window is resolved.
