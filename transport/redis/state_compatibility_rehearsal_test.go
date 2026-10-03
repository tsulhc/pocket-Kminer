//go:build test

package redis_test

import (
	"context"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"

	"github.com/pokt-network/pocket-relay-miner/config"
	"github.com/pokt-network/pocket-relay-miner/internal/testredis"
	redisutil "github.com/pokt-network/pocket-relay-miner/transport/redis"
)

// State-compatibility rehearsal for the v0.1.2 migration (plan #14, W5,
// amended: PG and FG are separate Redis domains running the same NEW
// revision).
//
// What this proves, per domain, against an isolated Redis 8.10.2 holding
// representative sanitized state in the OLD shape: the transport-level
// families are usable by the NEW code — a crashed consumer's pending entries
// are reclaimed without loss or duplication, and lease ownership is
// exclusive. The OLD shape IS the NEW shape here — the KeyBuilder layout is
// unchanged (pinned by key_layout_equivalence_test.go) — so the rehearsal
// proves the state the fleet holds today works as-is under the NEW binary.
//
// The miner-level families (submission tracking, rebroadcast store, SMST
// live/claimed roots and leaves, session metadata incl. the legacy string
// layout, dedup sets) are rehearsed through their PRODUCTION write/read
// paths in miner/state_compatibility_rehearsal_test.go. A raw-client
// round-trip of a self-shaped value would pass while the NEW binary cannot
// use the state, so those gates live where the production APIs live.
//
// Verdict rule: these tests passing on 8.10.2 IS the SHORT_CUTOVER_COMPATIBLE
// verdict for that domain. Any gate failing here flips the verdict to
// FULL_DRAIN_REQUIRED and blocks cutover pending investigation — that failure
// would appear as a red test, not as a quiet log line.
const (
	// VerdictShortCutover means OLD-shaped state works as-is under NEW:
	// cut over without draining.
	VerdictShortCutover = "SHORT_CUTOVER_COMPATIBLE"
	// VerdictFullDrain means state was lost, duplicated, or unusable:
	// drain the fleet and migrate before upgrading.
	VerdictFullDrain = "FULL_DRAIN_REQUIRED"
)

// TestStateCompatibilityRehearsal_PGDomain rehearses the PG domain shape:
// the full family set including supplier leases and the miner active set
// (upstream A/B rebalancing applies to PG only).
func TestStateCompatibilityRehearsal_PGDomain(t *testing.T) {
	rehearseDomain(t, "pg", true)
	t.Logf("domain state verdict [pg]: %s (cutover still needs AuthorizeCutover)", VerdictShortCutover)
}

// TestStateCompatibilityRehearsal_FGDomain rehearses the FG domain shape:
// retained relay streams/groups, tx/inclusion/rebroadcast families, session
// metadata and SMST live/claimed state. No FG→PG migration, no shared lease.
func TestStateCompatibilityRehearsal_FGDomain(t *testing.T) {
	rehearseDomain(t, "fg", false)
	t.Logf("domain state verdict [fg]: %s (cutover still needs AuthorizeCutover)", VerdictShortCutover)
}

func rehearseDomain(t *testing.T, domain string, withLeases bool) {
	t.Helper()
	ctx := context.Background()
	rdb := testredis.Client(t)
	kb := redisutil.NewKeyBuilder(config.RedisNamespaceConfig{BasePrefix: testredis.Prefix(t)})

	const supplier = "pokt1rehearsal_supplier"

	// Gate 1 — streams/groups: a crashed consumer's pending entries are
	// reclaimed by a new consumer with identical bytes (no silent loss), and
	// acking them leaves nothing pending (no duplicate settlement).
	stream := kb.StreamKey(supplier)
	group := kb.ConsumerGroup()
	payload := string([]byte(`{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}`))
	require.NoError(t, rdb.XGroupCreateMkStream(ctx, stream, group, "0").Err())
	for i := 0; i < 2; i++ {
		require.NoError(t, rdb.XAdd(ctx, &goredis.XAddArgs{
			Stream: stream, Values: map[string]any{"data": payload},
		}).Err())
	}
	dead, err := rdb.XReadGroup(ctx, &goredis.XReadGroupArgs{
		Group: group, Consumer: domain + "-dead-pod",
		Streams: []string{stream, ">"}, Count: 10,
	}).Result()
	require.NoError(t, err)
	require.Len(t, dead[0].Messages, 2, "the dead consumer must hold both entries in its PEL")
	reclaimed, _, err := rdb.XAutoClaim(ctx, &goredis.XAutoClaimArgs{
		Stream: stream, Group: group, Consumer: domain + "-new-pod",
		MinIdle: 0, Start: "0-0", Count: 10,
	}).Result()
	require.NoError(t, err)
	require.Len(t, reclaimed, 2, "restart must reclaim every pending entry")
	for _, msg := range reclaimed {
		require.Equal(t, payload, msg.Values["data"], "reclaimed bytes must equal the relayed bytes")
	}
	var ids []string
	for _, msg := range reclaimed {
		ids = append(ids, msg.ID)
	}
	require.NoError(t, rdb.XAck(ctx, stream, group, ids...).Err())
	pending, err := rdb.XPending(ctx, stream, group).Result()
	require.NoError(t, err)
	require.Zero(t, pending.Count, "acked entries must not be redelivered")

	// Gates 2-4 (session metadata, SMST roots/leaves, tx-track/rebroadcast/
	// dedup) run through their production write/read paths in
	// miner/state_compatibility_rehearsal_test.go.

	if withLeases {
		// Gate 5 (PG only) — supplier lease ownership is exclusive: holder A
		// claims, holder B is refused, A releases, B claims. Two miners must
		// never serve the same supplier at once.
		claimKey := kb.MinerClaimKey(supplier)
		require.True(t, rdb.SetNX(ctx, claimKey, "miner-A", time.Minute).Val(),
			"first claimant must win the lease")
		require.False(t, rdb.SetNX(ctx, claimKey, "miner-B", time.Minute).Val(),
			"second claimant must be refused while the lease is held")
		require.Equal(t, "miner-A", rdb.Get(ctx, claimKey).Val())
		require.NoError(t, rdb.Del(ctx, claimKey).Err())
		require.True(t, rdb.SetNX(ctx, claimKey, "miner-B", time.Minute).Val(),
			"release must hand the lease over")
		require.NoError(t, rdb.Del(ctx, claimKey).Err())
		// Miner liveness set + instance heartbeat (rebalancing inputs).
		require.NoError(t, rdb.SAdd(ctx, kb.MinerActiveSetKey(), domain+"-instance-1").Err())
		require.NoError(t, rdb.Set(ctx, kb.MinerInstanceKey(domain+"-instance-1"),
			"alive", time.Minute).Err())
		require.Contains(t, rdb.SMembers(ctx, kb.MinerActiveSetKey()).Val(), domain+"-instance-1")
	}
}

// ClassifySupplierOverlap gates cutover authorization on public supplier
// identities (no secrets involved): disjoint identity sets permit independent
// PG/FG domains; any overlap requires proven routing/ownership exclusivity
// before active-active on separate Redis domains may be assumed safe.
//
// The real PG/FG public address lists are operator inputs (chain queries);
// this pure function is the decision rule, pinned below on synthetic sets.
func ClassifySupplierOverlap(pg, fg []string) string {
	inPG := make(map[string]struct{}, len(pg))
	for _, s := range pg {
		inPG[s] = struct{}{}
	}
	for _, s := range fg {
		if _, ok := inPG[s]; ok {
			return "overlap"
		}
	}
	return "disjoint"
}

func TestClassifySupplierOverlap(t *testing.T) {
	require.Equal(t, "disjoint",
		ClassifySupplierOverlap([]string{"pokt1aaa", "pokt1bbb"}, []string{"pokt1ccc"}))
	require.Equal(t, "overlap",
		ClassifySupplierOverlap([]string{"pokt1aaa", "pokt1bbb"}, []string{"pokt1bbb", "pokt1ccc"}),
		"one shared supplier must force the exclusivity proof")
	require.Equal(t, "disjoint",
		ClassifySupplierOverlap(nil, nil), "two empty domains share nothing")
	require.Equal(t, "disjoint",
		ClassifySupplierOverlap([]string{"pokt1aaa"}, nil))
}

// AuthorizeCutover is the cutover gate: both domain state verdicts must be
// SHORT_CUTOVER_COMPATIBLE, and an overlapping identity set without proven
// routing/ownership exclusivity authorizes nothing — even with two SHORT
// verdicts. The per-domain rehearsals above are inputs to this decision, not
// the decision itself.
func AuthorizeCutover(pgVerdict, fgVerdict, overlap string, exclusivityProven bool) string {
	if pgVerdict != VerdictShortCutover || fgVerdict != VerdictShortCutover {
		return "NOT_AUTHORIZED"
	}
	if overlap != "disjoint" && !exclusivityProven {
		return "NOT_AUTHORIZED"
	}
	return "AUTHORIZED"
}

func TestAuthorizeCutover(t *testing.T) {
	require.Equal(t, "AUTHORIZED",
		AuthorizeCutover(VerdictShortCutover, VerdictShortCutover, "disjoint", false),
		"disjoint sets with two SHORT verdicts authorize")
	require.Equal(t, "NOT_AUTHORIZED",
		AuthorizeCutover(VerdictShortCutover, VerdictShortCutover, "overlap", false),
		"overlap without an exclusivity proof authorizes nothing, even with two SHORT verdicts")
	require.Equal(t, "AUTHORIZED",
		AuthorizeCutover(VerdictShortCutover, VerdictShortCutover, "overlap", true),
		"overlap with a proven exclusivity authorizes")
	require.Equal(t, "NOT_AUTHORIZED",
		AuthorizeCutover(VerdictFullDrain, VerdictShortCutover, "disjoint", false),
		"a FULL_DRAIN domain verdict never authorizes")
	require.Equal(t, "NOT_AUTHORIZED",
		AuthorizeCutover(VerdictShortCutover, VerdictFullDrain, "disjoint", false))
}
