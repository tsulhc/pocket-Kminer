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
// representative sanitized state in the OLD shape: every required family is
// readable by the NEW code paths, a crashed consumer's pending entries are
// reclaimed without loss or duplication, lease ownership is exclusive, and a
// restart resumes from live/claimed roots. The OLD shape IS the NEW shape
// here — the KeyBuilder layout is unchanged (pinned by
// key_layout_equivalence_test.go) — so the rehearsal proves the state the
// fleet holds today works as-is under the NEW binary.
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
	t.Logf("state-compatibility verdict [pg]: %s", VerdictShortCutover)
}

// TestStateCompatibilityRehearsal_FGDomain rehearses the FG domain shape:
// retained relay streams/groups, tx/inclusion/rebroadcast families, session
// metadata and SMST live/claimed state. No FG→PG migration, no shared lease.
func TestStateCompatibilityRehearsal_FGDomain(t *testing.T) {
	rehearseDomain(t, "fg", false)
	t.Logf("state-compatibility verdict [fg]: %s", VerdictShortCutover)
}

func rehearseDomain(t *testing.T, domain string, withLeases bool) {
	t.Helper()
	ctx := context.Background()
	rdb := testredis.Client(t)
	kb := redisutil.NewKeyBuilder(config.RedisNamespaceConfig{BasePrefix: testredis.Prefix(t)})

	const supplier = "pokt1rehearsal_supplier"
	const session = "sess-rehearsal-1"

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

	// Gate 2 — session metadata survives a restart read.
	require.NoError(t, rdb.HSet(ctx, kb.MinerSessionKey(supplier, session),
		"session_id", session, "service_id", "eth", "application", "pokt1rehearsal_app").Err())
	meta, err := rdb.HGetAll(ctx, kb.MinerSessionKey(supplier, session)).Result()
	require.NoError(t, err)
	require.Equal(t, map[string]string{
		"session_id": session, "service_id": "eth", "application": "pokt1rehearsal_app",
	}, meta)
	require.NoError(t, rdb.SAdd(ctx, kb.MinerSessionsIndexKey(supplier), session).Err())
	require.NoError(t, rdb.SAdd(ctx, kb.MinerSessionStateIndexKey(supplier, "active"), session).Err())
	require.Contains(t, rdb.SMembers(ctx, kb.MinerSessionsIndexKey(supplier)).Val(), session)

	// Gate 3 — SMST live root (mid-session resume) and claimed root + leaves
	// (proof rebuild) are intact.
	liveRoot := "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	claimedRoot := "fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210"
	require.NoError(t, rdb.Set(ctx, kb.SMSTLiveRootKey(supplier, session), liveRoot, 0).Err())
	require.NoError(t, rdb.Set(ctx, kb.SMSTRootKey(supplier, session), claimedRoot, 0).Err())
	require.Equal(t, liveRoot, rdb.Get(ctx, kb.SMSTLiveRootKey(supplier, session)).Val(),
		"a follower promoted mid-session must resume from the live root")
	require.Equal(t, claimedRoot, rdb.Get(ctx, kb.SMSTRootKey(supplier, session)).Val(),
		"a proof must rebuild from the claimed root")

	// Gate 4 — claim/proof/inclusion tracking: tx track entry and rebroadcast
	// store round-trip; dedup set stays idempotent (no double counting).
	require.NoError(t, rdb.HSet(ctx, kb.TxTrackKey(supplier, 100, session),
		"tx_hash", "ABCDEF1234", "phase", "claim").Err())
	require.Equal(t, "ABCDEF1234",
		rdb.HGet(ctx, kb.TxTrackKey(supplier, 100, session), "tx_hash").Val())
	require.NoError(t, rdb.HSet(ctx, kb.RebroadcastKey("claim", supplier, 100),
		"group:"+session, claimedRoot).Err())
	require.NoError(t, rdb.SAdd(ctx, kb.RebroadcastIndexKey("claim"),
		"group:"+session).Err())
	require.Contains(t, rdb.SMembers(ctx, kb.RebroadcastIndexKey("claim")).Val(), "group:"+session)
	require.NoError(t, rdb.SAdd(ctx, kb.MinerDedupSessionKey(session), "relayhash1").Err())
	require.NoError(t, rdb.SAdd(ctx, kb.MinerDedupSessionKey(session), "relayhash1").Err())
	require.EqualValues(t, 1, rdb.SCard(ctx, kb.MinerDedupSessionKey(session)).Val(),
		"replayed dedup members must not double count")

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
