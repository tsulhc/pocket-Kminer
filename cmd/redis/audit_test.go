//go:build test

package redis

import (
	"bytes"
	"context"
	"crypto/sha256"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"

	"github.com/pokt-network/pocket-relay-miner/miner"
)

// TestAuditSnapshot_ShortOnHealthyState seeds every audited family through
// the production writers and requires the SHORT verdict with exact counts.
// It runs against real Redis (fail-fast, never skip) like the W5
// rehearsal tests.
func TestAuditSnapshot_ShortOnHealthyState(t *testing.T) {
	ctx := context.Background()
	client, _ := newNamespacedDebugClient(t)
	defer func() { _ = client.Close() }()

	const supplier = "pokt1audit_supplier"
	const session = "sess-audit-1"
	const sessionEnd = int64(100)

	// Sessions via the production store.
	store := miner.NewRedisSessionStore(client.Logger, client.Client,
		miner.SessionStoreConfig{SupplierAddress: supplier, SessionTTL: time.Hour})
	require.NoError(t, store.Save(ctx, &miner.SessionSnapshot{
		SessionID: session, SupplierOperatorAddress: supplier,
		ServiceID: "eth", ApplicationAddress: "pokt1audit_app",
		SessionStartHeight: 90, SessionEndHeight: sessionEnd,
		State: miner.SessionStateActive, RelayCount: 3,
	}))

	// Submission tracking via the production tracker.
	tr := miner.NewSubmissionTracker(client.Logger, client.Client, time.Hour)
	require.NoError(t, tr.TrackClaimSubmission(ctx,
		supplier, "eth", "pokt1audit_app", session, 90, sessionEnd,
		"0xclaimhash", "0xtxclaim", true, "", 95, 99, 2, 200, true, "seed"))

	// Rebroadcast via the production store (hand-built JSON entry: the tool
	// requires JSON-valid codec output, and the full codec round-trip is
	// covered by the W5 rehearsal tests on this revision).
	rb := miner.NewRebroadcastStore(client.Client, time.Hour)
	require.NoError(t, rb.Put(ctx, miner.RebroadcastPhaseClaim,
		supplier, sessionEnd, session, []byte(`{"m":"c2lnbmVk","h":95}`)))

	// SMST via the production manager.
	mgr := miner.NewRedisSMSTManager(client.Logger, client.Client,
		miner.RedisSMSTManagerConfig{SupplierAddress: supplier, CacheTTL: time.Hour})
	leaf := sha256.Sum256([]byte{0x01, 0xA5})
	require.NoError(t, mgr.UpdateTree(ctx, session, leaf[:], []byte{0x01, 0x02}, 1))
	_, _, err := mgr.CheckpointLiveRoot(ctx, session)
	require.NoError(t, err)

	// Stream + group with one pending entry.
	stream := client.KB().StreamKey(supplier)
	_, err = client.XAdd(ctx, &redis.XAddArgs{Stream: stream, Values: map[string]interface{}{"relay": "bytes"}}).Result()
	require.NoError(t, err)
	require.NoError(t, client.XGroupCreateMkStream(ctx, stream, client.KB().ConsumerGroup(), "0").Err())
	_, err = client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group: client.KB().ConsumerGroup(), Consumer: "audit-consumer",
		Streams: []string{stream, ">"}, Count: 1, Block: 100 * time.Millisecond,
	}).Result()
	require.NoError(t, err)

	// Lease, dedup member, metering keys via raw commands in the exact
	// production shapes: the active-session set, the per-(session, supplier)
	// metadata JSON blob production GETs, and the consumed integer string
	// production reads with Int64.
	require.NoError(t, client.SetNX(ctx, client.KB().MinerClaimKey(supplier), "instance-a", 0).Err())
	require.NoError(t, client.SAdd(ctx, client.KB().MinerDedupSessionKey(session), "relayhash1").Err())
	require.NoError(t, client.SAdd(ctx, client.KB().MeterActiveSessionsKey(), session).Err())
	require.NoError(t, client.Set(ctx, client.KB().MeterMetaKey(session, supplier),
		`{"session_id":"sess-audit-1","app_address":"pokt1audit_app","service_id":"eth",`+
			`"supplier_address":"pokt1audit_supplier","session_end_height":100,`+
			`"max_stake_upokt":500000,"created_at":1,"created_with_factor":1.0,`+
			`"created_with_app_stake":1000000}`, 0).Err())
	require.NoError(t, client.Set(ctx, client.KB().MeterConsumedKey(session, supplier), "42", 0).Err())

	var buf bytes.Buffer
	require.NoError(t, runSnapshotAudit(ctx, client, "pg", &buf))
	out := buf.String()
	require.Contains(t, out, "verdict=SHORT_CUTOVER_COMPATIBLE")
	require.Contains(t, out, "family=sessions keys=3 ok=3 vanished=0 fail=0")
	require.Contains(t, out, "family=submission keys=1 ok=1 vanished=0 fail=0")
	require.Contains(t, out, "family=metering keys=3 ok=3 vanished=0 fail=0")
	require.Contains(t, out, "family=smst ")
	require.NotContains(t, out, "fail=1")
	for _, line := range strings.Split(out, "\n") {
		if strings.HasPrefix(line, "family=") {
			require.NotContains(t, line, "pokt1audit", "audit output must never carry identities")
			require.NotContains(t, line, "sess-audit", "audit output must never carry session ids")
		}
	}
}

// TestAuditSnapshot_DrainOnCorruptSubmission is the teeth test: a
// submission key the production reader cannot decode must flip the
// verdict to FULL_DRAIN_REQUIRED and fail exactly one gate.
func TestAuditSnapshot_DrainOnCorruptSubmission(t *testing.T) {
	ctx := context.Background()
	client, _ := newNamespacedDebugClient(t)
	defer func() { _ = client.Close() }()

	const supplier = "pokt1audit_supplier"
	const session = "sess-audit-1"

	require.NoError(t, client.Set(ctx, client.KB().TxTrackKey(supplier, 100, session),
		"this is not a submission record", 0).Err())

	var buf bytes.Buffer
	err := runSnapshotAudit(ctx, client, "pg", &buf)
	require.Error(t, err, "an undecodable retained record must not authorize SHORT")
	require.Contains(t, buf.String(), "verdict=FULL_DRAIN_REQUIRED")
	require.Contains(t, buf.String(), "family=submission keys=1 ok=0 vanished=0 fail=1")
}

// TestAuditSnapshot_PreservesSnapshotEvidence is the read-only regression
// test: the audit must not write, expire, refresh TTLs, or delete — even
// for corrupt state. Production GetOrCreateTree DELETES corrupt roots and
// refreshes TTLs; the audit preserves them as drain evidence instead.
// Every key's value (DUMP) must be identical after the run, no key may
// appear or vanish, and no TTL may be added, removed, or refreshed (TTL
// values only ever decrease, so after<=before for every key).
func TestAuditSnapshot_PreservesSnapshotEvidence(t *testing.T) {
	ctx := context.Background()
	client, _ := newNamespacedDebugClient(t)
	defer func() { _ = client.Close() }()

	const supplier = "pokt1audit_supplier"

	// Healthy SMST tree with a live root.
	mgr := miner.NewRedisSMSTManager(client.Logger, client.Client,
		miner.RedisSMSTManagerConfig{SupplierAddress: supplier, CacheTTL: time.Hour})
	leaf := sha256.Sum256([]byte{0x02, 0xA5})
	require.NoError(t, mgr.UpdateTree(ctx, "sess-healthy", leaf[:], []byte{0x02, 0x03}, 1))
	_, _, err := mgr.CheckpointLiveRoot(ctx, "sess-healthy")
	require.NoError(t, err)

	// Corrupt claimed root: production would delete this key on resume.
	// The audit must fail on it AND leave it byte-identical.
	require.NoError(t, client.Set(ctx,
		client.KB().SMSTRootKey(supplier, "sess-corrupt"), "too-short", 0).Err())

	// Nodes without any root: what a fresh start sees.
	leaf2 := sha256.Sum256([]byte{0x03, 0xA5})
	require.NoError(t, mgr.UpdateTree(ctx, "sess-noroots", leaf2[:], []byte{0x03, 0x04}, 1))

	snapshot := func() map[string][2]string {
		var keys []string
		var cursor uint64
		for {
			batch, next, err := client.Scan(ctx, cursor, client.KB().AllKeysPattern(), 1000).Result()
			require.NoError(t, err)
			keys = append(keys, batch...)
			if next == 0 {
				break
			}
			cursor = next
		}
		out := map[string][2]string{}
		for _, k := range keys {
			dump, err := client.Dump(ctx, k).Result()
			require.NoError(t, err)
			pttl, err := client.PTTL(ctx, k).Result()
			require.NoError(t, err)
			out[k] = [2]string{dump, pttl.String()}
		}
		return out
	}
	before := snapshot()

	var buf bytes.Buffer
	err = runSnapshotAudit(ctx, client, "pg", &buf)
	require.Error(t, err, "the corrupt root must flip the verdict to drain")
	require.Contains(t, buf.String(), "verdict=FULL_DRAIN_REQUIRED")

	after := snapshot()
	require.Equal(t, len(before), len(after), "the audit must not create or delete keys")
	for k, bv := range before {
		av, ok := after[k]
		require.True(t, ok, "key vanished during the audit")
		require.Equal(t, bv[0], av[0], "key value changed during the audit")
		// PTTL renders "-1ns" when no expiry is set; otherwise a positive
		// duration that only ticks down unless something refreshes it.
		require.Equal(t, (bv[1] == "-1ns"), (av[1] == "-1ns"),
			"TTL presence must not change during the audit")
		if bv[1] != "-1ns" {
			bd, err := time.ParseDuration(bv[1])
			require.NoError(t, err)
			ad, err := time.ParseDuration(av[1])
			require.NoError(t, err)
			require.LessOrEqual(t, ad, bd,
				"TTL must only decrease during the audit (a refresh would increase it)")
		}
	}
	got, err := client.Get(ctx, client.KB().SMSTRootKey(supplier, "sess-corrupt")).Result()
	require.NoError(t, err, "the corrupt root key must survive the audit as evidence")
	require.Equal(t, "too-short", got)
}
