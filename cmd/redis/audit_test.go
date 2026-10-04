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

	// Lease, dedup member, metering member via raw commands (opaque values;
	// the tool checks presence and type only).
	require.NoError(t, client.SetNX(ctx, client.KB().MinerClaimKey(supplier), "instance-a", 0).Err())
	require.NoError(t, client.SAdd(ctx, client.KB().MinerDedupSessionKey(session), "relayhash1").Err())
	require.NoError(t, client.SAdd(ctx, client.KB().MeterActiveSessionsKey(), session).Err())

	var buf bytes.Buffer
	require.NoError(t, runSnapshotAudit(ctx, client, "pg", &buf))
	out := buf.String()
	require.Contains(t, out, "verdict=SHORT_CUTOVER_COMPATIBLE")
	require.Contains(t, out, "family=sessions keys=1 ok=1 vanished=0 fail=0")
	require.Contains(t, out, "family=submission keys=1 ok=1 vanished=0 fail=0")
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
