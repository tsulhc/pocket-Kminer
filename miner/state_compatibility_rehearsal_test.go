//go:build test

package miner

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

// State-compatibility rehearsal, miner side (plan #14, W5): the gates here
// seed state and recover it through the PRODUCTION write/read paths — the
// submission tracker, the rebroadcast store, the SMST manager, the session
// store, and the deduplicator — against an isolated Redis 8.10.2. The
// transport-level gates (relay streams/groups PEL reclaim, supplier-lease
// exclusivity) live in transport/redis/state_compatibility_rehearsal_test.go,
// beside the PG/FG verdict constants and the supplier-overlap decision rule.
//
// A gate that seeded a self-shaped value and read it back with a raw client
// would pass while the NEW binary cannot use the state. Every gate below
// writes through the production writer (or the exact legacy layout the
// production reader still accepts) and reads through the production reader
// the restart path uses, on a FRESH handle against the same server — the
// closest a test gets to a restarted binary without one.
func TestStateCompatibilityRehearsal_PGDomain_MinerState(t *testing.T) {
	rehearseMinerState(t, true)
}

func TestStateCompatibilityRehearsal_FGDomain_MinerState(t *testing.T) {
	rehearseMinerState(t, false)
}

func rehearseMinerState(t *testing.T, withCompaction bool) {
	t.Helper()
	ctx := context.Background()

	const supplier = "pokt1rehearsal_supplier"
	const session = "sess-rehearsal-1"
	const sessionEnd = int64(100)

	// Gate 1 — claim/proof/inclusion tracking: the retained submission record
	// is written by the tracker and read back by the reconciler's readers.
	tr, _ := setupSubmissionTracker(t)
	require.NoError(t, tr.TrackClaimSubmission(ctx,
		supplier, "eth", "pokt1rehearsal_app", session, 90, sessionEnd,
		"0xclaimhash", "0xtxclaim", true, "", 95, 99, 2, 200, true, "seed"))
	require.NoError(t, tr.TrackProofSubmission(ctx,
		supplier, sessionEnd, session, []byte("proof-bytes"), "0xtxproof",
		true, "", 96, 99, true, "seed"))
	record, err := tr.GetRecord(ctx, supplier, sessionEnd, session)
	require.NoError(t, err, "the reconciler's GetRecord must read the retained submission")
	require.Equal(t, "0xtxclaim", record.ClaimTxHash)
	require.Equal(t, "0xtxproof", record.ProofTxHash)
	require.Equal(t, int64(2), record.NumRelays)
	require.NoError(t, tr.UpdateProofOnChainOutcome(ctx, ProofOnChainUpdate{
		Supplier: supplier, SessionEnd: sessionEnd, SessionID: session,
		Outcome: "on_chain_confirmed", InclusionHeight: 101,
	}))
	confirmed, err := tr.GetRecord(ctx, supplier, sessionEnd, session)
	require.NoError(t, err)
	require.Equal(t, "on_chain_confirmed", confirmed.ProofOnChainOutcome)
	require.Equal(t, int64(101), confirmed.ProofInclusionHeight)
	listed, err := tr.ListRecordsForSupplier(ctx, supplier)
	require.NoError(t, err)
	require.Len(t, listed, 1, "the supplier listing must find the retained record")
	_, err = tr.GetRecord(ctx, supplier, sessionEnd, "sess-never-tracked")
	require.Error(t, err, "a missing record must error, not return an empty success")

	// Gate 2 — rebroadcast: the retained entry is Put through the store and
	// recovered through the failover path (ActiveGroups index scan + List),
	// with the production JSON entry codec on both ends.
	rbClient, _ := newTestRedis(t)
	rebroadcast := NewRebroadcastStore(rbClient, time.Hour)
	entry := rebroadcastEntry{
		MsgBytes: []byte("signed-claim-tx-bytes"), SubmitHeight: 95,
		TxHash: "0xtxclaim", OrigTxHash: "0xtxclaim", ServiceID: "eth",
	}
	payload, err := marshalRebroadcastEntry(entry)
	require.NoError(t, err)
	require.NoError(t, rebroadcast.Put(ctx, RebroadcastPhaseClaim,
		supplier, sessionEnd, session, payload))
	groups, err := rebroadcast.ActiveGroups(ctx, RebroadcastPhaseClaim)
	require.NoError(t, err, "the failover index scan must find the retained group")
	require.Contains(t, groups, RebroadcastGroup{Supplier: supplier, SessionEnd: sessionEnd})
	pending, err := rebroadcast.List(ctx, RebroadcastPhaseClaim, supplier, sessionEnd)
	require.NoError(t, err)
	got, ok := pending[session]
	require.True(t, ok, "the retained session must still be pending")
	back, err := unmarshalRebroadcastEntry(got)
	require.NoError(t, err)
	require.Equal(t, entry.MsgBytes, back.MsgBytes, "rebroadcast bytes must survive the restart")
	require.Equal(t, "0xtxclaim", back.TxHash)
	proofPayload, err := marshalRebroadcastEntry(rebroadcastEntry{
		MsgBytes: []byte("signed-proof-tx-bytes"), SubmitHeight: 96,
		TxHash: "0xtxproof", OrigTxHash: "0xtxproof", ServiceID: "eth",
	})
	require.NoError(t, err)
	require.NoError(t, rebroadcast.Put(ctx, RebroadcastPhaseProof,
		supplier, sessionEnd, session, proofPayload))
	proofPending, err := rebroadcast.List(ctx, RebroadcastPhaseProof, supplier, sessionEnd)
	require.NoError(t, err)
	require.Contains(t, proofPending, session)

	// Gate 3 — SMST: leaves go in through the manager, and a FRESH manager
	// on the same server resumes from the live root (mid-session restart),
	// then from the claimed root (post-claim restart), and proves.
	smstClient, _ := newTestRedis(t)
	newSMSTManager := func() *RedisSMSTManager {
		return NewRedisSMSTManager(zerolog.Nop(), smstClient,
			RedisSMSTManagerConfig{SupplierAddress: supplier, CacheTTL: time.Hour})
	}
	mgr := newSMSTManager()
	leafKeys := make([][]byte, 0, 8)
	for i := 0; i < 8; i++ {
		h := sha256.Sum256([]byte{byte(i), 0xA5})
		leafKeys = append(leafKeys, h[:])
		require.NoError(t, mgr.UpdateTree(ctx, session, h[:],
			[]byte{byte(i), 0x01, 0x02}, 1))
	}
	_, _, err = mgr.CheckpointLiveRoot(ctx, session)
	require.NoError(t, err, "mid-session live root must checkpoint")
	resumed := newSMSTManager()
	_, err = resumed.GetOrCreateTree(ctx, session)
	require.NoError(t, err, "a restarted miner must resume the live tree")
	// A live tree has no claimed root yet, so ProveClosest correctly
	// refuses it — proofs only exist for sealed trees (gate below).
	// Resume continuity instead: the reimported root matches the
	// checkpoint, and the tree keeps accumulating relays past it.
	checkpointed, err := mgr.GetTreeRoot(ctx, session)
	require.NoError(t, err)
	resumedRoot, err := resumed.GetTreeRoot(ctx, session)
	require.NoError(t, err)
	require.Equal(t, checkpointed, resumedRoot, "the resumed root must equal the checkpointed live root")
	extra := sha256.Sum256([]byte{0xFF, 0xA5})
	require.NoError(t, resumed.UpdateTree(ctx, session, extra[:], []byte{0xFF, 0x01, 0x02}, 1),
		"the resumed tree must accept new relays")
	_, _, err = resumed.CheckpointLiveRoot(ctx, session)
	require.NoError(t, err)
	advanced, err := resumed.GetTreeRoot(ctx, session)
	require.NoError(t, err)
	require.NotEqual(t, resumedRoot, advanced, "the resumed tree must advance past the checkpoint")
	root, err := resumed.FlushTree(ctx, session)
	require.NoError(t, err, "the resumed tree must flush a claimed root")
	require.Len(t, root, SMSTRootLen)
	claimed := newSMSTManager()
	_, err = claimed.GetOrCreateTree(ctx, session)
	require.NoError(t, err, "a restarted miner must reimport the claimed tree")
	claimedRoot, err := claimed.GetTreeRoot(ctx, session)
	require.NoError(t, err)
	require.Equal(t, root, claimedRoot, "the reimported root must equal the flushed root")
	claimedProof, err := claimed.ProveClosest(ctx, session, leafKeys[3])
	require.NoError(t, err, "the claimed tree must prove for a rebuild")
	require.NotEmpty(t, claimedProof)

	if withCompaction {
		// Gate 4 (PG only) — cold path: after compaction the nodes hash is
		// gone and a failover miner proves from the leaves blob alone.
		_, err = newSMSTManager().CompactColdTree(ctx, session)
		require.NoError(t, err, "cold compaction of the retained tree must succeed")
		failover := newSMSTManager()
		coldProof, err := failover.ProveClosest(ctx, session, leafKeys[5])
		require.NoError(t, err, "the compacted tree must prove from the leaves blob")
		require.NotEmpty(t, coldProof)
	}

	// Gate 5 — session metadata: Save through the store, read back through a
	// FRESH store (restart), plus the legacy JSON string layout the reader
	// still accepts during the rolling upgrade.
	store, storeClient := setupTestSessionStore(t)
	saveTestSession(t, store, session, SessionStateActive, 3, 300)
	restarted := NewRedisSessionStore(testLogger(), storeClient,
		SessionStoreConfig{SupplierAddress: "pokt1test", SessionTTL: time.Hour})
	snapshot, err := restarted.Get(ctx, session)
	require.NoError(t, err, "a restarted miner must read the retained session")
	require.NotNil(t, snapshot)
	require.Equal(t, SessionStateActive, snapshot.State)
	require.EqualValues(t, 3, snapshot.RelayCount)
	legacy := SessionSnapshot{
		SessionID: "sess-rehearsal-legacy", SupplierOperatorAddress: "pokt1test",
		ServiceID: "svc-test", ApplicationAddress: "pokt1app",
		SessionStartHeight: 100, SessionEndHeight: 110, State: SessionStateActive,
	}
	blob, err := json.Marshal(legacy)
	require.NoError(t, err)
	require.NoError(t, storeClient.Set(ctx, store.sessionKey("sess-rehearsal-legacy"), blob, 0).Err())
	legacyBack, err := restarted.Get(ctx, "sess-rehearsal-legacy")
	require.NoError(t, err, "the legacy string layout must still decode")
	require.NotNil(t, legacyBack)
	require.Equal(t, SessionStateActive, legacyBack.State)

	// Gate 6 — dedup: MarkProcessed through the deduplicator, IsDuplicate on a
	// FRESH deduplicator (restart) still refuses the replay, and an unknown
	// hash is not a duplicate.
	dedupClient, _ := newTestRedis(t)
	dedupCfg := DeduplicatorConfig{TTLBlocks: 10, BlockTimeSeconds: 30}
	dedup := NewRedisDeduplicator(testLogger(), dedupClient, dedupCfg)
	relayHash := sha256.Sum256([]byte("rehearsal-relay-bytes"))
	marked, err := dedup.MarkProcessed(ctx, relayHash[:], session)
	require.NoError(t, err)
	require.True(t, marked, "first processing of a hash must be admitted")
	restartedDedup := NewRedisDeduplicator(testLogger(), dedupClient, dedupCfg)
	dup, err := restartedDedup.IsDuplicate(ctx, relayHash[:], session)
	require.NoError(t, err)
	require.True(t, dup, "a restarted miner must still refuse the replay (no double counting)")
	other := sha256.Sum256([]byte("another-relay"))
	fresh, err := restartedDedup.IsDuplicate(ctx, other[:], session)
	require.NoError(t, err)
	require.False(t, fresh)
}
