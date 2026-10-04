package redis

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/spf13/cobra"

	"github.com/pokt-network/pocket-relay-miner/miner"
	transportredis "github.com/pokt-network/pocket-relay-miner/transport/redis"
)

// AuditCmd returns the snapshot-audit command for G1 realistic-state
// compatibility evidence (plan #14, global-audit finding G1).
//
// It walks EVERY key under the configured base prefix on an ISOLATED Redis
// holding a restored production copy/snapshot and decodes each known family
// through the exact production read paths the NEW binary uses after a
// restart. It is strictly read-only: SCAN, TYPE, GET, HGETALL, SMEMBERS,
// SCARD, XINFO, XPENDING. It never writes, never expires, never deletes.
//
// Output is sanitized by construction: per-family key/ok/fail counts plus
// bounded supplier/session counts. No key names, values, or identities are
// printed, so the output is safe to paste into the public plan issue. The
// command exits non-zero unless every decoded family is clean, in which
// case the last line is verdict=SHORT_CUTOVER_COMPATIBLE.
//
// Run it once per domain against the isolated copy:
//
//	pocket-relay-miner redis audit-snapshot --redis redis://isolated-pg:6379 --domain pg
//	pocket-relay-miner redis audit-snapshot --redis redis://isolated-fg:6379 --domain fg
func AuditCmd() *cobra.Command {
	var domain string

	cmd := &cobra.Command{
		Use:   "audit-snapshot",
		Short: "Read-only compatibility audit of a restored Redis snapshot copy",
		Long: `Walk every key under the base prefix and decode each known state
family (relay streams/groups, session metadata, claim/proof submission
tracking, rebroadcast store, SMST roots, dedup sets, supplier leases,
metering sets) through the production read paths.

Strictly read-only and sanitized: output carries counts only, never key
names, values, or identities. Exits non-zero unless the verdict is
SHORT_CUTOVER_COMPATIBLE. Run once per deployment domain against an
ISOLATED copy -- never against live production Redis.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()
			client, err := CreateRedisClient(ctx)
			if err != nil {
				return err
			}
			defer func() { _ = client.Close() }()
			return runSnapshotAudit(ctx, client, domain, cmd.OutOrStdout())
		},
	}

	cmd.Flags().StringVar(&domain, "domain", "", "Deployment domain label for the report (pg or fg)")
	_ = cmd.MarkFlagRequired("domain")

	return cmd
}

// auditVerdictShort and auditVerdictDrain are the only two verdicts the
// audit can report. Any decode failure flips the verdict to drain: the
// operator then investigates whether the domain needs a full drain or the
// finding is a new substantive issue.
const (
	auditVerdictShort = "SHORT_CUTOVER_COMPATIBLE"
	auditVerdictDrain = "FULL_DRAIN_REQUIRED"
)

// auditFamily names every state family the audit decodes.
type auditFamily string

const (
	auditStreams     auditFamily = "streams"
	auditSessions    auditFamily = "sessions"
	auditSubmission  auditFamily = "submission"
	auditRebroadcast auditFamily = "rebroadcast"
	auditSMST        auditFamily = "smst"
	auditDedup       auditFamily = "dedup"
	auditLeases      auditFamily = "leases"
	auditMetering    auditFamily = "metering"
)

// auditCounters holds sanitized counts for one family: keys seen, keys
// successfully decoded through the production reader, keys that vanished
// between SCAN and read (expiry race, not a finding), and keys that
// failed to decode (the only verdict-flipping signal).
type auditCounters struct {
	keys     int64
	ok       int64
	vanished int64
	fail     int64
}

// auditReport is the sanitized per-domain result. It carries counts only.
type auditReport struct {
	domain    string
	families  map[auditFamily]*auditCounters
	suppliers int
	sessions  int
	listed    int64
	pending   int64
	entries   int64
	members   int64
	unclassed int64
	verdict   string
}

func newAuditReport(domain string) *auditReport {
	r := &auditReport{domain: domain, families: map[auditFamily]*auditCounters{}, verdict: auditVerdictShort}
	for _, f := range []auditFamily{auditStreams, auditSessions, auditSubmission, auditRebroadcast, auditSMST, auditDedup, auditLeases, auditMetering} {
		r.families[f] = &auditCounters{}
	}
	return r
}

// runSnapshotAudit executes the whole read-only walk and prints the
// sanitized report. It returns a non-nil error (non-zero exit) unless the
// verdict is SHORT_CUTOVER_COMPATIBLE.
func runSnapshotAudit(ctx context.Context, client *DebugRedisClient, domain string, w io.Writer) error {
	rep, err := auditSnapshotState(ctx, client, domain)
	if err != nil {
		return err
	}
	fmt.Fprintf(w, "snapshot_audit domain=%s suppliers=%d sessions=%d\n", rep.domain, rep.suppliers, rep.sessions) //nolint:errcheck // matches the _, _ = Fprintf report idiom used across cmd/redis; a short write here cannot change the returned verdict, which travels via err
	for _, f := range []auditFamily{auditStreams, auditSessions, auditSubmission, auditRebroadcast, auditSMST, auditDedup, auditLeases, auditMetering} {
		c := rep.families[f]
		fmt.Fprintf(w, "family=%s keys=%d ok=%d vanished=%d fail=%d\n", f, c.keys, c.ok, c.vanished, c.fail) //nolint:errcheck // report line; verdict travels via err below
	}
	fmt.Fprintf(w, "listed_submissions=%d stream_pending=%d rebroadcast_entries=%d meter_members=%d unclassified_keys=%d\n", //nolint:errcheck // report line; verdict travels via err below
		rep.listed, rep.pending, rep.entries, rep.members, rep.unclassed)
	fmt.Fprintf(w, "verdict=%s\n", rep.verdict) //nolint:errcheck // report line; verdict travels via err below
	if rep.verdict != auditVerdictShort {
		return fmt.Errorf("snapshot audit verdict: %s (see family fail counts above)", rep.verdict)
	}
	return nil
}

// auditSnapshotState performs the read-only walk. Any production-reader
// error on a key that still exists flips the domain verdict to drain.
func auditSnapshotState(ctx context.Context, client *DebugRedisClient, domain string) (*auditReport, error) {
	rep := newAuditReport(domain)
	kb := client.KB()

	keys, err := scanAllKeys(ctx, client, kb)
	if err != nil {
		return nil, err
	}

	// Group keys by family without ever printing them.
	var streams, sessions, submissions, rebGroups, smst, dedup, leases []string
	var meterKeys []string
	suppliers := map[string]struct{}{}
	sessionsSet := map[string]struct{}{}
	for _, k := range keys {
		switch fam, supplier, session := classifySnapshotKey(kb, k); fam {
		case auditStreams:
			streams = append(streams, k)
			suppliers[supplier] = struct{}{}
		case auditSessions:
			sessions = append(sessions, k)
			suppliers[supplier] = struct{}{}
			if session != "" {
				sessionsSet[session] = struct{}{}
			}
		case auditSubmission:
			submissions = append(submissions, k)
			suppliers[supplier] = struct{}{}
			sessionsSet[session] = struct{}{}
		case auditRebroadcast:
			rebGroups = append(rebGroups, k)
		case auditMetering:
			meterKeys = append(meterKeys, k)
		case auditSMST:
			smst = append(smst, k)
			suppliers[supplier] = struct{}{}
			sessionsSet[session] = struct{}{}
		case auditDedup:
			dedup = append(dedup, k)
			sessionsSet[session] = struct{}{}
		case auditLeases:
			leases = append(leases, k)
			suppliers[supplier] = struct{}{}
		default:
			rep.unclassed++
		}
	}
	rep.suppliers = len(suppliers)
	rep.sessions = len(sessionsSet)

	auditStreamFamily(ctx, client, rep, streams)
	auditSessionFamily(ctx, client, rep, sessions)
	auditSubmissionFamily(ctx, client, rep, submissions)
	auditRebroadcastFamily(ctx, client, rep, rebGroups)
	auditSMSTFamily(ctx, client, rep, smst)
	auditDedupFamily(ctx, client, rep, dedup)
	auditLeaseFamily(ctx, client, rep, leases)
	auditMeterFamily(ctx, client, rep, meterKeys)

	for _, c := range rep.families {
		if c.fail > 0 {
			rep.verdict = auditVerdictDrain
		}
	}
	return rep, nil
}

// scanAllKeys returns every key under the base prefix. Read-only.
func scanAllKeys(ctx context.Context, client *DebugRedisClient, kb *transportredis.KeyBuilder) ([]string, error) {
	var keys []string
	var cursor uint64
	for {
		batch, next, err := client.Scan(ctx, cursor, kb.AllKeysPattern(), 1000).Result()
		if err != nil {
			return nil, fmt.Errorf("scan snapshot keys: %w", err)
		}
		keys = append(keys, batch...)
		if next == 0 {
			return keys, nil
		}
		cursor = next
	}
}

// classifySnapshotKey maps a key to its audited family plus the supplier
// and session segments when the shape carries them. Shapes are validated
// segment by segment: anything that does not match a known production
// layout is unclassified (counted, never a finding — block cache, params,
// leader and cache keys legitimately live beside the audited families).
// stripAuditBase removes the namespace base prefix so shape checks below
// work under any base: production uses a single segment ("ha"), but test and
// isolated-restore namespaces carry several, and fixed whole-key positions
// would silently unclassify everything there.
func stripAuditBase(kb *transportredis.KeyBuilder, key string) (string, bool) {
	rest, ok := strings.CutPrefix(key, strings.TrimSuffix(kb.AllKeysPattern(), "*"))
	if !ok || rest == "" {
		return "", false
	}
	return rest, true
}

func classifySnapshotKey(kb *transportredis.KeyBuilder, key string) (auditFamily, string, string) {
	if supplier, ok := kb.StreamAddress(key); ok {
		return auditStreams, supplier, ""
	}
	rest, ok := stripAuditBase(kb, key)
	if !ok {
		return "", "", ""
	}
	segs := strings.Split(rest, ":")
	// All audited miner/tx families share miner:... or tx:... once the
	// base is stripped; smst lives at smst:....
	switch {
	case len(segs) == 4 && segs[0] == "miner" && segs[1] == "sessions":
		// miner:sessions:{supplier}:{session} hash; miner:sessions:{supplier}:index
		// is the supplier's session list (audited via SMembers, marked by an
		// empty session below).
		if segs[3] == "index" {
			return auditSessions, segs[2], ""
		}
		return auditSessions, segs[2], segs[3]
	case len(segs) == 5 && segs[0] == "miner" && segs[1] == "sessions" && segs[3] == "state":
		// miner:sessions:{supplier}:state:{state} per-state list, same
		// SMembers readability gate as the supplier index.
		return auditSessions, segs[2], ""
	case len(segs) == 5 && segs[0] == "tx" && segs[1] == "track":
		if _, err := strconv.ParseInt(segs[3], 10, 64); err != nil {
			return "", "", ""
		}
		return auditSubmission, segs[2], segs[4]
	case len(segs) == 5 && segs[0] == "miner" && segs[1] == "rebroadcast":
		return auditRebroadcast, "", ""
	case len(segs) == 4 && segs[0] == "miner" && segs[1] == "rebroadcast" && segs[3] == "index":
		// miner:rebroadcast:{phase}:index group list; recovery reads it via
		// ActiveGroups, so the gate below needs no per-key work.
		return auditRebroadcast, "", ""
	case len(segs) == 4 && segs[0] == "smst":
		switch segs[3] {
		case "nodes", "root", "stats", "live_root", "leaves":
			return auditSMST, segs[1], segs[2]
		}
		return "", "", ""
	case len(segs) == 4 && segs[0] == "miner" && segs[1] == "dedup" && segs[2] == "session":
		return auditDedup, "", segs[3]
	case len(segs) == 3 && segs[0] == "miner" && segs[1] == "claim":
		return auditLeases, segs[2], ""
	case segs[0] == "meter":
		// Production metering keys come in three shapes with three types:
		//   meter:active_sessions            SET read by SMembers;
		//   meter:{session}:{supplier}:meta     STRING JSON blob read by GET;
		//   meter:{session}:{supplier}:consumed STRING integer read by GET.
		// Anything else under meter: is not a state the restart path
		// reads and stays unclassified (counted, never a finding).
		switch {
		case len(segs) == 2 && segs[1] == "active_sessions":
			return auditMetering, "", ""
		case len(segs) == 4 && segs[3] == "meta":
			return auditMetering, "", ""
		case len(segs) == 4 && segs[3] == "consumed":
			return auditMetering, "", ""
		}
		return "", "", ""
	}
	return "", "", ""
}

// auditStreamFamily proves each stream's groups and pending state are
// readable through the production introspection calls.
func auditStreamFamily(ctx context.Context, client *DebugRedisClient, rep *auditReport, streams []string) {
	c := rep.families[auditStreams]
	for _, s := range streams {
		c.keys++
		groups, err := client.XInfoGroups(ctx, s).Result()
		if err != nil {
			if err == redis.Nil {
				c.vanished++
				continue
			}
			c.fail++
			continue
		}
		for _, g := range groups {
			p, err := client.XPending(ctx, s, g.Name).Result()
			if err != nil {
				c.fail++
				break
			}
			rep.pending += p.Count
		}
		c.ok++
	}
}

// auditSessionFamily decodes every session metadata key through the
// production session store. Session-list index keys carry no session id
// (empty marker from the classifier); the restart path enumerates them,
// so the honest gate is readability via SMembers. A hash key that reads
// absent raced expiry and is not a finding; any other error is.
func auditSessionFamily(ctx context.Context, client *DebugRedisClient, rep *auditReport, keys []string) {
	c := rep.families[auditSessions]
	stores := map[string]*miner.RedisSessionStore{}
	for _, k := range keys {
		c.keys++
		_, supplier, session := classifySnapshotKey(client.KB(), k)
		if session == "" {
			if _, err := client.SMembers(ctx, k).Result(); err != nil {
				c.fail++
				continue
			}
			c.ok++
			continue
		}
		st, ok := stores[supplier]
		if !ok {
			st = miner.NewRedisSessionStore(client.Logger, client.Client,
				miner.SessionStoreConfig{SupplierAddress: supplier, SessionTTL: time.Hour})
			stores[supplier] = st
		}
		snap, err := st.Get(ctx, session)
		if err != nil {
			c.fail++
			continue
		}
		if snap == nil {
			c.vanished++
			continue
		}
		c.ok++
	}
}

// auditSubmissionFamily decodes every submission-tracking record through
// the reconciler's GetRecord reader, then cross-checks the per-supplier
// listing the on-chain outcome writer uses.
func auditSubmissionFamily(ctx context.Context, client *DebugRedisClient, rep *auditReport, keys []string) {
	c := rep.families[auditSubmission]
	tr := miner.NewSubmissionTracker(client.Logger, client.Client, time.Hour)
	bySupplier := map[string][]string{}
	for _, k := range keys {
		c.keys++
		_, supplier, session := classifySnapshotKey(client.KB(), k)
		rest, ok := stripAuditBase(client.KB(), k)
		if !ok {
			c.fail++
			continue
		}
		end, err := strconv.ParseInt(strings.Split(rest, ":")[3], 10, 64)
		if err != nil {
			c.fail++
			continue
		}
		rec, err := tr.GetRecord(ctx, supplier, end, session)
		if err != nil {
			// GetRecord wraps the Redis error (%w), so errors.Is — not == —
			// detects the raced-expiry case. Anything else (notably an
			// undecodable record) fails the gate.
			if errors.Is(err, redis.Nil) {
				c.vanished++
				continue
			}
			c.fail++
			continue
		}
		if rec == nil {
			c.vanished++
			continue
		}
		c.ok++
		bySupplier[supplier] = append(bySupplier[supplier], session)
	}
	for supplier := range bySupplier {
		listed, err := tr.ListRecordsForSupplier(ctx, supplier)
		if err != nil {
			c.fail++
			continue
		}
		rep.listed += int64(len(listed))
	}
}

// auditRebroadcastFamily recovers every group through the failover path
// (ActiveGroups index scan + List) and requires each entry to be
// JSON-valid production codec output.
func auditRebroadcastFamily(ctx context.Context, client *DebugRedisClient, rep *auditReport, _ []string) {
	c := rep.families[auditRebroadcast]
	rb := miner.NewRebroadcastStore(client.Client, time.Hour)
	for _, phase := range []miner.RebroadcastPhase{miner.RebroadcastPhaseClaim, miner.RebroadcastPhaseProof} {
		groups, err := rb.ActiveGroups(ctx, phase)
		if err != nil {
			c.fail++
			continue
		}
		for _, g := range groups {
			pending, err := rb.List(ctx, phase, g.Supplier, g.SessionEnd)
			if err != nil {
				c.fail++
				continue
			}
			for _, v := range pending {
				c.keys++
				rep.entries++
				if len(v) == 0 || !json.Valid(v) {
					c.fail++
					continue
				}
				c.ok++
			}
		}
	}
}

// auditSMSTFamily proves each retained tree is resumable WITHOUT touching
// it. The production resume path (GetOrCreateTree) is deliberately NOT
// used here: it sets TTLs on creation and DELETES corrupt roots, so
// running it would mutate the snapshot under audit and destroy the very
// evidence a drain verdict must preserve. Instead every check below is a
// read (GET/HLEN/HGETALL/HEXISTS/STRLEN) decoded with the production
// codec and layout constants:
//
//   - a present root must be exactly SMSTRootLen bytes, else nothing can
//     import it;
//   - every stored node must decode with DecodeStoredNode, the same codec
//     the map store uses on read;
//   - the root must link to a stored node: the root node's digest field
//     is hex(root[:32]), so HExists proves the root references retained
//     state rather than a foreign blob;
//   - when the nodes hash is gone the cold leaves blob must be present
//     and non-empty (the compacted failover path); full cold-prove from
//     the blob is covered by the W5 rehearsal tests on this revision.
//
// Nodes without any root are what a fresh start sees (production ignores
// them and starts empty with identical behavior), so they are not a
// finding; a root with neither nodes nor blob is retained state the NEW
// binary cannot use and fails the gate.
func auditSMSTFamily(ctx context.Context, client *DebugRedisClient, rep *auditReport, keys []string) {
	c := rep.families[auditSMST]
	byPair := map[string][]string{}
	for _, k := range keys {
		_, supplier, session := classifySnapshotKey(client.KB(), k)
		byPair[supplier+"\x00"+session] = append(byPair[supplier+"\x00"+session], k)
	}
	for pair, pairKeys := range byPair {
		parts := strings.SplitN(pair, "\x00", 2)
		supplier, session := parts[0], parts[1]
		kb := client.KB()
		nodesKey := kb.SMSTNodesKey(supplier, session)
		claimed, claimedErr := client.Get(ctx, kb.SMSTRootKey(supplier, session)).Bytes()
		live, liveErr := client.Get(ctx, kb.SMSTLiveRootKey(supplier, session)).Bytes()
		if claimedErr != nil && claimedErr != redis.Nil {
			c.fail += int64(len(pairKeys))
			continue
		}
		if liveErr != nil && liveErr != redis.Nil {
			c.fail += int64(len(pairKeys))
			continue
		}
		hasClaimed := claimedErr == nil && len(claimed) > 0
		hasLive := liveErr == nil && len(live) > 0
		anchor := claimed
		if !hasClaimed {
			anchor = live
		}
		if (hasClaimed || hasLive) && len(anchor) != miner.SMSTRootLen {
			// Malformed root: production would delete this key and start
			// fresh. The audit preserves it as drain evidence instead.
			c.fail += int64(len(pairKeys))
			continue
		}
		nodeCount, err := client.HLen(ctx, nodesKey).Result()
		if err != nil {
			c.fail += int64(len(pairKeys))
			continue
		}
		if nodeCount > 0 {
			nodes, err := client.HGetAll(ctx, nodesKey).Result()
			if err != nil {
				c.fail += int64(len(pairKeys))
				continue
			}
			bad := false
			for _, v := range nodes {
				if _, err := miner.DecodeStoredNode([]byte(v)); err != nil {
					bad = true
					break
				}
			}
			if bad {
				c.fail += int64(len(pairKeys))
				continue
			}
			if hasClaimed || hasLive {
				// The map-store keys nodes by digest with the node's
				// count/sum suffix appended, so the root digest is a
				// PREFIX of its field, never the whole field: match on
				// the prefix over the nodes already fetched above (no
				// extra round trip, still strictly read-only).
				wantPrefix := hex.EncodeToString(anchor[:32])
				linked := false
				for f := range nodes {
					if strings.HasPrefix(f, wantPrefix) {
						linked = true
						break
					}
				}
				if !linked {
					c.fail += int64(len(pairKeys))
					continue
				}
			}
			c.keys += int64(len(pairKeys))
			c.ok += int64(len(pairKeys))
			continue
		}
		if hasClaimed || hasLive {
			// Compacted shape: nodes gone, the leaves blob must carry
			// the tree. STRLEN proves presence without pulling the blob.
			n, err := client.StrLen(ctx, kb.SMSTLeavesKey(supplier, session)).Result()
			if err != nil || n == 0 {
				c.fail += int64(len(pairKeys))
				continue
			}
		}
		c.keys += int64(len(pairKeys))
		c.ok += int64(len(pairKeys))
	}
}

// auditDedupFamily requires every dedup set to be a set. Members are
// opaque relay hashes with no schema to decode; presence plus type is
// the honest gate.
func auditDedupFamily(ctx context.Context, client *DebugRedisClient, rep *auditReport, keys []string) {
	c := rep.families[auditDedup]
	for _, k := range keys {
		c.keys++
		t, err := client.Type(ctx, k).Result()
		if err != nil {
			c.fail++
			continue
		}
		if t != "set" {
			c.fail++
			continue
		}
		c.ok++
	}
}

// auditLeaseFamily requires every supplier-claim key to hold a non-empty
// holder. Exclusivity across holders is an operational property of one
// point-in-time copy and is not decidable here; the per-domain OLD/NEW
// exclusion remains a manual cutover gate.
func auditLeaseFamily(ctx context.Context, client *DebugRedisClient, rep *auditReport, keys []string) {
	c := rep.families[auditLeases]
	for _, k := range keys {
		c.keys++
		v, err := client.Get(ctx, k).Result()
		if err != nil {
			if err == redis.Nil {
				c.vanished++
				continue
			}
			c.fail++
			continue
		}
		if v == "" {
			c.fail++
			continue
		}
		c.ok++
	}
}

// auditMeterFamily requires every metering key to decode through its
// production read path: the active-session set via SMembers (the O(1)
// counter source), the per-(session, supplier) metadata blob via GET plus
// JSON validity with the required fields production unmarshals, and the
// consumed counter via GET plus integer parse. A set-typed read against a
// string key (or vice versa) is WRONGTYPE and fails the gate: that is
// retained state the NEW binary cannot use.
func auditMeterFamily(ctx context.Context, client *DebugRedisClient, rep *auditReport, keys []string) {
	c := rep.families[auditMetering]
	for _, k := range keys {
		c.keys++
		rest, ok := stripAuditBase(client.KB(), k)
		if !ok {
			c.fail++
			continue
		}
		segs := strings.Split(rest, ":")
		switch {
		case len(segs) == 2 && segs[1] == "active_sessions":
			members, err := client.SMembers(ctx, k).Result()
			if err != nil {
				c.fail++
				continue
			}
			rep.members += int64(len(members))
			c.ok++
		case len(segs) == 4 && segs[3] == "meta":
			data, err := client.Get(ctx, k).Bytes()
			if err != nil {
				if err == redis.Nil {
					c.vanished++
					continue
				}
				c.fail++
				continue
			}
			var meta map[string]json.RawMessage
			if err := json.Unmarshal(data, &meta); err != nil {
				c.fail++
				continue
			}
			// The fields production requires to price a session: without
			// them the allowance is unknown and the meter refuses.
			if _, ok := meta["session_id"]; !ok {
				c.fail++
				continue
			}
			if _, ok := meta["max_stake_upokt"]; !ok {
				c.fail++
				continue
			}
			c.ok++
		case len(segs) == 4 && segs[3] == "consumed":
			raw, err := client.Get(ctx, k).Result()
			if err != nil {
				if err == redis.Nil {
					c.vanished++
					continue
				}
				c.fail++
				continue
			}
			if _, err := strconv.ParseInt(raw, 10, 64); err != nil {
				c.fail++
				continue
			}
			c.ok++
		default:
			c.fail++
		}
	}
}
