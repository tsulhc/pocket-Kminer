package redis

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"text/tabwriter"

	"github.com/redis/go-redis/v9"
	"github.com/spf13/cobra"

	"github.com/pokt-network/pocket-relay-miner/cache"
	"github.com/pokt-network/pocket-relay-miner/config"
	transportredis "github.com/pokt-network/pocket-relay-miner/transport/redis"
)

const bulkConfirmThreshold = 100
const bulkProgressInterval = 25

func CacheCmd() *cobra.Command {
	var (
		cacheType  string
		key        string
		keyFile    string
		invalidate bool
		listAll    bool
		all        bool
		dryRun     bool
		yes        bool
	)

	cmd := &cobra.Command{
		Use:   "cache",
		Short: "Inspect and invalidate cache entries (regenerable data only)",
		Long: `Inspect and manage regenerable cache entries in Redis.

This command ONLY operates on regenerable cache types. Session metadata,
SMST trees, relay streams, miner leader locks, metering data, and
submission tracking are NEVER touched — even with --type all.

Cache types:
  - application:     application cache entries
  - service:         service cache entries
  - supplier:        supplier state cache entries
  - shared_params:   shared on-chain params singleton
  - session_params:  session params singleton
  - proof_params:    proof params singleton
  - account:         account pubkey cache entries
  - all:             ALL regenerable cache types above

Examples:
  # Inspect a single entry
  pocket-relay-miner redis cache --type supplier --key pokt1abc...

  # List all entries of a type
  pocket-relay-miner redis cache --type supplier --list

  # Invalidate a single entry (publishes to L1 caches)
  pocket-relay-miner redis cache --type supplier --invalidate --key pokt1abc...

  # Bulk invalidate every entry of a type (cluster-safe SCAN)
  pocket-relay-miner redis cache --type supplier --invalidate --all

  # Preview bulk invalidation
  pocket-relay-miner redis cache --type supplier --invalidate --all --dry-run

  # Skip confirmation on large bulk invalidations
  pocket-relay-miner redis cache --type supplier --invalidate --all --yes

  # Invalidate ALL regenerable cache types (safe: never touches sessions/SMST/streams)
  pocket-relay-miner redis cache --type all --invalidate --all --dry-run
  pocket-relay-miner redis cache --type all --invalidate --all --yes

  # Invalidate from file
  pocket-relay-miner redis cache --type supplier --invalidate --key-file addrs.txt`,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()

			if cacheType == "all" && (key != "" || keyFile != "") {
				return fmt.Errorf("--type all cannot be used with --key or --key-file; use --list or --invalidate --all")
			}

			if cacheType == "all" && !dryRun && !yes {
				return fmt.Errorf("--type all requires --yes (or --dry-run to preview); use --type <single> for interactive confirmation")
			}

			if invalidate {
				sel := 0
				if key != "" {
					sel++
				}
				if all {
					sel++
				}
				if keyFile != "" {
					sel++
				}
				if sel == 0 {
					return fmt.Errorf("--invalidate requires exactly one of --key, --all, or --key-file")
				}
				if sel > 1 {
					return fmt.Errorf("--key, --all, and --key-file are mutually exclusive")
				}
				if dryRun && key != "" {
					return fmt.Errorf("--dry-run is only meaningful with --all or --key-file")
				}
			} else {
				if dryRun || all || keyFile != "" || yes {
					return fmt.Errorf("--all, --key-file, --dry-run, and --yes require --invalidate")
				}
			}

			client, err := CreateRedisClient(ctx)
			if err != nil {
				return err
			}
			defer func() { _ = client.Close() }()

			if invalidate {
				switch {
				case key != "":
					return invalidateCache(ctx, client, cacheType, key)
				case all:
					return invalidateAll(ctx, client, cacheType, dryRun, yes)
				case keyFile != "":
					return invalidateFromFile(ctx, client, cacheType, keyFile, dryRun)
				}
			}

			if listAll {
				return listCacheKeys(ctx, client, cacheType)
			}

			if key != "" {
				return inspectCacheKey(ctx, client, cacheType, key)
			}

			return fmt.Errorf("specify --key to inspect, --list to list all, or --invalidate with --key/--all/--key-file")
		},
	}

	cmd.Flags().StringVar(&cacheType, "type", "", "Cache type (application|service|supplier|shared_params|session_params|proof_params|account|all)")
	cmd.Flags().StringVar(&key, "key", "", "Cache key (address, service ID, etc)")
	cmd.Flags().BoolVar(&invalidate, "invalidate", false, "Invalidate the cache entry")
	cmd.Flags().BoolVar(&all, "all", false, "With --invalidate, invalidate every entry matching the type's prefix")
	cmd.Flags().StringVar(&keyFile, "key-file", "", "With --invalidate, path to file with one key per line ('#' comments allowed)")
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "With --all or --key-file, print what would be invalidated without deleting")
	cmd.Flags().BoolVar(&yes, "yes", false, "Skip confirmation prompt for large bulk invalidations")
	cmd.Flags().BoolVar(&listAll, "list", false, "List all cached entries")
	_ = cmd.MarkFlagRequired("type")

	return cmd
}

// errUnknownCacheType returns a consistent error for unknown cache types.
func errUnknownCacheType(cacheType string) error {
	return fmt.Errorf("unknown cache type: %q (valid: %s)",
		cacheType, strings.Join(transportredis.AllCacheTypes(), "|"))
}

// cacheTypesForCmd returns the cache types to operate on. When cacheType is
// "all", it returns every regenerable cache type.
func cacheTypesForCmd(cacheType string) ([]string, error) {
	if cacheType == "all" {
		return transportredis.AllCacheTypes(), nil
	}
	// Validate the single type.
	_, err := clientKB(nil).CachePattern(cacheType)
	if err != nil {
		return nil, errUnknownCacheType(cacheType)
	}
	return []string{cacheType}, nil
}

// clientKB is a helper to get a KeyBuilder from a client or a zero-value
// KeyBuilder when client is nil (to validate cache type names).
func clientKB(client *DebugRedisClient) *transportredis.KeyBuilder {
	if client != nil {
		return client.KB()
	}
	return transportredis.NewKeyBuilder(config.DefaultRedisNamespaceConfig())
}

func inspectCacheKey(ctx context.Context, client *DebugRedisClient, cacheType, key string) error {
	redisKey := client.KB().CacheKeyForType(cacheType, key)

	exists, err := client.Exists(ctx, redisKey).Result()
	if err != nil {
		return fmt.Errorf("failed to check cache existence: %w", err)
	}

	if exists == 0 {
		fmt.Printf("Cache entry not found: %s\n", redisKey)
		return nil
	}

	val, err := client.Get(ctx, redisKey).Result()
	if err != nil {
		return fmt.Errorf("failed to get cache value: %w", err)
	}

	ttl, err := client.TTL(ctx, redisKey).Result()
	if err != nil {
		return fmt.Errorf("failed to get TTL: %w", err)
	}

	fmt.Printf("Cache Entry: %s\n", redisKey)
	fmt.Printf("TTL: %v\n", ttl)
	fmt.Printf("Size: %d bytes\n", len(val))
	fmt.Printf("\nValue (first 500 chars):\n")
	if len(val) > 500 {
		fmt.Printf("%s...\n", val[:500])
	} else {
		fmt.Printf("%s\n", val)
	}

	return nil
}

func listCacheKeys(ctx context.Context, client *DebugRedisClient, cacheType string) error {
	types, err := cacheTypesForCmd(cacheType)
	if err != nil {
		return err
	}

	total := 0
	for _, ct := range types {
		info, err := client.KB().CachePattern(ct)
		if err != nil {
			return err
		}
		total += listCacheKeysForType(ctx, client, info)
	}

	if total == 0 {
		fmt.Printf("No cache entries found.\n")
	}
	return nil
}

func listCacheKeysForType(ctx context.Context, client *DebugRedisClient, info transportredis.CachePatternInfo) int {
	// Try known set first.
	if info.KnownSet != "" {
		members, err := client.SMembers(ctx, info.KnownSet).Result()
		if err == nil && len(members) > 0 {
			fmt.Printf("Known %s entries (from tracking set):\n", info.Type)
			for _, member := range members {
				fmt.Printf("  - %s\n", member)
			}
			fmt.Printf("\nTotal: %d entries\n\n", len(members))
			return len(members)
		}
	}

	keys, err := scanAllKeys(ctx, client, info.Pattern)
	if err != nil {
		fmt.Printf("Warning: scan for %s failed: %v\n", info.Type, err)
		return 0
	}

	if len(keys) == 0 {
		fmt.Printf("No %s cache entries found.\n", info.Type)
		return 0
	}

	fmt.Printf("Cache entries for type '%s':\n", info.Type)
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	_, _ = fmt.Fprintf(w, "KEY\tTTL\tSIZE\n")

	for _, k := range keys {
		ttl, _ := client.TTL(ctx, k).Result()
		size := client.StrLen(ctx, k).Val()
		_, _ = fmt.Fprintf(w, "%s\t%v\t%d bytes\n", k, ttl, size)
	}

	_ = w.Flush()
	fmt.Printf("\nTotal: %d entries\n\n", len(keys))

	return len(keys)
}

func scanAllKeys(ctx context.Context, client *DebugRedisClient, pattern string) ([]string, error) {
	var cursor uint64
	var keys []string
	for {
		scanKeys, next, err := client.Scan(ctx, cursor, pattern, 100).Result()
		if err != nil {
			return nil, fmt.Errorf("failed to scan keys with pattern %q: %w", pattern, err)
		}
		keys = append(keys, scanKeys...)
		cursor = next
		if cursor == 0 {
			break
		}
	}
	return keys, nil
}

func invalidateCache(ctx context.Context, client *DebugRedisClient, cacheType, key string) error {
	redisKey := client.KB().CacheKeyForType(cacheType, key)

	if err := client.Del(ctx, redisKey).Err(); err != nil {
		return fmt.Errorf("failed to delete cache key: %w", err)
	}

	fmt.Printf("Invalidated cache entry: %s\n", redisKey)

	channel := client.KB().EventClearAllChannel(cacheType)
	payload := fmt.Sprintf(`{"key": "%s"}`, key)

	if err := client.Publish(ctx, channel, payload).Err(); err != nil {
		fmt.Printf("Warning: failed to publish invalidation event: %v\n", err)
	} else {
		fmt.Printf("Published invalidation event to channel: %s\n", channel)
	}

	// Best-effort SREM from known set.
	if info, err := client.KB().CachePattern(cacheType); err == nil && info.KnownSet != "" {
		_ = client.SRem(ctx, info.KnownSet, key).Err()
	}

	return nil
}

func invalidateOneQuiet(ctx context.Context, client *DebugRedisClient, cacheType, key string) error {
	redisKey := client.KB().CacheKeyForType(cacheType, key)

	var deleted bool
	if cacheType == "supplier" {
		var err error
		deleted, err = atomicSupplierDel(ctx, client, redisKey)
		if err != nil {
			return fmt.Errorf("failed atomic supplier delete for %q: %w", redisKey, err)
		}
	} else {
		if err := client.Del(ctx, redisKey).Err(); err != nil {
			return fmt.Errorf("failed to delete cache key %q: %w", redisKey, err)
		}
		deleted = true
	}

	if deleted {
		if info, err := client.KB().CachePattern(cacheType); err == nil && info.KnownSet != "" {
			_ = client.SRem(ctx, info.KnownSet, key).Err()
		}
	}

	return nil
}

// atomicSupplierDel deletes a supplier cache entry only if it is genuinely
// contaminated (staked+active with empty services). Inside WATCH:
//  1. Read and JSON-deserialize the value.
//  2. If unreadable (corrupt JSON), preserve the entry — don't guess.
//  3. If readable but not contaminated, preserve the entry.
//  4. Only delete when IsContaminated() returns true inside the transaction.
//
// On WATCH conflict (TxFailedErr) the entry was modified by a running miner —
// we preserve it without retrying. A conflict means the previous classification
// is stale and aggressive deletion could remove a freshly-repaired entry.
//
// Returns (deleted=true, nil) when the entry was deleted, (deleted=false, nil)
// when it was preserved (healthy, illegible, already gone, or conflicted).
func atomicSupplierDel(ctx context.Context, client *DebugRedisClient, redisKey string) (deleted bool, retErr error) {
	var didDelete bool

	err := client.Watch(ctx, func(tx *redis.Tx) error {
		raw, getErr := tx.Get(ctx, redisKey).Result()
		if getErr == redis.Nil {
			return nil
		}
		if getErr != nil {
			return getErr
		}

		var state cache.SupplierState
		if jsonErr := json.Unmarshal([]byte(raw), &state); jsonErr != nil {
			return nil
		}

		if !state.IsContaminated() {
			return nil
		}

		_, pipeErr := tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.Del(ctx, redisKey)
			return nil
		})
		if pipeErr != nil {
			return pipeErr
		}
		didDelete = true
		return nil
	}, redisKey)

	if err == redis.TxFailedErr {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return didDelete, nil
}

func invalidateAll(ctx context.Context, client *DebugRedisClient, cacheType string, dryRun, yes bool) error {
	types, err := cacheTypesForCmd(cacheType)
	if err != nil {
		return err
	}

	grandTotal := 0
	for _, ct := range types {
		info, err := client.KB().CachePattern(ct)
		if err != nil {
			return err
		}

		redisKeys, err := scanAllKeysCluster(ctx, client, info.Pattern)
		if err != nil {
			return err
		}

		total := len(redisKeys)
		grandTotal += total

		if total == 0 {
			fmt.Printf("No %s cache entries found (pattern %q)\n", ct, info.Pattern)
			continue
		}

		if dryRun {
			fmt.Printf("[dry-run] would invalidate %d %s entries matching %q:\n", total, ct, info.Pattern)
			for _, k := range redisKeys {
				fmt.Printf("  - %s\n", k)
			}
			fmt.Printf("[dry-run] no keys were deleted\n\n")
			continue
		}

		if total > bulkConfirmThreshold && !yes {
			fmt.Printf("About to invalidate %d %s entries (pattern %q).\n", total, ct, info.Pattern)
			fmt.Printf("This publishes pub/sub invalidations and removes known-set membership.\n")
			fmt.Printf("Type 'y' to proceed (or use --yes to bypass): ")
			reader := bufio.NewReader(os.Stdin)
			resp, _ := reader.ReadString('\n')
			if strings.TrimSpace(resp) != "y" {
				fmt.Printf("Aborted. No keys were invalidated.\n")
				return nil
			}
		}

		done := 0
		for _, rk := range redisKeys {
			logicalKey := keyFromRedisKey(ct, info, rk)
			if err := invalidateOneQuiet(ctx, client, ct, logicalKey); err != nil {
				return fmt.Errorf("bulk invalidate failed at key %q (completed %d/%d): %w", rk, done, total, err)
			}
			done++
			if done%bulkProgressInterval == 0 {
				fmt.Printf("invalidated %s %d/%d...\n", ct, done, total)
			}
		}
		fmt.Printf("invalidated %s: %d entries total\n\n", ct, done)
	}

	if dryRun {
		fmt.Printf("[dry-run] would invalidate %d entries total across %d cache types\n", grandTotal, len(types))
		return nil
	}

	// Publish all-clear to every type — even when zero keys were found,
	// because orphaned L1 entries may exist. A single {} per channel at the
	// very end, after ALL deletions are complete.
	for _, ct := range types {
		channel := channelForClearAll(client, ct)
		if channel == "" {
			continue
		}
		payload := "{}"
		if err := client.Publish(ctx, channel, payload).Err(); err != nil {
			fmt.Printf("Warning: failed to publish all-clear for %s (%s): %v\n", ct, channel, err)
		} else {
			fmt.Printf("published all-clear to %s\n", channel)
		}
	}

	return nil
}

// channelForClearAll returns the invalidation channel for a cache type.
// supplier_params uses a non-standard channel pattern.
func channelForClearAll(client *DebugRedisClient, cacheType string) string {
	if cacheType == "supplier_params" {
		return client.KB().SupplierParamInvalidateChannel()
	}
	return client.KB().EventClearAllChannel(cacheType)
}

// scanAllKeysCluster uses SCAN, but when the underlying client is a
// redis.ClusterClient it iterates over every master node to ensure no
// keys are missed on other shards.
func scanAllKeysCluster(ctx context.Context, client *DebugRedisClient, pattern string) ([]string, error) {
	if cluster, ok := client.UniversalClient.(*redis.ClusterClient); ok {
		return scanAllKeysClusterNodes(ctx, cluster, pattern)
	}
	return scanAllKeys(ctx, client, pattern)
}

// scanAllKeysClusterNodes enumerates every master node in the cluster and
// runs SCAN on each one, aggregating results.
func scanAllKeysClusterNodes(ctx context.Context, cluster *redis.ClusterClient, pattern string) ([]string, error) {
	var (
		mu  sync.Mutex
		all []string
	)
	err := cluster.ForEachMaster(ctx, func(ctx context.Context, shard *redis.Client) error {
		var cursor uint64
		for {
			keys, next, err := shard.Scan(ctx, cursor, pattern, 100).Result()
			if err != nil {
				return fmt.Errorf("cluster shard SCAN failed with pattern %q: %w", pattern, err)
			}
			mu.Lock()
			all = append(all, keys...)
			mu.Unlock()
			cursor = next
			if cursor == 0 {
				break
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return all, nil
}

func invalidateFromFile(ctx context.Context, client *DebugRedisClient, cacheType, path string, dryRun bool) error {
	keys, err := readKeyFile(path)
	if err != nil {
		return err
	}

	total := len(keys)
	if total == 0 {
		fmt.Printf("key-file %q contained no keys (blank lines and '#' comments are ignored)\n", path)
		fmt.Printf("invalidated 0 entries total\n")
		return nil
	}

	if dryRun {
		fmt.Printf("[dry-run] would invalidate %d %s entries from %s:\n", total, cacheType, path)
		for _, k := range keys {
			fmt.Printf("  - %s\n", client.KB().CacheKeyForType(cacheType, k))
		}
		fmt.Printf("[dry-run] no keys were deleted\n")
		return nil
	}

	done := 0
	for _, k := range keys {
		if err := invalidateOneQuiet(ctx, client, cacheType, k); err != nil {
			return fmt.Errorf("bulk invalidate from file failed at key %q (completed %d/%d): %w", k, done, total, err)
		}
		done++
		if done%bulkProgressInterval == 0 {
			fmt.Printf("invalidated %d/%d...\n", done, total)
		}
	}
	fmt.Printf("invalidated %d entries total\n", done)

	// Publish all-clear to invalidate L1 caches.
	channel := channelForClearAll(client, cacheType)
	if channel != "" {
		_ = client.Publish(ctx, channel, "{}")
	}

	return nil
}

func readKeyFile(path string) ([]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open key-file %q: %w", path, err)
	}
	defer func() { _ = f.Close() }()

	var keys []string
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		keys = append(keys, line)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("failed to read key-file %q: %w", path, err)
	}
	return keys, nil
}

// keyFromRedisKey extracts the logical key from a full Redis key using the
// cache pattern info (prefix-based stripping).
func keyFromRedisKey(cacheType string, info transportredis.CachePatternInfo, redisKey string) string {
	switch cacheType {
	case "supplier":
		prefix := info.Pattern
		prefix = strings.TrimSuffix(prefix, ":*")
		return strings.TrimPrefix(redisKey, prefix+":")
	case "shared_params", "session_params", "proof_params":
		return cacheType
	default:
		prefix := info.Pattern
		prefix = strings.TrimSuffix(prefix, ":*")
		return strings.TrimPrefix(redisKey, prefix+":")
	}
}
