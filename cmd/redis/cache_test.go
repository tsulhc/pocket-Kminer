//go:build test

package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/pokt-network/pocket-relay-miner/cache"
	"github.com/pokt-network/pocket-relay-miner/config"
	"github.com/pokt-network/pocket-relay-miner/logging"
	transportredis "github.com/pokt-network/pocket-relay-miner/transport/redis"
)

// newTestCacheClient spins up a miniredis-backed DebugRedisClient.
func newTestCacheClient(t *testing.T) (*DebugRedisClient, *miniredis.Miniredis) {
	t.Helper()

	mr, err := miniredis.Run()
	require.NoError(t, err)
	t.Cleanup(mr.Close)

	ctx := context.Background()
	cli, err := transportredis.NewClient(ctx, transportredis.ClientConfig{
		URL: fmt.Sprintf("redis://%s", mr.Addr()),
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = cli.Close() })

	logger := logging.NewLoggerFromConfig(logging.Config{Level: "error", Format: "text", Async: false})
	return &DebugRedisClient{Client: cli, Logger: logger}, mr
}

// contaminatedSupplierState returns a JSON-encoded SupplierState that is
// staked+active with empty services (contaminated).
func contaminatedSupplierJSON(t *testing.T) string {
	t.Helper()
	b, err := json.Marshal(cache.SupplierState{
		Status:   cache.SupplierStatusActive,
		Staked:   true,
		Services: []string{},
	})
	require.NoError(t, err)
	return string(b)
}

// healthySupplierState returns a JSON-encoded SupplierState that is
// staked+active with non-empty services (NOT contaminated).
func healthySupplierJSON(t *testing.T) string {
	t.Helper()
	b, err := json.Marshal(cache.SupplierState{
		Status:   cache.SupplierStatusActive,
		Staked:   true,
		Services: []string{"svc-a", "svc-b"},
	})
	require.NoError(t, err)
	return string(b)
}

func seedContaminatedSupplier(t *testing.T, mr *miniredis.Miniredis, addr string) {
	t.Helper()
	payload := contaminatedSupplierJSON(t)
	require.NoError(t, mr.Set(fmt.Sprintf("ha:supplier:%s", addr), payload))
	if _, err := mr.SAdd("ha:cache:known:suppliers", addr); err != nil {
		t.Fatalf("seed known-set: %v", err)
	}
}

func seedHealthySupplier(t *testing.T, mr *miniredis.Miniredis, addr string) {
	t.Helper()
	payload := healthySupplierJSON(t)
	require.NoError(t, mr.Set(fmt.Sprintf("ha:supplier:%s", addr), payload))
	if _, err := mr.SAdd("ha:cache:known:suppliers", addr); err != nil {
		t.Fatalf("seed known-set: %v", err)
	}
}

func seedIllegibleSupplier(t *testing.T, mr *miniredis.Miniredis, addr string) {
	t.Helper()
	require.NoError(t, mr.Set(fmt.Sprintf("ha:supplier:%s", addr), "not-valid-json"))
	if _, err := mr.SAdd("ha:cache:known:suppliers", addr); err != nil {
		t.Fatalf("seed known-set: %v", err)
	}
}

// legacy seedSuppliers kept for dry-run and non-supplier tests.
func seedSuppliers(t *testing.T, mr *miniredis.Miniredis, addrs ...string) {
	t.Helper()
	for _, a := range addrs {
		require.NoError(t, mr.Set(fmt.Sprintf("ha:supplier:%s", a), "payload"))
		if _, err := mr.SAdd("ha:cache:known:suppliers", a); err != nil {
			t.Fatalf("seed known-set: %v", err)
		}
	}
}

// =========================================================================
// Dry-run tests
// =========================================================================

func TestInvalidateAll_DryRunListsWithoutDeleting(t *testing.T) {
	client, mr := newTestCacheClient(t)
	seedContaminatedSupplier(t, mr, "pokt1a")
	seedContaminatedSupplier(t, mr, "pokt1b")
	seedContaminatedSupplier(t, mr, "pokt1c")

	err := invalidateAll(context.Background(), client, "supplier", true /*dryRun*/, false)
	require.NoError(t, err)

	// Keys still present.
	assert.True(t, mr.Exists("ha:supplier:pokt1a"))
	assert.True(t, mr.Exists("ha:supplier:pokt1b"))
	assert.True(t, mr.Exists("ha:supplier:pokt1c"))

	// Known-set untouched.
	members, err := mr.SMembers("ha:cache:known:suppliers")
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"pokt1a", "pokt1b", "pokt1c"}, members)
}

// =========================================================================
// Contamination-aware delete tests
// =========================================================================

func TestInvalidateAll_RemovesContaminatedSupplier(t *testing.T) {
	client, mr := newTestCacheClient(t)
	seedContaminatedSupplier(t, mr, "pokt1contaminated")

	err := invalidateAll(context.Background(), client, "supplier", false, true /*yes*/)
	require.NoError(t, err)

	assert.False(t, mr.Exists("ha:supplier:pokt1contaminated"))
	assert.False(t, mr.Exists("ha:cache:known:suppliers"))
}

func TestInvalidateAll_PreservesHealthySupplier(t *testing.T) {
	client, mr := newTestCacheClient(t)
	seedHealthySupplier(t, mr, "pokt1healthy")

	err := invalidateAll(context.Background(), client, "supplier", false, true /*yes*/)
	require.NoError(t, err)

	// Healthy entry must survive.
	assert.True(t, mr.Exists("ha:supplier:pokt1healthy"),
		"healthy supplier must NOT be deleted")
	// Known-set still has the member.
	members, err := mr.SMembers("ha:cache:known:suppliers")
	require.NoError(t, err)
	assert.Contains(t, members, "pokt1healthy")
}

func TestInvalidateAll_PreservesIllegibleEntry(t *testing.T) {
	client, mr := newTestCacheClient(t)
	seedIllegibleSupplier(t, mr, "pokt1corrupt")

	err := invalidateAll(context.Background(), client, "supplier", false, true /*yes*/)
	require.NoError(t, err)

	// Illegible entry must survive — we don't guess.
	assert.True(t, mr.Exists("ha:supplier:pokt1corrupt"),
		"illegible supplier entry must NOT be deleted")
}

func TestInvalidateAll_MixedEntries(t *testing.T) {
	client, mr := newTestCacheClient(t)
	seedContaminatedSupplier(t, mr, "pokt1bad")
	seedHealthySupplier(t, mr, "pokt2good")
	seedIllegibleSupplier(t, mr, "pokt3corrupt")

	err := invalidateAll(context.Background(), client, "supplier", false, true /*yes*/)
	require.NoError(t, err)

	// Only the contaminated one is deleted.
	assert.False(t, mr.Exists("ha:supplier:pokt1bad"), "contaminated must be deleted")
	assert.True(t, mr.Exists("ha:supplier:pokt2good"), "healthy must survive")
	assert.True(t, mr.Exists("ha:supplier:pokt3corrupt"), "illegible must survive")
}

func TestInvalidateAll_ZeroEntriesCleanly(t *testing.T) {
	client, _ := newTestCacheClient(t)

	err := invalidateAll(context.Background(), client, "supplier", false, true)
	require.NoError(t, err)
}

// =========================================================================
// L1 all-clear publication tests
// =========================================================================

func TestInvalidateAll_DryRun_NoPubSub(t *testing.T) {
	client, mr := newTestCacheClient(t)
	seedContaminatedSupplier(t, mr, "pokt1a")

	channel := client.KB().EventClearAllChannel("supplier")
	pubsub := client.Subscribe(context.Background(), channel)
	defer func() { _ = pubsub.Close() }()

	_, err := pubsub.Receive(context.Background())
	require.NoError(t, err)

	err = invalidateAll(context.Background(), client, "supplier", true /*dryRun*/, true)
	require.NoError(t, err)

	// Dry-run must NOT publish any all-clear.
	select {
	case msg := <-pubsub.Channel():
		t.Fatalf("dry-run published unexpected message: %s", msg.Payload)
	case <-time.After(500 * time.Millisecond):
		// Expected: no message.
	}

	// Keys untouched.
	assert.True(t, mr.Exists("ha:supplier:pokt1a"))
}

func TestInvalidateAll_PublishesOneClearAll_PerChannel(t *testing.T) {
	// Bulk deletions must NOT publish per-key; only one {} per channel at end.
	client, mr := newTestCacheClient(t)
	seedContaminatedSupplier(t, mr, "pokt1a")
	seedContaminatedSupplier(t, mr, "pokt1b")

	channel := client.KB().EventClearAllChannel("supplier")
	pubsub := client.Subscribe(context.Background(), channel)
	defer func() { _ = pubsub.Close() }()

	_, err := pubsub.Receive(context.Background())
	require.NoError(t, err)

	err = invalidateAll(context.Background(), client, "supplier", false, true)
	require.NoError(t, err)

	assert.False(t, mr.Exists("ha:supplier:pokt1a"))
	assert.False(t, mr.Exists("ha:supplier:pokt1b"))

	// Must receive exactly one {} after all deletions.
	select {
	case msg := <-pubsub.Channel():
		assert.Equal(t, "{}", msg.Payload)
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for all-clear message")
	}

	// No more messages after the all-clear.
	select {
	case msg := <-pubsub.Channel():
		t.Fatalf("unexpected extra pub/sub message: %s", msg.Payload)
	case <-time.After(500 * time.Millisecond):
		// Good — only one message.
	}
}

func TestInvalidateAll_PublishesClearAll_ZeroKeys(t *testing.T) {
	client, _ := newTestCacheClient(t)

	// Subscribe using go-redis's native PubSub.
	channel := client.KB().EventClearAllChannel("supplier")
	pubsub := client.Subscribe(context.Background(), channel)
	defer func() { _ = pubsub.Close() }()

	// Wait for subscription to be active.
	_, err := pubsub.Receive(context.Background())
	require.NoError(t, err)

	errChan := make(chan error, 1)
	go func() {
		errChan <- invalidateAll(context.Background(), client, "supplier", false, true)
	}()

	// Wait for the all-clear message.
	select {
	case msg := <-pubsub.Channel():
		assert.Equal(t, "{}", msg.Payload)
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for all-clear message")
	}

	require.NoError(t, <-errChan)
}

func TestInvalidateAll_PublishesClearAll_AfterDeletion(t *testing.T) {
	client, mr := newTestCacheClient(t)
	seedContaminatedSupplier(t, mr, "pokt1a")

	channel := client.KB().EventClearAllChannel("supplier")
	pubsub := client.Subscribe(context.Background(), channel)
	defer func() { _ = pubsub.Close() }()

	_, err := pubsub.Receive(context.Background())
	require.NoError(t, err)

	errChan := make(chan error, 1)
	go func() {
		errChan <- invalidateAll(context.Background(), client, "supplier", false, true)
	}()

	select {
	case msg := <-pubsub.Channel():
		assert.Equal(t, "{}", msg.Payload)
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for all-clear message")
	}

	require.NoError(t, <-errChan)
}

// =========================================================================
// --type all tests
// =========================================================================

func TestInvalidateAll_CleanupTypeAll(t *testing.T) {
	client, mr := newTestCacheClient(t)
	seedContaminatedSupplier(t, mr, "pokt1a")

	// Seed a non-supplier cache entry.
	require.NoError(t, mr.Set("ha:cache:application:pokt1app", "app-payload"))
	_, err := mr.SAdd("ha:cache:known:applications", "pokt1app")
	require.NoError(t, err)

	err = invalidateAll(context.Background(), client, "all", false, true /*yes*/)
	require.NoError(t, err)

	// Contaminated supplier deleted.
	assert.False(t, mr.Exists("ha:supplier:pokt1a"))
	// Application entry deleted.
	assert.False(t, mr.Exists("ha:cache:application:pokt1app"))
}

// =========================================================================
// Key-file tests
// =========================================================================

func TestInvalidateFromFile_DeletesContaminatedSupplier(t *testing.T) {
	client, mr := newTestCacheClient(t)
	seedContaminatedSupplier(t, mr, "pokt1a")

	dir := t.TempDir()
	path := filepath.Join(dir, "addrs.txt")
	content := "# comment line\n" +
		"pokt1a\n"
	require.NoError(t, os.WriteFile(path, []byte(content), 0o600))

	err := invalidateFromFile(context.Background(), client, "supplier", path, false)
	require.NoError(t, err)

	assert.False(t, mr.Exists("ha:supplier:pokt1a"))
	assert.False(t, mr.Exists("ha:cache:known:suppliers"))
}

func TestInvalidateFromFile_PreservesHealthySupplier(t *testing.T) {
	client, mr := newTestCacheClient(t)
	seedHealthySupplier(t, mr, "pokt1healthy")

	dir := t.TempDir()
	path := filepath.Join(dir, "addrs.txt")
	require.NoError(t, os.WriteFile(path, []byte("pokt1healthy\n"), 0o600))

	err := invalidateFromFile(context.Background(), client, "supplier", path, false)
	require.NoError(t, err)

	assert.True(t, mr.Exists("ha:supplier:pokt1healthy"),
		"healthy supplier must NOT be deleted by key-file invalidation")
}

func TestInvalidateFromFile_DryRunDoesNotDelete(t *testing.T) {
	client, mr := newTestCacheClient(t)
	seedContaminatedSupplier(t, mr, "pokt1a")

	dir := t.TempDir()
	path := filepath.Join(dir, "addrs.txt")
	require.NoError(t, os.WriteFile(path, []byte("pokt1a\n"), 0o600))

	err := invalidateFromFile(context.Background(), client, "supplier", path, true)
	require.NoError(t, err)
	assert.True(t, mr.Exists("ha:supplier:pokt1a"))
}

// =========================================================================
// CLI flag tests
// =========================================================================

func TestCacheCmd_MutualExclusivity(t *testing.T) {
	cases := []struct {
		name string
		args []string
	}{
		{"key + all", []string{"--type", "supplier", "--invalidate", "--key", "pokt1a", "--all"}},
		{"key + key-file", []string{"--type", "supplier", "--invalidate", "--key", "pokt1a", "--key-file", "/tmp/whatever"}},
		{"all + key-file", []string{"--type", "supplier", "--invalidate", "--all", "--key-file", "/tmp/whatever"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := CacheCmd()
			c.SetArgs(tc.args)
			c.SilenceUsage = true
			c.SilenceErrors = true
			err := c.Execute()
			require.Error(t, err)
			assert.Contains(t, err.Error(), "mutually exclusive")
		})
	}
}

func TestCacheCmd_InvalidateWithoutSelector(t *testing.T) {
	c := CacheCmd()
	c.SetArgs([]string{"--type", "supplier", "--invalidate"})
	c.SilenceUsage = true
	c.SilenceErrors = true
	err := c.Execute()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "exactly one of --key")
}

func TestCacheCmd_DryRunRequiresBulkMode(t *testing.T) {
	c := CacheCmd()
	c.SetArgs([]string{"--type", "supplier", "--invalidate", "--key", "pokt1a", "--dry-run"})
	c.SilenceUsage = true
	c.SilenceErrors = true
	err := c.Execute()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "--dry-run")
}

func TestCacheCmd_BulkFlagsRequireInvalidate(t *testing.T) {
	c := CacheCmd()
	c.SetArgs([]string{"--type", "supplier", "--all"})
	c.SilenceUsage = true
	c.SilenceErrors = true
	err := c.Execute()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "--invalidate")
}

func TestCacheCmd_TypeAllWithKeyFileRejected(t *testing.T) {
	c := CacheCmd()
	c.SetArgs([]string{"--type", "all", "--invalidate", "--key-file", "/tmp/x"})
	c.SilenceUsage = true
	c.SilenceErrors = true
	err := c.Execute()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "--type all cannot be used")
}

func TestInvalidateCache_SingleKeyPreservesBehavior(t *testing.T) {
	client, mr := newTestCacheClient(t)
	// Single-key invalidation doesn't check contamination — it's a direct
	// operator command. But it uses the same invalidateOneQuiet path which
	// for supplier uses atomicSupplierDel with contamination check.
	// For this test we use a non-supplier type to verify the core path.
	require.NoError(t, mr.Set("ha:cache:application:pokt1a", "data"))
	_, err := mr.SAdd("ha:cache:known:applications", "pokt1a")
	require.NoError(t, err)

	err = invalidateCache(context.Background(), client, "application", "pokt1a")
	require.NoError(t, err)

	assert.False(t, mr.Exists("ha:cache:application:pokt1a"))
	assert.False(t, mr.Exists("ha:cache:known:applications"))
}

// =========================================================================
// Namespace tests
// =========================================================================

func TestKeyBuilder_PerFieldNormalization(t *testing.T) {
	// Create a KeyBuilder with only BasePrefix set; all other fields
	// should be individually normalized to avoid malformed keys like
	// "prod::application:x".
	nsCfg := transportredis.ClientConfig{
		Namespace: config.RedisNamespaceConfig{
			BasePrefix: "prod",
		},
	}.Namespace
	kb := transportredis.NewKeyBuilder(nsCfg)
	key := kb.CacheKey("application", "pokt1abc")
	// Must NOT contain double separators.
	assert.NotContains(t, key, "::", "normalized key must not have double separators")
	// Must use prod prefix.
	assert.Contains(t, key, "prod:", "key must use configured base prefix")
	assert.Equal(t, "prod:cache:application:pokt1abc", key)
}

// =========================================================================
// Supplier param cache tests
// =========================================================================

func TestInvalidateAll_SupplierParamsChannelNonStandard(t *testing.T) {
	client, _ := newTestCacheClient(t)
	channel := channelForClearAll(client, "supplier_params")
	// Supplier params uses :cache:invalidate:supplier_params.
	assert.Contains(t, channel, ":cache:invalidate:supplier_params")
	assert.NotContains(t, channel, ":cache:supplier_params:")
}

func TestInvalidateAll_CleanupSupplierParams(t *testing.T) {
	client, mr := newTestCacheClient(t)
	// Matches the key generated by CacheKeys.SupplierParams().
	require.NoError(t, mr.Set("ha:cache:params:supplier", "params-payload"))

	err := invalidateAll(context.Background(), client, "supplier_params", false, true)
	require.NoError(t, err)

	assert.False(t, mr.Exists("ha:cache:params:supplier"))
}

// =========================================================================
// Lock protection tests
// =========================================================================

func TestInvalidateAll_TypeAll_PreservesLockKeys(t *testing.T) {
	client, mr := newTestCacheClient(t)
	// Populate a lock key (should NOT be matched by cache patterns).
	require.NoError(t, mr.Set("ha:cache:lock:application:pokt1a", "locked"))
	// Populate a cache key (SHOULD be matched).
	require.NoError(t, mr.Set("ha:cache:application:pokt1b", "cached"))

	err := invalidateAll(context.Background(), client, "all", false, true)
	require.NoError(t, err)

	// Lock key must survive — it's not matched by CachePattern.
	assert.True(t, mr.Exists("ha:cache:lock:application:pokt1a"),
		"lock keys must NOT be matched by cache patterns")
	// Cache key must be gone.
	assert.False(t, mr.Exists("ha:cache:application:pokt1b"))
}
