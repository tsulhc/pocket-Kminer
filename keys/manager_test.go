//go:build test

package keys

import (
	"context"
	"fmt"
	"testing"

	"github.com/cosmos/cosmos-sdk/crypto/keys/secp256k1"
	cryptotypes "github.com/cosmos/cosmos-sdk/crypto/types"
	"github.com/pokt-network/pocket-relay-miner/logging"
	"github.com/stretchr/testify/require"
)

type reloadTestProvider struct {
	name string
	keys map[string]cryptotypes.PrivKey
	err  error
}

func (p *reloadTestProvider) Name() string { return p.name }

func (p *reloadTestProvider) LoadKeys(context.Context) (map[string]cryptotypes.PrivKey, error) {
	if p.err != nil {
		return nil, p.err
	}

	keys := make(map[string]cryptotypes.PrivKey, len(p.keys))
	for addr, key := range p.keys {
		keys[addr] = key
	}
	return keys, nil
}

func (p *reloadTestProvider) SupportsHotReload() bool { return false }
func (p *reloadTestProvider) WatchForChanges(context.Context) <-chan struct{} {
	return nil
}
func (p *reloadTestProvider) Close() error { return nil }

func TestMultiProviderKeyManager_ReloadKeepsExistingKeysWhenProviderFails(t *testing.T) {
	logger := logging.NewLoggerFromConfig(logging.DefaultConfig())
	provider := &reloadTestProvider{
		name: "supplier_keys_file:test",
		keys: map[string]cryptotypes.PrivKey{
			"pokt1existing": testPrivKey(1),
		},
	}
	manager := NewMultiProviderKeyManager(logger, []KeyProvider{provider}, KeyManagerConfig{})

	var changes []string
	manager.OnKeyChange(func(operatorAddr string, added bool) {
		changes = append(changes, fmt.Sprintf("%s:%t", operatorAddr, added))
	})

	require.NoError(t, manager.Reload(context.Background()))
	require.Equal(t, []string{"pokt1existing"}, manager.ListSuppliers())
	require.Equal(t, []string{"pokt1existing:true"}, changes)

	provider.keys = nil
	provider.err = fmt.Errorf("supplier keys file test.yaml: invalid key file: 'keys' field is required")

	err := manager.Reload(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to load keys from provider supplier_keys_file:test")
	require.Equal(t, []string{"pokt1existing"}, manager.ListSuppliers(), "failed reload must not replace the active key set")
	require.Equal(t, []string{"pokt1existing:true"}, changes, "failed reload must not emit removal callbacks")

	provider.err = nil
	provider.keys = map[string]cryptotypes.PrivKey{
		"pokt1existing": testPrivKey(1),
		"pokt1new":      testPrivKey(2),
	}

	require.NoError(t, manager.Reload(context.Background()))
	require.ElementsMatch(t, []string{"pokt1existing", "pokt1new"}, manager.ListSuppliers())
	require.Equal(t, []string{"pokt1existing:true", "pokt1new:true"}, changes)
}

func TestMultiProviderKeyManager_ReloadRejectsEmptySnapshotWhenKeysExist(t *testing.T) {
	logger := logging.NewLoggerFromConfig(logging.DefaultConfig())
	provider := &reloadTestProvider{
		name: "file:test",
		keys: map[string]cryptotypes.PrivKey{
			"pokt1existing": testPrivKey(1),
		},
	}
	manager := NewMultiProviderKeyManager(logger, []KeyProvider{provider}, KeyManagerConfig{})

	var changes []string
	manager.OnKeyChange(func(operatorAddr string, added bool) {
		changes = append(changes, fmt.Sprintf("%s:%t", operatorAddr, added))
	})

	require.NoError(t, manager.Reload(context.Background()))
	require.Equal(t, []string{"pokt1existing"}, manager.ListSuppliers())
	require.Equal(t, []string{"pokt1existing:true"}, changes)

	provider.keys = nil

	err := manager.Reload(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "key reload produced empty snapshot")
	require.Equal(t, []string{"pokt1existing"}, manager.ListSuppliers(), "empty successful reload must not replace the active key set")
	require.Equal(t, []string{"pokt1existing:true"}, changes, "empty successful reload must not emit removal callbacks")
}

func TestMultiProviderKeyManager_ReloadIsAllOrNothingWithMultipleProviders(t *testing.T) {
	logger := logging.NewLoggerFromConfig(logging.DefaultConfig())
	providerA := &reloadTestProvider{
		name: "supplier_keys_file:a",
		keys: map[string]cryptotypes.PrivKey{
			"pokt1a": testPrivKey(1),
		},
	}
	providerB := &reloadTestProvider{
		name: "supplier_keys_file:b",
		keys: map[string]cryptotypes.PrivKey{
			"pokt1b": testPrivKey(2),
		},
	}
	manager := NewMultiProviderKeyManager(logger, []KeyProvider{providerA, providerB}, KeyManagerConfig{})

	var changes []string
	manager.OnKeyChange(func(operatorAddr string, added bool) {
		changes = append(changes, fmt.Sprintf("%s:%t", operatorAddr, added))
	})

	require.NoError(t, manager.Reload(context.Background()))
	require.ElementsMatch(t, []string{"pokt1a", "pokt1b"}, manager.ListSuppliers())
	require.ElementsMatch(t, []string{"pokt1a:true", "pokt1b:true"}, changes)

	providerA.keys = map[string]cryptotypes.PrivKey{
		"pokt1a-new": testPrivKey(3),
	}
	providerB.err = fmt.Errorf("temporary reload failure")

	err := manager.Reload(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to load keys from provider supplier_keys_file:b")
	require.ElementsMatch(t, []string{"pokt1a", "pokt1b"}, manager.ListSuppliers(), "failed reload must not apply partial provider snapshots")
	require.ElementsMatch(t, []string{"pokt1a:true", "pokt1b:true"}, changes, "failed reload must not emit callbacks for partial snapshots")
}

func testPrivKey(seed byte) cryptotypes.PrivKey {
	key := make([]byte, 32)
	for i := range key {
		key[i] = seed
	}
	return &secp256k1.PrivKey{Key: key}
}
