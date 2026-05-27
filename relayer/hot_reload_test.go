//go:build test

package relayer

import (
	"testing"

	"github.com/cosmos/cosmos-sdk/crypto/keys/secp256k1"
	cryptotypes "github.com/cosmos/cosmos-sdk/crypto/types"
	"github.com/pokt-network/pocket-relay-miner/logging"
	"github.com/stretchr/testify/require"
)

func TestResponseSigner_UpdateKeys(t *testing.T) {
	logger := logging.NewLoggerFromConfig(logging.Config{Level: "error"})
	initialKey := secp256k1.GenPrivKey()
	updatedKey := secp256k1.GenPrivKey()

	signer, err := NewResponseSigner(logger, map[string]cryptotypes.PrivKey{
		"supplier-1": initialKey,
	})
	require.NoError(t, err)
	require.True(t, signer.HasSigner("supplier-1"))
	require.False(t, signer.HasSigner("supplier-2"))

	signer.UpdateKeys(map[string]cryptotypes.PrivKey{
		"supplier-2": updatedKey,
	})

	require.False(t, signer.HasSigner("supplier-1"))
	require.True(t, signer.HasSigner("supplier-2"))
	require.Equal(t, []string{"supplier-2"}, signer.GetOperatorAddresses())
}

func TestRelayValidator_UpdateAllowedSuppliers(t *testing.T) {
	logger := logging.NewLoggerFromConfig(logging.Config{Level: "error"})
	validator := NewRelayValidator(
		logger,
		&ValidatorConfig{AllowedSupplierAddresses: []string{"supplier-1"}},
		nil,
		nil,
		nil,
	).(*relayValidator)

	require.True(t, validator.isSupplierAllowed("supplier-1"))
	require.False(t, validator.isSupplierAllowed("supplier-2"))

	validator.UpdateAllowedSuppliers([]string{"supplier-2"})
	require.False(t, validator.isSupplierAllowed("supplier-1"))
	require.True(t, validator.isSupplierAllowed("supplier-2"))

	validator.UpdateAllowedSuppliers(nil)
	require.True(t, validator.isSupplierAllowed("any-supplier"))
}
