//go:build test

package relayer

import (
	"context"
	"crypto/sha256"
	"testing"

	"github.com/pokt-network/poktroll/pkg/crypto/protocol"
	servicetypes "github.com/pokt-network/poktroll/x/service/types"
	"github.com/stretchr/testify/require"
)

// TestProcessRelay_HashesTheDehydratedRelay pins the canonical-hash contract:
// the SMST leaf hash is computed over the relay with the response payload
// stripped, while the stripped response stays verifiable through its payload
// hash. Every assertion below goes red if the dehydration line moves after the
// marshal, is deleted, or if marshaling stops being deterministic.
func TestProcessRelay_HashesTheDehydratedRelay(t *testing.T) {
	req := testRelayRequest(t,
		[]byte(`{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}`), nil)
	reqBz, err := req.Marshal()
	require.NoError(t, err)
	respBody := []byte(`{"jsonrpc":"2.0","result":"0x10","id":1}`)

	// No difficulty provider: base difficulty applies, so every relay is
	// applicable and the mined message comes back. No signer, meter, Redis or
	// network is involved.
	rp := NewRelayProcessor(testLogger(), nil, nil, nil)
	msg, err := rp.ProcessRelay(context.Background(), reqBz, respBody, "pokt1supplier", "test-service", 6)
	require.NoError(t, err)
	require.NotNil(t, msg, "base difficulty applies to every relay, so a message must come back")

	// The leaf hash is the hash of the stored bytes, not of something else.
	wantLeaf := protocol.GetRelayHashFromBytes(msg.RelayBytes)
	require.Equal(t, wantLeaf[:], msg.RelayHash,
		"RelayHash must be the canonical hash of RelayBytes")

	// The stored bytes carry no payload: it was dehydrated before marshaling.
	stored := &servicetypes.Relay{}
	require.NoError(t, stored.Unmarshal(msg.RelayBytes))
	require.NotNil(t, stored.Res, "premise: the stored relay has a response")
	require.Empty(t, stored.Res.Payload,
		"the response payload must be stripped before the relay is marshaled for storage")
	wantPayloadHash := sha256.Sum256(respBody)
	require.Equal(t, wantPayloadHash[:], stored.Res.PayloadHash,
		"the stripped response stays verifiable through its payload hash")

	// Hashing with the payload still attached must give a different leaf: this
	// is the exact regression the dehydration line prevents.
	hydrated := &servicetypes.Relay{}
	require.NoError(t, hydrated.Unmarshal(msg.RelayBytes))
	hydrated.Res.Payload = respBody
	hydratedBz, err := hydrated.Marshal()
	require.NoError(t, err)
	wantHydrated := protocol.GetRelayHashFromBytes(hydratedBz)
	require.NotEqual(t, msg.RelayHash, wantHydrated[:],
		"a relay hashed with its payload must not collide with the dehydrated leaf")

	// Same input, same leaf: the serialization the hash runs over is canonical.
	again, err := rp.ProcessRelay(context.Background(), reqBz, respBody, "pokt1supplier", "test-service", 6)
	require.NoError(t, err)
	require.NotNil(t, again)
	require.Equal(t, msg.RelayHash, again.RelayHash, "the canonical hash is deterministic")
	require.Equal(t, msg.RelayBytes, again.RelayBytes, "the canonical bytes are deterministic")
}
