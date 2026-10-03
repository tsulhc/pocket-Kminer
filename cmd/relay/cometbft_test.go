package relay

import (
	"testing"

	sdktypes "github.com/pokt-network/shannon-sdk/types"
	"github.com/stretchr/testify/require"
)

// TestBuildCometBFTPayload_DefaultStatus proves the default CometBFT payload is a
// JSON-RPC 2.0 `status` call — CometBFT RPC is JSON-RPC over HTTP, so this is the
// natural smoke-test method (returns node/sync info).
func TestBuildCometBFTPayload_DefaultStatus(t *testing.T) {
	defer restorePayloadJSON(RelayPayloadJSON)
	RelayPayloadJSON = ""

	bz, err := buildCometBFTPayload()
	require.NoError(t, err)

	body := decodeStreamBody(t, bz)
	require.Equal(t, "2.0", body["jsonrpc"])
	require.Equal(t, "status", body["method"], "default CometBFT method must be status")
}

// TestBuildCometBFTPayload_CustomVerbatim proves a caller-supplied --payload is
// forwarded byte-for-byte (e.g. to call `health` or `abci_info` instead), with no
// map round-trip that could reorder keys or coerce numbers.
func TestBuildCometBFTPayload_CustomVerbatim(t *testing.T) {
	defer restorePayloadJSON(RelayPayloadJSON)
	RelayPayloadJSON = `{"jsonrpc":"2.0","id":9,"method":"health","params":[]}`

	bz, err := buildCometBFTPayload()
	require.NoError(t, err)

	poktReq, err := sdktypes.DeserializeHTTPRequest(bz)
	require.NoError(t, err)
	require.Equal(t, "POST", poktReq.Method)
	require.Equal(t, RelayPayloadJSON, string(poktReq.BodyBz),
		"custom CometBFT payload must be forwarded verbatim")
}
