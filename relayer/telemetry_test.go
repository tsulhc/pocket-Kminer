package relayer

import (
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"strings"
	"testing"

	servicetypes "github.com/pokt-network/poktroll/x/service/types"
	sdktypes "github.com/pokt-network/shannon-sdk/types"
	"github.com/stretchr/testify/require"
)

func TestPocketRequestIDUsesCompleteRelayRequestBytes(t *testing.T) {
	relayBytes := []byte("signed-relay-request")
	digest := sha256.Sum256(relayBytes)

	require.Equal(t, "pocket-req-"+hex.EncodeToString(digest[:]), PocketRequestID(relayBytes))
	require.NotEqual(t, PocketRequestID(relayBytes), PocketRequestID([]byte("different-request")))
	require.Empty(t, PocketRequestID(nil))
}

func TestClassifyRelayWorkloadJSONRPC(t *testing.T) {
	relayRequest := relayRequestWithHTTPRequest(t, `{"jsonrpc":"2.0","method":"eth_call","params":[{"data":"secret"}],"id":1}`, "3", "")

	classification := ClassifyRelayWorkload(relayRequest)

	require.Equal(t, BackendTypeJSONRPC, classification.RPCType)
	require.Equal(t, "eth_call", classification.Workload)
	require.Equal(t, "eth_call", classification.JSONRPCMethod)
	require.False(t, classification.JSONRPCBatch)
}

func TestClassifyRelayWorkloadJSONRPCMethod(t *testing.T) {
	relayRequest := relayRequestWithHTTPRequest(t, `{"jsonrpc":"2.0","method":"eth_getLogs","params":[{"address":"secret"}],"id":1}`, "jsonrpc", "")

	classification := ClassifyRelayWorkload(relayRequest)

	require.Equal(t, "eth_getLogs", classification.Workload)
	require.Equal(t, "eth_getLogs", classification.JSONRPCMethod)
}

func TestClassifyRelayWorkloadJSONRPCBatch(t *testing.T) {
	relayRequest := relayRequestWithHTTPRequest(t, `[{"jsonrpc":"2.0","method":"eth_call","params":["secret"]},{"jsonrpc":"2.0","method":"eth_getLogs","params":["secret"]}]`, "3", "")

	classification := ClassifyRelayWorkload(relayRequest)

	require.Equal(t, BackendTypeJSONRPC, classification.RPCType)
	require.Equal(t, workloadJSONRPCBatch, classification.Workload)
	require.True(t, classification.JSONRPCBatch)
	require.Equal(t, 2, classification.JSONRPCBatchSize)
	require.Empty(t, classification.JSONRPCMethod)
}

func TestClassifyRelayWorkloadRESTOmitsQuery(t *testing.T) {
	relayRequest := relayRequestWithHTTPRequest(t, `{"query":"secret"}`, "4", "https://backend.example/v1/blocks?token=secret")

	classification := ClassifyRelayWorkload(relayRequest)

	require.Equal(t, BackendTypeREST, classification.RPCType)
	require.Equal(t, workloadREST, classification.Workload)
	require.Equal(t, "/v1/blocks", classification.RESTPath)
}

func relayRequestWithHTTPRequest(t *testing.T, body, rpcType, rawURL string) *servicetypes.RelayRequest {
	t.Helper()
	if rawURL == "" {
		rawURL = "https://backend.example/"
	}
	req, err := http.NewRequest(http.MethodPost, rawURL, strings.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Rpc-Type", rpcType)

	_, serialized, err := sdktypes.SerializeHTTPRequest(req)
	require.NoError(t, err)
	return &servicetypes.RelayRequest{Payload: serialized}
}
