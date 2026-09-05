package relayer

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"regexp"
	"strings"
	"testing"

	servicetypes "github.com/pokt-network/poktroll/x/service/types"
	sdktypes "github.com/pokt-network/shannon-sdk/types"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

func TestPocketRequestIDUsesCompleteRelayRequestBytes(t *testing.T) {
	relayBytes := []byte("signed-relay-request")
	digest := sha256.Sum256(relayBytes)

	requestID := PocketRequestID(relayBytes)
	require.Equal(t, hex.EncodeToString(digest[:16]), requestID)
	require.Len(t, requestID, 32)
	require.Regexp(t, regexp.MustCompile(`^[0-9a-f]{32}$`), requestID)
	require.NotEqual(t, requestID, PocketRequestID([]byte("different-request")))
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
	require.Equal(t, []string{"eth_call", "eth_getLogs"}, classification.JSONRPCMethods)
	require.False(t, classification.BatchMethodsTruncated)
}

func TestClassifyRelayWorkloadJSONRPCBatchBoundsMethodSummary(t *testing.T) {
	methods := make([]string, 10)
	for i := range methods {
		methods[i] = `{"jsonrpc":"2.0","method":"method_` + string(rune('j'-i)) + `","id":1}`
	}
	methods = append(methods, methods[0])
	body := "[" + strings.Join(methods, ",") + "]"
	relayRequest := relayRequestWithHTTPRequest(t, body, "3", "")

	classification := ClassifyRelayWorkload(relayRequest)

	require.Equal(t, []string{"method_a", "method_b", "method_c", "method_d", "method_e", "method_f", "method_g", "method_h"}, classification.JSONRPCMethods)
	require.True(t, classification.BatchMethodsTruncated)
}

func TestClassifyRelayWorkloadRESTOmitsQuery(t *testing.T) {
	relayRequest := relayRequestWithHTTPRequest(t, `{"query":"secret"}`, "4", "https://backend.example/v1/blocks/123/0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa?token=secret")

	classification := ClassifyRelayWorkload(relayRequest)

	require.Equal(t, BackendTypeREST, classification.RPCType)
	require.Equal(t, workloadREST, classification.Workload)
	require.Equal(t, http.MethodPost, classification.HTTPMethod)
	require.Equal(t, "/v1/blocks/:height/:hash", classification.NormalizedPath)
	require.Equal(t, "/v1/blocks/:height/:hash", classification.RESTPath)
	require.Equal(t, len(`{"query":"secret"}`), classification.BackendRequestBytes)
}

func TestClassifyRelayWorkloadSeparatesBackendAndOuterRequestBytes(t *testing.T) {
	backendBody := `{"jsonrpc":"2.0","method":"eth_blockNumber","id":1}`
	relayRequest := relayRequestWithHTTPRequest(t, backendBody, "3", "https://backend.example/123")
	outerBytes, err := relayRequest.Marshal()
	require.NoError(t, err)

	classification := ClassifyRelayWorkload(relayRequest)

	require.Equal(t, len(backendBody), classification.BackendRequestBytes)
	require.NotEqual(t, len(outerBytes), classification.BackendRequestBytes)
	require.Equal(t, http.MethodPost, classification.HTTPMethod)
	require.Equal(t, "/:height", classification.NormalizedPath)
}

func TestSetPocketRequestIDOverridesSpoofedHeader(t *testing.T) {
	relayBytes := []byte("signed-relay-request")
	header := make(http.Header)
	header.Set(HeaderPocketRequestID, "spoofed-by-client")

	setPocketRequestID(header, relayBytes)

	require.Equal(t, PocketRequestID(relayBytes), header.Get(HeaderPocketRequestID))
	require.NotEqual(t, "spoofed-by-client", header.Get(HeaderPocketRequestID))
}

func TestRelayObservationIncludesBackendFailure(t *testing.T) {
	var output bytes.Buffer
	logger := zerolog.New(&output)
	logRelayObservation(logger, &servicetypes.RelayRequest{}, RelayObservation{
		RequestID: "0123456789abcdef0123456789abcdef",
		RPCType:   BackendTypeJSONRPC,
		Workload: RelayWorkload{
			Workload:            "eth_call",
			HTTPMethod:          http.MethodPost,
			NormalizedPath:      "/",
			BackendRequestBytes: 23,
		},
		RelayRequestBytes: 99,
		StatusCode:        http.StatusBadGateway,
		BackendLatency:    10,
		TotalLatency:      20,
		Outcome:           "backend_error",
		RejectReason:      rejectReasonBackendTimeout,
	})

	logLine := output.String()
	require.Contains(t, logLine, `"event":"pocket_relay_observation"`)
	require.Contains(t, logLine, `"outcome":"backend_error"`)
	require.Contains(t, logLine, `"reject_reason":"backend_timeout"`)
	require.Contains(t, logLine, `"status_code":502`)
	require.Contains(t, logLine, `"http_method":"POST"`)
	require.Contains(t, logLine, `"normalized_path":"/"`)
	require.Contains(t, logLine, `"backend_request_bytes":23`)
	require.Contains(t, logLine, `"relay_request_bytes":99`)
	require.NotContains(t, logLine, "params")
	require.NotContains(t, logLine, "calldata")
	require.NotContains(t, logLine, "secret")
	require.NotContains(t, logLine, "request_payload")
	require.NotContains(t, logLine, "response_payload")
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
