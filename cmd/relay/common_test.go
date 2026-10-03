package relay

import (
	"testing"

	servicetypes "github.com/pokt-network/poktroll/x/service/types"
	sdktypes "github.com/pokt-network/shannon-sdk/types"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// makeRelayResponse builds a RelayResponse whose Payload is a serialized
// POKTHTTPResponse, matching what the relayer signs onto the wire (the backend
// HTTP response wrapped in protobuf).
func makeRelayResponse(t *testing.T, statusCode uint32, bodyBz []byte) *servicetypes.RelayResponse {
	t.Helper()

	poktResp := &sdktypes.POKTHTTPResponse{
		StatusCode: statusCode,
		Header:     make(map[string]*sdktypes.Header),
		BodyBz:     bodyBz,
	}
	poktResp.Header["Content-Type"] = &sdktypes.Header{
		Key:    "Content-Type",
		Values: []string{"application/json"},
	}

	payloadBz, err := proto.MarshalOptions{Deterministic: true}.Marshal(poktResp)
	require.NoError(t, err, "marshal POKTHTTPResponse")

	return &servicetypes.RelayResponse{Payload: payloadBz}
}

// TestCheckRelayResponseError_SignedBackendError500 covers the exact live false
// positive: the relayer signs a POKTHTTPResponse{StatusCode:500} carrying a
// backend transport error, and the CLI must count it as a failure.
func TestCheckRelayResponseError_SignedBackendError500(t *testing.T) {
	resp := makeRelayResponse(t, 500, []byte(`{"error":"backend error: Post \"backend:///\": unsupported protocol scheme"}`))

	err := CheckRelayResponseError(resp)

	require.Error(t, err)
	require.ErrorContains(t, err, "backend HTTP 500")
}

// TestCheckRelayResponseError_Status429 verifies a non-500 4xx/5xx status is
// also surfaced as a backend error.
func TestCheckRelayResponseError_Status429(t *testing.T) {
	resp := makeRelayResponse(t, 429, []byte(`{"error":"too many requests"}`))

	err := CheckRelayResponseError(resp)

	require.Error(t, err)
	require.ErrorContains(t, err, "backend HTTP 429")
}

// TestCheckRelayResponseError_OK200CleanJSONRPC verifies a clean 200 JSON-RPC
// result is not flagged as an error.
func TestCheckRelayResponseError_OK200CleanJSONRPC(t *testing.T) {
	resp := makeRelayResponse(t, 200, []byte(`{"jsonrpc":"2.0","result":"0x10","id":1}`))

	err := CheckRelayResponseError(resp)

	require.NoError(t, err)
}

// TestCheckRelayResponseError_OK200WithJSONRPCError verifies a 200 response
// carrying a JSON-RPC error object in the body is flagged, with the code and
// message preserved.
func TestCheckRelayResponseError_OK200WithJSONRPCError(t *testing.T) {
	resp := makeRelayResponse(t, 200, []byte(`{"jsonrpc":"2.0","error":{"code":-32601,"message":"method not found"},"id":1}`))

	err := CheckRelayResponseError(resp)

	require.Error(t, err)
	require.ErrorContains(t, err, "-32601")
	require.ErrorContains(t, err, "method not found")
}

// TestCheckRelayResponseError_FallbackRawJSONError verifies the fallback path:
// when the payload is NOT a wrapped POKTHTTPResponse but raw JSON carrying a
// JSON-RPC error, the best-effort check still detects it.
func TestCheckRelayResponseError_FallbackRawJSONError(t *testing.T) {
	resp := &servicetypes.RelayResponse{
		Payload: []byte(`{"error":{"code":1,"message":"boom"}}`),
	}

	err := CheckRelayResponseError(resp)

	require.Error(t, err)
	require.ErrorContains(t, err, "boom")
}

// TestCheckRelayResponseError_FallbackRawNonJSON verifies the best-effort
// fallback stays quiet for arbitrary bytes that are neither protobuf nor JSON.
func TestCheckRelayResponseError_FallbackRawNonJSON(t *testing.T) {
	resp := &servicetypes.RelayResponse{
		Payload: []byte{0x01, 0x02, 0x03, 0xff, 0xfe, 'h', 'e', 'l', 'l', 'o'},
	}

	err := CheckRelayResponseError(resp)

	require.NoError(t, err)
}

// TestCheckRelayResponseError_NilResponse verifies the nil guard.
func TestCheckRelayResponseError_NilResponse(t *testing.T) {
	err := CheckRelayResponseError(nil)

	require.Error(t, err)
	require.ErrorContains(t, err, "nil relay response")
}

// TestCheckRelayResponseError_OK200NonJSONBody verifies a 200 response whose
// body is binary (not JSON, e.g. gRPC/protobuf payload) is not flagged.
func TestCheckRelayResponseError_OK200NonJSONBody(t *testing.T) {
	resp := makeRelayResponse(t, 200, []byte{0x00, 0x01, 0x02, 0x03, 0xff})

	err := CheckRelayResponseError(resp)

	require.NoError(t, err)
}
