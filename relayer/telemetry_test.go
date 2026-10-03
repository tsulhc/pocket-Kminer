//go:build test

package relayer

import (
	"bytes"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	servicetypes "github.com/pokt-network/poktroll/x/service/types"
	sessiontypes "github.com/pokt-network/poktroll/x/session/types"
	sdktypes "github.com/pokt-network/shannon-sdk/types"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/pokt-network/pocket-relay-miner/logging"
)

func testRelayRequest(t *testing.T, body []byte, headers map[string]*sdktypes.Header) *servicetypes.RelayRequest {
	t.Helper()
	inner := &sdktypes.POKTHTTPRequest{
		Method: http.MethodPost,
		Url:    "/",
		Header: headers,
		BodyBz: body,
	}
	payload, err := proto.Marshal(inner)
	require.NoError(t, err)
	return &servicetypes.RelayRequest{
		Meta: servicetypes.RelayRequestMetadata{
			SessionHeader: &sessiontypes.SessionHeader{
				ServiceId:               "test-service",
				SessionId:               "session-1",
				ApplicationAddress:      "pokt1app",
				SessionEndBlockHeight:   10,
				SessionStartBlockHeight: 5,
			},
			SupplierOperatorAddress: "pokt1supplier",
		},
		Payload: payload,
	}
}

func TestPocketRequestIDIs32Hex(t *testing.T) {
	id := pocketRequestID([]byte("relay-bytes"))
	require.Len(t, id, 32)
	require.Regexp(t, `^[0-9a-f]{32}$`, id)
	require.Equal(t, "", pocketRequestID(nil))
	require.Equal(t, "", pocketRequestID([]byte{}))
	// Stable for same input, different for different input.
	require.Equal(t, pocketRequestID([]byte("a")), pocketRequestID([]byte("a")))
	require.NotEqual(t, pocketRequestID([]byte("a")), pocketRequestID([]byte("b")))
}

func TestPocketRequestIDFromRelayRequest(t *testing.T) {
	req := testRelayRequest(t, []byte(`{"jsonrpc":"2.0"}`), nil)
	id := pocketRequestIDFromRelayRequest(req)
	require.Len(t, id, 32)
	require.Equal(t, "", pocketRequestIDFromRelayRequest(nil))
	// Same serialization path as newRelayObservation fallback.
	raw, err := req.Marshal()
	require.NoError(t, err)
	require.Equal(t, pocketRequestID(raw), id)
}

func TestClassifyWorkloadIsClosedSet(t *testing.T) {
	cases := []struct {
		rpcType string
		body    []byte
		headers map[string]*sdktypes.Header
		wantRPC string
		wantWL  string
	}{
		{"jsonrpc", []byte(`{"jsonrpc":"2.0"}`), nil, workloadJSONRPC, workloadJSONRPC},
		{"jsonrpc", []byte(`[{},{}]`), nil, workloadJSONRPC, workloadJSONRPCBatch},
		{"jsonrpc", []byte(`[garbage`), nil, workloadJSONRPC, workloadJSONRPCBatch},
		{"rest", []byte("hello"), nil, workloadREST, workloadREST},
		{"grpc", []byte("x"), nil, workloadGRPC, workloadGRPC},
		{"websocket", []byte("x"), nil, workloadWebSocket, workloadWebSocket},
		{"cometbft", []byte("x"), nil, workloadCometBFT, workloadCometBFT},
		{"bogus-type", []byte("x"), nil, workloadUnknown, workloadUnknown},
		{"", []byte(""), nil, workloadUnknown, workloadUnknown},
	}
	for _, tc := range cases {
		inner := &sdktypes.POKTHTTPRequest{Method: "POST", Url: "/", Header: tc.headers, BodyBz: tc.body}
		got := classifyHTTPRequestWorkload(inner, tc.rpcType)
		require.Equal(t, tc.wantRPC, got.RPCType, "rpc %q", tc.rpcType)
		require.Equal(t, tc.wantWL, got.Workload, "rpc %q", tc.rpcType)
		require.Equal(t, len(tc.body), got.BackendRequestBytes)
	}
	// Inner Rpc-Type header is honored when route is unknown.
	inner := &sdktypes.POKTHTTPRequest{
		Method: "POST",
		Url:    "/",
		Header: map[string]*sdktypes.Header{"Rpc-Type": {Key: "Rpc-Type", Values: []string{"rest"}}},
		BodyBz: []byte("x"),
	}
	require.Equal(t, workloadREST, classifyHTTPRequestWorkload(inner, "").RPCType)
	// Oversized header values are dropped to unknown.
	big := strings.Repeat("x", maxTelemetryHeaderBytes+1)
	inner.Header = map[string]*sdktypes.Header{"Rpc-Type": {Key: "Rpc-Type", Values: []string{big}}}
	require.Equal(t, workloadUnknown, classifyHTTPRequestWorkload(inner, "").RPCType)
	// JSON content-type fallback applies when the route is unknown.
	inner.Header = map[string]*sdktypes.Header{"Content-Type": {Key: "Content-Type", Values: []string{"application/json"}}}
	inner.BodyBz = []byte("x")
	require.Equal(t, workloadJSONRPC, classifyHTTPRequestWorkload(inner, "bogus").RPCType)
	inner2 := &sdktypes.POKTHTTPRequest{
		Method: "POST", Url: "/",
		Header: map[string]*sdktypes.Header{"Content-Type": {Key: "Content-Type", Values: []string{"application/json"}}},
		BodyBz: []byte(`{}`),
	}
	require.Equal(t, workloadJSONRPC, classifyHTTPRequestWorkload(inner2, "").RPCType)
}

func TestSetPocketRequestIDOverridesSpoof(t *testing.T) {
	header := http.Header{}
	header.Set(HeaderPocketRequestID, "spoofed")
	header.Set("Pocket-Request-Id", "spoofed-lower")
	setPocketRequestID(header, []byte("envelope"))
	require.Equal(t, pocketRequestID([]byte("envelope")), header.Get(HeaderPocketRequestID))
	// Empty derivation deletes any spoofed value.
	header.Set(HeaderPocketRequestID, "spoofed")
	setPocketRequestID(header, nil)
	require.Equal(t, "", header.Get(HeaderPocketRequestID))
}

func TestSafeEndpointName(t *testing.T) {
	require.Equal(t, "localhost:8545", safeEndpointName("localhost:8545"))
	require.Equal(t, "backend", safeEndpointName("backend"))
	require.Equal(t, "[::1]:8545", safeEndpointName("[::1]:8545"))
	require.Equal(t, "", safeEndpointName("https://api.example.com/v1?key=x"))
	require.Equal(t, "", safeEndpointName("has space"))
	require.Equal(t, "", safeEndpointName(strings.Repeat("a", maxTelemetryEndpointBytes+1)))
}

func TestSafeTelemetryIdentity(t *testing.T) {
	require.Equal(t, "session-1", safeTelemetryIdentity("session-1"))
	require.Equal(t, "", safeTelemetryIdentity("has space"))
	require.Equal(t, "", safeTelemetryIdentity("semicolon;evil"))
	require.Equal(t, "", safeTelemetryIdentity(strings.Repeat("a", maxTelemetryIdentityBytes+1)))
}

func TestNewRelayObservationDefaults(t *testing.T) {
	req := testRelayRequest(t, []byte(`{}`), nil)
	obs := newRelayObservation(req, []byte("raw-envelope"), reqPayloadForTest(req), "svc", "jsonrpc")
	require.Equal(t, pocketRequestID([]byte("raw-envelope")), obs.RequestID)
	require.Equal(t, len("raw-envelope"), obs.RequestBytes)
	require.Equal(t, relayOutcomeRejected, obs.Outcome)
	require.Equal(t, "svc", obs.ServiceID)
}

func reqPayloadForTest(req *servicetypes.RelayRequest) *sdktypes.POKTHTTPRequest {
	inner, err := sdktypes.DeserializeHTTPRequest(req.Payload)
	if err != nil {
		return nil
	}
	return inner
}

func TestBoundedHelpers(t *testing.T) {
	require.Equal(t, workloadUnknown, boundedWorkload("arbitrary-method"))
	require.Equal(t, workloadJSONRPC, boundedWorkload(workloadJSONRPC))
	require.Equal(t, relayOutcomeRejected, boundedObservationOutcome("success"))
	require.Equal(t, relayOutcomeServed, boundedObservationOutcome(relayOutcomeServed))
	require.Equal(t, "success", boundedBackendOutcome("success"))
	require.Equal(t, "", boundedBackendOutcome("arbitrary"))
	require.Equal(t, rejectReasonBackend5xx, boundedRejectReason(rejectReasonBackend5xx))
	require.Equal(t, "", boundedRejectReason("client error text with secret"))
}

func captureObservationLog(t *testing.T, parentLevel zerolog.Level, req *servicetypes.RelayRequest, obs relayObservation) (string, logging.Logger, *bytes.Buffer) {
	t.Helper()
	var buf bytes.Buffer
	parent := zerolog.New(&buf).Level(parentLevel)
	logRelayObservation(parent, req, obs)
	return buf.String(), parent, &buf
}

func TestRelayObservationVisibleUnderWarnParent(t *testing.T) {
	req := testRelayRequest(t, []byte(`{}`), nil)
	obs := newRelayObservation(req, []byte("raw"), reqPayloadForTest(req), "svc", "jsonrpc")
	obs.Outcome = relayOutcomeServed
	out, parent, _ := captureObservationLog(t, zerolog.WarnLevel, req, obs)
	require.Equal(t, zerolog.WarnLevel, parent.GetLevel(), "parent level must not change")
	require.Contains(t, out, relayObservationEvent)
	require.Contains(t, out, obs.RequestID)
	// Ordinary Info on the Warn parent stays suppressed.
	var buf2 bytes.Buffer
	warnParent := zerolog.New(&buf2).Level(zerolog.WarnLevel)
	warnParent.Info().Msg("ordinary info")
	require.Empty(t, buf2.String())
}

func TestObservationNeverCarriesPayload(t *testing.T) {
	secretBody := []byte(`{"jsonrpc":"2.0","method":"eth_send","params":["SECRET_PARAM"],"calldata":"0xdeadbeef"}`)
	headers := map[string]*sdktypes.Header{
		"Content-Type": {Key: "Content-Type", Values: []string{"application/json"}},
	}
	req := testRelayRequest(t, secretBody, headers)
	obs := newRelayObservation(req, []byte("raw-envelope"), reqPayloadForTest(req), "svc", "jsonrpc")
	obs.Outcome = relayOutcomeServed
	obs.BackendOutcome = "success"
	obs.BackendEndpoint = "backend"
	obs.BackendResponseBytes = 12
	out, _, _ := captureObservationLog(t, zerolog.WarnLevel, req, obs)
	require.NotContains(t, out, "SECRET_PARAM")
	require.NotContains(t, out, "deadbeef")
	require.NotContains(t, out, "params")
	require.NotContains(t, out, "calldata")
	require.NotContains(t, out, string(secretBody))
	// Structured JSON must not contain method/path/query fields.
	var decoded map[string]any
	require.NoError(t, json.Unmarshal([]byte(strings.TrimSpace(out)), &decoded))
	for _, forbidden := range []string{"jsonrpc_method", "http_method", "normalized_path", "params", "calldata", "query"} {
		_, ok := decoded[forbidden]
		require.False(t, ok, "forbidden field %s present", forbidden)
	}
}

func TestObservationSessionIdentityBounded(t *testing.T) {
	big := strings.Repeat("A", 5000) + " evil;space"
	inner := &sdktypes.POKTHTTPRequest{Method: "POST", Url: "/", BodyBz: []byte(`{}`)}
	payload, err := proto.Marshal(inner)
	require.NoError(t, err)
	req := &servicetypes.RelayRequest{
		Meta: servicetypes.RelayRequestMetadata{
			SessionHeader: &sessiontypes.SessionHeader{
				ServiceId:          big,
				SessionId:          big,
				ApplicationAddress: big,
			},
			SupplierOperatorAddress: big,
		},
		Payload: payload,
	}
	obs := newRelayObservation(req, []byte("raw"), inner, "configured-service", "jsonrpc")
	out, _, _ := captureObservationLog(t, zerolog.WarnLevel, req, obs)
	require.NotContains(t, out, big)
	require.NotContains(t, out, "evil;space")
	require.Contains(t, out, "configured-service")
}
