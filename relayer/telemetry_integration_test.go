//go:build test

package relayer

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	cryptotypes "github.com/cosmos/cosmos-sdk/crypto/types"
	"github.com/gorilla/websocket"
	ring_secp256k1 "github.com/pokt-network/go-dleq/secp256k1"
	servicetypes "github.com/pokt-network/poktroll/x/service/types"
	sessiontypes "github.com/pokt-network/poktroll/x/session/types"
	sdktypes "github.com/pokt-network/shannon-sdk/types"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/pokt-network/pocket-relay-miner/logging"
	"github.com/pokt-network/pocket-relay-miner/rings"
	"github.com/pokt-network/pocket-relay-miner/transport"
)

// buildSpoofedSimBody is buildSignedSimBody with caller-supplied inner headers,
// signed afterwards so the ring signature covers the spoofed inner header. The
// trusted ID must still be derived from the final envelope bytes, not from any
// header value.
func buildSpoofedSimBody(t *testing.T, f *simHTTPFixture, appAddr, serviceID, sessionID string, inner map[string]*sdktypes.Header) []byte {
	t.Helper()
	poktReq := &sdktypes.POKTHTTPRequest{Method: http.MethodPost, Url: "/", Header: inner, BodyBz: []byte(`{"jsonrpc":"2.0","method":"eth_blockNumber","id":1}`)}
	payloadBz, err := proto.Marshal(poktReq)
	require.NoError(t, err)

	rr := &servicetypes.RelayRequest{
		Payload: payloadBz,
		Meta: servicetypes.RelayRequestMetadata{
			SessionHeader: &sessiontypes.SessionHeader{
				ApplicationAddress:      appAddr,
				ServiceId:               serviceID,
				SessionId:               sessionID,
				SessionStartBlockHeight: 1,
				SessionEndBlockHeight:   2,
			},
			SupplierOperatorAddress: f.supplierAddr,
		},
	}
	hash, err := rr.GetSignableBytesHash()
	require.NoError(t, err)
	ring, err := rings.GetRingFromPubKeys([]cryptotypes.PubKey{f.appPriv.PubKey(), f.gwPriv.PubKey()})
	require.NoError(t, err)
	scalar, err := ring_secp256k1.NewCurve().DecodeToScalar(f.gwPriv.Bytes())
	require.NoError(t, err)
	sig, err := ring.Sign(hash, scalar)
	require.NoError(t, err)
	sigBz, err := sig.Serialize()
	require.NoError(t, err)
	rr.Meta.Signature = sigBz

	bodyBz, err := rr.Marshal()
	require.NoError(t, err)
	return bodyBz
}

func postWithOuterHeaders(t *testing.T, f *simHTTPFixture, body []byte, outer map[string]string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body))
	req.Header.Set("Rpc-Type", "3")
	for k, v := range outer {
		req.Header.Set(k, v)
	}
	w := httptest.NewRecorder()
	f.proxy.handleRelay(w, req)
	return w
}

func setConfigHeaderSpoof(t *testing.T, f *simHTTPFixture, value string) {
	t.Helper()
	svc, ok := f.proxy.config.Services[simTestService]
	require.True(t, ok, "sim service must exist in fixture config")
	bc := svc.Backends["jsonrpc"]
	if bc.Headers == nil {
		bc.Headers = map[string]string{}
	}
	bc.Headers["Pocket-Request-ID"] = value
	svc.Backends["jsonrpc"] = bc
	f.proxy.config.Services[simTestService] = svc
}

// TestTrustedRequestIDSurvivesInnerOuterAndConfigSpoof is the HTTP
// spoof-resistance proof: inner signed headers, outer wrapper headers and
// operator backend headers all carry a forged Pocket-Request-ID, and the
// backend must receive the SHA-derived ID of the final envelope bytes.
func TestTrustedRequestIDSurvivesInnerOuterAndConfigSpoof(t *testing.T) {
	var gotID atomic.Value
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotID.Store(r.Header.Get("Pocket-Request-ID"))
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","result":"0x10","id":1}`))
	}))
	defer backend.Close()

	f, _, _, _ := newEagerChargeFixture(t, backend.URL, acceptAnyValidator{})
	setConfigHeaderSpoof(t, f, "CONFIG-SPOOF")

	inner := map[string]*sdktypes.Header{
		"Pocket-Request-ID": {Key: "Pocket-Request-ID", Values: []string{"INNER-SPOOF"}},
	}
	body := buildSpoofedSimBody(t, f, f.appAddr, simTestService, "sess-triple-spoof", inner)
	expected := pocketRequestID(body)
	require.Len(t, expected, 32)

	w := postWithOuterHeaders(t, f, body, map[string]string{"Pocket-Request-ID": "OUTER-SPOOF"})
	require.Equal(t, http.StatusOK, w.Code, "body=%s", w.Body.String())
	got, ok := gotID.Load().(string)
	require.True(t, ok && got != "", "premise: backend observed the request header")
	require.Equal(t, expected, got,
		"backend must receive the derived ID, not any of the three spoof sources")
}

// TestGRPCRequestIDPropagatesTrustedID proves the gRPC path derives the ID
// from the typed RelayRequest and overwrites a spoofed inner header.
func TestGRPCRequestIDPropagatesTrustedID(t *testing.T) {
	var gotID atomic.Value
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotID.Store(r.Header.Get("Pocket-Request-ID"))
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","result":"0x10","id":1}`))
	}))
	defer backend.Close()

	fx := newGRPCPublishFixture(t, backend.URL)

	var inner sdktypes.POKTHTTPRequest
	require.NoError(t, proto.Unmarshal(fx.stream.req.Payload, &inner))
	if inner.Header == nil {
		inner.Header = map[string]*sdktypes.Header{}
	}
	inner.Header["Pocket-Request-ID"] = &sdktypes.Header{Key: "Pocket-Request-ID", Values: []string{"GRPC-INNER-SPOOF"}}
	mutated, err := proto.Marshal(&inner)
	require.NoError(t, err)
	fx.stream.req.Payload = mutated
	expected := pocketRequestIDFromRelayRequest(fx.stream.req)
	require.Len(t, expected, 32)

	require.NoError(t, fx.svc.handleSendRelay(fx.stream))
	require.Equal(t, int32(1), fx.proc.calls.Load(), "premise: the relay was served, so the backend was hit")
	got, ok := gotID.Load().(string)
	require.True(t, ok && got != "", "premise: backend observed the request header")
	require.Equal(t, expected, got,
		"gRPC backend must receive the derived ID, not the spoofed inner header")
}

// TestForwardToBackend_RequestIDOverridesConfigSpoof is the Redis-free unit
// proof that operator-configured Pocket-Request-ID headers never survive: the
// derived value wins, and an empty derivation deletes the header.
func TestForwardToBackend_RequestIDOverridesConfigSpoof(t *testing.T) {
	var gotID atomic.Value
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotID.Store(r.Header.Get("Pocket-Request-ID"))
		w.WriteHeader(http.StatusOK)
	}))
	defer backend.Close()

	svc := newTestGRPCService(t)
	svcConfig := &ServiceConfig{
		Backends: map[string]BackendConfig{
			BackendTypeREST: {URL: backend.URL, Headers: map[string]string{"Pocket-Request-ID": "CONFIG-SPOOF"}},
		},
	}
	poktReq := &sdktypes.POKTHTTPRequest{
		Method: http.MethodPost,
		Url:    "/v1",
		Header: map[string]*sdktypes.Header{
			"Pocket-Request-ID": {Key: "Pocket-Request-ID", Values: []string{"INNER-SPOOF"}},
		},
	}

	_, _, _, err := svc.forwardToBackend(t.Context(), "svc", svcConfig, poktReq, nil, BackendTypeREST, "trusted-derived-id")
	require.NoError(t, err)
	require.Equal(t, "trusted-derived-id", gotID.Load().(string))

	_, _, _, err = svc.forwardToBackend(t.Context(), "svc", svcConfig, poktReq, nil, BackendTypeREST, "")
	require.NoError(t, err)
	require.Equal(t, "", gotID.Load().(string), "empty derivation must delete the header, not leave a spoof")
}

// TestHTTPRetryKeepsSingleRequestID proves a retried relay carries one trusted
// ID: the failing endpoint and the succeeding endpoint observe the same value,
// which equals the envelope digest, and the client sees Backend-Retries: 1.
func TestHTTPRetryKeepsSingleRequestID(t *testing.T) {
	var firstID, secondID atomic.Value
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		firstID.Store(r.Header.Get("Pocket-Request-ID"))
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","error":{"code":-32000,"message":"down"},"id":1}`))
	}))
	defer bad.Close()
	good := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		secondID.Store(r.Header.Get("Pocket-Request-ID"))
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","result":"0x10","id":1}`))
	}))
	defer good.Close()

	f, _, _, _ := newEagerChargeFixture(t, good.URL, acceptAnyValidator{})
	one := 1
	svc := f.proxy.config.Services[simTestService]
	svc.Backends["jsonrpc"] = BackendConfig{
		URLs: []BackendEndpointConfig{
			{Name: "bad", URL: bad.URL},
			{Name: "good", URL: good.URL},
		},
		LoadBalancing: "first_healthy",
		MaxRetries:    &one,
	}
	f.proxy.config.Services[simTestService] = svc
	require.NoError(t, f.proxy.config.BuildPools())

	body := f.buildSignedSimBody(t, f.appAddr, simTestService, "sess-retry-id")
	expected := pocketRequestID(body)

	w := f.post(t, body, false)
	require.Equal(t, http.StatusOK, w.Code, "body=%s", w.Body.String())
	require.Equal(t, "1", w.Header().Get("Backend-Retries"), "exactly one retry must be reported")
	first, ok := firstID.Load().(string)
	require.True(t, ok && first != "", "premise: failing endpoint observed the request header")
	second, ok := secondID.Load().(string)
	require.True(t, ok && second != "", "premise: retry endpoint observed the request header")
	require.Equal(t, expected, first, "failing endpoint sees the trusted ID")
	require.Equal(t, expected, second, "retry preserves the same trusted ID")
}

// telemetrySyncBuffer is a log sink the bridge goroutines and the test share.
type telemetrySyncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *telemetrySyncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *telemetrySyncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// newPublishingV1BridgeWithLogger mirrors newPublishingV1Bridge with an
// injectable logger so tests can capture the Warn-visible observation events.
func newPublishingV1BridgeWithLogger(
	t *testing.T,
	backendURL string,
	signer *ResponseSigner,
	pipeline *RelayPipeline,
	publisher transport.MinedRelayPublisher,
	logger zerolog.Logger,
) *websocket.Conn {
	t.Helper()
	relayerConn, gwClient := newGatewaySideHarness(t)
	bridge, err := NewWebSocketBridge(
		logging.Logger(logger), relayerConn, backendURL, simWSTestService,
		"",
		atHeight(100),
		&recordingProcessor{}, publisher, signer, http.Header{},
		nil, pipeline, 2*time.Second, false, nil, "", nil, nil,
	)
	require.NoError(t, err)
	runBridge(t, bridge)
	return gwClient
}

// TestWebSocketRolloverKeepsPinnedIdentityWithDistinctRequestIDs serves two
// accepted sessions on one connection and proves the observation events carry
// distinct request IDs and session IDs but identical pinned
// supplier/service/application.
func TestWebSocketRolloverKeepsPinnedIdentityWithDistinctRequestIDs(t *testing.T) {
	verifyNoBridgeGoroutines(t)
	pipeline, _, _ := newOwnerTestPipeline(t)
	backendURL, _, _ := newSimWSBackendServer(t)
	supplier, signer := newSupplier(t)
	published := newPublishSignal()

	sink := &telemetrySyncBuffer{}
	logger := zerolog.New(sink).Level(zerolog.WarnLevel)
	conn := newPublishingV1BridgeWithLogger(t, backendURL, signer, pipeline, published, logger)

	sendRelay(t, conn, identityRelay("next-session-1", supplier, simWSTestService, ownerTestAppAddr, 91, 100))
	readServedResponse(t, conn)
	published.await(t, 1)

	sendRelay(t, conn, identityRelay("next-session-2", supplier, simWSTestService, ownerTestAppAddr, 101, 110))
	readServedResponse(t, conn)
	published.await(t, 1)

	records := decodeObservations(t, sink.String())
	require.Len(t, records, 2, "one observation per served frame; got %q", sink.String())
	require.NotEqual(t, records[0].RequestID, records[1].RequestID, "each relay gets its own request ID")
	require.Len(t, records[0].RequestID, 32)
	require.Len(t, records[1].RequestID, 32)
	require.Equal(t, "next-session-1", records[0].SessionID)
	require.Equal(t, "next-session-2", records[1].SessionID)
	for _, rec := range records {
		require.Equal(t, supplier, rec.Supplier, "supplier stays pinned across rollover")
		require.Equal(t, simWSTestService, rec.ServiceID, "service stays pinned across rollover")
		require.Equal(t, ownerTestAppAddr, rec.Application, "application stays pinned across rollover")
	}
}

// observationRecord is one decoded pocket_relay_observation log line.
type observationRecord struct {
	Event       string `json:"event"`
	RequestID   string `json:"pocket_request_id"`
	SessionID   string `json:"session_id"`
	ServiceID   string `json:"service_id"`
	Supplier    string `json:"supplier"`
	Application string `json:"application"`
}

func decodeObservations(t *testing.T, raw string) []observationRecord {
	t.Helper()
	var out []observationRecord
	for _, line := range strings.Split(strings.TrimSpace(raw), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		var rec observationRecord
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			continue
		}
		if rec.Event == relayObservationEvent {
			out = append(out, rec)
		}
	}
	return out
}
