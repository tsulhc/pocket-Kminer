//go:build test

package relayer

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/cosmos/cosmos-sdk/crypto/keys/secp256k1"
	cryptotypes "github.com/cosmos/cosmos-sdk/crypto/types"
	cosmostypes "github.com/cosmos/cosmos-sdk/types"
	"github.com/gorilla/websocket"
	servicetypes "github.com/pokt-network/poktroll/x/service/types"
	sessiontypes "github.com/pokt-network/poktroll/x/session/types"
	sharedtypes "github.com/pokt-network/poktroll/x/shared/types"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/pokt-network/pocket-relay-miner/logging"
	"github.com/pokt-network/pocket-relay-miner/transport"
	redisutil "github.com/pokt-network/pocket-relay-miner/transport/redis"
)

const websocketIdentityTestApp = "pokt1websocketidentityapp"

type websocketIdentityValidator struct {
	calls atomic.Int64
}

func (v *websocketIdentityValidator) ValidateRelayRequest(context.Context, *servicetypes.RelayRequest) error {
	v.calls.Add(1)
	return nil
}

func (*websocketIdentityValidator) CheckRewardEligibility(context.Context, *servicetypes.RelayRequest) error {
	return nil
}

func (*websocketIdentityValidator) GetCurrentBlockHeight() int64 { return 100 }

func (*websocketIdentityValidator) SetCurrentBlockHeight(int64) {}

func (*websocketIdentityValidator) UpdateAllowedSuppliers([]string) {}

type websocketIdentityProcessCall struct {
	supplier string
	service  string
	session  string
}

type websocketIdentityProcessor struct {
	calls chan websocketIdentityProcessCall
	total atomic.Int64
}

func (p *websocketIdentityProcessor) ProcessRelay(
	_ context.Context,
	reqBody, _ []byte,
	supplier, service string,
	_ int64,
) (*transport.MinedRelayMessage, error) {
	req := &servicetypes.RelayRequest{}
	if err := req.Unmarshal(reqBody); err != nil {
		return nil, err
	}
	var sessionID string
	if req.Meta.SessionHeader != nil {
		sessionID = req.Meta.SessionHeader.SessionId
	}
	p.total.Add(1)
	p.calls <- websocketIdentityProcessCall{supplier: supplier, service: service, session: sessionID}
	return &transport.MinedRelayMessage{}, nil
}

func (*websocketIdentityProcessor) GetServiceDifficulty(context.Context, string, int64) ([]byte, error) {
	return nil, nil
}

func (*websocketIdentityProcessor) SetDifficultyProvider(DifficultyProvider) {}

type websocketIdentityPublisher struct {
	calls chan struct{}
	total atomic.Int64
}

func (p *websocketIdentityPublisher) Publish(context.Context, *transport.MinedRelayMessage) error {
	p.total.Add(1)
	p.calls <- struct{}{}
	return nil
}

func (*websocketIdentityPublisher) PublishBatch(context.Context, []*transport.MinedRelayMessage) error {
	return nil
}

func (*websocketIdentityPublisher) Close() error { return nil }

type websocketIdentityFixture struct {
	conn        *websocket.Conn
	bridge      *WebSocketBridge
	backendHits *atomic.Int64
	validator   *websocketIdentityValidator
	processor   *websocketIdentityProcessor
	publisher   *websocketIdentityPublisher
	meter       *RelayMeter
	miniRedis   *miniredis.Miniredis
	supplier    string
}

func newWebSocketIdentityFixture(t *testing.T, service string, pinSupplierInHandshake bool, application string) *websocketIdentityFixture {
	t.Helper()

	logger := logging.NewLoggerFromConfig(logging.DefaultConfig())
	miniRedis, err := miniredis.Run()
	require.NoError(t, err)
	redisClient, err := redisutil.NewClient(context.Background(), redisutil.ClientConfig{
		URL: fmt.Sprintf("redis://%s", miniRedis.Addr()),
	})
	require.NoError(t, err)
	validator := &websocketIdentityValidator{}
	processor := &websocketIdentityProcessor{calls: make(chan websocketIdentityProcessCall, 8)}
	publisher := &websocketIdentityPublisher{calls: make(chan struct{}, 8)}

	appClient := &fakeAppClient{addr: application}
	appClient.stakeUpokt.Store(1_000_000_000)
	meter := NewRelayMeter(
		logger,
		redisClient,
		appClient,
		nil,
		&fakeSessionClient{numSuppliers: 1},
		nil,
		&fakeSharedParamCache{params: &sharedtypes.Params{
			NumBlocksPerSession:            10,
			ComputeUnitsToTokensMultiplier: 1,
			ComputeUnitCostGranularity:     1,
		}},
		nil,
		staticServiceFactor{f: 1},
		RelayMeterConfig{RedisKeyPrefix: "ha"},
	)
	require.NoError(t, meter.Start(context.Background()))
	t.Cleanup(func() {
		require.NoError(t, meter.Close())
		require.NoError(t, redisClient.Close())
		miniRedis.Close()
	})

	privateKey := secp256k1.GenPrivKey()
	supplier := cosmostypes.AccAddress(privateKey.PubKey().Address()).String()
	handshakeSupplier := ""
	if pinSupplierInHandshake {
		handshakeSupplier = supplier
	}
	responseSigner, err := NewResponseSigner(logger, map[string]cryptotypes.PrivKey{supplier: privateKey})
	require.NoError(t, err)
	pipeline := NewRelayPipeline(validator, meter, responseSigner, processor, logger, nil, nil)

	backendHits := &atomic.Int64{}
	backendServer := websocketIdentityHTTPServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		backendConn, upgradeErr := WebSocketUpgrader.Upgrade(w, r, nil)
		if upgradeErr != nil {
			return
		}
		defer backendConn.Close()
		for {
			_, _, readErr := backendConn.ReadMessage()
			if readErr != nil {
				return
			}
			backendHits.Add(1)
			if writeErr := backendConn.WriteMessage(websocket.BinaryMessage, []byte("backend-response")); writeErr != nil {
				return
			}
		}
	}))
	backendURL := "ws" + strings.TrimPrefix(backendServer.URL, "http")

	bridgeReady := make(chan struct {
		bridge *WebSocketBridge
		err    error
	}, 1)
	gatewayServer := websocketIdentityHTTPServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gatewayConn, upgradeErr := WebSocketUpgrader.Upgrade(w, r, nil)
		if upgradeErr != nil {
			bridgeReady <- struct {
				bridge *WebSocketBridge
				err    error
			}{err: upgradeErr}
			return
		}
		bridge, bridgeErr := NewWebSocketBridge(
			logger,
			gatewayConn,
			backendURL,
			service,
			handshakeSupplier,
			100,
			processor,
			publisher,
			responseSigner,
			http.Header{},
			nil,
			pipeline,
			1,
			time.Second,
		)
		bridgeReady <- struct {
			bridge *WebSocketBridge
			err    error
		}{bridge: bridge, err: bridgeErr}
		if bridgeErr == nil {
			bridge.Run()
		}
	}))
	wsURL := "ws" + strings.TrimPrefix(gatewayServer.URL, "http")
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	started := <-bridgeReady
	require.NoError(t, started.err)
	require.NotNil(t, started.bridge)

	return &websocketIdentityFixture{
		conn:        conn,
		bridge:      started.bridge,
		backendHits: backendHits,
		validator:   validator,
		processor:   processor,
		publisher:   publisher,
		meter:       meter,
		miniRedis:   miniRedis,
		supplier:    supplier,
	}
}

// websocketIdentityHTTPServer waits for handlers after closing the test server.
// WebSocket handlers are hijacked by net/http and are not included in the
// standard httptest.Server shutdown wait.
func websocketIdentityHTTPServer(t *testing.T, handler http.Handler) *httptest.Server {
	t.Helper()
	var handlers sync.WaitGroup
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handlers.Add(1)
		defer handlers.Done()
		handler.ServeHTTP(w, r)
	}))
	t.Cleanup(func() {
		server.Close()
		done := make(chan struct{})
		go func() {
			handlers.Wait()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("WebSocket test server handler did not return after shutdown")
		}
	})
	return server
}

func (f *websocketIdentityFixture) relay(sessionID, supplier, service, application string, start, end int64) *servicetypes.RelayRequest {
	return &servicetypes.RelayRequest{
		Payload: []byte("request-payload"),
		Meta: servicetypes.RelayRequestMetadata{
			SessionHeader: &sessiontypes.SessionHeader{
				ApplicationAddress:      application,
				ServiceId:               service,
				SessionId:               sessionID,
				SessionStartBlockHeight: start,
				SessionEndBlockHeight:   end,
			},
			SupplierOperatorAddress: supplier,
		},
	}
}

func (f *websocketIdentityFixture) serveRelay(t *testing.T, request *servicetypes.RelayRequest) {
	t.Helper()
	requestBytes, err := request.Marshal()
	require.NoError(t, err)
	require.NoError(t, f.conn.WriteMessage(websocket.BinaryMessage, requestBytes))
	require.NoError(t, f.conn.SetReadDeadline(time.Now().Add(3*time.Second)))
	_, responseBytes, err := f.conn.ReadMessage()
	require.NoError(t, err, "the relay was not returned through the WebSocket")
	response := &servicetypes.RelayResponse{}
	require.NoError(t, response.Unmarshal(responseBytes))
	require.Equal(t, request.Meta.SessionHeader.SessionId, response.Meta.SessionHeader.SessionId)
	select {
	case call := <-f.processor.calls:
		require.Equal(t, request.Meta.SupplierOperatorAddress, call.supplier)
		require.Equal(t, request.Meta.SessionHeader.ServiceId, call.service)
		require.Equal(t, request.Meta.SessionHeader.SessionId, call.session)
	case <-time.After(3 * time.Second):
		t.Fatal("relay processor was not called for the served frame")
	}
	select {
	case <-f.publisher.calls:
	case <-time.After(3 * time.Second):
		t.Fatal("relay was not published after the backend response")
	}
}

func requireWebSocketValidationClose(t *testing.T, conn *websocket.Conn, description string) {
	t.Helper()
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(3*time.Second)))
	_, data, err := conn.ReadMessage()
	require.Error(t, err, "%s must close the connection; unexpectedly received %q", description, string(data))
	require.True(t, websocket.IsCloseError(err, CloseValidationFailed),
		"%s must close with CloseValidationFailed, got %v", description, err)
}

func rejectionCount(service, reason string) float64 {
	return testutil.ToFloat64(relaysRejected.WithLabelValues(service, "websocket", reason))
}

func TestWebSocketIdentityRejectsServiceChangeBeforeDownstream(t *testing.T) {
	service := "ws-identity-service-first"
	f := newWebSocketIdentityFixture(t, service, false, websocketIdentityTestApp)
	supplierA := f.supplier
	before := rejectionCount(service, rejectReasonServiceChanged)
	request := f.relay("session-service-mismatch", supplierA, "other-service", websocketIdentityTestApp, 101, 110)
	requestBytes, err := request.Marshal()
	require.NoError(t, err)
	require.NoError(t, f.conn.WriteMessage(websocket.BinaryMessage, requestBytes))
	requireWebSocketValidationClose(t, f.conn, "a first frame naming another service")

	require.Equal(t, int64(0), f.backendHits.Load(), "mismatched service frame was forwarded")
	require.Zero(t, f.validator.calls.Load(), "mismatched service frame reached validation/metering")
	require.Empty(t, f.processor.calls, "mismatched service frame reached mining")
	require.Empty(t, f.publisher.calls, "mismatched service frame reached publication")
	require.Zero(t, f.bridge.sessionEndHeight, "mismatched frame pinned session state")
	require.False(t, f.miniRedis.Exists(f.meter.consumedKey("session-service-mismatch", supplierA)),
		"mismatched service frame changed the metering budget")
	require.Equal(t, before+1, rejectionCount(service, rejectReasonServiceChanged),
		"service rejection uses a bounded identity reason")
}

func TestWebSocketIdentityRejectsServiceChangeOnLaterFrame(t *testing.T) {
	service := "ws-identity-service-later"
	f := newWebSocketIdentityFixture(t, service, false, websocketIdentityTestApp)
	f.serveRelay(t, f.relay("session-service-valid", f.supplier, service, websocketIdentityTestApp, 101, 110))
	beforeBackend := f.backendHits.Load()
	beforeValidation := f.validator.calls.Load()
	beforeProcessor := f.processor.total.Load()
	beforePublished := f.publisher.total.Load()
	beforeReject := rejectionCount(service, rejectReasonServiceChanged)

	request := f.relay("session-service-changed", f.supplier, "other-service", websocketIdentityTestApp, 111, 120)
	requestBytes, err := request.Marshal()
	require.NoError(t, err)
	require.NoError(t, f.conn.WriteMessage(websocket.BinaryMessage, requestBytes))
	requireWebSocketValidationClose(t, f.conn, "a later frame naming another service")

	require.Equal(t, beforeBackend, f.backendHits.Load(), "mismatched frame reached the backend")
	require.Equal(t, beforeValidation, f.validator.calls.Load(), "mismatched frame reached validation/metering")
	require.Equal(t, beforeProcessor, f.processor.total.Load(), "mismatched frame reached mining")
	require.Equal(t, beforePublished, f.publisher.total.Load(), "mismatched frame reached publication")
	require.False(t, f.miniRedis.Exists(f.meter.consumedKey("session-service-changed", f.supplier)),
		"mismatched service frame changed the metering budget")
	require.Equal(t, beforeReject+1, rejectionCount(service, rejectReasonServiceChanged))
}

func TestWebSocketIdentityRejectsApplicationChangeBeforeDownstream(t *testing.T) {
	service := "ws-identity-app"
	f := newWebSocketIdentityFixture(t, service, false, websocketIdentityTestApp)
	f.serveRelay(t, f.relay("session-app-valid", f.supplier, service, websocketIdentityTestApp, 101, 110))
	beforeBackend := f.backendHits.Load()
	beforeValidation := f.validator.calls.Load()
	beforeProcessor := f.processor.total.Load()
	beforePublished := f.publisher.total.Load()
	beforeReject := rejectionCount(service, rejectReasonApplicationChanged)

	request := f.relay("session-app-changed", f.supplier, service, "pokt1otherapplication", 111, 120)
	requestBytes, err := request.Marshal()
	require.NoError(t, err)
	require.NoError(t, f.conn.WriteMessage(websocket.BinaryMessage, requestBytes))
	requireWebSocketValidationClose(t, f.conn, "a later frame naming another application")

	require.Equal(t, beforeBackend, f.backendHits.Load(), "mismatched frame reached the backend")
	require.Equal(t, beforeValidation, f.validator.calls.Load(), "mismatched frame reached validation/metering")
	require.Equal(t, beforeProcessor, f.processor.total.Load(), "mismatched frame reached mining")
	require.Equal(t, beforePublished, f.publisher.total.Load(), "mismatched frame reached publication")
	require.False(t, f.miniRedis.Exists(f.meter.consumedKey("session-app-changed", f.supplier)),
		"mismatched application frame changed the metering budget")
	require.Equal(t, beforeReject+1, rejectionCount(service, rejectReasonApplicationChanged))
}

func TestWebSocketIdentityRejectsSupplierChangeBeforeDownstream(t *testing.T) {
	service := "ws-identity-supplier"
	f := newWebSocketIdentityFixture(t, service, false, websocketIdentityTestApp)
	f.serveRelay(t, f.relay("session-supplier-valid", f.supplier, service, websocketIdentityTestApp, 101, 110))
	beforeBackend := f.backendHits.Load()
	beforeValidation := f.validator.calls.Load()
	beforeProcessor := f.processor.total.Load()
	beforePublished := f.publisher.total.Load()
	beforeReject := rejectionCount(service, rejectReasonSupplierChanged)

	otherKey := secp256k1.GenPrivKey()
	otherSupplier := cosmostypes.AccAddress(otherKey.PubKey().Address()).String()
	request := f.relay("session-supplier-changed", otherSupplier, service, websocketIdentityTestApp, 111, 120)
	requestBytes, err := request.Marshal()
	require.NoError(t, err)
	require.NoError(t, f.conn.WriteMessage(websocket.BinaryMessage, requestBytes))
	requireWebSocketValidationClose(t, f.conn, "a later frame naming another supplier")

	require.Equal(t, beforeBackend, f.backendHits.Load(), "mismatched supplier frame reached the backend")
	require.Equal(t, beforeValidation, f.validator.calls.Load(), "mismatched frame reached validation/metering")
	require.Equal(t, beforeProcessor, f.processor.total.Load(), "mismatched supplier frame reached mining")
	require.Equal(t, beforePublished, f.publisher.total.Load(), "mismatched supplier frame reached publication")
	require.False(t, f.miniRedis.Exists(f.meter.consumedKey("session-supplier-changed", otherSupplier)),
		"mismatched supplier frame changed the metering budget")
	require.Equal(t, beforeReject+1, rejectionCount(service, rejectReasonSupplierChanged))
}

func TestWebSocketIdentityRejectsFrameSupplierDifferentFromHandshake(t *testing.T) {
	service := "ws-identity-handshake-supplier"
	f := newWebSocketIdentityFixture(t, service, true, websocketIdentityTestApp)
	otherKey := secp256k1.GenPrivKey()
	otherSupplier := cosmostypes.AccAddress(otherKey.PubKey().Address()).String()
	before := rejectionCount(service, rejectReasonSupplierChanged)

	request := f.relay("session-handshake-supplier", otherSupplier, service, websocketIdentityTestApp, 101, 110)
	requestBytes, err := request.Marshal()
	require.NoError(t, err)
	require.NoError(t, f.conn.WriteMessage(websocket.BinaryMessage, requestBytes))
	requireWebSocketValidationClose(t, f.conn, "a frame naming a supplier other than the handshake supplier")

	require.Zero(t, f.backendHits.Load())
	require.Zero(t, f.validator.calls.Load())
	require.Empty(t, f.processor.calls)
	require.Empty(t, f.publisher.calls)
	require.False(t, f.miniRedis.Exists(f.meter.consumedKey("session-handshake-supplier", otherSupplier)))
	require.Equal(t, before+1, rejectionCount(service, rejectReasonSupplierChanged))
}

func TestWebSocketIdentityAllowsSessionRolloverForPinnedIdentity(t *testing.T) {
	service := "ws-identity-rollover"
	f := newWebSocketIdentityFixture(t, service, false, websocketIdentityTestApp)
	first := f.relay("session-rollover-one", f.supplier, service, websocketIdentityTestApp, 101, 110)
	second := f.relay("session-rollover-two", f.supplier, service, websocketIdentityTestApp, 111, 120)
	beforeRejected := rejectionCount(service, rejectReasonSupplierChanged) +
		rejectionCount(service, rejectReasonServiceChanged) + rejectionCount(service, rejectReasonApplicationChanged)

	f.serveRelay(t, first)
	f.serveRelay(t, second)

	require.Equal(t, int64(2), f.backendHits.Load(), "both same-identity sessions must reach the backend")
	require.Equal(t, int64(2), f.validator.calls.Load())
	require.Equal(t, int64(2), f.processor.total.Load())
	require.Equal(t, int64(2), f.publisher.total.Load())
	afterRejected := rejectionCount(service, rejectReasonSupplierChanged) +
		rejectionCount(service, rejectReasonServiceChanged) + rejectionCount(service, rejectReasonApplicationChanged)
	require.Equal(t, beforeRejected, afterRejected, "a session ID rollover is not an identity change")
}

func TestWebSocketIdentityPinsAnEmptyFirstApplication(t *testing.T) {
	service := "ws-identity-empty-application"
	f := newWebSocketIdentityFixture(t, service, false, "")
	f.serveRelay(t, f.relay("session-empty-app-one", f.supplier, service, "", 101, 110))
	beforeBackend := f.backendHits.Load()
	beforeValidation := f.validator.calls.Load()
	beforeReject := rejectionCount(service, rejectReasonApplicationChanged)

	request := f.relay("session-empty-app-two", f.supplier, service, websocketIdentityTestApp, 111, 120)
	requestBytes, err := request.Marshal()
	require.NoError(t, err)
	require.NoError(t, f.conn.WriteMessage(websocket.BinaryMessage, requestBytes))
	requireWebSocketValidationClose(t, f.conn, "an application change after an empty application was pinned")

	require.Equal(t, beforeBackend, f.backendHits.Load())
	require.Equal(t, beforeValidation, f.validator.calls.Load())
	require.False(t, f.miniRedis.Exists(f.meter.consumedKey("session-empty-app-two", f.supplier)))
	require.Equal(t, beforeReject+1, rejectionCount(service, rejectReasonApplicationChanged))
}

func TestWebSocketIdentityRejectsMissingSupplier(t *testing.T) {
	service := "ws-identity-missing-supplier"
	f := newWebSocketIdentityFixture(t, service, false, websocketIdentityTestApp)
	before := rejectionCount(service, rejectReasonMissingSupplierAddress)
	request := f.relay("session-missing-supplier", "", service, websocketIdentityTestApp, 101, 110)
	requestBytes, err := request.Marshal()
	require.NoError(t, err)
	require.NoError(t, f.conn.WriteMessage(websocket.BinaryMessage, requestBytes))
	requireWebSocketValidationClose(t, f.conn, "a frame with no supplier identity")

	require.Zero(t, f.backendHits.Load())
	require.Zero(t, f.validator.calls.Load())
	require.Zero(t, f.processor.total.Load())
	require.Zero(t, f.publisher.total.Load())
	require.Equal(t, before+1, rejectionCount(service, rejectReasonMissingSupplierAddress))
}

func TestWebSocketIdentityRejectsMissingSessionHeader(t *testing.T) {
	service := "ws-identity-missing-session"
	f := newWebSocketIdentityFixture(t, service, false, websocketIdentityTestApp)
	before := rejectionCount(service, rejectReasonInvalidRelayRequest)
	request := &servicetypes.RelayRequest{
		Meta: servicetypes.RelayRequestMetadata{SupplierOperatorAddress: f.supplier},
	}
	requestBytes, err := request.Marshal()
	require.NoError(t, err)
	require.NoError(t, f.conn.WriteMessage(websocket.BinaryMessage, requestBytes))
	requireWebSocketValidationClose(t, f.conn, "a frame with no session header")

	require.Zero(t, f.backendHits.Load())
	require.Zero(t, f.validator.calls.Load())
	require.Zero(t, f.processor.total.Load())
	require.Zero(t, f.publisher.total.Load())
	require.Equal(t, before+1, rejectionCount(service, rejectReasonInvalidRelayRequest))
}
