//go:build test

package relayer

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/cosmos/cosmos-sdk/crypto/keys/secp256k1"
	cryptotypes "github.com/cosmos/cosmos-sdk/crypto/types"
	"github.com/pokt-network/shannon-sdk/types"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	servicetypes "github.com/pokt-network/poktroll/x/service/types"
	sessiontypes "github.com/pokt-network/poktroll/x/session/types"

	"github.com/pokt-network/pocket-relay-miner/pool"
	transporttypes "github.com/pokt-network/pocket-relay-miner/transport"
)

const testGRPCSupplier = "pokt1supplier"

type recordingProcessor struct {
	mu             sync.Mutex
	calls          int
	lastReqBody    []byte
	lastRespBody   []byte
	lastContextErr error
	hasDeadline    bool
	remaining      time.Duration
	message        *transporttypes.MinedRelayMessage
	err            error
}

func (p *recordingProcessor) ProcessRelay(ctx context.Context, reqBody, respBody []byte, _ string, _ string, _ int64) (*transporttypes.MinedRelayMessage, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls++
	p.lastReqBody = append([]byte(nil), reqBody...)
	p.lastRespBody = append([]byte(nil), respBody...)
	p.lastContextErr = ctx.Err()
	if deadline, ok := ctx.Deadline(); ok {
		p.hasDeadline = true
		p.remaining = time.Until(deadline)
	}
	return p.message, p.err
}

func (p *recordingProcessor) GetServiceDifficulty(context.Context, string, int64) ([]byte, error) {
	return nil, nil
}

func (p *recordingProcessor) SetDifficultyProvider(DifficultyProvider) {}

func (p *recordingProcessor) snapshot() (int, []byte, []byte, error, bool, time.Duration) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.calls, append([]byte(nil), p.lastReqBody...), append([]byte(nil), p.lastRespBody...), p.lastContextErr, p.hasDeadline, p.remaining
}

type recordingPublisher struct {
	mu             sync.Mutex
	calls          int
	lastMessage    *transporttypes.MinedRelayMessage
	lastContextErr error
	hasDeadline    bool
	remaining      time.Duration
	publishError   error
}

func (p *recordingPublisher) Publish(ctx context.Context, message *transporttypes.MinedRelayMessage) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls++
	p.lastMessage = message
	p.lastContextErr = ctx.Err()
	if deadline, ok := ctx.Deadline(); ok {
		p.hasDeadline = true
		p.remaining = time.Until(deadline)
	}
	return p.publishError
}

func (p *recordingPublisher) PublishBatch(context.Context, []*transporttypes.MinedRelayMessage) error {
	return nil
}

func (p *recordingPublisher) Close() error { return nil }

func (p *recordingPublisher) snapshot() (int, *transporttypes.MinedRelayMessage, error, bool, time.Duration) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.calls, p.lastMessage, p.lastContextErr, p.hasDeadline, p.remaining
}

type mockServerStream struct {
	ctx     context.Context
	request *servicetypes.RelayRequest
	sent    []interface{}
	onSend  func()
	mu      sync.Mutex
}

func (s *mockServerStream) SetHeader(metadata.MD) error  { return nil }
func (s *mockServerStream) SendHeader(metadata.MD) error { return nil }
func (s *mockServerStream) SetTrailer(metadata.MD)       {}
func (s *mockServerStream) Context() context.Context     { return s.ctx }
func (s *mockServerStream) SendMsg(message interface{}) error {
	s.mu.Lock()
	s.sent = append(s.sent, message)
	onSend := s.onSend
	s.mu.Unlock()
	if onSend != nil {
		onSend()
	}
	return nil
}
func (s *mockServerStream) RecvMsg(message interface{}) error {
	request, ok := message.(*servicetypes.RelayRequest)
	if !ok {
		return errors.New("unexpected receive type")
	}
	*request = *s.request
	return nil
}

func newGRPCTestFixture(t *testing.T, statusCode int, body string, processor *recordingProcessor, publisher *recordingPublisher) (*RelayGRPCService, *mockServerStream) {
	return newGRPCTestFixtureWithDelay(t, statusCode, body, 0, processor, publisher)
}

func newGRPCTestFixtureWithDelay(t *testing.T, statusCode int, body string, delay time.Duration, processor *recordingProcessor, publisher *recordingPublisher) (*RelayGRPCService, *mockServerStream) {
	t.Helper()
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if delay > 0 {
			time.Sleep(delay)
		}
		w.WriteHeader(statusCode)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(backend.Close)

	endpoint, err := pool.NewBackendEndpoint("test", backend.URL)
	require.NoError(t, err)
	backendPool := pool.NewPool("svc:jsonrpc", []*pool.BackendEndpoint{endpoint}, &pool.FirstHealthySelector{}, "first_healthy(test)")
	privateKey := secp256k1.GenPrivKey()
	signer, err := NewResponseSigner(testLogger(), map[string]cryptotypes.PrivKey{testGRPCSupplier: privateKey})
	require.NoError(t, err)

	req, reqBz, err := types.SerializeHTTPRequest(&http.Request{
		Method: http.MethodPost,
		URL:    mustParseURL(t, "http://relay/"),
		Header: http.Header{"Content-Type": []string{"application/json"}},
		Body:   http.NoBody,
	})
	require.NoError(t, err)
	require.NotNil(t, req)

	relayRequest := &servicetypes.RelayRequest{
		Meta: servicetypes.RelayRequestMetadata{
			SessionHeader: &sessiontypes.SessionHeader{
				ApplicationAddress:      "pokt1application",
				ServiceId:               "svc",
				SessionId:               "session",
				SessionStartBlockHeight: 1,
				SessionEndBlockHeight:   10,
			},
			SupplierOperatorAddress: testGRPCSupplier,
		},
		Payload: reqBz,
	}
	_ = req

	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("rpc-type", "3"))
	stream := &mockServerStream{ctx: ctx, request: relayRequest}
	serviceConfig := RelayGRPCServiceConfig{
		ServiceConfigs: map[string]ServiceConfig{
			"svc": {Backends: map[string]BackendConfig{BackendTypeJSONRPC: {URL: backend.URL}}},
		},
		ResponseSigner: signer,
		RelayProcessor: processor,
		GetPool: func(string, string) *pool.Pool {
			return backendPool
		},
	}
	if publisher != nil {
		serviceConfig.Publisher = publisher
	}
	service := NewRelayGRPCService(testLogger(), serviceConfig)
	return service, stream
}

func mustParseURL(t *testing.T, raw string) *url.URL {
	t.Helper()
	parsed, err := url.Parse(raw)
	require.NoError(t, err)
	return parsed
}

func TestHandleSendRelay_TransportErrorNotPublished(t *testing.T) {
	processor := &recordingProcessor{message: &transporttypes.MinedRelayMessage{}}
	publisher := &recordingPublisher{}
	service, stream := newGRPCTestFixture(t, http.StatusOK, "ok", processor, publisher)
	endpoint, err := pool.NewBackendEndpoint("dead", "http://127.0.0.1:1")
	require.NoError(t, err)
	deadPool := pool.NewPool("svc:jsonrpc", []*pool.BackendEndpoint{endpoint}, &pool.FirstHealthySelector{}, "first_healthy(test)")
	service.getPool = func(string, string) *pool.Pool { return deadPool }

	require.NoError(t, service.handleSendRelay(stream))
	calls, _, _, _, _, _ := processor.snapshot()
	publishCalls, _, _, _, _ := publisher.snapshot()
	require.Zero(t, calls)
	require.Zero(t, publishCalls)
	require.Len(t, stream.sent, 1)
}

func TestHandleSendRelay_SuccessMinesRawBackendBody(t *testing.T) {
	const body = `{"jsonrpc":"2.0","result":"0x10","id":1}`
	processor := &recordingProcessor{message: &transporttypes.MinedRelayMessage{ServiceId: "svc"}}
	publisher := &recordingPublisher{}
	service, stream := newGRPCTestFixture(t, http.StatusOK, body, processor, publisher)

	require.NoError(t, service.handleSendRelay(stream))
	calls, reqBody, respBody, contextErr, _, _ := processor.snapshot()
	publishCalls, _, _, _, _ := publisher.snapshot()
	require.Equal(t, 1, calls)
	require.Equal(t, body, string(respBody))
	require.NoError(t, contextErr)
	require.Equal(t, 1, publishCalls)
	decoded := &servicetypes.RelayRequest{}
	require.NoError(t, decoded.Unmarshal(reqBody))
	require.Equal(t, testGRPCSupplier, decoded.Meta.SupplierOperatorAddress)
}

func TestHandleSendRelay_Backend4xxWithBodyIsPublished(t *testing.T) {
	const body = `{"error":"bad request"}`
	processor := &recordingProcessor{message: &transporttypes.MinedRelayMessage{ServiceId: "svc"}}
	publisher := &recordingPublisher{}
	service, stream := newGRPCTestFixture(t, http.StatusBadRequest, body, processor, publisher)

	require.NoError(t, service.handleSendRelay(stream))
	_, _, respBody, _, _, _ := processor.snapshot()
	publishCalls, _, _, _, _ := publisher.snapshot()
	require.Equal(t, body, string(respBody))
	require.Equal(t, 1, publishCalls)
}

func TestHandleSendRelay_Backend5xxNotPublished(t *testing.T) {
	processor := &recordingProcessor{message: &transporttypes.MinedRelayMessage{}}
	publisher := &recordingPublisher{}
	service, stream := newGRPCTestFixture(t, http.StatusInternalServerError, "failure", processor, publisher)

	err := service.handleSendRelay(stream)
	require.Error(t, err)
	require.Equal(t, codes.Unavailable, status.Code(err))
	calls, _, _, _, _, _ := processor.snapshot()
	publishCalls, _, _, _, _ := publisher.snapshot()
	require.Zero(t, calls)
	require.Zero(t, publishCalls)
}

func TestHandleSendRelay_NotApplicableNotPublished(t *testing.T) {
	processor := &recordingProcessor{}
	publisher := &recordingPublisher{}
	service, stream := newGRPCTestFixture(t, http.StatusOK, "ok", processor, publisher)

	require.NoError(t, service.handleSendRelay(stream))
	calls, _, _, _, _, _ := processor.snapshot()
	publishCalls, _, _, _, _ := publisher.snapshot()
	require.Equal(t, 1, calls)
	require.Zero(t, publishCalls)
}

func TestHandleSendRelay_NilPublisherDoesNotPanic(t *testing.T) {
	processor := &recordingProcessor{message: &transporttypes.MinedRelayMessage{}}
	service, stream := newGRPCTestFixture(t, http.StatusOK, "ok", processor, nil)

	require.NoError(t, service.handleSendRelay(stream))
	calls, _, _, _, _, _ := processor.snapshot()
	require.Equal(t, 1, calls)
}

func TestHandleSendRelay_PublishFailureDoesNotChangeServedResponse(t *testing.T) {
	processor := &recordingProcessor{message: &transporttypes.MinedRelayMessage{}}
	publisher := &recordingPublisher{publishError: errors.New("publisher unavailable")}
	service, stream := newGRPCTestFixture(t, http.StatusOK, "ok", processor, publisher)

	require.NoError(t, service.handleSendRelay(stream))
	publishCalls, _, _, _, _ := publisher.snapshot()
	require.Equal(t, 1, publishCalls)
	require.Len(t, stream.sent, 1)
}

func TestHandleSendRelay_PublishBudgetFreshAfterSlowBackend(t *testing.T) {
	processor := &recordingProcessor{message: &transporttypes.MinedRelayMessage{}}
	publisher := &recordingPublisher{}
	service, stream := newGRPCTestFixtureWithDelay(t, http.StatusOK, "ok", 300*time.Millisecond, processor, publisher)

	require.NoError(t, service.handleSendRelay(stream))
	_, _, _, _, hasDeadline, remaining := processor.snapshot()
	require.True(t, hasDeadline)
	require.Greater(t, remaining, grpcPublishTimeout-100*time.Millisecond)
}

func TestHandleSendRelay_ClientCancelAfterResponseStillPublishes(t *testing.T) {
	processor := &recordingProcessor{message: &transporttypes.MinedRelayMessage{}}
	publisher := &recordingPublisher{}
	service, stream := newGRPCTestFixture(t, http.StatusOK, "ok", processor, publisher)
	parentCtx, cancel := context.WithCancel(stream.ctx)
	stream.ctx = parentCtx
	stream.onSend = cancel

	require.NoError(t, service.handleSendRelay(stream))
	_, _, _, processorContextErr, _, _ := processor.snapshot()
	publishCalls, _, publisherContextErr, _, _ := publisher.snapshot()
	require.NoError(t, processorContextErr)
	require.Equal(t, 1, publishCalls)
	require.NoError(t, publisherContextErr)
}

func TestHandleSendRelay_PublishContextHasDeadline(t *testing.T) {
	processor := &recordingProcessor{message: &transporttypes.MinedRelayMessage{}}
	publisher := &recordingPublisher{}
	service, stream := newGRPCTestFixture(t, http.StatusOK, "ok", processor, publisher)

	require.NoError(t, service.handleSendRelay(stream))
	_, _, _, _, processorHasDeadline, _ := processor.snapshot()
	_, _, _, publisherHasDeadline, _ := publisher.snapshot()
	require.True(t, processorHasDeadline)
	require.True(t, publisherHasDeadline)
}

var _ grpc.ServerStream = (*mockServerStream)(nil)
