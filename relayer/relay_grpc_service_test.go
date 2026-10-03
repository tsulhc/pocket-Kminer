//go:build test

package relayer

import (
	"context"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	sdktypes "github.com/pokt-network/shannon-sdk/types"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
)

// grpcReqWithContentType builds a minimal POKTHTTPRequest carrying a single
// Content-Type header (or none when ct is empty), used to exercise the inner
// Content-Type heuristic in resolveGRPCRelayRPCType.
func grpcReqWithContentType(ct string) *sdktypes.POKTHTTPRequest {
	if ct == "" {
		return &sdktypes.POKTHTTPRequest{}
	}
	return &sdktypes.POKTHTTPRequest{
		Header: map[string]*sdktypes.Header{
			"Content-Type": {Key: "Content-Type", Values: []string{ct}},
		},
	}
}

// TestResolveGRPCRelayRPCType pins the precedence contract that fixes native
// gRPC relays: the client's declared "rpc-type" metadata wins over the inner
// Content-Type, then the service default, then the global default. This mirrors
// the HTTP path (proxy.go:719-728).
func TestResolveGRPCRelayRPCType(t *testing.T) {
	tests := []struct {
		name           string
		md             metadata.MD
		contentType    string
		defaultBackend string
		want           string
	}{
		{
			name: "metadata numeric code 1 maps to grpc",
			md:   metadata.Pairs("rpc-type", "1"),
			want: BackendTypeGRPC,
		},
		{
			name: "metadata name grpc stays grpc",
			md:   metadata.Pairs("rpc-type", "grpc"),
			want: BackendTypeGRPC,
		},
		{
			name: "metadata numeric code 4 maps to rest",
			md:   metadata.Pairs("rpc-type", "4"),
			want: BackendTypeREST,
		},
		{
			name:        "no metadata falls back to inner content-type grpc",
			md:          nil,
			contentType: "application/grpc+proto",
			want:        BackendTypeGRPC,
		},
		{
			name:           "no metadata json content-type uses service default",
			md:             nil,
			contentType:    "application/json",
			defaultBackend: BackendTypeGRPC,
			want:           BackendTypeGRPC,
		},
		{
			name:        "no metadata json content-type no default uses global default",
			md:          nil,
			contentType: "application/json",
			want:        DefaultBackendType,
		},
		{
			name: "empty metadata no content-type no default uses global default",
			md:   metadata.MD{},
			want: DefaultBackendType,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svcConfig := &ServiceConfig{DefaultBackend: tt.defaultBackend}
			got := resolveGRPCRelayRPCType(tt.md, grpcReqWithContentType(tt.contentType), svcConfig)
			require.Equal(t, tt.want, got)
		})
	}
}

// TestNormalizeBackendURLForRPCType pins that gRPC backends given as bare
// host:port become h2c-dialable http:// URLs, real http/https URLs are left
// alone, non-gRPC types are never rewritten, and an explicit non-http scheme is
// a hard error.
func TestNormalizeBackendURLForRPCType(t *testing.T) {
	tests := []struct {
		name    string
		url     string
		rpcType string
		want    string
		wantErr bool
	}{
		{
			name:    "grpc bare host:port gets http scheme",
			url:     "backend:50051",
			rpcType: BackendTypeGRPC,
			want:    "http://backend:50051",
		},
		{
			name:    "grpc http url unchanged",
			url:     "http://b:1",
			rpcType: BackendTypeGRPC,
			want:    "http://b:1",
		},
		{
			name:    "grpc https url unchanged",
			url:     "https://b",
			rpcType: BackendTypeGRPC,
			want:    "https://b",
		},
		{
			name:    "rest bare host:port unchanged (not grpc, not normalized)",
			url:     "backend:50051",
			rpcType: BackendTypeREST,
			want:    "backend:50051",
		},
		{
			name:    "grpc explicit non-http scheme errors",
			url:     "ftp://x",
			rpcType: BackendTypeGRPC,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := normalizeBackendURLForRPCType(tt.url, tt.rpcType)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

// newTestGRPCService builds a RelayGRPCService with only the dependencies that
// forwardToBackend touches (the h2c client, default HTTP client, and buffer
// pool are all created by the constructor). The remaining collaborators are
// nil because the forwarding path under test never reaches them.
func newTestGRPCService(t *testing.T) *RelayGRPCService {
	t.Helper()
	return NewRelayGRPCService(testLogger(), RelayGRPCServiceConfig{})
}

// TestForwardToBackend_MisconfigDoesNotHitNetwork proves a scheme-less backend
// for a non-gRPC relay is reported as errBackendMisconfigured before any dial,
// so it never reaches -- and never poisons -- the circuit breaker.
func TestForwardToBackend_MisconfigDoesNotHitNetwork(t *testing.T) {
	svc := newTestGRPCService(t)

	svcConfig := &ServiceConfig{
		Backends: map[string]BackendConfig{
			// Scheme-less URL: url.Parse reads "backend" as the scheme, which
			// http.NewRequest cannot dial. For a REST relay this is a config error.
			BackendTypeREST: {URL: "backend:50051"},
		},
	}
	poktReq := &sdktypes.POKTHTTPRequest{
		Method: http.MethodPost,
		Url:    "/v1",
	}

	respBody, respHeaders, respStatus, err := svc.forwardToBackend(
		context.Background(), "svc", svcConfig, poktReq, nil, BackendTypeREST,
	)

	require.Error(t, err)
	require.ErrorIs(t, err, errBackendMisconfigured)
	require.Nil(t, respBody)
	require.Nil(t, respHeaders)
	require.Zero(t, respStatus)
}

// TestForwardToBackend_GRPCBackendViaH2C is the end-to-end proof that a gRPC
// relay reaches a real gRPC backend over h2c: the backend, configured as a bare
// host:port, must be dialed as HTTP/2 cleartext with Content-Type
// application/grpc, and the grpc-status trailer must come back inside the
// returned headers so the gRPC client can interpret the response.
func TestForwardToBackend_GRPCBackendViaH2C(t *testing.T) {
	type observed struct {
		proto       string
		contentType string
	}
	obsCh := make(chan observed, 1)

	echoBody := []byte{0x00, 0x00, 0x00, 0x00, 0x05, 'h', 'e', 'l', 'l', 'o'}

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Report what the server negotiated so the assertions reflect the
		// server-side view, not just the client's. Channel send/receive gives a
		// happens-before edge, keeping this race-free under -race.
		obsCh <- observed{proto: r.Proto, contentType: r.Header.Get("Content-Type")}

		body, _ := io.ReadAll(r.Body)

		// Announce the trailer before writing the body, then set it after -- the
		// standard Go pattern for HTTP trailers (how gRPC carries grpc-status).
		w.Header().Set("Trailer", "Grpc-Status")
		w.Header().Set("Content-Type", "application/grpc")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(body)
		w.Header().Set("Grpc-Status", "0")
	})

	backendAddr := serveH2CBackend(t, handler)

	svc := newTestGRPCService(t)
	svcConfig := &ServiceConfig{
		Backends: map[string]BackendConfig{
			// Bare host:port, exactly how gRPC backends are configured in yaml.
			BackendTypeGRPC: {URL: backendAddr},
		},
	}
	poktReq := &sdktypes.POKTHTTPRequest{
		Method: http.MethodPost,
		Url:    "/pb.Demo/Echo",
		Header: map[string]*sdktypes.Header{
			"Content-Type": {Key: "Content-Type", Values: []string{"application/grpc"}},
		},
		BodyBz: echoBody,
	}

	respBody, respHeaders, respStatus, err := svc.forwardToBackend(
		context.Background(), "svc", svcConfig, poktReq, nil, BackendTypeGRPC,
	)
	require.NoError(t, err)

	obs := <-obsCh
	require.Equal(t, "HTTP/2.0", obs.proto, "backend must be dialed over HTTP/2 cleartext")
	require.Equal(t, "application/grpc", obs.contentType, "gRPC content-type must reach the backend")

	require.Equal(t, http.StatusOK, respStatus)
	require.Equal(t, echoBody, respBody)
	require.Equal(t, "0", respHeaders.Get("Grpc-Status"), "grpc-status trailer must be folded into returned headers")
}

// serveH2CBackend starts an HTTP server on an ephemeral port that speaks both
// HTTP/1.1 and HTTP/2 cleartext (h2c), mirroring the relayer listener, and
// returns its bare host:port address (no scheme) -- the form gRPC backends take
// in configuration. The server is shut down on test cleanup.
func serveH2CBackend(t *testing.T, handler http.Handler) string {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	protocols := new(http.Protocols)
	protocols.SetHTTP1(true)
	protocols.SetUnencryptedHTTP2(true)

	srv := &http.Server{
		Handler:           handler,
		Protocols:         protocols,
		ReadHeaderTimeout: 5 * time.Second,
	}

	serveErrCh := make(chan error, 1)
	go func() {
		serveErrCh <- srv.Serve(listener)
	}()

	t.Cleanup(func() {
		require.NoError(t, srv.Close())
		// Serve always returns a non-nil error; after Close it must be ErrServerClosed.
		require.ErrorIs(t, <-serveErrCh, http.ErrServerClosed)
	})

	return listener.Addr().String()
}
