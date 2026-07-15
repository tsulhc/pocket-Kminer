//go:build test

package relayer

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	sdktypes "github.com/pokt-network/shannon-sdk/types"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
	"google.golang.org/grpc/metadata"

	"github.com/pokt-network/pocket-relay-miner/pool"
)

func TestResolveGRPCRelayRPCType(t *testing.T) {
	tests := []struct {
		name        string
		metadata    metadata.MD
		contentType string
		defaultRPC  string
		want        string
	}{
		{name: "metadata numeric grpc", metadata: metadata.Pairs("rpc-type", "1"), want: BackendTypeGRPC},
		{name: "metadata named grpc", metadata: metadata.Pairs("rpc-type", "grpc"), want: BackendTypeGRPC},
		{name: "metadata rest wins", metadata: metadata.Pairs("rpc-type", "4"), contentType: "application/grpc", want: BackendTypeREST},
		{name: "content type grpc", contentType: "application/grpc", want: BackendTypeGRPC},
		{name: "default grpc", defaultRPC: BackendTypeGRPC, want: BackendTypeGRPC},
		{name: "global default", want: DefaultBackendType},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := &sdktypes.POKTHTTPRequest{}
			if tt.contentType != "" {
				req.Header = map[string]*sdktypes.Header{
					"Content-Type": {Values: []string{tt.contentType}},
				}
			}
			got := resolveGRPCRelayRPCType(tt.metadata, req, &ServiceConfig{DefaultBackend: tt.defaultRPC})
			require.Equal(t, tt.want, got)
		})
	}
}

func TestNormalizeBackendURLForRPCType(t *testing.T) {
	tests := []struct {
		name    string
		backend string
		rpcType string
		want    string
		wantErr bool
	}{
		{name: "grpc bare host", backend: "backend:50051", rpcType: BackendTypeGRPC, want: "http://backend:50051"},
		{name: "grpc http", backend: "http://backend:50051", rpcType: BackendTypeGRPC, want: "http://backend:50051"},
		{name: "grpc https", backend: "https://backend:50051", rpcType: BackendTypeGRPC, want: "https://backend:50051"},
		{name: "grpc unsupported scheme", backend: "ftp://backend:50051", rpcType: BackendTypeGRPC, wantErr: true},
		{name: "rest bare host misconfigured", backend: "backend:50051", rpcType: BackendTypeREST, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := normalizeBackendURLForRPCType(tt.backend, tt.rpcType)
			if tt.wantErr {
				require.Error(t, err)
				require.ErrorIs(t, err, errBackendMisconfigured)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestForwardToBackend_MisconfigDoesNotHitNetwork(t *testing.T) {
	var requests int
	client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		requests++
		return nil, context.Canceled
	})}
	svc := &RelayGRPCService{
		getHTTPClient: func(string) *http.Client { return client },
		bufferPool:    NewBufferPool(1024),
	}
	ep, err := pool.NewBackendEndpoint("bare", "backend:50051")
	require.NoError(t, err)

	_, _, _, err = svc.forwardToBackend(
		context.Background(),
		"svc",
		&ServiceConfig{Backends: map[string]BackendConfig{BackendTypeREST: {URL: "backend:50051"}}},
		&sdktypes.POKTHTTPRequest{Method: http.MethodPost, Url: "http://relay/", BodyBz: []byte("x")},
		ep,
		BackendTypeREST,
	)
	require.ErrorIs(t, err, errBackendMisconfigured)
	require.Zero(t, requests)
}

func TestForwardToBackend_GRPCBackendViaH2C(t *testing.T) {
	server := httptest.NewUnstartedServer(h2c.NewHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "HTTP/2.0", r.Proto)
		w.Header().Set("Content-Type", "application/grpc")
		w.Header().Set("Grpc-Status", "0")
		_, _ = w.Write([]byte("grpc-body"))
	}), &http2.Server{}))
	server.Start()
	t.Cleanup(server.Close)

	svc := NewRelayGRPCService(testLogger(), RelayGRPCServiceConfig{
		ServiceConfigs: map[string]ServiceConfig{},
		MaxBodySize:    1024,
	})
	ep, err := pool.NewBackendEndpoint("h2c", server.URL)
	require.NoError(t, err)

	body, headers, statusCode, err := svc.forwardToBackend(
		context.Background(),
		"svc",
		&ServiceConfig{Backends: map[string]BackendConfig{BackendTypeGRPC: {URL: server.URL}}},
		&sdktypes.POKTHTTPRequest{Method: http.MethodPost, Url: "http://relay/", BodyBz: []byte("request")},
		ep,
		BackendTypeGRPC,
	)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, statusCode)
	require.Equal(t, []byte("grpc-body"), body)
	require.Equal(t, "0", headers.Get("Grpc-Status"))
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}
