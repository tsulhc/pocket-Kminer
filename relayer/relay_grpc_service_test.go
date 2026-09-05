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
		"",
	)
	require.ErrorIs(t, err, errBackendMisconfigured)
	require.Zero(t, requests)
}

func TestForwardToBackend_GRPCBackendViaH2C(t *testing.T) {
	type observation struct {
		proto       string
		contentType string
		requestID   string
	}
	observed := make(chan observation, 1)
	server := httptest.NewUnstartedServer(h2c.NewHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		observed <- observation{
			proto:       r.Proto,
			contentType: r.Header.Get("Content-Type"),
			requestID:   r.Header.Get(HeaderPocketRequestID),
		}
		w.Header().Set("Trailer", "Grpc-Status")
		w.Header().Set("Content-Type", "application/grpc")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("grpc-body"))
		w.Header().Set("Grpc-Status", "0")
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
		&sdktypes.POKTHTTPRequest{
			Method: http.MethodPost,
			Url:    "http://relay/",
			Header: map[string]*sdktypes.Header{"Content-Type": {Values: []string{"application/grpc"}}},
			BodyBz: []byte("request"),
		},
		ep,
		BackendTypeGRPC,
		"pocket-req-test",
	)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, statusCode)
	require.Equal(t, []byte("grpc-body"), body)
	require.Equal(t, "0", headers.Get("Grpc-Status"))
	select {
	case got := <-observed:
		require.Equal(t, "HTTP/2.0", got.proto)
		require.Equal(t, "application/grpc", got.contentType)
		require.Equal(t, "pocket-req-test", got.requestID)
	default:
		t.Fatal("backend observation was not recorded")
	}
}

func TestMergeTrailersIntoHeader_NilHeader(t *testing.T) {
	merged := mergeTrailersIntoHeader(nil, http.Header{"Grpc-Status": []string{"0"}})
	require.Equal(t, "0", merged.Get("Grpc-Status"))
}

func TestForwardToBackend_FallbackPoolUsesFallbackBackendConfig(t *testing.T) {
	var requests int
	var receivedHeader string
	var receivedAuth string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		receivedHeader = r.Header.Get("X-Fallback")
		receivedAuth = r.Header.Get("Authorization")
		_, _ = w.Write([]byte("ok"))
	}))
	t.Cleanup(server.Close)

	endpoint, err := pool.NewBackendEndpoint("fallback", server.URL)
	require.NoError(t, err)
	service := &RelayGRPCService{
		bufferPool:    NewBufferPool(1024),
		getHTTPClient: func(string) *http.Client { return http.DefaultClient },
	}

	body, _, _, err := service.forwardToBackend(
		context.Background(),
		"svc",
		&ServiceConfig{Backends: map[string]BackendConfig{
			BackendTypeJSONRPC: {
				Headers:        map[string]string{"X-Fallback": "selected"},
				Authentication: &AuthenticationConfig{BearerToken: "fallback-token"},
			},
		}},
		&sdktypes.POKTHTTPRequest{Method: http.MethodPost, Url: "http://relay/", BodyBz: []byte("request")},
		endpoint,
		BackendTypeREST,
		"",
	)
	require.NoError(t, err)
	require.Equal(t, []byte("ok"), body)
	require.Equal(t, 1, requests)
	require.Equal(t, "selected", receivedHeader)
	require.Equal(t, "Bearer fallback-token", receivedAuth)
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}
