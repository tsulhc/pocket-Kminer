//go:build test

package relayer

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func wsSchemeConfig(backendURL string) string {
	return `
listen_addr: "0.0.0.0:8080"
redis:
  url: "redis://localhost:6379"
pocket_node:
  query_node_rpc_url: "http://localhost:26657"
  query_node_grpc_url: "localhost:9090"
keys:
  supplier_keys_path: "/keys"
default_validation_mode: optimistic
services:
  eth:
    default_backend: websocket
    backends:
      websocket:
        url: "` + backendURL + `"
`
}

// TestValidate_WebSocketBackendRejectsHTTPScheme catches at config load the
// exact mistake that reached production: a websocket backend with an http://
// url. Without this it passes validation and fails at connect time with
// gorilla's "malformed ws or wss URL".
func TestValidate_WebSocketBackendRejectsHTTPScheme(t *testing.T) {
	for _, bad := range []string{
		"http://node:8546",
		"https://node:8546",
		"node:8546", // scheme-less
	} {
		cfg := loadFullConfig(t, wsSchemeConfig(bad))
		err := cfg.Validate()
		require.Error(t, err, "websocket backend url %q must be rejected", bad)
		require.Contains(t, err.Error(), "ws://", "error must name the required scheme for %q", bad)
	}
}

// TestValidate_WebSocketBackendAcceptsWSScheme pins the boundary: ws:// and
// wss:// must pass.
func TestValidate_WebSocketBackendAcceptsWSScheme(t *testing.T) {
	for _, good := range []string{
		"ws://node:8546",
		"wss://node:8546/ws",
	} {
		cfg := loadFullConfig(t, wsSchemeConfig(good))
		require.NoError(t, cfg.Validate(), "websocket backend url %q must be accepted", good)
	}
}
