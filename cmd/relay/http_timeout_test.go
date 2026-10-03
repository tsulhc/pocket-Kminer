package relay

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestApplyRelayTimeout proves the --timeout flag actually reaches the shared
// HTTP client used by the jsonrpc and cometbft modes. The client is constructed
// with a 30s default; without applyRelayTimeout that default silently caps every
// relay below --timeout, because http.Client.Timeout bounds the whole request
// regardless of the per-request context deadline.
func TestApplyRelayTimeout(t *testing.T) {
	origFlag := RelayTimeout
	origTimeout := sharedHTTPClient.Timeout
	t.Cleanup(func() {
		RelayTimeout = origFlag
		sharedHTTPClient.Timeout = origTimeout
	})

	t.Run("positive flag overrides the client default", func(t *testing.T) {
		sharedHTTPClient.Timeout = 30 * time.Second
		RelayTimeout = 120
		applyRelayTimeout()
		require.Equal(t, 120*time.Second, sharedHTTPClient.Timeout,
			"--timeout must drive the shared client timeout, not the 30s default")
	})

	t.Run("larger flag is honored", func(t *testing.T) {
		sharedHTTPClient.Timeout = 30 * time.Second
		RelayTimeout = 300
		applyRelayTimeout()
		require.Equal(t, 300*time.Second, sharedHTTPClient.Timeout)
	})

	t.Run("non-positive flag leaves the client untouched", func(t *testing.T) {
		sharedHTTPClient.Timeout = 45 * time.Second
		RelayTimeout = 0
		applyRelayTimeout()
		require.Equal(t, 45*time.Second, sharedHTTPClient.Timeout,
			"a zero/unset flag must not disable the client timeout entirely")
	})
}
