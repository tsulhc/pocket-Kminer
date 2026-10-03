package relay

import (
	"bytes"
	"encoding/json"
	"errors"
	"testing"

	sdktypes "github.com/pokt-network/shannon-sdk/types"
	"github.com/stretchr/testify/require"
)

// streamDelimiterBytes is the on-wire batch separator, resolved once for tests.
var streamDelimiterBytes = []byte(streamDelimiter)

// errBoom is a non-EOF reader failure used to exercise the mid-stream error path.
var errBoom = errors.New("boom: connection dropped")

// makeBatches builds n distinct binary batches. Each batch embeds its index and
// deliberately includes a newline plus non-UTF8 bytes to prove the reader hands
// back raw protobuf payloads untouched (no trimming, no re-encoding).
func makeBatches(n int) [][]byte {
	batches := make([][]byte, n)
	for i := range batches {
		batches[i] = []byte{'B', byte(i), '\n', 0x00, 0xFF, 0xFE, byte(i*31 + 7)}
	}
	return batches
}

// withTrailingDelimiters concatenates batches each followed by the delimiter,
// matching the relayer's wire format (every signed batch is suffixed with the
// delimiter).
func withTrailingDelimiters(batches [][]byte) []byte {
	var out []byte
	for _, b := range batches {
		out = append(out, b...)
		out = append(out, streamDelimiterBytes...)
	}
	return out
}

// errReader yields data on the first Read(s) and then returns err on the next
// Read. With data set to a complete, delimiter-suffixed batch stream it models a
// server that sends some batches and then drops the connection (or the client
// timeout firing).
type errReader struct {
	data []byte
	err  error
}

func (r *errReader) Read(p []byte) (int, error) {
	if len(r.data) > 0 {
		n := copy(p, r.data)
		r.data = r.data[n:]
		return n, nil
	}
	return 0, r.err
}

// TestReadStreamingBatches_ReadsAllUntilEOF proves the redesigned contract: the
// reader collects EVERY batch the server sends and returns when the server closes
// the stream (EOF), not at some client-imposed count. A finite stream of N
// batches yields exactly N, byte-for-byte.
func TestReadStreamingBatches_ReadsAllUntilEOF(t *testing.T) {
	batches := makeBatches(5)
	// bytes.Join produces "b0 <delim> b1 <delim> ... b4" (no trailing delimiter),
	// so b4 is the trailing token emitted at EOF.
	reader := bytes.NewReader(bytes.Join(batches, streamDelimiterBytes))

	got, err := readStreamingBatches(reader)

	require.NoError(t, err)
	require.Len(t, got, len(batches), "must collect every batch until server-close")
	for i := range batches {
		require.Equal(t, batches[i], got[i], "batch %d must be returned byte-exact", i)
	}
}

// TestReadStreamingBatches_TrailingDelimiterAtEOF proves the wire format where
// every batch (including the last) is delimiter-suffixed produces exactly N
// batches, not an extra empty trailing one.
func TestReadStreamingBatches_TrailingDelimiterAtEOF(t *testing.T) {
	batches := makeBatches(3)
	reader := bytes.NewReader(withTrailingDelimiters(batches))

	got, err := readStreamingBatches(reader)

	require.NoError(t, err)
	require.Len(t, got, len(batches), "trailing delimiter must not yield an empty batch")
	for i := range batches {
		require.Equal(t, batches[i], got[i])
	}
}

// TestReadStreamingBatches_PartialOnReaderError proves that batches collected
// before a mid-stream reader error are returned (with nil error), not discarded —
// this is the exact bug: the client timeout fired and thousands of received bytes
// were thrown away.
func TestReadStreamingBatches_PartialOnReaderError(t *testing.T) {
	batches := makeBatches(2)
	// Deliver both complete, delimiter-terminated batches, then fail.
	reader := &errReader{data: withTrailingDelimiters(batches), err: errBoom}

	got, err := readStreamingBatches(reader)

	require.NoError(t, err, "reader error after collecting batches must be swallowed")
	require.Len(t, got, 2)
	require.Equal(t, batches[0], got[0])
	require.Equal(t, batches[1], got[1])
}

// TestReadStreamingBatches_DropsTruncatedTailOnError proves the redesign's core
// promise ("a timeout never discards data already received"). The relayer suffixes
// a delimiter after every COMPLETE batch, so a timeout/reset landing mid-batch
// leaves a partial batch with no trailing delimiter on the wire. That partial must
// be dropped, not returned: if it survived, the caller's signature verification
// would fail on the truncated protobuf and abort the whole run, discarding every
// valid batch received before it.
func TestReadStreamingBatches_DropsTruncatedTailOnError(t *testing.T) {
	batches := makeBatches(2)
	// b0 complete (delimiter-terminated); b1 partial — NO trailing delimiter,
	// exactly what a mid-write timeout/reset leaves on the wire.
	var wire []byte
	wire = append(wire, batches[0]...)
	wire = append(wire, streamDelimiterBytes...)
	wire = append(wire, batches[1]...)
	reader := &errReader{data: wire, err: errBoom}

	got, err := readStreamingBatches(reader)

	require.NoError(t, err, "complete batches before the truncated tail must survive")
	require.Len(t, got, 1, "the truncated (non-delimiter-terminated) trailing batch must be dropped")
	require.Equal(t, batches[0], got[0])
}

// TestReadStreamingBatches_KeepsCompleteTailOnCleanEOF guards the inverse: a clean
// EOF (server closed gracefully) after a non-delimiter-terminated final batch is a
// COMPLETE batch and must be kept — only an error-terminated partial is dropped.
func TestReadStreamingBatches_KeepsCompleteTailOnCleanEOF(t *testing.T) {
	batches := makeBatches(3)
	// No trailing delimiter after the last batch, but a clean io.EOF (bytes.Reader).
	reader := bytes.NewReader(bytes.Join(batches, streamDelimiterBytes))

	got, err := readStreamingBatches(reader)

	require.NoError(t, err)
	require.Len(t, got, 3, "a complete final batch on clean EOF must be kept")
	require.Equal(t, batches[2], got[2])
}

// TestReadStreamingBatches_ErrorWithZeroBatches proves the error is surfaced only
// when nothing was collected. The reader errors before emitting any bytes: any
// leftover bytes at error would be emitted as a trailing-token batch, so a true
// zero-batch failure requires the reader to fail before the first byte.
func TestReadStreamingBatches_ErrorWithZeroBatches(t *testing.T) {
	reader := &errReader{data: nil, err: errBoom}

	got, err := readStreamingBatches(reader)

	require.Error(t, err)
	require.ErrorIs(t, err, errBoom, "underlying reader error must be wrapped")
	require.Empty(t, got, "no batches must be returned on a pre-delimiter failure")
}

// decodeStreamBody deserializes the POKTHTTPRequest produced by buildStreamPayload
// and returns the inner JSON body as a generic map for field-level assertions.
func decodeStreamBody(t *testing.T, poktHTTPRequestBz []byte) map[string]any {
	t.Helper()
	poktReq, err := sdktypes.DeserializeHTTPRequest(poktHTTPRequestBz)
	require.NoError(t, err)
	require.Equal(t, "POST", poktReq.Method, "stream payload must be a POST")

	var body map[string]any
	require.NoError(t, json.Unmarshal(poktReq.BodyBz, &body), "inner body must be a JSON object")
	return body
}

// TestBuildStreamPayload_DefaultWithBatches proves the default eth_blockNumber
// body carries a "batches" field when RelayBatches > 0 — this is how the CLI tells
// the demo backend how many SSE batches to emit before closing.
func TestBuildStreamPayload_DefaultWithBatches(t *testing.T) {
	defer restorePayloadJSON(RelayPayloadJSON)
	RelayPayloadJSON = ""

	bz, err := buildStreamPayload(5)
	require.NoError(t, err)

	body := decodeStreamBody(t, bz)
	require.Equal(t, "eth_blockNumber", body["method"], "default method must be preserved")
	require.Equal(t, float64(5), body["batches"], "batches must be injected into the body")
}

// TestBuildStreamPayload_ZeroOmitsBatches proves that with RelayBatches == 0 (the
// mainnet-style "receive everything until close" case) no batches field is added,
// so a real backend is not handed a control field it never defined.
func TestBuildStreamPayload_ZeroOmitsBatches(t *testing.T) {
	defer restorePayloadJSON(RelayPayloadJSON)
	RelayPayloadJSON = ""

	bz, err := buildStreamPayload(0)
	require.NoError(t, err)

	body := decodeStreamBody(t, bz)
	_, hasBatches := body["batches"]
	require.False(t, hasBatches, "batches must be omitted when count is 0")
}

// TestBuildStreamPayload_CustomObjectMerged proves a caller-supplied JSON object
// payload is preserved and the batches field is merged in alongside it.
func TestBuildStreamPayload_CustomObjectMerged(t *testing.T) {
	defer restorePayloadJSON(RelayPayloadJSON)
	RelayPayloadJSON = `{"jsonrpc":"2.0","method":"custom_method","id":7}`

	bz, err := buildStreamPayload(3)
	require.NoError(t, err)

	body := decodeStreamBody(t, bz)
	require.Equal(t, "custom_method", body["method"], "custom method must survive")
	require.Equal(t, float64(7), body["id"], "custom id must survive")
	require.Equal(t, float64(3), body["batches"], "batches must be merged into custom body")
}

// TestBuildStreamPayload_NonObjectPayloadErrors proves non-object custom payloads
// (which cannot carry a batches field) are rejected up front with a clear error,
// instead of silently dropping the batches control or panicking. A JSON "null"
// unmarshals to a nil map without error, so it must be caught explicitly — a
// naive body["batches"]=n on a nil map panics.
func TestBuildStreamPayload_NonObjectPayloadErrors(t *testing.T) {
	defer restorePayloadJSON(RelayPayloadJSON)

	for _, payload := range []string{`[1,2,3]`, `"just a string"`, `42`, `null`} {
		t.Run(payload, func(t *testing.T) {
			RelayPayloadJSON = payload
			_, err := buildStreamPayload(3)
			require.Error(t, err, "a non-object payload cannot carry batches and must be rejected")
		})
	}
}

// TestBuildStreamPayload_ZeroForwardsCustomBytesVerbatim proves that with no
// batches injection (the mainnet/real-backend path) the custom --payload reaches
// the backend byte-for-byte. A generic map round-trip would reorder keys and
// coerce integers to float64, corrupting a request a real service may care about;
// only the localnet demo batches>0 path is allowed to reserialize.
func TestBuildStreamPayload_ZeroForwardsCustomBytesVerbatim(t *testing.T) {
	defer restorePayloadJSON(RelayPayloadJSON)
	// Key order json.Marshal would NOT reproduce (method before jsonrpc) plus a
	// large integer id that a float64 round-trip mangles.
	RelayPayloadJSON = `{"method":"eth_getBlockByNumber","jsonrpc":"2.0","id":12345678901234567,"params":["0x1",false]}`

	bz, err := buildStreamPayload(0)
	require.NoError(t, err)

	poktReq, err := sdktypes.DeserializeHTTPRequest(bz)
	require.NoError(t, err)
	require.Equal(t, RelayPayloadJSON, string(poktReq.BodyBz),
		"custom payload must be forwarded byte-for-byte when no batches are injected")
}

// restorePayloadJSON resets the package-level RelayPayloadJSON flag after a test
// mutates it, keeping tests independent.
func restorePayloadJSON(v string) { RelayPayloadJSON = v }
