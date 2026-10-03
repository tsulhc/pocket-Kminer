package relayer

import (
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"strings"
	"time"

	"github.com/pokt-network/pocket-relay-miner/logging"
	servicetypes "github.com/pokt-network/poktroll/x/service/types"
	sdktypes "github.com/pokt-network/shannon-sdk/types"
	"github.com/rs/zerolog"
)

type relayWorkload struct {
	RPCType             string
	Workload            string
	BackendRequestBytes int
}

const (
	workloadUnknown      = "unknown"
	workloadJSONRPC      = "jsonrpc"
	workloadJSONRPCBatch = "jsonrpc_batch"
	workloadREST         = "rest"
	workloadGRPC         = "grpc"
	workloadWebSocket    = "websocket"
	workloadCometBFT     = "cometbft"

	relayOutcomeRejected      = "rejected"
	relayOutcomeServed        = "served"
	relayOutcomeBackendError  = "backend_error"
	relayOutcomeBackend5xx    = "backend_5xx"
	relayOutcomeSigningError  = "signing_error"
	relayObservationEvent     = "pocket_relay_observation"
	relayObservationMessage   = "relay observation"
	maxTelemetryHeaderBytes   = 32
	maxTelemetryProbeBytes    = 64
	maxTelemetryIdentityBytes = 128
	maxTelemetryEndpointBytes = 64
)

// pocketRequestID is an opaque correlation identifier for one serialized
// signed RelayRequest. It intentionally contains no request data.
func pocketRequestID(relayRequestBytes []byte) string {
	if len(relayRequestBytes) == 0 {
		return ""
	}
	digest := sha256.Sum256(relayRequestBytes)
	return hex.EncodeToString(digest[:16])
}

// pocketRequestIDFromRelayRequest hashes the protobuf serialization of a typed
// RelayRequest. Transports that do not expose the received envelope bytes use
// this form.
func pocketRequestIDFromRelayRequest(relayRequest *servicetypes.RelayRequest) string {
	if relayRequest == nil {
		return ""
	}
	serialized, err := relayRequest.Marshal()
	if err != nil {
		return ""
	}
	return pocketRequestID(serialized)
}

func classifyHTTPRequestWorkload(poktHTTPRequest *sdktypes.POKTHTTPRequest, rpcType string) relayWorkload {
	if poktHTTPRequest == nil {
		return relayWorkload{RPCType: telemetryRPCType(rpcType), Workload: workloadUnknown}
	}
	rpcType = telemetryRPCType(rpcType)
	if rpcType == workloadUnknown {
		rpcType = telemetryRPCType(relayHeaderValue(poktHTTPRequest.Header, "Rpc-Type"))
	}
	if rpcType == workloadUnknown && isJSONContentType(relayHeaderValue(poktHTTPRequest.Header, "Content-Type")) {
		rpcType = workloadJSONRPC
	}
	return relayWorkload{
		RPCType:             rpcType,
		Workload:            workloadForRPCType(poktHTTPRequest.BodyBz, rpcType),
		BackendRequestBytes: len(poktHTTPRequest.BodyBz),
	}
}

func telemetryRPCType(value string) string {
	if len(value) > maxTelemetryHeaderBytes {
		return workloadUnknown
	}
	value = strings.ToLower(strings.TrimSpace(value))
	backendType := RPCTypeToBackendType(value)
	switch backendType {
	case workloadJSONRPC, workloadREST, workloadGRPC, workloadWebSocket, workloadCometBFT:
		return backendType
	default:
		return workloadUnknown
	}
}

func workloadForRPCType(body []byte, rpcType string) string {
	switch rpcType {
	case workloadJSONRPC:
		first := firstNonWhitespaceByte(body, maxTelemetryProbeBytes)
		switch first {
		case '{':
			return workloadJSONRPC
		case '[':
			return workloadJSONRPCBatch
		default:
			return workloadUnknown
		}
	case workloadREST:
		return workloadREST
	case workloadGRPC:
		return workloadGRPC
	case workloadWebSocket:
		return workloadWebSocket
	case workloadCometBFT:
		return workloadCometBFT
	default:
		return workloadUnknown
	}
}

func firstNonWhitespaceByte(value []byte, limit int) byte {
	if limit > len(value) {
		limit = len(value)
	}
	for _, current := range value[:limit] {
		switch current {
		case ' ', '\t', '\n', '\r':
			continue
		default:
			return current
		}
	}
	return 0
}

func isJSONContentType(value string) bool {
	if len(value) > maxTelemetryHeaderBytes {
		return false
	}
	return strings.HasPrefix(strings.ToLower(strings.TrimSpace(value)), "application/json")
}

func relayHeaderValue(headers map[string]*sdktypes.Header, name string) string {
	for key, header := range headers {
		if header == nil || !strings.EqualFold(key, name) {
			continue
		}
		if len(header.Values) == 0 {
			return ""
		}
		value := header.Values[0]
		if len(value) > maxTelemetryHeaderBytes {
			return ""
		}
		return value
	}
	return ""
}

func setPocketRequestID(header http.Header, relayRequestBytes []byte) {
	setPocketRequestIDValue(header, pocketRequestID(relayRequestBytes))
}

func setPocketRequestIDValue(header http.Header, requestID string) {
	if header == nil {
		return
	}
	if requestID == "" {
		header.Del(HeaderPocketRequestID)
		return
	}
	header.Set(HeaderPocketRequestID, requestID)
}

type relayObservation struct {
	RequestID            string
	ServiceID            string
	RPCType              string
	Workload             string
	RequestBytes         int
	BackendRequestBytes  int
	BackendResponseBytes int
	StatusCode           int
	BackendStatusCode    int
	BackendEndpoint      string
	Retries              int
	BackendLatency       time.Duration
	TotalLatency         time.Duration
	BackendOutcome       string
	Outcome              string
	RejectReason         string
	SignatureVerified    bool
	sessionContext       *logging.SessionContext
}

func newRelayObservation(
	relayRequest *servicetypes.RelayRequest,
	rawRequest []byte,
	poktHTTPRequest *sdktypes.POKTHTTPRequest,
	serviceID string,
	rpcType string,
) relayObservation {
	requestBytes := rawRequest
	if requestBytes == nil && relayRequest != nil {
		requestBytes, _ = relayRequest.Marshal()
	}
	workload := classifyHTTPRequestWorkload(poktHTTPRequest, rpcType)
	return relayObservation{
		RequestID:           pocketRequestID(requestBytes),
		ServiceID:           serviceID,
		RPCType:             workload.RPCType,
		Workload:            workload.Workload,
		RequestBytes:        len(requestBytes),
		BackendRequestBytes: workload.BackendRequestBytes,
		Outcome:             relayOutcomeRejected,
	}
}

func boundedWorkload(value string) string {
	switch value {
	case workloadJSONRPC, workloadJSONRPCBatch, workloadREST, workloadGRPC, workloadWebSocket, workloadCometBFT:
		return value
	default:
		return workloadUnknown
	}
}

func boundedObservationOutcome(value string) string {
	switch value {
	case relayOutcomeRejected, relayOutcomeServed, relayOutcomeBackendError, relayOutcomeBackend5xx, relayOutcomeSigningError:
		return value
	default:
		return relayOutcomeRejected
	}
}

func boundedBackendOutcome(value string) string {
	switch value {
	case "success", "backend_5xx", "client_disconnected", "backend_timeout", "backend_network_error":
		return value
	default:
		return ""
	}
}

func boundedRejectReason(value string) string {
	switch value {
	case rejectReasonReadBodyError,
		rejectReasonBodyTooLarge,
		rejectReasonInvalidRelayRequest,
		rejectReasonMissingServiceID,
		rejectReasonNilRelayRequest,
		rejectReasonResponseSignerNotConfigured,
		rejectReasonSupplierCacheNotConfigured,
		rejectReasonUnknownService,
		rejectReasonMissingSupplierAddress,
		rejectReasonSupplierChanged,
		rejectReasonServiceChanged,
		rejectReasonApplicationChanged,
		rejectReasonNoRelayYet,
		rejectReasonBackendDialFailed,
		rejectReasonSupplierCacheError,
		rejectReasonNoLocalSigner,
		rejectReasonSupplierInactive,
		rejectReasonNoServices,
		rejectReasonWrongService,
		rejectReasonMeterError,
		rejectReasonStakeExhausted,
		rejectReasonValidationFailed,
		rejectReasonImplausibleSession,
		rejectReasonClientDisconnected,
		rejectReasonBackendTimeout,
		rejectReasonBackendNetworkError,
		rejectReasonBackend5xx,
		rejectReasonSigningError,
		rejectReasonSessionExpired,
		rejectReasonPublishQueueFull,
		rejectReasonMeteringNotConfigured,
		rejectReasonPricingUnavailable,
		rejectReasonValidationQueueFull,
		rejectReasonStorageSaturated,
		rejectReasonRecvError,
		rejectReasonSendError:
		return value
	default:
		return ""
	}
}

func safeEndpointName(value string) string {
	if len(value) == 0 || len(value) > maxTelemetryEndpointBytes {
		return ""
	}
	for _, character := range value {
		if (character >= 'a' && character <= 'z') || (character >= 'A' && character <= 'Z') || (character >= '0' && character <= '9') || strings.ContainsRune("._-:[]", character) {
			continue
		}
		return ""
	}
	return value
}

func safeTelemetryIdentity(value string) string {
	if value == "" || len(value) > maxTelemetryIdentityBytes {
		return ""
	}
	for _, character := range value {
		if (character >= 'a' && character <= 'z') || (character >= 'A' && character <= 'Z') || (character >= '0' && character <= '9') || strings.ContainsRune("._-:", character) {
			continue
		}
		return ""
	}
	return value
}

func observationSessionContext(relayRequest *servicetypes.RelayRequest, serviceID string, override *logging.SessionContext) *logging.SessionContext {
	var context *logging.SessionContext
	if override != nil {
		copiedContext := *override
		context = &copiedContext
	} else {
		context = logging.SessionContextFromRelayRequest(relayRequest)
	}
	if context == nil {
		context = &logging.SessionContext{}
	}
	context.SessionID = safeTelemetryIdentity(context.SessionID)
	context.Supplier = safeTelemetryIdentity(context.Supplier)
	context.Application = safeTelemetryIdentity(context.Application)
	context.ServiceID = safeTelemetryIdentity(serviceID)
	context.SessionEndHeight = 0
	return context
}

func logRelayObservation(logger logging.Logger, relayRequest *servicetypes.RelayRequest, observation relayObservation) {
	// The Info-level child keeps this structured event visible when the parent
	// logger is configured at Warn, without changing the parent logger's level.
	telemetryLogger := logger.Level(zerolog.InfoLevel).Sample(nil)
	event := telemetryLogger.Info()
	event = logging.WithSessionContext(event, observationSessionContext(relayRequest, observation.ServiceID, observation.sessionContext))
	event.Str("event", relayObservationEvent).
		Str("message", relayObservationMessage).
		Str(logging.FieldRequestID, observation.RequestID).
		Str(logging.FieldRPCType, telemetryRPCType(observation.RPCType)).
		Str(logging.FieldWorkload, boundedWorkload(observation.Workload)).
		Int(logging.FieldRequestSize, max(observation.RequestBytes, 0)).
		Int(logging.FieldBackendRequestSize, max(observation.BackendRequestBytes, 0)).
		Int(logging.FieldBackendResponseSize, max(observation.BackendResponseBytes, 0)).
		Int("retries", max(observation.Retries, 0)).
		Str("outcome", boundedObservationOutcome(observation.Outcome)).
		Bool("signature_verified", observation.SignatureVerified)
	if outcome := boundedBackendOutcome(observation.BackendOutcome); outcome != "" {
		event.Str("backend_outcome", outcome)
	}
	if reason := boundedRejectReason(observation.RejectReason); reason != "" {
		event.Str("reject_reason", reason)
	}
	if observation.StatusCode > 0 {
		event.Int("status_code", observation.StatusCode)
	}
	if observation.BackendStatusCode > 0 {
		event.Int("backend_status_code", observation.BackendStatusCode)
	}
	if endpoint := safeEndpointName(observation.BackendEndpoint); endpoint != "" {
		event.Str("backend_endpoint", endpoint)
	}
	if observation.BackendLatency > 0 {
		event.Dur("backend_latency_ms", observation.BackendLatency)
	}
	if observation.TotalLatency > 0 {
		event.Dur("total_latency_ms", observation.TotalLatency)
	}
	event.Msg(relayObservationMessage)
}
