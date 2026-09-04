package relayer

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"

	sdktypes "github.com/pokt-network/shannon-sdk/types"

	"github.com/pokt-network/pocket-relay-miner/logging"
	servicetypes "github.com/pokt-network/poktroll/x/service/types"
)

const (
	workloadUnknown      = "unknown"
	workloadJSONRPCBatch = "jsonrpc_batch"
	workloadJSONRPCOther = "jsonrpc_unknown"
	workloadREST         = "rest"
)

// RelayWorkload is the bounded, body-safe classification emitted with relay
// telemetry. JSON-RPC params and REST query strings are intentionally ignored.
type RelayWorkload struct {
	RPCType               string
	Workload              string
	HTTPMethod            string
	NormalizedPath        string
	BackendRequestBytes   int
	JSONRPCMethod         string
	JSONRPCMethods        []string
	JSONRPCBatch          bool
	JSONRPCBatchSize      int
	BatchMethodsTruncated bool
	RESTPath              string
}

// PocketRequestID returns a stable opaque ID derived from the complete signed
// RelayRequest protobuf, including its signature. The raw request is never
// logged or included in the ID value.
func PocketRequestID(relayRequestBytes []byte) string {
	if len(relayRequestBytes) == 0 {
		return ""
	}

	digest := sha256.Sum256(relayRequestBytes)
	return hex.EncodeToString(digest[:16])
}

// PocketRequestIDFromRelayRequest returns the same ID for typed relay paths
// that no longer retain the original protobuf byte slice.
func PocketRequestIDFromRelayRequest(relayRequest *servicetypes.RelayRequest) string {
	if relayRequest == nil {
		return ""
	}

	serialized, err := relayRequest.Marshal()
	if err != nil {
		return ""
	}
	return PocketRequestID(serialized)
}

// ClassifyRelayWorkload identifies the transport and JSON-RPC workload without
// decoding or logging request params, calldata, query strings, or payloads.
func ClassifyRelayWorkload(relayRequest *servicetypes.RelayRequest) RelayWorkload {
	classification := RelayWorkload{
		RPCType:  workloadUnknown,
		Workload: workloadUnknown,
	}
	if relayRequest == nil {
		return classification
	}

	httpRequest, err := sdktypes.DeserializeHTTPRequest(relayRequest.Payload)
	if err != nil || httpRequest == nil {
		return classification
	}

	classification.HTTPMethod = httpRequest.Method
	classification.NormalizedPath = safeRESTPath(httpRequest.Url)
	classification.BackendRequestBytes = len(httpRequest.BodyBz)

	rpcType := relayHeaderValue(httpRequest, "Rpc-Type")
	if rpcType != "" {
		classification.RPCType = RPCTypeToBackendType(rpcType)
	} else if strings.HasPrefix(strings.ToLower(relayHeaderValue(httpRequest, "Content-Type")), "application/json") {
		classification.RPCType = BackendTypeJSONRPC
	}

	switch classification.RPCType {
	case BackendTypeJSONRPC:
		classifyJSONRPC(httpRequest.BodyBz, &classification)
	case BackendTypeREST:
		classification.Workload = workloadREST
		classification.RESTPath = classification.NormalizedPath
	}

	return classification
}

func relayHeaderValue(request *sdktypes.POKTHTTPRequest, key string) string {
	if request == nil {
		return ""
	}
	for name, header := range request.Header {
		if strings.EqualFold(name, key) && header != nil && len(header.Values) > 0 {
			return header.Values[0]
		}
	}
	return ""
}

func classifyJSONRPC(body []byte, classification *RelayWorkload) {
	trimmed := bytes.TrimSpace(body)
	if len(trimmed) == 0 {
		classification.Workload = workloadJSONRPCOther
		return
	}

	if bytes.HasPrefix(trimmed, []byte("[")) {
		var batch []struct {
			Method string `json:"method"`
		}
		if err := json.Unmarshal(body, &batch); err != nil {
			classification.Workload = workloadJSONRPCOther
			return
		}
		classification.Workload = workloadJSONRPCBatch
		classification.JSONRPCBatch = true
		classification.JSONRPCBatchSize = len(batch)
		seenMethods := make(map[string]struct{}, len(batch))
		for _, request := range batch {
			if request.Method == "" {
				continue
			}
			seenMethods[request.Method] = struct{}{}
		}
		methods := make([]string, 0, len(seenMethods))
		for method := range seenMethods {
			methods = append(methods, method)
		}
		sort.Strings(methods)
		classification.BatchMethodsTruncated = len(methods) > 8
		if classification.BatchMethodsTruncated {
			methods = methods[:8]
		}
		classification.JSONRPCMethods = methods
		return
	}

	var request struct {
		Method string `json:"method"`
	}
	if err := json.Unmarshal(body, &request); err != nil || request.Method == "" {
		classification.Workload = workloadJSONRPCOther
		return
	}
	classification.JSONRPCMethod = request.Method
	classification.Workload = request.Method
}

func safeRESTPath(rawURL string) string {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return ""
	}
	parts := strings.Split(parsed.Path, "/")
	for i, part := range parts {
		switch {
		case isNumericPathPart(part):
			parts[i] = ":height"
		case isHexPathPart(part):
			parts[i] = ":hash"
		case isAddressPathPart(part):
			parts[i] = ":address"
		}
	}
	return strings.Join(parts, "/")
}

func isNumericPathPart(part string) bool {
	if part == "" {
		return false
	}
	for _, char := range part {
		if char < '0' || char > '9' {
			return false
		}
	}
	return true
}

func isHexPathPart(part string) bool {
	part = strings.TrimPrefix(part, "0x")
	if len(part) < 16 {
		return false
	}
	for _, char := range part {
		if !((char >= '0' && char <= '9') || (char >= 'a' && char <= 'f') || (char >= 'A' && char <= 'F')) {
			return false
		}
	}
	return true
}

func isAddressPathPart(part string) bool {
	separator := strings.IndexByte(part, '1')
	return separator > 0 && len(part)-separator-1 >= 20
}

// setPocketRequestID overwrites any client-provided value with the identity
// derived from the signed request bytes.
func setPocketRequestID(header http.Header, relayRequestBytes []byte) {
	if requestID := PocketRequestID(relayRequestBytes); requestID != "" {
		header.Set(HeaderPocketRequestID, requestID)
	}
}

// RelayObservation captures the request lifecycle at the proxy boundary.
type RelayObservation struct {
	RequestID         string
	ServiceID         string
	RPCType           string
	Workload          RelayWorkload
	RelayRequestBytes int
	ResponseBytes     int
	StatusCode        int
	BackendEndpoint   string
	Retries           int
	BackendLatency    time.Duration
	TotalLatency      time.Duration
	Outcome           string
	RejectReason      string
}

func logRelayObservation(logger logging.Logger, relayRequest *servicetypes.RelayRequest, observation RelayObservation) {
	sessionContext := logging.SessionContextFromRelayRequest(relayRequest)
	if sessionContext.ServiceID == "" {
		sessionContext.ServiceID = observation.ServiceID
	}

	event := logging.WithSessionContext(logger.Info(), sessionContext).
		Str("event", "pocket_relay_observation").
		Str(logging.FieldRequestID, observation.RequestID).
		Str(logging.FieldRPCType, observation.RPCType).
		Str(logging.FieldWorkload, observation.Workload.Workload).
		Str("outcome", observation.Outcome).
		Int("backend_request_bytes", observation.Workload.BackendRequestBytes).
		Int("relay_request_bytes", observation.RelayRequestBytes).
		Int(logging.FieldResponseSize, observation.ResponseBytes).
		Int("status_code", observation.StatusCode).
		Int("retries", observation.Retries).
		Float64("backend_latency_ms", float64(observation.BackendLatency)/float64(time.Millisecond)).
		Float64("total_latency_ms", float64(observation.TotalLatency)/float64(time.Millisecond))
	if observation.BackendEndpoint != "" {
		event = event.Str("backend_endpoint", observation.BackendEndpoint)
	}
	if observation.Workload.HTTPMethod != "" {
		event = event.Str("http_method", observation.Workload.HTTPMethod)
	}
	if observation.Workload.NormalizedPath != "" {
		event = event.Str("normalized_path", observation.Workload.NormalizedPath)
	}
	if observation.RejectReason != "" {
		event = event.Str("reject_reason", observation.RejectReason)
	}
	if observation.Workload.JSONRPCMethod != "" {
		event = event.Str("jsonrpc_method", observation.Workload.JSONRPCMethod)
	}
	if observation.Workload.JSONRPCBatch {
		event = event.Bool("jsonrpc_batch", true).
			Int("jsonrpc_batch_size", observation.Workload.JSONRPCBatchSize).
			Strs("jsonrpc_methods", observation.Workload.JSONRPCMethods).
			Bool("batch_methods_truncated", observation.Workload.BatchMethodsTruncated)
	}
	event.Msg("pocket relay observation")
}
