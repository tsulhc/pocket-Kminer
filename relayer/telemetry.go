package relayer

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/url"
	"strings"

	sdktypes "github.com/pokt-network/shannon-sdk/types"

	"github.com/pokt-network/pocket-relay-miner/logging"
	servicetypes "github.com/pokt-network/poktroll/x/service/types"
)

const (
	pocketRequestIDPrefix = "pocket-req-"

	workloadUnknown      = "unknown"
	workloadJSONRPCBatch = "jsonrpc_batch"
	workloadJSONRPCOther = "jsonrpc_unknown"
	workloadREST         = "rest"
)

// RelayWorkload is the bounded, body-safe classification emitted with relay
// telemetry. JSON-RPC params and REST query strings are intentionally ignored.
type RelayWorkload struct {
	RPCType          string
	Workload         string
	JSONRPCMethod    string
	JSONRPCBatch     bool
	JSONRPCBatchSize int
	RESTPath         string
}

// PocketRequestID returns a stable opaque ID derived from the complete signed
// RelayRequest protobuf, including its signature. The raw request is never
// logged or included in the ID value.
func PocketRequestID(relayRequestBytes []byte) string {
	if len(relayRequestBytes) == 0 {
		return ""
	}

	digest := sha256.Sum256(relayRequestBytes)
	return pocketRequestIDPrefix + hex.EncodeToString(digest[:])
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
		classification.RESTPath = safeRESTPath(httpRequest.Url)
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
	return parsed.Path
}

func logRelayTelemetry(
	logger logging.Logger,
	relayRequest *servicetypes.RelayRequest,
	relayRequestBytes, responseBytes []byte,
	serviceID, supplierAddress, requestID string,
	classification RelayWorkload,
	outcome, relayHash string,
	computeUnits uint64,
) {
	sessionContext := logging.SessionContextFromRelayRequest(relayRequest)
	if sessionContext.ServiceID == "" {
		sessionContext.ServiceID = serviceID
	}
	if sessionContext.Supplier == "" {
		sessionContext.Supplier = supplierAddress
	}

	event := logging.WithSessionContext(logger.Info(), sessionContext).
		Str("event", "relay").
		Str(logging.FieldRequestID, requestID).
		Str(logging.FieldRPCType, classification.RPCType).
		Str(logging.FieldWorkload, classification.Workload).
		Str("outcome", outcome).
		Int(logging.FieldRequestSize, len(relayRequestBytes)).
		Int(logging.FieldResponseSize, len(responseBytes)).
		Uint64("compute_units", computeUnits)

	if classification.JSONRPCMethod != "" {
		event = event.Str("jsonrpc_method", classification.JSONRPCMethod)
	}
	if classification.JSONRPCBatch {
		event = event.Bool("jsonrpc_batch", true).Int("jsonrpc_batch_size", classification.JSONRPCBatchSize)
	}
	if classification.RESTPath != "" {
		event = event.Str("rest_path", classification.RESTPath)
	}
	if relayHash != "" {
		event = event.Str("relay_hash", relayHash)
	}

	event.Msg("relay telemetry")
}
