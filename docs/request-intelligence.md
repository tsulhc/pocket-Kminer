# Pocket Request Intelligence

This contract is implemented in the relayer and is intended for the Phase 1
observability canary. It does not change relay admission, backend selection,
mining eligibility, claims, proofs, or settlement.

## Request Identity

`Pocket-Request-ID` is the lowercase 32-character hexadecimal SHA-256 digest
truncated to 128 bits of the complete serialized, signed `RelayRequest`
protobuf. The request body is never logged. The relayer sets this header on
HTTP upstream requests after copying client and configured headers, so an
incoming header cannot replace the derived identity.

Typed gRPC relay handling derives the same ID from the received
`RelayRequest`. The ID is not sent in the WebSocket handshake because the
individual relay request is received after the connection is established.

## Structured Event

For each valid relay accepted by the HTTP relay lifecycle, the relayer emits
one `pocket relay observation` JSON event containing:

- `event`: `pocket_relay_observation`
- `pocket_request_id`: derived request identity
- `service_id`, `session_id`, `supplier`, `application`
- `rpc_type`: normalized backend type
- `workload`: JSON-RPC method, `jsonrpc_batch`, `jsonrpc_unknown`, `rest`, or `unknown`
- `jsonrpc_method` for a single JSON-RPC request
- `http_method` and `normalized_path` for the actual backend request
- `jsonrpc_batch`, `jsonrpc_batch_size`, up to 8 sorted unique
  `jsonrpc_methods`, and `batch_methods_truncated`
- `backend_request_bytes` for the payload sent to Janus/RPC and
  `relay_request_bytes` for the outer signed RelayRequest
- `response_bytes`, `status_code`, `retries`, backend endpoint, backend
  latency, and total latency
- coarse `outcome`: `served`, `rejected`, `backend_error`,
  `backend_5xx`, or `signing_error`
- bounded `reject_reason` for the specific rejection/failure class

The event does not contain JSON-RPC `params`, calldata, request or response
payloads, REST query strings, client IPs, public keys, or Prometheus labels for
request/session/customer identity. `jsonrpc_method` is a log field only; it is
not a Prometheus label.

## Review Boundary

This PR adds identity, classification, propagation, and structured lifecycle
logging only. It does not enable a deployment, alter Ansible configuration, or
change admission, backend selection, mining, claims, proofs, or settlement.
The infra PR consumes `Pocket-Request-ID` and records the selected final
upstream RPC endpoint.
