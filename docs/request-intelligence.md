# Pocket Request Intelligence

This contract is implemented in the relayer and is intended for the Phase 1
observability canary. It does not change relay admission, backend selection,
mining eligibility, claims, proofs, or settlement.

## Request Identity

`Pocket-Request-ID` is the value `pocket-req-` followed by the lowercase
SHA-256 digest of the complete serialized, signed `RelayRequest` protobuf. The
request body is never logged. The relayer sets this header on HTTP upstream
requests after copying client and configured headers, so an incoming header
cannot replace the derived identity.

Typed gRPC relay handling derives the same ID from the received
`RelayRequest`. WebSocket messages are included in structured relayer events,
but the ID is not sent in the WebSocket handshake because the individual relay
request is received after the connection is established.

## Structured Event

For each valid relay processed by `RelayProcessor`, the relayer emits one
`relay telemetry` JSON event containing:

- `event`: `relay`
- `pocket_request_id`: derived request identity
- `service_id`, `session_id`, `supplier`, `application`
- `rpc_type`: normalized backend type
- `workload`: JSON-RPC method, `jsonrpc_batch`, `jsonrpc_unknown`, `rest`, or `unknown`
- `jsonrpc_method` for a single JSON-RPC request
- `jsonrpc_batch` and `jsonrpc_batch_size` for a batch
- `rest_path` without the query string for REST requests
- `request_bytes`, `response_bytes`, `relay_hash`, `compute_units`
- `outcome`: `mined`, `not_mined`, or processing error

The event does not contain JSON-RPC `params`, calldata, request or response
payloads, REST query strings, client IPs, public keys, or Prometheus labels for
request/session/customer identity. `jsonrpc_method` is a log field only; it is
not a Prometheus label.

## Review Boundary

This PR adds identity, classification, propagation, and structured logging
only. It does not enable a deployment, alter Ansible configuration, or define
the Janus upstream endpoint field. The infra PR consumes `Pocket-Request-ID`
and separately records the selected final upstream RPC endpoint.
