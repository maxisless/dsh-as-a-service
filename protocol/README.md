# Service Protocol

The http-contract.json file is the language-neutral contract for the Python
Worker and the TypeScript Gateway. Both implementations preserve these routes,
request fields, session semantics, and SSE event names.

The Python Worker is the current stable execution implementation. The
TypeScript Gateway is an edge/control-plane implementation that validates and
forwards this protocol to a Worker; it deliberately does not duplicate DSH
runtime or memory logic.

## Request and session semantics

- message is required and must be a non-empty string.
- session_id is optional. The service creates one if omitted.
- model is an allowlisted alias, not an arbitrary provider endpoint.
- A session binds permanently to the first model it uses. Selecting a different
  model with the same session ID returns 409 session_model_conflict.
- Requests belonging to one session are serialized. Separate sessions can run
  concurrently, subject to the Worker concurrency limit.

## SSE semantics

POST /chat/stream uses text/event-stream. Consumers should use the done event
as the terminal success result and the error event as a terminal failure signal.
Keep unknown event names forward-compatible.

## Hosted control-plane preview

When `DSH_CONTROL_PLANE_TOKEN` is configured, the Worker also exposes a
server-owned `/v1` API. The token protects the initial single-principal mode;
`DSH_CONTROL_PLANE_IDENTITIES_JSON` can map separate bearer tokens to tenant,
principal, and role for development/testing of multiple tenants. It is not a
substitute for production OAuth/API-key management or a Vault.

- `POST /v1/agents` publishes an immutable Agent version (admin/manager role).
- `POST /v1/sessions` creates a server-issued session bound to a tenant,
  principal, Agent version, and allowlisted model.
- `POST /v1/sessions/{session_id}/runs` creates a durable run. Supplying an
  `Idempotency-Key` returns the existing run rather than creating a duplicate.
- `GET /v1/runs/{run_id}/events` is SSE with numeric event IDs and supports
  `Last-Event-ID` replay from the durable event log.
- `POST /v1/runs/{run_id}/cancel` records cancellation intent. The current DSH
  SDK cannot forcibly interrupt an in-flight prompt, so it reaches a durable
  terminal boundary before acknowledging cancellation.

The Worker now gives every control-plane session a distinct workspace, DSH
session state root, conversation file, and Harness instance. This is the safe
single-container implementation for the current SDK's process-level cwd model;
cross-container executor isolation, an external Vault, and a Model/Tool Gateway
remain later deployment components.
