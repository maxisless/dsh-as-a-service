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
