# dsh-as-a-service

dsh-as-a-service turns a DeepSeek Harness agent into a persistent HTTP and SSE
service. It is local-first today and organized for a later cloud, multi-tenant
platform.

## Architecture

    client
      |
      | HTTP and SSE contract
      v
    TypeScript Gateway (experimental control plane)
      |
      | unchanged HTTP and SSE forwarding
      v
    Python Worker (stable execution plane)
      |
      | official DeepSeek Harness Python SDK
      v
    bundled DSH JSON-RPC runtime

The Worker is the only component that starts DSH, owns model selection, and
persists conversation memory. The Gateway deliberately owns no agent memory;
it is the future home for authentication, tenant routing, quotas, audit logs,
and asynchronous job coordination.

The SDK is Python, but the DSH runtime is a bundled target-native executable
from the Node-based DSH ecosystem. Production containers do not need a separate
Node installation. Node 22 is only needed to run the TypeScript Gateway.

## Repository layout

    protocol/                   Shared HTTP and SSE contract
    implementations/python/     Stable DSH Worker
    implementations/typescript/ Experimental HTTP and SSE Gateway
    deploy/docker/              Python Worker Docker and Compose deployment

## Protocol

Both implementations preserve the protocol in protocol/http-contract.json:

- GET /health and GET /models
- POST /chat for JSON responses
- POST /chat/stream for Server-Sent Events
- allowlisted model aliases
- persistent session-to-model binding
- per-session serialization and cross-session concurrency

When the first request uses a session ID, that session binds to the requested
model or the configured default. Reusing it with a different model returns
409 session_model_conflict.

## Python Worker: current stable implementation

The Worker uses the official deepseek-harness-sdk and the included DSH runtime.
It provides session persistence, restart-safe conversation memory, model
routing, workspace-write policy, Todo, compaction, and optional DeepSeek web
search.

### Local start

    cd implementations/python
    python3 -m venv .venv
    .venv/bin/pip install -r requirements.txt
    cp .env.example .env
    # Edit .env and set DEEPSEEK_API_KEY.
    ./start.sh

    curl -sS http://127.0.0.1:8765/health

The default models.json uses provider deepseek-official and model
deepseek-chat. For another provider, edit models.json and place the matching
provider configuration and credentials in DSH_HOME.

### API examples

    curl -sS http://127.0.0.1:8765/chat \
      -H 'Content-Type: application/json' \
      -d '{"session_id":"demo-1","message":"Explain the files in the workspace.","model":"deepseek"}'

    curl -N -sS http://127.0.0.1:8765/chat/stream \
      -H 'Content-Type: application/json' \
      -d '{"session_id":"demo-1","message":"Give me a brief status update."}'

The stream can emit session, assistant.delta, tool.call, tool.result, usage,
status, done, and error events. Consumers should treat done or error as a
terminal event and accept unknown future event types.

## TypeScript Gateway: experimental

The Gateway is a dependency-free Node 22 implementation. It validates the
shared route set, forwards JSON bodies and status codes, and streams SSE without
buffering it. It does not replace the Python Worker yet.

    cd implementations/typescript
    DSH_WORKER_URL=http://127.0.0.1:8765 npm start

It listens on 127.0.0.1:8780 by default. See implementations/typescript/README.md.

## Docker deployment

Docker first deploys only the stable Python Worker. See deploy/docker/README.md.
The Compose stack binds the service to 127.0.0.1:8765 and keeps runtime state in
a Docker volume. Account-specific endpoint routes and DSH credentials stay in
deployment-only files ignored by Git.

## Development checks

    cd implementations/python
    python -m unittest discover -s tests -v

    cd ../typescript
    npm test

## Security

The Worker has no HTTP authentication and should remain loopback-only until it
is behind an authenticated TLS reverse proxy. Workspace-write protects mutation
scope, but it is not a tenant isolation boundary. Before public exposure, add
authentication, authorization, quotas, audit logs, tenant-aware storage, and
stronger sandboxing.

This repository contains no application Skills, media adapters, bot bridges,
runtime state, credentials, or account-specific model endpoint IDs.

## License

[MIT](LICENSE). DeepSeek Harness remains subject to its upstream license.
