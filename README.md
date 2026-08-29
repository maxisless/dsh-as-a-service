# dsh-as-a-service

dsh-as-a-service is a small, local-first service layer for [DeepSeek Harness](https://github.com/deepseek-ai/deepseek-harness). It exposes isolated agent conversations through a stable HTTP API and Server-Sent Events (SSE).

It is intentionally a single-node foundation, not a hosted multi-tenant platform. The design keeps the boundaries needed for that direction: model aliases are allowlisted, each external session has persistent memory, a session remains bound to one model, and independent sessions can run concurrently.

## What is included

- POST /chat for JSON request/response interaction
- POST /chat/stream for SSE streaming
- GET /health and GET /models
- Model alias allowlisting through models.json
- Persistent conversation history across service restarts
- Per-session serialization and configurable cross-session concurrency
- DSH workspace-write sandbox policy, Todo, compaction, and optional DeepSeek web search

This repository deliberately does **not** include application Skills, model-generation adapters, media tooling, bot bridges, downloaded files, runtime state, or credentials. Those are deployment-specific extensions and should be integrated privately by each operator.

## Requirements

- Python 3.11 or newer
- A DeepSeek Harness-compatible model provider
- Node.js, as required by DeepSeek Harness runtime components

The included default route uses a standard DeepSeek API key. For another provider or endpoint, edit models.json and configure the provider in your DSH settings.

## Quick start

~~~bash
git clone https://github.com/YOUR_GITHUB_USER/dsh-as-a-service.git
cd dsh-as-a-service

python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
cp .env.example .env
# Edit .env and set DEEPSEEK_API_KEY.

./start.sh
~~~

The service listens on 127.0.0.1:8765 by default.

~~~bash
curl -sS http://127.0.0.1:8765/health
~~~

## API

### List available models

~~~bash
curl -sS http://127.0.0.1:8765/models
~~~

The default models.json intentionally contains only the portable deepseek route:

~~~json
{
  "default": "deepseek",
  "models": {
    "deepseek": {
      "provider": "deepseek",
      "endpoint": "deepseek-chat"
    }
  }
}
~~~

To expose multiple provider endpoints, add canonical aliases to models.json. Callers can select only configured aliases, never arbitrary endpoints or credentials.

### Non-streaming chat

~~~bash
curl -sS http://127.0.0.1:8765/chat -H 'Content-Type: application/json' -d '{"session_id":"demo-1","message":"Explain the files in the workspace.","model":"deepseek"}'
~~~

Example response:

~~~json
{
  "session_id": "demo-1",
  "model": "deepseek",
  "answer": "...",
  "finish_reason": "completed",
  "elapsed_ms": 1234
}
~~~

Reuse the same session_id to continue the conversation. The first request binds that session to its explicit model, or to the configured default. A later request that specifies another model returns 409 session_model_conflict; use a new session ID for a separate model context.

### Streaming chat (SSE)

~~~bash
curl -N -sS http://127.0.0.1:8765/chat/stream -H 'Content-Type: application/json' -d '{"session_id":"demo-1","message":"Give me a brief status update."}'
~~~

The standard SSE event sequence includes:

- session — external session ID and selected model
- assistant.delta — generated text chunks
- tool.call / tool.result — tool lifecycle notifications
- usage / status — optional runtime status
- done — final answer, finish reason, and elapsed time

Pass raw_events: true only when debugging and you need the underlying DSH events.

## Configuration

Copy .env.example to .env. The following configuration is most useful in a local deployment:

| Variable | Default | Purpose |
| --- | --- | --- |
| DEEPSEEK_API_KEY | — | Required for the default route. |
| DSH_HTTP_HOST | 127.0.0.1 | Bind address. Keep loopback unless you add an authentication layer. |
| DSH_HTTP_PORT | 8765 | Listen port. |
| DSH_HTTP_MAX_PARALLEL_SESSIONS | 4 | Maximum concurrent sessions. Requests within one session are serialized. |
| DSH_WORKSPACE | ./workspace | Agent filesystem workspace. |
| DSH_SESSION_ROOT | ./state/sessions | DSH event/session storage. |
| DSH_HTTP_CONVERSATION_ROOT | ./state/conversations | Restart-safe external conversation memory. |
| DSH_HTTP_MODELS_CONFIG | ./models.json | Alternate model alias catalog. |

state/, workspace/, .env, and models.local.json are ignored by Git. Do not commit credentials, downloaded content, runtime transcripts, or account-specific endpoint IDs.

## Security

This service has **no HTTP authentication** and is intentionally bound to loopback by default. Do not expose it directly to untrusted networks.

The DSH agent uses workspace-write policy for filesystem mutation, but read and network capabilities are not a multi-tenant isolation boundary. Before putting this behind a public or shared endpoint, add authentication, tenant-aware storage, request limits, stronger sandboxing, observability, and policy enforcement appropriate for your environment.

## Development

~~~bash
.venv/bin/python -m unittest discover -s tests -v
~~~

## License

[MIT](LICENSE). DeepSeek Harness is used under its upstream license; see its repository for details.
