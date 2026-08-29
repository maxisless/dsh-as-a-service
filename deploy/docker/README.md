# Docker deployment

This Compose stack runs the stable Python Worker only. It stores workspace,
DSH session data, conversation memory, and package cache in the named Docker
volume dsh-python-data. The host port is bound to 127.0.0.1:8765 by default.

## First start

    cp .env.example .env
    cp models.example.json models.json
    mkdir -p dsh-home
    # Edit .env and set DEEPSEEK_API_KEY.
    docker compose -f compose.yml up --build -d
    curl -sS http://127.0.0.1:8765/health

Run the commands from this directory. The models.json file is mounted read-only
into the container. Keep it out of Git when it contains account-specific model
endpoint IDs.

For a provider configured through DSH pi-ai, put settings.yaml and, when needed,
.credentials.yaml in dsh-home. The whole directory is mounted read-only at the
container user's DSH_HOME. Both the directory and its files should be readable
only by the container user (uid 10001) on the host.

## Restricted Docker networks

The default base image is python:3.11-slim-bookworm. If a Docker host cannot
reach Docker Hub but already has a compatible Debian or Ubuntu image, set
DSH_BASE_IMAGE in .env to that local image tag. The Dockerfile installs a Python
virtual environment and all Worker dependencies inside the image.

## Operations

    docker compose -f compose.yml ps
    docker compose -f compose.yml logs -f python-worker
    docker compose -f compose.yml down

To expose this outside the server, put an authenticated TLS reverse proxy in
front of it. Do not change the loopback binding to a public address until an
authentication and network policy boundary is in place.
