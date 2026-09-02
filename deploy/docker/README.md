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

For a host where deployment-only assets must stay outside this Git checkout,
point Compose at their secure locations instead:

```bash
DSH_WORKER_ENV_FILE=/secure/dsh/worker.env \
DSH_MODELS_FILE=/secure/dsh/models.json \
DSH_HOME_DIR=/secure/dsh/dsh-home \
docker compose -f compose.yml up --build -d
```

The same variables are used by `deploy/bootstrap-machine.sh` when preparing a
fresh machine.

## Restricted Docker networks

The default base image is python:3.11-slim-bookworm. If a Docker host cannot
reach Docker Hub but already has a compatible Debian or Ubuntu image, set
DSH_BASE_IMAGE in .env to that local image tag. The Dockerfile installs a Python
virtual environment and all Worker dependencies inside the image.

For a host with very slow PyPI access, download compatible wheels into the
ignored wheelhouse directory before building. When wheelhouse contains .whl
files, the Dockerfile installs only from those files; otherwise it installs from
the configured Python package index.

For a Linux x86_64 host running Python 3.12, one compatible wheelhouse can be
prepared on a machine with fast PyPI access as follows:

    python -m pip download --dest wheelhouse --only-binary=:all: \
      --platform manylinux_2_28_x86_64 --platform manylinux2014_x86_64 \
      --implementation cp --python-version 312 --abi cp312 \
      --requirement implementations/python/requirements.txt

Do not commit the downloaded wheels. Copy them into deploy/docker/wheelhouse on
the target server before invoking Docker Compose.

## Operations

    docker compose -f compose.yml ps
    docker compose -f compose.yml logs -f python-worker
    docker compose -f compose.yml down

To expose this outside the server, put an authenticated TLS reverse proxy in
front of it. Do not change the loopback binding to a public address until an
authentication and network policy boundary is in place.
