#!/usr/bin/env bash
set -euo pipefail

service_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)

if [ -f "$service_dir/.env" ]; then
  set -a
  # shellcheck disable=SC1091
  . "$service_dir/.env"
  set +a
fi

exec "$service_dir/.venv/bin/python" "$service_dir/server.py"
