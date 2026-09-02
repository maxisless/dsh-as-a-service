#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  sudo deploy/bootstrap-machine.sh \
    --repo-url https://github.com/owner/dsh-as-a-service.git \
    --install-root /srv/dsh-as-a-service \
    --worker-env /secure/dsh/worker.env \
    --models-file /secure/dsh/models.json \
    --dsh-home /secure/dsh/dsh-home \
    --channel-profile /secure/dsh-instances/example/profile.json \
    --channel-secrets /secure/dsh-instances/example/secrets.env

Creates a fresh host installation from public source and supplied private
configuration. It never imports an old bridge database, attachments, logs, or
conversation state. Omit both --channel-* options to install only the Worker.
EOF
}

REPO_URL=""
REF="main"
INSTALL_ROOT=""
WORKER_ENV=""
MODELS_FILE=""
DSH_HOME_DIR=""
CHANNEL_PROFILE=""
CHANNEL_SECRETS=""
CHANNEL_CONFIG_ROOT=""
CHANNEL_STATE_ROOT=""
CHANNEL_SERVICE_USER="dshbridge"
COMPOSE_PROJECT_NAME="dsh"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --repo-url) REPO_URL=${2:?missing value for --repo-url}; shift 2 ;;
    --ref) REF=${2:?missing value for --ref}; shift 2 ;;
    --install-root) INSTALL_ROOT=${2:?missing value for --install-root}; shift 2 ;;
    --worker-env) WORKER_ENV=${2:?missing value for --worker-env}; shift 2 ;;
    --models-file) MODELS_FILE=${2:?missing value for --models-file}; shift 2 ;;
    --dsh-home) DSH_HOME_DIR=${2:?missing value for --dsh-home}; shift 2 ;;
    --channel-profile) CHANNEL_PROFILE=${2:?missing value for --channel-profile}; shift 2 ;;
    --channel-secrets) CHANNEL_SECRETS=${2:?missing value for --channel-secrets}; shift 2 ;;
    --channel-config-root) CHANNEL_CONFIG_ROOT=${2:?missing value for --channel-config-root}; shift 2 ;;
    --channel-state-root) CHANNEL_STATE_ROOT=${2:?missing value for --channel-state-root}; shift 2 ;;
    --channel-service-user) CHANNEL_SERVICE_USER=${2:?missing value for --channel-service-user}; shift 2 ;;
    --compose-project-name) COMPOSE_PROJECT_NAME=${2:?missing value for --compose-project-name}; shift 2 ;;
    --help|-h) usage; exit 0 ;;
    *) echo "Unknown argument: $1" >&2; usage >&2; exit 2 ;;
  esac
done

if [[ ${EUID} -ne 0 ]]; then
  echo "Run this bootstrap script as root." >&2
  exit 1
fi
if [[ -z ${REPO_URL} || -z ${INSTALL_ROOT} || -z ${WORKER_ENV} || -z ${MODELS_FILE} || -z ${DSH_HOME_DIR} ]]; then
  echo "--repo-url, --install-root, --worker-env, --models-file, and --dsh-home are required." >&2
  usage >&2
  exit 2
fi
if [[ -n ${CHANNEL_PROFILE} && -z ${CHANNEL_SECRETS} ]] || [[ -z ${CHANNEL_PROFILE} && -n ${CHANNEL_SECRETS} ]]; then
  echo "--channel-profile and --channel-secrets must be supplied together." >&2
  exit 2
fi

for command in git docker curl; do
  command -v "${command}" >/dev/null 2>&1 || { echo "Required command is missing: ${command}" >&2; exit 1; }
done
docker compose version >/dev/null

WORKER_ENV=$(realpath "${WORKER_ENV}")
MODELS_FILE=$(realpath "${MODELS_FILE}")
DSH_HOME_DIR=$(realpath "${DSH_HOME_DIR}")
INSTALL_ROOT=$(realpath -m "${INSTALL_ROOT}")
if [[ ! -f ${WORKER_ENV} || ! -f ${MODELS_FILE} || ! -d ${DSH_HOME_DIR} ]]; then
  echo "Worker configuration must contain an env file, models file, and DSH_HOME directory." >&2
  exit 1
fi
if [[ -e ${INSTALL_ROOT} ]]; then
  echo "Installation root already exists; bootstrap only targets a fresh machine/root." >&2
  exit 1
fi

install -d -m 0755 "$(dirname "${INSTALL_ROOT}")"
git clone --branch "${REF}" --single-branch "${REPO_URL}" "${INSTALL_ROOT}"

export DSH_WORKER_ENV_FILE="${WORKER_ENV}"
export DSH_MODELS_FILE="${MODELS_FILE}"
export DSH_HOME_DIR="${DSH_HOME_DIR}"
export COMPOSE_PROJECT_NAME
docker compose -f "${INSTALL_ROOT}/deploy/docker/compose.yml" --project-directory "${INSTALL_ROOT}/deploy/docker" up --build -d
worker_ready=false
for _attempt in $(seq 1 30); do
  if curl --fail --silent --show-error http://127.0.0.1:8765/health >/dev/null 2>&1; then
    worker_ready=true
    break
  fi
  sleep 2
done
if [[ ${worker_ready} != true ]]; then
  echo "Worker did not become healthy within 60 seconds." >&2
  docker compose -f "${INSTALL_ROOT}/deploy/docker/compose.yml" --project-directory "${INSTALL_ROOT}/deploy/docker" logs --tail 200 python-worker >&2 || true
  exit 1
fi

if [[ -n ${CHANNEL_PROFILE} ]]; then
  channel_args=(
    --profile "${CHANNEL_PROFILE}"
    --secrets-file "${CHANNEL_SECRETS}"
    --install-root "${INSTALL_ROOT}"
    --service-user "${CHANNEL_SERVICE_USER}"
  )
  if [[ -n ${CHANNEL_CONFIG_ROOT} ]]; then
    channel_args+=(--config-root "${CHANNEL_CONFIG_ROOT}")
  fi
  if [[ -n ${CHANNEL_STATE_ROOT} ]]; then
    channel_args+=(--state-root "${CHANNEL_STATE_ROOT}")
  fi
  "${INSTALL_ROOT}/deploy/feishu/install-channel.sh" "${channel_args[@]}"
fi

echo "Bootstrap completed for ${INSTALL_ROOT}."
