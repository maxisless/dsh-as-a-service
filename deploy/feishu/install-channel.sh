#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  sudo deploy/feishu/install-channel.sh \
    --profile /secure/instances/example/profile.json \
    --secrets-file /secure/instances/example/secrets.env \
    --install-root /srv/dsh-as-a-service \
    --service-user dshbridge

Installs a generic Feishu channel bridge. The profile is non-secret JSON.
The secrets file stays outside the Git checkout and must be mode 0600.
The script deliberately creates fresh bridge state; it does not copy messages,
attachments, sessions, or task databases from another machine.
EOF
}

PROFILE=""
SECRETS_FILE=""
INSTALL_ROOT=""
SERVICE_USER="dshbridge"
CONFIG_ROOT=""
STATE_ROOT=""
ENABLE_SERVICE=true

while [[ $# -gt 0 ]]; do
  case "$1" in
    --profile) PROFILE=${2:?missing value for --profile}; shift 2 ;;
    --secrets-file) SECRETS_FILE=${2:?missing value for --secrets-file}; shift 2 ;;
    --install-root) INSTALL_ROOT=${2:?missing value for --install-root}; shift 2 ;;
    --service-user) SERVICE_USER=${2:?missing value for --service-user}; shift 2 ;;
    --config-root) CONFIG_ROOT=${2:?missing value for --config-root}; shift 2 ;;
    --state-root) STATE_ROOT=${2:?missing value for --state-root}; shift 2 ;;
    --no-enable) ENABLE_SERVICE=false; shift ;;
    --help|-h) usage; exit 0 ;;
    *) echo "Unknown argument: $1" >&2; usage >&2; exit 2 ;;
  esac
done

if [[ ${EUID} -ne 0 ]]; then
  echo "Run this installer as root." >&2
  exit 1
fi
if [[ -z ${PROFILE} || -z ${SECRETS_FILE} || -z ${INSTALL_ROOT} ]]; then
  echo "--profile, --secrets-file, and --install-root are required." >&2
  usage >&2
  exit 2
fi

PROFILE=$(realpath "${PROFILE}")
SECRETS_FILE=$(realpath "${SECRETS_FILE}")
INSTALL_ROOT=$(realpath "${INSTALL_ROOT}")
if [[ ! -f ${PROFILE} || ! -f ${SECRETS_FILE} || ! -f ${INSTALL_ROOT}/integrations/feishu/bridge.py ]]; then
  echo "Profile, secrets file, or generic bridge source is missing." >&2
  exit 1
fi

if [[ -z ${CONFIG_ROOT} ]]; then
  CONFIG_ROOT="/etc/dsh/feishu/$(basename "$(dirname "${PROFILE}")")"
fi
if [[ -z ${STATE_ROOT} ]]; then
  STATE_ROOT="/var/lib/dsh-feishu-channel/$(basename "${CONFIG_ROOT}")"
fi
CONFIG_ROOT=$(realpath -m "${CONFIG_ROOT}")
STATE_ROOT=$(realpath -m "${STATE_ROOT}")

secrets_mode=$(stat -c '%a' "${SECRETS_FILE}")
if (( 8#${secrets_mode} & 077 )); then
  echo "Secrets file must not be readable by group or other users; run chmod 600." >&2
  exit 1
fi

if ! id -u "${SERVICE_USER}" >/dev/null 2>&1; then
  useradd --system --create-home --home-dir "/var/lib/${SERVICE_USER}" --shell /usr/sbin/nologin "${SERVICE_USER}"
fi
DOCKER_SOCKET_GROUP=$(stat -c '%G' /var/run/docker.sock 2>/dev/null || stat -f '%Sg' /var/run/docker.sock 2>/dev/null || true)
if [[ -z ${DOCKER_SOCKET_GROUP} || ${DOCKER_SOCKET_GROUP} == UNKNOWN ]]; then
  echo "Docker socket is unavailable; start Docker before installing a media-capable channel bridge." >&2
  exit 1
fi
usermod -aG "${DOCKER_SOCKET_GROUP}" "${SERVICE_USER}"

python3 - "${PROFILE}" "${INSTALL_ROOT}" <<'PY'
import os
import sys
from pathlib import Path

profile_path = Path(sys.argv[1])
install_root = Path(sys.argv[2])
sys.path.insert(0, str(install_root))
from integrations.feishu.profile import load_profile

profile = load_profile(profile_path)
print(f"Validated Feishu channel profile: {profile.instance_id}")
PY

VENV="${INSTALL_ROOT}/.venv-feishu-channel"
if [[ ! -x ${VENV}/bin/python ]]; then
  python3 -m venv "${VENV}"
fi
"${VENV}/bin/python" -m pip install --upgrade pip
"${VENV}/bin/python" -m pip install -r "${INSTALL_ROOT}/integrations/feishu/requirements.txt"

install -d -m 0750 -o root -g "${SERVICE_USER}" "${CONFIG_ROOT}"
install -d -m 0750 -o "${SERVICE_USER}" -g "${SERVICE_USER}" "${STATE_ROOT}" "${STATE_ROOT}/artifacts"
profile_destination="${CONFIG_ROOT}/profile.json"
secrets_destination="${CONFIG_ROOT}/secrets.env"
if [[ ${PROFILE} != "${profile_destination}" ]]; then
  install -m 0640 -o root -g "${SERVICE_USER}" "${PROFILE}" "${profile_destination}"
else
  chown root:"${SERVICE_USER}" "${profile_destination}"
  chmod 0640 "${profile_destination}"
fi
if [[ ${SECRETS_FILE} != "${secrets_destination}" ]]; then
  install -m 0600 -o root -g "${SERVICE_USER}" "${SECRETS_FILE}" "${secrets_destination}"
else
  chown root:"${SERVICE_USER}" "${secrets_destination}"
  chmod 0600 "${secrets_destination}"
fi

unit_template="${INSTALL_ROOT}/deploy/feishu/systemd/dsh-feishu-channel@.service"
unit_path="/etc/systemd/system/dsh-feishu-channel@${SERVICE_USER}.service"
sed \
  -e "s|__INSTALL_ROOT__|${INSTALL_ROOT}|g" \
  -e "s|__CONFIG_ROOT__|${CONFIG_ROOT}|g" \
  -e "s|__STATE_ROOT__|${STATE_ROOT}|g" \
  -e "s|__VENV__|${VENV}|g" \
  "${unit_template}" > "${unit_path}"
chmod 0644 "${unit_path}"

systemctl daemon-reload
if [[ ${ENABLE_SERVICE} == true ]]; then
  systemctl enable --now "dsh-feishu-channel@${SERVICE_USER}.service"
  systemctl --no-pager --full status "dsh-feishu-channel@${SERVICE_USER}.service"
else
  echo "Installed but did not enable dsh-feishu-channel@${SERVICE_USER}.service"
fi
EOF
