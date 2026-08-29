# The DSH SDK installs a target-native manylinux runtime wheel. Debian/glibc is
# required; Alpine/musl is intentionally not used. DSH_BASE_IMAGE can point to
# a compatible locally cached Debian or Ubuntu image in restricted networks.
ARG BASE_IMAGE=python:3.11-slim-bookworm
FROM ${BASE_IMAGE}

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    DSH_HTTP_HOST=0.0.0.0 \
    DSH_HTTP_PORT=8765 \
    DSH_WORKSPACE=/data/workspace \
    DSH_SESSION_ROOT=/data/state/sessions \
    DSH_HTTP_CONVERSATION_ROOT=/data/state/conversations \
    PKG_CACHE_PATH=/data/state/pkg-cache \
    DSH_HTTP_MODELS_CONFIG=/config/models.json \
    PATH=/opt/venv/bin:$PATH

WORKDIR /app

RUN apt-get update \
    && apt-get install --yes --no-install-recommends python3-venv ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && python3 -m venv /opt/venv \
    && groupadd --system --gid 10001 dsh \
    && useradd --system --uid 10001 --gid dsh --create-home dsh \
    && mkdir -p /data/workspace /data/state/sessions /data/state/conversations /data/state/pkg-cache /config \
    && chown -R dsh:dsh /app /data /config /opt/venv

COPY implementations/python/requirements.txt ./requirements.txt
COPY deploy/docker/wheelhouse /wheelhouse
RUN if find /wheelhouse -type f -name '*.whl' -print -quit | grep -q .; then \
      python -m pip install --no-index --find-links=/wheelhouse --requirement requirements.txt; \
    else \
      python -m pip install --requirement requirements.txt; \
    fi

COPY --chown=dsh:dsh implementations/python/server.py implementations/python/cordis.yml ./
COPY --chown=dsh:dsh implementations/python/models.json /config/models.json

USER dsh

EXPOSE 8765

HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
    CMD ["python", "-c", "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8765/health', timeout=3).read()"]

CMD ["python", "server.py"]
