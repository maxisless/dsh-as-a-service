#!/usr/bin/env python3
"""Configuration-driven Feishu WebSocket bridge for a local DSH Worker.

Credentials are read from a separately permissioned environment file. They must
never be written to this source tree, logs, Git, or chat.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import mimetypes
import os
import re
import shutil
import sqlite3
import subprocess
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from pathlib import PurePosixPath
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit, urlunsplit
from urllib.request import Request, urlopen

import lark_oapi as lark

from integrations.feishu.bridge_core import (
    InboundMedia, extract_inbound_content, normalized_artifact, prompt_with_inbound_media,
    prompt_with_message_context, prompt_with_sender_context, session_id_for, should_handle,
)
from integrations.feishu.profile import InstanceProfile, load_profile, secret_from_environment


LOG = logging.getLogger("feishu_channel")

PROFILE: InstanceProfile | None = None
APP_ID = ""
APP_SECRET = ""
BOT_OPEN_ID = ""
DSH_URL = ""
INTERNAL_BRIDGE_TOKEN = ""
STATE_DB = Path("./state/feishu-channel.sqlite3").resolve()
MAX_WORKERS = 4
REQUEST_TIMEOUT = 2400
MAX_MESSAGE_CHARS = 20000
MAX_REPLY_CHARS = 20000
CARD_UPDATE_INTERVAL = 1.0
GROUP_REQUIRE_MENTION = True
CONTACT_ENRICHMENT = True
WORKER_CONTAINER = ""
ARTIFACT_DIR = Path("./state/artifacts").resolve()
MAX_IMAGE_ARTIFACT_BYTES = 5 * 1024 * 1024
MAX_AUDIO_ARTIFACT_BYTES = 20 * 1024 * 1024
MAX_VIDEO_ARTIFACT_BYTES = 512 * 1024 * 1024
MAX_FILE_ARTIFACT_BYTES = 100 * 1024 * 1024
ARTIFACT_COPY_TIMEOUT_SECONDS = 60
FFMPEG_BIN = shutil.which("ffmpeg") or "ffmpeg"
FFPROBE_BIN = shutil.which("ffprobe") or "ffprobe"
MAX_INBOUND_MEDIA_ITEMS = 12
DELIVERY_REPLAY_INTERVAL_SECONDS = 60
DISPLAY_NAME = "Assistant"
DISPLAY_SUBTITLE = "正在为你处理"
INBOUND_MEDIA_LIMITS = {"image": 30 * 1024 * 1024, "audio": 25 * 1024 * 1024, "video": 512 * 1024 * 1024}
INBOUND_EXTENSIONS = {
    "image": {".bmp", ".gif", ".jpeg", ".jpg", ".png", ".webp"},
    "audio": {".aac", ".flac", ".m4a", ".mp3", ".oga", ".ogg", ".opus", ".wav"},
    "video": {".avi", ".m4v", ".mkv", ".mov", ".mp4", ".mpeg", ".mpg", ".webm"},
}
INBOUND_CONTENT_TYPE_EXTENSIONS = {
    "image/jpeg": ".jpg", "image/png": ".png", "image/webp": ".webp", "image/gif": ".gif",
    "audio/mpeg": ".mp3", "audio/mp4": ".m4a", "audio/ogg": ".ogg", "audio/opus": ".opus", "audio/wav": ".wav",
    "video/mp4": ".mp4", "video/quicktime": ".mov", "video/webm": ".webm", "video/x-matroska": ".mkv",
}
ARTIFACT_TYPES = frozenset({"image", "audio", "video", "file"})

_database_lock = threading.Lock()
_workers: ThreadPoolExecutor | None = None
_delivery_replay_stop = threading.Event()


def configure(profile_path: str | Path) -> InstanceProfile:
    """Load one non-secret instance profile and resolve its secret references."""
    global PROFILE, APP_ID, APP_SECRET, BOT_OPEN_ID, DSH_URL, INTERNAL_BRIDGE_TOKEN
    global STATE_DB, MAX_WORKERS, REQUEST_TIMEOUT, MAX_MESSAGE_CHARS, MAX_REPLY_CHARS
    global CARD_UPDATE_INTERVAL, GROUP_REQUIRE_MENTION, CONTACT_ENRICHMENT, WORKER_CONTAINER
    global ARTIFACT_DIR, MAX_INBOUND_MEDIA_ITEMS, DELIVERY_REPLAY_INTERVAL_SECONDS
    global DISPLAY_NAME, DISPLAY_SUBTITLE, _workers

    profile = load_profile(Path(profile_path))
    PROFILE = profile
    APP_ID = secret_from_environment(profile.credentials.app_id_env)
    APP_SECRET = secret_from_environment(profile.credentials.app_secret_env)
    BOT_OPEN_ID = secret_from_environment(profile.credentials.bot_open_id_env, required=False)
    INTERNAL_BRIDGE_TOKEN = secret_from_environment(profile.credentials.internal_bridge_token_env)
    DSH_URL = profile.worker_chat_url
    STATE_DB = Path(os.environ.get("FEISHU_CHANNEL_STATE_DB", "./state/feishu-channel.sqlite3")).expanduser().resolve()
    ARTIFACT_DIR = Path(os.environ.get("FEISHU_CHANNEL_ARTIFACT_DIR", "./state/artifacts")).expanduser().resolve()
    MAX_WORKERS = profile.max_workers
    REQUEST_TIMEOUT = profile.request_timeout_seconds
    MAX_MESSAGE_CHARS = profile.max_message_chars
    MAX_REPLY_CHARS = profile.max_reply_chars
    CARD_UPDATE_INTERVAL = profile.card_update_interval_seconds
    GROUP_REQUIRE_MENTION = profile.group_require_mention
    CONTACT_ENRICHMENT = profile.contact_enrichment
    WORKER_CONTAINER = profile.worker_container
    MAX_INBOUND_MEDIA_ITEMS = profile.max_inbound_media_items
    DELIVERY_REPLAY_INTERVAL_SECONDS = profile.delivery_replay_interval_seconds
    DISPLAY_NAME = profile.display_name
    DISPLAY_SUBTITLE = profile.display_subtitle
    if _workers is not None:
        _workers.shutdown(wait=False, cancel_futures=True)
    _workers = ThreadPoolExecutor(max_workers=MAX_WORKERS, thread_name_prefix=f"feishu-{profile.instance_id}")
    return profile

def require_configuration() -> None:
    if PROFILE is None:
        raise RuntimeError("channel profile is not loaded")
    if not APP_ID.startswith("cli_"):
        raise RuntimeError("configured Feishu App ID must start with cli_")
    if len(INTERNAL_BRIDGE_TOKEN) < 32:
        raise RuntimeError("internal bridge token must be a server-managed secret of at least 32 characters")
    worker_base_url()


def worker_base_url() -> str:
    """Derive the local Worker origin without allowing a separate arbitrary URL."""
    parsed = urlsplit(DSH_URL)
    path = parsed.path.rstrip("/")
    if not parsed.scheme or not parsed.netloc or not path.endswith("/chat"):
        raise RuntimeError("FEISHU_WORKER_CHAT_URL must be a Worker /chat URL")
    return urlunsplit((parsed.scheme, parsed.netloc, path[:-5], "", "")).rstrip("/")


def internal_worker_url(path: str) -> str:
    if not path.startswith("/internal/"):
        raise ValueError("internal Worker path is invalid")
    return worker_base_url() + path


def internal_headers() -> dict[str, str]:
    return {"X-DSH-Internal-Token": INTERNAL_BRIDGE_TOKEN}


def init_db() -> None:
    STATE_DB.parent.mkdir(parents=True, exist_ok=True)
    ARTIFACT_DIR.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(STATE_DB) as database:
        database.execute("PRAGMA journal_mode=WAL")
        database.execute(
            """
            CREATE TABLE IF NOT EXISTS messages (
              message_id TEXT PRIMARY KEY,
              session_id TEXT NOT NULL,
              status TEXT NOT NULL,
              reply_message_id TEXT,
              error TEXT,
              updated_at INTEGER NOT NULL
            )
            """
        )


def safe_inbound_extension(media: InboundMedia, content_type: str | None, file_name: str | None) -> str:
    candidates = [file_name or media.file_name, mimetypes.guess_extension((content_type or "").split(";", 1)[0].strip().lower()) or ""]
    for candidate in candidates:
        suffix = Path(candidate).suffix.lower()
        if suffix in INBOUND_EXTENSIONS[media.kind]:
            return suffix
    extension = INBOUND_CONTENT_TYPE_EXTENSIONS.get((content_type or "").split(";", 1)[0].strip().lower(), "")
    if extension in INBOUND_EXTENSIONS[media.kind]:
        return extension
    raise RuntimeError(f"unsupported inbound {media.kind} format")


def stage_inbound_media(client: lark.Client, message_id: str, refs: list[InboundMedia]) -> tuple[list[dict[str, str]], Path | None]:
    """Download only trusted resources from the current Feishu message."""
    refs = refs[:MAX_INBOUND_MEDIA_ITEMS]
    if not refs:
        return [], None
    directory = ARTIFACT_DIR / "inbound" / hashlib.sha256(message_id.encode("utf-8")).hexdigest()[:24]
    directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    staged: list[dict[str, str]] = []
    try:
        for index, media in enumerate(refs, start=1):
            request = (
                lark.im.v1.GetMessageResourceRequest.builder()
                .message_id(message_id)
                .file_key(media.key)
                .type("image" if media.kind == "image" else "file")
                .build()
            )
            response = client.im.v1.message_resource.get(request)
            stream = response.file if response.success() else None
            if stream is None:
                raise RuntimeError(f"Feishu {media.kind} download failed code={response.code} msg={response.msg}")
            content_type = response.raw.headers.get("Content-Type") if response.raw is not None else None
            name = getattr(response, "file_name", "") or media.file_name
            suffix = safe_inbound_extension(media, content_type, name)
            target = directory / f"{media.kind}-{index}{suffix}"
            limit = INBOUND_MEDIA_LIMITS[media.kind]
            with target.open("xb") as output:
                total = 0
                while chunk := stream.read(1024 * 1024):
                    total += len(chunk)
                    if total > limit:
                        raise RuntimeError(f"inbound {media.kind} exceeds its size limit")
                    output.write(chunk)
            if target.stat().st_size < 1:
                raise RuntimeError(f"inbound {media.kind} is empty")
            os.chmod(target, 0o600)
            digest = hashlib.sha256()
            with target.open("rb") as handle:
                for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                    digest.update(chunk)
            staged.append({
                "kind": media.kind,
                "name": target.name,
                "content_type": (content_type or "").split(";", 1)[0].strip() or None,
                "size_bytes": target.stat().st_size,
                "sha256": digest.hexdigest(),
                "staged_path": str(target),
                "source_ref": media.key,
            })
        return staged, directory
    except Exception:
        shutil.rmtree(directory, ignore_errors=True)
        raise


def worker_session_workspace(session: dict[str, Any]) -> str:
    value = session.get("workspace")
    if not isinstance(value, str) or not value:
        raise RuntimeError("Worker did not return a session workspace")
    candidate = PurePosixPath(value)
    suffix = candidate.parts[-8:]
    if (
        not candidate.is_absolute()
        or ".." in candidate.parts
        or len(suffix) != 8
        or suffix[0] != "tenants"
        or suffix[2] != "agents"
        or suffix[7] != "workspace"
    ):
        raise RuntimeError("Worker returned an invalid session workspace")
    return str(candidate)


def resolve_worker_session(
    session_id: str, *, external_conversation_id: str | None = None, conversation_kind: str | None = None,
    principal_ref: str | None = None,
) -> dict[str, Any]:
    payload: dict[str, str] = {"session_id": session_id}
    if PROFILE is None:
        raise RuntimeError("channel profile is not loaded")
    payload["agent_id"] = PROFILE.agent_id
    if PROFILE.agent_version is not None:
        payload["agent_version"] = PROFILE.agent_version
    if external_conversation_id is not None and conversation_kind is not None:
        payload["external_conversation_id"] = external_conversation_id
        payload["conversation_kind"] = conversation_kind
    if principal_ref is not None:
        payload["principal_ref"] = principal_ref
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    request = Request(
        internal_worker_url("/internal/sessions/resolve"), data=body,
        headers={**internal_headers(), "Content-Type": "application/json"}, method="POST",
    )
    try:
        with urlopen(request, timeout=ARTIFACT_COPY_TIMEOUT_SECONDS) as response:
            value = json.loads(response.read())
    except HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")[:1000]
        raise RuntimeError(f"Worker session resolve HTTP {exc.code}: {detail}") from None
    except (URLError, OSError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"Worker session resolve failed: {exc}") from None
    if not isinstance(value, dict) or not isinstance(value.get("session_id"), str):
        raise RuntimeError("Worker returned an invalid session record")
    worker_session_workspace(value)
    return value


def prepare_worker_run(session_id: str, prompt: str, message_id: str) -> dict[str, Any]:
    body = json.dumps({"message": prompt, "source_ref": message_id}, ensure_ascii=False).encode("utf-8")
    request = Request(
        internal_worker_url(f"/internal/sessions/{session_id}/runs/prepare"), data=body,
        headers={**internal_headers(), "Content-Type": "application/json"}, method="POST",
    )
    try:
        with urlopen(request, timeout=ARTIFACT_COPY_TIMEOUT_SECONDS) as response:
            value = json.loads(response.read())
    except HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")[:1000]
        raise RuntimeError(f"Worker run prepare HTTP {exc.code}: {detail}") from None
    except (URLError, OSError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"Worker run prepare failed: {exc}") from None
    if not isinstance(value, dict) or not isinstance(value.get("run_id"), str) or not isinstance(value.get("inbox"), str):
        raise RuntimeError("Worker returned an invalid prepared run")
    return value


def finalize_worker_run_inputs(run_id: str, inputs: list[dict[str, Any]], prompt: str) -> dict[str, Any]:
    manifest = [{
        key: item[key]
        for key in ("kind", "name", "content_type", "size_bytes", "sha256", "staged_path", "workspace_path", "source_ref")
        if key in item
    } for item in inputs]
    body = json.dumps({"inputs": manifest, "message": prompt}, ensure_ascii=False).encode("utf-8")
    request = Request(
        internal_worker_url(f"/internal/runs/{run_id}/inputs/finalize"), data=body,
        headers={**internal_headers(), "Content-Type": "application/json"}, method="POST",
    )
    try:
        with urlopen(request, timeout=ARTIFACT_COPY_TIMEOUT_SECONDS) as response:
            value = json.loads(response.read())
    except HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")[:1000]
        raise RuntimeError(f"Worker input finalize HTTP {exc.code}: {detail}") from None
    except (URLError, OSError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"Worker input finalize failed: {exc}") from None
    if not isinstance(value, dict) or value.get("input_manifest_status") != "FINALIZED":
        raise RuntimeError("Worker did not finalize the input manifest")
    return value


def create_worker_delivery(run_id: str, message_id: str, *, suffix: str, artifact_id: str | None = None, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    idempotency_key = ("feishu-" + hashlib.sha256(f"{run_id}:{suffix}".encode("utf-8")).hexdigest())[:64]
    body = json.dumps({
        "destination_ref": message_id, "idempotency_key": idempotency_key,
        "artifact_id": artifact_id, "payload": payload or {},
    }, ensure_ascii=False).encode("utf-8")
    request = Request(
        internal_worker_url(f"/internal/runs/{run_id}/deliveries"), data=body,
        headers={**internal_headers(), "Content-Type": "application/json"}, method="POST",
    )
    try:
        with urlopen(request, timeout=ARTIFACT_COPY_TIMEOUT_SECONDS) as response:
            value = json.loads(response.read())
    except HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")[:1000]
        raise RuntimeError(f"Worker delivery create HTTP {exc.code}: {detail}") from None
    except (URLError, OSError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"Worker delivery create failed: {exc}") from None
    if not isinstance(value, dict) or not isinstance(value.get("delivery_id"), str):
        raise RuntimeError("Worker returned an invalid delivery")
    return value


def finish_worker_delivery(delivery_id: str, *, delivered: bool, provider_delivery_ref: str | None = None, error: str | None = None) -> None:
    body = json.dumps({
        "delivered": delivered, "provider_delivery_ref": provider_delivery_ref, "error": error,
    }, ensure_ascii=False).encode("utf-8")
    request = Request(
        internal_worker_url(f"/internal/deliveries/{delivery_id}/finish"), data=body,
        headers={**internal_headers(), "Content-Type": "application/json"}, method="POST",
    )
    try:
        with urlopen(request, timeout=ARTIFACT_COPY_TIMEOUT_SECONDS) as response:
            value = json.loads(response.read())
    except HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")[:1000]
        raise RuntimeError(f"Worker delivery finish HTTP {exc.code}: {detail}") from None
    except (URLError, OSError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"Worker delivery finish failed: {exc}") from None
    if not isinstance(value, dict) or value.get("status") not in {"DELIVERED", "RETRY"}:
        raise RuntimeError("Worker did not update delivery state")


def claim_worker_deliveries() -> list[dict[str, Any]]:
    request = Request(internal_worker_url("/internal/deliveries/claim"), headers=internal_headers(), method="GET")
    try:
        with urlopen(request, timeout=ARTIFACT_COPY_TIMEOUT_SECONDS) as response:
            value = json.loads(response.read())
    except HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")[:1000]
        raise RuntimeError(f"Worker delivery claim HTTP {exc.code}: {detail}") from None
    except (URLError, OSError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"Worker delivery claim failed: {exc}") from None
    rows = value.get("deliveries") if isinstance(value, dict) else None
    return [row for row in rows if isinstance(row, dict)] if isinstance(rows, list) else []


def begin_delivery(run_id: str | None, message_id: str, *, suffix: str, artifact_id: str | None = None, payload: dict[str, Any] | None = None) -> str | None:
    if run_id is None:
        return None
    delivery = create_worker_delivery(run_id, message_id, suffix=suffix, artifact_id=artifact_id, payload=payload)
    return str(delivery["delivery_id"])


def complete_delivery(delivery_id: str | None, provider_ref: str | None) -> None:
    if delivery_id is not None:
        finish_worker_delivery(delivery_id, delivered=True, provider_delivery_ref=provider_ref)


def fail_delivery(delivery_id: str | None, error: BaseException) -> None:
    if delivery_id is not None:
        try:
            finish_worker_delivery(delivery_id, delivered=False, error=f"{type(error).__name__}: {error}")
        except Exception:
            LOG.exception("could not record failed delivery delivery_id=%s", delivery_id)


def replay_due_text_deliveries(client: lark.Client) -> int:
    """Replay text-only deliveries without re-running an Agent or media tool."""
    completed = 0
    for delivery in claim_worker_deliveries():
        delivery_id = delivery.get("delivery_id")
        message_id = delivery.get("destination_ref")
        payload = delivery.get("payload")
        if not isinstance(delivery_id, str) or not isinstance(message_id, str) or not isinstance(payload, dict):
            continue
        if payload.get("kind") != "text":
            # Keep non-text deliveries visible as RETRY until a media-specific
            # replayer has the required Card/attachment context.
            try:
                finish_worker_delivery(delivery_id, delivered=False, error="media delivery replay pending")
            except Exception:
                LOG.exception("could not defer media delivery delivery_id=%s", delivery_id)
            continue
        text = payload.get("text")
        if not isinstance(text, str) or not text:
            finish_worker_delivery(delivery_id, delivered=False, error="text delivery payload missing")
            continue
        try:
            reply_id = reply_text_for_delivery(client, message_id, text, delivery_id)
            finish_worker_delivery(delivery_id, delivered=True, provider_delivery_ref=reply_id)
            completed += 1
        except Exception as exc:
            fail_delivery(delivery_id, exc)
    return completed


def delivery_replay_loop(client: lark.Client) -> None:
    while not _delivery_replay_stop.is_set():
        try:
            completed = replay_due_text_deliveries(client)
            if completed:
                LOG.info("replayed text deliveries count=%s", completed)
        except Exception as exc:
            LOG.warning("delivery replay unavailable: %s", exc)
        _delivery_replay_stop.wait(DELIVERY_REPLAY_INTERVAL_SECONDS)


def copy_inbound_media_to_worker(
    directory: Path, media: list[dict[str, Any]], inbox: str, workspace: str,
) -> list[dict[str, Any]]:
    """Copy trusted inbound media into exactly one prepared Run inbox."""
    if not WORKER_CONTAINER or any(character.isspace() for character in WORKER_CONTAINER):
        raise RuntimeError("configured Worker container is invalid")
    inbox_path = PurePosixPath(inbox)
    workspace_path = PurePosixPath(workspace)
    if not inbox_path.is_absolute() or "inbox" not in inbox_path.parts or ".." in inbox_path.parts:
        raise RuntimeError("Worker returned an invalid run inbox")
    try:
        relative_inbox = inbox_path.relative_to(workspace_path)
    except ValueError as exc:
        raise RuntimeError("Worker run inbox is outside its session workspace") from exc
    prepare = subprocess.run(
        ["docker", "exec", WORKER_CONTAINER, "mkdir", "-p", str(inbox_path)],
        check=False, capture_output=True, text=True, timeout=30,
    )
    if prepare.returncode != 0:
        raise RuntimeError("could not prepare the Worker inbound-media directory")
    completed = subprocess.run(
        ["docker", "cp", f"{directory}/.", f"{WORKER_CONTAINER}:{inbox_path}"],
        check=False, capture_output=True, text=True, timeout=ARTIFACT_COPY_TIMEOUT_SECONDS,
    )
    if completed.returncode != 0:
        detail = completed.stderr.strip().replace("\n", " ")[:300]
        raise RuntimeError(f"could not copy inbound media to Worker: {detail or 'docker cp failed'}")
    ownership = subprocess.run(
        ["docker", "exec", "-u", "0", WORKER_CONTAINER, "chown", "-R", "10001:10001", str(inbox_path)],
        check=False, capture_output=True, text=True, timeout=30,
    )
    if ownership.returncode != 0:
        raise RuntimeError("could not set Worker inbound-media ownership")
    permissions = subprocess.run(
        ["docker", "exec", "-u", "0", WORKER_CONTAINER, "chmod", "-R", "u=rwX,go=", str(inbox_path)],
        check=False, capture_output=True, text=True, timeout=30,
    )
    if permissions.returncode != 0:
        raise RuntimeError("could not set Worker inbound-media permissions")
    return [
        {**item, "path": str(relative_inbox / item["name"]), "staged_path": str(inbox_path / item["name"])}
        for item in media
    ]


def cleanup_inbound_media(directory: Path | None) -> None:
    if directory is not None:
        shutil.rmtree(directory, ignore_errors=True)


def claim_message(message_id: str, session_id: str) -> bool:
    with _database_lock, sqlite3.connect(STATE_DB) as database:
        cursor = database.execute(
            "INSERT OR IGNORE INTO messages(message_id, session_id, status, updated_at) VALUES (?, ?, 'processing', ?)",
            (message_id, session_id, int(time.time())),
        )
        return cursor.rowcount == 1


def finish_message(message_id: str, status: str, *, reply_message_id: str | None = None, error: str | None = None) -> None:
    with _database_lock, sqlite3.connect(STATE_DB) as database:
        database.execute(
            "UPDATE messages SET status=?, reply_message_id=?, error=?, updated_at=? WHERE message_id=?",
            (status, reply_message_id, error, int(time.time()), message_id),
        )


def stream_dsh_chat(run_id: str):
    """Yield events from one already-prepared, manifest-finalized Feishu Run."""
    request = Request(
        internal_worker_url(f"/internal/runs/{run_id}/stream"),
        headers=internal_headers(),
        method="POST",
    )
    try:
        with urlopen(request, timeout=REQUEST_TIMEOUT) as response:
            event_name = "message"
            data_lines: list[str] = []
            for raw in response:
                line = raw.decode("utf-8").rstrip("\r\n")
                if not line:
                    if data_lines:
                        payload = json.loads("\n".join(data_lines))
                        if isinstance(payload, dict):
                            yield event_name, payload
                    event_name = "message"
                    data_lines = []
                elif line.startswith("event:"):
                    event_name = line[6:].strip() or "message"
                elif line.startswith("data:"):
                    data_lines.append(line[5:].lstrip())
    except HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")[:1000]
        raise RuntimeError(f"worker stream HTTP {exc.code}: {detail}") from None
    except (URLError, OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"worker stream failed: {exc}") from None


def streaming_card_json() -> dict[str, Any]:
    """Card 2.0 surface. Do not disclose implementation or model branding."""
    return {
        "schema": "2.0",
        "config": {
            "update_multi": True,
            # Card 2.0's audio player is not forward-compatible. Keep the
            # completed response private to its original chat so images and
            # generated audio remain playable in the same card.
            "enable_forward": False,
            "width_mode": "default",
            "streaming_mode": True,
            "summary": {"content": f"{DISPLAY_NAME}正在回答"},
            "streaming_config": {
                "print_frequency_ms": {"default": 70, "android": 70, "ios": 70, "pc": 70},
                "print_step": {"default": 1, "android": 1, "ios": 1, "pc": 1},
                "print_strategy": "fast",
            },
        },
        "header": {
            "title": {"tag": "plain_text", "content": DISPLAY_NAME},
            "subtitle": {"tag": "plain_text", "content": DISPLAY_SUBTITLE},
            "template": "blue",
            "icon": {"tag": "standard_icon", "token": "myai_colorful"},
        },
        "body": {
            "direction": "vertical",
            "padding": "12px 12px 20px 12px",
            "vertical_spacing": "12px",
            "elements": [
                {"tag": "markdown", "element_id": "answer", "content": "正在思考…"},
                {"tag": "markdown", "element_id": "status", "content": "<font color='grey'>正在处理…</font>", "text_size": "notation"},
            ],
        },
    }



class StreamingCard:
    def __init__(self, client: lark.Client, message_id: str) -> None:
        self.client = client
        self.message_id = message_id
        self.card_id = ""
        self.sequence = 0
        self.last_answer = ""
        self.last_status = ""
        self.reply_message_id: str | None = None
        self.media_index = 0

    def next_sequence(self) -> int:
        self.sequence += 1
        return self.sequence

    def start(self) -> None:
        card_body = (
            lark.cardkit.v1.CreateCardRequestBody.builder()
            .type("card_json")
            .data(json.dumps(streaming_card_json(), ensure_ascii=False, separators=(",", ":")))
            .build()
        )
        response = self.client.cardkit.v1.card.create(
            lark.cardkit.v1.CreateCardRequest.builder().request_body(card_body).build()
        )
        if not response.success() or response.data is None or not response.data.card_id:
            raise RuntimeError(f"card creation failed code={response.code} msg={response.msg}")
        self.card_id = response.data.card_id
        reply_body = (
            lark.im.v1.ReplyMessageRequestBody.builder()
            .content(json.dumps({"type": "card", "data": {"card_id": self.card_id}}, ensure_ascii=False, separators=(",", ":")))
            .msg_type("interactive")
            .uuid(("feishu-" + hashlib.sha256(self.message_id.encode("utf-8")).hexdigest()[:28] + "-card")[:50])
            .build()
        )
        reply = self.client.im.v1.message.reply(
            lark.im.v1.ReplyMessageRequest.builder().message_id(self.message_id).request_body(reply_body).build()
        )
        if not reply.success():
            raise RuntimeError(f"card reply failed code={reply.code} msg={reply.msg}")
        self.reply_message_id = str(reply.data.message_id) if reply.data is not None and reply.data.message_id else None

    def update_element(self, element_id: str, content: str) -> None:
        body = (
            lark.cardkit.v1.ContentCardElementRequestBody.builder()
            .content(content)
            .sequence(self.next_sequence())
            .uuid(uuid.uuid4().hex)
            .build()
        )
        request = (
            lark.cardkit.v1.ContentCardElementRequest.builder()
            .card_id(self.card_id)
            .element_id(element_id)
            .request_body(body)
            .build()
        )
        response = self.client.cardkit.v1.card_element.content(request)
        if not response.success():
            raise RuntimeError(f"card update failed code={response.code} msg={response.msg}")

    def update(self, answer: str, status: str) -> None:
        visible_answer = answer[:MAX_REPLY_CHARS] or "正在思考…"
        if visible_answer != self.last_answer:
            self.update_element("answer", visible_answer)
            self.last_answer = visible_answer
        visible_status = f"<font color='grey'>{status}</font>"
        if visible_status != self.last_status:
            self.update_element("status", visible_status)
            self.last_status = visible_status

    def append_elements(self, elements: list[dict[str, Any]]) -> None:
        """Append media components to the same Card 2.0 entity."""
        body = (
            lark.cardkit.v1.CreateCardElementRequestBody.builder()
            .type("append")
            .elements(json.dumps(elements, ensure_ascii=False, separators=(",", ":")))
            .sequence(self.next_sequence())
            .uuid(uuid.uuid4().hex)
            .build()
        )
        request = (
            lark.cardkit.v1.CreateCardElementRequest.builder()
            .card_id(self.card_id)
            .request_body(body)
            .build()
        )
        response = self.client.cardkit.v1.card_element.create(request)
        if not response.success():
            raise RuntimeError(f"card media append failed code={response.code} msg={response.msg}")

    def append_image(self, image_key: str) -> None:
        self.media_index += 1
        self.append_elements([{
            "tag": "img",
            "element_id": f"media{self.media_index}",
            "img_key": image_key,
            "alt": {"tag": "plain_text", "content": "生成的图片"},
            "scale_type": "fit_horizontal",
            "preview": True,
        }])

    def append_audio(self, file_key: str) -> None:
        self.media_index += 1
        self.append_elements([{
            "tag": "audio",
            "element_id": f"media{self.media_index}",
            "file_key": file_key,
            "audio_id": f"audio{self.media_index}",
            "show_time": True,
            "style": "speak",
        }])

    def append_audio_download_notice(self) -> None:
        self.append_elements([{
            "tag": "markdown",
            "element_id": f"download{uuid.uuid4().hex[:12]}",
            "content": "<font color='grey'>原始 MP3 下载文件已作为本条请求的附件发送。</font>",
            "text_size": "notation",
        }])

    def disable_forwarding_for_audio(self) -> None:
        """Apply the Card 2.0 prerequisite for adding an audio player."""
        settings = {"config": {"enable_forward": False}}
        body = (
            lark.cardkit.v1.SettingsCardRequestBody.builder()
            .settings(json.dumps(settings, ensure_ascii=False, separators=(",", ":")))
            .sequence(self.next_sequence())
            .uuid(uuid.uuid4().hex)
            .build()
        )
        request = (
            lark.cardkit.v1.SettingsCardRequest.builder()
            .card_id(self.card_id)
            .request_body(body)
            .build()
        )
        response = self.client.cardkit.v1.card.settings(request)
        if not response.success():
            raise RuntimeError(f"card audio settings update failed code={response.code} msg={response.msg}")

    @classmethod
    def existing_card(cls, client: lark.Client, message_id: str, card_id: str) -> "StreamingCard":
        """Attach media to an already-sent card using a monotonic sequence."""
        card = cls(client, message_id)
        card.card_id = card_id
        # CardKit only requires a later sequence number. Existing streamed cards
        # have small local sequences; a current Unix timestamp is safely later.
        card.sequence = int(time.time())
        return card

    def close(self, success: bool) -> None:
        settings = {"config": {"streaming_mode": False, "summary": {"content": "回答完成" if success else "处理失败"}}}
        body = (
            lark.cardkit.v1.SettingsCardRequestBody.builder()
            .settings(json.dumps(settings, ensure_ascii=False, separators=(",", ":")))
            .sequence(self.next_sequence())
            .uuid(uuid.uuid4().hex)
            .build()
        )
        response = self.client.cardkit.v1.card.settings(
            lark.cardkit.v1.SettingsCardRequest.builder().card_id(self.card_id).request_body(body).build()
        )
        if not response.success():
            raise RuntimeError(f"card close failed code={response.code} msg={response.msg}")


def reply_text(client: lark.Client, message_id: str, text: str, suffix: str) -> str | None:
    visible = text[:MAX_REPLY_CHARS]
    if len(text) > MAX_REPLY_CHARS:
        visible += "\n\n[回答过长，已截断]"
    message_uuid = ("feishu-" + hashlib.sha256(message_id.encode("utf-8")).hexdigest()[:28] + "-" + suffix)[:50]
    body = (
        lark.im.v1.ReplyMessageRequestBody.builder()
        .content(json.dumps({"text": visible}, ensure_ascii=False, separators=(",", ":")))
        .msg_type("text")
        .uuid(message_uuid)
        .build()
    )
    request = lark.im.v1.ReplyMessageRequest.builder().message_id(message_id).request_body(body).build()
    response = client.im.v1.message.reply(request)
    if not response.success():
        raise RuntimeError(f"Feishu reply failed code={response.code} msg={response.msg} log_id={response.get_log_id()}")
    data = response.data
    return str(data.message_id) if data is not None and isinstance(data.message_id, str) else None


def reply_text_for_delivery(client: lark.Client, message_id: str, text: str, delivery_id: str) -> str | None:
    """Use the durable delivery ID as Feishu's stable de-duplication UUID."""
    visible = text[:MAX_REPLY_CHARS]
    if len(text) > MAX_REPLY_CHARS:
        visible += "\n\n[回答过长，已截断]"
    body = (
        lark.im.v1.ReplyMessageRequestBody.builder()
        .content(json.dumps({"text": visible}, ensure_ascii=False, separators=(",", ":")))
        .msg_type("text")
        .uuid(("delivery-" + delivery_id)[:50])
        .build()
    )
    response = client.im.v1.message.reply(
        lark.im.v1.ReplyMessageRequest.builder().message_id(message_id).request_body(body).build()
    )
    if not response.success():
        raise RuntimeError(f"Feishu delivery reply failed code={response.code} msg={response.msg} log_id={response.get_log_id()}")
    return str(response.data.message_id) if response.data is not None and response.data.message_id else None


def artifact_limit(kind: str) -> int:
    return {
        "image": MAX_IMAGE_ARTIFACT_BYTES,
        "audio": MAX_AUDIO_ARTIFACT_BYTES,
        "video": MAX_VIDEO_ARTIFACT_BYTES,
        "file": MAX_FILE_ARTIFACT_BYTES,
    }[kind]


def stage_artifact(message_id: str, artifact: dict[str, Any]) -> Path:
    """Download one registered artifact through the trusted Worker bridge API."""
    normalized = normalized_artifact(artifact)
    if normalized is None:
        raise ValueError("artifact metadata is invalid")
    artifact_id = normalized["artifact_id"]
    kind = normalized["type"]
    target_dir = ARTIFACT_DIR / hashlib.sha256(message_id.encode("utf-8")).hexdigest()[:24]
    target_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
    target = target_dir / f"{artifact_id}-{normalized['name']}"
    if target.exists():
        target.unlink()
    request = Request(
        internal_worker_url(f"/internal/artifacts/{artifact_id}/download"),
        headers=internal_headers(), method="GET",
    )
    maximum_bytes = artifact_limit(kind)
    try:
        with urlopen(request, timeout=ARTIFACT_COPY_TIMEOUT_SECONDS) as response, target.open("xb") as output:
            total = 0
            while chunk := response.read(1024 * 1024):
                total += len(chunk)
                if total > maximum_bytes:
                    raise RuntimeError(f"generated {kind} artifact exceeds its upload limit")
                output.write(chunk)
    except HTTPError as exc:
        target.unlink(missing_ok=True)
        detail = exc.read().decode("utf-8", errors="replace")[:1000]
        raise RuntimeError(f"Worker artifact download HTTP {exc.code}: {detail}") from None
    except (URLError, OSError) as exc:
        target.unlink(missing_ok=True)
        raise RuntimeError(f"Worker artifact download failed: {exc}") from None
    if target.is_symlink() or not target.is_file() or target.stat().st_size < 1:
        target.unlink(missing_ok=True)
        raise RuntimeError("generated artifact is not a regular non-empty file")
    os.chmod(target, 0o600)
    return target


def upload_image(client: lark.Client, image_path: Path) -> str:
    """Upload a validated image for either a card component or image reply."""
    with image_path.open("rb") as image_file:
        upload_body = (
            lark.im.v1.CreateImageRequestBody.builder()
            .image_type("message")
            .image(image_file)
            .build()
        )
        upload = client.im.v1.image.create(
            lark.im.v1.CreateImageRequest.builder().request_body(upload_body).build()
        )
    image_key = upload.data.image_key if upload.success() and upload.data is not None else None
    if not isinstance(image_key, str) or not image_key:
        raise RuntimeError(f"image upload failed code={upload.code} msg={upload.msg}")
    return image_key


def reply_image(client: lark.Client, message_id: str, image_path: Path, suffix: str) -> str | None:
    """Upload and reply with a generated image using the bot tenant token."""
    image_key = upload_image(client, image_path)
    message_uuid = (
        "feishu-" + hashlib.sha256((message_id + suffix).encode("utf-8")).hexdigest()[:28] + "-image"
    )[:50]
    body = (
        lark.im.v1.ReplyMessageRequestBody.builder()
        .content(json.dumps({"image_key": image_key}, ensure_ascii=False, separators=(",", ":")))
        .msg_type("image")
        .uuid(message_uuid)
        .build()
    )
    response = client.im.v1.message.reply(
        lark.im.v1.ReplyMessageRequest.builder().message_id(message_id).request_body(body).build()
    )
    if not response.success():
        raise RuntimeError(f"image reply failed code={response.code} msg={response.msg} log_id={response.get_log_id()}")
    return str(response.data.message_id) if response.data is not None and response.data.message_id else None


def audio_duration_ms(file_path: Path) -> int:
    """Read duration in milliseconds for Feishu's OPUS upload contract."""
    completed = subprocess.run(
        [FFPROBE_BIN, "-v", "error", "-show_entries", "format=duration", "-of", "default=nw=1:nk=1", str(file_path)],
        check=False, capture_output=True, text=True, timeout=20,
    )
    if completed.returncode != 0:
        raise RuntimeError("could not inspect generated audio duration")
    try:
        milliseconds = round(float(completed.stdout.strip()) * 1000)
    except ValueError as exc:
        raise RuntimeError("generated audio has invalid duration metadata") from exc
    if milliseconds < 1 or milliseconds > 10 * 60 * 1000:
        raise RuntimeError("generated audio duration is outside the supported range")
    return milliseconds


def opus_for_card(file_path: Path) -> Path:
    """Convert an output audio file to Ogg Opus required by Card 2.0."""
    opus_path = file_path.with_suffix(".opus")
    completed = subprocess.run(
        [FFMPEG_BIN, "-y", "-v", "error", "-i", str(file_path), "-vn", "-c:a", "libopus", "-b:a", "96k", str(opus_path)],
        check=False, capture_output=True, text=True, timeout=120,
    )
    if completed.returncode != 0 or not opus_path.is_file() or opus_path.stat().st_size < 1:
        detail = completed.stderr.strip().replace("\n", " ")[:300]
        opus_path.unlink(missing_ok=True)
        raise RuntimeError(f"could not convert generated audio to OPUS: {detail or 'ffmpeg failed'}")
    if opus_path.stat().st_size > MAX_AUDIO_ARTIFACT_BYTES:
        opus_path.unlink(missing_ok=True)
        raise RuntimeError("converted OPUS audio exceeds the upload limit")
    os.chmod(opus_path, 0o600)
    return opus_path


def upload_audio(client: lark.Client, file_path: Path) -> str:
    """Upload an Ogg Opus file for the Card 2.0 audio component."""
    if file_path.suffix.lower() != ".opus":
        raise RuntimeError("Card audio must be encoded as OPUS")
    duration = audio_duration_ms(file_path)
    with file_path.open("rb") as source:
        upload_body = (
            lark.im.v1.CreateFileRequestBody.builder()
            .file_type("opus")
            .file_name(file_path.name)
            .duration(duration)
            .file(source)
            .build()
        )
        upload = client.im.v1.file.create(
            lark.im.v1.CreateFileRequest.builder().request_body(upload_body).build()
        )
    file_key = upload.data.file_key if upload.success() and upload.data is not None else None
    if not isinstance(file_key, str) or not file_key:
        raise RuntimeError(f"audio upload failed code={upload.code} msg={upload.msg}")
    return file_key


def reply_downloadable_file(client: lark.Client, message_id: str, file_path: Path, suffix: str) -> str | None:
    """Attach one generated artifact as a generic downloadable file reply."""
    with file_path.open("rb") as source:
        upload_body = (
            lark.im.v1.CreateFileRequestBody.builder()
            .file_type("stream")
            .file_name(file_path.name)
            .file(source)
            .build()
        )
        upload = client.im.v1.file.create(
            lark.im.v1.CreateFileRequest.builder().request_body(upload_body).build()
        )
    file_key = upload.data.file_key if upload.success() and upload.data is not None else None
    if not isinstance(file_key, str) or not file_key:
        raise RuntimeError(f"artifact upload failed code={upload.code} msg={upload.msg}")
    body = (
        lark.im.v1.ReplyMessageRequestBody.builder()
        .content(json.dumps({"file_key": file_key}, ensure_ascii=False, separators=(",", ":")))
        .msg_type("file")
        .uuid(("feishu-" + hashlib.sha256((message_id + suffix).encode("utf-8")).hexdigest()[:28] + "-download")[:50])
        .build()
    )
    response = client.im.v1.message.reply(
        lark.im.v1.ReplyMessageRequest.builder().message_id(message_id).request_body(body).build()
    )
    if not response.success():
        raise RuntimeError(f"artifact download reply failed code={response.code} msg={response.msg} log_id={response.get_log_id()}")
    return str(response.data.message_id) if response.data is not None and response.data.message_id else None


def deliver_audio_download(
    client: lark.Client, message_id: str, artifact: dict[str, Any], card: StreamingCard | None = None,
) -> str | None:
    """Send the original generated audio as a private downloadable attachment."""
    staged: Path | None = None
    try:
        staged = stage_artifact(message_id, artifact)
        reply_id = reply_downloadable_file(client, message_id, staged, "original")
        if card is not None:
            card.append_audio_download_notice()
        LOG.info("delivered downloadable original audio message_id=%s artifact_id=%s", message_id, artifact.get("artifact_id"))
        return reply_id
    finally:
        if staged is not None:
            try:
                staged.unlink(missing_ok=True)
            except OSError:
                LOG.warning("could not remove staged downloadable audio path=%s", staged)


def deliver_image_artifacts(
    client: lark.Client, message_id: str, artifacts: list[dict[str, Any]], card: StreamingCard | None = None,
    run_id: str | None = None,
) -> tuple[list[str], list[str]]:
    """Embed images in the original card, or reply separately if no card exists."""
    delivered: list[str] = []
    errors: list[str] = []
    seen: set[str] = set()
    for raw_artifact in artifacts:
        artifact = normalized_artifact(raw_artifact)
        if artifact is None or artifact["type"] != "image" or artifact["artifact_id"] in seen:
            continue
        seen.add(artifact["artifact_id"])
        staged: Path | None = None
        delivery_id: str | None = None
        try:
            delivery_id = begin_delivery(run_id, message_id, suffix=f"image:{artifact['artifact_id']}", artifact_id=artifact["artifact_id"])
            staged = stage_artifact(message_id, artifact)
            if card is not None:
                card.append_image(upload_image(client, staged))
                delivered.append(card.card_id)
                complete_delivery(delivery_id, card.card_id)
                LOG.info("embedded generated image in card message_id=%s artifact_id=%s", message_id, artifact["artifact_id"])
            else:
                reply_id = reply_image(client, message_id, staged, str(len(delivered)))
                if reply_id:
                    delivered.append(reply_id)
                complete_delivery(delivery_id, reply_id)
                LOG.info("replied generated image message_id=%s artifact_id=%s", message_id, artifact["artifact_id"])
        except Exception as exc:
            fail_delivery(delivery_id, exc)
            errors.append(f"{artifact['artifact_id']}: {type(exc).__name__}: {exc}")
            LOG.warning("image delivery failed message_id=%s artifact_id=%s error=%s", message_id, artifact["artifact_id"], exc)
        finally:
            if staged is not None:
                try:
                    staged.unlink(missing_ok=True)
                except OSError:
                    LOG.warning("could not remove staged image path=%s", staged)
    return delivered, errors


def deliver_audio_artifacts(
    client: lark.Client, message_id: str, artifacts: list[dict[str, Any]], card: StreamingCard | None = None,
    run_id: str | None = None,
) -> tuple[list[str], list[str]]:
    """Embed generated audio in the original Card 2.0 response."""
    delivered: list[str] = []
    errors: list[str] = []
    seen: set[str] = set()
    for raw_artifact in artifacts:
        artifact = normalized_artifact(raw_artifact)
        if artifact is None or artifact["type"] != "audio" or artifact["artifact_id"] in seen:
            continue
        seen.add(artifact["artifact_id"])
        staged: Path | None = None
        opus_path: Path | None = None
        delivery_id: str | None = None
        try:
            delivery_id = begin_delivery(run_id, message_id, suffix=f"audio:{artifact['artifact_id']}", artifact_id=artifact["artifact_id"])
            staged = stage_artifact(message_id, artifact)
            if card is None:
                raise RuntimeError("no Card 2.0 response is available for generated audio")
            card.disable_forwarding_for_audio()
            opus_path = opus_for_card(staged)
            card.append_audio(upload_audio(client, opus_path))
            download_reply_id = reply_downloadable_file(client, message_id, staged, str(len(delivered)))
            if not download_reply_id:
                raise RuntimeError("Feishu did not return a downloadable audio attachment id")
            card.append_audio_download_notice()
            delivered.append(card.card_id)
            complete_delivery(delivery_id, download_reply_id)
            LOG.info("embedded generated audio in card message_id=%s artifact_id=%s", message_id, artifact["artifact_id"])
        except Exception as exc:
            fail_delivery(delivery_id, exc)
            errors.append(f"{artifact['artifact_id']}: {type(exc).__name__}: {exc}")
            LOG.warning("audio delivery failed message_id=%s artifact_id=%s error=%s", message_id, artifact["artifact_id"], exc)
        finally:
            if staged is not None:
                try:
                    staged.unlink(missing_ok=True)
                except OSError:
                    LOG.warning("could not remove staged audio path=%s", staged)
            if opus_path is not None:
                try:
                    opus_path.unlink(missing_ok=True)
                except OSError:
                    LOG.warning("could not remove converted OPUS path=%s", opus_path)
    return delivered, errors


def deliver_file_artifacts(
    client: lark.Client, message_id: str, artifacts: list[dict[str, Any]], run_id: str | None = None,
) -> tuple[list[str], list[str]]:
    """Reply with registered video or generic-file artifacts as downloads."""
    delivered: list[str] = []
    errors: list[str] = []
    seen: set[str] = set()
    for raw_artifact in artifacts:
        artifact = normalized_artifact(raw_artifact)
        if artifact is None or artifact["type"] not in {"video", "file"} or artifact["artifact_id"] in seen:
            continue
        seen.add(artifact["artifact_id"])
        staged: Path | None = None
        delivery_id: str | None = None
        try:
            delivery_id = begin_delivery(run_id, message_id, suffix=f"file:{artifact['artifact_id']}", artifact_id=artifact["artifact_id"])
            staged = stage_artifact(message_id, artifact)
            reply_id = reply_downloadable_file(client, message_id, staged, str(len(delivered)))
            if reply_id:
                delivered.append(reply_id)
            complete_delivery(delivery_id, reply_id)
            LOG.info("replied generated file message_id=%s artifact_id=%s", message_id, artifact["artifact_id"])
        except Exception as exc:
            fail_delivery(delivery_id, exc)
            errors.append(f"{artifact['artifact_id']}: {type(exc).__name__}: {exc}")
            LOG.warning("file delivery failed message_id=%s artifact_id=%s error=%s", message_id, artifact["artifact_id"], exc)
        finally:
            if staged is not None:
                try:
                    staged.unlink(missing_ok=True)
                except OSError:
                    LOG.warning("could not remove staged generated file path=%s", staged)
    return delivered, errors


def sender_profile(client: lark.Client, open_id: str | None) -> dict[str, Any] | None:
    if not CONTACT_ENRICHMENT or not open_id:
        return None
    try:
        request = lark.contact.v3.GetUserRequest.builder().user_id_type("open_id").user_id(open_id).build()
        response = client.contact.v3.user.get(request)
        if not response.success() or response.data is None or response.data.user is None:
            LOG.info("contact enrichment unavailable code=%s", response.code)
            return None
        user = response.data.user
        result: dict[str, Any] = {"source": "feishu_contact", "open_id": open_id}
        for source, target in (("name", "name"), ("en_name", "en_name"), ("job_title", "job_title")):
            value = getattr(user, source, None)
            if isinstance(value, str) and value.strip():
                result[target] = value.strip()
        departments = getattr(user, "department_ids", None)
        if isinstance(departments, list):
            result["department_ids"] = [str(value) for value in departments[:10] if value]
        return result
    except Exception as exc:
        LOG.info("contact enrichment failed: %s", exc)
        return None


class Bridge:
    def __init__(self, client: lark.Client) -> None:
        self.client = client

    def on_message(self, data: Any) -> None:
        event = getattr(data, "event", None)
        sender = getattr(event, "sender", None)
        message = getattr(event, "message", None)
        if sender is None or message is None:
            return
        sender_open_id = getattr(getattr(sender, "sender_id", None), "open_id", None)
        if not should_handle(
            sender_type=getattr(sender, "sender_type", None),
            chat_type=getattr(message, "chat_type", None),
            message_type=getattr(message, "message_type", None),
            mentions=getattr(message, "mentions", None),
            bot_open_id=BOT_OPEN_ID,
            group_require_mention=GROUP_REQUIRE_MENTION,
        ):
            return
        message_id = getattr(message, "message_id", None)
        if not isinstance(message_id, str) or not message_id:
            return
        session_id = session_id_for(
            getattr(message, "chat_type", None),
            getattr(message, "chat_id", None),
            getattr(message, "thread_id", None),
            getattr(message, "root_id", None),
        )
        if not claim_message(message_id, session_id):
            LOG.info("duplicate ignored message_id=%s", message_id)
            return
        text, inbound_refs = extract_inbound_content(
            getattr(message, "message_type", None), getattr(message, "content", None),
        )
        text = text[:MAX_MESSAGE_CHARS]
        if not text and not inbound_refs:
            finish_message(message_id, "ignored", error="empty message")
            return
        if _workers is None:
            raise RuntimeError("channel bridge is not configured")
        _workers.submit(self.process_message, message_id, session_id, text, sender_open_id, inbound_refs)

    def process_message(
        self, message_id: str, session_id: str, text: str, sender_open_id: str | None,
        inbound_refs: list[InboundMedia] | None = None,
    ) -> None:
        card: StreamingCard | None = None
        inbound_directory: Path | None = None
        staged_media: list[dict[str, str]] = []
        try:
            sender_context = sender_profile(self.client, sender_open_id)
            prompt = prompt_with_sender_context(text, sender_context)
            session = resolve_worker_session(
                session_id, external_conversation_id=session_id,
                conversation_kind=(
                    "thread" if session_id.startswith("feishu:group:") and ":" in session_id.removeprefix("feishu:group:")
                    else "group" if session_id.startswith("feishu:group:") else "p2p"
                ),
                principal_ref=(
                    f"group:{session_id}" if session_id.startswith("feishu:group:") else f"user:{sender_open_id or session_id}"
                ),
            )
            prompt = prompt_with_message_context(prompt, message_id)
            prepared = prepare_worker_run(str(session["session_id"]), prompt, message_id)
            run_id = str(prepared["run_id"])
            inbox = str(prepared["inbox"])
            workspace = worker_session_workspace(session)
            if inbound_refs:
                staged_media, inbound_directory = stage_inbound_media(self.client, message_id, inbound_refs)
                staged_media = copy_inbound_media_to_worker(inbound_directory, staged_media, inbox, workspace)
                prompt = prompt_with_inbound_media(prompt, staged_media)
            finalized = finalize_worker_run_inputs(run_id, staged_media, prompt)
            if str(finalized.get("run_id")) != run_id:
                raise RuntimeError("Worker finalized a different run")
            manifest_inputs = finalized.get("inputs")
            if not isinstance(manifest_inputs, list):
                raise RuntimeError("Worker did not return a run input manifest")
            try:
                card = StreamingCard(self.client, message_id)
                card.start()
            except Exception as exc:
                LOG.warning("streaming card unavailable message_id=%s: %s", message_id, exc)
                card = None

            chunks: list[str] = []
            final_answer = ""
            last_update = 0.0
            generated_artifacts: list[dict[str, Any]] = []
            for event_name, payload in stream_dsh_chat(run_id):
                if event_name == "assistant.delta":
                    delta = payload.get("text")
                    if isinstance(delta, str):
                        chunks.append(delta)
                        now = time.monotonic()
                        if card is not None and now - last_update >= CARD_UPDATE_INTERVAL:
                            card.update("".join(chunks), "正在生成回答…")
                            last_update = now
                elif event_name == "tool.call" and card is not None:
                    card.update("".join(chunks), "正在处理…")
                elif event_name == "done":
                    answer = payload.get("answer")
                    final_answer = answer if isinstance(answer, str) and answer else "".join(chunks)
                    artifacts = payload.get("artifacts")
                    if isinstance(artifacts, list):
                        for artifact in artifacts:
                            normalized = normalized_artifact(artifact)
                            if normalized is not None:
                                generated_artifacts.append(normalized)
                    if payload.get("error"):
                        raise RuntimeError(str(payload["error"]))

            if not final_answer:
                final_answer = "".join(chunks)
            if not final_answer:
                raise RuntimeError("stream completed without an answer")
            image_artifacts = [artifact for artifact in generated_artifacts if artifact["type"] == "image"]
            audio_artifacts = [artifact for artifact in generated_artifacts if artifact["type"] == "audio"]
            file_artifacts = [artifact for artifact in generated_artifacts if artifact["type"] in {"video", "file"}]
            image_errors: list[str] = []
            if image_artifacts:
                if card is not None:
                    card.update(final_answer, "正在嵌入图片…")
                _image_reply_ids, image_errors = deliver_image_artifacts(self.client, message_id, image_artifacts, card, run_id)
            audio_errors: list[str] = []
            if audio_artifacts:
                if card is not None:
                    card.update(final_answer, "正在发送音频…")
                _audio_reply_ids, audio_errors = deliver_audio_artifacts(self.client, message_id, audio_artifacts, card, run_id)
            file_errors: list[str] = []
            if file_artifacts:
                if card is not None:
                    card.update(final_answer, "正在发送文件…")
                _file_reply_ids, file_errors = deliver_file_artifacts(self.client, message_id, file_artifacts, run_id)
            if card is not None:
                media_errors = image_errors + audio_errors + file_errors
                card.update(final_answer, "回答完成" if not media_errors else "回答完成；部分媒体发送失败")
                card.close(success=True)
                reply_id = card.reply_message_id
            else:
                text_delivery = begin_delivery(
                    run_id, message_id, suffix="text", payload={"kind": "text", "text": final_answer},
                )
                try:
                    reply_id = (
                        reply_text_for_delivery(self.client, message_id, final_answer, text_delivery)
                        if text_delivery is not None else reply_text(self.client, message_id, final_answer, "reply")
                    )
                    complete_delivery(text_delivery, reply_id)
                except Exception as exc:
                    fail_delivery(text_delivery, exc)
                    raise
            finish_message(message_id, "replied", reply_message_id=reply_id)
            LOG.info("replied message_id=%s", message_id)
        except Exception as exc:
            error = f"{type(exc).__name__}: {exc}"
            LOG.exception("failed message_id=%s", message_id)
            finish_message(message_id, "failed", error=error[:2000])
            try:
                if card is not None:
                    card.update("处理消息时遇到错误，请稍后重试。", "处理失败")
                    card.close(success=False)
                else:
                    reply_text(self.client, message_id, "处理消息时遇到错误，请稍后重试。", "error")
            except Exception:
                LOG.exception("failed error reply message_id=%s", message_id)
        finally:
            cleanup_inbound_media(inbound_directory)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run a configuration-driven Feishu channel bridge")
    parser.add_argument(
        "--profile",
        default=os.environ.get("FEISHU_CHANNEL_PROFILE", ""),
        help="absolute path to a non-secret Feishu channel profile JSON file",
    )
    args = parser.parse_args()
    if not args.profile:
        raise RuntimeError("--profile or FEISHU_CHANNEL_PROFILE is required")
    configure(args.profile)
    logging.basicConfig(
        level=os.environ.get("FEISHU_CHANNEL_LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    logging.getLogger("Lark").setLevel(logging.WARNING)
    require_configuration()
    init_db()
    client = lark.Client.builder().app_id(APP_ID).app_secret(APP_SECRET).build()
    threading.Thread(target=delivery_replay_loop, args=(client,), name="feishu-delivery-replay", daemon=True).start()
    bridge = Bridge(client)
    handler = lark.EventDispatcherHandler.builder("", "").register_p2_im_message_receive_v1(bridge.on_message).build()
    websocket = lark.ws.Client(
        APP_ID,
        APP_SECRET,
        log_level=lark.LogLevel.WARNING,
        event_handler=handler,
    )
    LOG.info("starting channel bridge instance_id=%s", PROFILE.instance_id if PROFILE else "unknown")
    websocket.start()


if __name__ == "__main__":
    try:
        main()
    finally:
        _delivery_replay_stop.set()
        if _workers is not None:
            _workers.shutdown(wait=False, cancel_futures=True)
