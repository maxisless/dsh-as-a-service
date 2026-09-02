"""Load the non-secret configuration of one Feishu channel instance.

Secrets are intentionally referenced by environment-variable name rather than
stored in this profile.  This makes a profile portable while keeping app
credentials and bridge tokens in a separately permissioned file or secret
manager.
"""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping
from urllib.parse import urlsplit


_INSTANCE_ID_RE = re.compile(r"^[a-z0-9][a-z0-9-]{0,62}$")
_ENV_NAME_RE = re.compile(r"^[A-Z_][A-Z0-9_]{0,127}$")


class ProfileError(ValueError):
    """An instance profile is missing, malformed, or unsafe."""


def _mapping(value: Any, name: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ProfileError(f"{name} must be an object")
    return value


def _string(value: Any, name: str, *, required: bool = True, default: str = "") -> str:
    if value is None and not required:
        return default
    if not isinstance(value, str) or (required and not value.strip()):
        raise ProfileError(f"{name} must be a non-empty string")
    return value.strip()


def _boolean(value: Any, name: str, default: bool) -> bool:
    if value is None:
        return default
    if not isinstance(value, bool):
        raise ProfileError(f"{name} must be true or false")
    return value


def _integer(value: Any, name: str, default: int, *, minimum: int, maximum: int) -> int:
    if value is None:
        return default
    if isinstance(value, bool) or not isinstance(value, int) or not minimum <= value <= maximum:
        raise ProfileError(f"{name} must be an integer between {minimum} and {maximum}")
    return value


def _number(value: Any, name: str, default: float, *, minimum: float, maximum: float) -> float:
    if value is None:
        return default
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not minimum <= float(value) <= maximum:
        raise ProfileError(f"{name} must be a number between {minimum} and {maximum}")
    return float(value)


def _env_name(value: Any, name: str, default: str) -> str:
    result = _string(value, name, required=False, default=default)
    if _ENV_NAME_RE.fullmatch(result) is None:
        raise ProfileError(f"{name} must be an upper-case environment variable name")
    return result


@dataclass(frozen=True)
class CredentialReferences:
    app_id_env: str
    app_secret_env: str
    bot_open_id_env: str
    internal_bridge_token_env: str


@dataclass(frozen=True)
class InstanceProfile:
    instance_id: str
    display_name: str
    display_subtitle: str
    worker_chat_url: str
    worker_container: str
    agent_id: str
    agent_version: str | None
    group_require_mention: bool
    contact_enrichment: bool
    max_workers: int
    request_timeout_seconds: int
    max_message_chars: int
    max_reply_chars: int
    card_update_interval_seconds: float
    delivery_replay_interval_seconds: int
    max_inbound_media_items: int
    credentials: CredentialReferences


def load_profile(path: Path) -> InstanceProfile:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except OSError as exc:
        raise ProfileError(f"could not read profile: {exc}") from exc
    except json.JSONDecodeError as exc:
        raise ProfileError(f"profile is not valid JSON: {exc.msg}") from exc
    root = _mapping(raw, "profile")
    if root.get("schema_version") != 1:
        raise ProfileError("schema_version must be 1")
    if root.get("kind") != "feishu-channel-instance":
        raise ProfileError("kind must be feishu-channel-instance")

    instance_id = _string(root.get("instance_id"), "instance_id")
    if _INSTANCE_ID_RE.fullmatch(instance_id) is None:
        raise ProfileError("instance_id must be lower-case letters, digits, and hyphens")
    display = _mapping(root.get("display"), "display")
    worker = _mapping(root.get("worker"), "worker")
    agent = _mapping(root.get("agent"), "agent")
    behavior = _mapping(root.get("behavior", {}), "behavior")
    credentials = _mapping(root.get("credentials", {}), "credentials")

    worker_chat_url = _string(worker.get("chat_url"), "worker.chat_url")
    parsed = urlsplit(worker_chat_url)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc or parsed.path.rstrip("/") != "/chat":
        raise ProfileError("worker.chat_url must be an HTTP(S) Worker /chat URL")
    worker_container = _string(worker.get("container"), "worker.container", required=False)
    if worker_container and any(character.isspace() for character in worker_container):
        raise ProfileError("worker.container cannot contain whitespace")

    agent_version_raw = agent.get("version")
    if agent_version_raw is not None and (not isinstance(agent_version_raw, str) or not agent_version_raw.strip()):
        raise ProfileError("agent.version must be a non-empty string when present")

    return InstanceProfile(
        instance_id=instance_id,
        display_name=_string(display.get("name"), "display.name"),
        display_subtitle=_string(display.get("subtitle"), "display.subtitle", required=False, default="正在为你处理"),
        worker_chat_url=worker_chat_url,
        worker_container=worker_container,
        agent_id=_string(agent.get("id"), "agent.id"),
        agent_version=agent_version_raw.strip() if isinstance(agent_version_raw, str) else None,
        group_require_mention=_boolean(behavior.get("group_require_mention"), "behavior.group_require_mention", True),
        contact_enrichment=_boolean(behavior.get("contact_enrichment"), "behavior.contact_enrichment", True),
        max_workers=_integer(behavior.get("max_workers"), "behavior.max_workers", 4, minimum=1, maximum=64),
        request_timeout_seconds=_integer(behavior.get("request_timeout_seconds"), "behavior.request_timeout_seconds", 2400, minimum=10, maximum=7200),
        max_message_chars=_integer(behavior.get("max_message_chars"), "behavior.max_message_chars", 20000, minimum=1, maximum=100000),
        max_reply_chars=_integer(behavior.get("max_reply_chars"), "behavior.max_reply_chars", 20000, minimum=1, maximum=100000),
        card_update_interval_seconds=_number(behavior.get("card_update_interval_seconds"), "behavior.card_update_interval_seconds", 1.0, minimum=0.25, maximum=30),
        delivery_replay_interval_seconds=_integer(behavior.get("delivery_replay_interval_seconds"), "behavior.delivery_replay_interval_seconds", 60, minimum=10, maximum=3600),
        max_inbound_media_items=_integer(behavior.get("max_inbound_media_items"), "behavior.max_inbound_media_items", 12, minimum=1, maximum=12),
        credentials=CredentialReferences(
            app_id_env=_env_name(credentials.get("app_id_env"), "credentials.app_id_env", "FEISHU_APP_ID"),
            app_secret_env=_env_name(credentials.get("app_secret_env"), "credentials.app_secret_env", "FEISHU_APP_SECRET"),
            bot_open_id_env=_env_name(credentials.get("bot_open_id_env"), "credentials.bot_open_id_env", "FEISHU_BOT_OPEN_ID"),
            internal_bridge_token_env=_env_name(credentials.get("internal_bridge_token_env"), "credentials.internal_bridge_token_env", "DSH_INTERNAL_BRIDGE_TOKEN"),
        ),
    )


def secret_from_environment(reference: str, environment: Mapping[str, str] | None = None, *, required: bool = True) -> str:
    value = (environment or os.environ).get(reference, "").strip()
    if required and not value:
        raise ProfileError(f"missing required secret environment variable: {reference}")
    return value
