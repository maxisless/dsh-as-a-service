"""Durable local control plane for tenant, Agent, session, and run ownership.

The default implementation uses SQLite so the single-worker deployment can use
server-owned identities, durable run state, idempotency, and replayable events
without introducing another service.  Its interface deliberately keeps the
storage boundary narrow enough to replace SQLite with a multi-node backend.
"""

from __future__ import annotations

import contextlib
import hashlib
import json
import os
import sqlite3
import threading
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator


TERMINAL_RUN_STATUSES = frozenset({"SUCCEEDED", "FAILED", "CANCELED", "EXPIRED"})
ACTIVE_RUN_STATUSES = frozenset({"LEASED", "RUNNING"})


class ControlPlaneError(RuntimeError):
    """Expected control-plane error safe to expose as an API problem."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message


@dataclass(frozen=True)
class Identity:
    tenant_id: str
    principal_id: str
    role: str = "chat"


@dataclass(frozen=True)
class AgentVersion:
    tenant_id: str
    agent_id: str
    version: str
    display_name: str
    default_model: str
    config: dict[str, Any]


@dataclass(frozen=True)
class SessionRecord:
    id: str
    tenant_id: str
    principal_id: str
    agent_id: str
    agent_version: str
    model_alias: str
    status: str
    created_at: float
    updated_at: float


@dataclass(frozen=True)
class RunRecord:
    id: str
    tenant_id: str
    principal_id: str
    agent_id: str
    agent_version: str
    session_id: str
    model_alias: str
    source: str
    message: str
    status: str
    idempotency_key: str
    lease_epoch: int
    executor_id: str | None
    lease_expires_at: float | None
    attempt: int
    cancel_requested: bool
    config_snapshot: dict[str, Any]
    response: dict[str, Any] | None
    error: dict[str, Any] | None
    created_at: float
    started_at: float | None
    finished_at: float | None


@dataclass(frozen=True)
class SessionPaths:
    root: Path
    workspace: Path
    dsh_state: Path
    conversation: Path
    artifacts: Path


def utc_timestamp() -> float:
    return time.time()


def opaque_component(value: str) -> str:
    """Map an ID to a filesystem-safe opaque path component."""
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:32]


def canonical_json(value: dict[str, Any]) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def parse_json(value: str | None) -> dict[str, Any]:
    if not value:
        return {}
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


class ControlPlane:
    """SQLite-backed authoritative metadata/state for a single control-plane node."""

    def __init__(self, root: Path, *, default_model: str) -> None:
        self.root = root.resolve()
        self.database_path = self.root / "control-plane.sqlite3"
        self.sessions_root = self.root / "tenants"
        self.default_model = default_model
        self._lock = threading.RLock()
        self.root.mkdir(parents=True, exist_ok=True)
        self.sessions_root.mkdir(parents=True, exist_ok=True)
        self._initialize()
        self.bootstrap_local_defaults()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.database_path, timeout=15, isolation_level=None)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA busy_timeout = 15000")
        return connection

    @contextlib.contextmanager
    def transaction(self, *, immediate: bool = False) -> Iterator[sqlite3.Connection]:
        with self._lock:
            connection = self._connect()
            try:
                connection.execute("BEGIN IMMEDIATE" if immediate else "BEGIN")
                yield connection
                connection.commit()
            except BaseException:
                connection.rollback()
                raise
            finally:
                connection.close()

    def _initialize(self) -> None:
        with self.transaction(immediate=True) as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS tenants (
                    id TEXT PRIMARY KEY,
                    display_name TEXT NOT NULL,
                    status TEXT NOT NULL,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS principals (
                    tenant_id TEXT NOT NULL,
                    id TEXT NOT NULL,
                    role TEXT NOT NULL,
                    status TEXT NOT NULL,
                    created_at REAL NOT NULL,
                    PRIMARY KEY (tenant_id, id),
                    FOREIGN KEY (tenant_id) REFERENCES tenants(id)
                );
                CREATE TABLE IF NOT EXISTS agent_versions (
                    tenant_id TEXT NOT NULL,
                    agent_id TEXT NOT NULL,
                    version TEXT NOT NULL,
                    display_name TEXT NOT NULL,
                    default_model TEXT NOT NULL,
                    config_json TEXT NOT NULL,
                    status TEXT NOT NULL,
                    created_at REAL NOT NULL,
                    published_at REAL NOT NULL,
                    PRIMARY KEY (tenant_id, agent_id, version),
                    FOREIGN KEY (tenant_id) REFERENCES tenants(id)
                );
                CREATE TABLE IF NOT EXISTS sessions (
                    id TEXT PRIMARY KEY,
                    tenant_id TEXT NOT NULL,
                    principal_id TEXT NOT NULL,
                    agent_id TEXT NOT NULL,
                    agent_version TEXT NOT NULL,
                    model_alias TEXT NOT NULL,
                    status TEXT NOT NULL,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL,
                    FOREIGN KEY (tenant_id) REFERENCES tenants(id),
                    FOREIGN KEY (tenant_id, principal_id) REFERENCES principals(tenant_id, id),
                    FOREIGN KEY (tenant_id, agent_id, agent_version) REFERENCES agent_versions(tenant_id, agent_id, version)
                );
                CREATE INDEX IF NOT EXISTS sessions_scope_idx
                    ON sessions(tenant_id, principal_id, agent_id, status);
                CREATE TABLE IF NOT EXISTS runs (
                    id TEXT PRIMARY KEY,
                    tenant_id TEXT NOT NULL,
                    principal_id TEXT NOT NULL,
                    agent_id TEXT NOT NULL,
                    agent_version TEXT NOT NULL,
                    session_id TEXT NOT NULL,
                    model_alias TEXT NOT NULL,
                    source TEXT NOT NULL DEFAULT 'http',
                    message TEXT NOT NULL,
                    status TEXT NOT NULL,
                    idempotency_key TEXT NOT NULL,
                    lease_epoch INTEGER NOT NULL DEFAULT 0,
                    executor_id TEXT,
                    lease_expires_at REAL,
                    attempt INTEGER NOT NULL DEFAULT 0,
                    cancel_requested INTEGER NOT NULL DEFAULT 0,
                    config_snapshot_json TEXT NOT NULL,
                    response_json TEXT,
                    error_json TEXT,
                    event_seq INTEGER NOT NULL DEFAULT 0,
                    created_at REAL NOT NULL,
                    started_at REAL,
                    finished_at REAL,
                    FOREIGN KEY (session_id) REFERENCES sessions(id),
                    UNIQUE (tenant_id, idempotency_key)
                );
                CREATE INDEX IF NOT EXISTS runnable_runs_idx
                    ON runs(status, created_at);
                CREATE INDEX IF NOT EXISTS session_runs_idx
                    ON runs(session_id, status, created_at);
                CREATE TABLE IF NOT EXISTS run_events (
                    run_id TEXT NOT NULL,
                    event_id INTEGER NOT NULL,
                    event_type TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    created_at REAL NOT NULL,
                    PRIMARY KEY (run_id, event_id),
                    FOREIGN KEY (run_id) REFERENCES runs(id)
                );
                CREATE TABLE IF NOT EXISTS artifacts (
                    id TEXT PRIMARY KEY,
                    tenant_id TEXT NOT NULL,
                    agent_id TEXT NOT NULL,
                    session_id TEXT NOT NULL,
                    run_id TEXT NOT NULL,
                    kind TEXT NOT NULL DEFAULT 'file',
                    name TEXT NOT NULL,
                    content_type TEXT,
                    size_bytes INTEGER,
                    storage_key TEXT NOT NULL,
                    status TEXT NOT NULL,
                    created_at REAL NOT NULL,
                    deleted_at REAL,
                    FOREIGN KEY (run_id) REFERENCES runs(id)
                );
                CREATE INDEX IF NOT EXISTS artifact_scope_idx
                    ON artifacts(tenant_id, agent_id, session_id, run_id, status);
                CREATE TABLE IF NOT EXISTS tenant_memory_items (
                    id TEXT PRIMARY KEY,
                    tenant_id TEXT NOT NULL,
                    agent_scope TEXT,
                    source_type TEXT NOT NULL,
                    source_ref TEXT NOT NULL,
                    revision TEXT NOT NULL,
                    access_scope TEXT NOT NULL,
                    trust_class TEXT NOT NULL,
                    status TEXT NOT NULL,
                    metadata_json TEXT NOT NULL,
                    created_at REAL NOT NULL,
                    published_at REAL,
                    FOREIGN KEY (tenant_id) REFERENCES tenants(id)
                );
                CREATE TABLE IF NOT EXISTS secret_refs (
                    id TEXT PRIMARY KEY,
                    tenant_id TEXT NOT NULL,
                    purpose TEXT NOT NULL,
                    provider TEXT NOT NULL,
                    version TEXT NOT NULL,
                    status TEXT NOT NULL,
                    metadata_json TEXT NOT NULL,
                    created_at REAL NOT NULL,
                    rotated_at REAL,
                    revoked_at REAL,
                    FOREIGN KEY (tenant_id) REFERENCES tenants(id)
                );
                CREATE TABLE IF NOT EXISTS audit_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    tenant_id TEXT NOT NULL,
                    principal_id TEXT,
                    run_id TEXT,
                    event_type TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    created_at REAL NOT NULL
                );
                """
            )
            columns = {row["name"] for row in connection.execute("PRAGMA table_info(runs)").fetchall()}
            if "source" not in columns:
                connection.execute("ALTER TABLE runs ADD COLUMN source TEXT NOT NULL DEFAULT 'http'")
            artifact_columns = {row["name"] for row in connection.execute("PRAGMA table_info(artifacts)").fetchall()}
            if "kind" not in artifact_columns:
                connection.execute("ALTER TABLE artifacts ADD COLUMN kind TEXT NOT NULL DEFAULT 'file'")

    def bootstrap_local_defaults(self) -> None:
        now = utc_timestamp()
        default_config = {
            "allowed_models": ["*"],
            "default_model": self.default_model,
            "skill_bundle_version": "local-v1",
            "memory_collections": [],
            "network_policy": "local-trusted",
            "artifact_retention": "local-default",
        }
        with self.transaction(immediate=True) as connection:
            connection.execute(
                "INSERT OR IGNORE INTO tenants(id, display_name, status, created_at, updated_at) VALUES (?, ?, 'ACTIVE', ?, ?)",
                ("local", "Local trusted tenant", now, now),
            )
            connection.execute(
                "INSERT OR IGNORE INTO principals(tenant_id, id, role, status, created_at) VALUES (?, ?, ?, 'ACTIVE', ?)",
                ("local", "local", "admin", now),
            )
            connection.execute(
                """INSERT OR IGNORE INTO agent_versions(
                    tenant_id, agent_id, version, display_name, default_model,
                    config_json, status, created_at, published_at
                ) VALUES (?, ?, ?, ?, ?, ?, 'PUBLISHED', ?, ?)""",
                ("local", "default", "v1", "Default local agent", self.default_model, canonical_json(default_config), now, now),
            )

    def ensure_identity(self, identity: Identity) -> None:
        now = utc_timestamp()
        default_config = {
            "allowed_models": ["*"],
            "default_model": self.default_model,
            "skill_bundle_version": "local-v1",
            "memory_collections": [],
            "network_policy": "local-trusted",
            "artifact_retention": "local-default",
        }
        with self.transaction(immediate=True) as connection:
            tenant = connection.execute("SELECT status FROM tenants WHERE id = ?", (identity.tenant_id,)).fetchone()
            if tenant is None:
                connection.execute(
                    "INSERT INTO tenants(id, display_name, status, created_at, updated_at) VALUES (?, ?, 'ACTIVE', ?, ?)",
                    (identity.tenant_id, identity.tenant_id, now, now),
                )
            elif tenant["status"] != "ACTIVE":
                raise ControlPlaneError("tenant_inactive", "Tenant is not active")
            principal = connection.execute(
                "SELECT status FROM principals WHERE tenant_id = ? AND id = ?",
                (identity.tenant_id, identity.principal_id),
            ).fetchone()
            if principal is None:
                connection.execute(
                    "INSERT INTO principals(tenant_id, id, role, status, created_at) VALUES (?, ?, ?, 'ACTIVE', ?)",
                    (identity.tenant_id, identity.principal_id, identity.role, now),
                )
            elif principal["status"] != "ACTIVE":
                raise ControlPlaneError("principal_inactive", "Principal is not active")
            connection.execute(
                """INSERT OR IGNORE INTO agent_versions(
                    tenant_id, agent_id, version, display_name, default_model,
                    config_json, status, created_at, published_at
                ) VALUES (?, 'default', 'v1', 'Default agent', ?, ?, 'PUBLISHED', ?, ?)""",
                (identity.tenant_id, self.default_model, canonical_json(default_config), now, now),
            )

    def publish_agent_version(
        self,
        identity: Identity,
        *,
        agent_id: str,
        version: str,
        display_name: str,
        default_model: str,
        config: dict[str, Any],
    ) -> AgentVersion:
        if identity.role not in {"admin", "manager"}:
            raise ControlPlaneError("forbidden", "Agent publication requires tenant management permission")
        if not agent_id or not version or not default_model:
            raise ControlPlaneError("invalid_agent", "agent_id, version, and default_model are required")
        self.ensure_identity(identity)
        now = utc_timestamp()
        with self.transaction(immediate=True) as connection:
            exists = connection.execute(
                "SELECT 1 FROM agent_versions WHERE tenant_id = ? AND agent_id = ? AND version = ?",
                (identity.tenant_id, agent_id, version),
            ).fetchone()
            if exists is not None:
                raise ControlPlaneError("agent_version_exists", "Agent version is immutable and already exists")
            connection.execute(
                """INSERT INTO agent_versions(
                    tenant_id, agent_id, version, display_name, default_model,
                    config_json, status, created_at, published_at
                ) VALUES (?, ?, ?, ?, ?, ?, 'PUBLISHED', ?, ?)""",
                (identity.tenant_id, agent_id, version, display_name or agent_id, default_model, canonical_json(config), now, now),
            )
        return AgentVersion(identity.tenant_id, agent_id, version, display_name or agent_id, default_model, config)

    def _agent_row(self, connection: sqlite3.Connection, tenant_id: str, agent_id: str, version: str | None = None) -> sqlite3.Row:
        if version is None:
            row = connection.execute(
                """SELECT * FROM agent_versions WHERE tenant_id = ? AND agent_id = ? AND status = 'PUBLISHED'
                   ORDER BY published_at DESC LIMIT 1""",
                (tenant_id, agent_id),
            ).fetchone()
        else:
            row = connection.execute(
                """SELECT * FROM agent_versions WHERE tenant_id = ? AND agent_id = ? AND version = ? AND status = 'PUBLISHED'""",
                (tenant_id, agent_id, version),
            ).fetchone()
        if row is None:
            raise ControlPlaneError("agent_not_found", "Published Agent version was not found")
        return row

    def create_session(
        self,
        identity: Identity,
        *,
        agent_id: str = "default",
        agent_version: str | None = None,
        model_alias: str | None = None,
    ) -> SessionRecord:
        self.ensure_identity(identity)
        now = utc_timestamp()
        with self.transaction(immediate=True) as connection:
            agent = self._agent_row(connection, identity.tenant_id, agent_id, agent_version)
            config = parse_json(agent["config_json"])
            allowed = config.get("allowed_models")
            selected = model_alias or str(agent["default_model"])
            if isinstance(allowed, list) and allowed and "*" not in allowed and selected not in allowed:
                raise ControlPlaneError("model_not_allowed", "Model is not allowed by this Agent version")
            session_id = f"s_{uuid.uuid4().hex}"
            connection.execute(
                """INSERT INTO sessions(
                    id, tenant_id, principal_id, agent_id, agent_version, model_alias,
                    status, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, 'ACTIVE', ?, ?)""",
                (session_id, identity.tenant_id, identity.principal_id, agent["agent_id"], agent["version"], selected, now, now),
            )
        record = self.get_session(identity, session_id)
        self.ensure_session_storage(record)
        return record

    def get_or_create_compat_session(
        self,
        identity: Identity,
        session_id: str | None,
        *,
        model_alias: str | None,
    ) -> SessionRecord:
        if session_id is None:
            return self.create_session(identity, model_alias=model_alias)
        try:
            record = self.get_session(identity, session_id)
        except ControlPlaneError as exc:
            if exc.code != "session_not_found":
                raise
            # Compatibility callers may supply their own stable local session ID.
            # This is permitted only for the local/trusted identity path.
            if identity.tenant_id != "local" or identity.principal_id != "local":
                raise
            self.ensure_identity(identity)
            now = utc_timestamp()
            with self.transaction(immediate=True) as connection:
                agent = self._agent_row(connection, identity.tenant_id, "default")
                selected = model_alias or str(agent["default_model"])
                config = parse_json(agent["config_json"])
                allowed = config.get("allowed_models")
                if isinstance(allowed, list) and allowed and "*" not in allowed and selected not in allowed:
                    raise ControlPlaneError("model_not_allowed", "Model is not allowed by this Agent version")
                connection.execute(
                    """INSERT INTO sessions(
                        id, tenant_id, principal_id, agent_id, agent_version, model_alias,
                        status, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, 'ACTIVE', ?, ?)""",
                    (session_id, identity.tenant_id, identity.principal_id, agent["agent_id"], agent["version"], selected, now, now),
                )
            record = self.get_session(identity, session_id)
        if model_alias is not None and model_alias != record.model_alias:
            raise ControlPlaneError("session_model_conflict", "Session is already bound to a different model")
        self.ensure_session_storage(record)
        return record

    def import_legacy_conversation(self, session: SessionRecord, legacy_path: Path) -> bool:
        """Copy a v1 conversation once without overwriting session-owned state."""
        paths = self.ensure_session_storage(session)
        if paths.conversation.exists() or not legacy_path.is_file():
            return False
        try:
            parsed = json.loads(legacy_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return False
        if not isinstance(parsed, dict) or parsed.get("session_id") != session.id:
            return False
        temporary = paths.conversation.with_name(f".{paths.conversation.name}.{uuid.uuid4().hex}.tmp")
        temporary.write_text(canonical_json(parsed), encoding="utf-8")
        try:
            os.chmod(temporary, 0o600)
        except OSError:
            pass
        os.replace(temporary, paths.conversation)
        return True

    def get_session(self, identity: Identity, session_id: str) -> SessionRecord:
        self.ensure_identity(identity)
        with self.transaction() as connection:
            row = connection.execute("SELECT * FROM sessions WHERE id = ?", (session_id,)).fetchone()
        if row is None:
            raise ControlPlaneError("session_not_found", "Session was not found")
        if row["tenant_id"] != identity.tenant_id or row["principal_id"] != identity.principal_id:
            raise ControlPlaneError("session_not_found", "Session was not found")
        if row["status"] != "ACTIVE":
            raise ControlPlaneError("session_inactive", "Session is not active")
        return self._session_from_row(row)

    def _session_from_row(self, row: sqlite3.Row) -> SessionRecord:
        return SessionRecord(
            id=str(row["id"]), tenant_id=str(row["tenant_id"]), principal_id=str(row["principal_id"]),
            agent_id=str(row["agent_id"]), agent_version=str(row["agent_version"]),
            model_alias=str(row["model_alias"]), status=str(row["status"]),
            created_at=float(row["created_at"]), updated_at=float(row["updated_at"]),
        )

    def session_paths(self, session: SessionRecord) -> SessionPaths:
        root = self.sessions_root / opaque_component(session.tenant_id) / "agents" / opaque_component(session.agent_id) / opaque_component(session.agent_version) / "sessions" / opaque_component(session.id)
        return SessionPaths(
            root=root,
            workspace=root / "workspace",
            dsh_state=root / "dsh-state",
            conversation=root / "conversation.json",
            artifacts=root / "workspace" / "artifacts",
        )

    def ensure_session_storage(self, session: SessionRecord) -> SessionPaths:
        paths = self.session_paths(session)
        for path in (paths.root, paths.workspace, paths.dsh_state, paths.artifacts):
            path.mkdir(parents=True, exist_ok=True)
            try:
                os.chmod(path, 0o700)
            except OSError:
                pass
        return paths

    def agent_version_for_session(self, session: SessionRecord) -> AgentVersion:
        with self.transaction() as connection:
            row = self._agent_row(connection, session.tenant_id, session.agent_id, session.agent_version)
        return AgentVersion(
            tenant_id=session.tenant_id, agent_id=session.agent_id, version=session.agent_version,
            display_name=str(row["display_name"]), default_model=str(row["default_model"]),
            config=parse_json(row["config_json"]),
        )

    def create_run(
        self,
        identity: Identity,
        session: SessionRecord,
        *,
        message: str,
        idempotency_key: str | None = None,
        source: str = "http",
    ) -> tuple[RunRecord, bool]:
        if not message.strip():
            raise ControlPlaneError("invalid_message", "message must be a non-empty string")
        if source not in {"http", "feishu"}:
            raise ControlPlaneError("invalid_source", "Run source is not supported")
        key = idempotency_key or uuid.uuid4().hex
        if len(key) > 256:
            raise ControlPlaneError("invalid_idempotency_key", "idempotency key is too long")
        now = utc_timestamp()
        agent = self.agent_version_for_session(session)
        snapshot = {
            "tenant_id": session.tenant_id,
            "agent_id": session.agent_id,
            "agent_version": session.agent_version,
            "model_alias": session.model_alias,
            "source": source,
            "agent_display_name": agent.display_name,
            "memory_collections": agent.config.get("memory_collections", []),
            "policy_version": session.agent_version,
            "skill_bundle_version": agent.config.get("skill_bundle_version", "local-v1"),
            "agent_config": agent.config,
        }
        with self.transaction(immediate=True) as connection:
            existing = connection.execute(
                "SELECT * FROM runs WHERE tenant_id = ? AND idempotency_key = ?",
                (identity.tenant_id, key),
            ).fetchone()
            if existing is not None:
                return self._run_from_row(existing), False
            run_id = f"r_{uuid.uuid4().hex}"
            connection.execute(
                """INSERT INTO runs(
                    id, tenant_id, principal_id, agent_id, agent_version, session_id,
                    model_alias, source, message, status, idempotency_key, config_snapshot_json,
                    created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'QUEUED', ?, ?, ?)""",
                (run_id, session.tenant_id, identity.principal_id, session.agent_id, session.agent_version,
                 session.id, session.model_alias, source, message, key, canonical_json(snapshot), now),
            )
            self._append_event_locked(connection, run_id, "queued", {"run_id": run_id, "status": "QUEUED"}, now)
            row = connection.execute("SELECT * FROM runs WHERE id = ?", (run_id,)).fetchone()
        assert row is not None
        return self._run_from_row(row), True

    def _run_from_row(self, row: sqlite3.Row) -> RunRecord:
        return RunRecord(
            id=str(row["id"]), tenant_id=str(row["tenant_id"]), principal_id=str(row["principal_id"]),
            agent_id=str(row["agent_id"]), agent_version=str(row["agent_version"]), session_id=str(row["session_id"]),
            model_alias=str(row["model_alias"]), source=str(row["source"]), message=str(row["message"]), status=str(row["status"]),
            idempotency_key=str(row["idempotency_key"]), lease_epoch=int(row["lease_epoch"]),
            executor_id=str(row["executor_id"]) if row["executor_id"] is not None else None,
            lease_expires_at=float(row["lease_expires_at"]) if row["lease_expires_at"] is not None else None,
            attempt=int(row["attempt"]), cancel_requested=bool(row["cancel_requested"]),
            config_snapshot=parse_json(row["config_snapshot_json"]), response=parse_json(row["response_json"]) if row["response_json"] else None,
            error=parse_json(row["error_json"]) if row["error_json"] else None,
            created_at=float(row["created_at"]), started_at=float(row["started_at"]) if row["started_at"] is not None else None,
            finished_at=float(row["finished_at"]) if row["finished_at"] is not None else None,
        )

    def get_run(self, identity: Identity, run_id: str) -> RunRecord:
        self.ensure_identity(identity)
        with self.transaction() as connection:
            row = connection.execute("SELECT * FROM runs WHERE id = ?", (run_id,)).fetchone()
        if row is None or row["tenant_id"] != identity.tenant_id or row["principal_id"] != identity.principal_id:
            raise ControlPlaneError("run_not_found", "Run was not found")
        return self._run_from_row(row)

    def session_for_run(self, run: RunRecord) -> SessionRecord:
        with self.transaction() as connection:
            row = connection.execute("SELECT * FROM sessions WHERE id = ?", (run.session_id,)).fetchone()
        if row is None:
            raise ControlPlaneError("session_not_found", "Run session was not found")
        return self._session_from_row(row)

    def claim_run(self, run_id: str, executor_id: str, *, lease_seconds: float) -> RunRecord:
        now = utc_timestamp()
        with self.transaction(immediate=True) as connection:
            row = connection.execute("SELECT * FROM runs WHERE id = ?", (run_id,)).fetchone()
            if row is None:
                raise ControlPlaneError("run_not_found", "Run was not found")
            if row["status"] != "QUEUED" or bool(row["cancel_requested"]):
                raise ControlPlaneError("run_not_claimable", "Run is not available for execution")
            active = connection.execute(
                "SELECT 1 FROM runs WHERE session_id = ? AND status IN ('LEASED', 'RUNNING') LIMIT 1",
                (row["session_id"],),
            ).fetchone()
            if active is not None:
                raise ControlPlaneError("session_busy", "Session already has an active run")
            epoch = int(row["lease_epoch"]) + 1
            expires = now + lease_seconds
            updated = connection.execute(
                "UPDATE runs SET status = 'LEASED', lease_epoch = ?, executor_id = ?, lease_expires_at = ?, attempt = attempt + 1 WHERE id = ? AND status = 'QUEUED'",
                (epoch, executor_id, expires, run_id),
            )
            if updated.rowcount != 1:
                raise ControlPlaneError("run_not_claimable", "Run is not available for execution")
            self._append_event_locked(connection, run_id, "status", {"run_id": run_id, "status": "LEASED", "lease_epoch": epoch}, now)
            claimed = connection.execute("SELECT * FROM runs WHERE id = ?", (run_id,)).fetchone()
        assert claimed is not None
        return self._run_from_row(claimed)

    def claim_next_run(self, executor_id: str, *, lease_seconds: float) -> RunRecord | None:
        now = utc_timestamp()
        with self.transaction(immediate=True) as connection:
            connection.execute(
                """UPDATE runs SET status = 'QUEUED', executor_id = NULL, lease_expires_at = NULL
                   WHERE status IN ('LEASED', 'RUNNING') AND lease_expires_at IS NOT NULL AND lease_expires_at < ?""",
                (now,),
            )
            row = connection.execute(
                """SELECT candidate.* FROM runs AS candidate
                   WHERE candidate.status = 'QUEUED' AND candidate.cancel_requested = 0
                   AND NOT EXISTS (
                     SELECT 1 FROM runs AS active
                     WHERE active.session_id = candidate.session_id
                     AND active.status IN ('LEASED', 'RUNNING')
                   )
                   ORDER BY candidate.created_at, candidate.id LIMIT 1"""
            ).fetchone()
            if row is None:
                return None
            epoch = int(row["lease_epoch"]) + 1
            expires = now + lease_seconds
            updated = connection.execute(
                """UPDATE runs SET status = 'LEASED', lease_epoch = ?, executor_id = ?,
                   lease_expires_at = ?, attempt = attempt + 1
                   WHERE id = ? AND status = 'QUEUED'""",
                (epoch, executor_id, expires, row["id"]),
            )
            if updated.rowcount != 1:
                return None
            self._append_event_locked(connection, str(row["id"]), "status", {"run_id": row["id"], "status": "LEASED", "lease_epoch": epoch}, now)
            claimed = connection.execute("SELECT * FROM runs WHERE id = ?", (row["id"],)).fetchone()
        assert claimed is not None
        return self._run_from_row(claimed)

    def mark_running(self, run: RunRecord, executor_id: str, *, lease_seconds: float) -> RunRecord:
        now = utc_timestamp()
        with self.transaction(immediate=True) as connection:
            updated = connection.execute(
                """UPDATE runs SET status = 'RUNNING', started_at = COALESCE(started_at, ?),
                   lease_expires_at = ? WHERE id = ? AND status = 'LEASED'
                   AND lease_epoch = ? AND executor_id = ?""",
                (now, now + lease_seconds, run.id, run.lease_epoch, executor_id),
            )
            if updated.rowcount != 1:
                raise ControlPlaneError("lease_lost", "Run lease was lost before execution")
            self._append_event_locked(connection, run.id, "status", {"run_id": run.id, "status": "RUNNING", "lease_epoch": run.lease_epoch}, now)
            row = connection.execute("SELECT * FROM runs WHERE id = ?", (run.id,)).fetchone()
        assert row is not None
        return self._run_from_row(row)

    def heartbeat_lease(self, run: RunRecord, executor_id: str, *, lease_seconds: float) -> bool:
        now = utc_timestamp()
        with self.transaction(immediate=True) as connection:
            updated = connection.execute(
                """UPDATE runs SET lease_expires_at = ? WHERE id = ? AND status = 'RUNNING'
                   AND lease_epoch = ? AND executor_id = ?""",
                (now + lease_seconds, run.id, run.lease_epoch, executor_id),
            )
        return updated.rowcount == 1

    def request_cancel(self, identity: Identity, run_id: str) -> RunRecord:
        run = self.get_run(identity, run_id)
        now = utc_timestamp()
        with self.transaction(immediate=True) as connection:
            if run.status in TERMINAL_RUN_STATUSES:
                row = connection.execute("SELECT * FROM runs WHERE id = ?", (run.id,)).fetchone()
            elif run.status == "QUEUED":
                connection.execute(
                    "UPDATE runs SET status = 'CANCELED', cancel_requested = 1, finished_at = ? WHERE id = ?",
                    (now, run.id),
                )
                self._append_event_locked(connection, run.id, "done", {"run_id": run.id, "status": "CANCELED"}, now)
                row = connection.execute("SELECT * FROM runs WHERE id = ?", (run.id,)).fetchone()
            else:
                connection.execute("UPDATE runs SET cancel_requested = 1 WHERE id = ?", (run.id,))
                self._append_event_locked(connection, run.id, "status", {"run_id": run.id, "status": "CANCEL_REQUESTED"}, now)
                row = connection.execute("SELECT * FROM runs WHERE id = ?", (run.id,)).fetchone()
        assert row is not None
        return self._run_from_row(row)

    def cancel_requested(self, run: RunRecord, executor_id: str) -> bool:
        with self.transaction() as connection:
            row = connection.execute(
                "SELECT cancel_requested FROM runs WHERE id = ? AND lease_epoch = ? AND executor_id = ?",
                (run.id, run.lease_epoch, executor_id),
            ).fetchone()
        return bool(row and row["cancel_requested"])

    def finish_run(
        self,
        run: RunRecord,
        executor_id: str,
        *,
        status: str,
        response: dict[str, Any] | None = None,
        error: dict[str, Any] | None = None,
    ) -> RunRecord:
        if status not in TERMINAL_RUN_STATUSES:
            raise ValueError("status must be terminal")
        now = utc_timestamp()
        with self.transaction(immediate=True) as connection:
            updated = connection.execute(
                """UPDATE runs SET status = ?, response_json = ?, error_json = ?, finished_at = ?,
                   lease_expires_at = NULL WHERE id = ? AND status IN ('LEASED', 'RUNNING')
                   AND lease_epoch = ? AND executor_id = ?""",
                (status, canonical_json(response or {}) if response is not None else None,
                 canonical_json(error or {}) if error is not None else None, now,
                 run.id, run.lease_epoch, executor_id),
            )
            if updated.rowcount != 1:
                raise ControlPlaneError("lease_lost", "Run lease was lost before completion")
            event = "done" if status == "SUCCEEDED" else "error"
            payload = {"run_id": run.id, "status": status}
            if response is not None:
                payload.update(response)
            if error is not None:
                payload["error"] = error
            self._append_event_locked(connection, run.id, event, payload, now)
            row = connection.execute("SELECT * FROM runs WHERE id = ?", (run.id,)).fetchone()
        assert row is not None
        return self._run_from_row(row)

    def append_event(self, run_id: str, event_type: str, payload: dict[str, Any]) -> int:
        now = utc_timestamp()
        with self.transaction(immediate=True) as connection:
            return self._append_event_locked(connection, run_id, event_type, payload, now)

    def register_artifact(
        self, run: RunRecord, *, name: str, kind: str, storage_key: str,
        size_bytes: int | None = None, content_type: str | None = None,
    ) -> dict[str, Any]:
        if kind not in {"image", "audio", "video", "file"}:
            raise ControlPlaneError("invalid_artifact", "Artifact kind is not allowed")
        artifact_id = f"a_{uuid.uuid4().hex}"
        now = utc_timestamp()
        with self.transaction(immediate=True) as connection:
            connection.execute(
                """INSERT INTO artifacts(
                    id, tenant_id, agent_id, session_id, run_id, kind, name, content_type,
                    size_bytes, storage_key, status, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'READY', ?)""",
                (artifact_id, run.tenant_id, run.agent_id, run.session_id, run.id, kind, name, content_type,
                 size_bytes, storage_key, now),
            )
        return {
            "artifact_id": artifact_id, "type": kind, "path": storage_key,
            "session_id": run.session_id, "run_id": run.id,
            **({"size": str(size_bytes)} if size_bytes is not None else {}),
        }

    def get_artifact(self, identity: Identity, artifact_id: str) -> dict[str, Any]:
        self.ensure_identity(identity)
        with self.transaction() as connection:
            row = connection.execute("SELECT * FROM artifacts WHERE id = ? AND status = 'READY'", (artifact_id,)).fetchone()
            if row is None or row["tenant_id"] != identity.tenant_id:
                raise ControlPlaneError("artifact_not_found", "Artifact was not found")
            if identity.role not in {"admin", "manager"} and row["session_id"]:
                session = connection.execute("SELECT principal_id FROM sessions WHERE id = ?", (row["session_id"],)).fetchone()
                if session is None or session["principal_id"] != identity.principal_id:
                    raise ControlPlaneError("artifact_not_found", "Artifact was not found")
            return {key: row[key] for key in row.keys()}

    def _append_event_locked(self, connection: sqlite3.Connection, run_id: str, event_type: str, payload: dict[str, Any], now: float) -> int:
        row = connection.execute("SELECT event_seq FROM runs WHERE id = ?", (run_id,)).fetchone()
        if row is None:
            raise ControlPlaneError("run_not_found", "Run was not found")
        event_id = int(row["event_seq"]) + 1
        connection.execute("UPDATE runs SET event_seq = ? WHERE id = ?", (event_id, run_id))
        connection.execute(
            "INSERT INTO run_events(run_id, event_id, event_type, payload_json, created_at) VALUES (?, ?, ?, ?, ?)",
            (run_id, event_id, event_type, canonical_json(payload), now),
        )
        return event_id

    def events_after(self, identity: Identity, run_id: str, event_id: int, *, limit: int = 200) -> list[dict[str, Any]]:
        self.get_run(identity, run_id)
        with self.transaction() as connection:
            rows = connection.execute(
                """SELECT event_id, event_type, payload_json, created_at FROM run_events
                   WHERE run_id = ? AND event_id > ? ORDER BY event_id LIMIT ?""",
                (run_id, event_id, limit),
            ).fetchall()
        return [
            {"event_id": int(row["event_id"]), "event": str(row["event_type"]), "data": parse_json(row["payload_json"]), "created_at": float(row["created_at"])}
            for row in rows
        ]

    def append_audit(self, tenant_id: str, event_type: str, payload: dict[str, Any], *, principal_id: str | None = None, run_id: str | None = None) -> None:
        with self.transaction(immediate=True) as connection:
            connection.execute(
                "INSERT INTO audit_events(tenant_id, principal_id, run_id, event_type, payload_json, created_at) VALUES (?, ?, ?, ?, ?, ?)",
                (tenant_id, principal_id, run_id, event_type, canonical_json(payload), utc_timestamp()),
            )
