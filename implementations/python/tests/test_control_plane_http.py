from __future__ import annotations

import json
import os
import tempfile
import threading
import unittest
from http.client import HTTPConnection
from http.server import ThreadingHTTPServer
from pathlib import Path
from unittest.mock import patch

import server
from control_plane import ControlPlane


class FakeHarness:
    def __init__(self, **kwargs: object) -> None:
        self.kwargs = kwargs
        self.started = False

    def start(self) -> None:
        self.started = True

    def close(self) -> None:
        self.started = False

    def run(self, prompt: str, *, session_id: str, on_notification=None):
        if on_notification is not None:
            on_notification(server.Notification(method="session.status", payload={"sessionId": session_id, "status": "working"}))
        events: list[dict[str, object]] = []
        if prompt == "artifact":
            output = Path(str(self.kwargs["cwd"])) / "outputs" / "test.txt"
            output.parent.mkdir(parents=True, exist_ok=True)
            output.write_text("artifact", encoding="utf-8")
            events = [{
                "type": "tool/result",
                "data": {"message": {"content": [{
                    "type": "tool-result", "isError": False,
                    "content": [{"type": "text", "text": json.dumps({
                        "ok": True, "type": "private.result",
                        "artifacts": [{"kind": "file", "path": "outputs/test.txt"}],
                    })}],
                }]}}
            }]
        return type("Result", (), {"final_response": "control-plane-ok", "finish_reason": "completed", "events": events})()


class WorkerControlPlaneHttpTests(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.control = ControlPlane(Path(self.directory.name) / "control", default_model="deepseek")
        self.server_patch = patch.object(server, "CONTROL_PLANE", self.control)
        self.token_patch = patch.object(server, "CONTROL_PLANE_TOKEN", "test-token")
        self.internal_token_patch = patch.object(server, "INTERNAL_BRIDGE_TOKEN", "internal-test-token")
        self.key_patch = patch.object(server, "API_KEY_CONFIGURED", True)
        self.harness_patch = patch.object(server, "DeepSeekHarness", FakeHarness)
        self.scheduler_patch = patch.object(server, "ensure_scheduler", lambda: None)
        self.server_patch.start()
        self.token_patch.start()
        self.internal_token_patch.start()
        self.key_patch.start()
        self.harness_patch.start()
        self.scheduler_patch.start()
        server._model_runtimes.clear()
        server._session_locks.clear()
        server._runtime_session_ids.clear()
        server._hydrated_sessions.clear()
        self.http = ThreadingHTTPServer(("127.0.0.1", 0), server.Handler)
        self.thread = threading.Thread(target=self.http.serve_forever, daemon=True)
        self.thread.start()

    def tearDown(self) -> None:
        self.http.shutdown()
        self.http.server_close()
        self.thread.join(timeout=2)
        self.harness_patch.stop()
        self.scheduler_patch.stop()
        self.key_patch.stop()
        self.internal_token_patch.stop()
        self.token_patch.stop()
        self.server_patch.stop()
        server._model_runtimes.clear()
        self.directory.cleanup()

    @property
    def port(self) -> int:
        return int(self.http.server_address[1])

    def request(self, method: str, path: str, body: dict[str, object] | None = None, headers: dict[str, str] | None = None):
        payload = json.dumps(body).encode() if body is not None else None
        all_headers = dict(headers or {})
        if payload is not None:
            all_headers["Content-Type"] = "application/json"
            all_headers["Content-Length"] = str(len(payload))
        connection = HTTPConnection("127.0.0.1", self.port, timeout=5)
        connection.request(method, path, body=payload, headers=all_headers)
        response = connection.getresponse()
        raw = response.read()
        connection.close()
        return response.status, dict(response.getheaders()), raw

    def test_control_session_run_and_event_replay(self) -> None:
        auth = {"Authorization": "Bearer test-token"}
        status, _, raw = self.request("POST", "/v1/sessions", {"agent_id": "default"}, auth)
        self.assertEqual(status, 201)
        session = json.loads(raw)
        self.assertTrue(session["session_id"].startswith("s_"))
        # Public responses never disclose server filesystem topology.
        self.assertNotIn("workspace", session)
        stored_session = self.control.get_session(server.LOCAL_IDENTITY, session["session_id"])
        self.assertTrue(self.control.ensure_session_storage(stored_session).workspace.is_dir())

        status, _, raw = self.request(
            "POST",
            f"/v1/sessions/{session['session_id']}/runs",
            {"message": "hello"},
            {**auth, "Idempotency-Key": "message-1"},
        )
        self.assertEqual(status, 202)
        run = json.loads(raw)
        self.assertTrue(run["run_id"].startswith("r_"))
        claimed = self.control.claim_run(run["run_id"], "test-executor", lease_seconds=30)
        running = self.control.mark_running(claimed, "test-executor", lease_seconds=30)
        self.control.append_event(running.id, "assistant.delta", {"text": "hello"})
        self.control.finish_run(running, "test-executor", status="SUCCEEDED", response={"answer": "ok"})

        status, headers, raw = self.request(
            "GET",
            f"/v1/runs/{run['run_id']}/events",
            headers={**auth, "Last-Event-ID": "1"},
        )
        self.assertEqual(status, 200)
        self.assertEqual(headers["Content-Type"], "text/event-stream; charset=utf-8")
        text = raw.decode()
        self.assertIn("id: 2", text)
        self.assertIn("event: status", text)
        self.assertIn("event: assistant.delta", text)
        self.assertIn("event: done", text)

    def test_control_plane_requires_bearer_token(self) -> None:
        status, _, raw = self.request("POST", "/v1/sessions", {"agent_id": "default"})
        self.assertEqual(status, 401)
        self.assertEqual(json.loads(raw)["error"]["code"], "unauthorized")

    def test_admin_can_publish_agent_and_session_pins_it(self) -> None:
        auth = {"Authorization": "Bearer test-token"}
        status, _, raw = self.request(
            "POST", "/v1/agents",
            {
                "agent_id": "research", "version": "v1",
                "display_name": "Research", "default_model": "deepseek",
                "config": {"allowed_models": ["deepseek"], "memory_collections": ["approved-research"]},
            },
            auth,
        )
        self.assertEqual(status, 201)
        self.assertEqual(json.loads(raw)["agent_id"], "research")
        status, _, raw = self.request("POST", "/v1/sessions", {"agent_id": "research", "agent_version": "v1"}, auth)
        self.assertEqual(status, 201)
        session = json.loads(raw)
        self.assertEqual(session["agent_id"], "research")
        self.assertEqual(session["agent_version"], "v1")

    def test_idempotency_key_reuses_one_durable_run(self) -> None:
        auth = {"Authorization": "Bearer test-token"}
        status, _, raw = self.request("POST", "/v1/sessions", {"agent_id": "default"}, auth)
        self.assertEqual(status, 201)
        session_id = json.loads(raw)["session_id"]
        headers = {**auth, "Idempotency-Key": "same-message"}
        status, _, first_raw = self.request("POST", f"/v1/sessions/{session_id}/runs", {"message": "hello"}, headers)
        duplicate_status, _, second_raw = self.request("POST", f"/v1/sessions/{session_id}/runs", {"message": "hello again"}, headers)

        self.assertEqual(status, 202)
        self.assertEqual(duplicate_status, 200)
        self.assertEqual(json.loads(first_raw)["run_id"], json.loads(second_raw)["run_id"])

    def test_compat_chat_uses_control_plane_session_workspace(self) -> None:
        status, _, raw = self.request("POST", "/chat", {"session_id": "legacy-demo", "message": "hello"})
        self.assertEqual(status, 200)
        response = json.loads(raw)
        self.assertTrue(response["run_id"].startswith("r_"))
        session = self.control.get_session(server.LOCAL_IDENTITY, "legacy-demo")
        paths = self.control.ensure_session_storage(session)
        self.assertTrue(paths.workspace.is_dir())
        self.assertTrue(paths.conversation.is_file())
        self.assertNotIn(str(server.WORKSPACE), str(paths.workspace))

    def test_internal_session_resolve_is_token_protected_and_exposes_only_bridge_workspace(self) -> None:
        status, _, raw = self.request(
            "POST", "/internal/sessions/resolve", {"session_id": "bridge-demo"},
            {"X-DSH-Internal-Token": "internal-test-token"},
        )
        self.assertEqual(status, 200)
        payload = json.loads(raw)
        self.assertTrue(Path(payload["workspace"]).is_dir())
        status, _, raw = self.request(
            "POST", "/internal/sessions/resolve", {"session_id": "bridge-demo"},
        )
        self.assertEqual(status, 401)
        self.assertEqual(json.loads(raw)["error"]["code"], "unauthorized")

    def test_compat_chat_returns_registered_artifacts_without_server_path(self) -> None:
        with patch.object(server, "ARTIFACT_RESULT_TYPES", frozenset({"private.result"})):
            status, _, raw = self.request(
                "POST", "/chat", {"session_id": "artifact-demo", "message": "artifact"},
            )
        self.assertEqual(status, 200)
        response = json.loads(raw)
        self.assertEqual(len(response["artifacts"]), 1)
        artifact = response["artifacts"][0]
        self.assertEqual(artifact["type"], "file")
        self.assertNotIn("path", artifact)
        self.assertNotIn("storage_key", artifact)

        status, headers, body = self.request(
            "GET", f"/v1/artifacts/{artifact['artifact_id']}",
            headers={"Authorization": "Bearer test-token"},
        )
        self.assertEqual(status, 200)
        self.assertEqual(headers["Content-Type"], "text/plain")
        self.assertEqual(body, b"artifact")

    def test_stream_done_returns_registered_artifacts_without_server_path(self) -> None:
        with patch.object(server, "ARTIFACT_RESULT_TYPES", frozenset({"private.result"})):
            status, headers, raw = self.request(
                "POST", "/chat/stream", {"session_id": "stream-artifact-demo", "message": "artifact"},
            )
        self.assertEqual(status, 200)
        self.assertEqual(headers["Content-Type"], "text/event-stream; charset=utf-8")
        frames = raw.decode("utf-8").split("\n\n")
        done = next(frame for frame in frames if "event: done" in frame)
        data_line = next(line for line in done.splitlines() if line.startswith("data: "))
        payload = json.loads(data_line.removeprefix("data: "))
        self.assertEqual(len(payload["artifacts"]), 1)
        self.assertNotIn("path", payload["artifacts"][0])

    def test_artifact_materialization_rejects_symlink_even_when_target_is_in_workspace(self) -> None:
        session = self.control.create_session(server.LOCAL_IDENTITY)
        run, _ = self.control.create_run(server.LOCAL_IDENTITY, session, message="artifact")
        workspace = self.control.ensure_session_storage(session).workspace
        target = workspace / "target.txt"
        target.write_text("artifact", encoding="utf-8")
        output = workspace / "outputs" / "linked.txt"
        output.parent.mkdir(parents=True)
        os.symlink(target, output)
        events = [{
            "type": "tool/result",
            "data": {"message": {"content": [{
                "type": "tool-result", "isError": False,
                "content": [{"type": "text", "text": json.dumps({
                    "ok": True, "type": "private.result",
                    "artifacts": [{"kind": "file", "path": "outputs/linked.txt"}],
                })}],
            }]}}
        }]
        with patch.object(server, "ARTIFACT_RESULT_TYPES", frozenset({"private.result"})):
            self.assertEqual(server.materialize_run_artifacts(run, session, events), [])

    def test_artifact_materialization_requires_declared_result_type_and_workspace_path(self) -> None:
        session = self.control.create_session(server.LOCAL_IDENTITY)
        run, _ = self.control.create_run(server.LOCAL_IDENTITY, session, message="artifact")
        workspace = self.control.ensure_session_storage(session).workspace
        output = workspace / "outputs" / "test.txt"
        output.parent.mkdir(parents=True)
        output.write_text("artifact", encoding="utf-8")
        events = [{
            "type": "tool/result",
            "data": {"message": {"content": [{
                "type": "tool-result",
                "isError": False,
                "content": [{"type": "text", "text": json.dumps({
                    "ok": True, "type": "private.result",
                    "artifacts": [{"kind": "file", "path": "outputs/test.txt"}],
                })}],
            }]}}
        }]
        with patch.object(server, "ARTIFACT_RESULT_TYPES", frozenset({"private.result"})):
            artifacts = server.materialize_run_artifacts(run, session, events)
        self.assertEqual(len(artifacts), 1)
        stored = self.control.get_artifact(server.LOCAL_IDENTITY, artifacts[0]["artifact_id"])
        self.assertTrue(Path(stored["storage_key"]).is_file())


if __name__ == "__main__":
    unittest.main()
