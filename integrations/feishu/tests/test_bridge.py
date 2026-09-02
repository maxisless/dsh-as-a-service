from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

TEST_DIRECTORY = Path(__file__).parent
if str(TEST_DIRECTORY) not in __import__("sys").path:
    __import__("sys").path.insert(0, str(TEST_DIRECTORY))


class BridgeRuntimeTests(unittest.TestCase):
    def profile_path(self, directory: Path) -> Path:
        path = directory / "profile.json"
        path.write_text(json.dumps({
            "schema_version": 1, "kind": "feishu-channel-instance", "instance_id": "test-channel",
            "display": {"name": "Test Channel", "subtitle": "Working"},
            "worker": {"chat_url": "http://127.0.0.1:8765/chat", "container": "worker-1"},
            "agent": {"id": "test-agent", "version": "v2"},
            "credentials": {
                "app_id_env": "TEST_FEISHU_APP_ID", "app_secret_env": "TEST_FEISHU_APP_SECRET",
                "bot_open_id_env": "TEST_FEISHU_BOT_OPEN_ID", "internal_bridge_token_env": "TEST_DSH_BRIDGE_TOKEN",
            },
        }), encoding="utf-8")
        return path

    def test_profile_drives_card_and_worker_agent_payload(self) -> None:
        from integrations.feishu import bridge

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            profile = self.profile_path(root)
            environment = {
                "TEST_FEISHU_APP_ID": "cli_test", "TEST_FEISHU_APP_SECRET": "secret",
                "TEST_FEISHU_BOT_OPEN_ID": "ou_test", "TEST_DSH_BRIDGE_TOKEN": "x" * 32,
                "FEISHU_CHANNEL_STATE_DB": str(root / "state.sqlite3"),
                "FEISHU_CHANNEL_ARTIFACT_DIR": str(root / "artifacts"),
            }
            with patch.dict(os.environ, environment, clear=False):
                bridge.configure(profile)
                self.assertEqual(bridge.streaming_card_json()["header"]["title"]["content"], "Test Channel")
                captured: dict[str, object] = {}

                class Response:
                    def __enter__(self):
                        return self

                    def __exit__(self, *_args):
                        return False

                    def read(self):
                        return json.dumps({
                            "session_id": "s_test",
                            "workspace": "/data/state/control-plane/tenants/t/agents/a/v1/sessions/s/workspace",
                        }).encode()

                def fake_urlopen(request, timeout):
                    captured["url"] = request.full_url
                    captured["body"] = json.loads(request.data.decode())
                    return Response()

                with patch.object(bridge, "urlopen", fake_urlopen):
                    bridge.resolve_worker_session(
                        "feishu:p2p:oc_test", external_conversation_id="feishu:p2p:oc_test",
                        conversation_kind="p2p", principal_ref="user:ou_test",
                    )
                self.assertEqual(captured["body"]["agent_id"], "test-agent")
                self.assertEqual(captured["body"]["agent_version"], "v2")

    def test_media_staging_keeps_only_run_relative_paths(self) -> None:
        from integrations.feishu import bridge

        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            input_file = directory / "image-1.png"
            input_file.write_bytes(b"input")
            staged = [{
                "kind": "image", "name": input_file.name, "content_type": "image/png",
                "size_bytes": 5, "sha256": "digest", "staged_path": str(input_file), "source_ref": "img_v3_test",
            }]
            completed = type("Completed", (), {"returncode": 0, "stderr": ""})
            with patch.object(bridge, "WORKER_CONTAINER", "worker-1"), patch.object(bridge.subprocess, "run", return_value=completed):
                result = bridge.copy_inbound_media_to_worker(
                    directory, staged,
                    "/data/state/control-plane/tenants/t/agents/a/v1/sessions/s/workspace/inbox/r1",
                    "/data/state/control-plane/tenants/t/agents/a/v1/sessions/s/workspace",
                )
            self.assertEqual(result[0]["path"], "inbox/r1/image-1.png")
            self.assertIn("/data/state/control-plane", result[0]["staged_path"])


if __name__ == "__main__":
    unittest.main()
