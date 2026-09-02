from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from integrations.feishu import bridge_core
from integrations.feishu.profile import ProfileError, load_profile, secret_from_environment


def example_profile() -> dict[str, object]:
    return {
        "schema_version": 1,
        "kind": "feishu-channel-instance",
        "instance_id": "example-channel",
        "display": {"name": "Example Assistant", "subtitle": "Processing"},
        "worker": {"chat_url": "http://127.0.0.1:8765/chat", "container": "worker-1"},
        "agent": {"id": "research", "version": "v3"},
        "behavior": {"group_require_mention": True, "max_workers": 3},
        "credentials": {
            "app_id_env": "TEST_FEISHU_APP_ID",
            "app_secret_env": "TEST_FEISHU_APP_SECRET",
            "bot_open_id_env": "TEST_FEISHU_BOT_OPEN_ID",
            "internal_bridge_token_env": "TEST_DSH_BRIDGE_TOKEN",
        },
    }


class ProfileTests(unittest.TestCase):
    def write_profile(self, payload: dict[str, object]) -> Path:
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        path = Path(directory.name) / "profile.json"
        path.write_text(json.dumps(payload), encoding="utf-8")
        return path

    def test_loads_non_secret_instance_profile(self) -> None:
        profile = load_profile(self.write_profile(example_profile()))
        self.assertEqual(profile.instance_id, "example-channel")
        self.assertEqual(profile.agent_id, "research")
        self.assertEqual(profile.agent_version, "v3")
        self.assertEqual(profile.max_workers, 3)

    def test_rejects_non_worker_chat_url(self) -> None:
        payload = example_profile()
        payload["worker"] = {"chat_url": "https://example.com/not-chat", "container": "worker-1"}
        with self.assertRaisesRegex(ProfileError, "Worker /chat"):
            load_profile(self.write_profile(payload))

    def test_secret_values_are_environment_only(self) -> None:
        self.assertEqual(secret_from_environment("BRIDGE_SECRET", {"BRIDGE_SECRET": " value "}), "value")
        with self.assertRaisesRegex(ProfileError, "BRIDGE_SECRET"):
            secret_from_environment("BRIDGE_SECRET", {})


class BridgeCoreTests(unittest.TestCase):
    def test_extracts_text_and_media(self) -> None:
        content = json.dumps({
            "title": "Media",
            "content_v2": [[
                {"tag": "img", "image_key": "img_v3_abc123"},
                {"tag": "text", "text": "Describe this"},
            ]],
        })
        text, media = bridge_core.extract_inbound_content("post", content)
        self.assertIn("Describe this", text)
        self.assertEqual([(item.kind, item.key) for item in media], [("image", "img_v3_abc123")])

    def test_group_requires_a_bot_mention_when_configured(self) -> None:
        self.assertFalse(bridge_core.should_handle(
            sender_type="user", chat_type="group", message_type="text", mentions=[],
            bot_open_id="ou_bot", group_require_mention=True,
        ))
        mention = SimpleNamespace(id=SimpleNamespace(open_id="ou_bot"))
        self.assertTrue(bridge_core.should_handle(
            sender_type="user", chat_type="group", message_type="text", mentions=[mention],
            bot_open_id="ou_bot", group_require_mention=True,
        ))

    def test_media_prompt_reveals_only_workspace_relative_paths(self) -> None:
        prompt = bridge_core.prompt_with_inbound_media("look", [{
            "kind": "image", "path": "inbox/r1/ref.png", "sha256": "private-digest",
            "staged_path": "/private/worker/input.png", "source_ref": "img_private",
        }])
        self.assertIn('\"path\":\"inbox/r1/ref.png\"', prompt)
        self.assertNotIn("private-digest", prompt)
        self.assertNotIn("/private/worker/input.png", prompt)
        self.assertNotIn("img_private", prompt)

    def test_artifact_envelope_never_accepts_a_path(self) -> None:
        valid = {"artifact_id": "a_0123456789abcdef0123456789abcdef", "type": "image", "name": "result.png"}
        self.assertEqual(bridge_core.normalized_artifact(valid), valid)
        self.assertIsNone(bridge_core.normalized_artifact({**valid, "name": "../result.png"}))


if __name__ == "__main__":
    unittest.main()
