from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import server


class ModelCatalogTests(unittest.TestCase):
    def test_default_catalog_is_portable(self) -> None:
        self.assertEqual(server.CONFIG_DEFAULT_MODEL_ALIAS, "deepseek")
        self.assertEqual(server.MODEL_ROUTES["deepseek"].provider, "deepseek")
        self.assertEqual(server.MODEL_ROUTES["deepseek"].endpoint, "deepseek-chat")
        self.assertEqual(server.resolve_model_alias("deepseek"), "deepseek")

    def test_loads_multiple_aliases_from_a_custom_catalog(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "models.json"
            path.write_text(json.dumps({
                "default": "fast",
                "models": {
                    "fast": {"provider": "provider-a", "endpoint": "model-fast"},
                    "accurate": {"provider": "provider-a", "endpoint": "model-accurate"},
                },
                "aliases": {"default": "fast"},
            }), encoding="utf-8")
            routes, aliases, default = server.load_model_catalog(path)

        self.assertEqual(default, "fast")
        self.assertEqual(aliases, {"default": "fast"})
        self.assertEqual(routes["accurate"].endpoint, "model-accurate")

    def test_models_payload_lists_canonical_models_only(self) -> None:
        payload = server.models_payload()

        self.assertEqual(payload["default"], "deepseek")
        self.assertEqual([model["alias"] for model in payload["models"]], ["deepseek"])
        self.assertEqual(payload["models"][0]["aliases"], [])

    def test_unknown_model_is_rejected(self) -> None:
        with self.assertRaises(server.ApiError) as raised:
            server.resolve_model_alias("not-a-model")

        self.assertEqual(raised.exception.status, 400)
        self.assertEqual(raised.exception.code, "invalid_model")


class SessionModelBindingTests(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.conversation_root = Path(self.directory.name)
        self.root_patch = patch.object(server, "CONVERSATION_ROOT", self.conversation_root)
        self.root_patch.start()
        server._session_model_bindings.clear()
        server._runtime_session_ids.clear()
        server._hydrated_sessions.clear()

    def tearDown(self) -> None:
        self.root_patch.stop()
        self.directory.cleanup()

    def test_first_request_binds_and_persists_default_alias(self) -> None:
        selected = server.bind_session_model("route-test", None)

        self.assertEqual(selected, "deepseek")
        record = json.loads(server.conversation_path("route-test").read_text(encoding="utf-8"))
        self.assertEqual(record["model"], "deepseek")
        self.assertEqual(record["turns"], [])
        self.assertEqual(server.bind_session_model("route-test", "deepseek"), "deepseek")

    def test_legacy_conversation_is_bound_to_default_without_losing_turns(self) -> None:
        path = server.conversation_path("legacy-session")
        path.write_text(json.dumps({
            "version": 1,
            "session_id": "legacy-session",
            "turns": [{"user": "hello", "assistant": "hi"}],
        }), encoding="utf-8")

        selected = server.bind_session_model("legacy-session", None)
        record = json.loads(path.read_text(encoding="utf-8"))

        self.assertEqual(selected, "deepseek")
        self.assertEqual(record["model"], "deepseek")
        self.assertEqual(record["turns"], [{"user": "hello", "assistant": "hi"}])

    def test_runtime_session_identity_includes_model_alias(self) -> None:
        with patch.object(server, "MODEL_ROUTES", {
            "first": server.ModelRoute("first", "provider-a", "model-first"),
            "second": server.ModelRoute("second", "provider-a", "model-second"),
        }), patch.object(server, "MODEL_ALIASES", {}), patch.object(server, "DEFAULT_MODEL_ALIAS", "first"):
            first = server.runtime_session_id("same-session", "first")
            second = server.runtime_session_id("same-session", "second")

        self.assertNotEqual(first, second)


class RuntimeRoutingTests(unittest.TestCase):
    class FakeHarness:
        def __init__(self, **kwargs: object) -> None:
            self.kwargs = kwargs
            self.start_calls = 0
            self.close_calls = 0

        def start(self) -> None:
            self.start_calls += 1

        def close(self) -> None:
            self.close_calls += 1

    def setUp(self) -> None:
        server._model_runtimes.clear()

    def tearDown(self) -> None:
        server._model_runtimes.clear()

    def test_each_alias_gets_an_independent_harness(self) -> None:
        routes = {
            "first": server.ModelRoute("first", "provider-a", "model-first"),
            "second": server.ModelRoute("second", "provider-a", "model-second"),
        }
        with patch.object(server, "MODEL_ROUTES", routes), patch.object(server, "MODEL_ALIASES", {}), patch.object(server, "DEFAULT_MODEL_ALIAS", "first"), patch.object(server, "DeepSeekHarness", self.FakeHarness):
            first = server.get_model_runtime("first")
            second = server.get_model_runtime("second")

        self.assertIsNot(first, second)
        self.assertIsNot(first.harness, second.harness)
        self.assertEqual(first.harness.kwargs["model"], "model-first")
        self.assertEqual(second.harness.kwargs["model"], "model-second")

    def test_runtime_start_is_idempotent_per_model(self) -> None:
        with patch.object(server, "DeepSeekHarness", self.FakeHarness):
            server.ensure_started("deepseek")
            server.ensure_started("deepseek")

        self.assertEqual(server._model_runtimes["deepseek"].harness.start_calls, 1)


class RequestModelTests(unittest.TestCase):
    def setUp(self) -> None:
        self.api_key_patch = patch.object(server, "API_KEY_CONFIGURED", True)
        self.api_key_patch.start()

    def tearDown(self) -> None:
        self.api_key_patch.stop()

    class RequestStub:
        def __init__(self, request: dict[str, object]) -> None:
            self.request = request

        def read_json(self) -> dict[str, object]:
            return self.request

    def test_model_is_optional_and_defaults_later_at_binding(self) -> None:
        parsed = server.Handler.parse_chat_request(self.RequestStub({"message": "hello"}))

        self.assertIsNone(parsed.model_alias)
        self.assertTrue(parsed.session_id.startswith("session-"))

    def test_model_alias_is_normalized_in_request(self) -> None:
        parsed = server.Handler.parse_chat_request(self.RequestStub({
            "message": "hello",
            "session_id": "route-test",
            "model": "deepseek",
        }))

        self.assertEqual(parsed.model_alias, "deepseek")

    def test_invalid_model_type_is_rejected(self) -> None:
        with self.assertRaises(server.ApiError) as raised:
            server.Handler.parse_chat_request(self.RequestStub({
                "message": "hello",
                "model": 42,
            }))

        self.assertEqual(raised.exception.status, 400)
        self.assertEqual(raised.exception.code, "invalid_model")


if __name__ == "__main__":
    unittest.main()
