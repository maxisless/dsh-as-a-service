from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from control_plane import ControlPlane, ControlPlaneError, Identity


class ControlPlaneTests(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.plane = ControlPlane(Path(self.directory.name), default_model="fast")
        self.admin = Identity("acme", "alice", "admin")
        self.user = Identity("acme", "bob", "chat")
        self.other = Identity("other", "eve", "chat")
        self.plane.ensure_identity(self.user)
        self.plane.publish_agent_version(
            self.admin,
            agent_id="sales",
            version="v1",
            display_name="Sales",
            default_model="fast",
            config={
                "allowed_models": ["fast"],
                "memory_collections": ["sales-approved"],
                "skill_bundle_version": "sales-bundle-v1",
            },
        )

    def tearDown(self) -> None:
        self.directory.cleanup()

    def test_session_binds_agent_version_and_has_opaque_private_paths(self) -> None:
        session = self.plane.create_session(self.user, agent_id="sales", model_alias="fast")
        paths = self.plane.ensure_session_storage(session)

        self.assertEqual(session.tenant_id, "acme")
        self.assertEqual(session.agent_id, "sales")
        self.assertEqual(session.agent_version, "v1")
        self.assertTrue(paths.workspace.is_dir())
        self.assertTrue(paths.dsh_state.is_dir())
        self.assertNotIn(paths.workspace, paths.artifacts.parents)
        self.assertNotIn("acme", str(paths.root))
        self.assertNotIn(session.id, str(paths.root))

    def test_idempotent_run_creation_pins_agent_snapshot(self) -> None:
        session = self.plane.create_session(self.user, agent_id="sales")
        first, created = self.plane.create_run(self.user, session, message="draft a quote", idempotency_key="message-1")
        second, duplicate = self.plane.create_run(self.user, session, message="ignored", idempotency_key="message-1")

        self.assertTrue(created)
        self.assertFalse(duplicate)
        self.assertEqual(first.id, second.id)
        self.assertEqual(first.config_snapshot["agent_id"], "sales")
        self.assertEqual(first.config_snapshot["agent_version"], "v1")
        self.assertEqual(first.config_snapshot["memory_collections"], ["sales-approved"])
        self.assertEqual(first.source, "http")

    def test_session_owner_cannot_be_crossed(self) -> None:
        session = self.plane.create_session(self.user, agent_id="sales")
        self.plane.ensure_identity(self.other)

        with self.assertRaises(ControlPlaneError) as raised:
            self.plane.get_session(self.other, session.id)

        self.assertEqual(raised.exception.code, "session_not_found")

    def test_lease_epoch_and_events_guard_terminal_completion(self) -> None:
        session = self.plane.create_session(self.user, agent_id="sales")
        run, _ = self.plane.create_run(self.user, session, message="hello")
        leased = self.plane.claim_run(run.id, "executor-1", lease_seconds=60)
        running = self.plane.mark_running(leased, "executor-1", lease_seconds=60)
        finished = self.plane.finish_run(
            running, "executor-1", status="SUCCEEDED", response={"answer": "done"},
        )

        self.assertEqual(finished.status, "SUCCEEDED")
        events = self.plane.events_after(self.user, run.id, 0)
        self.assertEqual([event["event_id"] for event in events], list(range(1, len(events) + 1)))
        self.assertEqual(events[-1]["event"], "done")
        with self.assertRaises(ControlPlaneError) as raised:
            self.plane.finish_run(running, "executor-1", status="SUCCEEDED", response={})
        self.assertEqual(raised.exception.code, "lease_lost")

    def test_cancel_queued_run_is_terminal_and_durable(self) -> None:
        session = self.plane.create_session(self.user, agent_id="sales")
        run, _ = self.plane.create_run(self.user, session, message="cancel me")
        canceled = self.plane.request_cancel(self.user, run.id)

        self.assertEqual(canceled.status, "CANCELED")
        self.assertTrue(canceled.cancel_requested)
        self.assertEqual(self.plane.events_after(self.user, run.id, 0)[-1]["event"], "done")

    def test_artifact_is_tenant_scoped(self) -> None:
        session = self.plane.create_session(self.user, agent_id="sales")
        run, _ = self.plane.create_run(self.user, session, message="artifact")
        artifact = self.plane.register_artifact(
            run, name="report.txt", kind="file", storage_key="/tmp/report.txt", size_bytes=3,
        )
        fetched = self.plane.get_artifact(self.user, artifact["artifact_id"])
        self.assertEqual(fetched["run_id"], run.id)
        self.plane.ensure_identity(self.other)
        with self.assertRaises(ControlPlaneError) as raised:
            self.plane.get_artifact(self.other, artifact["artifact_id"])
        self.assertEqual(raised.exception.code, "artifact_not_found")

    def test_artifact_owner_isolation_within_tenant(self) -> None:
        session = self.plane.create_session(self.user, agent_id="sales")
        run, _ = self.plane.create_run(self.user, session, message="artifact")
        artifact = self.plane.register_artifact(
            run, name="report.txt", kind="file", storage_key="/tmp/report.txt", size_bytes=3,
        )
        other_principal = Identity("acme", "charlie", "chat")
        self.plane.ensure_identity(other_principal)
        with self.assertRaises(ControlPlaneError) as raised:
            self.plane.get_artifact(other_principal, artifact["artifact_id"])
        self.assertEqual(raised.exception.code, "artifact_not_found")


if __name__ == "__main__":
    unittest.main()
