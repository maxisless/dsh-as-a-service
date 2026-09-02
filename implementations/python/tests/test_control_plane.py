from __future__ import annotations

import tempfile
import unittest
import hashlib
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

    def test_external_binding_is_stable_and_rejects_another_principal(self) -> None:
        first = self.plane.bind_external_conversation(
            self.user, source="feishu", external_conversation_id="oc_group_123", conversation_kind="group",
        )
        second = self.plane.bind_external_conversation(
            self.user, source="feishu", external_conversation_id="oc_group_123", conversation_kind="group",
        )
        self.assertEqual(first.session_id, second.session_id)
        self.assertEqual(first.external_conversation_hash, hashlib.sha256(b"oc_group_123").hexdigest())
        other = Identity("acme", "charlie", "chat")
        with self.assertRaises(ControlPlaneError) as raised:
            self.plane.bind_external_conversation(
                other, source="feishu", external_conversation_id="oc_group_123", conversation_kind="group",
            )
        self.assertEqual(raised.exception.code, "external_conversation_conflict")

    def test_external_binding_rejects_a_later_agent_change(self) -> None:
        first = self.plane.bind_external_conversation(
            self.user, source="feishu", external_conversation_id="oc_agent_123", conversation_kind="group",
            agent_id="sales", agent_version="v1",
        )
        self.assertTrue(first.session_id.startswith("s_"))
        with self.assertRaises(ControlPlaneError) as raised:
            self.plane.bind_external_conversation(
                self.user, source="feishu", external_conversation_id="oc_agent_123", conversation_kind="group",
                agent_id="default",
            )
        self.assertEqual(raised.exception.code, "external_conversation_conflict")

    def test_prepared_run_input_manifest_action_delivery_and_operations_summary(self) -> None:
        session = self.plane.create_session(self.user, agent_id="sales")
        run, created = self.plane.create_run(
            self.user, session, message="with attachment", idempotency_key="prepare-1", source="feishu", initial_status="PREPARING",
        )
        self.assertTrue(created)
        self.assertEqual(run.status, "PREPARING")
        input_record = self.plane.record_run_input(
            run, kind="image", name="reference.png", content_type="image/png", size_bytes=7,
            sha256=hashlib.sha256(b"content").hexdigest(), workspace_path="inbox/r/reference.png",
            source_type="feishu", source_ref="om_message_1",
        )
        self.assertEqual(input_record.source_ref_hash, hashlib.sha256(b"om_message_1").hexdigest())
        finalized = self.plane.finalize_input_manifest(run.id)
        self.assertEqual(finalized.status, "QUEUED")
        self.assertEqual(finalized.input_manifest_status, "FINALIZED")
        action, created = self.plane.create_or_get_action(
            finalized, action_key="tool-call-1", action_type="seedance_video", risk_class="billable",
            request_digest=hashlib.sha256(b"request").hexdigest(),
        )
        self.assertTrue(created)
        submitted = self.plane.update_action(action.id, status="SUBMITTED", provider_ref="task-1")
        self.assertEqual(submitted.provider_ref, "task-1")
        delivery, created = self.plane.create_delivery(
            finalized, destination_type="feishu_message", destination_ref="om_message_1",
            idempotency_key="delivery-1", action_id=action.id,
        )
        self.assertTrue(created)
        claimed = self.plane.claim_due_deliveries(destination_type="feishu_message")
        self.assertEqual([item.id for item in claimed], [delivery.id])
        done = self.plane.finish_delivery(delivery.id, delivered=True, provider_delivery_ref="om_reply_1")
        self.assertEqual(done.status, "DELIVERED")
        self.plane.record_usage(finalized, category="media", quantity={"seconds": 5}, estimated_cost_micros=100)
        summary = self.plane.operations_summary(self.admin)
        self.assertEqual(summary["runs"]["QUEUED"], 1)
        self.assertEqual(summary["deliveries"]["DELIVERED"], 1)
        self.assertEqual(summary["estimated_cost_micros"], 100)

    def test_stale_preparing_run_expires_when_scheduler_claims_work(self) -> None:
        session = self.plane.create_session(self.user, agent_id="sales")
        run, _ = self.plane.create_run(
            self.user, session, message="pending input", source="feishu", initial_status="PREPARING",
        )
        with self.plane.transaction(immediate=True) as connection:
            connection.execute("UPDATE runs SET created_at = ? WHERE id = ?", (0, run.id))
        self.assertIsNone(self.plane.claim_next_run("executor", lease_seconds=30))
        expired = self.plane.get_run(self.user, run.id)
        self.assertEqual(expired.status, "EXPIRED")


if __name__ == "__main__":
    unittest.main()
