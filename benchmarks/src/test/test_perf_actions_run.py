from datetime import datetime
from io import BytesIO
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
import actions_run
import actions_report
import actions_health
import actions_watch
import catalog
import notifications
import runner


class ActionsRunTest(unittest.TestCase):
    def test_admission_ignores_nightly_conclusion_but_checks_source_and_attempts(self):
        upstream = {"name": "Nightly Test", "event": "schedule", "run_attempt": 1,
                    "head_branch": "main", "head_repository": {"id": 1}}
        event = {"action": "completed", "repository": {"id": 1, "default_branch": "main"},
                 "workflow_run": upstream}
        for conclusion in ("success", "failure"):
            upstream["conclusion"] = conclusion
            self.assertTrue(actions_run.eligible_event("workflow_run", event, 1))
        self.assertFalse(actions_run.eligible_event("workflow_run", event, 2))
        for field, value in (("event", "workflow_dispatch"), ("run_attempt", 2), ("head_branch", "feature"),
                             ("head_repository", {"id": 2})):
            with self.subTest(field=field):
                changed = dict(event, workflow_run=dict(upstream, **{field: value}))
                self.assertFalse(actions_run.eligible_event("workflow_run", changed, 1))
        self.assertTrue(actions_run.eligible_event("workflow_dispatch", {}, 1))

    def test_manual_inputs_map_to_the_shared_selector(self):
        self.assertEqual([], actions_run.selection("all"))
        self.assertEqual(["--where", "group=nest"], actions_run.selection("group", group="nest"))
        self.assertEqual(["--id", "copy.snapshot.basic.standard@1", "--id", "map.live.basic.standard"],
                         actions_run.selection("ids", case_ids="copy.snapshot.basic.standard@1, map.live.basic.standard"))
        self.assertEqual(["--where", "operators=nest", "--where", "profile=wide"],
                         actions_run.selection("filter", filters="operators=nest profile=wide"))
        with self.assertRaisesRegex(ValueError, "matching input"):
            actions_run.selection("all", group="copy")
        with self.assertRaisesRegex(ValueError, "matching input"):
            actions_run.selection("group", group="copy", case_ids="map.live.basic.standard")

    def test_queue_is_separate_from_deadline_and_nightly_sets_business_date(self):
        run = {"created_at": "2026-11-21T16:04:00Z", "run_started_at": "2026-11-21T16:14:00Z"}
        upstream = {"id": 12, "run_attempt": 1, "created_at": "2026-11-21T10:00:00Z",
                    "updated_at": "2026-11-21T16:03:59Z", "html_url": "https://github.com/example/runs/12"}
        timing = actions_run.run_timing(run, upstream)
        self.assertEqual("2026-11-21", timing["business_date"])
        self.assertEqual(600, timing["queue_seconds"])
        self.assertEqual(upstream["updated_at"], timing["nightly"]["completed_at"])
        started = datetime.fromisoformat(timing["started_at"]).timestamp()
        self.assertEqual(25200, timing["deadline_epoch_seconds"] - started)
        with patch.object(actions_run.time, "time", return_value=started + 3600):
            self.assertEqual(21000, actions_run.remaining_seconds(timing, reserve=600))

    def test_report_indexes_only_same_attempt_artifacts_and_retains_expired_batches(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            attempt = {"run_id": 111, "attempt": 1}
            runner.write_json(root / "run.json", {"github_attempt": attempt, "scope": "all"})
            ids = ["copy.snapshot.basic.standard", "copy.backlog.fixed-iud.standard"]
            manifest = {"github_attempt": attempt, "identity": {"code_sha": "current"}, "selected_ids": ids,
                        "unallocated": [], "environment": "runner",
                        "batches": [{"index": index, "entries": [
                            {"case": {"id": case_id, "case_version": 2, "group": "copy", "mode": "snapshot",
                                      "tags": [], "spec": "specs/copy-basic.json"}, "repeats": 1}
                        ]} for index, case_id in enumerate(ids)]}
            manifest["manifest_sha256"] = catalog.digest(manifest)
            runner.write_json(root / "manifest.json", manifest)
            batch = root / "performance-batch-1-0" / "run"
            batch.mkdir(parents=True)
            runner.write_json(batch / "summary.json", {
                "identity": manifest["identity"], "manifest_sha256": manifest["manifest_sha256"],
                "batch": 0, "github_attempt": dict(attempt, attempt=2), "results": [],
            })
            expired = root / "performance-batch-1-1"
            expired.mkdir()
            (expired / "expired.txt").touch()
            result = actions_report.collect(root, root, attempt, {"prepare": "success", "measure": "failure"})
            self.assertEqual(ids, result["selected_ids"])
            self.assertEqual({}, result["artifact_names"])
            self.assertEqual(["batch summary binding mismatch", "batch expired before measurement"],
                             [error["reason"] for error in result["errors"]])
            self.assertEqual(["missing", "missing"], [row["status"] for row in result["results"]])
            text = actions_report.summary(result, "tapstate/tapstate")
            self.assertIn("copy.backlog.fixed-iud.standard@2", text)
            self.assertIn("--name performance-report-1", text)

    def test_independent_health_distinguishes_a_queued_handoff_from_a_missing_run(self):
        now = datetime.fromisoformat("2026-11-21T15:00:00+00:00")
        nightly = {"id": 20, "event": "schedule", "run_attempt": 1, "status": "completed",
                   "created_at": "2026-11-21T10:00:00Z", "updated_at": "2026-11-21T14:00:00Z"}
        queued = {"id": 21, "event": "workflow_run", "run_attempt": 1, "status": "queued",
                  "display_title": "Pipeline Performance | Nightly 20/1", "created_at": "2026-11-21T14:00:01Z"}
        cases = [
            ([], [], "missing-nightly"),
            ([dict(nightly, status="in_progress")], [], "nightly-running"),
            ([nightly], [], "missing-handoff"),
            ([nightly], [dict(queued, run_attempt=2)], "missing-handoff"),
            ([nightly], [queued], "performance-queued"),
            ([nightly], [dict(queued, status="completed", run_started_at="2026-11-21T14:00:01Z")], "report-missing"),
        ]
        for nightlies, jobs, status in cases:
            with self.subTest(status=status):
                self.assertEqual(status, actions_health.assess(now, nightlies, jobs, {})["status"])

    def test_failed_allocation_accounts_for_every_selected_case_as_not_run(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            attempt = {"run_id": 111, "attempt": 1}
            runner.write_json(root / "run.json", {"github_attempt": attempt, "scope": "all"})
            selected = [{"id": case, "version": 1, "repeats": 3, "profile": "standard"} for case in ("first", "second")]
            runner.write_json(root / "manifest.json", {
                "identity": {"code_sha": "candidate"}, "github_attempt": attempt, "environment": "hosted",
                "batches": [], "selected_cases": selected, "selected_ids": ["first", "second"],
                "unallocated": [{"id": "first", "reason": "missing formal workload"}],
            })
            result = actions_report.collect(root, root, attempt, {"prepare": "failure", "measure": "skipped"})
            self.assertEqual({"not-run": 2}, result["case_counts"])
            self.assertEqual({"not-run": 6}, result["repeat_counts"])
            self.assertEqual("missing formal workload", result["results"][0]["reason"])
            self.assertEqual("allocation incomplete", result["results"][-1]["reason"])
            active, _ = notifications.signals({"status": "reported", "report": result})
            self.assertEqual({"workflow"}, set(active))

    def test_notification_only_repeats_on_escalation_and_qualified_recovery(self):
        first = {"kind": "workflow", "reason": "missing-handoff"}
        changed = {"kind": "workflow", "reason": "report-missing"}
        initial, state = notifications.changes({}, {"workflow": first}, set())
        self.assertEqual([first], initial)
        self.assertEqual(([], state), notifications.changes(state, {"workflow": first}, set()))
        self.assertEqual(([], state), notifications.changes(state, {}, set()))
        update, state = notifications.changes(state, {"workflow": changed}, set())
        self.assertEqual([changed], update)
        recovery, state = notifications.changes(state, {}, {"workflow"})
        self.assertEqual([dict(changed, recovered=True)], recovery)
        self.assertEqual(([], {}), notifications.changes(state, {}, {"workflow"}))

    def test_notification_state_advances_only_after_an_accepted_delivery_receipt(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory)
            runner.write_json(output / "card.json", {"msg_type": "interactive"})
            runner.write_json(output / "next-state.json", {"active": {"workflow": {"reason": "missing-handoff"}}})
            with patch.object(actions_watch, "urlopen", return_value=BytesIO(b'{"code":1}')):
                with self.assertRaisesRegex(ValueError, "did not accept"):
                    actions_watch.send(output, "https://example.invalid/fixture")
            self.assertFalse((output / "state.json").exists())
            self.assertTrue((output / "next-state.json").exists())
            with patch.object(actions_watch, "urlopen", return_value=BytesIO(b'{"code":0}')):
                actions_watch.send(output, "https://example.invalid/fixture")
            self.assertTrue((output / "state.json").exists())
            self.assertFalse((output / "next-state.json").exists())

    def test_delivery_intent_prevents_a_duplicate_when_state_artifact_upload_is_lost(self):
        now = datetime.fromisoformat("2026-11-21T15:00:00+00:00")
        delivery_intent = {"workflow_run": {"id": 10}, "id": 11, "created_at": now.isoformat()}
        draft_state = {"updated_at": now.isoformat(), "active": {"workflow": {"reason": "missing-handoff"}}}
        with patch.object(actions_watch, "latest_artifact", side_effect=[None, delivery_intent]), \
                patch.object(actions_watch, "artifact", side_effect=[{"status": "accepted"}, draft_state]):
            self.assertEqual(draft_state, actions_watch.latest_state("example/repo", "main", now))
        with patch.object(actions_watch, "latest_artifact", side_effect=[None, delivery_intent]), \
                patch.object(actions_watch, "artifact", return_value={"status": "unknown"}):
            with self.assertRaisesRegex(ValueError, "run 10 is uncertain"):
                actions_watch.latest_state("example/repo", "main", now)

    def test_mixed_card_uses_full_counts_and_one_example_per_category(self):
        result = {"environment": "hosted", "errors": [{"reason": "batch expired"}],
                  "case_counts": {"failed": 4, "completed": 4},
                  "comparison_counts": {"suspected-regression": 4, "unavailable": 4},
                  "selected_ids": [], "case_context": {}, "case_verdicts": {}, "results": []}
        for category in ("failure", "regression"):
            for number in range(1, 5):
                case_id = f"{category}.{number}"
                result["selected_ids"].append(case_id)
                result["case_context"][case_id] = {"version": 2, "profile": "standard", "suites": ["basic"],
                                                   "scenario": f"Scenario {case_id}", "effective_load": {"rows": 10000}}
                if category == "failure":
                    result["case_verdicts"][case_id] = {"execution": "failed", "metrics": []}
                    result["results"].append({"case_id": case_id, "status": "failed",
                                              "last_stage": "readback", "reason": "different rows"})
                else:
                    metric = {"status": "suspected-regression", "metric": "endpoint_p95_nanos", "arm": "single",
                              "baseline": 100, "observed_median": 125, "change_ratio": 0.25}
                    result["case_verdicts"][case_id] = {"execution": "completed", "metrics": [metric]}
        health = {"status": "reported", "due_at": "2026-11-21T10:00:00+00:00", "report": result}
        active, healthy = notifications.signals(health)
        updates, _ = notifications.changes({}, active, healthy)
        url = "https://github.com/example/repo/actions/runs/10/attempts/1"
        card = notifications.card(health, updates, "ou_fixture", url)
        markdown, action = card["card"]["elements"]
        content = markdown["content"]
        self.assertIn("<at id=ou_fixture></at>", content)
        self.assertIn("Planned 8 | Completed 4 | Failed 4", content)
        self.assertIn("Comparable 4 | Suspected regressions 4", content)
        self.assertIn("failure.1@2 (basic)", content)
        self.assertIn("readback: different rows", content)
        self.assertIn("regression.1@2 (basic)", content)
        self.assertIn("100 → 125 (+25.0%)", content)
        self.assertNotIn("failure.2", content)
        self.assertNotIn("regression.2", content)
        self.assertEqual(url, action["actions"][0]["url"])
