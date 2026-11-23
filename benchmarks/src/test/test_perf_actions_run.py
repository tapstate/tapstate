from argparse import Namespace
import copy
import hashlib
import json
from datetime import datetime, timedelta
from io import BytesIO
import os
from pathlib import Path
import sys
import subprocess
import tempfile
import unittest
from urllib.error import HTTPError
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
import actions_run
import actions_report
import actions_health
import actions_watch
import baseline
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

    def test_preparation_reports_missing_work_and_emits_no_matrix(self):
        now = datetime.fromisoformat("2026-11-21T15:00:00+00:00")
        run = {"id": 111, "run_attempt": 1, "created_at": now.isoformat(), "run_started_at": now.isoformat()}
        attempt = {"run_id": 111, "attempt": 1}
        case_id = "nest.snapshot.fanout.wide"
        manifest = {
            "identity": {"code_sha": "candidate"}, "environment": "ubuntu-24.04-x64-op",
            "selected_ids": [case_id], "selected_cases": [
                {"id": case_id, "version": 2, "profile": "wide", "repeats": 3, "paired_control": True},
            ],
            "batches": [], "unallocated": [{"id": case_id, "reason": "missing whole-case estimate"}],
            "scale": "formal", "whole_case_estimates_seconds": {},
        }
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            summary = root / "summary"
            matrix = root / "matrix"
            args = Namespace(output=root / "prepare", workloads=root / "workloads", estimates=root / "estimates")

            def allocate(command, *, check):
                runner.write_json(Path(command[-1]), manifest)
                return Namespace(returncode=int(bool(manifest["unallocated"])))

            with patch.dict(os.environ, {"GITHUB_STEP_SUMMARY": str(summary), "GITHUB_OUTPUT": str(matrix)}), \
                    patch.object(actions_run.time, "time", return_value=now.timestamp()), \
                    patch.object(actions_run.baseline, "pin", return_value={"status": "unavailable"}), \
                    patch.object(actions_run.subprocess, "run", side_effect=allocate):
                self.assertEqual(1, actions_run.prepare(args, "workflow_dispatch", {}, attempt, run))
                self.assertIn("| nest.snapshot.fanout.wide@2 | wide | 3 | Yes | - | missing whole-case estimate |",
                              summary.read_text())
                self.assertFalse(matrix.exists())
                manifest.update(scale="qualification", unallocated=[])
                args.output = root / "qualification"
                with self.assertRaisesRegex(ValueError, "formal workload manifest"):
                    actions_run.prepare(args, "workflow_dispatch", {}, attempt, run)
                self.assertFalse(matrix.exists())

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
        self.assertEqual(1620, timing["deadline_epoch_seconds"] - timing["measurement_stop_epoch_seconds"])
        with patch.object(actions_run.time, "time", return_value=started + 3600):
            self.assertEqual(21000, actions_run.remaining_seconds(timing, reserve=600))

    def test_workflow_clock_skew_does_not_extend_budget_or_refuse_preparation(self):
        run = {"created_at": "2026-11-23T07:25:45Z", "run_started_at": "2026-11-23T07:25:44Z"}
        timing = actions_run.run_timing(run, None)
        self.assertEqual(0, timing["queue_seconds"])
        self.assertEqual(-1, timing["start_minus_creation_seconds"])
        self.assertEqual("2026-11-23T07:25:44+00:00", timing["started_at"])
        self.assertEqual(int(actions_run.timestamp(run["run_started_at"]).timestamp()) + 25200,
                         timing["deadline_epoch_seconds"])

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

    def test_missing_batch_evidence_keeps_distinct_upload_and_execution_failures(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            jobs_path = root / "jobs.json"
            runner.write_json(jobs_path, {"jobs": [
                {"name": "measure (0)", "conclusion": "failure", "steps": [
                    {"name": "Build and measure the fixed batch", "conclusion": "failure"},
                    {"name": "Upload batch evidence", "conclusion": "success"},
                ]},
                {"name": "measure (1)", "conclusion": "failure", "steps": [
                    {"name": "Build and measure the fixed batch", "conclusion": "success"},
                    {"name": "Upload batch evidence", "conclusion": "failure"},
                ]},
                {"name": "measure (3)", "conclusion": "failure", "steps": [
                    {"name": "Run actions/setup-java@v4", "conclusion": "failure"},
                    {"name": "Upload batch evidence", "conclusion": "failure"},
                ]},
            ]})
            attempt = {"run_id": 111, "attempt": 1}
            runner.write_json(root / "run.json", {"github_attempt": attempt})
            ids = ["copy.snapshot.basic.standard", "copy.backlog.fixed-iud.standard",
                   "copy.live.basic.standard", "join.live.hot-update.standard"]
            manifest = {"github_attempt": attempt, "identity": {"code_sha": "current"}, "selected_ids": ids,
                        "unallocated": [], "environment": "runner",
                        "batches": [{"index": index, "entries": [
                            {"case": {"id": case_id, "case_version": 1, "group": "copy", "mode": "snapshot",
                                      "tags": [], "spec": "specs/copy-basic.json"}, "repeats": 1}
                        ]} for index, case_id in enumerate(ids)]}
            manifest["manifest_sha256"] = catalog.digest(manifest)
            runner.write_json(root / "manifest.json", manifest)
            expired = root / "performance-batch-1-2"
            expired.mkdir()
            (expired / "expired.txt").touch()

            result = actions_report.collect(root, root, attempt, {"measure": "failure"},
                                            jobs=actions_report.jobs_for_report(jobs_path))
            self.assertEqual([
                {"batch": 0, "reason": "batch failed before writing a summary"},
                {"batch": 1, "reason": "batch evidence upload failed"},
                {"batch": 2, "reason": "batch expired before measurement"},
                {"batch": 3, "reason": "batch setup failed (Run actions/setup-java@v4)"},
            ], result["errors"])
            self.assertEqual({"missing": 4}, result["repeat_counts"])

    def test_report_cli_uses_only_the_selected_published_baseline(self):
        environment = "runner"
        good, bad, mismatched = "copy.snapshot.basic.standard", "union.snapshot.basic.standard", "copy.snapshot.wide"
        cases = []
        rows = []
        for case_id in (good, bad, mismatched):
            case = {"id": case_id, "case_version": 1, "group": "copy" if case_id != bad else "union",
                    "mode": "snapshot", "tags": [], "definition_sha256": case_id, "spec": "specs/copy-basic.json",
                    "profile": "standard", "description": f"Scenario {case_id}", "workload_spec": {"profile": {"fields": 5}}}
            cases.append({"case": case, "repeats": 2, "rows": 40})
            for number in (1, 2):
                rows.append({
                    "case_id": case_id, "case_version": 1, "repeat": number, "status": "completed",
                    "run_id": "fixture", "code_sha": "candidate", "definition_sha256": case_id,
                    "mode": "snapshot", "scale": "formal", "connector_sha256": "connector",
                    "effective_load": {"rows": 40}, "environment": {"cpu_count": 4}, "environment_profile": environment,
                    "effective_input_rows": {"left": 20, "right": 20},
                    "source_data": {"tables": {"left": {"rows": 20, "fields": 5}, "right": {"rows": 19, "fields": 7}}},
                    "confirmed_output_units": 38, "target_readback_rows": 39, "mongo_shared_instance_resident_mib": 512,
                    "resources": {"status": "observed", "scope": "OP process", "op_heap_limit_mib": 256,
                                  "windows": {"snapshot": {"covered": True, "source": "procfs", "rss_peak_kib": 126}}},
                })
        readback_failure = "different target keys\nExpected " + "x" * 4000
        rows[2].update(status="failed", last_stage="readback", reason=readback_failure)
        for field in ("source_data", "effective_input_rows", "confirmed_output_units", "target_readback_rows", "resources"):
            rows[3].pop(field)
        rows[3].update(status="failed", last_stage="prepare", reason="fixture failed early")
        reference = {row["case_id"]: copy.deepcopy(row) for row in (rows[0], rows[4])}
        reference[good]["resources"]["windows"]["snapshot"]["rss_peak_kib"] = 100
        reference[mismatched]["effective_load"]["rows"] = 80
        for row in reference.values():
            row["code_sha"] = "d" * 40
        metric = "snapshot.rss_peak_kib"
        policy = {"version": 1, "cases": {good: {
            metric: {"worse": "higher", "observe_ratio": 0.1, "suspect_ratio": 0.2,
                     "minimum_absolute": 2, "baseline_noise": 5, "minimum_repeats": 2}
        }}}
        content = {"kind": "accepted", "environment": environment, "cases": reference,
                   "code_sha": "d" * 40, "comparison_rules": policy}
        versions = {}
        bodies = {}
        for version, revision in (("v1", "b" * 40), ("v2", "c" * 40)):
            body = json.dumps(dict(content, id=f"baseline-{version}", version=version)).encode()
            bodies[revision] = body
            versions[version] = {"id": f"baseline-{version}", "commit": revision, "code_sha": content["code_sha"],
                                 "sha256": hashlib.sha256(body).hexdigest(), "environment": environment,
                                 "path": "baselines/case-results.json"}

        release_sha = "a" * 40
        record = None
        requested = []

        def fetch(url):
            parts = url.split("/")
            requested.append(url)
            if url.endswith("/commits/main"):
                return json.dumps({"sha": release_sha}).encode()
            if url.endswith("/releases.json"):
                if record is None:
                    raise HTTPError(url, 404, "not published", None, None)
                self.assertEqual(release_sha, parts[-3])
                return json.dumps(record).encode()
            return bodies[parts[-3]]

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            folder = root / "performance-batch-1-0"
            (folder / "run").mkdir(parents=True)
            (folder / "environment.txt").write_text("Image ubuntu24 version-1\nModel name: Fixture CPU\n")
            index = root / "artifacts.json"
            runner.write_json(index, {"artifacts": [
                {"workflow_run": {"id": 111}, "name": folder.name, "id": 222, "digest": "sha256:abc", "expired": False},
                {"workflow_run": {"id": 112}, "name": folder.name, "id": 333, "expired": False},
            ]})
            attempt = {"run_id": 111, "attempt": 1}
            run_record = {"github_attempt": attempt, "scope": "ids"}
            assembly = {"identity": {"code_sha": "candidate"}, "environment": environment, "github_attempt": attempt,
                        "selected_ids": [good, bad, mismatched], "unallocated": [],
                        "batches": [{"index": 0, "entries": cases}]}
            assembly["manifest_sha256"] = catalog.digest(assembly)
            runner.write_json(root / "manifest.json", assembly)
            runner.write_json(folder / "run" / "summary.json", {
                "batch": 0, "run_id": "fixture", "process_exit": 1, "github_attempt": attempt,
                "manifest_sha256": assembly["manifest_sha256"], "identity": assembly["identity"], "results": rows,
            })

            def publish_report(label, receipt):
                runner.write_json(root / "baseline-receipt.json", receipt)
                runner.write_json(root / "run.json", dict(run_record, baseline_receipt_sha256=catalog.digest(receipt)))
                output = root / label
                step = output / "step-summary.md"
                arguments = ["actions_report.py", "--inputs", str(root), "--artifacts", str(root),
                             "--output", str(output), "--artifact-index", str(index)]
                with patch.object(sys, "argv", arguments), patch.dict(os.environ, {
                    "GITHUB_RUN_ID": "111", "GITHUB_RUN_ATTEMPT": "1", "GITHUB_REPOSITORY": "example/repo",
                    "GITHUB_STEP_SUMMARY": str(step), "PERFORMANCE_JOB_RESULTS": '{"measure":"failure"}',
                }):
                    self.assertEqual(1, actions_report.main())
                data = json.loads((output / "report.json").read_text())
                markdown = step.read_text()
                self.assertEqual(markdown, (output / "report.md").read_text())
                self.assertEqual(attempt, data["github_attempt"])
                self.assertEqual({"completed": 2, "failed": 1}, data["case_counts"])
                self.assertEqual({"failed": 2, "completed": 4}, data["repeat_counts"])
                self.assertEqual(readback_failure, data["results"][2]["reason"])
                self.assertIn("different target keys", markdown)
                self.assertNotIn("x" * 300, markdown)
                self.assertIn(f"{bad}@1", markdown)
                self.assertIn("| single | right | 20 | 19 | 7 |", markdown)
                self.assertIn("| single | 38 | 39 |", markdown)
                self.assertIn("| single | unavailable | unavailable | unavailable | unavailable |", markdown)
                self.assertIn("Fixture CPU", markdown)
                self.assertIn("/runs/111/artifacts/222", markdown)
                self.assertEqual("sha256:abc", data["batch_evidence"]["0"]["digest"])
                return data, markdown

            receipt = baseline.pin(root, environment, fetch=fetch)
            unavailable, markdown = publish_report("none", receipt)
            self.assertEqual("baseline-not-published", unavailable["baseline"]["reason"])
            self.assertIn("Comparison unavailable (baseline-not-published)", markdown)

            record = {"schema_version": 1, "active": {environment: "v1"}, "versions": {"v1": versions["v1"]}}
            # The v2 bytes already exist, but the publication record still points to the verified v1.
            pinned = baseline.pin(root, environment, fetch=fetch)
            self.assertEqual("v1", pinned["version"])
            self.assertFalse(any("/" + "c" * 40 + "/" in url for url in requested))
            release_sha = "e" * 40
            record = {"schema_version": 1, "active": {environment: "v2"}, "versions": versions}
            other_dir = root / "next-inputs"
            other_dir.mkdir()
            next_receipt = baseline.pin(other_dir, environment, fetch=fetch)
            self.assertEqual("v2", next_receipt["version"])
            self.assertEqual(release_sha, next_receipt["release_record_commit"])
            self.assertEqual("v1", baseline.pin(other_dir, environment, "v1", fetch=fetch)["version"])
            data, markdown = publish_report("v1", pinned)
            self.assertEqual("a" * 40, data["baseline"]["release_record_commit"])
            self.assertEqual(pinned["sha256"], data["baseline"]["sha256"])
            self.assertIn(pinned["sha256"], markdown)
            self.assertIn("Baseline version v1", markdown)
            self.assertEqual({"comparable": 1, "incomparable": 1, "unavailable": 1}, data["comparison_coverage"])
            self.assertEqual("suspected-regression", data["case_verdicts"][good]["comparison"])
            self.assertEqual("incompatible", data["results"][4]["comparisons"]["status"])
            self.assertIn(f"| {good}@1 | single | 100 | 126.0 | +26.0%", markdown)
            self.assertEqual("performance-report-1", data["report_artifact_name"])

            baseline_file = root / "baseline.json"
            invalid_documents = {
                "baseline-unreadable": None, "baseline-invalid-json": b"{", "invalid-baseline-schema": b"{}",
                "baseline-identity-mismatch": bodies["c" * 40], "baseline-digest-mismatch": bodies["c" * 40],
            }
            for expected, body in invalid_documents.items():
                with self.subTest(expected=expected):
                    receipt = dict(pinned)
                    if body is None:
                        baseline_file.unlink()
                    else:
                        baseline_file.write_bytes(body)
                        if expected != "baseline-digest-mismatch":
                            receipt["sha256"] = hashlib.sha256(body).hexdigest()
                    data, markdown = publish_report(expected, receipt)
                    self.assertEqual(expected, data["baseline"]["reason"])
                    self.assertIn(f"Comparison unavailable ({expected})", markdown)
                    self.assertEqual({"unavailable": 3}, data["comparison_counts"])
                    self.assertFalse(any(case["metrics"] for case in data["case_verdicts"].values()))
                    self.assertNotIn("+26.0%", markdown)

    def test_independent_health_distinguishes_a_queued_handoff_from_a_missing_run(self):
        now = datetime.fromisoformat("2026-11-21T15:00:00+00:00")
        nightly = {"id": 20, "event": "schedule", "run_attempt": 1, "status": "completed",
                   "created_at": "2026-11-21T10:00:00Z", "updated_at": "2026-11-21T14:00:00Z"}
        queued = {"id": 21, "event": "workflow_run", "run_attempt": 1, "status": "queued",
                  "display_title": "Pipeline Performance | Nightly 20/1", "created_at": "2026-11-21T14:00:01Z"}
        cases = [
            ([], [], "awaiting-nightly"),
            ([dict(nightly, status="in_progress")], [], "nightly-running"),
            ([nightly], [], "missing-handoff"),
            ([nightly], [dict(queued, run_attempt=2)], "missing-handoff"),
            ([nightly], [queued], "performance-queued"),
            ([nightly], [dict(queued, status="completed", run_started_at="2026-11-21T14:00:01Z")], "report-missing"),
        ]
        for nightlies, jobs, status in cases:
            with self.subTest(status=status):
                self.assertEqual(status, actions_health.assess(now, nightlies, jobs, {})["status"])

    def test_cancellation_uses_reporting_reserve_only_for_unfinished_preparation_or_batches(self):
        performance = {"id": 21, "run_started_at": "2026-11-21T10:00:00Z"}
        deadline = actions_health.workflow_deadline(performance)
        waiting = {"name": "measure (0)", "status": "queued"}
        reporting = {"name": "report", "status": "in_progress"}
        for seconds_before, live, jobs, expected_calls in (
            (1501, {"run_attempt": 1, "status": "in_progress"}, [waiting], 0),
            (1500, {"run_attempt": 1, "status": "in_progress"}, [waiting], 1),
            (1500, {"run_attempt": 2, "status": "in_progress"}, [waiting], 0),
            (1500, {"run_attempt": 1, "status": "completed"}, [waiting], 0),
            (1500, {"run_attempt": 1, "status": "in_progress"}, [reporting], 0),
            (600, {"run_attempt": 1, "status": "in_progress"}, [waiting], 1),
            (0, {"run_attempt": 1, "status": "in_progress"}, [reporting], 1),
        ):
            with self.subTest(seconds_before=seconds_before, live=live, jobs=jobs), \
                    patch.object(actions_watch, "api",
                                 side_effect=lambda _, path: {"jobs": jobs} if "/jobs?" in path else live), \
                    patch.object(actions_watch.subprocess, "run") as execute:
                actions_watch.stop_expiring_run("example/repo", performance,
                                               deadline - timedelta(seconds=seconds_before))
                self.assertEqual(expected_calls, execute.call_count)
                if expected_calls:
                    execute.assert_called_once_with(
                        ["gh", "api", "--method", "POST", "repos/example/repo/actions/runs/21/cancel"],
                        check=True, stderr=subprocess.PIPE, timeout=30)

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

    def test_http_rate_limit_leaves_the_intent_retryable(self):
        now = datetime.fromisoformat("2026-11-21T15:00:00+00:00")
        source = {"id": 10}
        intent = {"id": 11, "created_at": now.isoformat()}
        proposed = {"updated_at": now.isoformat(), "active": {"workflow": {"reason": "missing-handoff"}}}
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory)
            runner.write_json(output / "card.json", {"msg_type": "interactive"})
            runner.write_json(output / "next-state.json", proposed)
            rejected = HTTPError("https://example.invalid", 429, "Too Many Requests", {}, BytesIO())
            with patch.object(actions_watch, "urlopen", side_effect=rejected):
                with self.assertRaises(ValueError):
                    actions_watch.send(output, "https://example.invalid")
            receipt = catalog.read_json(output / "delivery.json")
            with patch.object(actions_watch, "latest_artifact", side_effect=[None, (intent, source)]), \
                    patch.object(actions_watch, "artifact", side_effect=[proposed, receipt]):
                actual = actions_watch.latest_state("example/repo", "main", now)
            self.assertEqual({}, actual["active"])
            self.assertFalse((output / "state.json").exists())
            with patch.object(actions_watch, "urlopen", return_value=BytesIO(b'{"code":0}')):
                actions_watch.send(output, "https://example.invalid")
            self.assertEqual(proposed, catalog.read_json(output / "state.json"))

    def test_delivery_intent_prevents_a_duplicate_when_state_artifact_upload_is_lost(self):
        now = datetime.fromisoformat("2026-11-21T15:00:00+00:00")
        delivery_intent = {"workflow_run": {"id": 10}, "id": 11, "created_at": now.isoformat()}
        draft_state = {"updated_at": now.isoformat(), "active": {"workflow": {"reason": "missing-handoff"}}}
        source = {"id": 10}
        for status, digest, pending in (
            ("accepted", catalog.digest(draft_state), {}),
            ("rejected", catalog.digest(draft_state), {}),
            ("unknown", catalog.digest(draft_state), {"workflow": "alert"}),
            ("accepted", "wrong", {"workflow": "alert"}),
        ):
            with self.subTest(status=status, digest=digest), \
                    patch.object(actions_watch, "latest_artifact", side_effect=[None, (delivery_intent, source)]), \
                    patch.object(actions_watch, "artifact", side_effect=[
                        draft_state, {"status": status, "intent_sha256": digest},
                    ]):
                actual = actions_watch.latest_state("example/repo", "main", now)
                self.assertEqual(pending, actual.get("pending", {}))
                accepted = status == "accepted" and digest == catalog.digest(draft_state)
                self.assertEqual(draft_state["active"] if accepted else {}, actual["active"])

    def test_conflicting_cancellation_does_not_prevent_a_health_card(self):
        now = datetime.fromisoformat("2026-11-23T15:00:00+00:00")
        run = {"id": 21, "run_attempt": 1, "status": "in_progress", "event": "workflow_run",
               "created_at": "2026-11-22T11:00:01Z", "run_started_at": "2026-11-22T11:00:02Z",
               "display_title": "Pipeline Performance | Nightly 20/1"}
        nightly = {"id": 20, "event": "schedule", "run_attempt": 1, "status": "completed",
                   "created_at": "2026-11-22T10:00:00Z", "updated_at": "2026-11-22T11:00:00Z"}
        error = subprocess.CalledProcessError(1, "gh", stderr=b"gh: conflict (HTTP 409)\n")
        for status in ("in_progress", "completed"):
            with self.subTest(status=status), tempfile.TemporaryDirectory() as d:
                output = Path(d) / "health"
                with patch.object(actions_watch, "api", side_effect=[
                    {"workflow_runs": [nightly]}, {"workflow_runs": [run]}, run, dict(run, status=status),
                ]) as api, patch.object(actions_watch.subprocess, "run", side_effect=error), \
                        patch.object(actions_watch, "latest_state",
                                     return_value={"active": {}, "updated_at": now.isoformat()}):
                    actions_watch.prepare(output, "example/repo", "main", now=now)
                self.assertEqual(4, api.call_count)
                health = catalog.read_json(output / "health.json")
                self.assertEqual(bool(health["cancellation_errors"]), status == "in_progress")
                self.assertTrue((output / "card.json").is_file())

    def test_lost_committed_upload_cannot_undo_an_accepted_alert(self):
        now = datetime.fromisoformat("2026-11-23T15:00:00+00:00")
        initial = {"updated_at": now.isoformat(), "active": {}}
        confirmed = {"updated_at": now.isoformat(),
                     "active": {"workflow": {"kind": "workflow", "reason": "missing-handoff"}}}
        proposal = {"updated_at": now.isoformat(), "previous": confirmed, "pending": {},
                    "active": {**confirmed["active"], "case": {"kind": "execution", "reason": "read back failed"}}}
        for status, pending, active in (
            ("rejected", {}, confirmed["active"]),
            ("unknown", {"case": "alert"}, confirmed["active"]),
            ("accepted", {}, proposal["active"]),
        ):
            with self.subTest(status=status), patch.object(actions_watch, "latest_artifact", side_effect=[
                ({"id": 8, "created_at": "2026-11-23T10:00:00Z"}, {"id": 7}),
                ({"id": 10, "created_at": "2026-11-23T11:00:00Z"}, {"id": 9}),
            ]), patch.object(actions_watch, "artifact", side_effect=[
                initial, proposal, {"status": status, "intent_sha256": catalog.digest(proposal)},
            ]):
                loaded = actions_watch.latest_state("example/repo", "main", now)
                self.assertEqual(active, loaded["active"])
                self.assertEqual(pending, loaded["pending"])
                self.assertNotIn("previous", loaded)
