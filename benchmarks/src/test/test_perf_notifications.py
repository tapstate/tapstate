import copy
from datetime import datetime, timedelta, timezone
from hashlib import sha256
from io import BytesIO
import json
from pathlib import Path
import sys
import subprocess
import tempfile
import unittest
from unittest.mock import patch
from zipfile import ZipFile

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
import actions_health
import actions_report
import actions_run
import actions_watch
import catalog
import notifications
import runner


REPOSITORY = "example/repo"
ENVIRONMENT = "fixture-env"
CASE_IDS = [
    "copy.snapshot.dummy-baseline.standard", "copy.snapshot.basic.standard", "map.live.basic.standard",
    "nest.snapshot.basic.standard", "copy.snapshot.basic.wide", "copy.snapshot.basic.small",
    "filter.live.basic.standard", "union.snapshot.basic.standard", "copy.live.basic.standard",
    "copy.backlog.fixed-iud.standard", "copy.live.handoff.standard",
]
METRIC = "completion_nanos"
RULE = {"worse": "higher", "observe_ratio": .1, "suspect_ratio": .2,
        "minimum_absolute": 12, "baseline_noise": 24, "minimum_repeats": 2}


def scheduled(day):
    created = datetime(2026, 11, day, 19, tzinfo=timezone.utc)
    nightly = {"id": day * 10, "event": "schedule", "status": "completed", "run_attempt": 1,
               "created_at": created.isoformat(), "updated_at": (created + timedelta(hours=1)).isoformat()}
    started = created + timedelta(hours=1, seconds=90)
    performance = {"id": day * 10 + 1, "event": "workflow_run", "status": "completed", "run_attempt": 1,
                   "display_title": f"Pipeline Performance | Nightly {nightly['id']}/1", "head_sha": "candidate",
                   "created_at": (started - timedelta(seconds=30)).isoformat(), "run_started_at": started.isoformat(),
                   "updated_at": (started + timedelta(minutes=42)).isoformat()}
    return nightly, performance, started + timedelta(minutes=45)


def raw_results(folder, nightly, performance, samples, cases, *, failures=False, missing=False,
                available=True, cpu_count=4, reference=100, baseline_memory=None, observed_memory=None):
    attempt = {"run_id": performance["id"], "attempt": 1}
    upstream = {**nightly, "html_url": f"https://github.com/{REPOSITORY}/actions/runs/{nightly['id']}"}
    run = {"github_attempt": attempt, "scope": "all", "timing": actions_run.run_timing(performance, upstream)}
    folder.mkdir(parents=True, exist_ok=True)
    runner.write_json(folder / "run.json", run)
    manifest = {"identity": {"code_sha": performance["head_sha"]}, "environment": ENVIRONMENT,
                "github_attempt": attempt, "selected_ids": list(samples), "unallocated": [], "batches": []}
    entries, rows, baselines = [], [], {}
    rules = {}
    limits = {}
    for index, (case_id, values) in enumerate(samples.items()):
        case = cases[case_id]
        entries.append({"case": case, "repeats": 2, "rows": 40})
        first = {"case_id": case_id, "case_version": case["case_version"], "definition_sha256": case["definition_sha256"],
                 "run_id": "fixture", "status": "completed", "code_sha": "candidate", "connector_sha256": "fixed",
                 "mode": case["mode"], "scale": "formal", "environment_profile": ENVIRONMENT,
                 "environment": {"cpu_count": cpu_count}, "effective_input_rows": {"records": 40},
                 "effective_load": {"rows": 40}, "measurement_status": "eligible",
                 "phase_nanos": {"completion_observed": reference},
                 "resources": {"status": "observed", "windows": {
                     "snapshot": {"covered": True, "source": "procfs", "rss_peak_kib": 80},
                 }}}
        baselines[case_id] = copy.deepcopy(first)
        rules[case_id] = {METRIC: RULE}
        if baseline_memory:
            baselines[case_id].update(baseline_memory)
            rules[case_id].update({metric: RULE for metric in (
                "steady.rss_growth_kib_per_second", "lifecycle.idle_rss_retained_kib",
            )})
        if values == "incompatible":
            baselines[case_id]["effective_load"]["rows"] = 60
        for repeat, value in enumerate(values if isinstance(values, tuple) else (100, 100), 1):
            row = {**copy.deepcopy(first), "repeat": repeat}
            row["phase_nanos"]["completion_observed"] = value
            if observed_memory:
                row.update(observed_memory)
            if values == "insufficient" and repeat == 2:
                row["measurement_status"] = "bounded-observation"
            if failures and index == 0:
                row.update(status="failed", last_stage="load", reason="different target keys\n" + "x" * 3000)
            if failures and index == 1:
                row.update(status="failed", last_stage="process", exit_attribution="observed-java-out-of-memory")
            if failures and index == 2:
                row.update(status="timed-out", last_stage="measure", exit_attribution="observed-op-oom-kill")
            if failures and index == 3:
                limits[case_id] = {"snapshot.rss_peak_kib": {"source": "procfs", "limit_kib": 100}}
                row["resources"]["windows"]["snapshot"]["rss_peak_kib"] = 131
            rows.append(row)
    if missing:
        case_id = "copy.snapshot.missing.standard"
        manifest["selected_ids"].append(case_id)
        manifest["batches"].append({"index": 1, "entries": [
            {"case": {**entries[0]["case"], "id": case_id}, "repeats": 2, "rows": 40},
        ]})
    manifest["batches"].insert(0, {"index": 0, "entries": entries})
    manifest["manifest_sha256"] = catalog.digest(manifest)
    runner.write_json(folder / "manifest.json", manifest)
    batch = folder / "performance-batch-1-0" / "run"
    batch.mkdir(parents=True)
    runner.write_json(batch / "summary.json", {
        "batch": 0, "run_id": "fixture", "process_exit": 0, "github_attempt": attempt,
        "manifest_sha256": manifest["manifest_sha256"], "identity": manifest["identity"], "results": rows,
    })
    baseline = {"id": "fixture-only", "kind": "fixture", "environment": ENVIRONMENT, "cases": baselines,
                "comparison_rules": {"version": 1, "cases": rules}, "memory_limits": limits}
    with patch.object(actions_report, "read_baseline", return_value=(
        baseline if available else None, {"status": "available" if available else "unavailable"},
    )):
        return actions_report.collect(folder, folder, attempt, {"prepare": "success"})


def memory_observation(increment, idle_seconds):
    windows = {}
    cycles = []
    for index in (1, 2, 3):
        window = {"covered": True, "source": "procfs", "rss_median_kib": 100 + increment * index}
        windows[f"steady-{index}"] = {**window, "started_epoch_ms": 1000 * index}
        windows[f"lifecycle-cycle-{index}/idle"] = window
        cycles.append({"cycle": index, "idle_seconds": idle_seconds, "state": "STOPPED", "target_readback": "verified"})
    return {"resources": {"status": "observed", "windows": windows}, "lifecycle_cycles": cycles}


class NotificationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.cases = {case["id"]: case for case in catalog.build_catalog()["cases"]}

    def prepare(self, folder, report, nightly, performance, now, previous):
        def query(_, path):
            if path.startswith("workflows/nightly"):
                return {"workflow_runs": [nightly]}
            if path.startswith("workflows/pipeline-performance"):
                return {"workflow_runs": [performance]}
            self.fail(f"Unexpected Actions request: {path}")

        with patch.object(actions_watch, "api", side_effect=query), \
                patch.object(actions_watch, "artifact", return_value=report) as fetch, \
                patch.object(actions_watch, "latest_state", return_value=previous):
            actions_watch.prepare(folder, REPOSITORY, "main", now=now)
        fetch.assert_called_once_with(REPOSITORY, performance, "performance-report-1", "report.json")

    def test_mixed_report_and_card_reconcile_to_the_same_attempt(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            nightly, performance, now = scheduled(21)
            samples = dict(zip(CASE_IDS, [(100, 100)] * 4 + [(130, 130)] * 4
                               + [(99, 100), (115, 118), "incompatible"]))
            report = raw_results(base / "report-inputs", nightly, performance, samples, self.cases, failures=True, missing=True)
            output = base / "health"
            artifact = base / "report.json"
            runner.write_json(artifact, report)
            self.prepare(output, catalog.read_json(artifact), nightly, performance, now,
                         {"active": {}, "updated_at": now.isoformat()})
            body = catalog.read_json(output / "card.json")
            self.assertEqual(notifications.CARD_LABEL, body["card"]["header"]["title"]["content"])
            markdown, action = body["card"]["elements"]
            text = markdown["content"]
            summary = actions_report.summary(report, REPOSITORY)
            self.assertEqual({"completed": 7, "failed": 4, "not-run": 1}, report["case_counts"])
            self.assertEqual({"suspected-regression": 4, "normal": 1, "observe": 1, "unavailable": 6},
                             report["comparison_counts"])
            self.assertEqual({"comparable": 6, "incomparable": 1, "unavailable": 5}, report["comparison_coverage"])
            for expected in ("2026-11-22 (UTC+8)", "Elapsed 42m",
                             "Planned 12 | Completed 7 | Failed 4 | Not run 1",
                             "Comparable 6 | Suspected regressions 4 | Incomparable 1 | Unavailable 5",
                             "batch summary missing or unreadable", f"{CASE_IDS[0]}@1 (basic)",
                             "load: different target keys", f"{CASE_IDS[4]}@2 (operators, memory)",
                             "100 → 130 ns (+30.0%)"):
                self.assertIn(expected, text)
            self.assertEqual("https://github.com/example/repo/actions/runs/211/attempts/1",
                             action["actions"][0]["url"])
            self.assertEqual(1, text.count("**Execution failure**"))
            self.assertEqual(1, text.count("**Suspected regression**"))
            self.assertNotIn(CASE_IDS[1], text)
            self.assertNotIn(CASE_IDS[5], text)
            self.assertLess(len(text), 650)
            self.assertIn("observed-java-out-of-memory", summary)
            self.assertIn("observed-op-oom-kill", summary)
            self.assertIn("memory-budget-exceeded", summary)
            failures = [key for key, item in report["case_verdicts"].items() if item["execution"] == "failed"]
            regressions = [key for key, item in report["case_verdicts"].items()
                           if item["comparison"] == "suspected-regression"]
            self.assertEqual(CASE_IDS[:4], failures)
            self.assertEqual(CASE_IDS[4:8], regressions)

            unavailable = raw_results(base / "unavailable", nightly, performance, samples, self.cases,
                                      failures=True, available=False)
            self.prepare(base / "no-comparison", unavailable, nightly, performance, now,
                         {"active": {}, "updated_at": now.isoformat()})
            content = catalog.read_json(base / "no-comparison" / "card.json")["card"]["elements"][0]["content"]
            self.assertIn("Comparison unavailable", content)
            self.assertNotIn("**Suspected regression**", content)

    def test_sequence_uses_receipts_for_retry_dedup_escalation_and_one_recovery(self):
        sequence = [
            ("normal", (105, 106), None), ("observe", (117, 118), None), ("improved", (82, 86), None),
            ("within-minimum", (122, 122), None), ("insufficient", "insufficient", None),
            ("initial-rejected", (132, 134), 1), ("initial-accepted", (132, 134), 0),
            ("unchanged", (132, 134), None), ("one-spike", (132, 190), None),
            ("worse", (171, 172), 0), ("incompatible", "incompatible", None),
            ("observe-again", (115, 118), None), ("recovered", (99, 100), 0),
            ("still-healthy", (99, 100), None),
        ]
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            state = {"active": {}, "updated_at": "2026-11-09T00:00:00+00:00"}
            for index, (label, sample, code) in enumerate(sequence):
                with self.subTest(label=label):
                    nightly, performance, now = scheduled(10 + index)
                    report = raw_results(base / label / "inputs", nightly, performance, {CASE_IDS[4]: sample}, self.cases)
                    output = base / label / "output"
                    self.prepare(output, report, nightly, performance, now, state)
                    self.assertEqual(code is not None, (output / "card.json").exists())
                    if code is None:
                        continue
                    content = catalog.read_json(output / "card.json")["card"]["elements"][0]["content"]
                    if label == "recovered":
                        self.assertIn("**Recovered** performance cases 1", content)
                    self.assertNotIn("<at", content)
                    proposed = catalog.read_json(output / "next-state.json")
                    self.assertEqual(state, proposed["previous"])
                    with patch.object(actions_watch, "urlopen", return_value=BytesIO(json.dumps({"code": code}).encode())):
                        if code == 1:
                            with self.assertRaisesRegex(ValueError, "did not accept"):
                                actions_watch.send(output, "https://example.invalid")
                            self.assertFalse((output / "state.json").exists())
                        else:
                            actions_watch.send(output, "https://example.invalid")
                            state = catalog.read_json(output / "state.json")
                            self.assertNotIn("previous", state)
                            self.assertEqual({k: v for k, v in proposed.items() if k != "previous"}, state)
                    receipt = catalog.read_json(output / "delivery.json")
                    self.assertEqual("accepted" if code == 0 else "rejected", receipt["status"])
                    self.assertEqual(catalog.digest(proposed), receipt["intent_sha256"])
            self.assertEqual({}, state["active"])

    def test_independent_observation_and_recovery_require_the_original_scheduled_attempt(self):
        nightly, run, now = scheduled(21)
        due = now.replace(hour=10, minute=0, second=0, microsecond=0)
        self.assertEqual("awaiting-nightly", actions_health.assess(due + timedelta(hours=14), [], [], {})["status"])
        self.assertEqual("missing-nightly", actions_health.assess(due + timedelta(hours=16), [], [], {})["status"])
        late = {**nightly, "created_at": (due + timedelta(hours=14, minutes=35)).isoformat(), "status": "in_progress"}
        self.assertEqual("nightly-running", actions_health.assess(due + timedelta(hours=15), [late], [], {})["status"])

        with tempfile.TemporaryDirectory() as directory:
            report = raw_results(Path(directory), nightly, run, {CASE_IDS[0]: (100, 100)}, self.cases)
        observations = [
            ([], {}, "missing-handoff"),
            ([{**run, "status": "queued"}], {}, "performance-queued"),
            ([{**run, "status": "in_progress"}], {}, "performance-running"),
            ([{**run, "status": "in_progress", "run_started_at": nightly["created_at"]}],
             {}, "performance-running"),
            ([run], {}, "report-missing"),
            ([run], {run["id"]: dict(report, github_attempt={"run_id": run["id"], "attempt": 2})}, "report-missing"),
            ([run], {run["id"]: dict(report, workflow=dict(report["workflow"], scope="ids"))}, "report-missing"),
            ([run], {run["id"]: report}, "reported"),
        ]
        state = {}
        outcomes = []
        for runs, reports, expected in observations:
            observed = actions_health.assess(now, [nightly], runs, reports)
            self.assertEqual(expected, observed["status"])
            notices, state = notifications.changes(state, *notifications.signals(observed))
            outcomes.extend((item["reason"], item.get("recovered", False)) for item in notices)
        self.assertEqual([("missing-handoff", False), ("report-missing", False), ("report-missing", True)], outcomes)
        repeated = actions_health.assess(now, [nightly], [run], {run["id"]: report})
        self.assertEqual(([], {}), notifications.changes(state, *notifications.signals(repeated)))
        expired = actions_health.assess(now + timedelta(hours=7), [nightly], [{**run, "status": "in_progress"}], {})
        self.assertEqual("performance-expired", expired["status"])

    def test_recovery_and_dedup_are_scoped_to_conditions_and_comparison_reference(self):
        nightly, run, _ = scheduled(21)
        case = CASE_IDS[4]
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            state = {}
            for index, (cpu_count, reference, values, expected) in enumerate((
                    (4, 100, (133, 134), [False]),
                    (8, 100, (99, 100), []),
                    (4, 150, (181, 184), [False]),
                    (4, 150, (149, 150), [True]),
                    (4, 100, (99, 100), [True]),
            )):
                report = raw_results(base / str(index), nightly, run, {case: values}, self.cases,
                                     cpu_count=cpu_count, reference=reference)
                notices, state = notifications.changes(state, *notifications.signals({
                    "status": "reported", "report": report,
                }))
                self.assertEqual(expected, [item.get("recovered", False) for item in notices])
            self.assertEqual({}, state)

    def test_memory_growth_and_retention_use_comparable_samples_and_are_regressions(self):
        nightly, run, _ = scheduled(21)
        with tempfile.TemporaryDirectory() as directory:
            for seconds, expected in (
                    (5, {"steady.rss_growth_kib_per_second", "lifecycle.idle_rss_retained_kib"}),
                    (10, {"steady.rss_growth_kib_per_second"}),
            ):
                with self.subTest(idle_seconds=seconds):
                    report = raw_results(Path(directory) / str(seconds), nightly, run,
                                         {CASE_IDS[4]: (100, 100)}, self.cases,
                                         baseline_memory=memory_observation(40, 5),
                                         observed_memory=memory_observation(70, seconds))
                    active, _ = notifications.signals({"status": "reported", "report": report})
                    self.assertEqual({"comparison"}, {item["kind"] for item in active.values()})
                    self.assertEqual(expected, {item["metric"]["metric"] for item in active.values()})
                    self.assertEqual({"completed": 1}, report["case_counts"])

    def test_original_attempt_is_monitored_after_the_next_schedule_slot(self):
        nightly, run, _ = scheduled(21)
        started = datetime.fromisoformat("2026-11-22T04:00:00+00:00")
        late = {**run, "status": "in_progress", "run_started_at": started.isoformat()}
        next_nightly, _, _ = scheduled(22)
        for hour, minute, expected in ((9, 59, "performance-running"), (10, 1, "performance-running"),
                                       (11, 1, "performance-expired")):
            now = started.replace(hour=hour, minute=minute)
            outcome = actions_health.assess(now, [nightly, next_nightly], [late], {})
            self.assertEqual(expected, outcome["status"])
            self.assertEqual(run["id"], outcome["performance"]["id"])
        late.update(status="completed", updated_at=now.isoformat())
        outcome = actions_health.assess(now, [nightly, next_nightly], [late], {})
        self.assertEqual("report-missing", outcome["status"])
        with tempfile.TemporaryDirectory() as directory:
            report = raw_results(Path(directory), nightly, late, {CASE_IDS[0]: (100, 100)}, self.cases)
        outcome = actions_health.assess(now, [nightly, next_nightly], [late], {late["id"]: report})
        self.assertEqual("performance-expired", outcome["status"])
        self.assertIs(report, outcome["report"])

    def test_overlapping_terminal_runs_reconcile_in_slot_order_through_prepare(self):
        old_nightly, old_run, _ = scheduled(21)
        old_run.update(created_at="2026-11-22T03:59:30Z", run_started_at="2026-11-22T04:00:00Z",
                       updated_at="2026-11-22T11:01:00Z")
        new_nightly, new_run, _ = scheduled(22)
        new_nightly.update(created_at="2026-11-22T10:03:00Z", updated_at="2026-11-22T10:10:00Z")
        new_run.update(created_at="2026-11-22T10:11:00Z", run_started_at="2026-11-22T10:11:00Z",
                       updated_at="2026-11-22T10:50:00Z")
        now = datetime.fromisoformat("2026-11-22T11:05:00+00:00")
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            reports = {
                old_run["id"]: raw_results(base / "old-report", old_nightly, old_run, {CASE_IDS[0]: (100, 100)},
                                           self.cases, missing=True),
                new_run["id"]: raw_results(base / "new-report", new_nightly, new_run, {CASE_IDS[0]: (100, 100)},
                                           self.cases),
            }
            state = {"active": {}, "updated_at": now.isoformat()}
            for index, (expected_run, send) in enumerate(((old_run, True), (new_run, True), (new_run, False))):
                output = base / f"observation-{index}"
                with patch.object(actions_watch, "api", side_effect=[
                    {"workflow_runs": [new_nightly, old_nightly]}, {"workflow_runs": [new_run, old_run]},
                ]), patch.object(actions_watch, "artifact", side_effect=lambda _, run, *args: reports[run["id"]]), \
                        patch.object(actions_watch, "latest_state", return_value=state):
                    actions_watch.prepare(output, REPOSITORY, "main", now=now)
                outcome = catalog.read_json(output / "health.json")
                self.assertEqual(expected_run["id"], outcome["performance_run_id"])
                self.assertEqual(["reported", "performance-expired"],
                                 [item["status"] for item in outcome["observations"]])
                self.assertEqual(send, (output / "card.json").exists())
                if send:
                    content = catalog.read_json(output / "card.json")["card"]["elements"][0]["content"]
                    self.assertIn("exceeded its deadline" if index == 0 else "**Recovered** Workflow", content)
                    with patch.object(actions_watch, "urlopen", return_value=BytesIO(b'{"code":0}')):
                        actions_watch.send(output, "https://example.invalid")
                    state = catalog.read_json(output / "state.json")
            self.assertEqual({}, state["active"])

    def test_unsettled_prior_slot_is_reconciled_after_the_schedule_boundary(self):
        nightly, run, _ = scheduled(21)
        run.update(created_at="2026-11-22T03:59:30Z", run_started_at="2026-11-22T04:00:00Z",
                   updated_at="2026-11-22T09:59:00Z")
        now = datetime.fromisoformat("2026-11-22T10:07:00+00:00")
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            report = raw_results(base / "report", nightly, run, {CASE_IDS[4]: (130, 131)}, self.cases)
            with patch.object(actions_watch, "api", side_effect=[
                {"workflow_runs": [nightly]}, {"workflow_runs": [run]},
            ]), patch.object(actions_watch, "artifact", return_value=report) as fetch, \
                    patch.object(actions_watch, "latest_state",
                                 return_value={"active": {}, "updated_at": now.isoformat()}):
                actions_watch.prepare(base / "health", REPOSITORY, "main", now=now)
            fetch.assert_called_once_with(REPOSITORY, run, "performance-report-1", "report.json")
            state = catalog.read_json(base / "health/next-state.json")
            self.assertEqual("reported", state["observation"]["status"])
            observed = actions_health.observations(now, [nightly], [run], {run["id"]: report})
            self.assertEqual("awaiting-nightly",
                             actions_health.select_observation(observed, state["observation"])["status"])

        nightly["updated_at"] = "2026-11-22T09:50:00Z"
        self.assertEqual("awaiting-handoff",
                         actions_health.assess(now - timedelta(minutes=10), [nightly], [], {})["status"])
        self.assertEqual("missing-handoff", actions_health.assess(now, [nightly], [], {})["status"])

    def test_later_attempt_with_removed_report_requires_evidence_of_first_upload(self):
        nightly, original, now = scheduled(21)
        later = {**original, "run_attempt": 3}
        upload = {"name": "Upload the report", "conclusion": "success"}
        jobs = [{"name": "report", "conclusion": "failure", "steps": [upload]}]
        active = {"workflow": {"kind": "workflow", "reason": "missing-handoff"}}
        report_endpoint = f"runs/{original['id']}/artifacts?name=performance-report-1&per_page=20"
        cases = [
            ([], jobs, "report-removed-after-rerun"),
            ([], [{**jobs[0], "steps": [{**upload, "conclusion": "failure"}]}], "report-missing"),
            ([{"name": "performance-report-1", "expired": True}], jobs, "report-missing"),
        ]
        with tempfile.TemporaryDirectory() as directory:
            for index, (artifacts, first_jobs, status) in enumerate(cases):
                with self.subTest(status=status, inventory=artifacts):
                    responses = {
                        f"runs/{original['id']}/attempts/1": original,
                        report_endpoint: {"artifacts": artifacts},
                        f"runs/{original['id']}/attempts/1/jobs?per_page=100": {"jobs": first_jobs},
                    }

                    def query(_, path):
                        if path.startswith("workflows/nightly"):
                            return {"workflow_runs": [nightly]}
                        if path.startswith("workflows/pipeline-performance"):
                            return {"workflow_runs": [later]}
                        return responses[path]

                    output = Path(directory) / str(index)
                    with patch.object(actions_watch, "api", side_effect=query), \
                            patch.object(actions_watch, "latest_state",
                                         return_value={"active": active, "updated_at": now.isoformat()}):
                        actions_watch.prepare(output, REPOSITORY, "main", now=now)
                    health = catalog.read_json(output / "health.json")
                    self.assertEqual(status, health["status"])
                    self.assertEqual(original["id"], health["performance_run_id"])
                    if status == "report-removed-after-rerun":
                        self.assertFalse((output / "card.json").exists())
                        state = catalog.read_json(output / "state.json")
                        self.assertEqual(active, state["active"])
                        self.assertEqual(status, state["observation"]["status"])
                    else:
                        card = catalog.read_json(output / "card.json")
                        self.assertIn("Report for this attempt missing", card["card"]["elements"][0]["content"])

    def test_archive_disappearance_rechecks_inventory_and_first_upload(self):
        nightly, original, now = scheduled(21)
        later = {**original, "run_attempt": 2}
        archive = {"id": 10, "name": "performance-report-1", "expired": False, "size_in_bytes": 10,
                   "workflow_run": {"id": original["id"], "head_sha": original["head_sha"]},
                   "created_at": original["run_started_at"]}
        first_jobs = [{"name": "report", "steps": [{"name": "Upload the report", "conclusion": "success"}]}]
        for status, rerun_listed, after_download, removed in (
            (410, True, [], True),
            (404, True, [], True),
            (404, False, [later], True),
            (404, False, [original], False),
            (404, True, [{"artifacts": [archive]}], False),
            (403, True, [], False),
        ):
            with self.subTest(status=status, rerun_listed=rerun_listed, removed=removed, latest=after_download), \
                    tempfile.TemporaryDirectory() as directory:
                output = Path(directory) / "health"
                responses = [{"workflow_runs": [nightly]}, {"workflow_runs": [later if rerun_listed else original]}]
                if rerun_listed:
                    responses.append(original)
                responses.extend([{"artifacts": [archive]}, *after_download])
                if removed:
                    responses.extend([{"artifacts": []}, {"jobs": first_jobs}])
                failure = subprocess.CalledProcessError(1, "gh", stderr=f"gh: failed (HTTP {status})\n".encode())
                with patch.object(actions_watch, "api", side_effect=responses) as query, \
                        patch.object(actions_watch.subprocess, "check_output", side_effect=failure), \
                        patch.object(actions_watch, "latest_state",
                                     return_value={"active": {}, "updated_at": now.isoformat()}):
                    if removed:
                        actions_watch.prepare(output, REPOSITORY, "main", now=now)
                        health = catalog.read_json(output / "health.json")
                        self.assertEqual("report-removed-after-rerun", health["status"])
                        self.assertFalse((output / "card.json").exists())
                    else:
                        with self.assertRaises(subprocess.CalledProcessError):
                            actions_watch.prepare(output, REPOSITORY, "main", now=now)
                    self.assertEqual(len(responses), query.call_count)

    def test_uncertain_alert_allows_an_independent_alert_and_clears_on_healthy_evidence(self):
        nightly, run, now = scheduled(21)
        initial = {"active": {}, "updated_at": now.isoformat()}
        attempted = {"active": {"workflow": {"kind": "workflow", "reason": "missing-handoff"}},
                     "updated_at": now.isoformat()}
        unknown = {"status": "unknown", "intent_sha256": catalog.digest(attempted)}
        with patch.object(actions_watch, "latest_artifact", side_effect=[None, ({"id": 8}, {"id": 7})]), \
                patch.object(actions_watch, "artifact", side_effect=[attempted, unknown]):
            pending = actions_watch.latest_state(REPOSITORY, "main", now)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for index, missing in enumerate((True, False)):
                result = raw_results(root / f"report-{index}", nightly, run,
                                     {CASE_IDS[4]: (130, 132)}, self.cases, missing=missing)
                output = root / f"health-{index}"
                with patch.object(actions_watch, "api", side_effect=[
                    {"workflow_runs": [nightly]}, {"workflow_runs": [run]},
                ]), patch.object(actions_watch, "artifact", return_value=result), \
                        patch.object(actions_watch, "latest_state", return_value=pending):
                    actions_watch.prepare(output, REPOSITORY, "main", now=now)
                proposed = catalog.read_json(output / "next-state.json")
                keys = proposed["active"]
                self.assertEqual(1, len(keys))
                self.assertTrue(next(iter(keys)).startswith("comparison:"))
                self.assertEqual({"workflow": "alert"} if missing else {}, proposed["pending"])

                if missing:
                    receipt = {"status": "rejected", "intent_sha256": catalog.digest(proposed)}
                    with patch.object(actions_watch, "latest_artifact", side_effect=[
                        ({"id": 5, "created_at": "2026-11-21T11:00:00Z"}, {"id": 4}),
                        ({"id": 10, "created_at": "2026-11-21T12:00:00Z"}, {"id": 9}),
                    ]), patch.object(actions_watch, "artifact", side_effect=[initial, proposed, receipt]):
                        pending = actions_watch.latest_state(REPOSITORY, "main", now)
                    self.assertEqual({"workflow": "alert"}, pending["pending"])
                    self.assertEqual({}, pending["active"])

    def test_artifact_bytes_are_bound_to_original_attempt_and_verified_digest(self):
        _, run, _ = scheduled(21)
        buffer = BytesIO()
        with ZipFile(buffer, "w") as zipped:
            zipped.writestr("report.json", '{"fixture":true}')
            zipped.writestr("unused.txt", "ignored")
        body = buffer.getvalue()
        name = "performance-report-1"
        item = {"id": 10, "name": name, "expired": False, "workflow_run": {"id": run["id"], "head_sha": run["head_sha"]},
                "size_in_bytes": len(body), "digest": f"sha256:{sha256(body).hexdigest()}",
                "created_at": run["run_started_at"]}
        later = {**item, "id": 12, "created_at": "2026-11-22T20:00:00Z"}
        with patch.object(actions_watch, "api", return_value={"artifacts": [item, later]}), \
                patch.object(actions_watch.subprocess, "check_output", return_value=body) as download:
            self.assertEqual({"fixture": True}, actions_watch.artifact(REPOSITORY, run, name, "report.json"))
            download.assert_called_once_with(["gh", "api", "repos/example/repo/actions/artifacts/10/zip"],
                                             stderr=subprocess.PIPE, timeout=60)
        with patch.object(actions_watch, "api", return_value={"artifacts": [item]}), \
                patch.object(actions_watch.subprocess, "check_output", return_value=body + b"changed"):
            self.assertEqual({}, actions_watch.artifact(REPOSITORY, run, name, "report.json"))
