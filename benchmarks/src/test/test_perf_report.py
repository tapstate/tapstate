import copy
import hashlib
import json
from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
import baseline
import catalog
import report


class PerfReportTest(unittest.TestCase):
    def setUp(self):
        identity = {"code_sha": "candidate"}
        case = {"id": "copy.snapshot.basic.wide", "case_version": 1, "definition_sha256": "definition"}
        self.manifest = {
            "identity": identity, "environment": "fixture-env", "unallocated": [],
            "selected_ids": [case["id"]],
            "batches": [{"index": 0, "entries": [{"case": case, "repeats": 2, "rows": 32}]}],
        }
        self.manifest["manifest_sha256"] = catalog.digest(self.manifest)
        self.row = {
            "case_id": case["id"], "repeat": 1, "run_id": "run", "status": "completed",
            "case_version": 1, "code_sha": "candidate", "definition_sha256": "definition",
            "mode": "snapshot", "scale": "qualification", "effective_load": {"rows": 32},
            "effective_input_rows": {"records": 32}, "connector_sha256": "lock",
            "environment": {"os": "example"}, "environment_profile": "fixture-env",
            "measurement_status": "bounded-observation",
            "phase_nanos": {"completion_observed": 10},
            "resources": {"status": "observed", "windows": {
                "snapshot": {"source": "fixture", "covered": True, "rss_peak_kib": 120},
                "idle": {"source": "fixture", "covered": False, "rss_peak_kib": 130},
            }},
        }
        self.summary = {"identity": identity, "manifest_sha256": self.manifest["manifest_sha256"],
                        "batch": 0, "run_id": "run", "process_exit": 0, "results": [self.row]}
        self.baseline = {"id": "sample-rss", "kind": "fixture", "environment": "fixture-env",
                         "cases": {case["id"]: copy.deepcopy(self.row)}}
        self.baseline["cases"][case["id"]]["resources"]["windows"]["snapshot"]["rss_peak_kib"] = 100

    def test_reports_missing_repeat_and_only_eligible_resource_observations(self):
        first, second = report.reconcile(self.manifest, [self.summary], self.baseline)["results"]
        self.assertEqual("missing", second["status"])
        comparisons = first["comparisons"]
        self.assertEqual("fixture-comparison", comparisons["status"])
        self.assertEqual({"snapshot.rss_peak_kib"}, set(comparisons["arms"]["single"]))
        self.assertAlmostEqual(0.20, comparisons["arms"]["single"]["snapshot.rss_peak_kib"]["change_ratio"])

    def test_duplicate_repeat_invalidates_observation(self):
        result = report.reconcile(self.manifest, [self.summary, self.summary], self.baseline)
        self.assertEqual("invalid", result["results"][0]["status"])
        self.assertEqual("result-unavailable", result["results"][0]["comparisons"]["status"])

    def test_matching_manifest_digest_does_not_admit_another_github_attempt(self):
        current = {"run_id": 480, "attempt": 1}
        self.manifest["github_attempt"] = current
        del self.manifest["manifest_sha256"]
        self.manifest["manifest_sha256"] = catalog.digest(self.manifest)
        self.summary["manifest_sha256"] = self.manifest["manifest_sha256"]
        self.summary["github_attempt"] = {"run_id": 480, "attempt": 2}
        result = report.reconcile(self.manifest, [self.summary])
        self.assertEqual("summary binding mismatch", result["errors"][0]["reason"])
        self.assertEqual("missing", result["results"][0]["status"])
        self.summary["github_attempt"] = current
        result = report.reconcile(self.manifest, [self.summary])
        self.assertEqual("completed", result["results"][0]["status"])
        self.assertEqual([], result["errors"])

    def test_unreadable_or_invalid_baseline_keeps_execution_and_partial_evidence(self):
        self.row.update(status="failed", last_stage="readback", reason="different rows")
        with tempfile.TemporaryDirectory() as directory:
            file = Path(directory) / "baseline.json"
            for body, expected in ((None, "baseline-unreadable"), ("{", "baseline-invalid-json"),
                                   ('{"kind":"accepted","id":"b1","cases":{}}', "invalid-baseline-schema")):
                with self.subTest(expected=expected):
                    if body is not None:
                        file.write_text(body)
                    reference, status = baseline.load(file)
                    result = report.reconcile(self.manifest, [self.summary], reference, baseline_status=status)
                    self.assertEqual(expected, result["baseline"]["reason"])
                    self.assertEqual(["failed", "missing"], [row["status"] for row in result["results"]])
                    self.assertEqual("readback", result["results"][0]["last_stage"])
                    self.assertIn(expected, report.markdown(result))

    def test_a_run_retains_the_published_baseline_pinned_at_preparation(self):
        environment = "runner"
        record_sha = ["a" * 40]
        published = {"schema_version": 1, "active": {environment: "v1"}, "versions": {}}
        published_payloads = {}
        for version, commit in (("v1", "b" * 40), ("v2", "c" * 40)):
            content = json.dumps({"id": version, "version": version, "code_sha": "d" * 40,
                                  "kind": "accepted", "environment": environment, "cases": {}}).encode()
            published_payloads[commit] = content
            published["versions"][version] = {"id": version, "code_sha": "d" * 40, "environment": environment,
                                              "commit": commit, "path": "baselines/report.json",
                                              "sha256": hashlib.sha256(content).hexdigest()}

        def fetch(url):
            if url.endswith("/commits/main"):
                return json.dumps({"sha": record_sha[0]}).encode()
            if url.endswith("/releases.json"):
                return json.dumps(published).encode()
            commit = url.split("/")[-3]
            return published_payloads[commit]

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            first, second = root / "first", root / "second"
            first.mkdir()
            second.mkdir()
            receipt = baseline.pin(first, environment, fetch=fetch)
            self.assertEqual("available", receipt["status"])
            self.assertEqual("v1", receipt["version"])
            self.assertEqual("b" * 40, receipt["baseline_commit"])
            self.assertEqual("a" * 40, receipt["release_record_commit"])

            record_sha[0] = "e" * 40
            published["active"][environment] = "v2"
            self.assertEqual("v2", baseline.pin(second, environment, fetch=fetch)["version"])
            pinned, historical = baseline.load(first / "baseline.json", identity=receipt)
            self.assertEqual("v1", pinned["version"])
            self.assertEqual("a" * 40, historical["release_record_commit"])
            (first / "baseline.json").write_bytes(published_payloads["c" * 40])
            _, mismatch = baseline.load(first / "baseline.json", identity=receipt)
            self.assertEqual("baseline-digest-mismatch", mismatch["reason"])

    def test_endpoint_p95_suspected_regression_requires_repeat_and_noise_support(self):
        case_id = self.row["case_id"]
        metric = "endpoint_p95_nanos"
        latency = {"status": "eligible", "source": "correlated-events-v1", "p95Nanos": 100}
        self.baseline["cases"][case_id]["live_latency"] = latency
        self.baseline["comparison_rules"] = {"version": 1, "cases": {case_id: {
            metric: {"worse": "higher", "observe_ratio": 0.1, "suspect_ratio": 0.2,
                     "minimum_absolute": 5, "baseline_noise": 8, "minimum_repeats": 2}
        }}}
        second = copy.deepcopy(self.row)
        second["repeat"] = 2
        self.summary["results"].append(second)
        for values, expected in [((108, 109), "normal"), ((115, 118), "observe"),
                                 ((120, 126), "suspected-regression"), ((116, 130), "observe")]:
            with self.subTest(values=values):
                for row, value in zip(self.summary["results"], values):
                    row["live_latency"] = dict(latency, p95Nanos=value)
                result = report.reconcile(self.manifest, [self.summary], self.baseline)
                case = result["case_verdicts"][case_id]
                self.assertEqual("completed", case["execution"])
                self.assertEqual(expected, case["comparison"])
                self.assertEqual(list(values), case["metrics"][0]["samples"])
        second["live_latency"] = dict(latency, source="other-sampling-protocol", p95Nanos=150)
        result = report.reconcile(self.manifest, [self.summary], self.baseline)
        self.assertEqual("unavailable", result["case_verdicts"][case_id]["comparison"])

    def test_memory_budget_failure_is_independent_of_comparison_eligibility(self):
        case = self.row["case_id"]
        self.baseline["memory_limits"] = {case: {"snapshot.rss_peak_kib": {"source": "fixture", "limit_kib": 115}}}
        result = report.reconcile(self.manifest, [self.summary], self.baseline)
        self.assertEqual("failed", result["results"][0]["status"])
        self.assertEqual("memory-budget-exceeded", result["results"][0]["memory_failures"][0]["reason"])
        self.assertEqual("failed", result["case_verdicts"][case]["execution"])
        self.assertEqual("unavailable", result["case_verdicts"][case]["comparison"])
        self.assertEqual("completed", self.row["status"])

        self.row["resources"]["windows"]["snapshot"]["covered"] = False
        result = report.reconcile(self.manifest, [self.summary], self.baseline)
        self.assertEqual("completed", result["results"][0]["status"])
        self.row.update(status="failed", exit_attribution="observed-java-out-of-memory")
        result = report.reconcile(self.manifest, [self.summary])
        self.assertEqual("observed-java-out-of-memory", result["results"][0]["memory_failures"][0]["reason"])

    def test_attributed_oom_kill_counts_as_hard_failure_without_a_baseline(self):
        self.row.update(status="timed-out", reason="deadline", exit_attribution="observed-op-oom-kill")
        data = report.reconcile(self.manifest, [self.summary])
        first = data["results"][0]
        self.assertEqual("failed", first["status"])
        self.assertEqual("deadline", first["reason_before_memory_failure"])
        self.assertEqual("observed-op-oom-kill", first["memory_failures"][0]["reason"])
        self.row["exit_attribution"] = "unattributed-process-exit"
        unverified = report.reconcile(self.manifest, [self.summary])["results"][0]
        self.assertEqual("timed-out", unverified["status"])
        self.assertNotIn("memory_failures", unverified)

    def test_mismatched_or_missing_environment_profile_never_qualifies(self):
        variants = [
            dict(self.row, environment_profile="other-env"),
            {key: value for key, value in self.row.items() if key != "environment_profile"},
            dict(self.row, paired_arms={"hot": dict(self.row, environment_profile="other-env")}),
        ]
        for row in variants:
            with self.subTest(row=row):
                result = report.reconcile(self.manifest, [dict(self.summary, results=[row])], self.baseline)
                first = result["results"][0]
                self.assertEqual("invalid", first["status"])
                self.assertEqual("result-unavailable", first["comparisons"]["status"])
                self.assertEqual({}, result["statistics"])

    def test_foreign_batch_cannot_invalidate_its_owning_batchs_repeat(self):
        other = copy.deepcopy(self.manifest["batches"][0])
        other["index"] = 1
        other["entries"][0]["case"]["id"] = "another.case"
        self.manifest["batches"].append(other)
        self.manifest["selected_ids"].append("another.case")
        del self.manifest["manifest_sha256"]
        self.manifest["manifest_sha256"] = catalog.digest(self.manifest)
        self.summary["manifest_sha256"] = self.manifest["manifest_sha256"]
        foreign = {**self.summary, "batch": 1}
        results = [report.reconcile(self.manifest, summaries) for summaries in
                   ([self.summary, foreign], [foreign, self.summary])]
        self.assertEqual("completed", results[0]["results"][0]["status"])
        self.assertEqual(results[0]["results"], results[1]["results"])
        self.assertEqual(results[0]["statistics"], results[1]["statistics"])
        self.assertTrue(results[0]["errors"])

    def test_rss_comparisons_require_a_nonempty_matching_source(self):
        usage = self.row["resources"]["windows"]["snapshot"]
        reference = self.baseline["cases"][self.row["case_id"]]["resources"]["windows"]["snapshot"]
        for source in (None, "", " "):
            with self.subTest(source=source):
                usage["source"] = reference["source"] = source
                result = report.reconcile(self.manifest, [self.summary], self.baseline)
                self.assertEqual("no-eligible-observation", result["results"][0]["comparisons"]["status"])
                self.assertEqual({}, result["statistics"])

    def test_elapsed_time_cannot_be_compared_against_other_load(self):
        self.row["effective_load"] = {"rows": 64}
        result = report.reconcile(self.manifest, [self.summary], self.baseline)
        self.assertEqual("invalid", result["results"][0]["status"])

    def test_invalid_published_evidence_cannot_leave_a_partial_baseline_available(self):
        case_id = self.row["case_id"]
        identity = {"id": "accepted", "version": "v1", "code_sha": "a" * 40, "environment": "fixture-env"}
        document = {**self.baseline, **identity, "kind": "accepted"}
        variations = {
            "wrong-profile": {"environment_profile": "different"},
            "wrong-case": {"case_id": "different"},
            "wrong-code": {"code_sha": "b" * 40},
            "wrong-arm-run": {"paired_arms": {"control": dict(self.row, code_sha="a" * 40),
                                              "variant": dict(self.row, code_sha="a" * 40, run_id="different")}},
            "wrong-arm-profile": {"paired_arms": {"ordinary": self.row,
                                                   "other": dict(self.row, environment_profile="different")}},
            "bad-window": {"resources": {"status": "observed", "windows": {
                "snapshot": {"source": "procfs", "covered": True, "rss_peak_kib": "bad"}}}},
            "bad-percentile": {"live_latency": {"status": "eligible", "source": "events", "p95Nanos": None}},
            "bad-completion": {"phase_nanos": {"completion_observed": "unavailable"}},
        }
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "baseline.json"
            for name, override in variations.items():
                with self.subTest(name=name):
                    document["cases"][case_id] = {**self.row, "code_sha": identity["code_sha"], **override}
                    path.write_text(json.dumps(document))
                    reference, status = baseline.load(path, identity=identity)
                    self.assertIsNone(reference)
                    self.assertEqual("invalid-baseline-case", status["reason"])
                    result = report.reconcile(self.manifest, [self.summary], reference, baseline_status=status)
                    self.assertEqual("completed", result["results"][0]["status"])
                    self.assertEqual("invalid-baseline-case", result["results"][0]["comparisons"]["reason"])

    def test_paired_controls_require_both_matching_arms(self):
        case = self.manifest["batches"][0]["entries"][0]["case"]
        case["tags"] = ["paired-control"]
        del self.manifest["manifest_sha256"]
        self.manifest["manifest_sha256"] = catalog.digest(self.manifest)
        self.summary["manifest_sha256"] = self.manifest["manifest_sha256"]
        self.row["paired_arms"] = {"normal": dict(self.row)}
        incomplete = report.reconcile(self.manifest, [self.summary], self.baseline)
        self.assertEqual("invalid", incomplete["results"][0]["status"])

        self.row["paired_arms"]["stress"] = dict(self.row["paired_arms"]["normal"])
        reference = self.baseline["cases"][case["id"]]
        reference["paired_arms"] = {"normal": dict(reference), "stress": dict(reference, target_delay_millis=2)}
        result = report.reconcile(self.manifest, [self.summary], self.baseline)
        self.assertEqual("incompatible", result["results"][0]["comparisons"]["status"])

    def test_growth_and_retention_require_ordered_covered_measurements(self):
        growth = "steady.rss_growth_kib_per_second"
        retained = "lifecycle.idle_rss_retained_kib"
        metric_rule = {"worse": "higher", "observe_ratio": 0.1, "suspect_ratio": 0.2,
                       "minimum_absolute": 1, "baseline_noise": 3, "minimum_repeats": 2}
        case_id = self.row["case_id"]
        self.baseline["comparison_rules"] = {"version": 1, "cases": {case_id: {
            growth: metric_rule, retained: metric_rule,
        }}}
        for result, values in ((self.row, [100, 120, 144]), (self.baseline["cases"][case_id], [100, 108, 120])):
            resources = result["resources"]
            result["lifecycle_cycles"] = [
                {"cycle": cycle, "idle_seconds": 5, "state": "STOPPED", "target_readback": "verified"}
                for cycle in (1, 2, 3)
            ]
            for cycle, rss in enumerate(values, 1):
                for window in (f"steady-{cycle}", f"lifecycle-cycle-{cycle}/idle"):
                    resources["windows"][window] = {"covered": True, "source": "fixture", "rss_median_kib": rss,
                                                    "started_epoch_ms": (cycle - 1) * 5000}
        second = copy.deepcopy(self.row)
        second["repeat"] = 2
        self.summary["results"].append(second)

        compared = report.reconcile(self.manifest, [self.summary], self.baseline)["case_verdicts"][case_id]
        self.assertEqual("suspected-regression", compared["comparison"])
        self.assertEqual({growth: [4.4, 4.4], retained: [44, 44]},
                         {item["metric"]: item["samples"] for item in compared["metrics"]})
        for resource in self.baseline["cases"][case_id]["resources"]["windows"].values():
            if "rss_median_kib" in resource:
                resource["rss_median_kib"] = 100
        zero_baseline = report.reconcile(self.manifest, [self.summary], self.baseline)["case_verdicts"][case_id]
        self.assertEqual("suspected-regression", zero_baseline["comparison"])
        self.assertTrue(all(item["change_ratio"] is None for item in zero_baseline["metrics"]))

        second["resources"]["windows"]["steady-2"]["covered"] = False
        second["lifecycle_cycles"][1]["target_readback"] = "different"
        incomplete = report.reconcile(self.manifest, [self.summary], self.baseline)["case_verdicts"][case_id]
        self.assertEqual("unavailable", incomplete["comparison"])

    def test_reports_incompatible_baseline_load(self):
        self.baseline["cases"][self.row["case_id"]]["effective_load"] = {"rows": 64}
        result = report.reconcile(self.manifest, [self.summary], self.baseline)
        self.assertEqual("incompatible", result["results"][0]["comparisons"]["status"])

    def test_completed_arms_do_not_qualify_an_incomplete_baseline_repeat(self):
        self.row["paired_arms"] = {"limited": copy.deepcopy(self.row)}
        baseline = self.baseline["cases"][self.row["case_id"]]
        baseline["paired_arms"] = {"limited": copy.deepcopy(baseline)}
        baseline["status"] = "measurement-incomplete"
        result = report.reconcile(self.manifest, [self.summary], self.baseline)
        self.assertEqual("baseline-result-unavailable", result["results"][0]["comparisons"]["status"])

    def test_summarizes_comparable_covered_repeats_and_keeps_raw_values(self):
        second = copy.deepcopy(self.row)
        second["repeat"] = 2
        second["resources"]["windows"]["snapshot"]["rss_peak_kib"] = 140
        self.summary["results"].append(second)
        result = report.reconcile(self.manifest, [self.summary])
        case = self.row["case_id"]
        self.assertEqual(2, len(result["results"]))
        self.assertEqual([120, 140], [row["resources"]["windows"]["snapshot"]["rss_peak_kib"]
                                     for row in result["results"]])
        metrics = result["statistics"][case]["single"]
        self.assertEqual({"snapshot.rss_peak_kib"}, set(metrics))
        observed, = metrics["snapshot.rss_peak_kib"]
        self.assertEqual(2, observed["count"])
        self.assertEqual(130, observed["median"])
        self.assertEqual(20, observed["range"])
        second["environment"] = {"os": "other"}
        cohorts = report.reconcile(self.manifest, [self.summary])["statistics"][case]["single"]["snapshot.rss_peak_kib"]
        self.assertEqual([1, 1], [cohort["count"] for cohort in cohorts])
