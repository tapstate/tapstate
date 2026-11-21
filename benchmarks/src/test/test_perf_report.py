import copy
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
