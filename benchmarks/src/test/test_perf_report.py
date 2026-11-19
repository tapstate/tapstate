import copy
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
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
            "environment": {"os": "example"}, "measurement_status": "bounded-observation",
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

    def test_elapsed_time_cannot_be_compared_against_other_load(self):
        self.row["effective_load"] = {"rows": 64}
        result = report.reconcile(self.manifest, [self.summary], self.baseline)
        self.assertEqual("invalid", result["results"][0]["status"])

    def test_reports_incompatible_baseline_load(self):
        self.baseline["cases"][self.row["case_id"]]["effective_load"] = {"rows": 64}
        result = report.reconcile(self.manifest, [self.summary], self.baseline)
        self.assertEqual("incompatible", result["results"][0]["comparisons"]["status"])
