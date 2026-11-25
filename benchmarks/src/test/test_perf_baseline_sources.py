from copy import deepcopy
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
import verdict


class BaselineSourcesTest(unittest.TestCase):
    fixture = json.loads((Path(__file__).parent / "fixtures" / "baseline-v2-contract.json").read_bytes())
    nest = "nest.snapshot.basic.standard"
    missing = "copy.live.basic.standard"
    metric = "startup.rss_peak_bytes"
    profile = "ubuntu-24.04-x64-op"

    def updated_release(self):
        release = deepcopy(self.fixture["initial"])
        update = self.fixture["update"]
        release.update(version=update["version"], code_sha=update["code_sha"])
        for case_id, current in update["measurements"].items():
            case = release["cases"][case_id]
            case["first"] = case["current"]
            case["current"] = deepcopy(current)
        return release

    def observation(self, reference, *, case_id=None):
        conditions = deepcopy(reference["conditions"])
        group = reference["arms"]["single"]["metrics"][self.metric][0]
        resource = {
            "status": "observed",
            "scope": group["scope"],
            "op_heap_limit_mib": group["conditions"]["op_heap_limit_mib"],
            "sampling_interval_ms": group["conditions"]["sampling_interval_ms"],
            "windows": {"startup": {"covered": True, "rss_peak_kib": 425000, "source": group["source"]}},
        }
        return {"case_id": case_id or self.nest, "status": "completed", **conditions,
                "effective_input_rows": group["conditions"]["effective_input_rows"], "resources": resource}

    def test_public_pin_accepts_retained_per_case_provenance(self):
        release = self.updated_release()
        release["kind"] = "accepted"
        version = release["version"]
        publication = "a" * 40
        data_commit = "b" * 40
        data_path = "baselines/example/baseline.json"
        content = json.dumps(release).encode()
        record = {
            "schema_version": 1, "active": {self.profile: version},
            "versions": {version: {"id": release["id"], "environment": self.profile, "code_sha": release["code_sha"],
                                   "commit": data_commit, "path": data_path,
                                   "sha256": hashlib.sha256(content).hexdigest()}},
        }
        api = f"https://api.github.com/repos/{baseline.REPOSITORY}"
        raw = f"https://raw.githubusercontent.com/{baseline.REPOSITORY}"
        remote = {
            f"{api}/commits/main": json.dumps({"sha": publication}).encode(),
            f"{raw}/{publication}/{baseline.RELEASE_RECORD}": json.dumps(record).encode(),
            f"{raw}/{data_commit}/{data_path}": content,
        }
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            receipt = baseline.pin(root, self.profile, fetch=remote.__getitem__)
            accepted, verified = baseline.load_pinned(root / "baseline.json", receipt, self.profile)
        self.assertEqual(receipt, verified)
        self.assertEqual("available", receipt["status"], receipt)
        self.assertEqual(data_commit, receipt["baseline_commit"])
        for case_id in self.fixture["expected"]["unchanged"]:
            self.assertEqual(self.fixture["initial"]["cases"][case_id], accepted["cases"][case_id])

    def test_v2_current_and_first_compare_separately(self):
        release = self.updated_release()
        case = release["cases"][self.nest]
        row = self.observation(case["current"])
        comparison = report.compare(row, release, self.profile)
        self.assertEqual(self.fixture["expected"]["current_nest_median"], comparison["arms"]["single"][self.metric]["baseline"])
        self.assertEqual(self.fixture["expected"]["first_nest_median"], comparison["first"]["arms"]["single"][self.metric]["baseline"])
        case["first"]["conditions"]["runner"]["cpu"] = "Different CPU"
        comparison = report.compare(row, release, self.profile)
        self.assertEqual("fixture-comparison", comparison["status"])
        self.assertEqual("incompatible", comparison["first"]["status"])
        row["runner"]["runner_image"] = "Different revision"
        self.assertEqual("incompatible", report.compare(row, release, self.profile)["status"])

    def test_first_reference_alias_and_missing_case(self):
        release = self.fixture["initial"]
        row = self.observation(release["cases"][self.nest]["current"])
        comparison = report.compare(row, release, self.profile)
        self.assertEqual("fixture-comparison", comparison["status"])
        self.assertNotIn("first", comparison)
        self.assertNotIn("completion_ms", comparison["arms"]["single"])
        self.assertEqual("baseline-result-unavailable",
                         report.compare({**row, "case_id": self.missing}, release, self.profile)["status"])

    def test_reconcile_records_a_versioned_budget_failure_before_comparison(self):
        release = self.updated_release()
        reference = release["cases"][self.nest]["current"]
        group = reference["arms"]["single"]["metrics"][self.metric][0]
        budget = {key: group[key] for key in baseline.SOURCE_METRIC_BINDING}
        budget.update(limit=430000000, budget_version=2)
        release["memory_limits"] = {self.nest: {"single": {self.metric: [budget]}}}
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "baseline.json"
            path.write_text(json.dumps(release))
            accepted, receipt = baseline.load(path)
        self.assertEqual("available", receipt["status"])
        row = self.observation(reference)
        runner = row.pop("runner")
        row.update(code_sha="a" * 40, repeat=1, run_id="batch-run")
        case = {"id": self.nest, "case_version": row["case_version"], "definition_sha256": row["definition_sha256"]}
        manifest = {
            "identity": {"code_sha": row["code_sha"]}, "environment": self.profile, "unallocated": [],
            "selected_ids": [self.nest], "batches": [{"index": 0, "entries": [
                {"case": case, "repeats": 1, **row["effective_load"]},
            ]}],
        }
        manifest["manifest_sha256"] = catalog.digest(manifest)
        summary = {"batch": 0, "identity": manifest["identity"], "manifest_sha256": manifest["manifest_sha256"],
                   "run_id": row["run_id"], "process_exit": 0, "results": [row]}
        over = report.reconcile(manifest, [summary], accepted, batch_conditions={0: runner})["results"][0]
        self.assertEqual("failed", over["status"])
        self.assertEqual("result-unavailable", over["comparisons"]["status"])
        self.assertEqual("memory-budget-exceeded", over["memory_failures"][0]["reason"])
        self.assertEqual(2, over["memory_failures"][0]["budget_version"])
        self.assertEqual(425000 * 1024, over["memory_failures"][0]["observed"])
        incompatible = report.reconcile(manifest, [summary], accepted,
                                        batch_conditions={0: {**runner, "cpu": "Different CPU"}})["results"][0]
        self.assertEqual("incompatible", incompatible["comparisons"]["status"])
        self.assertNotIn("memory_failures", incompatible)

    def test_zero_reference_requires_explicit_thresholds(self):
        release = deepcopy(self.fixture["initial"])
        reference = release["cases"][self.nest]["current"]
        group = reference["arms"]["single"]["metrics"][self.metric][0]
        group["summary"]["median"] = 0
        row = self.observation(reference)
        comparison = report.compare(row, release, self.profile)
        self.assertEqual("zero-reference-no-absolute-rule", comparison["reason"])
        rule = release["comparison_rules"]["cases"][self.nest][self.metric]
        rule["zero_reference"] = {"observe_absolute": 100000000, "suspect_absolute": 200000000}
        comparison = report.compare(row, release, self.profile)
        sample = comparison["arms"]["single"][self.metric]
        result = verdict.classify([sample, sample], rule)
        self.assertEqual("suspected-regression", result["status"])
        self.assertIsNone(result["change_ratio"])
        self.assertEqual(425000 * 1024, result["absolute_change"])

    def test_bad_version_sample_or_case_identity_is_unavailable(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "baseline.json"
            for fault in ("schema", "median", "case-id", "evidence", "unit", "domain"):
                release = self.updated_release()
                case = release["cases"][self.nest]
                group = case["current"]["arms"]["single"]["metrics"][self.metric][0]
                if fault == "schema":
                    release["schema_version"] = 3
                elif fault == "median":
                    group["summary"]["median"] += 1
                elif fault == "case-id":
                    case["case_id"] = self.missing
                elif fault == "evidence":
                    group["samples"][0]["evidence"] = "evidence/../../secret.json"
                elif fault == "unit":
                    group["unit"] = "ms"
                else:
                    for sample in group["samples"]:
                        sample["value"] = -10
                    group["summary"].update(median=-10, min=-10, max=-10, range=0)
                with self.subTest(fault=fault):
                    path.write_text(json.dumps(release))
                    loaded, receipt = baseline.load(path)
                    self.assertIsNone(loaded)
                    self.assertEqual("unsupported-baseline-schema" if fault == "schema" else "invalid-baseline-case",
                                     receipt["reason"])
