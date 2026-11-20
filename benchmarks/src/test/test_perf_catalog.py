import importlib.util
import json
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import unittest
from copy import deepcopy


BENCHMARKS = Path(__file__).resolve().parents[2]
CATALOG = BENCHMARKS / "catalog.py"
MODULE = importlib.util.spec_from_file_location("perf_catalog", CATALOG)
catalog_tool = importlib.util.module_from_spec(MODULE)
MODULE.loader.exec_module(catalog_tool)
PROFILES = {"small", "standard", "wide", "large"}
BASE_GROUPS = {"copy", "map", "filter", "js", "unwind", "union", "join", "nest", "aggregate"}
SPECIAL_CASES = {
    "copy.snapshot.dummy-baseline.standard",
    "copy.live.handoff.standard",
    "mixed.snapshot.double-map.standard",
    "join.live.hot-update.standard",
    "nest.live.child-update.standard",
    "nest.snapshot.fanout.wide",
    "join.live.hot-update.wide",
    "copy.live.slow-target.large",
    "join.live.steady-state.standard",
    "join.lifecycle.release.standard",
}


class PerfCatalogTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.catalog = catalog_tool.build_catalog()
        cls.cases = {case["id"]: case for case in cls.catalog["cases"]}

    def test_inventory_matches_supported_operator_matrix(self):
        model = BENCHMARKS.parent / "core/core-model/src/main/java/io/tapstate/core/model/TransformBody.java"
        operators = set(re.findall(r'^\s*@YamlType\("([a-z0-9-]+)"\)', model.read_text(encoding="utf-8"), re.M))
        self.assertEqual(BASE_GROUPS - {"copy"}, operators)
        expected = set(SPECIAL_CASES)
        for group in BASE_GROUPS:
            expected.update(f"{group}.snapshot.basic.{profile}" for profile in PROFILES)
            expected.add(f"{group}.live.basic.standard")
            backlog = "fixed-iud" if group == "copy" else "basic"
            expected.add(f"{group}.backlog.{backlog}.standard")
            basic = [case for case in self.cases.values() if case["group"] == group
                     and case["spec"] == f"specs/{group}-basic.json"]
            self.assertEqual({"snapshot", "live-latency", "backlog-throughput"}, {case["mode"] for case in basic})
            for case in basic:
                self.assertEqual(case["source"], "mongodb")
                self.assertEqual(case["target"], "mongodb")
                self.assertEqual(case["operators"], [] if group == "copy" else [group])
        self.assertEqual(set(self.cases), expected)
        self.assertEqual(len(self.catalog["cases"]), len(expected))
        self.assertEqual(len({case["definition_sha256"] for case in self.cases.values()}), len(expected))
        for case in self.cases.values():
            self.assertEqual(case["execution_status"], "definition-only")
            self.assertEqual(case["workload_spec"]["eligibility"]["calibration_status"], "pending")

    def test_combination_and_reused_memory_metadata(self):
        nest_only = {case["id"] for case in self.cases.values() if case["group"] == "nest"}
        nest_related = {case["id"] for case in self.cases.values() if "nest" in case["operators"]}
        self.assertEqual(nest_related, nest_only)
        filter_only = {case["id"] for case in self.cases.values() if case["group"] == "filter"}
        filter_related = {case["id"] for case in self.cases.values() if "filter" in case["operators"]}
        self.assertEqual(filter_related - filter_only, {"mixed.snapshot.double-map.standard"})
        self.assertEqual(
            self.cases["mixed.snapshot.double-map.standard"]["operators"], ["map", "filter", "js"]
        )
        memory = {case["id"] for case in self.cases.values() if "memory-regression" in case["tags"]}
        self.assertEqual(memory, {
            "copy.snapshot.basic.wide", "copy.snapshot.basic.large", "nest.snapshot.fanout.wide",
            "join.live.hot-update.wide", "copy.live.slow-target.large",
            "join.live.steady-state.standard", "join.lifecycle.release.standard",
        })
        self.assertEqual(len(self.cases["copy.live.slow-target.large"]["environments"]), 2)

    def test_resolves_generation_boundaries_and_special_recipes(self):
        snapshot = self.cases["nest.snapshot.basic.standard"]["workload_spec"]
        self.assertEqual(snapshot["profile"]["logical_row_bytes"], 1024)
        self.assertEqual(snapshot["profile"]["fields"], 20)
        self.assertEqual(snapshot["generation"]["logical_row_size_tolerance_percent"], 5)
        self.assertEqual(snapshot["repetition"]["count"], 3)
        self.assertEqual(snapshot["scenario"]["inputs"]["roots"]["children_per_root"], 4)
        self.assertIn("later boundary", snapshot["measurements"]["snapshot"]["finish"])
        live = self.cases["nest.live.basic.standard"]["workload_spec"]
        self.assertEqual(live["incremental"]["mix_percent"], {"insert": 80, "update": 15, "delete": 5})
        self.assertEqual(live["measurements"]["live-latency"]["trial"]["p99"]["minimum_valid_samples"], 10000)
        backlog = self.cases["copy.backlog.fixed-iud.standard"]["workload_spec"]
        self.assertEqual(backlog["measurements"]["backlog-throughput"]["trial"]["batch_events_trial_target"], 300000)
        self.assertIn("all expected writes", backlog["measurements"]["backlog-throughput"]["finish"])
        self.assertEqual(set(self.cases["copy.live.handoff.standard"]["workload_spec"]["measurements"]),
                         {"snapshot", "live-latency"})
        steady = self.cases["join.live.steady-state.standard"]["workload_spec"]
        self.assertEqual(steady["incremental"]["operations"], ["update existing keys only"])
        self.assertNotIn("mix_percent", steady["incremental"])
        dummy = self.cases["copy.snapshot.dummy-baseline.standard"]["workload_spec"]
        self.assertNotIn("generation", dummy)

    def test_rejects_inconsistent_specs_and_binds_semantic_versions(self):
        case = self.cases["mixed.snapshot.double-map.standard"]
        contracts = catalog_tool.read_json(catalog_tool.CASE_ROOT / "contracts.json")
        spec = catalog_tool.read_json(catalog_tool.CASE_ROOT / case["spec"])
        metadata = {key: value for key, value in case.items()
                    if key not in ("execution_status", "definition_sha256", "workload_spec")}
        with self.assertRaisesRegex(ValueError, "operators must match"):
            catalog_tool.resolve_case(dict(metadata, operators=["map", "js"]), contracts, spec)
        with self.assertRaisesRegex(ValueError, "profile version does not match"):
            catalog_tool.resolve_case(dict(metadata, profile_version=2), contracts, spec)
        moved = catalog_tool.resolve_case(dict(metadata, line=1000), contracts, spec)
        self.assertEqual(moved["definition_sha256"], case["definition_sha256"])
        changed = deepcopy(spec)
        changed["expected"]["readback"].append("A changed correctness contract.")
        resolved = catalog_tool.resolve_case(metadata, contracts, changed)
        self.assertNotEqual(resolved["definition_sha256"], case["definition_sha256"])

    def test_working_catalog_and_case_description_commands(self):
        case_id = "nest.snapshot.basic.standard"
        description = subprocess.run(
            [sys.executable, str(CATALOG), "--format", "markdown", "--case", case_id],
            text=True, capture_output=True, check=True,
        ).stdout
        self.assertIn(case_id, description)
        self.assertIn("Group children independently", description)
        self.assertIn("Both all inputs are consumed", description)
        self.assertIn(self.cases[case_id]["definition_sha256"], description)
        self.assertIn("64 registered cases", catalog_tool.render_markdown(self.catalog))
        invalid = subprocess.run(
            [sys.executable, str(CATALOG), "--case", "missing-case"],
            text=True, capture_output=True, check=False,
        )
        self.assertEqual(invalid.returncode, 1)
        self.assertEqual(invalid.stdout, "")
        self.assertIn("Unknown case ID", invalid.stderr)
        self.assertEqual(json.loads(json.dumps(self.catalog)), catalog_tool.build_catalog())

    def test_declarations_compile_on_their_own(self):
        annotation = catalog_tool.SCANNER.with_name("PerfCase.java")
        sources = sorted(catalog_tool.CASE_ROOT.glob("*Cases.java"))
        with tempfile.TemporaryDirectory() as classes:
            result = subprocess.run(
                ["javac", "-proc:none", "-d", classes, str(annotation), *map(str, sources)],
                text=True, capture_output=True, check=False,
            )
        self.assertEqual(result.returncode, 0, result.stderr)
