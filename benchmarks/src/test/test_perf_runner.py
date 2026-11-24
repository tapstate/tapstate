from argparse import ArgumentTypeError, Namespace
from contextlib import redirect_stderr, redirect_stdout
from io import StringIO
from pathlib import Path
import os
import signal
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from actions_run import (BUDGET_SECONDS, BUILD_RESERVE_SECONDS, DEFAULT_BATCH_SECONDS, DEFAULT_SLOTS,
                         MEASUREMENT_RESERVE_SECONDS, RUNNER_PROFILE, selection)
import catalog
import runner
import workloads


class PerfRunnerTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.catalog = catalog.build_catalog()
        cls.cases = cls.catalog["cases"]

    def test_selection_filters_and_invalid_inputs(self):
        select = lambda filters: runner.select(self.cases, [], filters, "all")
        nest = {case["id"] for case in select(["group=nest"])}
        contains_nest = {case["id"] for case in select(["operators=nest"])}
        self.assertEqual(nest, contains_nest)
        filters = {case["id"] for case in select(["group=filter"])}
        contains_filter = {case["id"] for case in select(["operators=filter"])}
        self.assertEqual({"mixed.snapshot.double-map.standard"}, contains_filter - filters)
        result = select(["profile=wide", "profile=large", "tags=memory-regression"])
        self.assertEqual(len({case["id"] for case in result}), len(result))
        self.assertTrue(all(case["profile"] in {"wide", "large"} for case in result))
        self.assertEqual([], runner.select(self.cases, [], ["group=nest", "source=dummy"], "all", allow_empty=True))
        case_id = "copy.snapshot.basic.standard"
        for ids, filters, reason in [
            ([case_id, case_id + "@1"], [], "Duplicate"),
            ([case_id + "@2"], [], "Version mismatch"),
            (["no-such-case"], [], "Unknown case"),
            ([], ["labels=nest"], "Unknown case filter"),
            ([], ["group=nest", "source=dummy"], "zero cases"),
        ]:
            with self.assertRaisesRegex(ValueError, reason):
                runner.select(self.cases, ids, filters, "all")

    def test_batch_capacity_repeat_integrity_and_replay(self):
        cases = runner.select(self.cases, [], [], "basic")
        args = Namespace(repeats=3, rows=32, backlog_events=20, rate=10,
                         warmup_seconds=1, measurement_seconds=2, timeout_seconds=45, mix=None)
        entries = runner.make_entries(cases, args)
        estimates = {case["id"]: duration for case, duration in zip(cases, [7, 6, 5, 4])}
        allocation = runner.allocate(entries, estimates, 2, 15, 22, 2)
        self.assertEqual([11, 11], [batch["estimated_seconds"] for batch in allocation["batches"]])
        self.assertEqual(9, allocation["remaining_seconds"])
        self.assertEqual(allocation, runner.allocate(list(reversed(entries)), estimates, 2, 15, 22, 2))
        clean_catalog = dict(self.catalog, source_dirty=False)
        manifest = {"identity": runner.identity(clean_catalog), "selected_ids": [case["id"] for case in cases],
                    **allocation}
        manifest["manifest_sha256"] = catalog.digest(manifest)
        chosen = runner.replay(clean_catalog, manifest, 1)
        self.assertEqual(2, len(chosen))
        self.assertTrue(all(entry["repeats"] == 3 for entry in chosen))
        with self.assertRaisesRegex(ValueError, "different checkout"):
            runner.replay(dict(clean_catalog, code_sha="different"), manifest, 1)
        chosen[0]["rows"] = 11
        with self.assertRaisesRegex(ValueError, "digest"):
            runner.replay(clean_catalog, manifest, 1)
        unavailable = runner.allocate(entries, {cases[0]["id"]: 25}, 2, 10, 20, 3)
        self.assertEqual({"missing whole-case estimate", "exceeds available batch capacity"},
                         {entry["reason"] for entry in unavailable["unallocated"]})

    def test_allocation_and_replay_reject_fixture_edits_outside_benchmarks(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "benchmarks").mkdir()
            fixture = root / "BenchmarkRunIT.java"
            fixture.write_text("class BenchmarkRunIT {}")
            hooks = root / "empty-hooks"
            hooks.mkdir()
            subprocess.run(["git", "init", "-q", str(root)], check=True)
            subprocess.run(["git", "-C", str(root), "add", fixture.name], check=True)
            subprocess.run(["git", "-C", str(root), "-c", "user.name=Fixture", "-c",
                            f"core.hooksPath={hooks}", "-c",
                            "user.email=fixture@example.org", "commit", "--no-gpg-sign", "-qm", "Initial fixture"], check=True)
            with patch.object(catalog, "BENCHMARKS", root / "benchmarks"):
                clean = catalog.build_catalog()
                fixture.write_text("class BenchmarkRunIT { int changed; }")
                dirty = catalog.build_catalog()
                fixture.write_text("class BenchmarkRunIT { int changedAgain; }")
                changed_again = catalog.build_catalog()
        self.assertFalse(clean["source_dirty"])
        self.assertTrue(dirty["source_dirty"])
        self.assertTrue(changed_again["source_dirty"])
        self.assertEqual(clean["catalog_sha256"], dirty["catalog_sha256"])
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            allocate, replay = self.manifest_commands(root)
            self.assertEqual(0, self.invoke(allocate, clean)[0])
            self.assertIn("clean checkout", self.invoke(allocate, dirty)[1])
            self.assertIn("clean checkout", self.invoke(replay, changed_again)[1])

    def test_manifest_commands_check_load_before_writing_or_running(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            allocate, replay = self.manifest_commands(root)
            clean = dict(self.catalog, source_dirty=False)
            code, error = self.invoke(allocate + ["--mix", "100/0/0"], clean)
            self.assertEqual(1, code)
            self.assertIn("does not admit", error)
            self.assertFalse((root / "manifest.json").exists())
            code, error = self.invoke(allocate + ["--rows", "4294967328"], clean)
            self.assertEqual(1, code)
            self.assertIn("--rows must fit", error)
            self.assertFalse((root / "manifest.json").exists())
            self.assertEqual(0, self.invoke(allocate + ["--rows", "48"], clean)[0])
            with patch.object(runner, "run_entries", return_value=0) as execute:
                self.assertEqual(0, self.invoke(replay, clean)[0])
                self.assertEqual(48, execute.call_args.args[0][0]["rows"])
                self.assertEqual(0, self.invoke(replay + ["--rows", "48"], clean)[0])
                execute.reset_mock()
                self.assertIn("--rows", self.invoke(replay + ["--rows", "32"], clean)[1])
                self.assertIn("--mix", self.invoke(replay + ["--mix", "80/15/5"], clean)[1])
                execute.assert_not_called()

    def test_formal_allocation_requires_matching_whole_case_estimates(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            allocate, replay = self.manifest_commands(root)
            clean = dict(self.catalog, source_dirty=False)
            case = runner.select(self.cases, ["copy.snapshot.basic.standard"], [], "all")[0]
            reference = workloads.case_reference(case)
            registry = root / "workloads.json"
            estimates = root / "estimates.json"
            manifest = root / "manifest.json"
            allocate += ["--formal-workloads", str(registry)]
            recipe = {"definition_sha256": case["definition_sha256"], "version": 1,
                      "load": dict(runner.LOAD_DEFAULTS, rows=10000)}
            registry_data = {"schema_version": 1, "cases": {}}
            runner.write_json(registry, registry_data)
            estimates_data = {"schema_version": 1, "environment": "fixture", "cases": {}}
            runner.write_json(estimates, estimates_data)
            self.assertEqual(1, self.invoke(allocate, clean)[0])
            self.assertEqual([{"id": case["id"], "reason": f"missing formal workload for {reference}"}],
                             catalog.read_json(manifest)["unallocated"])

            registry_data["cases"][reference] = recipe
            runner.write_json(registry, registry_data)
            self.assertEqual(1, self.invoke(allocate, clean)[0])
            self.assertEqual("missing whole-case estimate", catalog.read_json(manifest)["unallocated"][0]["reason"])
            entries, _ = workloads.resolve([case], registry_data, runner.LOAD_DEFAULTS.keys())
            estimates_data["cases"][reference] = {"workload_sha256": entries[0]["workload_sha256"], "seconds": 1,
                                                  "evidence": "fixture-calibration"}
            runner.write_json(estimates, estimates_data)
            self.assertEqual(0, self.invoke(allocate, clean)[0])
            self.assertEqual("formal", catalog.read_json(manifest)["scale"])
            with patch.object(runner, "run_entries", return_value=0) as execute:
                self.assertEqual(0, self.invoke(replay, clean)[0])
                self.assertEqual(entries, execute.call_args.args[0])
            self.assertIn("load overrides", self.invoke(allocate + ["--rows", "96"], clean)[1])

            recipe["load"]["rows"] = 20000
            runner.write_json(registry, registry_data)
            self.assertEqual(1, self.invoke(allocate, clean)[0])
            self.assertEqual("missing whole-case estimate", catalog.read_json(manifest)["unallocated"][0]["reason"])

    def test_hosted_manifest_covers_the_versioned_catalog_and_manual_selections(self):
        directory = catalog.CASE_ROOT
        registry = directory / "formal-workloads.json"
        estimates = directory / "hosted-runner-estimates.json"
        references = {workloads.case_reference(case) for case in self.cases}
        self.assertEqual(references, set(catalog.read_json(registry)["cases"]))
        self.assertEqual(references, set(catalog.read_json(estimates)["cases"]))
        budget = BUDGET_SECONDS - MEASUREMENT_RESERVE_SECONDS
        with tempfile.TemporaryDirectory() as output:
            manifest_path = Path(output) / "manifest.json"
            allocate = [
                "allocate", "--formal-workloads", str(registry), "--estimates", str(estimates),
                "--environment", RUNNER_PROFILE, "--slots", str(DEFAULT_SLOTS), "--deadline-seconds", str(budget),
                "--batch-seconds", str(DEFAULT_BATCH_SECONDS), "--reserve-seconds", str(BUILD_RESERVE_SECONDS),
                "--output", str(manifest_path),
            ]
            checkout = dict(self.catalog, source_dirty=False)
            cases = [
                (selection("all"), [], []),
                (selection("group", group="nest"), [], ["group=nest"]),
                (selection("filter", filters="operators=nest, profile=wide"), [], ["operators=nest", "profile=wide"]),
                (selection("ids", case_ids="copy.snapshot.basic.standard@1"), ["copy.snapshot.basic.standard@1"], []),
                (selection("ids", case_ids="copy.snapshot.basic.standard@1, nest.snapshot.fanout.wide@2"),
                 ["copy.snapshot.basic.standard@1", "nest.snapshot.fanout.wide@2"], []),
            ]
            for flags, ids, filters in cases:
                with self.subTest(selection=flags):
                    self.assertEqual((0, ""), self.invoke(allocate + flags, checkout))
                    manifest = catalog.read_json(manifest_path)
                    expected = runner.select(self.cases, ids, filters, "all")
                    self.assertEqual([case["id"] for case in expected], manifest["selected_ids"])
                    self.assertEqual([], manifest["unallocated"])
                    entries = [entry for batch in manifest["batches"]
                               for entry in runner.replay(checkout, manifest, batch["index"])]
                    self.assertEqual(len(expected), len(entries))
                    self.assertTrue(all(entry["scale"] == "formal" and entry["repeats"] == 3 for entry in entries))
                    costs = manifest["whole_case_estimates_seconds"]
                    self.assertEqual(set(manifest["selected_ids"]), set(costs))
                    for batch in manifest["batches"]:
                        self.assertEqual(batch["estimated_seconds"], sum(
                            costs[entry["case"]["id"]] for entry in batch["entries"]
                        ))
                    for entry in entries:
                        case = entry["case"]
                        cycles = case["workload_spec"]["scenario"]["parameters"].get("same_jvm_cycles_per_repeat", 1)
                        if case["mode"] == "live-latency" or cycles > 1:
                            arms = 2 if "paired-control" in case["tags"] else 1
                            window = entry["warmup_seconds"] + entry["measurement_seconds"]
                            self.assertGreaterEqual(costs[case["id"]], entry["repeats"] * arms * cycles * window)
                    if not flags:
                        self.assertEqual({"small", "standard", "wide", "large"},
                                         {case["profile"] for case in manifest["selected_cases"]})
                        self.assertEqual({"snapshot", "live-latency", "backlog-throughput"},
                                         {case["mode"] for case in manifest["selected_cases"]})
                        self.assertTrue(any(case["paired_control"] for case in manifest["selected_cases"]))
                        self.assertLessEqual(manifest["longest_seconds"], DEFAULT_BATCH_SECONDS)

    def manifest_commands(self, root):
        estimates = root / "estimates.json"
        estimates.write_text('{"environment":"fixture","durations_seconds":{"copy.snapshot.basic.standard":1}}')
        manifest = str(root / "manifest.json")
        return (["allocate", "--id", "copy.snapshot.basic.standard", "--slots", "1", "--batch-seconds", "2",
                 "--deadline-seconds", "2", "--environment", "fixture", "--estimates", str(estimates),
                 "--output", manifest],
                ["run", "--manifest", manifest, "--connectors", str(root), "--output", str(root / "output")])

    def invoke(self, args, checkout):
        with patch.object(sys, "argv", ["runner.py", *args]), patch.object(catalog, "build_catalog",
                return_value=checkout), redirect_stdout(StringIO()), redirect_stderr(StringIO()) as errors:
            return runner.main(), errors.getvalue()

    def test_mix_validation_rejects_mixed_supported_and_unsupported_cases(self):
        cases = runner.select(self.cases, [], [], "basic")
        args = Namespace(repeats=1, rows=32, backlog_events=20, rate=10,
                         warmup_seconds=1, measurement_seconds=2, timeout_seconds=45, mix=[100, 0, 0])
        with self.assertRaisesRegex(ValueError, "does not admit"):
            runner.checked_entries(runner.make_entries(cases, args))
        self.assertEqual([100, 0, 0], runner.mix("100/0/0"))
        with self.assertRaisesRegex(ArgumentTypeError, "summing to 100"):
            runner.mix("99/0/0")

    def test_group_cleanup_also_signals_children_after_the_maven_process_exits(self):
        process = Mock(pid=123)
        process.poll.return_value = 0
        with patch.object(runner.os, "killpg", side_effect=[None, ProcessLookupError()]) as kill:
            self.assertTrue(runner.stop_process(process))
        self.assertEqual([(123, signal.SIGTERM), (123, 0)], [call.args for call in kill.call_args_list])

    def test_replay_reserves_build_time_and_bounds_both_phases(self):
        launch = 10
        wall = 1_700_000_000_000_000_000
        manifest = {"reserve_seconds": 1}
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory)
            process = Mock(args=["mvn"])
            with patch.object(runner.time, "monotonic", return_value=11.01):
                with self.assertRaises(subprocess.TimeoutExpired):
                    runner.wait_for_fixture(process, output, 1, manifest, launch, wall)
            marker = output / "fixture-started"
            marker.touch()
            for offset in (990_000_000, 1_000_000_000):
                with self.subTest(marker_offset=offset):
                    os.utime(marker, ns=(wall + offset, wall + offset))
                    with patch.object(runner.time, "monotonic", return_value=11.01):
                        process.wait.return_value = 0
                        self.assertEqual(0, runner.wait_for_fixture(process, output, 1, manifest, launch, wall))
                    process.wait.assert_called_once()
                    self.assertAlmostEqual(offset / 1_000_000_000 - 0.01, process.wait.call_args.kwargs["timeout"])
                    process.wait.reset_mock()
            with patch.object(runner.time, "monotonic", return_value=12.01):
                with self.assertRaises(subprocess.TimeoutExpired):
                    runner.wait_for_fixture(process, output, 1, manifest, launch, wall)
            os.utime(marker, ns=(wall + 1_020_000_000, wall + 1_020_000_000))
            with patch.object(runner.time, "monotonic", return_value=11.03):
                with self.assertRaises(subprocess.TimeoutExpired):
                    runner.wait_for_fixture(process, output, 1, manifest, launch, wall)
            process.wait.assert_not_called()

    def test_replay_without_a_reserve_preserves_the_combined_limit(self):
        for manifest in (None, {}, {"reserve_seconds": 0}):
            with self.subTest(manifest=manifest):
                process = Mock(args=["mvn"])
                process.wait.side_effect = subprocess.TimeoutExpired(process.args, 1)
                with self.assertRaises(subprocess.TimeoutExpired):
                    runner.wait_for_fixture(process, Path("unused"), 1, manifest, 10, 0)
                process.wait.assert_called_once_with(timeout=1)

    def test_wall_deadline_bounds_a_late_fixture_after_its_build(self):
        launch = 10
        wall = 1_700_000_000
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory)
            marker = output / "fixture-started"
            marker.touch()
            os.utime(marker, ns=((wall + 1) * 10**9,) * 2)
            process = Mock(args=["mvn"])
            with patch.object(runner.time, "monotonic", return_value=11.25):
                runner.wait_for_fixture(process, output, 300, {"reserve_seconds": 30}, launch, wall * 10**9,
                                        stop_at_epoch_seconds=wall + 2)
                process.wait.assert_called_once_with(timeout=0.75)
            process.reset_mock()
            with patch.object(runner.time, "monotonic", return_value=12.01):
                with self.assertRaises(subprocess.TimeoutExpired):
                    runner.wait_for_fixture(process, output, 300, {"reserve_seconds": 30}, launch, wall * 10**9,
                                            stop_at_epoch_seconds=wall + 2)
            process.wait.assert_not_called()

    def test_operator_suite_lists_executable_definitions_and_preserves_scope(self):
        cases = runner.select(self.cases, [], [], "operators")
        ids = {case["id"] for case in cases}
        self.assertEqual(55, len(ids))
        self.assertTrue({f"aggregate.snapshot.basic.{profile}" for profile in ("small", "standard", "wide", "large")}
                        | {"aggregate.live.basic.standard", "aggregate.backlog.basic.standard"} <= ids)
        self.assertNotIn("join.live.hot-update.wide", ids)
        self.assertNotIn("copy.live.slow-target.large", ids)
        self.assertEqual({"qualification-ready"}, {runner.case_listing(case)["execution_status"] for case in cases})
        self.assertIn("js.live.basic.standard", ids)
        self.assertIn("mixed.snapshot.double-map.standard", ids)
        self.assertNotIn("mixed.snapshot.union-branches.standard", ids)
        self.assertNotIn("mixed.snapshot.map-nest.standard", ids)
