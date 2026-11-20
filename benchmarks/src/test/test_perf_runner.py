from argparse import ArgumentTypeError, Namespace
from contextlib import redirect_stderr, redirect_stdout
from io import StringIO
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
import catalog
import runner


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
            subprocess.run(["git", "init", "-q", str(root)], check=True)
            subprocess.run(["git", "-C", str(root), "add", fixture.name], check=True)
            subprocess.run(["git", "-C", str(root), "-c", "user.name=Fixture", "-c",
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

    def test_operator_suite_lists_executable_definitions_and_preserves_scope(self):
        cases = runner.select(self.cases, [], [], "operators")
        ids = {case["id"] for case in cases}
        self.assertEqual(49, len(ids))
        self.assertNotIn("join.live.hot-update.wide", ids)
        self.assertNotIn("copy.live.slow-target.large", ids)
        self.assertEqual({"qualification-ready"}, {runner.case_listing(case)["execution_status"] for case in cases})
        self.assertIn("js.live.basic.standard", ids)
        self.assertIn("mixed.snapshot.double-map.standard", ids)
        self.assertNotIn("mixed.snapshot.union-branches.standard", ids)
        self.assertNotIn("mixed.snapshot.map-nest.standard", ids)
