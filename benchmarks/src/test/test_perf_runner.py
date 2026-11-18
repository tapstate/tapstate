from argparse import ArgumentTypeError, Namespace
from pathlib import Path
import sys
import unittest

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
        self.assertEqual({"mixed.snapshot.map-nest.standard"}, contains_nest - nest)
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
        manifest = {"identity": runner.identity(self.catalog), "selected_ids": [case["id"] for case in cases],
                    **allocation}
        manifest["manifest_sha256"] = catalog.digest(manifest)
        chosen = runner.replay(self.catalog, manifest, 1)
        self.assertEqual(2, len(chosen))
        self.assertTrue(all(entry["repeats"] == 3 for entry in chosen))
        with self.assertRaisesRegex(ValueError, "different checkout"):
            runner.replay(dict(self.catalog, code_sha="different"), manifest, 1)
        chosen[0]["rows"] = 11
        with self.assertRaisesRegex(ValueError, "digest"):
            runner.replay(self.catalog, manifest, 1)
        unavailable = runner.allocate(entries, {cases[0]["id"]: 25}, 2, 10, 20, 3)
        self.assertEqual({"missing whole-case estimate", "exceeds available batch capacity"},
                         {entry["reason"] for entry in unavailable["unallocated"]})

    def test_mix_validation_rejects_mixed_supported_and_unsupported_cases(self):
        cases = runner.select(self.cases, [], [], "basic")
        args = Namespace(repeats=1, rows=32, backlog_events=20, rate=10,
                         warmup_seconds=1, measurement_seconds=2, timeout_seconds=45, mix=[100, 0, 0])
        with self.assertRaisesRegex(ValueError, "does not admit"):
            runner.checked_entries(runner.make_entries(cases, args))
        self.assertEqual([100, 0, 0], runner.mix("100/0/0"))
        with self.assertRaisesRegex(ArgumentTypeError, "summing to 100"):
            runner.mix("99/0/0")

    def test_operator_suite_lists_blocked_definitions_and_preserves_scope(self):
        cases = runner.select(self.cases, [], [], "operators")
        ids = {case["id"] for case in cases}
        self.assertEqual(51, len(ids))
        self.assertNotIn("join.live.hot-update.wide", ids)
        self.assertNotIn("copy.live.slow-target.large", ids)
        blocked = {case["id"] for case in cases if runner.case_listing(case)["execution_status"] == "blocked"}
        self.assertEqual(8, len(blocked))
        self.assertIn("js.live.basic.standard", blocked)
        self.assertIn("mixed.snapshot.union-branches.standard", blocked)
        self.assertIn("mixed.snapshot.double-map.standard", blocked)
