from datetime import datetime
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
import actions_run


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
        with patch.object(actions_run.time, "time", return_value=started + 3600):
            self.assertEqual(21000, actions_run.remaining_seconds(timing, reserve=600))
