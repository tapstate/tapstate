"""Gate 8 must wait for the lane after its dispatcher has completed."""

import json
import os
from pathlib import Path
import re
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[2]
SHA = "b52b528bde6566f84989a55827867c22714f19dd"
RELEASE_RUN = "36406591740"


def workflow_jobs(filename):
    workflow = (ROOT / ".github/workflows" / filename).read_text()
    jobs = workflow.split("\njobs:\n", 1)[1]
    return dict(re.findall(
        r"^  ([\w-]+):\n(.*?)(?=^  [\w-]+:\n|\Z)",
        jobs, re.MULTILINE | re.DOTALL,
    ))


def check_name(job_id, body):
    name = re.search(r"^    name: (.+)$", body, re.MULTILINE)
    return name.group(1).strip().strip("\"'") if name else job_id


class JoinPerformanceReleaseGateTest(unittest.TestCase):
    def test_gate_waits_for_lane_after_dispatcher_succeeds(self):
        release_jobs = workflow_jobs("release.yml")
        dispatchers = [
            check_name(job_id, body) for job_id, body in release_jobs.items()
            if "gh workflow run join-perf.yml" in body
        ]
        lanes = [
            check_name(job_id, body)
            for job_id, body in workflow_jobs("join-perf.yml").items()
            if "-Dtest=JoinPerformanceGateTest" in body
        ]
        self.assertEqual(1, len(dispatchers), "Expected one join performance dispatcher")
        self.assertEqual(1, len(lanes), "Expected one join performance lane")

        gate_eight = release_jobs["gates"].split(
            "      - name: 8. the join performance lane is green on this commit\n", 1,
        )[1].split("\n      - name:", 1)[0]
        reader = re.search(
            r"^\s+(\.github/scripts/checks-on-commit\.sh [^\n]+?) && break$",
            gate_eight, re.MULTILINE,
        )
        self.assertIsNotNone(reader, "Expected Gate 8's check-reader invocation")
        command = reader.group(1)
        for expression, value in {
            "${{ needs.version.outputs.sha }}": SHA,
            "${{ github.run_id }}": RELEASE_RUN,
            "${{ github.repository }}": "tapstate/tapstate",
        }.items():
            command = command.replace(expression, value)

        # Snapshot at 10:01:19 in release run 36406591740: the dispatcher
        # finished at 09:57:07, but the lane did not finish until 10:11:18.
        # Read names from the workflows so renaming the dispatcher removes
        # the collision without changing this expectation.
        checks = [
            {
                "id": 108877071788,
                "name": lanes[0],
                "head_sha": SHA,
                "status": "in_progress",
                "conclusion": None,
                "started_at": "2026-09-28T09:57:08Z",
                "completed_at": None,
                "details_url": "https://github.com/tapstate/tapstate/actions/runs/36406705656/job/108877071788",
                "check_suite": {"id": 98575333575},
            },
            {
                "id": 108877016817,
                "name": dispatchers[0],
                "head_sha": SHA,
                "status": "completed",
                "conclusion": "success",
                "started_at": "2026-09-28T09:56:59Z",
                "completed_at": "2026-09-28T09:57:07Z",
                "details_url": f"https://github.com/tapstate/tapstate/actions/runs/{RELEASE_RUN}/job/108877016817",
                "check_suite": {"id": 98575022051},
            },
        ]
        endpoint = f"repos/tapstate/tapstate/commits/{SHA}/check-runs"
        with tempfile.TemporaryDirectory() as directory:
            scratch = Path(directory)
            (scratch / "responses.json").write_text(json.dumps({
                endpoint: {"total_count": len(checks), "check_runs": checks},
            }))
            # Stub only GitHub; execute the real reader and its real jq query.
            gh = scratch / "gh"
            gh.write_text("""#!/usr/bin/env python3
import json
import os
from pathlib import Path
import subprocess
import sys

scratch = Path(os.environ['JOIN_PERF_CHECKS_FIXTURE'])
args = sys.argv[1:]
try:
    if not args or args[0] != 'api':
        raise ValueError(f'unexpected gh command: {args}')
    endpoint = next(arg for arg in args if arg.startswith('repos/')).split('?', 1)[0]
    payload = json.loads((scratch / 'responses.json').read_text())[endpoint]
    with (scratch / 'requests').open('a') as trace:
        trace.write(endpoint + '\\n')
    if '--jq' in args:
        query = args[args.index('--jq') + 1]
        result = subprocess.run(['jq', '-r', query], input=json.dumps(payload),
                                text=True, capture_output=True, check=True)
        sys.stdout.write(result.stdout)
    else:
        print(json.dumps(payload))
except Exception as error:
    with (scratch / 'errors').open('a') as errors:
        errors.write(str(error) + '\\n')
    print(error, file=sys.stderr)
    sys.exit(2)
""")
            gh.chmod(0o755)
            env = dict(
                os.environ,
                PATH=f"{scratch}{os.pathsep}{os.environ['PATH']}",
                GITHUB_REPOSITORY="tapstate/tapstate",
                GITHUB_RUN_ID=RELEASE_RUN,
                GITHUB_SHA=SHA,
                JOIN_PERF_CHECKS_FIXTURE=str(scratch),
            )
            gate = subprocess.run(
                ["bash", "-c", command], cwd=ROOT, env=env,
                text=True, capture_output=True, timeout=15,
            )
            errors = scratch / "errors"
            self.assertFalse(errors.exists(), errors.read_text() if errors.exists() else "")
            self.assertIn(endpoint, (scratch / "requests").read_text().splitlines())
            self.assertEqual(
                3, gate.returncode,
                "Gate 8 must wait for the running join performance lane; "
                "the release dispatcher's success is not the lane's verdict.\n"
                f"Command: {command}\n"
                f"Got exit {gate.returncode}:\n{gate.stdout}{gate.stderr}",
            )


if __name__ == "__main__":
    unittest.main()
