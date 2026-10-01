"""A pending main push must wait before its aggregate checks appear, too."""

import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[2]
SHA = "1199a31168af8be189c999b289d21f5f051b8b1a"
HEAD_SHA = "7d3a99ab106cb292600764bb2c6a71dd30eb9f23"
TREE = "6eb178a854e93f67d0680db1a7f31bf60679c978"
REPO = "repos/tapstate/tapstate"


def check(name, status="completed", conclusion="success"):
    return {
        "name": name,
        "status": status,
        "conclusion": conclusion,
        "started_at": "2026-09-28T02:09:03Z",
    }


class PendingPushChecksTest(unittest.TestCase):
    def test_pin_waits_before_the_push_build_check_exists(self):
        with tempfile.TemporaryDirectory() as directory:
            scratch = Path(directory)
            # At pin time the push's shards are running; build and sonarqube
            # have no check-run yet. The merged PR's identical tree is green.
            push_checks = [check("test-shards (e2e-2)", "in_progress", None)]
            pending_run = {
                "id": 36368690033,
                "name": "CI",
                "path": ".github/workflows/ci.yml",
                "head_sha": SHA,
                "head_branch": "main",
                "event": "push",
                "status": "in_progress",
                "conclusion": None,
            }
            fixtures = {
                f"{REPO}/rules/branches/main": [{
                    "type": "required_status_checks",
                    "parameters": {"required_status_checks": [
                        {"context": name} for name in ("build", "sonarqube", "dco")
                    ]},
                }],
                f"{REPO}/commits/{SHA}/check-runs": {
                    "total_count": len(push_checks), "check_runs": push_checks,
                },
                f"{REPO}/commits/{HEAD_SHA}/check-runs": {
                    "total_count": 3,
                    "check_runs": [check(name) for name in ("build", "sonarqube", "dco")],
                },
                f"{REPO}/commits/{SHA}/pulls": [{
                    "number": 531, "head": {"sha": HEAD_SHA},
                    "merge_commit_sha": SHA,
                }],
                f"{REPO}/commits/{SHA}": {"commit": {"tree": {"sha": TREE}}},
                f"{REPO}/commits/{HEAD_SHA}": {"commit": {"tree": {"sha": TREE}}},
                f"{REPO}/actions/runs": {"total_count": 1, "workflow_runs": [pending_run]},
                f"{REPO}/actions/workflows/ci.yml/runs": {
                    "total_count": 1, "workflow_runs": [pending_run],
                },
            }
            fixture_file = scratch / "responses.json"
            fixture_file.write_text(json.dumps(fixtures))
            gh = scratch / "gh"
            # Only the GitHub boundary is stubbed. Apply the real jq queries
            # to JSON responses, and record unexpected requests as setup errors.
            gh.write_text("""#!/usr/bin/env python3
import json
import os
from pathlib import Path
import subprocess
import sys

scratch = Path(os.environ['CHECKS_FIXTURE'])
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
                CHECKS_FIXTURE=str(scratch),
            )
            command = [
                "bash", str(ROOT / ".github/scripts/checks-on-commit.sh"),
                "--sha", SHA, "--from-ruleset", "main",
            ]
            pin = subprocess.run(
                command, cwd=ROOT, env=env, text=True, capture_output=True, timeout=15,
            )
            # At gate 1 the shards have finished and build has appeared. The
            # same push still has no completed verdict for the required names.
            push_checks[0].update(status="completed", conclusion="success")
            build = check("build", "in_progress", None)
            build["started_at"] = "2026-09-28T02:32:40Z"
            push_checks.append(build)
            fixtures[f"{REPO}/commits/{SHA}/check-runs"]["total_count"] = len(push_checks)
            fixture_file.write_text(json.dumps(fixtures))
            gate_one = subprocess.run(
                command, cwd=ROOT, env=env, text=True, capture_output=True, timeout=15,
            )
            errors = scratch / "errors"
            self.assertFalse(errors.exists(), errors.read_text() if errors.exists() else "")
            self.assertIn(f"{REPO}/commits/{SHA}/check-runs", (scratch / "requests").read_text())
            self.assertEqual(
                (3, 3), (pin.returncode, gate_one.returncode),
                "Both release gates must wait for the pending push on the pinned commit.\n"
                f"Pin before build appears (exit {pin.returncode}):\n{pin.stdout}{pin.stderr}"
                f"Gate 1 after build appears (exit {gate_one.returncode}):\n"
                f"{gate_one.stdout}{gate_one.stderr}",
            )


if __name__ == "__main__":
    unittest.main()
