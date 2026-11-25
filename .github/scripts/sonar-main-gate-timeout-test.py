"""Exercise the main-branch scanner when the gate finishes after its default wait."""

import os
from pathlib import Path
import re
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[2]


def block(value, start, boundary):
    found = re.search(start, value, re.M)
    if not found:
        raise AssertionError(f"missing workflow block: {start}")
    remainder = value[found.end():]
    end = re.search(boundary, remainder, re.M)
    return remainder[:end.start()] if end else remainder


def analyze_command():
    workflow = (ROOT / ".github/workflows/ci.yml").read_text()
    build = block(workflow, r"^  build:\n", r"^  [\w-]+:")
    analyze = block(build, r"^      - name: Analyze admitted coverage\n", r"^      - ")
    command = block(analyze, r"^        run: \|\n", r"^ {0,9}\S")
    return "\n".join(line[10:] for line in command.splitlines())


class MainGateTimeoutTest(unittest.TestCase):
    def test_main_gate_ok_just_after_default_scanner_wait(self):
        # SonarScanner waits 300 seconds by default. Model a gate that turns OK
        # at logical second 301 without making the probe wait in real time.
        with tempfile.TemporaryDirectory() as temp:
            temp_path = Path(temp)
            fixture_env = {key: value for key, value in os.environ.items() if key not in {
                'GIT_DIR', 'GIT_WORK_TREE', 'GIT_INDEX_FILE', 'GIT_PREFIX', 'GIT_QUARANTINE_PATH'}}
            checkout = temp_path / 'checkout'
            subprocess.run(['git', 'init', '--quiet', str(checkout)], env=fixture_env, check=True)
            subprocess.run(['git', '-C', str(checkout), '-c', 'user.name=Fixture',
                            '-c', 'user.email=fixture@example.invalid', '-c', 'commit.gpgSign=false',
                            '-c', 'core.hooksPath=/dev/null', 'commit', '--allow-empty',
                            '--quiet', '-m', 'Fixture main revision'], env=fixture_env, check=True)
            head = subprocess.check_output(['git', '-C', str(checkout), 'rev-parse', 'HEAD'],
                                           env=fixture_env, text=True).strip()
            api_trace = temp_path / 'github-api-trace'
            github = temp_path / 'gh'
            github.write_text("""#!/usr/bin/env python3
import os
from pathlib import Path
import sys

expected = ['api', '--method', 'GET', 'repos/fixture/repo/git/ref/heads/main', '--jq', '.object.sha']
if sys.argv[1:] != expected:
    sys.exit('unexpected GitHub API invocation')
Path(os.environ['GITHUB_API_TRACE']).write_text('main ref checked\\n')
print(os.environ['FIXTURE_HEAD'])
""")
            github.chmod(0o755)
            trace = temp_path / "scanner-trace"
            maven = temp_path / "mvn"
            maven.write_text("""#!/usr/bin/env python3
import os
from pathlib import Path
import sys

args = sys.argv[1:]
if 'sonar:sonar' not in args or '-Dsonar.projectKey=tapstate' not in args:
    print('unexpected scanner invocation', file=sys.stderr)
    sys.exit(2)
wait = False
timeout = 300
for arg in args:
    if arg == '-Dsonar.qualitygate.wait=true':
        wait = True
    elif arg.startswith('-Dsonar.qualitygate.timeout='):
        timeout = int(arg.split('=', 1)[1])
trace = Path(os.environ['SCANNER_TRACE'])
trace.write_text('analysis submitted for main\\n')
if wait:
    if timeout < 301:
        print('[ERROR] Failed to execute goal org.sonarsource.scanner.maven:sonar-maven-plugin:3.11.0.3922:sonar (default-cli) on project tapstate: Quality Gate check timeout exceeded', file=sys.stderr)
        sys.exit(1)
    trace.write_text(trace.read_text() + 'current analysis quality gate checked: OK\\n')
""")
            maven.chmod(0o755)
            env = dict(
                fixture_env,
                PATH=f"{temp}{os.pathsep}{os.environ['PATH']}",
                FIXTURE_HEAD=head,
                GITHUB_API_TRACE=str(api_trace),
                SCANNER_TRACE=str(trace),
                RUNNER_TEMP=temp,
                SONAR_TOKEN="fixture-token",
                SONAR_HOST_URL="https://sonar.example.invalid",
                GITHUB_REF_TYPE="branch",
                GITHUB_REF_NAME="main",
                GITHUB_REF="refs/heads/main",
                GITHUB_EVENT_NAME="push",
                GITHUB_SHA=head,
                GITHUB_REPOSITORY="fixture/repo",
                GH_TOKEN="fixture-token",
                GITHUB_TOKEN="fixture-token",
            )
            result = subprocess.run(
                ["bash", "-e", "-c", analyze_command()],
                cwd=checkout,
                env=env,
                text=True,
                capture_output=True,
                timeout=15,
            )
            self.assertTrue(api_trace.exists(), f"main ref was not checked: {result.stdout} {result.stderr}")
            self.assertEqual('main ref checked\n', api_trace.read_text())
            self.assertTrue(trace.exists(), f"scanner was not invoked: {result.stderr}")
            self.assertIn("analysis submitted for main", trace.read_text())
            if result.returncode:
                self.assertIn("Quality Gate check timeout exceeded", result.stderr)
            self.assertEqual(
                0,
                result.returncode,
                "the current analysis reaches SonarQube and its gate becomes OK, "
                f"but the scanner exits before the verdict: {result.stderr}",
            )
            self.assertIn("current analysis quality gate checked: OK", trace.read_text())


if __name__ == "__main__":
    unittest.main()
