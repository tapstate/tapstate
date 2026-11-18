"""Exercise PR quality-gate delivery with an oversized Sonar issue list."""

import json
import os
from pathlib import Path
import re
import subprocess
import tempfile
from textwrap import dedent
import unittest


WORKFLOW = Path(__file__).resolve().parents[1] / "workflows/ci.yml"
COMMENT_LIMIT = 65_536
ISSUE_COUNT = 1_201
PAGE_SIZE = 100
HEAD = "a" * 40


class SonarPrCommentSizeTest(unittest.TestCase):
    def test_large_issue_list_delivers_a_comment_within_the_limit(self):
        step = re.search(
            r"^      - name: Comment quality gate on PR\n"
            r"(?:^        .*\n)*?^        run: \|\n((?:^          .*\n|^\n)+)",
            WORKFLOW.read_text(encoding="utf-8"), re.M,
        )
        self.assertIsNotNone(step, "workflow comment step was not found")

        with tempfile.TemporaryDirectory() as temp:
            scratch = Path(temp)
            # The full render exceeds both Linux's single-argument limit and
            # the aggregate exec limit on macOS, where this probe also runs.
            for offset in range(0, ISSUE_COUNT, PAGE_SIZE):
                page = offset // PAGE_SIZE + 1
                issues = [
                    {
                        "rule": f"java:S{1000 + index}",
                        "severity": "MAJOR",
                        "component": "tapstate:runtime/srs/LargeAnalysis.java",
                        "line": index + 1,
                        "message": "Review the related validation and error handling. " * 22,
                    }
                    for index in range(offset, min(offset + PAGE_SIZE, ISSUE_COUNT))
                ]
                (scratch / f"page-{page}.json").write_text(json.dumps({
                    "paging": {"pageIndex": page, "pageSize": PAGE_SIZE, "total": ISSUE_COUNT},
                    "issues": issues,
                }), encoding="utf-8")

            curl = scratch / "curl"
            curl.write_text(r"""#!/usr/bin/env bash
url="${@: -1}"
case "$url" in
  */api/qualitygates/project_status\?*)
    printf '%s' '{"projectStatus":{"status":"ERROR","conditions":[{"status":"ERROR","metricKey":"new_reliability_rating","actualValue":"3","comparator":"GT","errorThreshold":"1"}]}}'
    ;;
  */api/issues/search\?*\&p=*) cat "page-${url##*&p=}.json" ;;
  *) exit 2 ;;
esac
""", encoding="utf-8")
            curl.chmod(0o755)

            gh = scratch / "gh"
            gh.write_text(f"""#!/usr/bin/env python3
import json
from pathlib import Path
import sys

def read_source(source):
    return sys.stdin.read() if source == '-' else Path(source).read_text(encoding='utf-8')

args = sys.argv[1:]
if args == ['api', '--method', 'GET', 'repos/fixture/repo/pulls/944', '--jq', '.head.sha']:
    print('{HEAD}')
    sys.exit(0)
if args[:3] == ['api', '--paginate', 'repos/fixture/repo/issues/944/comments']:
    sys.exit(0)
if args[:4] not in (['api', '-X', 'POST', 'repos/fixture/repo/issues/944/comments'],
                    ['api', '--method', 'POST', 'repos/fixture/repo/issues/944/comments']):
    sys.exit(f'Unexpected GitHub invocation: {{args[:4]}}')

body = None
for flag, value in zip(args[4:], args[5:]):
    if flag == '--input':
        body = json.loads(read_source(value))['body']
    elif flag in ('-F', '--field') and value.startswith('body=@'):
        body = read_source(value[len('body=@'):])
    elif flag in ('-f', '--raw-field') and value.startswith('body='):
        body = value[len('body='):]
if not isinstance(body, str) or not body:
    sys.exit('The GitHub request did not include a comment body')
if len(body) > {COMMENT_LIMIT}:
    sys.exit(f'HTTP 422: comment body has {{len(body)}} characters (maximum is {COMMENT_LIMIT})')
Path('accepted-comment.md').write_text(body, encoding='utf-8')
""", encoding="utf-8")
            gh.chmod(0o755)

            result = subprocess.run(
                ["bash", "-e", "-c", dedent(step[1])], cwd=scratch,
                env={
                    "PATH": f"{scratch}{os.pathsep}{os.environ['PATH']}",
                    **{key: os.environ.get(key, temp) for key in ("TMPDIR", "TMP", "TEMP")},
                    "LC_ALL": "C",
                    "SONAR_HOST_URL": "https://sonar.example.invalid",
                    "SONAR_TOKEN": "fixture-token",
                    "GH_TOKEN": "fixture-token",
                    "GITHUB_REPOSITORY": "fixture/repo",
                    "GITHUB_RUN_ID": "123",
                    "GITHUB_RUN_ATTEMPT": "1",
                    "PR_NUMBER": "944",
                    "EXPECTED_HEAD_SHA": HEAD,
                },
                capture_output=True, encoding="utf-8", timeout=30,
            )
            accepted = scratch / "accepted-comment.md"
            if result.returncode == 126 and "Argument list too long" in result.stderr:
                rendered = (scratch / "comment.md").read_text(encoding="utf-8")
                self.assertGreater(len(rendered), COMMENT_LIMIT)
                self.assertIn(f"java:S{1000 + ISSUE_COUNT - 1}", rendered)
                self.assertFalse(accepted.exists())
                self.fail(f"The {len(rendered)}-character comment never reached gh "
                          f"(workflow exit {result.returncode}): {result.stderr.strip()}")

            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            self.assertTrue(accepted.exists(), "no quality-gate comment was delivered")
            body = accepted.read_text(encoding="utf-8")
            self.assertIn("<!-- sonarqube-gate -->", body)
            self.assertIn("new_reliability_rating", body)


if __name__ == "__main__":
    unittest.main()
