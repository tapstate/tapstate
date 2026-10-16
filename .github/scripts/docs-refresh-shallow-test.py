"""Post-publication documentation refresh must retain the frozen release range."""

import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import tempfile
import textwrap
import unittest


ROOT = Path(__file__).resolve().parents[2]
REPOSITORY = "tapstate/tapstate"
PR_URL = f"https://github.com/{REPOSITORY}/pull/"


class DocumentationRefreshShallowTest(unittest.TestCase):
    def test_satellite_refresh_carries_followups_from_shallow_release_checkout(self):
        workflow = (ROOT / ".github/workflows/release.yml").read_text()
        job = re.search(
            r"^  satellites:\n(.*?)(?=^  [\w-]+:\n|\Z)",
            workflow, re.MULTILINE | re.DOTALL,
        )
        self.assertIsNotNone(job, "Expected the post-publication satellite job")
        steps = re.findall(
            r"^      - (.*?)(?=^      - |\Z)",
            job.group(1), re.MULTILINE | re.DOTALL,
        )
        checkout = next(step for step in steps if "uses: actions/checkout@" in step)
        refresh = next(step for step in steps if ".github/scripts/docs-gate.sh" in step)
        settle = next(step for step in steps if ".github/scripts/docs-release.sh settle" in step)
        self.assertIn("needs: [version, publish]", job.group(1))
        self.assertIn("ref: ${{ needs.version.outputs.sha }}", checkout)
        self.assertIn("continue-on-error: true", refresh)
        self.assertIn("--report-only", refresh)
        self.assertNotIn("--bump", refresh)
        depth = re.search(r"^          fetch-depth: (\d+)\s*$", checkout, re.MULTILINE)
        depth = int(depth.group(1)) if depth else 1

        with tempfile.TemporaryDirectory(prefix="docs-refresh-shallow-") as directory:
            scratch = Path(directory)
            source = scratch / "source"
            source.mkdir()
            scripts = source / ".github/scripts"
            scripts.mkdir(parents=True)
            for name in (
                "docs-gate.sh", "docs-release.sh", "docs-impact.sh",
                "_pr-read.sh", "_pr-section.sh",
            ):
                shutil.copy2(ROOT / ".github/scripts" / name, scripts / name)
            bin_dir = scratch / "bin"
            bin_dir.mkdir()
            gh = bin_dir / "gh"
            gh.write_text(textwrap.dedent('''\
                #!/usr/bin/env python3
                import json
                import os
                from pathlib import Path
                import sys

                scratch = Path(os.environ["DOCS_REFRESH_FIXTURE"])
                args = sys.argv[1:]
                with (scratch / "requests.jsonl").open("a") as trace:
                    trace.write(json.dumps(args) + "\\n")
                if args[:2] == ["pr", "view"] and args[2] in ("501", "502", "503"):
                    print(json.dumps({
                        "body": "## Documentation impact\\n\\n"
                                "- **Draft in this repository:** docs/feature.md\\n"
                                "- **Public page it is headed for:** https://tapstate.dev/docs/feature\\n",
                        "labels": [{"name": "docs-needed"}],
                        "author": {"login": "contributor"},
                        "url": "https://github.com/tapstate/tapstate/pull/" + args[2],
                    }))
                elif args[:2] == ["issue", "list"]:
                    search = args[args.index("--search") + 1]
                    if "/pull/" in search:
                        number = int(search.rsplit("/", 1)[1])
                        state = "CLOSED" if number == 502 and os.environ["REFRESH_PHASE"] == "after" else "OPEN"
                        print(json.dumps([{"number": number + 1000, "state": state, "body": search}]))
                    else:
                        print(json.dumps([{"number": 70, "state": "OPEN",
                                           "title": "Release 0.7.0: publish the documentation site"}]))
                elif args[:2] == ["issue", "create"]:
                    body = args[args.index("--body") + 1]
                    (scratch / "settled-request.md").write_text(body)
                    print("https://github.com/tapstate/docs/issues/71")
                elif args[:2] == ["issue", "edit"]:
                    pass
                else:
                    with (scratch / "stub-errors").open("a") as errors:
                        errors.write(repr(args) + "\\n")
                    sys.exit(2)
            '''))
            gh.chmod(0o755)
            # Hooks can export repository selectors; none may escape into the fixture.
            env = {key: value for key, value in os.environ.items() if not key.startswith("GIT_")}
            env.update(
                PATH=f"{bin_dir}{os.pathsep}{os.environ['PATH']}",
                GIT_CONFIG_NOSYSTEM="1", GIT_CONFIG_GLOBAL=os.devnull,
                GIT_TERMINAL_PROMPT="0", GH_TOKEN="fixture-token",
                GITHUB_REPOSITORY=REPOSITORY, DOCS_REPO="tapstate/docs",
                DOCS_FOLLOWUP_REPO="tapstate/docs", DOCS_OWNER="docs-owner",
                DOCS_REFRESH_FIXTURE=str(scratch), REFRESH_PHASE="before",
                RUNNER_TEMP=str(scratch), GITHUB_STEP_SUMMARY=str(scratch / "summary"),
            )

            def run(command, cwd=source, check=True):
                result = subprocess.run(
                    command, cwd=cwd, env=env, text=True,
                    capture_output=True, timeout=30,
                )
                if check:
                    self.assertEqual(0, result.returncode,
                                     f"Fixture command failed: {command}\n{result.stdout}{result.stderr}")
                return result

            def commit(message):
                run(["git", "add", "."])
                run(["git", "commit", "--quiet", "--allow-empty", "-m", message])

            run(["git", "init", "--quiet", "--initial-branch=main"])
            run(["git", "config", "user.name", "Test"])
            run(["git", "config", "user.email", "test@example.com"])
            commit("Previous release")
            run(["git", "tag", "v0.6.0"])
            commit("Document the first feature (#501)")
            commit("Document the second feature (#502)")
            commit("Freeze the release")
            sha = run(["git", "rev-parse", "HEAD"]).stdout.strip()
            # A newer main must not add work outside the frozen release range.
            commit("A later feature outside this release (#503)")
            initial = scratch / "pre-publication.tsv"
            run(["bash", ".github/scripts/docs-gate.sh", "--base", "v0.6.0",
                 "--sha", sha, "--bump", "minor", "--list-open", str(initial)])
            self.assertEqual(
                f"501\t1501\t{PR_URL}501\n502\t1502\t{PR_URL}502\n",
                initial.read_text(), "The full-history minor gate must allow and list open followups",
            )

            checkout_dir = scratch / "checkout"
            checkout_dir.mkdir()
            run(["git", "init", "--quiet", "--initial-branch=main"], cwd=checkout_dir)
            run(["git", "remote", "add", "origin", source.as_uri()], cwd=checkout_dir)
            # Use the file transport: a local clone can silently ignore --depth.
            run(["git", "fetch", "--quiet", "--no-tags", "--depth=1", "origin", sha], cwd=checkout_dir)
            run(["git", "checkout", "--quiet", "--detach", "FETCH_HEAD"], cwd=checkout_dir)
            self.assertEqual("true", run(["git", "rev-parse", "--is-shallow-repository"],
                                         cwd=checkout_dir).stdout.strip())
            self.assertNotEqual(0, run(["git", "rev-parse", "--verify", "refs/tags/v0.6.0"],
                                       cwd=checkout_dir, check=False).returncode)
            # Honor the workflow's checkout depth, including a full-history checkout.
            if depth == 0:
                run(["git", "fetch", "--quiet", "--unshallow", "--tags", "origin"], cwd=checkout_dir)
            elif depth != 1:
                run(["git", "fetch", "--quiet", f"--depth={depth}", "origin", sha], cwd=checkout_dir)
            if re.search(r"^          fetch-tags: true\s*$", checkout, re.MULTILINE):
                run(["git", "fetch", "--quiet", "--tags", "origin"], cwd=checkout_dir)
            self.assertEqual(sha, run(["git", "rev-parse", "HEAD"], cwd=checkout_dir).stdout.strip())
            env["REFRESH_PHASE"] = "after"
            values = {
                "needs.version.outputs.base": "v0.6.0", "needs.version.outputs.sha": sha,
                "needs.version.outputs.version": "0.7.0", "needs.version.outputs.tag": "v0.7.0",
                "github.server_url": "https://github.com", "github.repository": REPOSITORY,
            }

            def step_command(step):
                body = step.split("        run: |\n", 1)[1]
                command = textwrap.dedent(body)
                for expression, value in values.items():
                    command = command.replace("${{ " + expression + " }}", value)
                self.assertNotIn("${{", command, "Unresolved workflow expression in fixture")
                return ["bash", "--noprofile", "--norc", "-e", "-c", command]

            # Replay history preparation, while leaving satellite publication outside this fixture.
            for step in steps[steps.index(checkout) + 1:steps.index(refresh)]:
                if "        run: |\n" in step and ".github/scripts/satellites.sh release" not in step:
                    run(step_command(step), cwd=checkout_dir)
            report = run(step_command(refresh), cwd=checkout_dir, check=False)
            # continue-on-error still lets settlement run after a failed refresh.
            settlement = run(step_command(settle), cwd=checkout_dir)
            errors = scratch / "stub-errors"
            self.assertFalse(errors.exists(), errors.read_text() if errors.exists() else "")
            request = (scratch / "settled-request.md").read_text()
            refreshed = scratch / "docs-open.tsv"
            self.assertEqual(
                f"501\t1501\t{PR_URL}501\n",
                refreshed.read_text() if refreshed.exists() else None,
                "Satellite refresh lost the still-open followup from before the shallow tip.\n"
                f"Refresh exit {report.returncode}:\n{report.stdout}{report.stderr}"
                f"Settlement exit {settlement.returncode}:\n{settlement.stdout}{settlement.stderr}",
            )
            self.assertEqual(0, report.returncode, report.stdout + report.stderr)
            self.assertIn(f"- #1501 -- {PR_URL}501", request)
            self.assertNotIn("#1502", request, "A followup closed since the minor gate must disappear")
            self.assertNotIn("#1503", request, "Later main commits are outside this release")


if __name__ == "__main__":
    unittest.main()
