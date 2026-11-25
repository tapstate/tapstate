"""A successful long catalog rebuild must leave its PR branch authenticated."""

import base64
import errno
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import os
from pathlib import Path
import re
import subprocess
import tempfile
import threading
import unittest


ROOT = Path(__file__).resolve().parents[2]
APP_TOKEN_LIFETIME = 3600
# Run 37434313592 minted at 08:10:15 and pushed at 09:10:25. Its successful
# rebuild alone took one hour; the other ten seconds were setup and publication.
MINT_TO_PUSH_SECONDS = 3610


def scalar(step, key):
    match = re.search(rf"^\s+{re.escape(key)}:\s*(.+)$", step, re.M)
    return match.group(1).strip().strip("\"'") if match else ""


def publication_credential():
    workflow = (ROOT / ".github/workflows/catalog-refresh.yml").read_text()
    job = re.search(r"^  refresh:\n(.*?)(?=^  [\w-]+:|\Z)", workflow, re.M | re.S)
    if not job:
        raise ValueError("missing catalog refresh job")
    steps = re.split(r"^      - ", job.group(1), flags=re.M)[1:]
    outputs = {}
    elapsed = 0
    rebuilt = False
    for step in steps:
        # The split removes the list marker and indentation from the first key.
        step = "        " + step
        action = scalar(step, "uses")
        if action.startswith("actions/create-github-app-token@"):
            step_id = scalar(step, "id")
            if not step_id:
                raise ValueError("app token step has no output ID")
            outputs[step_id] = (f"fixture-app-token-{step_id}", elapsed)
        elif re.search(r"^\s+run:.*scripts/refresh-catalog\.sh\b", step, re.M):
            elapsed += MINT_TO_PUSH_SECONDS
            rebuilt = True
        elif action.startswith("peter-evans/create-pull-request@"):
            if not rebuilt:
                raise ValueError("publication did not follow the catalog rebuild")
            # create-pull-request v7 defaults branch-token to its token input.
            credential = scalar(step, "branch-token") or scalar(step, "token")
            reference = re.fullmatch(
                r"\$\{\{\s*steps\.([\w-]+)\.outputs\.token\s*\}\}", credential,
            )
            if not reference or reference.group(1) not in outputs:
                raise ValueError(f"unmodeled publication credential: {credential}")
            token, minted_at = outputs[reference.group(1)]
            return scalar(step, "branch"), token, minted_at, elapsed
    raise ValueError("missing catalog publication action")


class CatalogRefreshPushAuthTest(unittest.TestCase):
    def test_hour_long_rebuild_keeps_pr_branch_push_authenticated(self):
        branch, token, minted_at, push_at = publication_credential()
        self.assertTrue(branch.startswith("ws/"), "expected a workstream PR branch")
        authorization = "Basic " + base64.b64encode(
            f"x-access-token:{token}".encode(),
        ).decode()

        with tempfile.TemporaryDirectory(prefix="catalog-push-auth-") as directory:
            scratch = Path(directory)
            # Do not inherit credentials, proxies, hooks or repository selectors.
            env = {
                "PATH": os.environ["PATH"],
                "HOME": str(scratch),
                "XDG_CONFIG_HOME": str(scratch),
                "GIT_CONFIG_NOSYSTEM": "1",
                "GIT_CONFIG_GLOBAL": os.devnull,
                "GIT_AUTHOR_NAME": "Catalog fixture",
                "GIT_AUTHOR_EMAIL": "catalog@example.invalid",
                "GIT_COMMITTER_NAME": "Catalog fixture",
                "GIT_COMMITTER_EMAIL": "catalog@example.invalid",
                "GIT_TERMINAL_PROMPT": "1",
                "LC_ALL": "C",
            }

            def git(*args, check=True):
                return subprocess.run(
                    ["git", "-c", "credential.helper=", "-c", "http.proxy=", *args],
                    cwd=scratch, env=env, stdin=subprocess.DEVNULL,
                    text=True, capture_output=True, check=check, timeout=15,
                    start_new_session=True,
                )

            work = scratch / "work"
            remote = scratch / "remote.git"
            git("init", "--initial-branch=main", str(work))
            catalog = work / "catalog.txt"
            catalog.write_text("before refresh\n")
            git("-C", str(work), "add", "catalog.txt")
            git("-C", str(work), "commit", "-m", "Catalog baseline fixture")
            # Seed the remote by cloning, without publishing any branch.
            git("clone", "--bare", str(work), str(remote))
            refs_before = git("-C", str(remote), "show-ref").stdout
            git("-C", str(work), "checkout", "-b", branch)
            catalog.write_text("after successful refresh\n")
            git("-C", str(work), "commit", "-am", "Catalog refresh fixture")
            self.assertEqual(
                "1", git("-C", str(work), "rev-list", "--count", f"main..{branch}").stdout.strip(),
                "the refresh must have committed before testing publication",
            )

            # The Git wire advertisement is real. Only GitHub's expiring-token
            # boundary and elapsed time are modeled; no real token is needed.
            advertisement = b"001f# service=git-receive-pack\n0000" + subprocess.run(
                ["git", "receive-pack", "--stateless-rpc", "--advertise-refs", str(remote)],
                env=env, capture_output=True, check=True, timeout=15,
            ).stdout
            clock = [minted_at]
            requests = []
            unexpected = []

            class Handler(BaseHTTPRequestHandler):
                def do_GET(self):
                    header = self.headers.get("Authorization", "")
                    if self.path != "/tapstate.git/info/refs?service=git-receive-pack":
                        unexpected.append(self.path)
                        self.send_error(404)
                        return
                    valid = header == authorization and clock[0] < minted_at + APP_TOKEN_LIFETIME
                    requests.append((header, 200 if valid else 401))
                    self.send_response(200 if valid else 401)
                    if valid:
                        self.send_header("Content-Type", "application/x-git-receive-pack-advertisement")
                    else:
                        self.send_header("WWW-Authenticate", 'Basic realm="catalog-refresh"')
                    body = advertisement if valid else b""
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)

                def do_POST(self):
                    unexpected.append(f"POST {self.path}")
                    self.send_error(405, "only read-only push discovery is allowed")

                def log_message(self, *args):
                    pass

            with ThreadingHTTPServer(("127.0.0.1", 0), Handler) as server:
                thread = threading.Thread(target=server.serve_forever, daemon=True)
                thread.start()
                try:
                    origin = f"http://127.0.0.1:{server.server_port}"
                    git("-C", str(work), "remote", "add", "origin", f"{origin}/tapstate.git")
                    git("-C", str(work), "config", f"http.{origin}/.extraheader", f"AUTHORIZATION: {authorization}")
                    command = (
                        "-C", str(work), "push", "--dry-run", "--force-with-lease",
                        "origin", f"{branch}:refs/heads/{branch}",
                    )
                    # Prove the same Git transport and commit work while this
                    # credential is fresh, before advancing the logical clock.
                    fresh = git(*command, check=False)
                    self.assertEqual(0, fresh.returncode, f"fresh-token prerequisite failed: {fresh.stderr}")
                    requests.clear()
                    clock[0] = push_at
                    result = git(*command, check=False)
                finally:
                    server.shutdown()
                    thread.join(timeout=5)

            self.assertEqual([], unexpected, "dry-run must not transmit a branch update")
            self.assertEqual(refs_before, git("-C", str(remote), "show-ref").stdout)
            self.assertTrue(requests, "Git must reach the authentication boundary")
            self.assertEqual(authorization, requests[0][0], "Git must send the workflow's publication token")
            if result.returncode:
                self.assertEqual(128, result.returncode, result.stderr)
                self.assertIn((authorization, 401), requests)
                self.assertIn("could not read Username", result.stderr)
                self.assertIn(os.strerror(errno.ENXIO), result.stderr)
            self.assertEqual(
                0, result.returncode,
                f"The catalog has one committed change, but its PR branch cannot authenticate "
                f"after the successful rebuild: the workflow's token is {push_at - minted_at}s old "
                f"and expires after {APP_TOKEN_LIFETIME}s.\n{result.stderr}",
            )


if __name__ == "__main__":
    unittest.main()
