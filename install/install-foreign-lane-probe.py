#!/usr/bin/env python3
"""One read-only reproduction of unfenced fork installer lanes. Refs tapstate/tapstate#483.

The 2026-09-20 install steps bracket the stored events on all four platforms:
https://github.com/tmdbdd/tapstate/actions/runs/35536706274 (8 events)
https://github.com/heywalter/tapstate/actions/runs/35542455247 (8 events)
https://github.com/czy006/tapstate/actions/runs/35542464783 (8 events)

Read each fork's current workflow so fencing that lane can change the result.
Replay only its two installer one-liners, with the real checked-in installer.
All curl requests are intercepted; no install event reaches any network endpoint.
"""

import collections
import hashlib
import io
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tarfile
import tempfile
import unittest

import yaml


ROOT = Path(__file__).resolve().parents[1]
INSTALLER = ROOT / "install/install.sh"
LANES = ("tmdbdd/tapstate", "heywalter/tapstate", "czy006/tapstate")
PRODUCTION = "https://install.tapstate.dev/e"
ONE_LINER = "curl -sSL https://install.tapstate.dev/cli | sh"
STEP_NAMES = (
    "Install from the published script",
    "Re-run is an in-place upgrade, not an accretion",
)


def checked(command, **kwargs):
    result = subprocess.run(
        command, text=True, capture_output=True, timeout=30, **kwargs
    )
    if result.returncode:
        raise RuntimeError(
            f"Probe prerequisite failed: {command[0]} exited {result.returncode}\n"
            f"{result.stdout}{result.stderr}"
        )
    return result.stdout


def executable(destination, body):
    destination.write_text(body, encoding="utf-8")
    destination.chmod(0o755)


def release_asset(destination, version, platform):
    with tarfile.open(destination, "w:gz") as archive:
        for name in ("bin/tapstate", "libexec/tapstate-mcp"):
            data = f"#!/bin/sh\nprintf 'tapstate {version} {platform}\\n'\n".encode()
            entry = tarfile.TarInfo(f"tapstate-cli-{version}/{name}")
            entry.size, entry.mode = len(data), 0o755
            archive.addfile(entry, io.BytesIO(data))
    digest = hashlib.sha256(destination.read_bytes()).hexdigest()
    Path(str(destination) + ".sha256").write_text(
        f"{digest}  {destination.name}\n", encoding="ascii"
    )


class ForeignLaneProbe(unittest.TestCase):
    def test_fork_nightly_installs_do_not_post_to_production(self):
        version = re.search(
            r'^PINNED_VERSION="([^"]+)"$', INSTALLER.read_text(), re.MULTILINE
        ).group(1)
        leaks = collections.Counter()
        with tempfile.TemporaryDirectory(prefix="install-foreign-lane-") as directory:
            scratch = Path(directory)
            shim = scratch / "shim"
            shim.mkdir()
            requests = scratch / "requests.jsonl"
            executable(shim / "curl", f"#!{sys.executable}\n" + r'''
import json, os, pathlib, sys
args = sys.argv[1:]
url = args[-1] if '-X' in args else next(
    arg for arg in args if arg.startswith('https://')
)
if '-X' in args:
    if args[args.index('-X') + 1] != 'POST':
        raise RuntimeError('Unexpected HTTP method in the probe')
    record = {'url': url, 'payload': json.loads(args[args.index('-d') + 1])}
    with open(os.environ['PROBE_REQUESTS'], 'a') as output:
        output.write(json.dumps(record) + '\n')
elif url == 'https://install.tapstate.dev/cli':
    sys.stdout.buffer.write(pathlib.Path(os.environ['PROBE_INSTALLER']).read_bytes())
else:
    prefix = 'https://github.com/tapstate/tapstate/releases/download/v'
    if not url.startswith(prefix):
        raise RuntimeError('Unexpected download URL in the probe: ' + url)
    source = pathlib.Path(os.environ['PROBE_ASSETS']) / url.rsplit('/', 1)[-1]
    pathlib.Path(args[args.index('-o') + 1]).write_bytes(source.read_bytes())
''')
            executable(shim / "uname", """#!/bin/sh
case "$1" in
  -s) printf '%s\\n' "$PROBE_OS" ;;
  -m) printf '%s\\n' "$PROBE_ARCH" ;;
  *) exit 2 ;;
esac
""")
            executable(shim / "ldd", "#!/bin/sh\necho 'ldd (GNU libc) 2.39'\n")
            executable(shim / "sw_vers", "#!/bin/sh\necho 15.0\n")
            assets = scratch / "assets"
            assets.mkdir()
            (assets / "platform-minimums.txt").write_text("", encoding="ascii")
            for platform in ("darwin-arm64", "darwin-x64", "linux-arm64", "linux-x64"):
                release_asset(assets / f"tapstate-{version}-{platform}.tar.gz", version, platform)

            for repository in LANES:
                workflow = yaml.safe_load(checked([
                    "gh", "api", f"repos/{repository}/contents/.github/workflows/install-e2e.yml",
                    "-H", "Accept: application/vnd.github.raw+json",
                ]))
                job = workflow["jobs"]["install"]
                steps = [next(step for step in job["steps"] if step.get("name") == name)
                         for name in STEP_NAMES]
                # Do not execute arbitrary fetched shell text. These exact install commands
                # are the golden sample; an unfamiliar path needs a new diagnosis.
                for step in steps:
                    if ONE_LINER not in step["run"].splitlines():
                        raise RuntimeError(f"Installer command changed in {repository}: {step['name']}")
                for leg in job["strategy"]["matrix"]["include"]:
                    platform = leg["platform"]
                    install_dir = scratch / repository.split("/")[0] / platform
                    env = os.environ.copy()
                    # Caller settings must not supply a fence absent from the actual lane.
                    for key in list(env):
                        if key.startswith("TAPSTATE_") or key.startswith("GITHUB_"):
                            del env[key]
                    env.update({
                        "PATH": str(shim) + os.pathsep + env["PATH"],
                        "GITHUB_ACTIONS": "true",
                        "GITHUB_REPOSITORY": repository,
                        "GITHUB_EVENT_NAME": "schedule",
                        "PROBE_INSTALLER": str(INSTALLER),
                        "PROBE_ASSETS": str(assets),
                        "PROBE_REQUESTS": str(requests),
                        "PROBE_OS": "Darwin" if platform.startswith("darwin-") else "Linux",
                        "PROBE_ARCH": "arm64" if platform.endswith("-arm64") else "x86_64",
                    })
                    for step in steps:
                        effective = env.copy()
                        for scope in (workflow, job, step):
                            effective.update(scope.get("env") or {})
                        # Redirect only installation files into the case's temporary directory.
                        effective["TAPSTATE_INSTALL_DIR"] = str(install_dir)
                        before = requests.read_text().splitlines() if requests.exists() else []
                        checked(["bash", "-o", "pipefail", "-c", ONE_LINER], env=effective)
                        installed = checked([str(install_dir / "tapstate"), "--version"])
                        if installed.strip() != f"tapstate {version} {platform}":
                            raise RuntimeError(f"Fixture installation did not complete: {installed}")
                        after = requests.read_text().splitlines() if requests.exists() else []
                        for line in after[len(before):]:
                            record = json.loads(line)
                            event = record["payload"]
                            if (event["os"] + "-" + event["arch"] != platform
                                    or event["version"] != version):
                                raise RuntimeError(f"Fixture event does not match its install: {event}")
                            if record["url"] == PRODUCTION:
                                leaks[repository] += 1

        self.assertFalse(
            leaks,
            "Unfenced fork nightly installs selected POST " + PRODUCTION + ": "
            + ", ".join(f"{repo}={count} events" for repo, count in leaks.items())
            + ". Each lane installs twice on four platforms; its environment carries no production fence.",
        )


if __name__ == "__main__":
    unittest.main()
