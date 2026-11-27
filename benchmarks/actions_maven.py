"""Build and verify the Maven repository shared by one performance run."""

import argparse
from hashlib import file_digest, sha256
import json
import os
from pathlib import Path
import re
import shutil
import stat
import subprocess
from tempfile import TemporaryDirectory
import time

import catalog
import runner


ROOT = Path(__file__).resolve().parent.parent
DIGEST = re.compile(r"[a-f0-9]{64}")
CACHE_PREFIX = re.compile(r"perf-maven-v2-Linux-X64-[a-f0-9]{64}-")
CENTRAL_REQUEST = re.compile(r"^(?:\[INFO\] )?Downloading from central:", re.MULTILINE)
CENTRAL_429 = re.compile(r"from/to central .*?(?:HTTP Status: 429|status code: 429)", re.IGNORECASE)
MAVEN_GOALS = (
    "-pl", "app,e2e", "-am", "-Dtest=BenchmarkRunIT",
    "-Dsurefire.failIfNoSpecifiedTests=false", "-Dapi.version=1.44", "clean", "package",
)


def store_digest(store):
    if (store.is_symlink() or not (store / "repository").is_dir()
            or not os.access(store / "maven-home" / "bin" / "mvn", os.X_OK)):
        raise ValueError("The Maven cache lacks the dependency repository or pinned executable")
    checksum = sha256()
    files = 0
    for path in sorted(store.rglob("*")):
        mode = path.lstat().st_mode
        relative = path.relative_to(store)
        if relative.parts[0] not in {"repository", "maven-home"}:
            raise ValueError(f"Unexpected Maven cache path: {relative}")
        if stat.S_ISDIR(mode):
            continue
        if not stat.S_ISREG(mode):
            raise ValueError(f"Non-regular entry in Maven cache: {relative}")
        with path.open("rb") as stream:
            digest = file_digest(stream, "sha256").hexdigest()
        checksum.update(json.dumps([relative.as_posix(), stat.S_IMODE(mode), digest]).encode() + b"\n")
        files += relative.parts[0] == "repository"
    if not files:
        raise ValueError("The Maven cache dependency repository is empty")
    return checksum.hexdigest()


def verify_store(store, key):
    prefix, digest = key[:-64], key[-64:]
    if not CACHE_PREFIX.fullmatch(prefix) or not DIGEST.fullmatch(digest) or store_digest(store) != digest:
        raise ValueError("Maven cache contents do not match the selected cache key")
    return digest


def within_budget(stop_at, command, *, log=None, env=None):
    remaining = stop_at - int(time.time())
    if remaining <= 0:
        raise TimeoutError("The performance budget expired during Maven preparation")
    result = subprocess.run(
        ["timeout", "--kill-after=10", str(remaining), *command], cwd=ROOT,
        stdout=log, stderr=subprocess.STDOUT if log else None, check=False, env=env,
    )
    if result.returncode in (124, 137) and time.time() >= stop_at:
        raise TimeoutError("The performance budget expired during Maven preparation")
    return result.returncode


def refresh(maven, repository, evidence, stop_at, receipt, env):
    for attempt, delay in enumerate((10, 30, 0), start=1):
        log_path = evidence / f"refresh-{attempt}.log"
        with log_path.open("w", encoding="utf-8") as log:
            result = within_budget(stop_at, [
                str(maven), "-B", "-U", "-Dstyle.color=never", "-Daether.connector.basic.downstreamThreads=2",
                f"-Dmaven.repo.local={repository}", *MAVEN_GOALS,
            ], log=log, env=env)
        text = log_path.read_text(errors="replace")
        receipt["central_download_requests"] += len(CENTRAL_REQUEST.findall(text))
        limited = bool(CENTRAL_429.search(text))
        receipt["central_429_attempts"] += int(limited)
        receipt["refresh_attempts"] = attempt
        if not result:
            return
        if not limited:
            raise ValueError("Maven dependency resolution failed")
        if not delay or time.time() + delay >= stop_at:
            raise ValueError("Maven Central returned HTTP 429 during dependency resolution")
        time.sleep(delay)


def prepare(args):
    args.evidence.mkdir(parents=True, exist_ok=True)
    receipt = {
        "github_attempt": runner.github_attempt(),
        "code_sha": os.environ["GITHUB_SHA"],
        "status": "failed",
        "stage": "inputs",
        "central_download_requests": 0,
        "central_429_attempts": 0,
        "refresh_attempts": 0,
    }
    started = time.monotonic()
    try:
        manifest = catalog.read_json(args.inputs / "manifest.json")
        receipt["manifest_sha256"] = manifest["manifest_sha256"]
        if (manifest["github_attempt"] != receipt["github_attempt"]
                or manifest["identity"]["code_sha"] != receipt["code_sha"]
                or catalog.digest({k: v for k, v in manifest.items() if k != "manifest_sha256"})
                != receipt["manifest_sha256"] or manifest["unallocated"]):
            raise ValueError("Maven preparation received a mismatched or incomplete manifest")
        if not CACHE_PREFIX.fullmatch(args.prefix):
            raise ValueError("Invalid Maven input cache prefix")
        with TemporaryDirectory(dir=args.store.parent, prefix="maven-seed-") as temporary:
            stage = Path(temporary) / "store"
            repository = stage / "repository"
            repository.mkdir(parents=True)
            receipt["stage"] = "restore"
            if args.warm_key:
                if not args.warm_key.startswith(args.prefix):
                    raise ValueError("The restored Maven cache uses different build inputs")
                verify_store(args.store, args.warm_key)
                source = args.store / "repository"
                home = args.store / "maven-home"
                excludes = ["--exclude=*-SNAPSHOT/", "--exclude=*.lastUpdated"]
                receipt["source"] = "performance-cache"
            else:
                source = args.legacy
                home = Path(os.environ["MAVEN_HOME"])
                excludes = ["--exclude=/io/tapstate/", "--exclude=*-SNAPSHOT/", "--exclude=*.lastUpdated"]
                receipt["source"] = "cold"
                receipt["legacy_cache_hit"] = args.legacy_hit
            if source.is_dir() and within_budget(
                    args.stop_at, ["rsync", "-a", "--no-links", *excludes, "--", f"{source}/", f"{repository}/"]):
                raise ValueError("Could not copy cached Maven dependencies")
            if args.legacy_hit and not args.warm_key and any(repository.rglob("*.pom")):
                receipt["source"] = "nightly-or-ci-cache"
            shutil.copytree(home, stage / "maven-home")
            maven = stage / "maven-home" / "bin" / "mvn"
            environment = {**os.environ, "MAVEN_HOME": str(maven.parent.parent)}
            receipt["stage"] = "refresh"
            refresh(maven, repository, args.evidence, args.stop_at, receipt, environment)
            receipt["stage"] = "offline-verification"
            with (args.evidence / "offline.log").open("w", encoding="utf-8") as log:
                if within_budget(args.stop_at, [str(maven), "-B", "-o", f"-Dmaven.repo.local={repository}",
                                                *MAVEN_GOALS], log=log, env=environment):
                    raise ValueError("The package and benchmark test entry point failed offline")
            receipt["stage"] = "cache-save"
            for pattern in ("*.lastUpdated", "resolver-status.properties"):
                for marker in repository.rglob(pattern):
                    marker.unlink()
            digest = store_digest(stage)
            key = args.prefix + digest
            if args.store.exists():
                shutil.rmtree(args.store)
            stage.rename(args.store)
        receipt.update(status="prepared", cache_key=key, sha256=digest)
        with open(os.environ["GITHUB_OUTPUT"], "a", encoding="utf-8") as out:
            print(f"key={key}", file=out)
        print(f"Maven cache key: {key}")
    except (KeyError, OSError, ValueError) as error:
        receipt.update(status="failed", reason=str(error))
        raise
    finally:
        receipt["elapsed_seconds"] = round(time.monotonic() - started, 3)
        runner.write_json(args.evidence / "receipt.json", receipt)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    seed = commands.add_parser("prepare")
    for parameter in ("inputs", "store", "legacy", "evidence"):
        seed.add_argument(f"--{parameter}", type=Path, required=True)
    seed.add_argument("--prefix", required=True)
    seed.add_argument("--warm-key", default="")
    seed.add_argument("--legacy-hit", action="store_true")
    seed.add_argument("--stop-at", type=int, required=True)
    verify = commands.add_parser("verify")
    verify.add_argument("store", type=Path)
    verify.add_argument("key")
    args = parser.parse_args()
    try:
        if args.command == "prepare":
            prepare(args)
        else:
            print(verify_store(args.store, args.key))
    except (KeyError, OSError, ValueError) as error:
        parser.exit(1, f"{error}\n")


if __name__ == "__main__":
    main()
