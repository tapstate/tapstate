#!/usr/bin/env python3
"""Inspect compiled authoritative definitions and compare them with an independent Git tree."""
import argparse
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[2]
SOURCE = "core/core-common/src/main/java/"
GOLDEN = "arch-tests/src/test/resources/error-codes.golden"
LEGACY = "arch-tests/src/test/resources/legacy-error-codes-before-request.golden"
TEXT = "messages/src/main/resources/messages/en.yml"
MIGRATION_BASE = "227c3a9e34838646a4bc880afa7c209cface10be"


def run(*args, cwd=None):
    return subprocess.check_output(args, cwd=ROOT if cwd is None else cwd, stderr=subprocess.PIPE)


def git_file(revision, path):
    return run("git", "show", revision + ":" + path)


def codes(data):
    return {line.strip() for line in data.decode().splitlines() if line.strip() and not line.startswith("#")}


def is_typed_enum(source):
    return re.search(r"\benum\s+\w+\b[^{};]*\bimplements\b[^{};]*\bTapstateErrorCode\b", source) is not None


def snapshot(revision=None, typed=False):
    with tempfile.TemporaryDirectory(prefix="tapstate-error-contract-") as temporary:
        scratch = Path(temporary)
        classes = scratch / "classes"
        classes.mkdir()
        if revision:
            paths = run("git", "ls-tree", "-r", "--name-only", revision, "--", SOURCE).decode().splitlines()
            if typed:
                candidates = [line.split(":", 1)[1] for line in run("git", "grep", "-l",
                              "TapstateErrorCode", revision, "--", "**/src/main/java/*.java").decode().splitlines()]
                paths += [name for name in candidates if is_typed_enum(git_file(revision, name).decode())]
            sources = []
            for name in sorted(set(paths)):
                if not name.endswith(".java"):
                    continue
                relative = Path(name)
                if relative.is_absolute() or ".." in relative.parts:
                    raise ValueError("invalid historical source path")
                target = scratch / relative
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(git_file(revision, name))
                sources.append(str(target))
        else:
            sources = [str(p) for p in sorted((ROOT / SOURCE).rglob("*.java"))]
            if typed:
                for name in run("git", "ls-files", "-co", "--exclude-standard", "--", "*.java").decode().splitlines():
                    if "/src/main/java/" in name and is_typed_enum((ROOT / name).read_text()):
                        sources.append(str(ROOT / name))
                sources = sorted(set(sources))
        java_home = os.environ.get("JAVA_HOME")
        java = str(Path(java_home) / "bin/java") if java_home else "java"
        javac = str(Path(java_home) / "bin/javac") if java_home else "javac"
        run(javac, "--release", "21", "-d", str(classes), *sources,
            str(ROOT / "tools/error-contract/ErrorContractSnapshot.java"))
        output = run(java, "-cp", str(classes), "io.tapstate.tools.ErrorContractSnapshot", str(classes), "typed" if typed else "definitions")
        result = {}
        for line in output.decode().splitlines():
            code, owner, severity, parameters, lifecycle = line.split("\t")
            if code in result:
                raise ValueError("duplicate definition " + code)
            result[code] = dict(owner=owner, severity=severity,
                                parameters=parameters.split(",") if parameters else [], lifecycle=lifecycle)
        return result


def compare(previous, current):
    errors = []
    for code, before in previous.items():
        after = current.get(code)
        if after is None:
            errors.append("removed published identity " + code)
            continue
        for key in ("owner", "severity", "parameters"):
            if before[key] != after[key]:
                errors.append(code + " changed " + key)
        if before["lifecycle"] == "deprecated" and after["lifecycle"] != "deprecated":
            errors.append(code + " reactivated a deprecated identity")
    if errors:
        raise ValueError("; ".join(errors))


def check(base, current):
    if not re.fullmatch(r"[0-9a-f]{40}", base):
        raise ValueError("--base must be the complete independently selected Git commit SHA")
    run("git", "cat-file", "-e", base + "^{commit}")
    run("git", "merge-base", "--is-ancestor", MIGRATION_BASE, "HEAD")
    original = codes(git_file(MIGRATION_BASE, GOLDEN))
    if codes((ROOT / LEGACY).read_bytes()) != original:
        raise ValueError("historical golden differs from the immutable pre-cutover Git tree")
    text_changed = not (ROOT / TEXT).is_file() or (ROOT / TEXT).read_bytes() != git_file(MIGRATION_BASE, TEXT)
    previous = snapshot(base)
    compare(previous, current)
    historical_typed = snapshot(base, typed=True)
    current_typed = snapshot(typed=True)
    if not historical_typed or not current_typed:
        raise ValueError("no independently compiled typed history")
    if set(historical_typed) != codes(git_file(base, GOLDEN)):
        raise ValueError("independent typed history does not cover its own Git golden")
    if set(current_typed) != set(current):
        raise ValueError("current typed declarations do not cover authoritative definitions")
    migrations = {}
    for line in (ROOT / "arch-tests/src/test/resources/product-code-migrations.tsv").read_text().splitlines():
        if line and not line.startswith("#"):
            old, new = line.split("\t")
            if old in migrations or new in migrations.values():
                raise ValueError("duplicate migration identity")
            migrations[old] = new
    mapped = {}
    for code, definition in historical_typed.items():
        target = code if code.startswith("TS") else migrations.get(code)
        if target is None or target in mapped:
            raise ValueError("missing or ambiguous historical typed migration " + code)
        mapped[target] = definition
    compare(mapped, current_typed)
    if codes((ROOT / GOLDEN).read_bytes()) != set(current):
        raise ValueError("current golden differs from compiled authoritative definitions")
    return dict(base=base, migrationBase=MIGRATION_BASE, historicalDefinitions=len(previous), historicalTypedDefinitions=len(historical_typed),
                currentDefinitions=len(current), wordingReviewRequired=text_changed, result="passed")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["export", "query", "check"])
    parser.add_argument("--base")
    parser.add_argument("--code")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    current = snapshot()
    if not current:
        raise ValueError("no authoritative definitions found")
    result = current
    if args.action == "query":
        if args.code not in current:
            raise ValueError("unknown product identity " + str(args.code))
        result = {args.code: current[args.code]}
    elif args.action == "check":
        if not args.base:
            parser.error("check requires an independent --base SHA")
        result = check(args.base, current)
    output = json.dumps(result, indent=2, sort_keys=True) + "\n"
    if args.output:
        args.output.write_text(output)
    else:
        print(output, end="")


if __name__ == "__main__":
    try:
        main()
    except subprocess.CalledProcessError as error:
        raise SystemExit("error-contract: " + error.stderr.decode(errors="replace"))
    except ValueError as error:
        raise SystemExit("error-contract: " + str(error))
