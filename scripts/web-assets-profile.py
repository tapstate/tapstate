#!/usr/bin/env python3
"""Build and verify a pinned Web bundle with explicit deployment inputs."""

from __future__ import annotations

import argparse
import hashlib
import ipaddress
import json
import os
import re
import subprocess
import sys
import unicodedata
from pathlib import Path
from urllib.parse import unquote, urlsplit, urlunsplit


BUILD_MARKER = ".tapstate-web-build.json"
REPOSITORY = "tapstate/tapstate-web"


class WebAssetsError(ValueError):
    """An input or generated bundle does not satisfy the packaging contract."""


def normalize_cloud_console_url(value: str) -> str:
    """Return a canonical, metadata-safe HTTPS URL without secret-bearing authority."""
    if not isinstance(value, str) or not value:
        raise WebAssetsError("Cloud Console URL is required")
    if any(character.isspace() or unicodedata.category(character) in {"Cc", "Cf"}
           for character in value):
        raise WebAssetsError("Cloud Console URL must not contain whitespace or control characters")
    if "\\" in value or "#" in value:
        raise WebAssetsError("Cloud Console URL must not contain a backslash or fragment")
    try:
        parsed = urlsplit(value)
        hostname = parsed.hostname
        port = parsed.port
    except ValueError as exc:
        raise WebAssetsError("Cloud Console URL has an invalid authority") from exc
    if parsed.scheme.lower() != "https" or not parsed.netloc or not hostname:
        raise WebAssetsError("Cloud Console URL must be an absolute HTTPS URL")
    if "@" in parsed.netloc or parsed.username is not None or parsed.password is not None:
        raise WebAssetsError("Cloud Console URL must not contain user information")
    if parsed.netloc.endswith(":") or port is not None and not 1 <= port <= 65535:
        raise WebAssetsError("Cloud Console URL has an invalid port")
    if "%" in hostname:
        raise WebAssetsError("Cloud Console URL has an invalid hostname")
    if ":" in hostname:
        try:
            host = "[" + ipaddress.IPv6Address(hostname).compressed + "]"
        except ValueError as exc:
            raise WebAssetsError("Cloud Console URL has an invalid IP address") from exc
    else:
        # These IDNA deviation characters differ between Python and browser hostname mapping.
        if any(character in hostname for character in ("\u00df", "\u03c2")):
            raise WebAssetsError("Cloud Console hostname cannot be normalized consistently for browser URLs")
        try:
            host = hostname.encode("idna").decode("ascii").lower()
        except UnicodeError as exc:
            raise WebAssetsError("Cloud Console URL has an invalid hostname") from exc
        labels = host.split(".")
        if len(host) > 253 or any(not re.fullmatch(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?", label)
                                 for label in labels):
            raise WebAssetsError("Cloud Console URL has an invalid hostname")
        for label in labels:
            if label.startswith("xn--"):
                try:
                    decoded = label[4:].encode("ascii").decode("punycode")
                    if decoded.encode("idna").decode("ascii").lower() != label:
                        raise WebAssetsError("Cloud Console URL has a noncanonical IDNA hostname")
                except UnicodeError as exc:
                    raise WebAssetsError("Cloud Console URL has an invalid IDNA hostname") from exc
        if re.fullmatch(r"[0-9.]+", host):
            try:
                host = str(ipaddress.IPv4Address(host))
            except ValueError as exc:
                raise WebAssetsError("Cloud Console URL has an invalid IP address") from exc
        elif re.fullmatch(r"(?:[0-9]+|0x[0-9a-f]+)", labels[-1]):
            raise WebAssetsError("Cloud Console URL must not use a noncanonical numeric hostname")
    path = parsed.path or "/"
    query = parsed.query
    if re.search(r"[^A-Za-z0-9._~!$&'()*+,;=:@%/-]", path) or \
            re.search(r"[^A-Za-z0-9._~!$&'()*+,;=:@%/?-]", query):
        raise WebAssetsError("Cloud Console URL path and query must use URL-safe ASCII encoding")
    if re.search(r"%(?![0-9A-Fa-f]{2})", path + query):
        raise WebAssetsError("Cloud Console URL has an invalid percent escape")
    if any(unquote(segment) in {".", ".."} for segment in path.split("/")):
        raise WebAssetsError("Cloud Console URL must not contain path dot segments")
    path = re.sub(r"%[0-9A-Fa-f]{2}", lambda match: match.group().upper(), path)
    query = re.sub(r"%[0-9A-Fa-f]{2}", lambda match: match.group().upper(), query)
    query = query.replace("'", "%27")
    authority = host + (":" + str(port) if port is not None and port != 443 else "")
    return urlunsplit(("https", authority, path, query, ""))


def validate_profile(profile: str, console_url: str | None) -> str | None:
    if profile not in {"cloud", "onprem"}:
        raise WebAssetsError("Web profile must be cloud or onprem")
    if profile == "onprem":
        if console_url is not None:
            raise WebAssetsError("onprem Web must not receive a Cloud Console URL")
        return None
    return normalize_cloud_console_url(console_url)  # type: ignore[arg-type]


def git(root: Path, *arguments: str) -> str:
    result = subprocess.run(["git", "-C", str(root), *arguments], text=True,
                            capture_output=True, check=False, env=git_environment())
    if result.returncode:
        raise WebAssetsError("Web checkout Git verification failed")
    return result.stdout.strip()


def git_environment() -> dict[str, str]:
    return {key: value for key, value in os.environ.items() if not key.startswith("GIT_")}


def validate_checkout(root: Path, revision: str) -> Path:
    if not re.fullmatch(r"[0-9a-f]{40}", revision):
        raise WebAssetsError("Web revision must be a full lowercase SHA-1 commit id")
    try:
        root = root.resolve(strict=True)
    except OSError as exc:
        raise WebAssetsError("Web checkout does not exist") from exc
    if Path(git(root, "rev-parse", "--show-toplevel")).resolve() != root:
        raise WebAssetsError("--web-root must name the checkout root, not a subdirectory")
    expected = git(root, "rev-parse", "--verify", revision + "^{commit}")
    if git(root, "rev-parse", "HEAD") != expected:
        raise WebAssetsError("Web checkout revision does not match the requested revision")
    if git(root, "status", "--porcelain", "--untracked-files=all"):
        raise WebAssetsError("Web checkout must be clean")
    for name in ("package.json", "pnpm-lock.yaml"):
        if not (root / name).is_file() or (root / name).is_symlink():
            raise WebAssetsError("Web checkout is missing a regular package or lock file")
    return root


def dist_path(root: Path) -> Path:
    current = root
    for component in ("apps", "web", "dist"):
        current = current / component
        if current.is_symlink():
            raise WebAssetsError("production bundle path contains symlinks")
        if current.exists() and not current.is_dir():
            raise WebAssetsError("production bundle path is not a directory")
    dist = current
    if git(root, "ls-files", "--", "apps/web/dist"):
        raise WebAssetsError("production bundle must not contain tracked files")
    ignored = subprocess.run(["git", "-C", str(root), "check-ignore", "--quiet", "--no-index",
                              "apps/web/dist/"], capture_output=True, check=False, env=git_environment())
    if ignored.returncode:
        raise WebAssetsError("production bundle must be ignored generated output")
    if dist.exists():
        bundle_files(dist, include_marker=True)
    return dist


def bundle_files(dist: Path, *, include_marker: bool = False) -> list[Path]:
    files: list[Path] = []
    for path in dist.rglob("*"):
        if path.is_symlink():
            raise WebAssetsError("production bundle contains symlinks")
        if path.is_dir():
            continue
        if not path.is_file():
            raise WebAssetsError("production bundle contains a non-regular file")
        relative = path.relative_to(dist).as_posix()
        if "\\" in relative or any(unicodedata.category(character) in {"Cc", "Cf"}
                                      for character in relative):
            raise WebAssetsError("production bundle contains an unsafe manifest path")
        if include_marker or relative != BUILD_MARKER:
            files.append(path)
    return sorted(files, key=lambda path: path.relative_to(dist).as_posix().encode("utf-8"))


def files_manifest(dist: Path) -> bytes:
    if not (dist / "index.html").is_file() or not (dist / "index.html").stat().st_size:
        raise WebAssetsError("production bundle has no nonempty apps/web/dist/index.html")
    files = bundle_files(dist)
    if not files:
        raise WebAssetsError("production bundle contains no files")
    lines = []
    for path in files:
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        lines.append(f"{digest}  ./{path.relative_to(dist).as_posix()}\n")
    return "".join(lines).encode("utf-8")


def marker_metadata(revision: str, profile: str, console_url: str | None, digest: str) -> dict:
    metadata = {"schemaVersion": 1, "repository": REPOSITORY, "revision": revision,
                "webProfile": profile, "filesSha256": digest}
    if console_url is not None:
        metadata["cloudConsoleUrl"] = console_url
    return metadata


def unique_object(pairs: list[tuple[str, object]]) -> dict:
    result = {}
    for key, value in pairs:
        if key in result:
            raise WebAssetsError("Web build attestation contains a duplicate field")
        result[key] = value
    return result


def verify_bundle(root: Path, revision: str, profile: str, console_url: str | None) -> str:
    console_url = validate_profile(profile, console_url)
    root = validate_checkout(root, revision)
    dist = dist_path(root)
    marker = dist / BUILD_MARKER
    try:
        metadata = json.loads(marker.read_text(encoding="utf-8"), object_pairs_hook=unique_object)
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise WebAssetsError("production bundle has no valid Web build attestation; use build-web-assets.sh") from exc
    digest = hashlib.sha256(files_manifest(dist)).hexdigest()
    expected = marker_metadata(revision, profile, console_url, digest)
    if not isinstance(metadata, dict) or type(metadata.get("schemaVersion")) is not int or metadata != expected:
        raise WebAssetsError("Web build attestation disagrees with profile, URL, revision, or actual bundle bytes")
    return digest


def build_bundle(root: Path, revision: str, profile: str, console_url: str | None) -> None:
    console_url = validate_profile(profile, console_url)
    root = validate_checkout(root, revision)
    dist = dist_path(root)
    # Local ignored configuration can otherwise influence Vite without changing the pinned commit.
    for configuration_root in (root, root / "apps" / "web"):
        for path in configuration_root.glob(".env*"):
            if path.name != ".env.example" and not git(root, "ls-files", "--", str(path.relative_to(root))):
                raise WebAssetsError("Web build refuses untracked local environment files")
    marker = dist / BUILD_MARKER
    if marker.exists():
        marker.unlink()
    environment = {key: value for key, value in git_environment().items() if not key.startswith("VITE_")}
    if console_url is not None:
        environment["VITE_CLOUD_CONSOLE_URL"] = console_url
    try:
        result = subprocess.run(["pnpm", "--filter", "web", "build", "--mode", profile,
                                 "--emptyOutDir"], cwd=root, env=environment, check=False)
    except OSError as exc:
        raise WebAssetsError("cannot execute the pinned Web build") from exc
    if result.returncode:
        raise WebAssetsError("Web build failed; no valid build attestation was created")
    validate_checkout(root, revision)
    dist_path(root)
    digest = hashlib.sha256(files_manifest(dist)).hexdigest()
    metadata = marker_metadata(revision, profile, console_url, digest)
    with marker.open("x", encoding="utf-8") as output:
        json.dump(metadata, output, sort_keys=True, separators=(",", ":"))
        output.write("\n")
    print(f"built {profile} Web bundle at revision {revision} with files digest {digest}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    normalize = commands.add_parser("normalize-url")
    normalize.add_argument("--value", required=True)
    for operation in ("build", "verify"):
        command = commands.add_parser(operation)
        command.add_argument("--web-root", required=True, type=Path)
        command.add_argument("--web-revision", required=True)
        command.add_argument("--web-profile", required=True)
        command.add_argument("--cloud-console-url")
    args = parser.parse_args()
    try:
        if args.command == "normalize-url":
            print(normalize_cloud_console_url(args.value))
        elif args.command == "build":
            build_bundle(args.web_root, args.web_revision, args.web_profile, args.cloud_console_url)
        else:
            print(verify_bundle(args.web_root, args.web_revision, args.web_profile, args.cloud_console_url))
    except WebAssetsError as exc:
        print(f"web-assets-profile: {exc}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
