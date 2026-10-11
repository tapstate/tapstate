#!/usr/bin/env python3
"""Check profile-safe Web build and staging with a tiny pinned checkout."""

from __future__ import annotations

import os
import hashlib
import importlib.util
import json
import shutil
import sys
import subprocess
import tempfile
import unittest
from pathlib import Path


SCRIPTS = Path(__file__).resolve().parent
sys.dont_write_bytecode = True
TAPSTATE_REVISION = "0123456789abcdef0123456789abcdef01234567"
CONSOLE_URL = "https://console.example.test/"
PROFILE_SPEC = importlib.util.spec_from_file_location("web_assets_profile", SCRIPTS / "web-assets-profile.py")
assert PROFILE_SPEC is not None and PROFILE_SPEC.loader is not None
PROFILE = importlib.util.module_from_spec(PROFILE_SPEC)
PROFILE_SPEC.loader.exec_module(PROFILE)


class CloudConsoleUrlSmoke(unittest.TestCase):
    def test_normalizes_root_host_port_and_idna(self) -> None:
        cases = {
            "HTTPS://Console.Example.Test:443": "https://console.example.test/",
            "https://console.example.test:8443": "https://console.example.test:8443/",
            "https://b\u00fccher.example.test": "https://xn--bcher-kva.example.test/",
            "https://[2001:0DB8:0:0:0:0:0:1]:443": "https://[2001:db8::1]/",
            "https://127.0.0.1:18443": "https://127.0.0.1:18443/",
        }
        for value, expected in cases.items():
            with self.subTest(value=value):
                self.assertEqual(PROFILE.normalize_cloud_console_url(value), expected)
                self.assertEqual(PROFILE.normalize_cloud_console_url(expected), expected)

    def test_retains_explicit_safe_path_and_query(self) -> None:
        value = "https://console.example.test/console/%7euser?tab=clusters%2ffavorites&x=a=b"
        expected = "https://console.example.test/console/%7Euser?tab=clusters%2Ffavorites&x=a=b"
        self.assertEqual(PROFILE.normalize_cloud_console_url(value), expected)
        self.assertEqual(PROFILE.normalize_cloud_console_url(expected), expected)

    def test_query_apostrophe_is_encoded_without_changing_path_apostrophe(self) -> None:
        value = "https://console.example.test/user's-console?name='value'"
        expected = "https://console.example.test/user's-console?name=%27value%27"
        self.assertEqual(PROFILE.normalize_cloud_console_url(value), expected)

    def test_rejects_credentials_fragments_controls_and_invalid_authority(self) -> None:
        cases = (
            "", "http://console.example.test/", "/console", "https:///console", "https://",
            "https://user:private-password@console.example.test/", "https://@console.example.test/",
            "https://console.example.test/#", "https://console.example.test/#section",
            " https://console.example.test/", "https://console.example.test/\n",
            "https://console.example.test/\x00", "https://console.example.test/\u200b",
            "https://console.example.test\\path", "https://console.example.test:",
            "https://console.example.test:0/", "https://console.example.test:65536/",
            "https://console.example.test:not-a-port/", "https://bad_host/", "https://-bad.test/",
            "https://console.example.test../", "https://[2001:db8:::1]/", "https://%65xample.test/",
            "https://[fe80::1%25eth0]/", "https://0x7f000001/", "https://127.1/", "https://example.123/",
            "https://fa\u00df.example/", "https://fa\u1e9e.example/", "https://final-\u03c2.example/",
            "https://xn--a.example/", "https://xn--fa-hia.example/",
        )
        for value in cases:
            with self.subTest(value=value):
                with self.assertRaises(PROFILE.WebAssetsError) as rejected:
                    PROFILE.normalize_cloud_console_url(value)
                self.assertNotIn("private-password", str(rejected.exception))

    def test_rejects_noncanonical_or_unsafe_path_and_query(self) -> None:
        cases = (
            "https://console.example.test/a/../b", "https://console.example.test/./b",
            "https://console.example.test/a/%2e%2e/b", "https://console.example.test/%2E/b",
            "https://console.example.test/a%", "https://console.example.test/?q=%0g",
            "https://console.example.test/caf\u00e9", "https://console.example.test/?q=\u00e9",
            "https://console.example.test/<path>", "https://console.example.test/?q={value}",
        )
        for value in cases:
            with self.subTest(value=value):
                with self.assertRaises(PROFILE.WebAssetsError):
                    PROFILE.normalize_cloud_console_url(value)

    def test_canonical_urls_roundtrip_through_the_browser_url_parser(self) -> None:
        cases = (
            "HTTPS://Console.Example.Test:443", "https://console.example.test:18443/console?tab=x%2Fy",
            "https://b\u00fccher.example.test", "https://[2001:0db8::1]:443", "https://127.0.0.1:18443",
            "https://console.example.test/user's-console?name='value'",
        )
        canonical = [PROFILE.normalize_cloud_console_url(value) for value in cases]
        result = subprocess.run([
            "node", "-e", "const urls = JSON.parse(process.argv[1]); process.stdout.write(JSON.stringify(urls.map(v => new URL(v).href)))",
            json.dumps(canonical),
        ], text=True, capture_output=True, check=True)
        self.assertEqual(json.loads(result.stdout), canonical)


class WebProfileSmoke(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory(prefix="tapstate-web-profile-")
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name).resolve()
        self.web = self.root / "web"
        self.web.mkdir()
        self.environment = {
            key: value for key, value in os.environ.items()
            if not key.startswith("GIT_")
        }
        self.command("git", "init", "-q", str(self.web))
        self.command("git", "-C", str(self.web), "config", "user.email", "web-smoke@example.invalid")
        self.command("git", "-C", str(self.web), "config", "user.name", "web-smoke")
        (self.web / ".gitignore").write_text("dist/\n/apps/web/dist\n", encoding="utf-8")
        (self.web / "package.json").write_text('{"packageManager":"pnpm@11.10.0"}\n', encoding="utf-8")
        (self.web / "pnpm-lock.yaml").write_text("lockfileVersion: 9.0\n", encoding="utf-8")
        self.command("git", "-C", str(self.web), "add", ".")
        self.command("git", "-C", str(self.web), "commit", "-qm", "fixture")
        self.revision = self.command("git", "-C", str(self.web), "rev-parse", "HEAD").stdout.strip()
        self.dist = self.web / "apps" / "web" / "dist"
        (self.dist / "assets").mkdir(parents=True)
        (self.dist / "index.html").write_text("<!doctype html><title>Tapstate</title>\n", encoding="utf-8")
        (self.dist / "assets" / "app.js").write_text('console.log("opaque fixture")\n', encoding="utf-8")
        self.output = self.root / "output"
        self.bin = self.root / "bin"
        self.bin.mkdir()
        pnpm = self.bin / "pnpm"
        pnpm.write_text('''#!/usr/bin/env python3
import json
import os
import pathlib
import shutil
import sys
root = pathlib.Path.cwd()
pathlib.Path(os.environ["WEB_PROFILE_SMOKE_TRACE"]).write_text(json.dumps({
    "arguments": sys.argv[1:], "consoleUrl": os.environ.get("VITE_CLOUD_CONSOLE_URL"),
    "otherVite": os.environ.get("VITE_OTHER_UNTRUSTED_INPUT"), "cwd": str(root),
    "gitDirectory": os.environ.get("GIT_DIR"), "gitWorkTree": os.environ.get("GIT_WORK_TREE"),
}))
if os.environ.get("WEB_PROFILE_SMOKE_FAIL"):
    sys.exit(23)
dist = root / "apps" / "web" / "dist"
if dist.exists():
    shutil.rmtree(dist)
(dist / "assets").mkdir(parents=True)
(dist / "index.html").write_text("<!doctype html><title>Tapstate</title>\\n")
(dist / "assets" / "app.js").write_text('console.log("opaque fixture")\\n')
if os.environ.get("WEB_PROFILE_SMOKE_MUTATE_SOURCE"):
    (root / "package.json").write_text("{}\\n")
''', encoding="utf-8")
        pnpm.chmod(0o755)
        self.trace = self.root / "build-trace.json"
        self.environment["PATH"] = str(self.bin) + os.pathsep + self.environment["PATH"]
        self.environment["WEB_PROFILE_SMOKE_TRACE"] = str(self.trace)

    def command(self, *arguments: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(arguments, cwd=self.root, env=self.environment, text=True,
                              capture_output=True, check=True)

    def stage(self, *arguments: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run([
            "bash", str(SCRIPTS / "prepare-web-assets.sh"),
            "--web-root", str(self.web), "--web-revision", self.revision,
            "--tapstate-revision", TAPSTATE_REVISION,
            "--release-version", "0.6.0", "--output", str(self.output), *arguments,
        ], cwd=self.root, env=self.environment, text=True, capture_output=True, check=False)

    def build(self, profile: str = "onprem", console_url: str | None = None) -> subprocess.CompletedProcess[str]:
        arguments = ["bash", str(SCRIPTS / "build-web-assets.sh"), "--web-root", str(self.web),
                     "--web-revision", self.revision, "--web-profile", profile]
        if console_url is not None:
            arguments.extend(["--cloud-console-url", console_url])
        return subprocess.run(arguments, cwd=self.root, env=self.environment, text=True,
                              capture_output=True, check=False)

    def built(self, profile: str = "onprem", console_url: str | None = None) -> None:
        result = self.build(profile, console_url)
        self.assertEqual(result.returncode, 0, result.stderr)

    def rejected(self, result: subprocess.CompletedProcess[str], reason: str) -> None:
        self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn(reason, result.stderr)
        self.assertFalse(self.output.exists(), "rejected staging must not create output")

    def marker(self) -> Path:
        return self.dist / PROFILE.BUILD_MARKER

    def change_marker(self, key: str, value: object) -> None:
        metadata = json.loads(self.marker().read_text(encoding="utf-8"))
        metadata[key] = value
        self.marker().write_text(json.dumps(metadata), encoding="utf-8")

    def test_missing_profile_fails_before_staging(self) -> None:
        result = self.stage()
        self.assertNotEqual(result.returncode, 0, "a missing deployment profile must fail closed")
        self.assertIn("--web-profile is required", result.stderr)
        self.assertFalse(self.output.exists(), "invalid input must not create staged resources")

    def test_unknown_profile_fails_before_staging(self) -> None:
        self.rejected(self.stage("--web-profile", "preview"), "Web profile must be cloud or onprem")

    def test_missing_option_value_fails_with_a_useful_reason(self) -> None:
        self.rejected(self.stage("--web-profile"), "--web-profile requires a value")

    def test_invalid_cloud_url_is_rejected_without_echoing_credentials(self) -> None:
        value = "https://user:private-password@console.example.test/"
        result = self.stage("--web-profile", "cloud", "--cloud-console-url", value)
        self.rejected(result, "must not contain user information")
        self.assertNotIn("private-password", result.stderr)
        result = self.build("cloud", value)
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(self.trace.exists())
        self.assertNotIn("private-password", result.stderr)

    def test_cloud_requires_an_explicit_console_url(self) -> None:
        self.rejected(self.stage("--web-profile", "cloud"), "--cloud-console-url is required")
        result = self.build("cloud")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Cloud Console URL is required", result.stderr)
        self.assertFalse(self.trace.exists())

    def test_onprem_refuses_even_an_empty_console_url_argument(self) -> None:
        for url in ("", CONSOLE_URL):
            with self.subTest(url=url):
                self.rejected(self.stage("--web-profile", "onprem", "--cloud-console-url", url),
                              "onprem Web must not receive a Cloud Console URL")
                self.assertNotEqual(self.build("onprem", url).returncode, 0)
        self.assertFalse(self.trace.exists())

    def test_onprem_build_drops_ambient_cloud_and_other_vite_inputs(self) -> None:
        self.environment["VITE_CLOUD_CONSOLE_URL"] = "https://unexpected.example.test/"
        self.environment["VITE_OTHER_UNTRUSTED_INPUT"] = "unexpected"
        self.built()
        trace = json.loads(self.trace.read_text(encoding="utf-8"))
        self.assertEqual(trace["arguments"], ["--filter", "web", "build", "--mode", "onprem", "--emptyOutDir"])
        self.assertIsNone(trace["consoleUrl"])
        self.assertIsNone(trace["otherVite"])
        self.assertEqual(trace["cwd"], str(self.web))

    def test_cloud_build_passes_only_the_explicit_normalized_console_url(self) -> None:
        self.environment["VITE_CLOUD_CONSOLE_URL"] = "https://unexpected.example.test/"
        self.built("cloud", "HTTPS://Console.Example.Test:443")
        trace = json.loads(self.trace.read_text(encoding="utf-8"))
        self.assertEqual(trace["arguments"], ["--filter", "web", "build", "--mode", "cloud", "--emptyOutDir"])
        self.assertEqual(trace["consoleUrl"], CONSOLE_URL)
        self.assertEqual(json.loads(self.marker().read_text(encoding="utf-8"))["cloudConsoleUrl"], CONSOLE_URL)

    def test_build_drops_foreign_git_environment_before_package_scripts(self) -> None:
        self.environment["GIT_DIR"] = str(self.root / "foreign.git")
        self.environment["GIT_WORK_TREE"] = str(self.root / "foreign-tree")
        self.built()
        trace = json.loads(self.trace.read_text(encoding="utf-8"))
        self.assertIsNone(trace["gitDirectory"])
        self.assertIsNone(trace["gitWorkTree"])

    def test_clean_onprem_bundle_retains_all_provenance_fields(self) -> None:
        self.built()
        result = self.stage("--web-profile", "onprem")
        self.assertEqual(result.returncode, 0, result.stderr)
        properties = (self.output / "META-INF" / "tapstate-web.properties").read_text(encoding="utf-8")
        self.assertIn("repository=tapstate/tapstate-web\n", properties)
        self.assertIn("revision=" + self.revision + "\n", properties)
        self.assertIn("tapstate.revision=" + TAPSTATE_REVISION + "\n", properties)
        self.assertIn("release.version=0.6.0\n", properties)
        self.assertIn("web.profile=onprem\n", properties)
        self.assertNotIn("cloud.console.url", properties)
        manifest = (self.output / "META-INF" / "tapstate-web.files.sha256").read_bytes()
        digest = hashlib.sha256(manifest).hexdigest()
        self.assertIn("files.sha256=" + digest + "\n", properties)
        self.assertEqual(json.loads(self.marker().read_text(encoding="utf-8"))["filesSha256"], digest)
        self.assertFalse((self.output / "static" / PROFILE.BUILD_MARKER).exists())
        self.assertNotIn(PROFILE.BUILD_MARKER.encode(), manifest)
        self.assertEqual((self.output / "static" / "assets" / "app.js").read_bytes(),
                         (self.dist / "assets" / "app.js").read_bytes())

    def test_clean_cloud_bundle_stages_profile_and_normalized_console_url(self) -> None:
        self.built("cloud", "HTTPS://Console.Example.Test:443")
        result = self.stage("--web-profile", "cloud", "--cloud-console-url", "HTTPS://Console.Example.Test:443")
        self.assertEqual(result.returncode, 0, result.stderr)
        properties = (self.output / "META-INF" / "tapstate-web.properties").read_text(encoding="utf-8")
        self.assertIn("web.profile=cloud\n", properties)
        self.assertIn("cloud.console.url=" + CONSOLE_URL + "\n", properties)
        self.assertFalse((self.output / "static" / PROFILE.BUILD_MARKER).exists())

    def test_a_bundle_without_build_attestation_is_rejected(self) -> None:
        self.rejected(self.stage("--web-profile", "onprem"), "no valid Web build attestation")

    def test_swapped_cloud_bundle_is_rejected_even_with_opaque_same_bytes(self) -> None:
        self.built("cloud", CONSOLE_URL)
        self.rejected(self.stage("--web-profile", "onprem"), "Web build attestation disagrees")

    def test_swapped_onprem_bundle_is_rejected_even_with_opaque_same_bytes(self) -> None:
        self.built()
        self.rejected(self.stage("--web-profile", "cloud", "--cloud-console-url", CONSOLE_URL),
                      "Web build attestation disagrees")

    def test_cloud_console_url_mismatch_is_rejected(self) -> None:
        self.built("cloud", CONSOLE_URL)
        self.rejected(self.stage("--web-profile", "cloud", "--cloud-console-url", "https://other.example.test/"),
                      "Web build attestation disagrees")

    def test_bundle_tampering_is_rejected(self) -> None:
        self.built()
        (self.dist / "assets" / "app.js").write_text("tampered\n", encoding="utf-8")
        self.rejected(self.stage("--web-profile", "onprem"), "Web build attestation disagrees")

    def test_attestation_revision_tampering_is_rejected(self) -> None:
        self.built()
        self.change_marker("revision", "a" * 40)
        self.rejected(self.stage("--web-profile", "onprem"), "Web build attestation disagrees")

    def test_attestation_digest_tampering_is_rejected(self) -> None:
        self.built()
        self.change_marker("filesSha256", "0" * 64)
        self.rejected(self.stage("--web-profile", "onprem"), "Web build attestation disagrees")

    def test_attestation_schema_and_profile_types_are_checked(self) -> None:
        self.built()
        original = self.marker().read_text(encoding="utf-8")
        for key, value in (("schemaVersion", True), ("schemaVersion", "1"), ("webProfile", "preview")):
            with self.subTest(key=key, value=value):
                self.marker().write_text(original, encoding="utf-8")
                self.change_marker(key, value)
                self.rejected(self.stage("--web-profile", "onprem"), "Web build attestation disagrees")

    def test_attestation_duplicate_or_unknown_fields_are_rejected(self) -> None:
        self.built()
        original = self.marker().read_text(encoding="utf-8")
        self.marker().write_text(original.rstrip()[:-1] + ',"webProfile":"onprem"}', encoding="utf-8")
        self.rejected(self.stage("--web-profile", "onprem"), "duplicate field")
        self.marker().write_text(original, encoding="utf-8")
        self.change_marker("unexpected", "field")
        self.rejected(self.stage("--web-profile", "onprem"), "Web build attestation disagrees")

    def test_cloud_url_must_not_leak_onto_an_onprem_attestation(self) -> None:
        self.built()
        self.change_marker("cloudConsoleUrl", CONSOLE_URL)
        self.rejected(self.stage("--web-profile", "onprem"), "Web build attestation disagrees")

    def test_symlink_bundle_is_rejected_before_build_or_stage(self) -> None:
        self.built()
        (self.dist / "bundle.js").symlink_to("assets/app.js")
        self.rejected(self.stage("--web-profile", "onprem"), "production bundle contains symlinks")
        self.assertNotEqual(self.build().returncode, 0)

    def test_symlink_dist_or_parent_does_not_delete_external_files(self) -> None:
        shutil.rmtree(self.dist)
        external = self.root / "external"
        external.mkdir()
        sentinel = external / "keep.txt"
        sentinel.write_text("keep\n", encoding="utf-8")
        self.dist.symlink_to(external, target_is_directory=True)
        result = self.build()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("production bundle path contains symlinks", result.stderr)
        self.assertEqual(sentinel.read_text(encoding="utf-8"), "keep\n")
        self.assertFalse(self.trace.exists())

    def test_tracked_dist_is_refused_before_build(self) -> None:
        self.command("git", "-C", str(self.web), "add", "--force", "apps/web/dist/index.html")
        self.command("git", "-C", str(self.web), "commit", "-qm", "tracked bundle")
        self.revision = self.command("git", "-C", str(self.web), "rev-parse", "HEAD").stdout.strip()
        result = self.build()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("production bundle must not contain tracked files", result.stderr)
        self.assertFalse(self.trace.exists())

    def test_unignored_dist_is_refused_before_build(self) -> None:
        shutil.rmtree(self.dist)
        (self.web / ".gitignore").write_text("", encoding="utf-8")
        self.command("git", "-C", str(self.web), "add", ".gitignore")
        self.command("git", "-C", str(self.web), "commit", "-qm", "remove generated ignore")
        self.revision = self.command("git", "-C", str(self.web), "rev-parse", "HEAD").stdout.strip()
        result = self.build()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("production bundle must be ignored generated output", result.stderr)
        self.assertFalse(self.trace.exists())

    def test_untracked_local_env_cannot_override_build_inputs(self) -> None:
        (self.web / ".gitignore").write_text("dist/\n.env*\n", encoding="utf-8")
        self.command("git", "-C", str(self.web), "add", ".gitignore")
        self.command("git", "-C", str(self.web), "commit", "-qm", "environment ignore")
        self.revision = self.command("git", "-C", str(self.web), "rev-parse", "HEAD").stdout.strip()
        (self.web / ".env.onprem").write_text("VITE_CLOUD_CONSOLE_URL=https://unexpected.example.test/\n", encoding="utf-8")
        result = self.build()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("untracked local environment files", result.stderr)
        self.assertFalse(self.trace.exists())

    def test_failed_build_invalidates_prior_attestation(self) -> None:
        self.built()
        self.environment["WEB_PROFILE_SMOKE_FAIL"] = "1"
        result = self.build()
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(self.marker().exists())
        self.rejected(self.stage("--web-profile", "onprem"), "no valid Web build attestation")

    def test_build_source_mutation_is_rejected_without_attestation(self) -> None:
        self.environment["WEB_PROFILE_SMOKE_MUTATE_SOURCE"] = "1"
        result = self.build()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Web checkout must be clean", result.stderr)
        self.assertFalse(self.marker().exists())

    def test_nonfull_or_unknown_revision_is_refused(self) -> None:
        for revision in ("short", "deadbeef", "e" * 40):
            with self.subTest(revision=revision):
                self.revision = revision
                self.rejected(self.stage("--web-profile", "onprem"), "revision")

    def test_mismatched_head_is_refused(self) -> None:
        self.command("git", "-C", str(self.web), "commit", "--allow-empty", "-qm", "second-commit")
        self.rejected(self.stage("--web-profile", "onprem"), "expected")

    def test_invalid_release_version_is_refused(self) -> None:
        self.rejected(self.stage("--web-profile", "onprem", "--release-version", "0x6.0"), "release version")

    def test_nonfull_tapstate_revision_is_refused(self) -> None:
        self.rejected(self.stage("--web-profile", "onprem", "--tapstate-revision", "abcdef"),
                      "Tapstate revision must be a full")

    def test_subdirectory_web_root_is_refused(self) -> None:
        self.rejected(self.stage("--web-profile", "onprem", "--web-root", str(self.web / "apps" / "web")),
                      "--web-root must name the checkout root")

    def test_unstaged_staged_and_untracked_changes_are_refused(self) -> None:
        self.built()
        package = self.web / "package.json"
        original = package.read_text(encoding="utf-8")
        package.write_text("{}\n", encoding="utf-8")
        self.rejected(self.stage("--web-profile", "onprem"), "unstaged changes")
        self.command("git", "-C", str(self.web), "add", "package.json")
        self.rejected(self.stage("--web-profile", "onprem"), "staged changes")
        package.write_text(original, encoding="utf-8")
        self.command("git", "-C", str(self.web), "add", "package.json")
        (self.web / "untracked.txt").write_text("untracked\n", encoding="utf-8")
        self.rejected(self.stage("--web-profile", "onprem"), "untracked files")

    def test_missing_entrypoint_is_refused(self) -> None:
        self.built()
        (self.dist / "index.html").unlink()
        self.rejected(self.stage("--web-profile", "onprem"), "production bundle has no apps/web/dist/index.html")

    def test_existing_destination_is_refused(self) -> None:
        self.built()
        self.output.mkdir()
        sentinel = self.output / "keep.txt"
        sentinel.write_text("keep\n", encoding="utf-8")
        result = self.stage("--web-profile", "onprem")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("already exists", result.stderr)
        self.assertEqual(sentinel.read_text(encoding="utf-8"), "keep\n")

    def test_dangling_destination_symlink_is_refused(self) -> None:
        self.built()
        self.output.symlink_to(self.root / "external-missing", target_is_directory=True)
        result = self.stage("--web-profile", "onprem")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("already exists", result.stderr)
        self.assertFalse((self.root / "external-missing").exists())

    def test_unsafe_manifest_filename_is_refused(self) -> None:
        self.built()
        (self.dist / "assets" / "bad\nname.js").write_text("unsafe\n", encoding="utf-8")
        self.rejected(self.stage("--web-profile", "onprem"), "unsafe manifest path")


if __name__ == "__main__":
    unittest.main(verbosity=2)
