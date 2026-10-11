#!/usr/bin/env python3
"""Exercise declared build inputs against real ZIP, layer, OCI, and provenance fixtures."""

from __future__ import annotations

import hashlib
import io
import json
import subprocess
import sys
import tarfile
import tempfile
import unittest
import zipfile
import warnings
from pathlib import Path


SCRIPT = Path(__file__).with_name("web-provenance.py")
VERSION = "0.0.0-smoke"
TAPSTATE_REVISION = "0123456789abcdef0123456789abcdef01234567"
WEB_REVISION = "abcdef0123456789abcdef0123456789abcdef01"
CLOUD_URL = "https://console.example.test/"
HTML = b"<!doctype html><title>Smoke</title>\n"
ASSET = b"window.__tapstateWebSmoke = true;\n"


def digest(payload: bytes) -> str:
    return "sha256:" + hashlib.sha256(payload).hexdigest()


def json_bytes(value: object) -> bytes:
    return json.dumps(value, separators=(",", ":")).encode()


def add_blob(layout: Path, payload: bytes) -> str:
    value = digest(payload)
    algorithm, encoded = value.split(":", 1)
    path = layout / "blobs" / algorithm / encoded
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)
    return value


def boot_jar(*, profile: str | None = "onprem", console_url: str | None = None,
             html: bytes = HTML, asset: bytes = ASSET,
             overrides: dict[str, str | None] | None = None,
             extra_entry: tuple[str, bytes] | None = None) -> bytes:
    files = {"index.html": html, "assets/app.js": asset}
    manifest = "".join(f"{hashlib.sha256(content).hexdigest()}  ./{name}\n"
                       for name, content in sorted(files.items())).encode()
    properties = {
        "repository": "tapstate/tapstate-web", "revision": WEB_REVISION,
        "files.sha256": hashlib.sha256(manifest).hexdigest(),
        "tapstate.revision": TAPSTATE_REVISION, "release.version": VERSION,
    }
    if profile is not None:
        properties["web.profile"] = profile
    if profile == "cloud":
        properties["cloud.console.url"] = console_url if console_url is not None else CLOUD_URL
    elif console_url is not None:
        properties["cloud.console.url"] = console_url
    for key, value in (overrides or {}).items():
        if value is None:
            properties.pop(key, None)
        else:
            properties[key] = value
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w", zipfile.ZIP_DEFLATED) as jar:
        def add(name: str, content: str | bytes) -> None:
            entry = zipfile.ZipInfo(name)
            entry.compress_type = zipfile.ZIP_DEFLATED
            jar.writestr(entry, content)

        add("META-INF/tapstate-web.properties", "".join(
            f"{key}={value}\n" for key, value in properties.items()))
        add("META-INF/tapstate-web.files.sha256", manifest)
        for name, content in files.items():
            add(f"BOOT-INF/classes/static/{name}", content)
        if extra_entry is not None:
            add(*extra_entry)
    return output.getvalue()


def tamper_jar(jar_bytes: bytes, updates: dict[str, bytes]) -> bytes:
    output = io.BytesIO()
    with zipfile.ZipFile(io.BytesIO(jar_bytes)) as source, zipfile.ZipFile(
            output, "w", zipfile.ZIP_DEFLATED) as altered:
        for name in source.namelist():
            altered.writestr(name, updates.get(name, source.read(name)))
    return output.getvalue()


def image_layer(files: dict[str, bytes]) -> bytes:
    output = io.BytesIO()
    with tarfile.open(fileobj=output, mode="w") as layer:
        for name, data in files.items():
            info = tarfile.TarInfo(name)
            info.size = len(data)
            layer.addfile(info, io.BytesIO(data))
    return output.getvalue()


def write_layout(layout: Path, *, profile: str = "onprem", jar_bytes: bytes | None = None,
                 console_url: str | None = None, distribution: str | None = "onprem",
                 label_overrides: dict[str, str | None] | None = None,
                 arm64_labels: dict[str, str | None] | None = None,
                 arm64_jar: bytes | None = None,
                 extra_layers: list[dict[str, bytes]] | None = None) -> None:
    jar_bytes = jar_bytes if jar_bytes is not None else boot_jar(profile=profile, console_url=console_url)
    with zipfile.ZipFile(io.BytesIO(jar_bytes)) as jar:
        files_sha256 = hashlib.sha256(jar.read("META-INF/tapstate-web.files.sha256")).hexdigest()
    platforms = []
    for architecture in ("amd64", "arm64"):
        payload = arm64_jar if architecture == "arm64" and arm64_jar is not None else jar_bytes
        layers = [add_blob(layout, image_layer({"opt/tapstate/tapstate.jar": payload}))]
        layers.extend(add_blob(layout, image_layer(files)) for files in (extra_layers or []))
        labels = {
            "org.opencontainers.image.version": VERSION,
            "org.opencontainers.image.revision": TAPSTATE_REVISION,
            "io.tapstate.web.revision": WEB_REVISION,
            "io.tapstate.web.files.sha256": files_sha256,
            "io.tapstate.web.profile": profile,
        }
        if distribution is not None:
            labels["io.tapstate.distribution"] = distribution
        if profile == "cloud":
            labels["io.tapstate.web.cloud-console-url"] = console_url if console_url is not None else CLOUD_URL
        changes = dict(label_overrides or {})
        if architecture == "arm64":
            changes.update(arm64_labels or {})
        for key, value in changes.items():
            if value is None:
                labels.pop(key, None)
            else:
                labels[key] = value
        config_digest = add_blob(layout, json_bytes({"config": {"Labels": labels}}))
        manifest_digest = add_blob(layout, json_bytes({
            "schemaVersion": 2, "config": {"digest": config_digest},
            "layers": [{"digest": value} for value in layers],
        }))
        platforms.append({"digest": manifest_digest,
                          "platform": {"os": "linux", "architecture": architecture}})
    top = add_blob(layout, json_bytes({"schemaVersion": 2, "manifests": platforms}))
    (layout / "index.json").write_text(json.dumps({"schemaVersion": 2, "manifests": [{"digest": top}]}))


def invoke(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run([sys.executable, str(SCRIPT), *args], text=True, capture_output=True, check=False)


class ProvenanceSmoke(unittest.TestCase):
    def setUp(self) -> None:
        temporary = tempfile.TemporaryDirectory(prefix="tapstate-web-provenance-")
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.layout = self.root / "oci"
        self.layout.mkdir()
        self.provenance = self.root / "release.json"

    def profile_args(self, profile: str, url: str | None = None) -> list[str]:
        result = ["--web-profile", profile]
        if url is not None:
            result.extend(("--cloud-console-url", url))
        return result

    def expected_boot_sha256(self, profile: str) -> str:
        return hashlib.sha256(boot_jar(profile=profile)).hexdigest()

    def create(self, profile: str = "onprem", url: str | None = None,
               boot_jar_sha256: str | None = None) -> subprocess.CompletedProcess[str]:
        return invoke("create", "--oci-layout", str(self.layout), "--output", str(self.provenance),
                      "--version", VERSION, "--tapstate-revision", TAPSTATE_REVISION,
                      "--web-revision", WEB_REVISION, *self.profile_args(profile, url),
                      "--boot-jar-sha256", boot_jar_sha256 if boot_jar_sha256 is not None
                      else self.expected_boot_sha256(profile))

    def verify(self, profile: str = "onprem", url: str | None = None,
               boot_jar_sha256: str | None = None) -> subprocess.CompletedProcess[str]:
        return invoke("verify", "--oci-layout", str(self.layout), "--provenance", str(self.provenance),
                      *self.profile_args(profile, url), "--boot-jar-sha256",
                      boot_jar_sha256 if boot_jar_sha256 is not None else self.expected_boot_sha256(profile))

    def assert_ok(self, result: subprocess.CompletedProcess[str]) -> None:
        self.assertEqual(result.returncode, 0, result.stderr)

    def assert_rejected(self, result: subprocess.CompletedProcess[str], reason: str) -> None:
        self.assertNotEqual(result.returncode, 0)
        self.assertIn(reason, result.stderr)
        self.assertNotIn("Traceback", result.stderr)

    def mutate_provenance(self, field: str, value: object) -> None:
        data = json.loads(self.provenance.read_text())
        owner = data
        keys = field.split(".")
        for key in keys[:-1]:
            owner = owner[key]
        owner[keys[-1]] = value
        self.provenance.write_text(json.dumps(data))

    def test_explicit_onprem_fixture_preserves_package_and_schema(self) -> None:
        write_layout(self.layout)
        self.assert_ok(self.create())
        data = json.loads(self.provenance.read_text())
        self.assertEqual(data["schemaVersion"], 1)
        self.assertEqual(data["bootJarSha256"], self.expected_boot_sha256("onprem"))
        self.assertEqual(data["web"]["profile"], "onprem")
        self.assertNotIn("cloudConsoleUrl", data["web"])
        self.assertEqual(data["image"]["repository"], "ghcr.io/tapstate/tapstate")
        self.assert_ok(self.verify())

    def test_explicit_cloud_fixture_records_profile_url_and_separate_package(self) -> None:
        write_layout(self.layout, profile="cloud", distribution="cloud")
        self.assert_ok(self.create("cloud", CLOUD_URL))
        data = json.loads(self.provenance.read_text())
        self.assertEqual(data["web"]["profile"], "cloud")
        self.assertEqual(data["bootJarSha256"], self.expected_boot_sha256("cloud"))
        self.assertEqual(data["web"]["cloudConsoleUrl"], CLOUD_URL)
        self.assertEqual(data["image"]["repository"], "ghcr.io/tapstate/tapstate-cloud")
        self.assert_ok(self.verify("cloud", CLOUD_URL))

    def test_declared_console_url_normalizes_before_exact_comparison(self) -> None:
        write_layout(self.layout, profile="cloud", distribution="cloud")
        self.assert_ok(self.create("cloud", "HTTPS://CONSOLE.EXAMPLE.TEST:443"))
        self.assert_ok(self.verify("cloud", "https://console.example.test:443/"))

    def test_profile_is_required_in_both_command_inputs(self) -> None:
        write_layout(self.layout)
        for command, options in (("create", ["--output", str(self.provenance), "--version", VERSION,
                                           "--tapstate-revision", TAPSTATE_REVISION, "--web-revision", WEB_REVISION]),
                                 ("verify", ["--provenance", str(self.provenance)])):
            with self.subTest(command=command):
                result = invoke(command, "--oci-layout", str(self.layout), *options)
                self.assert_rejected(result, "--web-profile")

    def test_boot_jar_sha256_is_required_in_both_command_inputs(self) -> None:
        write_layout(self.layout)
        for command, options in (("create", ["--output", str(self.provenance), "--version", VERSION,
                                           "--tapstate-revision", TAPSTATE_REVISION, "--web-revision", WEB_REVISION]),
                                 ("verify", ["--provenance", str(self.provenance)])):
            with self.subTest(command=command):
                result = invoke(command, "--oci-layout", str(self.layout), "--web-profile", "onprem", *options)
                self.assert_rejected(result, "--boot-jar-sha256")

    def test_declared_boot_sha_requires_full_lowercase_hex(self) -> None:
        write_layout(self.layout)
        for value in ("", "a" * 63, "A" * 64, "g" * 64, "sha256:" + "a" * 64):
            with self.subTest(digest=value):
                self.assert_rejected(self.create(boot_jar_sha256=value), "64 lowercase hexadecimal")

    def test_self_consistent_alternate_boot_bytes_cannot_replace_declared_input(self) -> None:
        changed = boot_jar(extra_entry=("BOOT-INF/classes/Other.class", b"different-bytecode"))
        write_layout(self.layout, jar_bytes=changed)
        self.assert_rejected(self.create(), "Boot JAR bytes disagree with the declared build input")

    def test_explicit_alternate_boot_input_can_verify_its_actual_bytes(self) -> None:
        changed = boot_jar(extra_entry=("BOOT-INF/classes/Other.class", b"different-bytecode"))
        expected = hashlib.sha256(changed).hexdigest()
        write_layout(self.layout, jar_bytes=changed)
        self.assert_ok(self.create(boot_jar_sha256=expected))
        self.assert_ok(self.verify(boot_jar_sha256=expected))
        self.assert_rejected(self.verify(), "Boot JAR bytes disagree with the declared build input")

    def test_cloud_declared_input_requires_url(self) -> None:
        write_layout(self.layout, profile="cloud", distribution="cloud")
        self.assert_rejected(self.create("cloud"), "requires a Cloud Console URL")

    def test_onprem_declared_input_rejects_cloud_url(self) -> None:
        write_layout(self.layout)
        self.assert_rejected(self.create("onprem", CLOUD_URL), "must not contain a Cloud Console URL")

    def test_missing_or_unknown_jar_profile_is_not_legacy_onprem(self) -> None:
        for profile in (None, "unknown", "Cloud", ""):
            with self.subTest(profile=profile):
                write_layout(self.layout, jar_bytes=boot_jar(profile=profile))
                self.assert_rejected(self.create(), "missing a known Web profile")

    def test_missing_or_unknown_distribution_is_rejected(self) -> None:
        for distribution in (None, "unknown", ""):
            with self.subTest(distribution=distribution):
                write_layout(self.layout, distribution=distribution)
                self.assert_rejected(self.create(), "missing a known distribution")

    def test_cloud_jar_with_onprem_labels_is_rejected(self) -> None:
        write_layout(self.layout, jar_bytes=boot_jar(profile="cloud"))
        self.assert_rejected(self.create(), "disagrees with Boot JAR metadata")

    def test_distribution_cannot_cross_a_matching_profile_label(self) -> None:
        write_layout(self.layout, profile="cloud", distribution="onprem")
        self.assert_rejected(self.create("cloud", CLOUD_URL), "distribution disagrees")

    def test_self_consistent_onprem_archive_cannot_fulfil_declared_cloud_build(self) -> None:
        write_layout(self.layout)
        self.assert_rejected(self.create("cloud", CLOUD_URL), "profile disagrees with the declared build input")

    def test_self_consistent_cloud_archive_cannot_fulfil_declared_onprem_build(self) -> None:
        write_layout(self.layout, profile="cloud", distribution="cloud")
        self.assert_rejected(self.create(), "profile disagrees with the declared build input")

    def test_missing_profile_label_is_rejected(self) -> None:
        write_layout(self.layout, label_overrides={"io.tapstate.web.profile": None})
        self.assert_rejected(self.create(), "missing label io.tapstate.web.profile")

    def test_cloud_jar_requires_a_canonical_console_url(self) -> None:
        for url in (None, "", "https://CONSOLE.EXAMPLE.TEST", "http://console.example.test/"):
            with self.subTest(url=url):
                jar = boot_jar(profile="cloud", overrides={"cloud.console.url": url})
                write_layout(self.layout, profile="cloud", distribution="cloud", jar_bytes=jar)
                rejected = self.create("cloud", CLOUD_URL)
                self.assertNotEqual(rejected.returncode, 0)
                self.assertIn("Cloud Console URL", rejected.stderr)
                self.assertNotIn("Traceback", rejected.stderr)

    def test_onprem_jar_must_omit_console_url_including_empty_value(self) -> None:
        for url in ("", CLOUD_URL):
            with self.subTest(url=url):
                write_layout(self.layout, jar_bytes=boot_jar(console_url=url))
                self.assert_rejected(self.create(), "on-prem metadata contains a Cloud Console URL")

    def test_cloud_console_label_cannot_be_missing_or_swapped(self) -> None:
        for url in (None, "", "https://other.example.test/", "https://CONSOLE.EXAMPLE.TEST/"):
            with self.subTest(url=url):
                write_layout(self.layout, profile="cloud", distribution="cloud",
                             label_overrides={"io.tapstate.web.cloud-console-url": url})
                self.assert_rejected(self.create("cloud", CLOUD_URL), "URL label disagrees")

    def test_onprem_console_label_must_be_absent_or_empty(self) -> None:
        write_layout(self.layout, label_overrides={"io.tapstate.web.cloud-console-url": ""})
        self.assert_ok(self.create())
        write_layout(self.layout, label_overrides={"io.tapstate.web.cloud-console-url": CLOUD_URL})
        self.assert_rejected(self.create(), "on-prem image has a Cloud Console URL label")

    def test_matching_archive_console_url_cannot_override_declared_build_input(self) -> None:
        write_layout(self.layout, profile="cloud", distribution="cloud", console_url="https://other.example.test/")
        self.assert_rejected(self.create("cloud", CLOUD_URL), "URL disagrees with the declared build input")

    def test_invalid_declared_url_is_rejected_without_echoing_secret(self) -> None:
        write_layout(self.layout, profile="cloud", distribution="cloud")
        for url in ("https://secret-sentinel@console.example.test/", "https://console.example.test/#secret-sentinel",
                    "https://console.example.test/\nsecret-sentinel", "https://console.example.test:99999/",
                    "http://console.example.test/", "https://console.example.test/#"):
            with self.subTest(kind=url.split(":", 1)[0]):
                result = self.create("cloud", url)
                self.assert_rejected(result, "valid explicit HTTPS URL")
                self.assertNotIn("secret-sentinel", result.stderr)

    def test_platforms_cannot_disagree_about_profile_or_distribution(self) -> None:
        for labels in ({"io.tapstate.web.profile": "cloud"}, {"io.tapstate.distribution": "cloud"}):
            with self.subTest(labels=labels):
                write_layout(self.layout, arm64_labels=labels)
                self.assertNotEqual(self.create().returncode, 0)

    def test_platforms_cannot_use_different_console_url_labels(self) -> None:
        write_layout(self.layout, profile="cloud", distribution="cloud",
                     arm64_labels={"io.tapstate.web.cloud-console-url": "https://other.example.test/"})
        self.assert_rejected(self.create("cloud", CLOUD_URL), "URL label disagrees")

    def test_platforms_require_identical_boot_bytes_not_only_matching_web_metadata(self) -> None:
        changed = boot_jar(extra_entry=("BOOT-INF/classes/Other.class", b"different-bytecode"))
        write_layout(self.layout, arm64_jar=changed)
        self.assert_rejected(self.create(), "different Web release metadata")

    def test_static_bytes_still_must_match_the_packaged_manifest(self) -> None:
        jar = tamper_jar(boot_jar(), {"BOOT-INF/classes/static/assets/app.js": b"tampered asset\n"})
        write_layout(self.layout, jar_bytes=jar)
        self.assert_rejected(self.create(), "Web file does not match its manifest")

    def test_extra_unmanifested_static_file_is_rejected(self) -> None:
        jar = boot_jar(extra_entry=("BOOT-INF/classes/static/other.js", b"unmanifested"))
        write_layout(self.layout, jar_bytes=jar)
        self.assert_rejected(self.create(), "static files do not match the Web file manifest")

    def test_duplicate_zip_entry_cannot_shadow_packaged_metadata(self) -> None:
        output = io.BytesIO(boot_jar())
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
            with zipfile.ZipFile(output, "a") as archive:
                archive.writestr("META-INF/tapstate-web.properties", "web.profile=cloud\n")
        write_layout(self.layout, jar_bytes=output.getvalue())
        self.assert_rejected(self.create(), "duplicate ZIP entry")

    def test_jar_files_sha_must_match_the_exact_manifest_bytes(self) -> None:
        write_layout(self.layout, jar_bytes=boot_jar(overrides={"files.sha256": "0" * 64}))
        self.assert_rejected(self.create(), "disagrees with files.sha256 metadata")

    def test_image_files_label_cannot_be_swapped(self) -> None:
        write_layout(self.layout, label_overrides={"io.tapstate.web.files.sha256": "0" * 64})
        self.assert_rejected(self.create(), "disagrees with Boot JAR metadata")

    def test_duplicate_boot_input_across_layers_is_rejected(self) -> None:
        write_layout(self.layout, extra_layers=[{"opt/tapstate/tapstate.jar": boot_jar()}])
        self.assert_rejected(self.create(), "duplicate Boot JAR inputs")

    def test_deleted_boot_input_in_a_later_layer_is_rejected(self) -> None:
        write_layout(self.layout, extra_layers=[{"opt/tapstate/.wh.tapstate.jar": b""}])
        self.assert_rejected(self.create(), "deletes a Boot JAR input")

    def test_provenance_profile_cannot_be_missing_or_swapped(self) -> None:
        write_layout(self.layout)
        self.assert_ok(self.create())
        for value in (None, "cloud", "unknown"):
            with self.subTest(profile=value):
                self.mutate_provenance("web.profile", value)
                self.assert_rejected(self.verify(), "Web profile disagrees")

    def test_cloud_provenance_console_url_cannot_be_missing_or_swapped(self) -> None:
        write_layout(self.layout, profile="cloud", distribution="cloud")
        self.assert_ok(self.create("cloud", CLOUD_URL))
        for value in (None, "https://other.example.test/", "https://CONSOLE.EXAMPLE.TEST/"):
            with self.subTest(url=value):
                self.mutate_provenance("web.cloudConsoleUrl", value)
                result = self.verify("cloud", CLOUD_URL)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("Cloud Console URL", result.stderr)

    def test_onprem_provenance_must_omit_console_url_even_when_empty(self) -> None:
        write_layout(self.layout)
        self.assert_ok(self.create())
        self.mutate_provenance("web.cloudConsoleUrl", "")
        self.assert_rejected(self.verify(), "must not contain a Cloud Console URL")

    def test_packages_cannot_be_swapped_between_profiles(self) -> None:
        for profile, package in (("onprem", "ghcr.io/tapstate/tapstate-cloud"),
                                 ("cloud", "ghcr.io/tapstate/tapstate")):
            with self.subTest(profile=profile):
                write_layout(self.layout, profile=profile, distribution=profile)
                url = CLOUD_URL if profile == "cloud" else None
                self.assert_ok(self.create(profile, url))
                self.mutate_provenance("image.repository", package)
                self.assert_rejected(self.verify(profile, url), "image repository is unexpected")

    def test_provenance_manifest_digest_cannot_be_changed(self) -> None:
        write_layout(self.layout)
        self.assert_ok(self.create())
        self.mutate_provenance("image.manifestDigest", "sha256:" + "0" * 64)
        self.assert_rejected(self.verify(), "manifest digest disagrees")

    def test_duplicate_json_field_cannot_shadow_provenance_profile(self) -> None:
        write_layout(self.layout)
        self.assert_ok(self.create())
        payload = self.provenance.read_text().replace('"profile": "onprem"',
                                                      '"profile": "cloud", "profile": "onprem"')
        self.provenance.write_text(payload)
        self.assert_rejected(self.verify(), "duplicate field")

    def test_provenance_files_digest_cannot_be_changed(self) -> None:
        write_layout(self.layout)
        self.assert_ok(self.create())
        self.mutate_provenance("web.filesSha256", "0" * 64)
        self.assert_rejected(self.verify(), "files.sha256 disagrees")

    def test_provenance_boot_sha256_cannot_be_missing_or_changed(self) -> None:
        write_layout(self.layout)
        self.assert_ok(self.create())
        for value in (None, "0" * 64):
            with self.subTest(digest=value):
                self.mutate_provenance("bootJarSha256", value)
                self.assert_rejected(self.verify(), "Boot JAR SHA-256 disagrees with the declared build input")

    def test_verify_declared_profile_cannot_follow_a_self_consistent_other_archive(self) -> None:
        write_layout(self.layout)
        self.assert_ok(self.create())
        self.assert_rejected(self.verify("cloud", CLOUD_URL), "profile disagrees with the declared build input")

    def test_oci_blob_digest_tampering_is_rejected(self) -> None:
        write_layout(self.layout)
        descriptor = json.loads((self.layout / "index.json").read_text())["manifests"][0]
        path = self.layout / "blobs/sha256" / descriptor["digest"].removeprefix("sha256:")
        path.write_bytes(path.read_bytes() + b"tampered")
        self.assert_rejected(self.create(), "content does not match digest")


if __name__ == "__main__":
    unittest.main()
