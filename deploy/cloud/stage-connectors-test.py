#!/usr/bin/env python3
"""Failure probes for the Cloud connector image-input gate."""

from __future__ import annotations

import hashlib
import importlib.util
import io
import json
import subprocess
import sys
import tarfile
import tempfile
import unittest
import zipfile
from types import SimpleNamespace
from pathlib import Path
from unittest.mock import patch


SCRIPT = Path(__file__).with_name("stage-connectors.py")
SPEC = importlib.util.spec_from_file_location("stage_connectors", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)
IMAGE_SCRIPT = Path(__file__).with_name("verify-image.py")
IMAGE_SPEC = importlib.util.spec_from_file_location("verify_image", IMAGE_SCRIPT)
assert IMAGE_SPEC is not None and IMAGE_SPEC.loader is not None
IMAGE = importlib.util.module_from_spec(IMAGE_SPEC)
IMAGE_SPEC.loader.exec_module(IMAGE)
PREPARE_SPEC = importlib.util.spec_from_file_location("prepare_test_inputs", Path(__file__).with_name("prepare-test-inputs.py"))
assert PREPARE_SPEC is not None and PREPARE_SPEC.loader is not None
PREPARE = importlib.util.module_from_spec(PREPARE_SPEC)
PREPARE_SPEC.loader.exec_module(PREPARE)
CLOUD_URL = "https://console.example.test/"


class StageConnectorsTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory(prefix="cloud-connector-gate-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.jars = self.root / "jars"
        self.jars.mkdir()
        self.lock = self.root / "connectors.lock.json"
        self.staged = self.root / "staged"
        self.boot_jar = self.make_boot_jar()
        self.entries = []
        for connector_id in MODULE.REQUIRED_IDS:
            self.make_jar(connector_id)
        self.license_files = []
        for name in MODULE.REQUIRED_LICENSE_FILES:
            content = f"synthetic terms for {name}\n".encode("utf-8")
            (self.jars / name).write_bytes(content)
            self.license_files.append({
                "name": name,
                "bytes": len(content),
                "sha256": hashlib.sha256(content).hexdigest(),
            })
        self.write_lock()

    def make_boot_jar(self, *, profile: str = "cloud", console_url: str | None = CLOUD_URL,
                      extra_bytes: bytes = b"") -> bytes:
        html = b"<!doctype html><title>Cloud image input</title>\n"
        manifest = f"{hashlib.sha256(html).hexdigest()}  ./index.html\n".encode("ascii")
        properties = (
            "repository=tapstate/tapstate-web\n"
            f"revision={'b' * 40}\n"
            f"files.sha256={hashlib.sha256(manifest).hexdigest()}\n"
            f"tapstate.revision={'a' * 40}\n"
            "release.version=1.2.3\n"
            f"web.profile={profile}\n"
        )
        if console_url is not None:
            properties += f"cloud.console.url={console_url}\n"
        output = io.BytesIO()
        with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_STORED) as archive:
            archive.writestr("META-INF/tapstate-web.properties", properties)
            archive.writestr("META-INF/tapstate-web.files.sha256", manifest)
            archive.writestr("BOOT-INF/classes/static/index.html", html)
            if extra_bytes:
                archive.writestr("BOOT-INF/classes/Other.class", extra_bytes)
        return output.getvalue()

    def verify_image(self, layout: Path, boot_jar: Path | None = None, *,
                     profile: str = "cloud", console_url: str = CLOUD_URL) -> str:
        return IMAGE.verify(layout, self.lock, boot_jar,
                            web_profile=profile, cloud_console_url=console_url)

    def make_jar(self, connector_id: str, *, spec_id: str | None = None,
                 pdk_version: str = "2.0.5-SNAPSHOT", spec_path: str | None = None,
                 implementation_title: str | None = None) -> None:
        jar_path = self.jars / f"{connector_id}-connector.jar"
        spec_path = spec_path or f"{connector_id}-spec.json"
        title = implementation_title or (
            "mssql-connector" if connector_id == "sqlserver" else f"{connector_id}-connector"
        )
        manifest = (
            "Manifest-Version: 1.0\r\n"
            f"Implementation-Title: {title}\r\n"
            f"Git-Commit-Id: {'a' * 40}\r\n"
            f"PDK-API-Version: {pdk_version}\r\n\r\n"
        )
        with zipfile.ZipFile(jar_path, "w", compression=zipfile.ZIP_STORED) as jar:
            jar.writestr("META-INF/MANIFEST.MF", manifest)
            jar.writestr(spec_path, json.dumps({"properties": {"id": spec_id or connector_id}}))
            jar.writestr("classes/Dependency.class", b"x" * 1_000_000)
        data = jar_path.read_bytes()
        entry = {
            "id": connector_id,
            "bytes": len(data),
            "sha256": hashlib.sha256(data).hexdigest(),
            "upstreamRevision": "a" * 40,
            "pdkApiVersion": "2.0.5-SNAPSHOT",
            "specPath": spec_path,
        }
        self.entries = [old for old in self.entries if old["id"] != connector_id]
        self.entries.append(entry)

    def write_lock(self) -> None:
        self.lock.write_text(json.dumps({
            "schemaVersion": 2,
            "connectors": self.entries,
            "licenseFiles": self.license_files,
        }), encoding="utf-8")

    def test_public_test_inputs_stage_only_the_locked_downloaded_bytes(self) -> None:
        def response(command, **options):
            self.assertIn("--max-filesize", command)
            self.assertIn("--proto-redir", command)
            self.assertNotIn("--insecure", command)
            destination = Path(command[command.index("--output") + 1])
            destination.write_bytes((self.jars / command[-1].rsplit("/", 1)[-1]).read_bytes())
            return SimpleNamespace(returncode=0)
        with patch.object(PREPARE.subprocess, "run", side_effect=response) as downloaded:
            PREPARE.prepare(self.lock, self.staged)
        self.assertEqual(downloaded.call_count, 9)
        self.assertEqual((self.staged / "release/connectors.lock.json").read_bytes(), self.lock.read_bytes())

    def test_tampered_public_test_input_fails_without_producing_output(self) -> None:
        def response(command, **options):
            data = (self.jars / command[-1].rsplit("/", 1)[-1]).read_bytes()
            Path(command[command.index("--output") + 1]).write_bytes(b"!" + data[1:])
            return SimpleNamespace(returncode=0)
        with patch.object(PREPARE.subprocess, "run", side_effect=response):
            with self.assertRaisesRegex(PREPARE.STAGE.StageError, "bytes differ"):
                PREPARE.prepare(self.lock, self.staged)
        self.assertFalse(self.staged.exists())

    def test_oversized_public_input_is_bounded_and_does_not_stage(self) -> None:
        data = (self.jars / "mysql-connector.jar").read_bytes() + b"!"
        def response(command, **options):
            self.assertEqual(int(command[command.index("--max-filesize") + 1]), len(data) - 1)
            Path(command[command.index("--output") + 1]).write_bytes(data)
            return SimpleNamespace(returncode=0)
        with patch.object(PREPARE.subprocess, "run", side_effect=response):
            with self.assertRaisesRegex(PREPARE.STAGE.StageError, "exceed lock length"):
                PREPARE.prepare(self.lock, self.staged)
        self.assertFalse(self.staged.exists())

    def test_public_input_transport_failure_does_not_echo_transport_details(self) -> None:
        with patch.object(PREPARE.subprocess, "run", return_value=SimpleNamespace(
                returncode=35, stderr=b"transport-secret-sentinel")):
            with self.assertRaises(PREPARE.STAGE.StageError) as failure:
                PREPARE.prepare(self.lock, self.staged)
        self.assertNotIn("transport-secret-sentinel", str(failure.exception))
        self.assertIsNone(failure.exception.__cause__)
        self.assertFalse(self.staged.exists())

    def make_oci(self, *, architectures: tuple[str, ...] = ("amd64", "arm64"),
                 tamper: str | None = None, license_label: str | None = "NOASSERTION",
                 boot_jars: dict[str, bytes] | None = None,
                 omit_path: str | None = None, add_license_directory: bool = False,
                 label_overrides: dict[str, str | None] | None = None,
                 duplicate_profile_label: bool = False) -> Path:
        MODULE.stage(self.lock, self.jars, self.staged)
        layout = self.root / "oci"
        (layout / "blobs/sha256").mkdir(parents=True)

        def add_blob(data: bytes) -> str:
            digest = hashlib.sha256(data).hexdigest()
            (layout / "blobs/sha256" / digest).write_bytes(data)
            return "sha256:" + digest

        def add_json(value: dict) -> str:
            return add_blob(json.dumps(value, sort_keys=True).encode("utf-8"))

        descriptors = []
        for architecture in architectures:
            layer_stream = io.BytesIO()
            with tarfile.open(fileobj=layer_stream, mode="w") as archive:
                if add_license_directory:
                    directory = tarfile.TarInfo("opt/tapstate/release/licenses")
                    directory.type = tarfile.DIRTYPE
                    archive.addfile(directory)
                boot_jar = (boot_jars or {}).get(architecture, self.boot_jar)
                files = {"opt/tapstate/tapstate.jar": boot_jar}
                for path in self.staged.rglob("*"):
                    if path.is_file():
                        name = "opt/tapstate/" + str(path.relative_to(self.staged))
                        if name == omit_path:
                            continue
                        files[name] = path.read_bytes()
                if tamper is not None:
                    files[tamper] = b"tampered"
                for name, data in files.items():
                    member = tarfile.TarInfo(name)
                    member.size = len(data)
                    archive.addfile(member, io.BytesIO(data))
            layer_digest = add_blob(layer_stream.getvalue())
            with zipfile.ZipFile(io.BytesIO(boot_jar)) as jar:
                files_digest = hashlib.sha256(jar.read("META-INF/tapstate-web.files.sha256")).hexdigest()
            labels = {
                "org.opencontainers.image.version": "1.2.3",
                "org.opencontainers.image.revision": "a" * 40,
                "io.tapstate.web.revision": "b" * 40,
                "io.tapstate.web.files.sha256": files_digest,
                "io.tapstate.distribution": "cloud",
                "io.tapstate.web.profile": "cloud",
                "io.tapstate.web.cloud-console-url": CLOUD_URL,
            }
            if license_label is not None:
                labels["org.opencontainers.image.licenses"] = license_label
            for key, value in (label_overrides or {}).items():
                if value is None:
                    labels.pop(key, None)
                else:
                    labels[key] = value
            config_bytes = json.dumps({"config": {"Labels": labels}}, sort_keys=True).encode("utf-8")
            if duplicate_profile_label:
                config_bytes = config_bytes.replace(
                    b'"io.tapstate.web.profile": "cloud"',
                    b'"io.tapstate.web.profile": "onprem", "io.tapstate.web.profile": "cloud"',
                )
            config_digest = add_blob(config_bytes)
            manifest_digest = add_json({
                "config": {"digest": config_digest},
                "layers": [{"digest": layer_digest}],
            })
            descriptors.append({
                "digest": manifest_digest,
                "platform": {"os": "linux", "architecture": architecture},
            })
        top_digest = add_json({"manifests": descriptors})
        (layout / "index.json").write_text(json.dumps({"manifests": [{"digest": top_digest}]}), encoding="utf-8")
        return layout

    def test_stages_exact_verified_bytes_and_lock(self) -> None:
        MODULE.stage(self.lock, self.jars, self.staged)
        self.assertEqual(
            {path.name for path in (self.staged / "connectors").iterdir()},
            {f"{connector_id}-connector.jar" for connector_id in MODULE.REQUIRED_IDS},
        )
        self.assertEqual((self.staged / "release/connectors.lock.json").read_bytes(), self.lock.read_bytes())
        lines = (self.staged / "release/connectors.sha256").read_text(encoding="ascii").splitlines()
        self.assertEqual(len(lines), 7)
        for entry in self.entries:
            self.assertIn(f"{entry['sha256']}  {entry['id']}-connector.jar", lines)
        for entry in self.license_files:
            staged_license = self.staged / "release/licenses" / entry["name"]
            self.assertEqual(staged_license.read_bytes(), (self.jars / entry["name"]).read_bytes())

    def test_missing_or_extra_jar_refuses_without_staging(self) -> None:
        (self.jars / "mysql-connector.jar").unlink()
        with self.assertRaisesRegex(MODULE.StageError, "missing="):
            MODULE.stage(self.lock, self.jars, self.staged)
        self.assertFalse(self.staged.exists())
        self.make_jar("mysql")
        self.write_lock()
        (self.jars / "unexpected.jar").write_bytes(b"extra")
        with self.assertRaisesRegex(MODULE.StageError, "extra="):
            MODULE.stage(self.lock, self.jars, self.staged)
        self.assertFalse(self.staged.exists())

    def test_changed_jar_bytes_refuse(self) -> None:
        with (self.jars / "mongodb-atlas-connector.jar").open("ab") as stream:
            stream.write(b"changed")
        with self.assertRaisesRegex(MODULE.StageError, "JAR bytes differ"):
            MODULE.stage(self.lock, self.jars, self.staged)
        self.assertFalse(self.staged.exists())

    def test_missing_or_changed_companion_license_refuses(self) -> None:
        path = self.jars / "ORACLE-FREE-USE-TERMS.txt"
        path.unlink()
        with self.assertRaisesRegex(MODULE.StageError, "companion license file is missing"):
            MODULE.stage(self.lock, self.jars, self.staged)
        self.assertFalse(self.staged.exists())
        path.write_bytes(b"changed terms")
        with self.assertRaisesRegex(MODULE.StageError, "companion license bytes differ"):
            MODULE.stage(self.lock, self.jars, self.staged)
        self.assertFalse(self.staged.exists())

    def test_wrong_spec_id_refuses_even_with_matching_hash(self) -> None:
        self.make_jar("mongodb-atlas", spec_id="mongodb")
        self.write_lock()
        with self.assertRaisesRegex(MODULE.StageError, "spec id disagrees"):
            MODULE.stage(self.lock, self.jars, self.staged)

    def test_wrong_pdk_level_refuses_even_with_matching_hash(self) -> None:
        self.make_jar("aws-rds-mysql", pdk_version="1.0")
        self.write_lock()
        with self.assertRaisesRegex(MODULE.StageError, "PDK-API-Version disagrees"):
            MODULE.stage(self.lock, self.jars, self.staged)

    def test_published_spec_filenames_are_accepted(self) -> None:
        self.make_jar("mongodb", spec_path="spec.json")
        self.make_jar("oracle", spec_path="spec_oracle.json")
        self.make_jar("postgres", spec_path="spec_postgres.json")
        self.write_lock()
        MODULE.stage(self.lock, self.jars, self.staged)
        self.assertTrue((self.staged / "connectors/oracle-connector.jar").is_file())

    def test_sqlserver_published_manifest_uses_mssql_module_title(self) -> None:
        self.make_jar("sqlserver", spec_path="mssql-spec.json",
                      implementation_title="mssql-connector")
        self.write_lock()
        MODULE.stage(self.lock, self.jars, self.staged)
        self.assertTrue((self.staged / "connectors/sqlserver-connector.jar").is_file())

    def test_unrelated_manifest_title_still_refuses(self) -> None:
        self.make_jar("sqlserver", implementation_title="unrelated-connector")
        self.write_lock()
        with self.assertRaisesRegex(MODULE.StageError, "Implementation-Title disagrees"):
            MODULE.stage(self.lock, self.jars, self.staged)

    def test_duplicate_id_and_duplicate_json_key_refuse(self) -> None:
        self.entries[1] = dict(self.entries[0])
        self.write_lock()
        with self.assertRaisesRegex(MODULE.StageError, "duplicate or missing id"):
            MODULE.stage(self.lock, self.jars, self.staged)
        self.lock.write_text('{"schemaVersion":1,"schemaVersion":1,"connectors":[]}', encoding="utf-8")
        with self.assertRaisesRegex(MODULE.StageError, "duplicate JSON field"):
            MODULE.stage(self.lock, self.jars, self.staged)

    def test_existing_stage_is_never_overwritten(self) -> None:
        self.staged.mkdir()
        sentinel = self.staged / "do-not-touch"
        sentinel.write_bytes(b"owned by another build")
        with self.assertRaisesRegex(MODULE.StageError, "already exists"):
            MODULE.stage(self.lock, self.jars, self.staged)
        self.assertEqual(sentinel.read_bytes(), b"owned by another build")

    def test_oci_requires_both_platforms_with_same_locked_jars(self) -> None:
        layout = self.make_oci()
        self.assertTrue(self.verify_image(layout).startswith("sha256:"))

    def test_oci_rejects_different_boot_jars_across_platforms(self) -> None:
        layout = self.make_oci(boot_jars={"arm64": self.make_boot_jar(extra_bytes=b"other-bytecode")})
        with self.assertRaisesRegex(IMAGE.ImageError, "different Boot JAR bytes"):
            self.verify_image(layout)

    def test_oci_boot_jar_matches_the_verified_build_input(self) -> None:
        layout = self.make_oci()
        boot_jar = self.root / "app-boot.jar"
        boot_jar.write_bytes(self.boot_jar)
        self.assertTrue(self.verify_image(layout, boot_jar).startswith("sha256:"))
        boot_jar.write_bytes(b"different-build-input")
        with self.assertRaisesRegex(IMAGE.ImageError, "differs from the verified build input"):
            self.verify_image(layout, boot_jar)

    def test_oci_rejects_tampered_jar(self) -> None:
        layout = self.make_oci(tamper="opt/tapstate/connectors/mysql-connector.jar")
        with self.assertRaisesRegex(IMAGE.ImageError, "JAR differs from the release lock"):
            self.verify_image(layout)

    def test_oci_rejects_unlocked_release_content(self) -> None:
        layout = self.make_oci(tamper="opt/tapstate/release/secret.txt")
        with self.assertRaisesRegex(IMAGE.ImageError, "unexpected files"):
            self.verify_image(layout)

    def test_oci_requires_companion_license_file(self) -> None:
        layout = self.make_oci(omit_path="opt/tapstate/release/licenses/ORACLE-FREE-USE-TERMS.txt")
        with self.assertRaisesRegex(IMAGE.ImageError, "release metadata has missing or unexpected files"):
            self.verify_image(layout)

    def test_oci_accepts_companion_license_directory_entry(self) -> None:
        layout = self.make_oci(add_license_directory=True)
        self.assertTrue(self.verify_image(layout).startswith("sha256:"))

    def test_oci_rejects_changed_companion_license_file(self) -> None:
        layout = self.make_oci(tamper="opt/tapstate/release/licenses/MICROSOFT-MIT-LICENSE.txt")
        with self.assertRaisesRegex(IMAGE.ImageError, "companion license differs"):
            self.verify_image(layout)

    def test_oci_rejects_missing_architecture(self) -> None:
        layout = self.make_oci(architectures=("amd64",))
        with self.assertRaisesRegex(IMAGE.ImageError, "expected amd64 and arm64"):
            self.verify_image(layout)

    def test_oci_requires_license_label(self) -> None:
        layout = self.make_oci(license_label=None)
        with self.assertRaisesRegex(IMAGE.ImageError, "missing image label org.opencontainers.image.licenses"):
            self.verify_image(layout)

    def test_oci_cannot_wrap_an_onprem_jar_with_cloud_labels(self) -> None:
        onprem = self.make_boot_jar(profile="onprem", console_url=None)
        layout = self.make_oci(boot_jars={"amd64": onprem, "arm64": onprem})
        with self.assertRaisesRegex(IMAGE.ImageError, "disagrees with Boot JAR metadata"):
            self.verify_image(layout)

    def test_oci_requires_explicit_cloud_profile_labels(self) -> None:
        layout = self.make_oci(label_overrides={"io.tapstate.web.profile": None})
        with self.assertRaisesRegex(IMAGE.ImageError, "missing image label io.tapstate.web.profile"):
            self.verify_image(layout)

    def test_oci_duplicate_profile_label_cannot_shadow_a_crossed_profile(self) -> None:
        layout = self.make_oci(duplicate_profile_label=True)
        with self.assertRaisesRegex(IMAGE.ImageError, "duplicate JSON field"):
            self.verify_image(layout)

    def test_oci_cannot_wrap_cloud_jar_as_onprem_distribution(self) -> None:
        layout = self.make_oci(label_overrides={"io.tapstate.distribution": "onprem"})
        with self.assertRaisesRegex(IMAGE.ImageError, "distribution disagrees"):
            self.verify_image(layout)

    def test_oci_cloud_console_label_must_match_jar_metadata(self) -> None:
        layout = self.make_oci(label_overrides={"io.tapstate.web.cloud-console-url": "https://other.example.test/"})
        with self.assertRaisesRegex(IMAGE.ImageError, "URL label disagrees"):
            self.verify_image(layout)

    def test_oci_console_metadata_cannot_override_declared_build_input(self) -> None:
        layout = self.make_oci()
        with self.assertRaisesRegex(IMAGE.ImageError, "URL disagrees with the declared build input"):
            self.verify_image(layout, console_url="https://other.example.test/")

    def test_oci_cloud_gate_rejects_declared_onprem_profile(self) -> None:
        layout = self.make_oci()
        with self.assertRaisesRegex(IMAGE.ImageError, "on-prem profile must not contain"):
            self.verify_image(layout, profile="onprem")

    def test_oci_cli_verifies_actual_cloud_zip_against_declared_normalized_url(self) -> None:
        layout = self.make_oci()
        boot_jar = self.root / "verified-boot.jar"
        boot_jar.write_bytes(self.boot_jar)
        result = subprocess.run([
            sys.executable, str(IMAGE_SCRIPT), "--oci-layout", str(layout),
            "--lock", str(self.lock), "--boot-jar", str(boot_jar),
            "--web-profile", "cloud", "--cloud-console-url", "HTTPS://CONSOLE.EXAMPLE.TEST:443",
        ], capture_output=True, text=True, check=False)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("verified linux/amd64 and linux/arm64", result.stdout)

    def test_oci_cli_requires_both_explicit_profile_inputs(self) -> None:
        layout = self.make_oci()
        boot_jar = self.root / "verified-boot.jar"
        boot_jar.write_bytes(self.boot_jar)
        base = [sys.executable, str(IMAGE_SCRIPT), "--oci-layout", str(layout),
                "--lock", str(self.lock), "--boot-jar", str(boot_jar)]
        for missing, supplied in (("--web-profile", ["--cloud-console-url", CLOUD_URL]),
                                  ("--cloud-console-url", ["--web-profile", "cloud"])):
            with self.subTest(missing=missing):
                result = subprocess.run(base + supplied, capture_output=True, text=True, check=False)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn(missing, result.stderr)
                self.assertNotIn("Traceback", result.stderr)


if __name__ == "__main__":
    unittest.main()
