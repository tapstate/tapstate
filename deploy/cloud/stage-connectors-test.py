#!/usr/bin/env python3
"""Failure probes for the Cloud connector image-input gate."""

from __future__ import annotations

import hashlib
import importlib.util
import io
import json
import tarfile
import tempfile
import unittest
import zipfile
from pathlib import Path


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


class StageConnectorsTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory(prefix="cloud-connector-gate-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.jars = self.root / "jars"
        self.jars.mkdir()
        self.lock = self.root / "connectors.lock.json"
        self.staged = self.root / "staged"
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

    def make_oci(self, *, architectures: tuple[str, ...] = ("amd64", "arm64"),
                 tamper: str | None = None, license_label: str | None = "NOASSERTION",
                 boot_jars: dict[str, bytes] | None = None,
                 omit_path: str | None = None, add_license_directory: bool = False) -> Path:
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
                boot_jar = (boot_jars or {}).get(architecture, b"synthetic-boot-jar")
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
            labels = {
                "org.opencontainers.image.version": "1.2.3",
                "org.opencontainers.image.revision": "a" * 40,
                "io.tapstate.web.revision": "b" * 40,
                "io.tapstate.web.files.sha256": "c" * 64,
            }
            if license_label is not None:
                labels["org.opencontainers.image.licenses"] = license_label
            config_digest = add_json({"config": {"Labels": labels}})
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
        self.assertTrue(IMAGE.verify(layout, self.lock).startswith("sha256:"))

    def test_oci_rejects_different_boot_jars_across_platforms(self) -> None:
        layout = self.make_oci(boot_jars={"arm64": b"other-boot-jar"})
        with self.assertRaisesRegex(IMAGE.ImageError, "different Boot JAR bytes"):
            IMAGE.verify(layout, self.lock)

    def test_oci_boot_jar_matches_the_verified_build_input(self) -> None:
        layout = self.make_oci()
        boot_jar = self.root / "app-boot.jar"
        boot_jar.write_bytes(b"synthetic-boot-jar")
        self.assertTrue(IMAGE.verify(layout, self.lock, boot_jar).startswith("sha256:"))
        boot_jar.write_bytes(b"different-build-input")
        with self.assertRaisesRegex(IMAGE.ImageError, "differs from the verified build input"):
            IMAGE.verify(layout, self.lock, boot_jar)

    def test_oci_rejects_tampered_jar(self) -> None:
        layout = self.make_oci(tamper="opt/tapstate/connectors/mysql-connector.jar")
        with self.assertRaisesRegex(IMAGE.ImageError, "JAR differs from the release lock"):
            IMAGE.verify(layout, self.lock)

    def test_oci_rejects_unlocked_release_content(self) -> None:
        layout = self.make_oci(tamper="opt/tapstate/release/secret.txt")
        with self.assertRaisesRegex(IMAGE.ImageError, "unexpected files"):
            IMAGE.verify(layout, self.lock)

    def test_oci_requires_companion_license_file(self) -> None:
        layout = self.make_oci(omit_path="opt/tapstate/release/licenses/ORACLE-FREE-USE-TERMS.txt")
        with self.assertRaisesRegex(IMAGE.ImageError, "release metadata has missing or unexpected files"):
            IMAGE.verify(layout, self.lock)

    def test_oci_accepts_companion_license_directory_entry(self) -> None:
        layout = self.make_oci(add_license_directory=True)
        self.assertTrue(IMAGE.verify(layout, self.lock).startswith("sha256:"))

    def test_oci_rejects_changed_companion_license_file(self) -> None:
        layout = self.make_oci(tamper="opt/tapstate/release/licenses/MICROSOFT-MIT-LICENSE.txt")
        with self.assertRaisesRegex(IMAGE.ImageError, "companion license differs"):
            IMAGE.verify(layout, self.lock)

    def test_oci_rejects_missing_architecture(self) -> None:
        layout = self.make_oci(architectures=("amd64",))
        with self.assertRaisesRegex(IMAGE.ImageError, "expected amd64 and arm64"):
            IMAGE.verify(layout, self.lock)

    def test_oci_requires_license_label(self) -> None:
        layout = self.make_oci(license_label=None)
        with self.assertRaisesRegex(IMAGE.ImageError, "missing image label org.opencontainers.image.licenses"):
            IMAGE.verify(layout, self.lock)


if __name__ == "__main__":
    unittest.main()
