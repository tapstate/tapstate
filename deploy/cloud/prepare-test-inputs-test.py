#!/usr/bin/env python3
"""Verify the narrow managed-target fixture with real lock-content validation and no network."""

import hashlib
import importlib.util
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import zipfile


SPEC = importlib.util.spec_from_file_location("test_inputs", Path(__file__).with_name("prepare-test-inputs.py"))
assert SPEC is not None and SPEC.loader is not None
INPUTS = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(INPUTS)


def jar_bytes(revision="pinned-revision", connector="mongodb"):
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w") as jar:
        jar.writestr("META-INF/MANIFEST.MF", f"Manifest-Version: 1.0\nImplementation-Title: {connector}-connector\n"
                     f"Git-Commit-Id: {revision}\nPDK-API-Version: test-api\n\n")
        jar.writestr(f"{connector}.json", json.dumps({"properties": {"id": connector}}))
    return output.getvalue()


def digest(content):
    return {"bytes": len(content), "sha256": hashlib.sha256(content).hexdigest()}


class MongoTestInputTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.output = self.root / "verified"
        self.jar = jar_bytes()
        self.entry = {"id": "mongodb", "specPath": "mongodb.json", "upstreamRevision": "pinned-revision",
                      "pdkApiVersion": "test-api", **digest(self.jar)}
        self.payloads = {"mongodb-connector.jar": self.jar, "NOTICE.txt": b"retained notice"}
        self.licenses = [{"name": "NOTICE.txt", **digest(self.payloads["NOTICE.txt"])}]
        self.downloaded = []

    def download(self, url, destination, maximum):
        self.downloaded.append((url, maximum))
        destination.write_bytes(self.payloads[destination.name])

    def prepare(self, *, with_dummy=False):
        dummy_jar = jar_bytes(connector="dummy")
        dummy = self.entry | {"id": "dummy", "specPath": "dummy.json", **digest(dummy_jar)}
        entries = [self.entry, dummy, {"id": "mysql"}]
        self.payloads.setdefault("dummy-connector.jar", dummy_jar)
        with patch.object(INPUTS.STAGE, "read_lock", return_value=(entries, self.licenses)), \
                patch.object(INPUTS, "download", side_effect=self.download):
            INPUTS.prepare_mongodb(self.root / "lock.json", self.output, "https://example.invalid/pinned",
                                   with_dummy=with_dummy)

    def test_only_the_requested_connector_and_verified_licenses_are_published(self):
        self.prepare()
        self.assertEqual(sorted(path.name for path in self.output.iterdir()), ["NOTICE.txt", "mongodb-connector.jar"])
        self.assertEqual((self.output / "mongodb-connector.jar").read_bytes(), self.jar)
        self.assertEqual(self.downloaded, [
            ("https://example.invalid/pinned/mongodb-connector.jar", len(self.jar)),
            ("https://example.invalid/pinned/NOTICE.txt", len(self.payloads["NOTICE.txt"]))])

    def test_dummy_opt_in_prepares_both_locked_connectors(self):
        self.prepare(with_dummy=True)
        self.assertEqual(sorted(path.name for path in self.output.iterdir()),
                         ["NOTICE.txt", "dummy-connector.jar", "mongodb-connector.jar"])
        self.assertEqual((self.output / "dummy-connector.jar").read_bytes(), self.payloads["dummy-connector.jar"])

    def test_changed_dummy_jar_prevents_publication(self):
        self.payloads["dummy-connector.jar"] = b"changed"
        with self.assertRaisesRegex(INPUTS.STAGE.StageError, "dummy: JAR bytes differ"):
            self.prepare(with_dummy=True)
        self.assertFalse(self.output.exists())

    def test_changed_jar_bytes_never_publish_an_output(self):
        self.payloads["mongodb-connector.jar"] = self.jar + b"changed"
        with self.assertRaises(INPUTS.STAGE.StageError):
            self.prepare()
        self.assertFalse(self.output.exists())

    def test_matching_hash_does_not_hide_a_different_manifest_revision(self):
        content = jar_bytes("different-revision")
        self.payloads["mongodb-connector.jar"] = content
        self.entry.update(digest(content))
        with self.assertRaises(INPUTS.STAGE.StageError):
            self.prepare()
        self.assertFalse(self.output.exists())

    def test_changed_license_never_publishes_the_valid_jar(self):
        self.payloads["NOTICE.txt"] = b"changed notice"
        with self.assertRaises(INPUTS.STAGE.StageError):
            self.prepare()
        self.assertFalse(self.output.exists())

    def test_an_existing_output_is_preserved_without_downloading(self):
        self.output.mkdir()
        retained = self.output / "keep.txt"
        retained.write_text("original")
        with self.assertRaises(INPUTS.STAGE.StageError):
            self.prepare()
        self.assertEqual(retained.read_text(), "original")
        self.assertEqual(self.downloaded, [])


if __name__ == "__main__":
    unittest.main()
