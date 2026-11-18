#!/usr/bin/env python3
"""Published bundle selection and transfer must preserve exact release identities."""
import importlib.util
import json
import shutil
from pathlib import Path
import unittest
from unittest.mock import patch


def load(name, filename):
    spec = importlib.util.spec_from_file_location(name, Path(__file__).with_name(filename))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


PUBLISHED = load("published_connectors", "stage-published-connectors.py")
FIXTURES = load("published_connector_fixtures", "stage-connectors-test.py")


class PublishedConnectorsTest(unittest.TestCase):
    def setUp(self):
        self.fixture = FIXTURES.StageConnectorsTest(methodName="runTest")
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.selection = []
        for entry in self.fixture.entries:
            if entry["id"] in PUBLISHED.STAGE.PUBLISHED_ONPREM_IDS:
                artifact = "mssql-connector" if entry["id"] == "sqlserver" else entry["id"] + "-connector"
                self.selection.append({"id": entry["id"], "groupId": "io.tapdata", "artifactId": artifact,
                    "version": "2.5.0", "pdkApiVersion": entry["pdkApiVersion"], "sha256": entry["sha256"],
                    "url": "https://maven.example.test/" + entry["id"] + "-connector.jar"})

    def download(self, url, destination):
        shutil.copyfile(self.fixture.jars / url.rsplit("/", 1)[-1], destination)

    def test_cloud_stages_four_maven_coordinates_without_enterprise_inputs(self):
        selected = [entry for entry in self.selection if entry["id"] in PUBLISHED.STAGE.CLOUD_IDS]
        with patch.object(PUBLISHED, "download", side_effect=self.download) as downloaded:
            PUBLISHED.stage(Path("fixture-boot.jar"), "cloud", self.fixture.staged, selection=selected)
        lock = self.fixture.staged / "release/connectors.lock.json"
        entries, licenses = PUBLISHED.STAGE.read_lock(lock, PUBLISHED.STAGE.CLOUD_IDS)
        self.assertEqual(downloaded.call_count, 4)
        self.assertEqual(licenses, [])
        self.assertEqual({entry["id"] for entry in entries}, set(PUBLISHED.STAGE.CLOUD_IDS))
        self.assertTrue(all(entry["groupId"] == "io.tapdata" and entry["version"] == "2.5.0" for entry in entries))

    def test_onprem_includes_dummy_and_companion_terms_and_excludes_db2(self):
        with patch.object(PUBLISHED, "download", side_effect=self.download):
            PUBLISHED.stage(Path("fixture-boot.jar"), "onprem", self.fixture.staged, selection=self.selection)
        entries, licenses = PUBLISHED.STAGE.read_lock(self.fixture.staged / "release/connectors.lock.json",
                                                     PUBLISHED.STAGE.PUBLISHED_ONPREM_IDS)
        self.assertEqual(len(entries), 8)
        self.assertIn("dummy", {entry["id"] for entry in entries})
        self.assertNotIn("db2", {entry["id"] for entry in entries})
        self.assertEqual({entry["name"] for entry in licenses}, set(PUBLISHED.STAGE.REQUIRED_LICENSE_FILES))

    def test_mismatched_digest_never_publishes_a_stage(self):
        selected = [entry for entry in self.selection if entry["id"] in PUBLISHED.STAGE.CLOUD_IDS]
        selected[0]["sha256"] = "f" * 64
        with patch.object(PUBLISHED, "download", side_effect=self.download):
            with self.assertRaisesRegex(PUBLISHED.STAGE.StageError, "SHA-256 differs"):
                PUBLISHED.stage(Path("fixture-boot.jar"), "cloud", self.fixture.staged, selection=selected)
        self.assertFalse(self.fixture.staged.exists())

    def test_missing_required_selection_never_downloads(self):
        with patch.object(PUBLISHED, "download") as downloaded:
            with self.assertRaisesRegex(PUBLISHED.STAGE.StageError, "incomplete"):
                PUBLISHED.stage(Path("fixture-boot.jar"), "cloud", self.fixture.staged, selection=[])
        downloaded.assert_not_called()
        self.assertFalse(self.fixture.staged.exists())


if __name__ == "__main__":
    unittest.main()
