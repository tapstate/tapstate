#!/usr/bin/env python3
"""Input identity failures must stop the shipping connector witness path."""
import copy
import importlib.util
import json
from pathlib import Path
import sys
import shutil
import subprocess
from unittest.mock import patch
import unittest

sys.dont_write_bytecode = True
ROOT = Path(__file__).resolve().parents[2]

def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module

INPUTS = load('inputs', ROOT / '.github/scripts/_connector-inputs.py')
FIXTURES = load('fixtures', ROOT / 'deploy/cloud/stage-connectors-test.py')

class ConnectorInputsTest(unittest.TestCase):
    def setUp(self):
        self.fixture = FIXTURES.StageConnectorsTest(methodName='runTest')
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.jars, self.lock = self.fixture.jars, self.fixture.lock
        (self.jars / 'db2-connector.jar').unlink()
        self.sha = 'a' * 40
        self.manifest = self.fixture.root / 'inputs.json'
        self.value = INPUTS.record(self.jars, 'release', self.lock, self.sha)
        self.manifest.write_text(json.dumps(self.value))

    def downloaded(self, command, **options):
        patterns = [command[i + 1] for i, argument in enumerate(command[:-1]) if argument == '--pattern']
        expected = {name + '-connector.jar' for name in INPUTS.IDS} | {entry['name'] for entry in self.fixture.license_files}
        self.assertEqual(set(patterns), expected)
        self.assertEqual(command[:4], ['gh', 'release', 'download', 'connectors-preview'])
        destination = Path(command[command.index('--dir') + 1])
        for name in patterns:
            shutil.copyfile(self.jars / name, destination / name)

    def test_downloaded_shipping_bytes_are_verified_before_publication(self):
        output = self.fixture.root / 'downloaded'
        with patch.object(INPUTS.subprocess, 'run', side_effect=self.downloaded) as download:
            INPUTS.prepare_release(output, self.lock, self.sha)
        self.assertEqual(download.call_count, 1)
        INPUTS.verify(output / 'connector-inputs.json', output, self.lock, self.sha, 'release')

    def test_transport_failure_does_not_fallback_or_publish_an_output(self):
        output = self.fixture.root / 'downloaded'
        with patch.object(INPUTS.subprocess, 'run', side_effect=subprocess.CalledProcessError(1, ['gh'])) as download:
            with self.assertRaises(subprocess.CalledProcessError):
                INPUTS.prepare_release(output, self.lock, self.sha)
        self.assertEqual(download.call_count, 1)
        self.assertFalse(output.exists())

    def test_partial_publication_cannot_be_reused_as_verified_inputs(self):
        output = self.fixture.root / 'downloaded'
        original = shutil.copyfile
        def fail_copy(source, destination):
            if Path(destination).parent == output:
                raise OSError('injected publication failure')
            return original(source, destination)
        with patch.object(INPUTS.subprocess, 'run', side_effect=self.downloaded), patch.object(INPUTS.shutil, 'copyfile', side_effect=fail_copy):
            with self.assertRaises(OSError):
                INPUTS.prepare_release(output, self.lock, self.sha)
        self.assertTrue(output.is_dir())
        self.assertFalse((output / 'connector-inputs.json').exists())
        with self.assertRaisesRegex(ValueError, 'already exists'):
            INPUTS.prepare_release(output, self.lock, self.sha)

    def test_exact_shipping_set_passes_without_weakening_cloud_bundle(self):
        self.assertEqual(len(INPUTS.verify(self.manifest, self.jars, self.lock, self.sha, 'release')['connectors']), 7)
        with self.assertRaises(FIXTURES.MODULE.StageError):
            FIXTURES.MODULE.stage(self.lock, self.jars, self.fixture.staged)

    def test_mutated_release_asset_fails_instead_of_becoming_an_upstream_input(self):
        with (self.jars / 'mysql-connector.jar').open('ab') as stream:
            stream.write(b'changed asset under the same tag')
        with self.assertRaisesRegex(INPUTS.stage.StageError, 'bytes differ'):
            INPUTS.verify(self.manifest, self.jars, self.lock, self.sha, 'release')

    def test_missing_asset_and_duplicate_variants_fail(self):
        path = self.jars / 'mysql-connector.jar'
        original = path.read_bytes()
        path.unlink()
        with self.assertRaises(ValueError):
            INPUTS.record(self.jars, 'release', self.lock, self.sha)
        path.write_bytes(original)
        (self.jars / 'mysql-connector-other.jar').write_bytes(original)
        with self.assertRaises(ValueError):
            INPUTS.record(self.jars, 'release', self.lock, self.sha)

    def test_manifest_version_mismatch_fails_even_with_updated_byte_digest(self):
        self.fixture.make_jar('mysql', pdk_version='2.0.11-SNAPSHOT')
        self.fixture.write_lock()
        with self.assertRaises(INPUTS.stage.StageError):
            INPUTS.record(self.jars, 'release', self.lock, self.sha)

    def test_wrong_source_mode_host_or_lock_identity_cannot_be_admitted(self):
        for key, value in [('mode', 'upstream'), ('source_sha', 'b' * 40), ('lock_sha256', '0' * 64)]:
            broken = copy.deepcopy(self.value)
            broken[key] = value
            with self.assertRaises(ValueError):
                INPUTS.validate_identity(broken, self.lock, self.sha, 'release')
        broken = copy.deepcopy(self.value)
        broken['connectors'][0]['sha256'] = '0' * 64
        with self.assertRaises(ValueError):
            INPUTS.validate_identity(broken, self.lock, self.sha, 'release')

    def test_licenses_and_symlinked_manifest_fail_closed(self):
        license_path = self.jars / self.fixture.license_files[0]['name']
        original = license_path.read_bytes()
        license_path.write_bytes(b'wrong terms')
        with self.assertRaises(INPUTS.stage.StageError):
            INPUTS.record(self.jars, 'release', self.lock, self.sha)
        license_path.write_bytes(original)
        linked = self.fixture.root / 'linked.json'
        linked.symlink_to(self.manifest)
        with self.assertRaises(ValueError):
            INPUTS.verify(linked, self.jars, self.lock, self.sha, 'release')

if __name__ == '__main__':
    unittest.main()
