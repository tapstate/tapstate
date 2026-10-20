#!/usr/bin/env python3
"""Discriminate source identities from Java escapes, injection and disabled templates."""
import importlib.util
import json
from pathlib import Path
import shutil
import sys
import tempfile
from types import SimpleNamespace
import unittest

sys.dont_write_bytecode = True
HERE = Path(__file__).resolve().parent


def module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    value = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(value)
    return value


evidence = module('evidence_regression', HERE / '_ci-evidence.py')
receipt = module('receipt_regression', HERE / '_ci-receipt.py')


class SourceIdentityTests(unittest.TestCase):
    def test_text_block_escape_atoms_are_consumed_left_to_right(self):
        for raw, expected in [(r'C:\\temp', 'C:\\temp'), (r'\\n', '\\n'),
                              (r'\\r', '\\r'), (r'\\"', '\\"'), (r'\\\\', '\\\\'),
                              (r'\n\t\r\b\f\"\\', '\n\t\r\b\f"\\')]:
            with self.subTest(raw=raw):
                actual = evidence.canonical_expression('"""\n' + raw + '"""', None, None, {}, '')
                self.assertEqual(actual, ['java.lang.String', expected])
        with self.assertRaisesRegex(ValueError, 'unsupported Java text block escape'):
            evidence.canonical_expression('"""\n\\q"""', None, None, {}, '')

    def test_java9_class_id_matches_capture_and_jacoco_compatibility_rule(self):
        java8 = bytes.fromhex('cafebabe0000003400010021')
        java9 = java8[:7] + b'\x35' + java8[8:]
        self.assertEqual(evidence.crc64(java9), receipt.crc64(java9))
        self.assertEqual(evidence.crc64(java9), evidence.crc64(java8))
        with self.assertRaisesRegex(ValueError, 'invalid production class file'):
            evidence.crc64(b'invalid')

    def inventory(self, root, declaration):
        (root / 'core/src/test/java/sample').mkdir(parents=True, exist_ok=True)
        (root / 'pom.xml').write_text('<project><modules><module>core</module></modules></project>')
        (root / 'core/pom.xml').write_text('<project/>')
        source = root / 'core/src/test/java/sample/IdentityTest.java'
        source.write_text('package sample; class IdentityTest {\n' + declaration + '\n}')
        target = root / 'inventory.json'
        evidence.inventory(SimpleNamespace(root=str(root), lane='ci', kind=None, parameters=None,
            source_archive=None, source_receipt=None, published_connectors='unknown',
            native_cli_presence='unknown', output=str(target)))
        return target, json.loads(target.read_text())

    def test_parameterized_test_info_does_not_consume_provider_value(self):
        with tempfile.TemporaryDirectory() as temporary:
            _, inv = self.inventory(Path(temporary),
                '@ParameterizedTest @ValueSource(strings={"case"}) void info(String value, TestInfo info) {}')
            self.assertTrue(inv['complete'], inv['unresolved'])
            expected = evidence.argument_digest([['java.lang.String', 'case'],
                                                ['injected', 'org.junit.jupiter.api.TestInfo']])
            self.assertEqual(inv['methods'][0]['argument_digests'], [expected])

    def test_repetition_info_uses_the_actual_repetition_without_provider_values(self):
        with tempfile.TemporaryDirectory() as temporary:
            _, inv = self.inventory(Path(temporary), '@RepeatedTest(2) void repeat(RepetitionInfo info) {}')
            self.assertTrue(inv['complete'], inv['unresolved'])
            expected = [evidence.argument_digest([['repetition', n, 2]]) for n in (1, 2)]
            self.assertEqual(inv['methods'][0]['argument_digests'], expected)

    def test_null_and_empty_sources_preserve_injected_test_info(self):
        with tempfile.TemporaryDirectory() as temporary:
            _, inv = self.inventory(Path(temporary),
                '@ParameterizedTest @NullAndEmptySource void info(String value, TestInfo info) {}')
            self.assertTrue(inv['complete'], inv['unresolved'])
            self.assertEqual(inv['methods'][0]['argument_digests'], [
                evidence.argument_digest([value, ['injected', 'org.junit.jupiter.api.TestInfo']])
                for value in (['null'], ['java.lang.String', ''])])

    def test_repetition_info_is_not_a_parameterized_test_injection(self):
        with tempfile.TemporaryDirectory() as temporary:
            _, inv = self.inventory(Path(temporary),
                '@ParameterizedTest @ValueSource(strings={"case"}) void info(String value, RepetitionInfo info) {}')
            self.assertFalse(inv['complete'])

    def observe(self, root, inventory, name, skipped=True, markers=''):
        report = root / 'core/target/surefire-reports/TEST-sample.IdentityTest.xml'
        report.parent.mkdir(parents=True, exist_ok=True)
        report.write_text('<testsuite name="sample.IdentityTest" tests="1" skipped="' + str(int(skipped))
            + '" errors="0" failures="0"><testcase classname="sample.IdentityTest" name="' + name + '">'
            + ('<skipped message="disabled by source"/>' if skipped else '')
            + '<system-out>' + markers + '</system-out></testcase></testsuite>')
        output = root / 'observation.json'
        evidence.reports(SimpleNamespace(root=str(root), inventory=str(inventory), lane='ci',
            artifact_manifest=None, shard_plan=None, class_gates=None, inputs=None, binding=None, output=str(output)))
        return json.loads(output.read_text())

    def test_disabled_template_has_one_source_bound_markerless_skip(self):
        for annotation in ('@Disabled', '@Disabled("reason")', '@org.junit.jupiter.api.Disabled'):
            with self.subTest(annotation=annotation), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                inventory, inv = self.inventory(root, annotation
                    + ' @ParameterizedTest @ValueSource(strings={"a", "b"}) void disabled(String value) {}')
                self.assertTrue(inv['complete'], inv['unresolved'])
                observed = self.observe(root, inventory, 'disabled(String)')
                self.assertTrue(observed['complete'], observed['unresolved'])
                self.assertEqual([case['parameter'] for case in observed['cases']],
                                 [case['parameter'] for case in inv['cases']])
                self.assertEqual(len(inv['cases']), 1)
                for name, skipped in [('disabled(String)[1]', True), ('disabled(String)', False), ('disabled(int)', True)]:
                    rejected = self.observe(root, inventory, name, skipped=skipped)
                    self.assertFalse(rejected['complete'], (name, skipped))

    def test_enabled_template_still_requires_runtime_identity(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            inventory, _ = self.inventory(root,
                '@ParameterizedTest @ValueSource(strings={"a"}) void enabled(String value) {}')
            self.assertFalse(self.observe(root, inventory, 'enabled(String)[1]')['complete'])

    def test_disabled_text_in_metadata_does_not_disable_an_enabled_template(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            inventory, inv = self.inventory(root,
                '@DisplayName("@Disabled") @ParameterizedTest @ValueSource(strings={"a"}) void enabled(String value) {}')
            self.assertTrue(inv['complete'], inv['unresolved'])
            self.assertIsNone(inv['methods'][0]['disabled_source'])
            self.assertEqual(inv['methods'][0]['mapping'], 'argument-marker')
            self.assertFalse(self.observe(root, inventory, 'enabled(String)')['complete'])


def junit_readback(report):
    with tempfile.TemporaryDirectory() as temporary:
        root = Path(temporary)
        (root / 'core/src/test/java/io/tapstate/testsupport').mkdir(parents=True)
        (root / 'pom.xml').write_text('<project><modules><module>core</module></modules></project>')
        (root / 'core/pom.xml').write_text('<project/>')
        shutil.copyfile(HERE.parents[1] / 'test-support/src/test/java/io/tapstate/testsupport/CiInjectedEvidenceTest.java',
                        root / 'core/src/test/java/io/tapstate/testsupport/CiInjectedEvidenceTest.java')
        target = root / 'inventory.json'
        evidence.inventory(SimpleNamespace(root=str(root), lane='ci', kind=None, parameters=None,
            source_archive=None, source_receipt=None, published_connectors='unknown',
            native_cli_presence='unknown', output=str(target)))
        reports = root / 'core/target/surefire-reports'
        reports.mkdir(parents=True)
        shutil.copyfile(report, reports / report.name)
        observed = root / 'observed.json'
        evidence.reports(SimpleNamespace(root=str(root), inventory=str(target), lane='ci',
            artifact_manifest=None, shard_plan=None, class_gates=None, inputs=None, binding=None, output=str(observed)))
        inv, value = json.loads(target.read_text()), json.loads(observed.read_text())
        assert inv['complete'] and value['complete'], (inv['unresolved'], value['unresolved'])
        assert sorted(map(evidence.identity, inv['cases'])) == sorted(map(evidence.identity, value['cases']))
        assert len(value['cases']) == 6 and sum(case['status'] == 'skipped' for case in value['cases']) == 2
        print('Actual JUnit XML and source identities agree: 4 executed, 2 disabled templates')


if __name__ == '__main__':
    if len(sys.argv) == 3 and sys.argv[1] == '--junit-report':
        junit_readback(Path(sys.argv[2]))
    else:
        unittest.main()
