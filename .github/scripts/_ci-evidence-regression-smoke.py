#!/usr/bin/env python3
"""Discriminate source identities from Java escapes, injection and disabled templates."""
import importlib.util
import hashlib
import json
from pathlib import Path
import shutil
import sys
import tempfile
from types import SimpleNamespace
import unittest
import xml.etree.ElementTree as ET

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

    def inventory(self, root, declaration, class_annotations='', parent=''):
        (root / 'core/src/test/java/sample').mkdir(parents=True, exist_ok=True)
        (root / 'pom.xml').write_text('<project><modules><module>core</module></modules></project>')
        (root / 'core/pom.xml').write_text('<project/>')
        source = root / 'core/src/test/java/sample/IdentityTest.java'
        source.write_text('package sample; ' + class_annotations + ' class IdentityTest ' + parent + ' {\n' + declaration + '\n}')
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

    def observe(self, root, inventory, name, skipped=True, markers='', classname='sample.IdentityTest'):
        report = root / 'core/target/surefire-reports/TEST-sample.IdentityTest.xml'
        report.parent.mkdir(parents=True, exist_ok=True)
        report.write_text('<testsuite name="sample.IdentityTest" tests="1" skipped="' + str(int(skipped))
            + '" errors="0" failures="0"><testcase classname="' + classname + '" name="' + name + '">'
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

    def test_class_disabled_template_has_one_markerless_method_skip(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            inventory, inv = self.inventory(root,
                '@ParameterizedTest @ValueSource(strings={"a", "b"}) void disabled(String value) {}',
                '@Disabled("class disabled")')
            self.assertTrue(inv['complete'], inv['unresolved'])
            observed = self.observe(root, inventory, 'disabled(String)')
            self.assertTrue(observed['complete'], observed['unresolved'])
            self.assertEqual(len(inv['cases']), 1)

    def test_superclass_disabled_annotation_does_not_disable_its_subclass(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            folder = root / 'core/src/test/java/sample'
            folder.mkdir(parents=True)
            (folder / 'Base.java').write_text('package sample; @Disabled class Base { '
                '@ParameterizedTest @ValueSource(strings={"a"}) void inherited(String value) {} }')
            _, inv = self.inventory(root, '', parent='extends Base')
            self.assertTrue(inv['complete'], inv['unresolved'])
            method = inv['methods'][0]
            self.assertIsNone(method['disabled_source'])
            self.assertEqual(method['mapping'], 'argument-marker')

    def test_subclass_disabled_annotation_applies_to_inherited_methods(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            folder = root / 'core/src/test/java/sample'
            folder.mkdir(parents=True)
            (folder / 'Base.java').write_text('package sample; class Base { '
                '@ParameterizedTest @ValueSource(strings={"a"}) void inherited(String value) {} }')
            inventory, inv = self.inventory(root, '', '@Disabled', parent='extends Base')
            self.assertTrue(inv['complete'], inv['unresolved'])
            self.assertEqual(inv['methods'][0]['mapping'], 'disabled-method')
            self.assertTrue(self.observe(root, inventory, 'inherited(String)')['complete'])

    def test_class_metadata_string_does_not_authorize_disabled_reports(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            inventory, inv = self.inventory(root,
                '@ParameterizedTest @ValueSource(strings={"a"}) void enabled(String value) {}',
                '@DisplayName("@Disabled")')
            self.assertIsNone(inv['methods'][0]['disabled_source'])
            self.assertFalse(self.observe(root, inventory, 'enabled(String)')['complete'])

    def test_disabled_nested_container_preserves_raw_count_and_source_skip_identities(self):
        for annotation, expected in [('@Disabled', True), ('', False)]:
            with self.subTest(annotation=annotation), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                inventory, inv = self.inventory(root,
                    '@Nested class Nested { @Test void first() {} @Test void second() {} }', annotation)
                value = self.observe(root, inventory, '', classname='sample.IdentityTest$Nested')
                self.assertEqual(value['complete'], expected, value['unresolved'])
                if expected:
                    self.assertEqual(value['actual_xml_case_count'], 1)
                    self.assertEqual(len(value['cases']), 2)
                    self.assertTrue(all(case['status'] == 'skipped' and case.get('source_disabled_container')
                                        for case in value['cases']))
                    self.assertEqual(sorted(map(evidence.identity, inv['cases'])),
                                     sorted(map(evidence.identity, value['cases'])))

    def test_disabled_varargs_signature_matches_junit_array_not_empty_type(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            inventory, inv = self.inventory(root, '@Disabled @ParameterizedTest void disabled(String... values) {}')
            self.assertTrue(inv['complete'], inv['unresolved'])
            self.assertTrue(self.observe(root, inventory, 'disabled(String[])')['complete'])
            self.assertFalse(self.observe(root, inventory, 'disabled()')['complete'])

    def test_ordinary_varargs_signature_matches_junit_array(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            inventory, _ = self.inventory(root, '@Test void ordinary(java.lang.String... values) {}')
            self.assertTrue(self.observe(root, inventory, 'ordinary(String[])', skipped=False)['complete'])
            self.assertFalse(self.observe(root, inventory, 'ordinary()', skipped=False)['complete'])

    def test_text_block_closing_indent_and_trailing_whitespace_follow_java(self):
        for raw, expected in [('"""\n            content\n        """', '    content\n'),
                              ('"""\n    content   \n\t\n    """', 'content\n\n'),
                              ('"""\r\n        content\r\n    """', '    content\n')]:
            with self.subTest(raw=raw):
                self.assertEqual(evidence.canonical_expression(raw, None, None, {}, ''),
                                 ['java.lang.String', expected])


def junit_readback(reports):
    with tempfile.TemporaryDirectory() as temporary:
        root = Path(temporary)
        (root / 'core/src/test/java/io/tapstate/testsupport').mkdir(parents=True)
        (root / 'pom.xml').write_text('<project><modules><module>core</module></modules></project>')
        (root / 'core/pom.xml').write_text('<project/>')
        for report in reports:
            classname = report.name.removeprefix('TEST-').removesuffix('.xml')
            assert classname in ('io.tapstate.testsupport.CiInjectedEvidenceTest',
                                 'io.tapstate.testsupport.CiDisabledContainerEvidenceTest'), classname
            source = Path(classname.replace('.', '/') + '.java')
            shutil.copyfile(HERE.parents[1] / 'test-support/src/test/java' / source,
                            root / 'core/src/test/java' / source)
        target = root / 'inventory.json'
        evidence.inventory(SimpleNamespace(root=str(root), lane='ci', kind=None, parameters=None,
            source_archive=None, source_receipt=None, published_connectors='unknown',
            native_cli_presence='unknown', output=str(target)))
        destination = root / 'core/target/surefire-reports'
        destination.mkdir(parents=True)
        for report in reports:
            shutil.copyfile(report, destination / report.name)
        observed = root / 'observed.json'
        evidence.reports(SimpleNamespace(root=str(root), inventory=str(target), lane='ci',
            artifact_manifest=None, shard_plan=None, class_gates=None, inputs=None, binding=None, output=str(observed)))
        inv, value = json.loads(target.read_text()), json.loads(observed.read_text())
        assert inv['complete'] and value['complete'], (inv['unresolved'], value['unresolved'])
        assert sorted(map(evidence.identity, inv['cases'])) == sorted(map(evidence.identity, value['cases']))
        methods = {evidence.method_key(method): method for method in inv['methods']}
        for case in value['cases']:
            expected = 'skipped' if methods[evidence.method_key(case)]['disabled_source'] is not None else 'executed'
            assert case['status'] == expected, case
        print('JUnit XML/source readback passed:', value['actual_xml_case_count'], 'raw cases,',
              len(value['cases']), 'source identities')


def fixture_readback(manifest):
    fixture = json.loads(manifest.read_text())
    classes = ('CiInjectedEvidenceTest', 'CiDisabledContainerEvidenceTest')
    assert fixture['schema'] == 1 and set(fixture['reports']) == {
        'TEST-io.tapstate.testsupport.' + name + '.xml' for name in classes}, 'JUnit fixture report set differs'
    assert set(fixture['source_hashes']) == {
        'test-support/src/test/java/io/tapstate/testsupport/' + name + '.java'
        for name in (*classes, 'CiCaseEvidence')}, 'JUnit fixture source set differs'
    repo = HERE.parents[1]
    namespace = {'m': 'http://maven.apache.org/POM/4.0.0'}
    versions = dict(junit=ET.parse(repo / 'bom/pom.xml').findtext('m:properties/m:junit.version', namespaces=namespace),
                    surefire=ET.parse(repo / 'pom.xml').findtext('m:properties/m:plugin.surefire.version', namespaces=namespace))
    assert fixture['versions'] == versions, 'JUnit/Surefire changed; regenerate the real report fixture'
    for source, expected in fixture['source_hashes'].items():
        assert hashlib.sha256((repo / source).read_bytes()).hexdigest() == expected, 'report fixture source differs: ' + source
    reports = []
    for name, expected in fixture['reports'].items():
        report = manifest.parent / name
        assert report.resolve().is_relative_to(manifest.parent.resolve()), 'report fixture escapes its directory'
        assert hashlib.sha256(report.read_bytes()).hexdigest() == expected, 'JUnit report fixture bytes differ: ' + name
        reports.append(report)
    junit_readback(reports)


def shard_readback(plan_path, shard_id):
    plan = json.loads(plan_path.read_text())
    shard = next(row for row in plan['shards'] if row['id'] == shard_id)
    classes = {'io.tapstate.testsupport.CiInjectedEvidenceTest', 'io.tapstate.testsupport.CiDisabledContainerEvidenceTest'}
    reports = [HERE.parents[1] / 'test-support/target/surefire-reports' / ('TEST-' + test['class'] + '.xml')
               for test in shard['tests'] if test['module'] == 'test-support' and test['kind'] == 'unit' and test['class'] in classes]
    if reports:
        junit_readback(reports)
    else:
        print('JUnit observer controls are not selected by shard', shard_id)


if __name__ == '__main__':
    if len(sys.argv) >= 3 and sys.argv[1] == '--junit-report':
        junit_readback([Path(value) for value in sys.argv[2:]])
    elif len(sys.argv) == 3 and sys.argv[1] == '--junit-fixtures':
        fixture_readback(Path(sys.argv[2]))
    elif len(sys.argv) == 4 and sys.argv[1] == '--junit-shard':
        shard_readback(Path(sys.argv[2]), sys.argv[3])
    else:
        unittest.main()
