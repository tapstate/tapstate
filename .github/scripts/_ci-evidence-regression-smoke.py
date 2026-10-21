#!/usr/bin/env python3
"""Discriminate source identities from Java escapes, injection and disabled templates."""
import importlib.util
import hashlib
import io
import json
from contextlib import redirect_stdout
from pathlib import Path
import shutil
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest import mock
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

    def inventory(self, root, declaration, class_annotations='', parent='', preamble=''):
        (root / 'core/src/test/java/sample').mkdir(parents=True, exist_ok=True)
        (root / 'pom.xml').write_text('<project><modules><module>core</module></modules></project>')
        (root / 'core/pom.xml').write_text('<project/>')
        source = root / 'core/src/test/java/sample/IdentityTest.java'
        source.write_text('package sample; import org.junit.jupiter.api.*; ' + preamble + ' '
                          + class_annotations + ' class IdentityTest ' + parent + ' {\n' + declaration + '\n}')
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
            (folder / 'Base.java').write_text('package sample; @org.junit.jupiter.api.Disabled class Base { '
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

    def test_interfaces_and_runtime_composed_annotations_disable_templates(self):
        runtime = '@java.lang.annotation.Retention(java.lang.annotation.RetentionPolicy.RUNTIME) '
        definitions = runtime + '@Disabled @interface Slow {} '
        for preamble, annotation, parent in [('@Disabled interface Slow {}', '', 'implements Slow'),
                ('@Disabled interface Parent {} interface Slow extends Parent {}', '', 'implements Slow'),
                (definitions, '@Slow', ''),
                (definitions + runtime + '@Slow @interface Slower {}', '@Slower', '')]:
            with self.subTest(preamble=preamble), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                inventory, inv = self.inventory(root,
                    '@ParameterizedTest @ValueSource(strings={"a"}) void disabled(String value) {}',
                    annotation, parent, preamble)
                self.assertTrue(inv['complete'], inv['unresolved'])
                self.assertEqual(inv['methods'][0]['mapping'], 'disabled-method')
                self.assertTrue(self.observe(root, inventory, 'disabled(String)')['complete'])

    def test_composed_method_annotation_is_bound_to_its_definition_file(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / 'core/src/test/java/sample/Slow.java'
            source.parent.mkdir(parents=True)
            source.write_text('package sample; @java.lang.annotation.Retention(java.lang.annotation.RetentionPolicy.RUNTIME) '
                              '@org.junit.jupiter.api.Disabled @interface Slow {}')
            inventory, inv = self.inventory(root,
                '@Slow @ParameterizedTest @ValueSource(strings={"a"}) void disabled(String value) {}')
            self.assertEqual(inv['methods'][0]['mapping'], 'disabled-method')
            self.assertEqual(inv['sources']['core/src/test/java/sample/Slow.java'], hashlib.sha256(source.read_bytes()).hexdigest())
            self.assertTrue(self.observe(root, inventory, 'disabled(String)')['complete'])

    def test_inherited_composed_annotation_differs_from_direct_disabled(self):
        runtime = '@java.lang.annotation.Retention(java.lang.annotation.RetentionPolicy.RUNTIME) '
        for inherited in ('', '@java.lang.annotation.Inherited '):
            with self.subTest(inherited=inherited), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                source = root / 'core/src/test/java/sample/Base.java'
                source.parent.mkdir(parents=True)
                source.write_text('package sample; ' + runtime + inherited + '@org.junit.jupiter.api.Disabled @interface Slow {} '
                                  '@Slow class Base { @ParameterizedTest @ValueSource(strings={"a"}) void test(String value) {} }')
                _, inv = self.inventory(root, '', parent='extends Base')
                self.assertTrue(inv['complete'], inv['unresolved'])
                self.assertEqual(inv['methods'][0]['mapping'], 'disabled-method' if inherited else 'argument-marker')

    def test_non_runtime_or_same_name_annotations_cannot_authorize_skips(self):
        for definitions, annotation in [('@interface Disabled {}', '@Disabled'),
                ('@Disabled @interface Slow {}', '@Slow'),
                ('@java.lang.annotation.Retention(java.lang.annotation.RetentionPolicy.RUNTIME) @B @interface A {} '
                 '@java.lang.annotation.Retention(java.lang.annotation.RetentionPolicy.RUNTIME) @A @interface B {}', '@A')]:
            with self.subTest(annotation=annotation), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                inventory, inv = self.inventory(root,
                    '@ParameterizedTest @ValueSource(strings={"a"}) void enabled(String value) {}',
                    annotation, preamble=definitions)
                self.assertTrue(inv['complete'], inv['unresolved'])
                self.assertIsNone(inv['methods'][0]['disabled_source'])
                self.assertFalse(self.observe(root, inventory, 'enabled(String)')['complete'])

    def test_static_import_and_inherited_member_annotations_shadow_jupiter(self):
        for preamble, parent, kind in [('import static sample.Tools.Disabled;', '', 'class'),
                ('import static sample.Tools.*;', '', 'class'), ('', 'extends Tools', 'class'),
                ('', 'implements Tools', 'interface'), ('', 'implements Mid', 'interface'),
                ('@java.lang.annotation.Retention(java.lang.annotation.RetentionPolicy.RUNTIME) '
                 '@org.junit.jupiter.api.Disabled @interface Disabled {}', 'extends Tools', 'class')]:
            with self.subTest(parent=parent), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                source = root / 'core/src/test/java/sample/Tools.java'
                source.parent.mkdir(parents=True)
                source.write_text('package sample; ' + kind + ' Tools { '
                    '@java.lang.annotation.Retention(java.lang.annotation.RetentionPolicy.RUNTIME) @interface Disabled {} }'
                    + (' interface Mid extends Tools {}' if kind == 'interface' else ''))
                inventory, inv = self.inventory(root,
                    '@Disabled @ParameterizedTest @ValueSource(strings={"a"}) void enabled(String value) {}',
                    parent=parent, preamble=preamble)
                if preamble == 'import static sample.Tools.*;':
                    self.assertFalse(inv['complete'], 'two on-demand imports must not guess an annotation binding')
                    self.assertTrue(any('ambiguous' in row['reason'] for row in inv['unresolved']))
                    continue
                self.assertTrue(inv['complete'], inv['unresolved'])
                self.assertIsNone(inv['methods'][0]['disabled_source'])
                self.assertFalse(self.observe(root, inventory, 'enabled(String)')['complete'])

    def test_generic_bounds_are_not_superclasses_or_parent_interfaces(self):
        runtime = '@java.lang.annotation.Retention(java.lang.annotation.RetentionPolicy.RUNTIME) '
        preamble = runtime + '@java.lang.annotation.Inherited @Disabled @interface Slow {} @Slow class Bound {} '
        for definitions, parent, expected in [(preamble, '<T extends Bound>', 'argument-marker'),
                ('@Disabled interface Marker {} interface Mid<T extends Runnable> extends Marker {}',
                 'implements Mid<Runnable>', 'disabled-method')]:
            with self.subTest(parent=parent), tempfile.TemporaryDirectory() as temporary:
                _, inv = self.inventory(Path(temporary),
                    '@ParameterizedTest @ValueSource(strings={"a"}) void test(String value) {}',
                    parent=parent, preamble=definitions)
                self.assertTrue(inv['complete'], inv['unresolved'])
                self.assertEqual(inv['methods'][0]['mapping'], expected)

    def test_inherited_member_visibility_controls_annotation_binding(self):
        runtime = '@java.lang.annotation.Retention(java.lang.annotation.RetentionPolicy.RUNTIME) '
        for access, package, disabled in [('private', 'sample', False), ('', 'other', False),
                                          ('', 'sample', True), ('protected', 'other', True), ('public', 'other', True)]:
            with self.subTest(access=access, package=package), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                source = root / ('core/src/test/java/' + package + '/Base.java')
                source.parent.mkdir(parents=True)
                source.write_text('package ' + package + '; public class Base { ' + runtime
                    + '@org.junit.jupiter.api.Disabled ' + access + ' @interface Slow {} }')
                _, inv = self.inventory(root,
                    '@Slow @ParameterizedTest @ValueSource(strings={"a"}) void test(String value) {}',
                    parent='extends Base', preamble=('import other.Base; ' if package == 'other' else '')
                    + runtime + '@interface Slow {}')
                self.assertTrue(inv['complete'], inv['unresolved'])
                self.assertEqual(inv['methods'][0]['mapping'], 'disabled-method' if disabled else 'argument-marker')

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

    def test_text_block_whitespace_matches_java_instead_of_python(self):
        for char in ('\u00a0', '\u0085', '\u2007', '\u202f'):
            for content in ('a' + char + 'b', char + 'a' + char):
                with self.subTest(content=repr(content)):
                    self.assertEqual(evidence.canonical_expression('"""\n' + content + '\n"""', None, None, {}, ''),
                                     ['java.lang.String', content + '\n'])
        for char in ('\v', '\u001c', '\u1680', '\u2000', '\u2028', '\u2029', '\u205f', '\u3000'):
            with self.subTest(char=repr(char)):
                self.assertEqual(evidence.canonical_expression('"""\n' + char + 'a' + char + '\n"""', None, None, {}, ''),
                                 ['java.lang.String', char + 'a\n'])
                self.assertEqual(evidence.canonical_expression('"""\n' + char + 'a' + char + '\n' + char + '"""', None, None, {}, ''),
                                 ['java.lang.String', 'a\n'])


class LiveReadbackTests(unittest.TestCase):
    def fixture(self, root):
        repo = HERE.parents[1]
        scripts = root / '.github/scripts'
        stored = scripts / 'fixtures/ci-evidence-junit'
        shutil.copytree(HERE / 'fixtures/ci-evidence-junit', stored)
        manifest = stored / 'manifest.json'
        value = json.loads(manifest.read_text())
        moved = {}
        for source, checksum in value['source_hashes'].items():
            target = source.replace('test-support/', 'moved-controls/', 1)
            (root / target).parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(repo / source, root / target)
            moved[target] = checksum
        value['source_hashes'] = moved
        manifest.write_text(json.dumps(value))
        (root / 'bom').mkdir()
        shutil.copyfile(repo / 'bom/pom.xml', root / 'bom/pom.xml')
        shutil.copyfile(repo / 'pom.xml', root / 'pom.xml')
        tests = [{'module': 'moved-controls', 'kind': 'unit', 'class': name[5:-4]}
                 for name in value['reports']]
        plan = {'schema': 1, 'expected': tests, 'shards': [{'id': 'owner', 'tests': tests}, {'id': 'other', 'tests': []}]}
        path = root / 'plan.json'
        path.write_text(json.dumps(plan))
        reports = root / 'moved-controls/target/surefire-reports'
        reports.mkdir(parents=True)
        for name in value['reports']:
            shutil.copyfile(stored / name, reports / name)
        return scripts, path, plan, reports

    def test_relocated_controls_are_read_from_the_planned_module(self):
        with tempfile.TemporaryDirectory() as temporary:
            scripts, path, _, reports = self.fixture(Path(temporary))
            calls = []
            with mock.patch.dict(globals(), HERE=scripts, junit_readback=lambda rows, *args, **kwargs: calls.append(rows)):
                shard_readback(path, 'owner')
            self.assertEqual(len(calls), 1, 'selected controls must execute live readback')
            self.assertEqual(set(calls[0]), set(reports.glob('*.xml')))

    def test_relocated_reports_reconcile_with_their_manifest_sources(self):
        with tempfile.TemporaryDirectory() as temporary:
            scripts, path, _, _ = self.fixture(Path(temporary))
            output = io.StringIO()
            with mock.patch.dict(globals(), HERE=scripts), redirect_stdout(output):
                shard_readback(path, 'owner')
            self.assertIn('JUnit XML/source readback passed:', output.getvalue())

    def test_relocated_owner_cannot_succeed_without_its_report(self):
        with tempfile.TemporaryDirectory() as temporary:
            scripts, path, _, reports = self.fixture(Path(temporary))
            next(reports.glob('*.xml')).unlink()
            with mock.patch.dict(globals(), HERE=scripts), self.assertRaisesRegex((ValueError, AssertionError), 'report'):
                shard_readback(path, 'owner')

    def test_every_control_requires_exactly_one_plan_owner(self):
        for duplicate in (False, True):
            with self.subTest(duplicate=duplicate), tempfile.TemporaryDirectory() as temporary:
                scripts, path, plan, _ = self.fixture(Path(temporary))
                if duplicate:
                    plan['shards'][1]['tests'].append(plan['shards'][0]['tests'][0])
                else:
                    plan['shards'][0]['tests'].pop()
                path.write_text(json.dumps(plan))
                with mock.patch.dict(globals(), HERE=scripts), self.assertRaisesRegex((ValueError, AssertionError), 'control'):
                    shard_readback(path, 'other')

    def test_non_owner_only_skips_after_global_control_validation(self):
        with tempfile.TemporaryDirectory() as temporary:
            scripts, path, _, reports = self.fixture(Path(temporary))
            shutil.rmtree(reports)
            with mock.patch.dict(globals(), HERE=scripts, junit_readback=mock.Mock()) as patched:
                shard_readback(path, 'other')
                patched['junit_readback'].assert_not_called()


def fixture_layout(manifest, repo):
    fixture = json.loads(manifest.read_text())
    assert fixture['schema'] == 1 and fixture['reports'], 'JUnit fixture report set is empty'
    sources, controls = {}, {}
    for name, expected in fixture['source_hashes'].items():
        path = repo / name
        assert not Path(name).is_absolute() and '..' not in Path(name).parts, 'JUnit fixture source escapes repository'
        assert path.resolve().is_relative_to(repo.resolve()), 'JUnit fixture source escapes repository'
        assert hashlib.sha256(path.read_bytes()).hexdigest() == expected, 'report fixture source differs: ' + name
        module, separator, source = name.partition('/src/test/java/')
        assert separator and module and source.endswith('.java'), 'JUnit fixture source is not a test Java file'
        classname = source[:-5].replace('/', '.')
        assert classname not in sources, 'duplicate JUnit fixture source class'
        sources[classname] = dict(module=module, source=source, path=path)
    for name in fixture['reports']:
        assert name.startswith('TEST-') and name.endswith('.xml') and Path(name).name == name, 'invalid JUnit fixture report name'
        classname = name[5:-4]
        assert classname in sources, 'JUnit fixture report has no source: ' + classname
        controls[classname] = sources[classname]
        sources[classname]['control'] = True
    namespace = {'m': 'http://maven.apache.org/POM/4.0.0'}
    versions = dict(junit=ET.parse(repo / 'bom/pom.xml').findtext('m:properties/m:junit.version', namespaces=namespace),
                    surefire=ET.parse(repo / 'pom.xml').findtext('m:properties/m:plugin.surefire.version', namespaces=namespace))
    assert fixture['versions'] == versions, 'JUnit/Surefire changed; regenerate the real report fixture'
    return fixture, sources, controls


def junit_readback(reports, sources=None):
    if sources is None:
        _, sources, _ = fixture_layout(HERE / 'fixtures/ci-evidence-junit/manifest.json', HERE.parents[1])
    with tempfile.TemporaryDirectory() as temporary:
        root = Path(temporary)
        (root / 'core/src/test/java/io/tapstate/testsupport').mkdir(parents=True)
        (root / 'pom.xml').write_text('<project><modules><module>core</module></modules></project>')
        (root / 'core/pom.xml').write_text('<project/>')
        selected = {report.name.removeprefix('TEST-').removesuffix('.xml') for report in reports}
        assert selected and selected <= set(sources), 'JUnit report source is missing'
        for classname, definition in sources.items():
            # Copy supporting annotation/interface sources, but not another control's tests.
            if classname not in selected and definition.get('control', False):
                continue
            target = root / 'core/src/test/java' / definition['source']
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(definition['path'], target)
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
    fixture, sources, _ = fixture_layout(manifest, HERE.parents[1])
    reports = []
    for name, expected in fixture['reports'].items():
        report = manifest.parent / name
        assert report.resolve().is_relative_to(manifest.parent.resolve()), 'report fixture escapes its directory'
        assert hashlib.sha256(report.read_bytes()).hexdigest() == expected, 'JUnit report fixture bytes differ: ' + name
        reports.append(report)
    junit_readback(reports, sources)


def shard_readback(plan_path, shard_id):
    plan = json.loads(plan_path.read_text())
    repo = HERE.parents[1]
    _, sources, controls = fixture_layout(HERE / 'fixtures/ci-evidence-junit/manifest.json', repo)
    assert plan['schema'] == 1 and len({row['id'] for row in plan['shards']}) == len(plan['shards']), 'invalid control shard plan'
    owners = {name: [] for name in controls}
    for row in plan['shards']:
        for test in row['tests']:
            name = test['class']
            if name in controls:
                assert test['module'] == controls[name]['module'] and test['kind'] == 'unit', 'control source and plan differ: ' + name
                owners[name].append((row['id'], test))
    assert all(len(rows) == 1 for rows in owners.values()), 'every JUnit control requires exactly one plan owner'
    assert shard_id in {row['id'] for row in plan['shards']}, 'unknown control shard'
    reports = [repo / test['module'] / 'target/surefire-reports' / ('TEST-' + name + '.xml')
               for name, rows in owners.items() for owner, test in rows if owner == shard_id]
    assert all(report.is_file() for report in reports), 'selected JUnit control report is missing'
    if reports:
        junit_readback(reports, sources)
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
