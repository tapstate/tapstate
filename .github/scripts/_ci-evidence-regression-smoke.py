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

    def inventory(self, root, declaration, class_annotations='', parent='', preamble='', type_declaration='class IdentityTest'):
        (root / 'core/src/test/java/sample').mkdir(parents=True, exist_ok=True)
        (root / 'pom.xml').write_text('<project><modules><module>core</module></modules></project>')
        (root / 'core/pom.xml').write_text('<project/>')
        source = root / 'core/src/test/java/sample/IdentityTest.java'
        source.write_text('package sample; import org.junit.jupiter.api.*; ' + preamble + ' '
                          + class_annotations + ' ' + type_declaration + ' ' + parent + ' {\n' + declaration + '\n}')
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


    def test_wrapped_and_sealed_interface_lists_preserve_disabled_identity(self):
        cases = [('', 'implements Other,\n Marker', ''),
                 ('sealed', 'implements Marker permits Child', 'final class Child extends IdentityTest {} '),
                 ('sealed', 'implements Other,\n Marker permits Child', 'final class Child extends IdentityTest {} '),
                 ('', 'implements Mid', 'interface Mid extends Other,\n Marker {} ')]
        for modifiers, parent, extra in cases:
            for declaration in ('@Test void disabled() {}',
                    '@ParameterizedTest @ValueSource(strings={"a"}) void disabled(String value) {}'):
                with self.subTest(parent=parent, declaration=declaration), tempfile.TemporaryDirectory() as temporary:
                    inventory, inv = self.inventory(Path(temporary), declaration, modifiers, parent,
                        '@Disabled interface Marker {} interface Other {} ' + extra)
                    self.assertTrue(inv['complete'], inv['unresolved'])
                    self.assertEqual(inv['methods'][0]['mapping'], 'disabled-method')
                    name = 'disabled(String)' if 'String' in declaration else 'disabled'
                    self.assertTrue(self.observe(Path(temporary), inventory, name)['complete'])

    def test_record_components_named_permits_do_not_hide_interfaces(self):
        for declaration in ('record IdentityTest(int permits)',
                'record IdentityTest(@TA( permits = "x") int value)'):
            with self.subTest(declaration=declaration), tempfile.TemporaryDirectory() as temporary:
                _, inv = self.inventory(Path(temporary), '@Test void disabled() {}', parent='implements Marker',
                    preamble='@Disabled interface Marker {} @interface TA { String permits(); }',
                    type_declaration=declaration)
                self.assertTrue(inv['complete'], inv['unresolved'])
                self.assertEqual(inv['methods'][0]['mapping'], 'disabled-method')

    def test_type_use_annotations_do_not_hide_disabled_supertypes(self):
        for annotation in ('@TA', '@TA(value="a")'):
            with self.subTest(annotation=annotation), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                _, inv = self.inventory(root, '@Test void disabled() {}', parent='implements ' + annotation + ' Marker',
                    preamble='@Disabled interface Marker {} @java.lang.annotation.Target(java.lang.annotation.ElementType.TYPE_USE) @interface TA { String[] value() default {}; }')
                self.assertEqual([m['mapping'] for m in inv['methods']], ['disabled-method'])
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            self.inventory(root, '@Test void disabled() {}')
            (root / 'core/src/test/java/sample/Base.java').write_text('package sample; import org.junit.jupiter.api.Disabled; '
                '@java.lang.annotation.Retention(java.lang.annotation.RetentionPolicy.RUNTIME) '
                '@java.lang.annotation.Inherited @Disabled @interface Slow {} @Slow class Base {}')
            _, inv = self.inventory(root, '@Test void disabled() {}', parent='extends @TA Base',
                preamble='@java.lang.annotation.Target(java.lang.annotation.ElementType.TYPE_USE) @interface TA {}')
            self.assertEqual([m['mapping'] for m in inv['methods']], ['disabled-method'])

    def test_type_use_annotation_itself_does_not_disable_the_test(self):
        with tempfile.TemporaryDirectory() as temporary:
            _, inv = self.inventory(Path(temporary), '@Test void ordinary() {}', parent='implements @TA Marker',
                preamble='@java.lang.annotation.Retention(java.lang.annotation.RetentionPolicy.RUNTIME) '
                    '@java.lang.annotation.Target(java.lang.annotation.ElementType.TYPE_USE) @Disabled @interface TA {} interface Marker {}')
            self.assertTrue(inv['complete'], inv['unresolved'])
            self.assertEqual([m['mapping'] for m in inv['methods']], ['method'])

    def test_annotation_array_braces_preserve_class_record_and_enum_bodies(self):
        for declaration, parent in [('class IdentityTest', 'implements @TA({"a", "b"}) Marker'),
                ('record IdentityTest(@TA({"a", "b"}) int value)', '')]:
            with self.subTest(declaration=declaration), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                preamble = 'interface Marker {} @java.lang.annotation.Target({java.lang.annotation.ElementType.TYPE_USE, java.lang.annotation.ElementType.RECORD_COMPONENT}) @interface TA { String[] value(); }'
                # Check the owner first, so the old truncated span fails for the expected reason.
                text = preamble + ' ' + declaration + ' ' + parent + ' { @Test void ordinary() {} }'
                node = next(n for n in evidence.class_declarations(text, evidence.masked(text), '')
                            if n['name'] == 'IdentityTest')
                self.assertEqual(text[node['start'] + 1:node['end']].strip(), '@Test void ordinary() {}')
                _, inv = self.inventory(root, '@Test void ordinary() {}', parent=parent,
                    preamble=preamble, type_declaration=declaration)
                self.assertEqual([m['method'] for m in inv['methods']], ['ordinary'])
        text = 'enum Flavor implements @TA({"a", "b"}) Marker { ONE, TWO; }'
        node = evidence.class_declarations(text, evidence.masked(text), '')[0]
        self.assertEqual(text[node['start'] + 1:node['end']].strip(), 'ONE, TWO;')
        self.assertEqual(evidence.enum_constants(text, 'Flavor'), ['ONE', 'TWO'])

    def test_annotation_array_headers_preserve_nested_enum_argument_identities(self):
        headers = [
            'class Holder extends @TA({1, 2}) Base',
            'class Holder implements @TA({1, 2}) Marker',
            'record Holder(@TA({1, 2}) int value)',
            'interface Holder extends @TA({1, 2}) Marker',
            'enum Holder implements @TA({1, 2}) Marker',
        ]
        for header in headers:
            with self.subTest(header=header), tempfile.TemporaryDirectory() as temporary:
                preamble = ('class Base {} interface Marker {} '
                    '@java.lang.annotation.Target({java.lang.annotation.ElementType.TYPE_USE, '
                    'java.lang.annotation.ElementType.RECORD_COMPONENT}) @interface TA { int[] value(); } '
                    + header + ' { ' + ('INSTANCE; ' if header.startswith('enum ') else '')
                    + 'enum Flavor { VANILLA, CHOCOLATE } }')
                _, inv = self.inventory(Path(temporary),
                    '@ParameterizedTest @EnumSource(Holder.Flavor.class) void flavor(Holder.Flavor value) {}',
                    preamble=preamble)
                self.assertTrue(inv['complete'], inv['unresolved'])
                self.assertEqual(inv['methods'][0]['argument_digests'], [
                    evidence.argument_digest([['enum', 'sample.Holder$Flavor', value]])
                    for value in ('VANILLA', 'CHOCOLATE')])

    def test_qualified_package_fallback_keeps_nonmatching_and_nested_definitions(self):
        for annotation in ('sample.Hidden', 'sample.Holder.Hidden'):
            with self.subTest(annotation=annotation), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                self.inventory(root, '@Test void disabled() {}')
                definition = root / 'core/src/test/java/sample/Other.java'
                declaration = '@java.lang.annotation.Retention(java.lang.annotation.RetentionPolicy.RUNTIME) @Disabled @interface Hidden {}'
                definition.write_text('package sample; import org.junit.jupiter.api.Disabled; '
                    + (('class Holder { ' + declaration + ' }') if 'Holder' in annotation else declaration))
                _, inv = self.inventory(root, '@Test void disabled() {}', '@' + annotation)
                self.assertEqual([m['mapping'] for m in inv['methods']], ['disabled-method'])
                self.assertEqual(inv['sources'][definition.relative_to(root).as_posix()],
                    hashlib.sha256(definition.read_bytes()).hexdigest())

    def test_injected_temp_dir_accepts_token_separators_without_reading_annotation_strings(self):
        for separator in (' ', '\n', '/* token separator */'):
            with self.subTest(separator=separator), tempfile.TemporaryDirectory() as temporary:
                _, inv = self.inventory(Path(temporary), '@ParameterizedTest @ValueSource(strings={"a"}) '
                    'void injected(String value, @' + separator + 'TempDir java.nio.file.Path directory) {}')
                self.assertTrue(inv['complete'], inv['unresolved'])
                self.assertEqual(inv['methods'][0]['argument_digests'], [evidence.argument_digest([
                    ['java.lang.String', 'a'], ['injected', 'org.junit.jupiter.api.io.TempDir', 'java.nio.file.Path']])])
        with tempfile.TemporaryDirectory() as temporary:
            _, inv = self.inventory(Path(temporary), '@ParameterizedTest @ValueSource(strings={"a"}) '
                'void ordinary(String value, @Label("@ TempDir") java.nio.file.Path directory) {}')
            self.assertFalse(inv['complete'])

    def test_before_all_accepts_token_separators_and_retains_gate_policy(self):
        for separator in (' ', '\n', '/* token separator */'):
            with self.subTest(separator=separator), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                self.inventory(root, '@Test void ordinary() {}')
                source = root / 'core/src/test/java/sample/IdentityTest.java'
                source.write_text('package sample; class IdentityTest { @' + separator + 'BeforeAll '
                    'static void gate() { Assumptions.assumeTrue(System.getenv("TAPSTATE_ATLAS_TEST_URI") != null, "private test gate"); } }')
                rows = evidence.before_all_contracts(root, source, {})
                self.assertEqual(len(rows), 1)
                self.assertEqual(rows[0]['method'], 'gate')
                self.assertEqual(rows[0]['policy']['inputs'], ['TAPSTATE_ATLAS_TEST_URI'])

    def test_type_annotated_parent_keeps_inherited_methods_from_nonmatching_files(self):
        for same_file in (False, True):
            with self.subTest(same_file=same_file), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                self.inventory(root, '@Test void ordinary() {}')
                base = 'class Base { @org.junit.jupiter.api.Test void inherited() {} }'
                preamble = '@java.lang.annotation.Target(java.lang.annotation.ElementType.TYPE_USE) @interface TA {} '
                if same_file:
                    preamble += base
                else:
                    (root / 'core/src/test/java/sample/Other.java').write_text('package sample; ' + base)
                _, inv = self.inventory(root, '@Test void ordinary() {}', parent='extends @TA Base', preamble=preamble)
                self.assertEqual({(m['class'], m['method']) for m in inv['methods']},
                    {('sample.IdentityTest', 'ordinary'), ('sample.IdentityTest', 'inherited')})

    def test_qualified_fallback_preserves_ambiguity_on_repeated_reads(self):
        for matching_file in (False, True):
            with self.subTest(matching_file=matching_file), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                self.inventory(root, '@Test void ordinary() {}')
                source = root / 'core/src/test/java/sample/IdentityTest.java'
                source.with_name('Other.java').write_text('package sample; @interface Hidden {}')
                second = root / 'core/src/main/java/sample' if matching_file else source.parent
                second.mkdir(parents=True, exist_ok=True)
                (second / ('Hidden.java' if matching_file else 'Duplicate.java')).write_text(
                    'package sample; @interface Hidden {}')
                lookup = evidence.DisabledLookup(root, {})
                for _ in range(2):
                    with self.assertRaisesRegex(ValueError, 'ambiguous'):
                        lookup.resolve(source, 'sample.Hidden')

    def test_qualified_annotation_tokens_reconcile_plain_cases_and_exported_hashes(self):
        for separator in (' . ', '/* before */ . /* after */', '\n.\n'):
            with self.subTest(separator=separator), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                annotation = '@' + separator.join('org.junit.jupiter.api.Test'.split('.'))
                inventory, inv = self.inventory(root, '@Test void ordinary() {} ' + annotation + ' void dotted() {}')
                self.assertTrue(inv['complete'], inv['unresolved'])
                self.assertEqual({m['method'] for m in inv['methods']}, {'ordinary', 'dotted'})
                report = root / 'core/target/surefire-reports/TEST-sample.IdentityTest.xml'
                report.parent.mkdir(parents=True)
                report.write_text('<testsuite name="sample.IdentityTest" tests="2" skipped="0" errors="0" failures="0">'
                    '<testcase classname="sample.IdentityTest" name="ordinary"/>'
                    '<testcase classname="sample.IdentityTest" name="dotted"/></testsuite>')
                observation = root / 'observation.json'
                evidence.reports(SimpleNamespace(root=str(root), inventory=str(inventory), lane='ci',
                    artifact_manifest=None, shard_plan=None, class_gates=None, inputs=None, binding=None, output=str(observation)))
                self.assertTrue(json.loads(observation.read_text())['complete'])
                hashes = []
                for source in (inventory, observation):
                    target = source.with_suffix('.hashes.json')
                    evidence.export_hashes(SimpleNamespace(input=str(source), output=str(target)))
                    hashes.append({r['case_digest'] for r in json.loads(target.read_text())['cases']})
                self.assertEqual(hashes[0], hashes[1])
                self.assertEqual(len(hashes[0]), 2)

    def test_qualified_disabled_tokens_preserve_markerless_template_skip(self):
        for separator in (' . ', '/* before */ . /* after */', '\n.\n'):
            with self.subTest(separator=separator), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                annotation = '@' + separator.join('org.junit.jupiter.api.Disabled'.split('.'))
                inventory, inv = self.inventory(root, annotation + '("disabled reason") '
                    '@ParameterizedTest @ValueSource(strings={"a", "b"}) void disabled(String value) {}')
                self.assertEqual(inv['methods'][0]['mapping'], 'disabled-method')
                self.assertEqual(len(inv['cases']), 1)
                self.assertTrue(self.observe(root, inventory, 'disabled(String)')['complete'])
                declarations = evidence.declarations((root / 'core/src/test/java/sample/IdentityTest.java').read_text(),
                    'IdentityTest', 'sample.')
                self.assertEqual(declarations[0]['disabled_source'], '"disabled reason"')

    def test_qualified_provider_and_temp_dir_tokens_preserve_injected_arguments(self):
        for separator in (' . ', '/* before */ . /* after */', '\n.\n'):
            with self.subTest(separator=separator), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                def annotation(name):
                    return '@' + separator.join(name.split('.'))
                inventory, inv = self.inventory(root, '@ParameterizedTest '
                    + annotation('org.junit.jupiter.params.provider.ValueSource') + '(strings={"a"}) '
                    'void injected(String value, ' + annotation('org.junit.jupiter.api.io.TempDir') + ' java.nio.file.Path directory) {}')
                self.assertTrue(inv['complete'], inv['unresolved'])
                self.assertEqual(len(inv['methods']), 1)
                expected = evidence.argument_digest([['java.lang.String', 'a'],
                    ['injected', 'org.junit.jupiter.api.io.TempDir', 'java.nio.file.Path']])
                self.assertEqual(inv['methods'][0]['argument_digests'], [expected])
                marker = dict(schema=1, unique_id='dotted-annotation', stable=True, **{'class': 'sample.IdentityTest'},
                    method='injected', argument_digest=expected)
                markers = '\n'.join('tapstate.ci-case=' + json.dumps(marker | extra) for extra in
                    [dict(phase='start'), dict(phase='finish', status='executed')])
                self.assertTrue(self.observe(root, inventory, 'injected(String, Path)[1]', skipped=False, markers=markers)['complete'])

    def test_qualified_before_all_tokens_preserve_class_gate_contract(self):
        for separator in (' . ', '/* before */ . /* after */', '\n.\n'):
            with self.subTest(separator=separator), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                self.inventory(root, '@Test void ordinary() {}')
                source = root / 'core/src/test/java/sample/IdentityTest.java'
                annotation = '@' + separator.join('org.junit.jupiter.api.BeforeAll'.split('.'))
                source.write_text('package sample; class IdentityTest { ' + annotation + ' static void gate() { '
                    'Assumptions.assumeTrue(System.getenv("TAPSTATE_ATLAS_TEST_URI") != null, "private test gate"); } }')
                contracts = evidence.before_all_contracts(root, source, {})
                self.assertEqual(len(contracts), 1)
                self.assertEqual(contracts[0]['policy']['inputs'], ['TAPSTATE_ATLAS_TEST_URI'])

    def test_qualified_annotations_do_not_authorize_foreign_disabled_or_unknown_providers(self):
        for annotation in ('@sample.Disabled("metadata")', '@sample /* qualifier */ . Disabled("metadata")'):
            for after_test in (False, True):
                with self.subTest(annotation=annotation, after_test=after_test), tempfile.TemporaryDirectory() as temporary:
                    root = Path(temporary)
                    inventory, inv = self.inventory(root, ('@Test ' + annotation if after_test else annotation + ' @Test')
                        + ' void ordinary() {}', preamble='@java.lang.annotation.Retention(java.lang.annotation.RetentionPolicy.RUNTIME) '
                            '@interface Disabled { String value(); }')
                    self.assertTrue(inv['complete'], inv['unresolved'])
                    self.assertIsNone(inv['methods'][0]['disabled_source'])
                    self.assertTrue(self.observe(root, inventory, 'ordinary', skipped=False)['complete'])
                    text = (root / 'core/src/test/java/sample/IdentityTest.java').read_text()
                    self.assertIsNone(evidence.declarations(text, 'IdentityTest', 'sample.')[0]['disabled_source'])
        with tempfile.TemporaryDirectory() as temporary:
            _, inv = self.inventory(Path(temporary), '@org . junit . jupiter . params . ParameterizedTest '
                '@org . junit . jupiter . params . provider . MethodSource("unresolvedProvider") void computed(String value) {}')
            self.assertEqual(len(inv['methods']), 1)
            self.assertFalse(inv['complete'])
            self.assertIsNone(inv['methods'][0]['parameters'])

    def test_before_all_does_not_read_annotation_argument_strings_as_callbacks(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            self.inventory(root, '@Test void ordinary() {}')
            source = root / 'core/src/test/java/sample/IdentityTest.java'
            source.write_text('package sample; class IdentityTest { @Label("@ BeforeAll") '
                'static void ordinary() { Assumptions.assumeTrue(System.getenv("TAPSTATE_ATLAS_TEST_URI") != null, "not a callback"); } }')
            self.assertEqual(evidence.before_all_contracts(root, source, {}), [])

    def test_nested_annotation_arguments_preserve_nested_type_owners(self):
        text = 'class IdentityTest implements @TA(value=@Nested({"a", "b"})) Marker { '
        text += '@Nested class Child { @Test void nested() {} } @Test void outer() {} }'
        declarations = evidence.declarations(text, 'IdentityTest', '')
        self.assertEqual({(m['class'], m['method']) for m in declarations},
                         {('IdentityTest$Child', 'nested'), ('IdentityTest', 'outer')})

    def test_annotation_name_token_separators_preserve_skip_contract(self):
        for separator in (' ', '\n', '/* token separator */'):
            for annotation in ('Disabled', 'Slow'):
                with self.subTest(separator=separator, annotation=annotation), tempfile.TemporaryDirectory() as temporary:
                    root = Path(temporary)
                    inventory, inv = self.inventory(root,
                        '@' + separator + annotation + '("reason") @Test void disabled() {}',
                        preamble='@java.lang.annotation.Retention(java.lang.annotation.RetentionPolicy.RUNTIME) '
                            '@Disabled @interface Slow { String value(); }')
                    self.assertTrue(inv['complete'], inv['unresolved'])
                    self.assertEqual(len(inv['methods']), 1)
                    self.assertEqual(inv['methods'][0]['mapping'], 'disabled-method')
                    self.assertTrue(self.observe(root, inventory, 'disabled')['complete'])

    def test_test_annotation_name_accepts_token_separators(self):
        for separator in (' ', '\n', '/* token separator */'):
            text = 'package sample; class IdentityTest { @' + separator + 'Test void ordinary() {} }'
            declarations = evidence.declarations(text, 'IdentityTest', 'sample.')
            self.assertEqual([row['method'] for row in declarations], ['ordinary'])

    def test_annotation_type_token_separators_preserve_runtime_composition(self):
        for separator in (' ', '\n', '/* token separator */'):
            with self.subTest(separator=separator), tempfile.TemporaryDirectory() as temporary:
                _, inv = self.inventory(Path(temporary), '@Test void disabled() {}', '@Slow',
                    preamble='@java.lang.annotation.Retention(java.lang.annotation.RetentionPolicy.RUNTIME) '
                        '@Disabled @' + separator + 'interface Slow {}')
                self.assertTrue(inv['complete'], inv['unresolved'])
                self.assertEqual(inv['methods'][0]['mapping'], 'disabled-method')

    def test_csv_text_block_uses_java_whitespace_without_losing_content(self):
        for character in ('\u00a0', '\v', '\u2028'):
            with self.subTest(character=repr(character)), tempfile.TemporaryDirectory() as temporary:
                value = 'a' + character + 'b'
                _, inv = self.inventory(Path(temporary), '@ParameterizedTest @CsvSource(textBlock="""\n'
                    + '  ' + value + '\n  """) void csv(String value) {}')
                self.assertTrue(inv['complete'], inv['unresolved'])
                self.assertEqual(inv['methods'][0]['argument_digests'],
                    [evidence.argument_digest([['java.lang.String', value]])])
        with self.assertRaisesRegex(ValueError, 'escapes'):
            evidence.csv_text_block('"""\n  a\\nb\n  """')

    def test_named_hits_and_misses_stop_repeating_filesystem_probes(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            self.inventory(root, '@Test void ordinary() {}')
            definition = root / 'core/src/test/java/sample/Marker.java'
            definition.write_text('package sample; interface Marker {}')
            sources = {}
            lookup = evidence.DisabledLookup(root, sources)
            original = Path.is_file
            with mock.patch.object(Path, 'is_file', autospec=True, side_effect=original) as probes:
                for name in ('sample.Marker', 'missing.Annotation'):
                    result = lookup.named(name)
                    count = probes.call_count
                    self.assertEqual(lookup.named(name), result)
                    self.assertEqual(probes.call_count, count)
            self.assertEqual(sources[definition.relative_to(root).as_posix()],
                hashlib.sha256(definition.read_bytes()).hexdigest())

    def test_package_fallback_scans_once_and_keeps_nonmatching_file_names(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            self.inventory(root, '@Test void ordinary() {}')
            source = root / 'core/src/test/java/sample/IdentityTest.java'
            definition = source.with_name('Other.java')
            definition.write_text('package sample; @interface Hidden {}')
            sources = {}
            lookup = evidence.DisabledLookup(root, sources)
            original = Path.glob
            with mock.patch.object(Path, 'glob', autospec=True, side_effect=original) as scans:
                self.assertIsNone(lookup.resolve(source, 'Unknown')[1])
                count = scans.call_count
                self.assertIsNone(lookup.resolve(source, 'DifferentUnknown')[1])
                target = lookup.resolve(source, 'Hidden')[1]
                self.assertIsNotNone(target)
                self.assertEqual(scans.call_count, count)
            self.assertEqual(target[0], definition)
            self.assertEqual(sources[definition.relative_to(root).as_posix()],
                hashlib.sha256(definition.read_bytes()).hexdigest())

    def test_lookup_cache_is_scoped_to_one_inventory_and_preserves_ambiguity(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            self.inventory(root, '@Test void ordinary() {}')
            definition = root / 'core/src/test/java/sample/Marker.java'
            first = evidence.DisabledLookup(root, {})
            self.assertIsNone(first.named('sample.Marker'))
            definition.write_text('package sample; interface Marker {}')
            sources = {}
            second = evidence.DisabledLookup(root, sources)
            self.assertIsNotNone(second.named('sample.Marker'))
            self.assertEqual(sources[definition.relative_to(root).as_posix()],
                hashlib.sha256(definition.read_bytes()).hexdigest())
            duplicate = root / 'core/src/main/java/sample/Marker.java'
            duplicate.parent.mkdir(parents=True)
            duplicate.write_text(definition.read_text())
            third = evidence.DisabledLookup(root, {})
            for _ in range(2):
                with self.assertRaisesRegex(ValueError, 'ambiguous'):
                    third.named('sample.Marker')


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
