#!/usr/bin/env python3
"""Default test-JAR capture and source/goal/member/scope replay controls."""
import argparse
import contextlib
import copy
import io
import os
import hashlib
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace
from unittest.mock import patch
import tempfile
import zipfile

sys.dont_write_bytecode = True
HERE = Path(__file__).resolve().parent


def module(name, file):
    spec = importlib.util.spec_from_file_location(name, HERE / file)
    value = importlib.util.module_from_spec(spec); spec.loader.exec_module(value)
    return value


roles = module('test_jar_controls', '_ci-test-jar-inputs.py')
receipt = module('test_jar_receipt_controls', '_ci-receipt.py')
reader = module('test_jar_reader_controls', '_ci-archive-inputs.py')
delta = module('test_jar_delta_controls', '_ci-group-delta.py')
count = 0


def check(value, message):
    global count
    assert value, message
    count += 1


def rejected(call, message):
    try:
        call()
    except (ValueError, OSError, KeyError, TypeError, zipfile.BadZipFile):
        check(True, message)
    else:
        raise AssertionError('accepted ' + message)


with tempfile.TemporaryDirectory(prefix='test-jar-inputs-') as temporary:
    root = Path(temporary)
    def put(name, raw):
        path = root / name; path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(raw.encode() if isinstance(raw, str) else raw)
        return path
    put('pom.xml', '<project><groupId>io.tapstate</groupId><artifactId>root</artifactId><version>${revision}</version>'
        '<modules><module>fixture</module><module>consumer</module></modules><properties><revision>1</revision><jar.version>3.4.2</jar.version></properties>'
        '<build><pluginManagement><plugins><plugin><artifactId>maven-jar-plugin</artifactId><version>${jar.version}</version>'
        '</plugin></plugins></pluginManagement></build></project>')
    child = '<parent><groupId>io.tapstate</groupId><artifactId>root</artifactId><version>${revision}</version></parent>'
    producer_pom = ('<project>' + child + '<artifactId>fixture</artifactId><build><plugins><plugin>'
        '<artifactId>maven-jar-plugin</artifactId><executions><execution><id>test-jar</id><goals><goal>test-jar</goal>'
        '</goals></execution></executions></plugin></plugins></build></project>')
    put('fixture/pom.xml', producer_pom)
    put('consumer/pom.xml', '<project>' + child + '<artifactId>consumer</artifactId></project>')
    for name in roles.PRODUCERS:
        put(name, (HERE / Path(name).name).read_bytes())
    source_files = {p.relative_to(root).as_posix(): roles.checksum(p.read_bytes()) for p in root.rglob('*') if p.is_file()}
    test_path = 'fixture/target/test-classes/io/tapstate/fixture/ExampleTest.class'
    resource_path = 'fixture/target/test-classes/fixture.txt'
    artifact = 'fixture/target/fixture-1-tests.jar'
    metadata_prefix = 'META-INF/maven/io.tapstate/fixture/'
    manifest = 'Manifest-Version: 1.0\r\nCreated-By: Maven JAR Plugin 3.4.2\r\nBuild-Jdk-Spec: 21\r\n\r\n'
    model_pom = '<project><groupId>io.tapstate</groupId><artifactId>fixture</artifactId><version>1</version></project>'
    plugin_path = 'org/apache/maven/plugins/maven-jar-plugin/3.4.2/maven-jar-plugin-3.4.2.jar'
    plugin_bytes = b'bounded plugin identity fixture'
    plugin_hash = roles.checksum(plugin_bytes)
    plugin_artifact = dict(group='org.apache.maven.plugins', artifact='maven-jar-plugin', version='3.4.2',
                           classifier=None, type='maven-plugin', scope=None, resolved=True, file=plugin_hash)

    def snapshot(version):
        bytecode = b'\xca\xfe\xba\xbe\x00\x00\x00\x41\x00\x01\x10\x01' + version.encode()
        put(test_path, bytecode); put(resource_path, 'case-local resource')
        members = {'io/tapstate/fixture/ExampleTest.class': bytecode, 'fixture.txt': b'case-local resource',
                   'META-INF/MANIFEST.MF': manifest.encode(), metadata_prefix + 'pom.xml': model_pom.encode(),
                   metadata_prefix + 'pom.properties': b'artifactId=fixture\ngroupId=io.tapstate\nversion=1\n'}
        def jar(values):
            with zipfile.ZipFile(put(artifact, b''), 'w', compression=zipfile.ZIP_DEFLATED) as stream:
                for name, raw in values.items(): stream.writestr(name, raw)
        jar(members)
        raw = (root / artifact).read_bytes()
        value = dict(source_sha='a' * 40, source_tree='b' * 40, run_id='1', run_attempt=1,
            source_files=source_files.copy(), test_files={test_path: roles.checksum(bytecode)},
            test_class_ids={test_path: receipt.crc64(bytecode)}, test_resource_files={resource_path: roles.checksum(b'case-local resource')},
            packaged_outputs={artifact: dict(sha256=roles.checksum(raw), size=len(raw), mode=420),
                              'fixture/target/fixture-1.jar': dict(sha256='f' * 64, size=20, mode=420)},
            dependency_repository_files={plugin_path: plugin_hash})
        entry = dict(identity='repository/io/tapstate/fixture/1/fixture-1-tests.jar', sha256=roles.checksum(raw), size=len(raw),
                     own_classes={'io/tapstate/fixture/ExampleTest': dict(sha256=roles.checksum(bytecode), class_id=receipt.crc64(bytecode))})
        value['resolved_test_classpaths'] = {'consumer': [entry]}
        observed_files = {}
        def file_reference(raw, label):
            identity = roles.checksum(label.encode())
            sha = roles.checksum(raw)
            reference = roles.checksum((identity + ':' + str(len(raw)) + ':' + sha).encode())
            observed_files[reference] = dict(state='observed-file', size=len(raw), sha256=sha, path_identity_sha256=identity)
            return reference
        actual_plugin = {**plugin_artifact, 'file': file_reference(plugin_bytes, 'plugin.jar')}
        actual_output = file_reference(raw, 'test-fixture.jar')
        core_reference = file_reference(b'bounded Maven core identity fixture', 'maven-core.jar')
        core_realm = dict(state='observed-realm-urls', id='d' * 64, ordered_urls=[{'file': core_reference}],
                          parent=dict(state='not-observed', reason='no-realm'), imports=[],
                          import_package_prefixes='not-observed', opaque_parent_class=None)
        realm = dict(state='observed-realm-urls', id='e' * 64, ordered_urls=[{'file': actual_plugin['file']}],
                     parent=dict(state='reference', id=core_realm['id']), imports=[core_realm],
                     import_package_prefixes='not-observed', opaque_parent_class='ClassRealm')
        rows = [dict(event='MojoStarted', execution='test-jar', project='io.tapstate:fixture:1',
                     plugin='org.apache.maven.plugins:maven-jar-plugin:3.4.2', goal='test-jar', nanos=1,
                     merged_xml_config_sha256='c' * 64),
                dict(event='MojoConfigured', execution='test-jar', project='io.tapstate:fixture:1',
                     plugin='org.apache.maven.plugins:maven-jar-plugin:3.4.2', goal='test-jar', nanos=2,
                     configured_implementation='org.apache.maven.plugins.jar.TestJarMojo', active_profile_identities=[]),
                dict(event='MojoSucceeded', execution='test-jar', project='io.tapstate:fixture:1',
                     plugin='org.apache.maven.plugins:maven-jar-plugin:3.4.2', goal='test-jar', nanos=3,
                     merged_xml_config_sha256='c' * 64, plugin_artifact=actual_plugin, plugin_artifacts=[actual_plugin.copy()], realm=realm),
                dict(event='MojoStarted', project='io.tapstate:consumer:1', project_artifacts_at_event=[
                     dict(group='io.tapstate', artifact='fixture', version='1', classifier='tests', type='test-jar',
                          scope='test', resolved=True, file=actual_output)])]
        value['maven_execution_observation'] = dict(capture_status='captured', source_sha=value['source_sha'],
            source_tree=value['source_tree'], run_id=value['run_id'], run_attempt=1, observation_metadata=dict(event_records=rows, files=observed_files))
        value['packaged_test_outputs'] = roles.capture(lambda n: (root / n).read_bytes(), lambda n: (root / n).read_bytes(), value)
        roles.validate(value['packaged_test_outputs'], value, lambda n: (root / n).read_bytes())
        return value, members, jar

    a, original_members, write_jar = snapshot('original')
    packet = a['packaged_test_outputs']; record = packet['outputs'][artifact]; model = record['model']
    check(len(packet['outputs']) == 1 and not packet['unresolved_models'], 'default source producer and complete member tree captured')
    check(roles.observed_execution(a, model), 'original goal implementation, descriptor and direct plugin realm byte bindings replay')
    check(roles.observed_test_scope(a, 'consumer', record, lambda n: (root / n).read_bytes()), 'original consumer test scope replays')
    check(roles.classpath_role(a['resolved_test_classpaths']['consumer'][0], packet['outputs']) == artifact,
          'resolved test-JAR binds exact coordinates, output bytes and class IDs')
    for label, mutate in [
        ('output hash', lambda x: x['outputs'][artifact]['output'].update(sha256='e' * 64)),
        ('missing member', lambda x: x['outputs'][artifact]['members'].pop('fixture.txt')),
        ('class ID', lambda x: x['outputs'][artifact]['class_ids'].update({'io/tapstate/fixture/ExampleTest.class': 'e' * 16})),
        ('extra metadata', lambda x: x['outputs'][artifact]['metadata'].update({'unexpected': 'a' * 64})),
        ('helper source', lambda x: x['producer_sources'].update({roles.PRODUCERS[0]: 'e' * 64})),
    ]:
        changed = copy.deepcopy(packet); mutate(changed)
        rejected(lambda: roles.validate(changed, a, lambda n: (root / n).read_bytes()), label)
    for label, change in [('foreign production class', {'io/tapstate/core/Production.class': b'production'}),
                          ('path traversal', {'../escaped': b'bad'}), ('wrong test byte', {'io/tapstate/fixture/ExampleTest.class': b'old'}),
                          ('unsupported manifest', {'META-INF/MANIFEST.MF': b'unknown'})]:
        write_jar(original_members | change)
        changed = copy.deepcopy(a); raw = (root / artifact).read_bytes()
        changed['packaged_outputs'][artifact].update(sha256=roles.checksum(raw), size=len(raw))
        rejected(lambda: roles.capture(lambda n: (root / n).read_bytes(), lambda n: (root / n).read_bytes(), changed), label)
    write_jar(original_members)
    for label, mutate in [
        ('wrong goal origin', lambda x: x['maven_execution_observation'].update(source_sha='e' * 40)),
        ('wrong implementation', lambda x: x['maven_execution_observation']['observation_metadata']['event_records'][1].update(configured_implementation='org.apache.maven.plugins.jar.JarMojo')),
        ('active profile', lambda x: x['maven_execution_observation']['observation_metadata']['event_records'][1].update(active_profile_identities=['unreviewed'])),
        ('wrong plugin bytes', lambda x: x['dependency_repository_files'].update({plugin_path: 'e' * 64})),
        ('missing descriptor artifacts', lambda x: x['maven_execution_observation']['observation_metadata']['event_records'][2].update(plugin_artifacts=[])),
        ('missing observed realm', lambda x: x['maven_execution_observation']['observation_metadata']['event_records'][2].update(realm=dict(state='not-observed', reason='no-realm'))),
        ('unknown realm URL', lambda x: x['maven_execution_observation']['observation_metadata']['event_records'][2]['realm']['ordered_urls'].append({'file': 'f' * 64})),
        ('inconsistent file reference', lambda x: next(iter(x['maven_execution_observation']['observation_metadata']['files'].values())).update(size=123)),
        ('duplicate goal', lambda x: x['maven_execution_observation']['observation_metadata']['event_records'].append(copy.deepcopy(x['maven_execution_observation']['observation_metadata']['event_records'][2]))),
    ]:
        changed = copy.deepcopy(a); mutate(changed)
        rejected(lambda: roles.observed_execution(changed, model), label)
    changed = copy.deepcopy(a)
    changed['maven_execution_observation']['observation_metadata']['event_records'][2]['realm']['ordered_urls'].reverse()
    check(roles.execution_inputs(changed, model) == roles.execution_inputs(a, model), 'single ordered URL has no artificial change')
    changed['maven_execution_observation']['observation_metadata']['event_records'][2]['realm']['ordered_urls'].append(
        copy.deepcopy(changed['maven_execution_observation']['observation_metadata']['event_records'][2]['realm']['ordered_urls'][0]))
    check(roles.execution_inputs(changed, model) != roles.execution_inputs(a, model), 'actual ordered realm URL changes are retained')
    check(roles.execution_inputs(a, model)['observed_direct_realm']['inherited_realm_inputs'] == 'not-replayed'
          and not roles.execution_inputs(a, model)['observed_direct_realm']['class_consumption_proven'],
          'inherited loaders and actual class consumption are explicitly outside direct-realm proof')
    bad_sources = {name: (root / name).read_bytes() for name in source_files}
    bad_sources['fixture/pom.xml'] = bad_sources['fixture/pom.xml'].replace(b'<artifactId>root</artifactId>', b'<artifactId>external</artifactId>')
    declared, unknown = roles.models(lambda n: bad_sources[n], {n: roles.checksum(raw) for n, raw in bad_sources.items()})
    check(not declared and 'local parent coordinates differ' in unknown['fixture/pom.xml'], 'mismatched relative parent cannot supply producer properties')
    changed = copy.deepcopy(a); changed.pop('maven_execution_observation')
    check(not roles.observed_execution(changed, model), 'missing original observation stays unknown')
    changed = copy.deepcopy(a)
    changed['maven_execution_observation']['observation_metadata']['event_records'][-1]['project_artifacts_at_event'][0]['scope'] = 'compile'
    rejected(lambda: roles.observed_test_scope(changed, 'consumer', record, lambda n: (root / n).read_bytes()), 'production consumer scope')
    changed = copy.deepcopy(a['resolved_test_classpaths']['consumer'][0]); changed['own_classes'].clear()
    rejected(lambda: roles.classpath_role(changed, packet['outputs']), 'missing resolved class bytes')
    changed = copy.deepcopy(a['resolved_test_classpaths']['consumer'][0]); changed['identity'] = 'repository/io/tapstate/other/1/fixture-1-tests.jar'
    check(roles.classpath_role(changed, packet['outputs']) is None, 'filename alone never selects a test producer')
    poms = {name: (root / name).read_bytes() for name in source_files if name.endswith('pom.xml')}
    plan = {'shards': [{'id': 'fixture', 'tests': [{'module': 'fixture'}]}]}
    def owner_results(value):
        return {'fixture': {'files': value['test_files'] | value['test_resource_files']}}
    def readback(value, results=None):
        return reader.test_jar_readback(plan, {'build': value, 'fixture': value}, {'fixture': value},
                                        owner_results(value) if results is None else results, poms)
    fact_a = readback(a)
    check(fact_a['original_owner_bytes_replayed'] and not fact_a['unverified'], 'original owner tree and original scope/goal replay')
    changed = owner_results(a); changed['fixture']['files'].pop(test_path)
    rejected(lambda: readback(a, changed), 'missing original owner byte')
    b, unused, unused = snapshot('candidate'); fact_b = readback(b)
    def payload(value, fact):
        return {'P': {'preserved_source_digest': 'a' * 64, 'production': {'files': {'production.class': 'b' * 64}},
                      'packaged_outputs': copy.deepcopy(value['packaged_outputs'])},
                'I': {'test_packaged_outputs': copy.deepcopy(fact), 'source_inventory': {'methods': {}, 'class_contracts': {}, 'sources': {},
                      'entry_contract': {}, 'cases': []}},
                'D': {'resolved_test_classpaths': copy.deepcopy(value['resolved_test_classpaths']), 'peer_dependency_observations': {}},
                'U': {'owned_recipe_digest': 'c' * 64}, 'spec': {'toolchain': 'fixed'},
                'R': {'original_labels': [], 'ruleset_records': {}, 'workflows': {}}}
    payloads = {'A': payload(a, fact_a), 'B': payload(b, fact_b), 'C': payload(b, fact_b)}
    partition = {'original_case_partition_verified': True}
    rows = delta.compare_payloads(payloads, partition, {})
    check(any(row['component'] == 'P' and row['field'] == 'packaged_outputs'
              and row['status'] == 'replayed_test_input_partition' and len(set(row['hashes'].values())) == 2 for row in rows),
          'raw differing output fingerprints are retained')
    check(any(row['component'] == 'I' and row['field'] == 'test_packaged_outputs' and row['status'] == 'needs_review' for row in rows),
          'real test input changes still need exact evidence policy')
    values = {name: value['I']['test_packaged_outputs'] for name, value in payloads.items()}
    policy = {('I', 'test_packaged_outputs'): {'hashes': delta.hashes(values), 'reason': 'Reviewed bounded fixture input delta', 'evidence': []}}
    rows = delta.compare_payloads(payloads, partition, policy)
    check(all(row['status'] in ('equal', 'source_partition', 'declared_expected_delta', 'side_observation_delta',
                               'replayed_test_input_partition') for row in rows), 'bounded structural comparator can proceed after exact test-input declaration')
    for label, mutate in [
        ('production output', lambda x: x['B']['P']['packaged_outputs']['fixture/target/fixture-1.jar'].update(sha256='e' * 64)),
        ('plugin runtime bytes', lambda x: x['B']['I']['test_packaged_outputs']['outputs'][artifact]['producer_runtime_inputs']['artifact'].update(file='e' * 64)),
        ('test-JAR output mismatch', lambda x: x['B']['P']['packaged_outputs'][artifact].update(sha256='e' * 64)),
        ('production class bytes', lambda x: x['B']['P']['production']['files'].update({'production.class': 'e' * 64})),
    ]:
        changed = copy.deepcopy(payloads); mutate(changed)
        rejected(lambda: delta.compare_payloads(changed, partition, policy), label)
    changed = copy.deepcopy(payloads)
    for value in changed.values(): value['I'].pop('test_packaged_outputs')
    rejected(lambda: delta.compare_payloads(changed, partition, {}), 'legacy unknown role cannot waive changed JARs')
    changed = copy.deepcopy(payloads); changed['B']['I']['test_packaged_outputs']['original_owner_bytes_replayed'] = False
    rejected(lambda: delta.compare_payloads(changed, partition, {}), 'unread owner proof cannot waive changed JARs')
    # Exercise the real input recorder and Git source binding; only version probes are stubs.
    put('.gitignore', '**/target/\n.capture-repository/\n')
    put('fixture/target/classes/io/tapstate/fixture/Product.class', b'\xca\xfe\xba\xbe\x00\x00\x00\x41\x00\x01\x00\x01')
    repository = root / '.capture-repository'
    dependency = repository / plugin_path
    dependency.parent.mkdir(parents=True)
    dependency.write_bytes(plugin_bytes)
    original_run, original_command = subprocess.run, receipt.command
    def version_probe(arguments, **kwargs):
        if arguments == ['java', '-version']:
            return SimpleNamespace(returncode=0, stderr='fixture Java version 21', stdout='')
        return original_run(arguments, **kwargs)
    def maven_version(arguments, **kwargs):
        return 'fixture Maven version 3.9.9' if arguments == ['mvn', '--version'] else original_command(arguments, **kwargs)
    with patch.dict(os.environ, {'PATH': os.environ['PATH']}, clear=True):
        def git(*arguments):
            original_run(['git', '-c', 'core.hooksPath=/dev/null', '-c', 'commit.gpgsign=false', '-C', str(root), *arguments],
                         check=True, capture_output=True)
        git('init', '-q'); git('config', 'user.name', 'Fixture'); git('config', 'user.email', 'fixture@example.test')
        git('add', '.gitignore', *source_files); git('commit', '-qm', 'Source capture fixture')
        output = root / 'captured-inputs.json'
        with patch.object(receipt.subprocess, 'run', version_probe), patch.object(receipt, 'command', maven_version), contextlib.redirect_stdout(io.StringIO()):
            receipt.inputs(argparse.Namespace(root=str(root), repo_local=str(repository), output=str(output),
                resolve_classpath=False, phase='pre-test-after-install', runtime_class_dir=None, shard_id=None))
        captured = json.loads(output.read_bytes())
        check(set(captured['packaged_test_outputs']['outputs']) == {artifact}
              and captured['packaged_test_outputs']['producer_sources'] == {n: captured['source_files'][n] for n in roles.PRODUCERS},
              'real source-bound input recorder emits packaged test roles without invoking Maven or compiling Java')
print('test-JAR source/member/goal/scope and group controls passed: ' + str(count))
