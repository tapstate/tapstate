#!/usr/bin/env python3
"""Resolved-artifact identity controls over original source, Maven observations and ZIPs."""
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

sys.dont_write_bytecode = True
HERE = Path(__file__).resolve().parent


def module(name, file):
    spec = importlib.util.spec_from_file_location(name, HERE / file)
    value = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(value)
    return value


roles = module('resolved_artifact_controls', '_ci-maven-artifacts.py')
archive_fixture = module('resolved_artifact_archive_fixture', '_ci-archive-inputs-smoke.py')
group_fixture = module('resolved_artifact_group_fixture', '_ci-group-delta-smoke.py')
delta, g = group_fixture.m, group_fixture.g
count = 0
metadata = copy.deepcopy(group_fixture.dependency_metadata)
producer = metadata['dependency_producer_facts']['example/lib/1/lib-1.jar']
producer.update(producer_identity_verified=False, producer_identity_source=None)
sdk_path = 'io/tapstate/published-api/2/published-api-2.jar'
sdk_pom = sdk_path.removesuffix('.jar') + '.pom'
parent_pom = 'io/tapstate/published-parent/2/published-parent-2.pom'
aggregate_pom = 'example/aggregate/1/aggregate-1.pom'
metadata['dependency_repository_files'].update({sdk_path: 'e' * 64, sdk_pom: 'f' * 64,
                                              parent_pom: 'a' * 64, aggregate_pom: 'b' * 64})
metadata['dependency_coordinate_models'].update({
    sdk_pom: dict(gav=['io.tapstate', 'published-api', '2'], packaging='jar', sha256='f' * 64,
        parent=dict(gav=['io.tapstate', 'published-parent', '2'], path=parent_pom, sha256='a' * 64),
        coordinate_type_verified=True, effective_dependency_model_proven=False),
    parent_pom: dict(gav=['io.tapstate', 'published-parent', '2'], packaging='pom', sha256='a' * 64,
        parent=None, coordinate_type_verified=True, effective_dependency_model_proven=False),
    aggregate_pom: dict(gav=['example', 'aggregate', '1'], packaging='pom', sha256='b' * 64,
        parent=None, coordinate_type_verified=True, effective_dependency_model_proven=False)})
metadata['dependency_producer_facts'][sdk_path] = dict(gav=['io.tapstate', 'published-api', '2'], sha256='e' * 64,
    pom_path=sdk_pom, pom_sha256='f' * 64, archive_type_verified=True, producer_identity_verified=False,
    producer_identity_source=None, runtime_realm_proven=False)
metadata['resolved_test_classpaths'] = {'fixture': [
    dict(identity='repository/example/lib/1/lib-1.jar', sha256='c' * 64, size=10, own_classes={}),
    dict(identity='repository/' + sdk_path, sha256='e' * 64, size=12, own_classes={}),
    dict(identity='repository/' + aggregate_pom, sha256='b' * 64, size=20, own_classes={})]}



def check(value, message):
    global count
    assert value, message
    count += 1


def rejected(call, message):
    try:
        call()
    except (ValueError, OSError, KeyError, TypeError):
        check(True, message)
    else:
        raise AssertionError('accepted ' + message)


def conflicting_started_input(value):
    changed = copy.deepcopy(value)
    observation = changed['maven_execution_observation']
    metadata = observation['observation_metadata']
    plugin_file = metadata['event_records'][2]['plugin_artifact']['file']
    for row in metadata['event_records']:
        if row['event'] == 'MojoStarted':
            for artifact in row.get('project_artifacts_at_event', []):
                if artifact.get('group') == 'example':
                    artifact['file'] = plugin_file
    reader = g.module('artifact_control_observation', '_ci-build-observation.py')
    observation.update(reader.observation_summary(json.dumps(metadata).encode()))
    return changed


def recording_control(context):
    """Run the real recorder on protocol files; Maven observations and version probes are fixtures."""
    root, evidence = context['work'], context['evidence']
    exclude = context['main'] / '.git/info/exclude'
    with exclude.open('a') as stream:
        stream.write('\n**/target/\n')
    def put(path, raw):
        path.parent.mkdir(parents=True, exist_ok=True); path.write_bytes(raw)
    for name, raw in context['result_files'].items():
        if name.startswith('fixture/target/test-classes/'):
            put(root / name, raw)
    put(root / 'fixture/target/classes/io/tapstate/fixture/Product.class', b'\xca\xfe\xba\xbe\x00\x00\x00\x41\x00\x01\x00\x01')
    put(root / 'fixture/target/fixture-1-tests.jar', context['packaged_raw'])
    repository = evidence / 'recorder-repository'
    lib = repository / 'example/lib/1/lib-1.jar'
    raw = archive_fixture.zipped({'fixture.txt': b'actual protocol dependency bytes'})
    put(lib, raw)
    put(repository / 'example/lib/1/lib-1.pom', b'<project><groupId>example</groupId><artifactId>lib</artifactId><version>1</version></project>')
    put(repository / 'org/apache/maven/plugins/maven-jar-plugin/3.4.2/maven-jar-plugin-3.4.2.jar', b'bounded plugin protocol fixture')
    sdk_path = 'io/tapstate/published-api/2/published-api-2.jar'
    sdk = repository / sdk_path
    sdk_raw = archive_fixture.zipped({'sdk.txt': b'non-reactor protocol JAR'})
    put(sdk, sdk_raw)
    put(repository / sdk_path.replace('.jar', '.pom'), b'<project><parent><groupId>io.tapstate</groupId><artifactId>published-parent</artifactId><version>2</version></parent><artifactId>published-api</artifactId></project>')
    parent_path = 'io/tapstate/published-parent/2/published-parent-2.pom'
    put(repository / parent_path, b'<project><groupId>io.tapstate</groupId><artifactId>published-parent</artifactId><version>2</version><packaging>pom</packaging></project>')
    aggregate_path = 'example/aggregate/1/aggregate-1.pom'
    aggregate = repository / aggregate_path
    aggregate_raw = b'<project><groupId>example</groupId><artifactId>aggregate</artifactId><version>1</version><packaging>pom</packaging></project>'
    put(aggregate, aggregate_raw)
    put(root / 'fixture/target/ci-dependency-classpath.txt', os.pathsep.join(map(str, (lib, sdk, aggregate))).encode())
    put(root / 'consumer/target/ci-dependency-classpath.txt', str(root / 'fixture/target/fixture-1-tests.jar').encode())
    observation = copy.deepcopy(context['pre']['maven_execution_observation'])
    metadata = observation['observation_metadata']
    sha, path_identity = hashlib.sha256(raw).hexdigest(), '1' * 64
    reference = hashlib.sha256((path_identity + ':' + str(len(raw)) + ':' + sha).encode()).hexdigest()
    metadata['files'][reference] = dict(state='observed-file', size=len(raw), sha256=sha, path_identity_sha256=path_identity)
    for row in metadata['event_records']:
        for artifact in row.get('project_artifacts_at_event', []):
            if (artifact.get('group'), artifact.get('artifact')) == ('example', 'lib'):
                artifact['file'] = reference
    for raw_bytes, label, group, artifact, version, kind, scope in [
            (sdk_raw, '2' * 64, 'io.tapstate', 'published-api', '2', 'jar', 'compile'),
            (aggregate_raw, '3' * 64, 'example', 'aggregate', '1', 'pom', 'runtime')]:
        checksum = hashlib.sha256(raw_bytes).hexdigest()
        version_ref = hashlib.sha256((label + ':' + str(len(raw_bytes)) + ':' + checksum).encode()).hexdigest()
        metadata['files'][version_ref] = dict(state='observed-file', size=len(raw_bytes), sha256=checksum, path_identity_sha256=label)
        for row in metadata['event_records']:
            if row.get('project') == 'io.tapstate:fixture:1' and 'project_artifacts_at_event' in row:
                row['project_artifacts_at_event'] = [a for a in row['project_artifacts_at_event']
                    if (a.get('group'), a.get('artifact'), a.get('version')) != (group, artifact, version)]
                row['project_artifacts_at_event'].append(dict(group=group, artifact=artifact, version=version, type=kind,
                    classifier=None, scope=scope, resolved=True, file=version_ref))
    reader = g.module('recorder_artifact_observation', '_ci-build-observation.py')
    observation.update(reader.observation_summary(json.dumps(metadata).encode()))
    receipt = g.receipt
    observation.update(source_sha=receipt.git(root, 'rev-parse', 'HEAD'), source_tree=receipt.git(root, 'rev-parse', 'HEAD^{tree}'),
                       run_id='local', run_attempt='1', event='local', runner_context=receipt.input_runner_snapshot({}, 'build'))
    summary, output = evidence / 'recorder-observation.json', evidence / 'recorder-inputs.json'
    summary.write_bytes(archive_fixture.encode(observation))
    original_run, original_command = subprocess.run, receipt.command
    def version_probe(arguments, **kwargs):
        if arguments == ['java', '-version']:
            return SimpleNamespace(returncode=0, stderr='fixture Java version 21', stdout='')
        return original_run(arguments, **kwargs)
    def version_command(arguments, **kwargs):
        return 'fixture Maven version 3.9.9' if arguments == ['mvn', '--version'] else original_command(arguments, **kwargs)
    with patch.dict(os.environ, {'PATH': os.environ['PATH']}, clear=True), patch.object(receipt.subprocess, 'run', version_probe), \
            patch.object(receipt, 'command', version_command), contextlib.redirect_stdout(io.StringIO()):
        receipt.inputs(argparse.Namespace(root=str(root), repo_local=str(repository), output=str(output), resolve_classpath=False,
            phase='pre-test-after-install', runtime_class_dir=None, shard_id='build', maven_observation=str(summary)))
    actual = json.loads(output.read_bytes())
    check('resolved_maven_artifacts' in actual and not actual['resolved_maven_artifacts']['unresolved']
          and actual['dependency_producer_facts']['example/lib/1/lib-1.jar']['producer_identity_verified'] is False,
          'real Git/file input recorder emits distinct resolved facts without upgrading embedded producer identity')
    packet = actual['resolved_maven_artifacts']
    sdk_identity, pom_identity = 'repository/' + sdk_path, 'repository/' + aggregate_path
    check(roles.consumer_identity(packet, 'fixture', sdk_identity)['artifact']['type'] == 'jar'
          and not roles.is_reactor(packet['reactor'], ['io.tapstate', 'published-api', '2']),
          'non-reactor first-party namespace is identified from source declarations and original observations')
    check(roles.consumer_identity(packet, 'fixture', pom_identity)['artifact']['type'] == 'pom',
          'actual aggregate POM classpath bytes retain their original artifact type')
    check(actual['dependency_coordinate_models'][sdk_path.replace('.jar', '.pom')]['parent']['sha256']
          == actual['dependency_repository_files'][parent_path], 'non-reactor inherited coordinates bind the literal repository parent bytes')
    dependency_fields = ('dependency_repository_files', 'dependency_coordinate_models', 'dependency_producer_facts', 'resolved_maven_artifacts')
    payload = {'P': {'packaged_outputs': actual['packaged_outputs']}, 'D': {'resolved_test_classpaths': actual['resolved_test_classpaths'],
        'peer_dependency_observations': {'build': {k: actual[k] for k in dependency_fields}}}}
    check(not delta.resolved_dependency_facts(payload)['unknown'], 'default JAR, inherited non-reactor JAR and aggregate POM are independently bound')
    changed = copy.deepcopy(payload)
    changed['D']['peer_dependency_observations']['build']['dependency_repository_files'].pop(parent_path)
    check(any(sdk_identity in r for r in delta.resolved_dependency_facts(changed)['unknown']), 'missing inherited repository parent cannot be waived')
    changed = copy.deepcopy(payload)
    changed['D']['peer_dependency_observations']['build']['dependency_coordinate_models'][aggregate_path]['packaging'] = 'jar'
    check(any(pom_identity in r for r in delta.resolved_dependency_facts(changed)['unknown']), 'POM-only admission does not accept an unverified aggregate model')
    changed = copy.deepcopy(payload)
    changed['D']['peer_dependency_observations']['build']['resolved_maven_artifacts']['reactor']['declared']['io.tapstate:published-api:2'] = ['source-module']
    check(any(sdk_identity in r for r in delta.resolved_dependency_facts(changed)['unknown']), 'native coordinates cannot turn a source reactor artifact into an external dependency')
    legacy = copy.deepcopy(payload)
    legacy['D']['peer_dependency_observations']['build'].pop('resolved_maven_artifacts')
    unknown = delta.resolved_dependency_facts(legacy)['unknown']
    check(any(sdk_identity in r for r in unknown) and any(pom_identity in r for r in unknown),
          'legacy first-party and POM unknowns retain their original strict boundary')



def controls(context):
    value, root = context['pre'], context['work']
    read_source = lambda name: (root / name).read_bytes()
    packet = value['resolved_maven_artifacts']
    identity = 'repository/example/lib/1/lib-1.jar'
    check(roles.validate(packet, value, read_source) == packet and not packet['unresolved'],
          'original consumer revision and resolved file references replay')
    observed = roles.consumer_identity(packet, 'fixture', identity)
    check(observed['consumer_gav'] == ['io.tapstate', 'fixture', '1']
          and observed['artifact']['scopes'] == ['test'], 'original consumer GAV and recorded scope remain explicit')
    dependency_fields = ('dependency_repository_files', 'dependency_coordinate_models', 'dependency_producer_facts', 'resolved_maven_artifacts')
    payload = {'P': {'packaged_outputs': copy.deepcopy(value['packaged_outputs'])},
        'D': {'resolved_test_classpaths': copy.deepcopy(value['resolved_test_classpaths']),
              'peer_dependency_observations': {'build': {k: copy.deepcopy(value[k]) for k in dependency_fields}}}}
    facts = delta.resolved_dependency_facts(payload)
    check(not facts['unknown'], 'original resolved Maven coordinates complement missing embedded producer metadata')
    check(value['dependency_producer_facts']['example/lib/1/lib-1.jar']['producer_identity_verified'] is False
          and not packet['archive_producer_identity_proven'] and not packet['actual_class_consumption_proven'],
          'resolved coordinates never become archive-producer or consumption proof')
    check('original_maven_resolved_identities' in facts['resolved'][identity][0], 'the distinct identity source is retained')
    legacy = copy.deepcopy(payload)
    legacy['D']['peer_dependency_observations']['build'].pop('resolved_maven_artifacts')
    check(bool(delta.resolved_dependency_facts(legacy)['unknown']), 'legacy absence retains strict producer unknown')
    for label, mutate in [
        ('missing consumer', lambda x: x['consumers'].pop('fixture')),
        ('missing identity', lambda x: x['consumers']['fixture']['artifacts'].pop(identity)),
        ('changed recorded bytes', lambda x: x['consumers']['fixture']['artifacts'][identity].update(sha256='e' * 64)),
        ('changed helper', lambda x: x['capture_helpers'].update({roles.PRODUCERS[0]: 'e' * 64})),
        ('false producer claim', lambda x: x.update(archive_producer_identity_proven=True)),
        ('false consumption claim', lambda x: x.update(actual_class_consumption_proven=True)),
        ('boolean schema', lambda x: x.update(schema=True)),
    ]:
        changed = copy.deepcopy(packet); mutate(changed)
        rejected(lambda: roles.validate(changed, value, read_source), label)
    for label, mutate in [
        ('wrong origin', lambda x: x['maven_execution_observation'].update(source_sha='e' * 40)),
        ('missing original source helper', lambda x: x['source_files'].pop(roles.PRODUCERS[1])),
        ('changed available dependency bytes', lambda x: x['dependency_repository_files'].update({'example/lib/1/lib-1.jar': 'e' * 64})),
    ]:
        changed = copy.deepcopy(value); mutate(changed)
        rejected(lambda: roles.capture(changed, read_source), label)
    for label, mutate in [
        ('wrong consumer GAV', lambda md: [r.update(project='io.tapstate:foreign:1') for r in md['event_records'] if r['project'] == 'io.tapstate:fixture:1']),
        ('missing resolved observation', lambda md: [a.update(resolved=False) for r in md['event_records'] for a in r.get('project_artifacts_at_event', []) if a.get('group') == 'example']),
        ('unsupported artifact type', lambda md: [a.update(type='bundle') for r in md['event_records'] for a in r.get('project_artifacts_at_event', []) if a.get('group') == 'example']),
        ('unsupported classifier', lambda md: [a.update(classifier='sources') for r in md['event_records'] for a in r.get('project_artifacts_at_event', []) if a.get('group') == 'example']),
        ('unknown scope', lambda md: [a.update(scope='unknown') for r in md['event_records'] for a in r.get('project_artifacts_at_event', []) if a.get('group') == 'example']),
        ('missing observed file', lambda md: [a.update(file='e' * 64) for r in md['event_records'] for a in r.get('project_artifacts_at_event', []) if a.get('group') == 'example']),
    ]:
        changed = copy.deepcopy(value); mutate(changed['maven_execution_observation']['observation_metadata'])
        actual = roles.capture(changed, read_source)
        check(bool(actual['unresolved']) and roles.consumer_identity(actual, 'fixture', identity) is None, label + ' remains unknown')
    changed = conflicting_started_input(value)
    check(bool(roles.capture(changed, read_source)['unresolved']), 'conflicting started input bytes remain unknown')
    changed = copy.deepcopy(value)
    md = changed['maven_execution_observation']['observation_metadata']
    success = next(r for r in md['event_records'] if r['event'] == 'MojoSucceeded' and r['project'] == 'io.tapstate:fixture:1')
    extra = copy.deepcopy(success['project_artifacts_at_event'][0])
    path_identity, sha, size = 'd' * 64, 'e' * 64, 11
    reference = hashlib.sha256((path_identity + ':' + str(size) + ':' + sha).encode()).hexdigest()
    md['files'][reference] = dict(state='observed-file', path_identity_sha256=path_identity, sha256=sha, size=size)
    extra['file'] = reference; success['project_artifacts_at_event'].append(extra)
    check(bool(roles.capture(changed, read_source)['unresolved']), 'a matching row cannot hide conflicting original resolved bytes')
    for label, mutate in [
        ('invalid JAR archive type', lambda x: x['dependency_producer_facts']['example/lib/1/lib-1.jar'].update(archive_type_verified=False)),
        ('unverified POM coordinate model', lambda x: x['dependency_coordinate_models']['example/lib/1/lib-1.pom'].update(coordinate_type_verified=False)),
        ('changed POM bytes', lambda x: x['dependency_repository_files'].update({'example/lib/1/lib-1.pom': 'e' * 64})),
        ('missing original producer record', lambda x: x['dependency_producer_facts'].clear()),
        ('changed original native bytes', lambda x: x['resolved_maven_artifacts']['consumers']['fixture']['artifacts'][identity].update(sha256='e' * 64)),
    ]:
        changed = copy.deepcopy(payload); mutate(changed['D']['peer_dependency_observations']['build'])
        check(bool(delta.resolved_dependency_facts(changed)['unknown']), label + ' cannot be waived by a Maven row')
    original = context['project']()
    check('observed_resolved_maven_artifacts' in original and not original['whole_build_provenance_complete'],
          'full original Git/ZIP reader replays new capture without whole-build proof')
    artifacts = context['artifacts']
    artifacts['ci-inputs-rest']['post-test-inputs.json'].pop('resolved_maven_artifacts')
    context['reseal'](); rejected(context['project'], 'missing original post capture'); context['reset']()
    artifacts['ci-inputs-build']['build-inputs.json'].pop('resolved_maven_artifacts')
    context['reseal'](); rejected(context['project'], 'mixed legacy/current peers'); context['reset']()
    artifacts['ci-inputs-rest']['build-inputs.json']['resolved_maven_artifacts']['consumers']['fixture']['artifacts'][identity]['sha256'] = 'e' * 64
    context['reseal'](); rejected(context['project'], 'resealed false identity capture'); context['reset']()
    expected = context['project']()
    check(expected['observed_resolved_maven_artifacts'] == original['observed_resolved_maven_artifacts'],
          'reset restores original identity facts while resealing refreshes ZIP reference hashes')
    output, manifest = context['evidence'] / 'maven-artifact-cli.json', context['evidence'] / 'maven-artifact-request.json'
    manifest.write_bytes(archive_fixture.encode(context['request']))
    child = subprocess.run(['bash', str(HERE / 'ci-archive-inputs.sh'), '--manifest', str(manifest),
        '--evidence-root', str(context['evidence']), '--root', str(root), '--output', str(output)], capture_output=True, text=True)
    check(child.returncode == 0 and json.loads(output.read_bytes()) == expected, 'original archive CLI status ' + str(child.returncode) + ': ' + child.stderr)
    recording_control(context)


def group_controls(root, contexts, unused_complete, unused_added):
    request, inventories, unused = group_fixture.fixture.prepare_request(root, contexts, group_fixture.original_event)
    def save(name, value):
        raw = archive_fixture.encode(value); (root / name).write_bytes(raw)
        return {'path': name, 'sha256': g.checksum(raw)}
    partition = save('native-partition-request.json', request)
    facts = {name: delta.components.project(g.References(root), entry['source_root'],
        {'schema': 1, 'type': 'archived-group-components-request', 'archive_cases': entry['archive_cases']})
        for name, entry in request['variants'].items()}
    payloads = {name: value['payloads'] for name, value in facts.items()}
    policy = []
    for component, field in sorted(delta.POLICY_FIELDS):
        values = {name: (value['I']['source_inventory'].get(field, value['I'].get(field)) if component == 'I'
                         else value[component][field]) for name, value in payloads.items()}
        hashes = delta.hashes(values)
        if len(set(hashes.values())) > 1:
            policy.append({'component': component, 'field': field, 'hashes': hashes,
                           'reason': 'Bounded original source fixture addition', 'evidence': [partition]})
    manifest = {'schema': 1, 'type': 'archived-group-delta-request', 'partition_request': partition,
                'review_policy': save('native-policy.json', {'schema': 1, 'type': 'declared-group-delta-policy', 'entries': policy})}
    report = delta.project(g.References(root), manifest)
    check(report['comparison_ready'] and not report['group_equivalence_proven'] and not report['performance_accepted'],
          'independently replayed native identity inputs reach bounded structural comparison only')
    check(all(value['D']['peer_dependency_observations']['build']['dependency_producer_facts']['example/lib/1/lib-1.jar']['producer_identity_verified'] is False
              for value in payloads.values()), 'all original producer unknowns remain false in full D fingerprints')
    parsed = delta.policy_entries(g.References(root), manifest['review_policy'])
    changed = copy.deepcopy(payloads)
    changed['B']['D']['peer_dependency_observations']['build'].pop('resolved_maven_artifacts')
    check(any(r['status'] == 'unknown' for r in delta.compare_payloads(changed, report['case_partition'], parsed)),
          'missing original peer proof blocks comparison readiness')
    def original_packet(mutate):
        context = contexts['B']
        original = copy.deepcopy(context['pre'])
        observation = original['maven_execution_observation']
        mutate(observation['observation_metadata'])
        reader = g.module('scope_control_observation', '_ci-build-observation.py')
        observation.update(reader.observation_summary(json.dumps(observation['observation_metadata']).encode()))
        read_source = lambda name: (context['work'] / name).read_bytes()
        packet = roles.capture(original, read_source)
        return roles.validate(packet, original, read_source)

    scope_packet = original_packet(lambda md: [a.update(scope='compile') for row in md['event_records']
        for a in row.get('project_artifacts_at_event', [])
        if (a.get('group'), a.get('artifact')) == ('example', 'lib')])
    def conflicting_type(md):
        row = next(r for r in md['event_records'] if r['event'] == 'MojoStarted')
        artifact = copy.deepcopy(next(a for a in row['project_artifacts_at_event'] if a['artifact'] == 'lib'))
        artifact['type'] = 'pom'
        row['project_artifacts_at_event'].append(artifact)
    conflict_packet = original_packet(conflicting_type)
    check(not scope_packet['unresolved'] and bool(conflict_packet['unresolved']),
          'schema-valid original scope and conflicting type controls independently replay')
    for embedded in (False, True):
        current = copy.deepcopy(payloads)
        for value in current.values():
            for peer in value['D']['peer_dependency_observations'].values():
                producer = peer['dependency_producer_facts']['example/lib/1/lib-1.jar']
                producer.update(producer_identity_verified=embedded,
                    producer_identity_source='synthetic-protocol-fixture' if embedded else None)
        check(not any(r['status'] in ('unknown', 'needs_review')
                      for r in delta.compare_payloads(current, report['case_partition'], parsed)),
              'unchanged resolved facts admit embedded identity ' + str(embedded))
        for packet, status, label in ((scope_packet, 'needs_review', 'used scope'),
                                      (conflict_packet, 'unknown', 'conflicting original type')):
            changed = copy.deepcopy(current)
            for peer in changed['B']['D']['peer_dependency_observations'].values():
                peer['resolved_maven_artifacts'] = packet
            check(any(r['field'] == 'resolved_dependency_identities' and r['status'] == status
                      for r in delta.compare_payloads(changed, report['case_partition'], parsed)),
                  label + ' remains an input difference with embedded identity ' + str(embedded))
    request_path, output = root / 'native-delta-request.json', root / 'native-delta-output.json'
    request_path.write_bytes(archive_fixture.encode(manifest))
    child = subprocess.run(['bash', str(HERE / 'ci-group-delta.sh'), '--manifest', str(request_path),
        '--evidence-root', str(root), '--output', str(output)], capture_output=True, text=True)
    check(child.returncode == 0 and json.loads(output.read_bytes()) == report, 'full native identity delta CLI: ' + child.stderr)


if __name__ == '__main__':
    archive_fixture.run(controls, only_callback=True, test_jar=True, resolved_artifacts=True, dependency_metadata=metadata)
    group_fixture.fixture.cohort(added=True, callback=group_controls, dependency_metadata=metadata,
                                test_jar=True, resolved_artifacts=True)
    print('original resolved Maven artifact identity controls passed:', count)
