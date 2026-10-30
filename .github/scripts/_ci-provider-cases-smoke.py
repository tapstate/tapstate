#!/usr/bin/env python3
"""Computed case identities join original XML and three archived variant inputs.

The shared Java boundary is a protocol recorder, not original provider/assertion proof.
"""
import copy
from contextlib import redirect_stdout
import importlib.util
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
from unittest.mock import patch
from types import SimpleNamespace
import xml.etree.ElementTree as ET
sys.dont_write_bytecode = True
HERE = Path(__file__).resolve().parent


def module(name, filename):
    spec = importlib.util.spec_from_file_location(name, HERE / filename)
    value = importlib.util.module_from_spec(spec); spec.loader.exec_module(value)
    return value


producer = module('provider_case_producer', '_ci-provider-replay-smoke.py')
cases_fixture = module('provider_case_reports', '_ci-archive-cases-smoke.py')
partition_fixture = module('provider_case_partition_fixture', '_ci-case-partition-smoke.py')
m = cases_fixture.m
g, ev, fixture = m.g, m.ev, producer.fixture
CHECKS = 0


def check(value, message):
    global CHECKS
    assert value, message
    CHECKS += 1


def rejected(call, reason):
    try: call()
    except (ValueError, KeyError, TypeError, OSError) as error:
        check(reason in str(error), 'specific provider/case rejection: ' + str(error))
    else: raise AssertionError('accepted ' + reason)


def raw_report_controls(original, source_context, files, manifest):
    baseline = original['root'] / 'producer-data'
    for mutation in ('wrong-marker', 'missing-original', 'duplicate-computed'):
        with tempfile.TemporaryDirectory(prefix='provider-case-raw-xml-') as temporary:
            root = Path(temporary).resolve(); tree = root / 'files'; tree.mkdir()
            for name, raw in files.items():m.data_file(tree, name, raw)
            xml = tree / original['xml_path']; suite = ET.fromstring(xml.read_bytes())
            if mutation == 'wrong-marker':
                output = suite[1].find('system-out'); output.text = output.text.replace('1' * 64, '3' * 64)
            elif mutation == 'missing-original':suite.remove(suite[0])
            else:suite.append(copy.deepcopy(suite[1]))
            suite.set('tests', str(len(suite))); raw = ET.tostring(suite); xml.write_bytes(raw)
            packet = copy.deepcopy(manifest); packet['files'][original['xml_path']] = g.checksum(raw)
            packet['statistics']['tests'] = len(suite)
            manifest_path = root / 'manifest.json'; manifest_path.write_bytes(fixture.fmod.encode(packet))
            (root / 'gates').mkdir(); (root / 'additions.json').write_text('[]')
            args = SimpleNamespace(root=str(tree), inventory=str(baseline/'inventory.json'), lane='ci',
                inputs=str(baseline/'inputs.json'), binding=str(baseline/'binding.json'), class_gates=str(root/'gates'),
                artifact_manifest=str(manifest_path), shard_plan=None, shard=None, output=str(root/'after.json'))
            try:
                with redirect_stdout(io.StringIO()):
                    ev.reports(args)
                    ev.compare_tests(SimpleNamespace(inventory=str(baseline/'inventory.json'), candidate_inventory=str(baseline/'inventory.json'),
                        before=str(baseline/'observations.json'), after=str(root/'after.json'), additions=str(root/'additions.json'), output=str(root/'comparison.json')))
            except ValueError as error:
                check(any(reason in str(error) for reason in ('case multiset', 'incomplete', 'unresolved', 'duplicate')),
                      'original XML/marker rejection has a case-level reason: ' + str(error))
            else:raise AssertionError('accepted raw provider XML mutation: ' + mutation)


def prepare(context, shared, name):
    fixture.fmod.v.git(context['f']['work'], 'commit', '--allow-empty', '-m', 'Active replay fixture variant tree')
    # This protocol fixture observes dependency bytes but has no producer-model proof.
    for artifact, members in context['f']['artifacts'].items():
        if artifact.startswith('ci-inputs-'):
            for filename in ('build-inputs.json', 'post-test-inputs.json'):
                if filename in members:
                    for field in ('dependency_coordinate_models', 'dependency_producer_facts'):
                        members[filename].setdefault(field, {})
    f = producer.prepare(context)
    original = context['f']
    local = original['evidence']
    artifacts = original['artifacts']
    manifest = artifacts['ci-test-results-rest']['rest/manifest.json']
    suite = ET.Element('testsuite', name=fixture.CLASS, tests='3', failures='0', errors='0', skipped='0')
    ET.SubElement(suite, 'testcase', classname=fixture.CLASS, name='holds')
    for index, parameter in enumerate(('1' * 64, '2' * 64), 1):
        node = ET.SubElement(suite, 'testcase', classname=fixture.CLASS, name='computed(int)[' + str(index) + ']')
        marker = {'schema': 1, 'phase': 'start', 'unique_id': 'computed-' + str(index), 'stable': True,
                  'class': fixture.CLASS, 'method': 'computed', 'argument_digest': parameter}
        ET.SubElement(node, 'system-out').text = 'tapstate.ci-case=' + json.dumps(marker) + '\n' + 'tapstate.ci-case=' + json.dumps(dict(marker, phase='finish', status='executed')) + '\n'
    xml = ET.tostring(suite)
    key = 'rest/files/' + original['xml_path']
    artifacts['ci-test-results-rest'][key] = xml
    manifest['files'][original['xml_path']] = g.checksum(xml)
    manifest['statistics']['tests'] = 3
    files = {path.removeprefix('rest/files/'): raw for path, raw in artifacts['ci-test-results-rest'].items() if path.startswith('rest/files/')}
    def expected_inventory(inv):
        method = next(row for row in inv['methods'] if row['method'] == 'computed')
        method.update(parameters=['1' * 64, '2' * 64], argument_digests=['1' * 64, '2' * 64], mapping='argument-marker',
                      provider_contract=copy.deepcopy(f['capture']['resolved_methods'][0]['provider_contract']))
        inv['unresolved'] = []
        inv['complete'] = True
        inv['cases'] += [{**{field: method[field] for field in ('lane','module','kind','class','method')}, 'parameter': parameter}
                         for parameter in method['parameters']]
        inv['cases'].sort(key=ev.identity)
        inv['provider_probe'] = {'original_inventory_sha256': f['capture']['original_inventory_sha256']}
        return inv
    source_context = dict(original, pre=artifacts['ci-inputs-rest']['build-inputs.json'], manifest=manifest,
                          result_files=files, reseal=context['reseal'])
    case_request, unused = cases_fixture.prepare_cases(source_context, expected_inventory)
    # Request graphs can be rebased before capture. Original run/ZIP bytes are never rewritten.
    reference = original['save']('provider-case-inputs.json', original['request'])
    input_ref = partition_fixture.rebase_reference(reference, local, shared, {})
    original['evidence'] = shared
    f['request']['archive_inputs'] = input_ref
    f['request_path'].write_bytes(fixture.fmod.encode(f['request']))
    result, path = producer.execute(f, name + '-provider-replay')
    check(result.returncode == 0, 'active protocol fixture captures matching provider cases: ' + result.stderr)
    replay_request = {'schema': 1, 'type': 'provider-replay-readback-request',
                      'replay': {'path': path.relative_to(shared).as_posix(), 'sha256': g.checksum(path.read_bytes())}}
    replay_path = shared / (name + '-provider-readback-request.json')
    replay_path.write_bytes(fixture.fmod.encode(replay_request))
    case_request['archive_inputs'] = input_ref
    case_request['provider_replays'] = {'rest': {'path': replay_path.relative_to(shared).as_posix(), 'sha256': g.checksum(replay_path.read_bytes())}}
    case_path = shared / (name + '-cases.json'); case_path.write_bytes(fixture.fmod.encode(case_request))
    answer = m.project(g.References(shared), f['root'], case_request)
    check(answer['all_source_cases_resolved'] and not answer['all_source_cases_statically_parsed'],
          'computed hash identities resolve without pretending they were statically parsed')
    check(len(answer['source_inventory']['cases']) == 3 and not answer['source_inventory']['unresolved'],
          'one static and two computed original identities independently join raw XML')
    check(not answer['whole_input_projection_verified'] and not answer['first_complete_PR_sample'] and not answer['performance_accepted'],
          'resolved identities do not certify assertions, whole provenance or performance')
    legacy = dict(case_request); legacy.pop('provider_replays')
    legacy_answer = m.project(g.References(shared), f['root'], legacy)
    check(not legacy_answer['all_source_cases_statically_parsed'] and len(legacy_answer['source_inventory']['cases']) == 1,
          'without replay the original computed source gap remains unknown')
    if name == 'C':
        raw_report_controls(original, source_context, files, manifest)
        inventory = copy.deepcopy(legacy_answer['source_inventory'])
        computed = next(row for row in inventory['methods'] if row['method'] == 'computed')
        other = dict(computed, **{'class': computed['class'] + '$Other'})
        inventory['methods'].append(other)
        unresolved, unused = m.provider_inventory(g.References(shared), f['root'], case_request, inventory, answer['scope'], ['rest'])
        check(not unresolved['complete'] and unresolved['unresolved'] and other['parameters'] is None,
              'same source/method name in an unresolved nested class cannot disappear with a resolved neighbor')
        inventory = copy.deepcopy(legacy_answer['source_inventory'])
        next(row for row in inventory['methods'] if row['method'] == 'computed')['parameters'] = ['caller-static']
        rejected(lambda: m.provider_inventory(g.References(shared), f['root'], case_request, inventory, answer['scope'], ['rest']),
                 'replaces a static or foreign method')
    changed = copy.deepcopy(case_request); changed['provider_replays'].clear()
    rejected(lambda: m.project(g.References(shared), f['root'], changed), 'worker set differs')
    changed = copy.deepcopy(case_request); changed['provider_replays']['engine'] = changed['provider_replays'].pop('rest')
    rejected(lambda: m.project(g.References(shared), f['root'], changed), 'worker set differs')
    changed = copy.deepcopy(case_request); changed['provider_replays']['rest']['sha256'] = 'f' * 64
    rejected(lambda: m.project(g.References(shared), f['root'], changed), 'referenced input hash differs')
    if name == 'C':
        counter = f['java'].with_suffix('.calls'); before_calls = counter.read_bytes()
        output = shared / 'computed-case-cli.json'
        command = ['bash', str(HERE/'ci-archive-cases.sh'), '--manifest', str(case_path), '--evidence-root', str(shared),
                   '--root', str(f['root']), '--output', str(output)]
        result = subprocess.run(command, capture_output=True, text=True)
        check(result.returncode == 0 and g.load(output)['source_inventory_digest'] == answer['source_inventory_digest'],
              'actual case CLI independently joins the original provider/report records: ' + result.stderr)
        check(counter.read_bytes() == before_calls, 'case readback invokes no Java/provider process')
    return {'archive_cases': {'path': case_path.relative_to(shared).as_posix(), 'sha256': g.checksum(case_path.read_bytes())},
            'source_root': str(f['root'])}, answer


def main():
    workflow = (HERE.parent / 'workflows/ci.yml').read_text()
    contract = {'schema': 1, 'classes': {fixture.CLASS: {'module': 'fixture', 'source': fixture.SOURCE,
        'source_sha256': g.checksum(fixture.CODE.encode()), 'providers': {'values': {'kind': 'source', 'projection': 'strict'}}}}}
    with tempfile.TemporaryDirectory(prefix='provider-case-partition-') as temporary:
        shared = Path(temporary).resolve(); entries = {}; answers = {}
        def enter(index):
            if index == 3:
                request = {'schema': 1, 'type': 'archived-original-case-partition-request', 'variants': entries}
                answer = partition_fixture.m.project(g.References(shared), request)
                check(answer['original_case_partition_verified'] and not answer['known_static_subset_partitioned'],
                      'three original archives derive a complete identity partition with computed cases')
                check(len(answer['original_cases']) == 3 and answer['additions'] == {'B': [], 'C': []},
                      'computed case multiset is retained exactly once in A/B/C')
                check(not answer['original_assertions_verified'] and not answer['whole_build_provenance_complete']
                      and not answer['performance_accepted'], 'identity partition keeps stronger proof false')
                manifest = shared / 'partition-request.json'; manifest.write_bytes(fixture.fmod.encode(request))
                output = shared / 'partition-output.json'
                command = ['bash', str(HERE/'ci-case-partition.sh'), '--manifest', str(manifest), '--evidence-root', str(shared), '--output', str(output)]
                result = subprocess.run(command, capture_output=True, text=True)
                check(result.returncode == 0 and g.load(output)['original_cases'] == answer['original_cases'],
                      'actual partition CLI reproduces computed identity partition: ' + result.stderr)
                previous = output.read_bytes(); result = subprocess.run(command, capture_output=True, text=True)
                check(result.returncode != 0 and output.read_bytes() == previous, 'partition CLI preserves existing evidence')
                components = module('provider_components_integration', '_ci-group-components.py')
                for name, entry in entries.items():
                    fact = components.project(g.References(shared), entry['source_root'],
                        {'schema': 1, 'type': 'archived-group-components-request', 'archive_cases': entry['archive_cases']})
                    check(fact['payloads']['I']['all_source_cases_resolved'] and not fact['payloads']['I']['all_source_cases_statically_parsed']
                          and len(fact['payloads']['I']['source_inventory']['cases']) == 3,
                          'typed group I consumes each original computed case inventory')
                policy = shared / 'delta-policy.json'; policy.write_bytes(fixture.fmod.encode(
                    {'schema': 1, 'type': 'declared-group-delta-policy', 'entries': []}))
                delta = module('provider_delta_integration', '_ci-group-delta.py').project(g.References(shared),
                    {'schema': 1, 'type': 'archived-group-delta-request',
                     'partition_request': {'path': manifest.relative_to(shared).as_posix(), 'sha256': g.checksum(manifest.read_bytes())},
                     'review_policy': {'path': policy.relative_to(shared).as_posix(), 'sha256': g.checksum(policy.read_bytes())}})
                check(any(row['component'] == 'I' and row['field'] == 'cases' and row['status'] == 'source_partition' for row in delta['fields'])
                      and not delta['group_equivalence_proven'] and not delta['original_assertions_verified'],
                      'group delta uses resolved identities while other proof remains independent')
                return
            name = 'ABC'[index]
            def accept_context(context):
                entries[name], answers[name] = prepare(context, shared, name)
                enter(index + 1)
            def callback(f): fixture.controls(f, callback=accept_context, only_callback=True)
            fixture.fmod.run(callback, only_callback=True, source_case=fixture.CODE, workflow_sources={'ci.yml': workflow},
                extra_source_files={fixture.CONTRACT: json.dumps(contract), fixture.PRODUCER: (HERE/'_ci-evidence.py').read_bytes(),
                                    fixture.RESOURCE: fixture.RESOURCE_BYTES}, variant=name, run_id=40 + index, temporary_parent=shared)
        with patch.dict(os.environ, GIT_AUTHOR_DATE='2026-10-14T00:00:00+00:00', GIT_COMMITTER_DATE='2026-10-14T00:00:00+00:00'):
            enter(0)
    print('CI computed provider case partition controls passed:', CHECKS)


if __name__ == '__main__': main()
