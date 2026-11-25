#!/usr/bin/env python3
"""Typed component omission and binding controls over original Git/ZIP evidence."""
import copy
import importlib.util
import json
from pathlib import Path
import subprocess
import sys

sys.dont_write_bytecode = True


def module(name, filename):
    spec = importlib.util.spec_from_file_location(name, Path(__file__).with_name(filename))
    value = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(value)
    return value


m = module('components', '_ci-group-components.py')
case_fixture = module('case_fixture', '_ci-archive-cases-smoke.py')
fixture = case_fixture.fixture
count = 0


def check(value, message):
    global count
    assert value, message
    count += 1


def rejected(call, message):
    try:
        call()
    except (ValueError, KeyError, TypeError, OSError):
        check(True, message)
    else:
        raise AssertionError('accepted ' + message)


def controls(context):
    case_request, unused = case_fixture.prepare_cases(context)
    request = {'schema': 1, 'type': 'archived-group-components-request',
               'archive_cases': context['save']('component-cases-request.json', case_request)}
    reference = context['save']('component-request.json', request)
    replay = m.project(m.g.References(context['evidence']), context['work'], request)
    check(set(replay['payloads']) == m.g.COMPONENTS and replay['source_case_input_replayed'], 'all component facts derive from original archives')
    check(not replay['normalized_original_inputs_verified'] and not replay['group_equivalence_proven']
          and not replay['whole_build_provenance_complete'] and not replay['first_complete_PR_sample']
          and not replay['performance_accepted'], 'full fingerprints do not invent normalization or acceptance')
    typed = {}
    for name in sorted(m.g.COMPONENTS):
        value = m.component(m.g.References(context['evidence']), context['work'], reference, name)
        path = context['save']('typed-' + name + '.json', value)
        typed[name] = path
        binding = replay['binding']
        actual = m.g.component(m.g.References(context['evidence']), name, path, binding)
        check(actual == replay['component_hashes'][name], 'typed ' + name + ' consumer independently replays its payload')
        changed = copy.deepcopy(value); changed['payload'].clear()
        rejected(lambda: m.g.component(m.g.References(context['evidence']), name,
            context['save']('typed-missing-' + name + '.json', changed), binding), 'omitted ' + name + ' facts')
    check(m.g.component_mode(m.g.References(context['evidence']), typed) == 'archive-replayed', 'complete typed component set is identified')
    legacy = m.g.load(context['evidence'] / typed['I']['path'])
    legacy['type'] = 'declared-group-component'
    mixed = dict(typed, I=context['save']('legacy-copy.json', legacy))
    rejected(lambda: m.g.component_mode(m.g.References(context['evidence']), mixed), 'mixed native/declaration components')
    rejected(lambda: m.g.impact(m.g.References(context['evidence']),
        {'group_state': 'pending', 'before': typed, 'after': mixed}), 'mixed main-impact capture')
    for label, mutate in [
        ('source case omitted', lambda x: x['payload']['source_inventory']['cases'].clear()),
        ('source method omitted', lambda x: x['payload']['source_inventory']['methods'].clear()),
        ('unresolved diagnostics omitted', lambda x: x['payload']['source_inventory'].pop('unresolved')),
        ('false original partition', lambda x: x['payload'].update(original_case_partition_verified=True)),
        ('boolean replaced by integer', lambda x: x['payload'].update(original_case_partition_verified=0)),
    ]:
        value = m.component(m.g.References(context['evidence']), context['work'], reference, 'I')
        mutate(value)
        rejected(lambda: m.g.component(m.g.References(context['evidence']), 'I',
            context['save']('typed-case-mutation.json', value), replay['binding']), label)
    for name, field in [('P', 'production'), ('P', 'packaged_outputs'), ('D', 'resolved_test_classpaths'),
                        ('D', 'peer_dependency_observations'), ('R', 'workflows'), ('spec', 'toolchain')]:
        value = m.component(m.g.References(context['evidence']), context['work'], reference, name)
        value['payload'].pop(field)
        rejected(lambda: m.g.component(m.g.References(context['evidence']), name,
            context['save']('typed-field-mutation.json', value), replay['binding']), 'missing ' + field)
    for field in ['dependency_repository_files', 'dependency_coordinate_models', 'dependency_producer_facts']:
        value = m.component(m.g.References(context['evidence']), context['work'], reference, 'D')
        value['payload']['peer_dependency_observations']['build'].pop(field)
        rejected(lambda: m.g.component(m.g.References(context['evidence']), 'D',
            context['save']('typed-dependency-field-mutation.json', value), replay['binding']), 'missing peer ' + field)
    value = m.component(m.g.References(context['evidence']), context['work'], reference, 'U')
    value['binding']['run_id'] += 1
    rejected(lambda: m.g.component(m.g.References(context['evidence']), 'U',
        context['save']('typed-foreign-run.json', value), replay['binding']), 'foreign component binding')
    check(replay['payloads']['I']['source_inventory']['cases'], 'caller cannot replace the actual nonempty source case inventory')
    request_path = context['evidence'] / reference['path']
    output = context['evidence'] / 'component-cli.json'
    command = ['bash', str(Path(__file__).with_name('ci-group-components.sh')), '--manifest', str(request_path),
        '--evidence-root', str(context['evidence']), '--root', str(context['work']), '--component', 'P', '--output', str(output)]
    result = subprocess.run(command, capture_output=True, text=True)
    check(result.returncode == 0, 'actual component CLI succeeds: ' + result.stderr)
    value = json.loads(output.read_bytes())
    check(value['payload'] == replay['payloads']['P'], 'CLI payload matches independent replay')
    check(m.g.component(m.g.References(context['evidence']), 'P',
        {'path': output.relative_to(context['evidence']).as_posix(), 'sha256': m.g.checksum(output.read_bytes())}, replay['binding'])
        == replay['component_hashes']['P'], 'CLI emitted request reference is independently consumable')
    original = output.read_bytes()
    check(subprocess.run(command, capture_output=True).returncode != 0 and output.read_bytes() == original,
          'CLI output is exclusive and existing evidence survives')


def resource_controls(context):
    case_request,unused=case_fixture.prepare_cases(context)
    request={'schema':1,'type':'archived-group-components-request',
             'archive_cases':context['save']('resource-cases-request.json',case_request)}
    reference=context['save']('resource-components-request.json',request)
    replay=m.project(m.g.References(context['evidence']),context['work'],request)
    resources=context['pre']['test_resource_files']
    check(resources and replay['payloads']['I']['test_resource_files']==resources,
          'test-resource component hashes derive from original prepared snapshots and raw packets')
    value=m.component(m.g.References(context['evidence']),context['work'],reference,'I')
    for label,change in [('omission',lambda payload:payload.pop('test_resource_files')),
                         ('change',lambda payload:payload['test_resource_files'].update({'fixture/target/test-classes/corpus/golden.txt':'f'*64}))]:
        changed=copy.deepcopy(value);change(changed['payload'])
        rejected(lambda:m.g.component(m.g.References(context['evidence']),'I',
            context['save']('resource-'+label+'.json',changed),replay['binding']), 'test resource '+label)
    check(not replay['normalized_original_inputs_verified'] and not replay['whole_build_provenance_complete'],
          'resource fingerprinting cannot certify original normalization or whole provenance')


fixture.run(controls, only_callback=True, dependency_metadata=True)
fixture.run(resource_controls,only_callback=True,dependency_metadata=True,
            test_resource_bytes={'fixture/target/test-classes/corpus/golden.txt':b'compiled resource fixture\n'})
print('CI archived group component controls passed:', count)
