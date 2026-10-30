#!/usr/bin/env python3
"""Manual A/B controls over original Git, ZIP, native-shaped inputs and XML."""
import copy
from contextlib import redirect_stdout
import io
import json
from pathlib import Path
import subprocess
from types import SimpleNamespace
from importlib.util import module_from_spec, spec_from_file_location


def module(name, filename):
    spec = spec_from_file_location(name, Path(__file__).with_name(filename))
    value = module_from_spec(spec)
    spec.loader.exec_module(value)
    return value


m = module('manual_mechanism_inputs', '_ci-mechanism-inputs.py')
fixture = module('manual_mechanism_fixture', '_ci-case-partition-smoke.py')
g, count = m.g, 0
fmod, cases = fixture.fixture, fixture.case_fixture.m


def check(value, message):
    global count
    assert value, message
    count += 1


def rejected(call, message, reason=None):
    try:
        call()
    except (ValueError, OSError, KeyError, TypeError) as error:
        check(reason is None or reason in str(error), message + ': ' + str(error))
    else:
        raise AssertionError('accepted ' + message)


EXTRA = {
    'runtime/engine/pom.xml': '<project><groupId>io.tapstate</groupId><artifactId>engine</artifactId><version>1</version></project>',
    'e2e/pom.xml': '<project><groupId>io.tapstate</groupId><artifactId>e2e</artifactId><version>1</version><build><plugins><plugin><artifactId>maven-failsafe-plugin</artifactId><executions><execution><goals><goal>integration-test</goal></goals></execution></executions></plugin></plugins></build></project>',
    'runtime/engine/src/test/java/io/tapstate/fixture/OtherTest.java': 'package io.tapstate.fixture; class OtherTest { @Test void holds() {} }\n',
    'e2e/src/test/java/io/tapstate/fixture/ThirdIT.java': 'package io.tapstate.fixture; class ThirdIT { @Test void holds() {} }\n',
    'e2e/src/test/java/io/tapstate/fixture/FourthIT.java': 'package io.tapstate.fixture; class FourthIT { @Test void holds() {} }\n',
    '.github/ci-shard-layout.json': '{"schema":1,"e2e_shards":2,"rest_modules":{},"new_test_seconds":30}\n'}


def source_setup(root):
    path = root / 'pom.xml'
    text = path.read_text()
    assert text.count('</modules>') == 1
    path.write_text(text.replace('</modules>', '<module>e2e</module><module>runtime/engine</module></modules>'))


def prepare_four(f):
    plan, pre, artifacts = f['plan'], f['pre'], f['artifacts']
    extra_classes = {path.replace('/src/test/java/', '/target/test-classes/').removesuffix('.java') + '.class':
                     (Path(path).stem + ' protocol class').encode() for path in EXTRA if path.endswith('.java')}
    for path, raw in extra_classes.items():
        pre['test_files'][path] = g.checksum(raw)
        pre['test_class_ids'][path] = g.receipt.crc64(raw)
    layout, layout_source = fmod.s.layout_config(SimpleNamespace(), f['work'])
    plan['expected'] = fmod.s.discover(f['work'])
    plan['shards'] = [{'id': name, 'tests': tests, 'estimated_seconds': seconds, 'required_exec': fmod.s.execs(tests)}
                      for name, tests, seconds in fmod.s.default_groups(plan['expected'], {}, layout)]
    plan.update(layout=layout, layout_source=layout_source)
    plan['cohort_hash'] = fmod.s.cohort(plan['expected'])
    pre['test_resource_files'] = {}
    pre['resolved_test_classpaths'].update({'runtime/engine': [], 'e2e': []})
    if 'resolved_maven_artifacts' in pre:
        pre['resolved_maven_artifacts'] = fmod.m.maven_artifacts.capture(pre, lambda path: (f['work'] / path).read_bytes())
    pre['runner_context']['workflow_source']['workflow_ref'] = ('example/project/.github/workflows/ci.yml@'
        + ('refs/heads/ws/fixture' if f['scope']['event'] == 'workflow_dispatch' else 'refs/pull/7/merge'))
    pre['maven_execution_observation']['runner_context'] = copy.deepcopy(pre['runner_context'])
    worker_inputs = {}
    for worker in plan['shards']:
        name, test = worker['id'], worker['tests'][0]
        before = copy.deepcopy(pre)
        before['runner_context']['shard_id'] = name
        before['maven_execution_observation']['runner_context'] = copy.deepcopy(before['runner_context'])
        after = dict(copy.deepcopy(before), phase='post-test-before-pack')
        worker_inputs[name] = before
        provenance = copy.deepcopy(f['provenance'])
        provenance.update(shard_id=name, cohort_hash=plan['cohort_hash'])
        provenance['case_observer']['build_inputs_sha256'] = g.checksum(fmod.encode(before))
        provenance['case_observer']['checked_scopes'] = [{'module': test['module'], 'kind': test['kind'], 'dependency_excludes': []}]
        for phase in ('reset', 'verify'):
            provenance[phase].update(shard_id=name, cohort_hash=plan['cohort_hash'])
        marker = fmod.s.provenance_path(worker).as_posix()
        if name == 'rest':
            files, xml_path, xml, reported = dict(f['result_files']), f['xml_path'], f['xml'], len(f['fixture_methods'])
        else:
            files = {path: raw for path, raw in extra_classes.items() if path.startswith(test['module'] + '/target/')}
            xml_path = test['module'] + '/target/' + ('surefire' if test['kind'] == 'unit' else 'failsafe') + '-reports/TEST-' + test['class'] + '.xml'
            xml = ('<testsuite name="' + test['class'] + '" tests="1" skipped="0" errors="0" failures="0">'
                   '<testcase classname="' + test['class'] + '" name="holds"/></testsuite>').encode()
            files[xml_path] = xml
            for execution in worker['required_exec']:
                files[execution] = b'coverage protocol fixture'
            reported = 1
        files[marker] = fmod.encode(provenance)
        manifest = copy.deepcopy(f['manifest'])
        manifest.update(shard_id=name, cohort_hash=plan['cohort_hash'], tests=worker['tests'], required_exec=worker['required_exec'],
                        provenance=provenance, provenance_path=marker, report_classes=[test['class']],
                        statistics={'reported_classes': 1, 'tests': reported, 'skipped': 0, 'zero_case_classes': 0},
                        files={path: g.checksum(raw) for path, raw in files.items()})
        artifacts['ci-inputs-' + name] = {'build-inputs.json': before, 'post-test-inputs.json': after}
        artifacts['ci-test-results-' + name] = {name + '/manifest.json': manifest,
                                               **{name + '/files/' + path: raw for path, raw in files.items()}}
        artifacts['ci-aggregate'].update({xml_path: xml, 'target/ci-shards/' + name + '/run.json': fmod.encode(provenance)})
    build = copy.deepcopy(pre)
    build['runner_context']['shard_id'] = 'build'
    build['runner_context']['workflow_source']['job'] = 'build'
    build['maven_execution_observation']['runner_context'] = copy.deepcopy(build['runner_context'])
    equality = g.receipt.compare_input_snapshots(plan, build, worker_inputs)
    equality['input_files'] = {'build': g.checksum(fmod.encode(build)),
                             **{name: g.checksum(fmod.encode(value)) for name, value in worker_inputs.items()}}
    artifacts['ci-inputs-build'] = {'build-inputs.json': build, 'build-input-equivalence.json': equality}
    artifacts['ci-shard-plan']['shards.json'] = plan
    f['records'][:] = [{'id': n, 'name': name, 'expired': False, 'workflow_run': {
        'id': f['scope']['run_id'], 'head_sha': f['scope']['head_sha'], 'head_branch': f['scope']['branch']}}
        for n, name in enumerate(artifacts, 1)]
    f['reseal']()


def prepare_cases(f):
    data, ev = f['root'] / 'manual-case-data', cases.ev
    data.mkdir()
    inv, unused = cases.source_inventory(f['work'], f['merge'], g.checksum(fmod.v.git(
        f['work'], 'archive', '--format=tar', f['merge']).stdout), 'ci', data / 'source.json')
    for worker in f['plan']['shards']:
        name = worker['id']
        members = f['artifacts']['ci-inputs-' + name]
        inv_path = data / (name + '-inventory.json')
        ev.output(inv_path, inv)
        expected = data / (name + '-expected.json')
        with redirect_stdout(io.StringIO()):
            ev.export_hashes(SimpleNamespace(input=str(inv_path), output=str(expected)))
        target = data / name
        target.mkdir()
        for path, raw in f['artifacts']['ci-test-results-' + name].items():
            cases.data_file(target, path, raw if isinstance(raw, bytes) else fmod.encode(raw))
        input_path = cases.data_file(target, 'inputs.json', fmod.encode(members['build-inputs.json']))
        binding = cases.data_file(target, 'binding.json', fmod.encode(
            {'source_sha': f['merge'], 'run_id': f['scope']['run_id'], 'attempt': 1}))
        gates = target / 'gates'
        gates.mkdir()
        observed, hashes = target / 'observed.json', target / 'hashes.json'
        with redirect_stdout(io.StringIO()):
            ev.reports(SimpleNamespace(root=str(target / name / 'files'), inventory=str(inv_path), lane='ci', inputs=str(input_path),
                       binding=str(binding), class_gates=str(gates), artifact_manifest=str(target / name / 'manifest.json'),
                       shard_plan=None, shard=None, output=str(observed)))
            ev.export_hashes(SimpleNamespace(input=str(observed), output=str(hashes)))
        members.update({'expected-case-hashes.json': json.loads(expected.read_bytes()),
                        'observed-case-hashes.json': json.loads(hashes.read_bytes())})
    f['reseal']()
    run = g.load(f['evidence'] / 'run/run.json')
    index = 'AB'.index(f['scope']['variant'])
    run.update(status='completed', conclusion='success', path='.github/workflows/ci.yml',
               created_at=f'2026-10-14T00:0{index * 2}:00Z', updated_at=f'2026-10-14T00:0{index * 2 + 1}:00Z')
    f['save']('run/run.json', run)
    f['source_request']['archive'] = f['seal']()
    f['request']['source'] = f['save']('source-request.json', f['source_request'])
    return {'schema': 1, 'type': 'archive-cases-request', 'archive_inputs': f['save']('input-request.json', f['request'])}


def prepare_pair(root, contexts, mutate=None):
    def save(name, value):
        path = root / name
        path.write_bytes(fmod.encode(value))
        return {'path': name, 'sha256': g.checksum(path.read_bytes())}
    variants, facts = {}, {}
    for name, context in contexts.items():
        prepare_four(context)
        if mutate:
            mutate(name, context)
        reference = context['save']('manual-cases-request.json', prepare_cases(context))
        reference = fixture.rebase_reference(reference, context['evidence'], root, {})
        variants[name] = {'archive_cases': reference, 'source_root': str(context['work'])}
        facts[name] = m.delta.components.project(g.References(root), context['work'],
            {'schema': 1, 'type': 'archived-group-components-request', 'archive_cases': reference})
    entries = []
    for component, field in sorted(m.delta.POLICY_FIELDS):
        values = {name: (value['payloads']['I']['source_inventory'].get(field, value['payloads']['I'].get(field))
                        if component == 'I' else value['payloads'][component][field]) for name, value in facts.items()}
        observed = m.delta.hashes(values)
        if len(set(observed.values())) != 1:
            entries.append({'component': component, 'field': field, 'hashes': observed,
                            'reason': 'Source addition remains separate from original assertion semantics', 'evidence': [variants['B']['archive_cases']]})
    policy = {'schema': 1, 'type': 'declared-mechanism-delta-policy', 'entries': entries}
    request = {'schema': 1, 'type': 'archived-mechanism-inputs-request', 'variants': variants,
               'review_policy': save('policy.json', policy), 'order': ['A', 'B'], 'cache_condition': 'warm'}
    return request, facts, policy, save


def controls(root, contexts, unused_complete, unused_added):
    request, facts, policy, save = prepare_pair(root, contexts)
    variants = request['variants']
    answer = m.project(g.References(root), request)
    check(answer['comparison_ready'] and answer['fixed_four_workers_verified'], 'original manual archives retain four workers and typed facts')
    check(len(answer['case_partition']['original_cases']) == 4 and len(answer['case_partition']['additions']['B']) == 1,
          'source and original reports independently derive originals and additions')
    check(all(answer[key] is False for key in ('cache_condition_verified', 'original_assertions_verified', 'normalized_original_inputs_verified',
              'whole_build_provenance_complete', 'group_equivalence_proven', 'first_complete_PR_sample', 'performance_accepted', 'latest_tip_verified')),
          'manual input readiness never becomes cache, assertions, PR or performance proof')
    changed = copy.deepcopy(request)
    changed['order'].reverse()
    rejected(lambda: m.project(g.References(root), changed), 'reversed actual run order')
    for label, mutate in [('extra C', lambda x: x['variants'].update(C=variants['B'])),
                          ('boolean schema', lambda x: x.update(schema=True)),
                          ('unknown cache', lambda x: x.update(cache_condition='unknown')),
                          ('reused source', lambda x: x['variants'].update(B=variants['A']))]:
        changed = copy.deepcopy(request)
        mutate(changed)
        rejected(lambda: m.project(g.References(root), changed), label)
    rejected(lambda: m.delta.policy_entries(g.References(root), request['review_policy']), 'manual policy cannot enter the three-variant PR path')
    rejected(lambda: m.partition.partition({'A': answer['source_inventories']['A'], 'B': answer['source_inventories']['B']}),
             'manual pair cannot enter the original PR partition')
    changed = copy.deepcopy(request)
    changed['review_policy'] = save('unreviewed-policy.json', dict(policy, entries=[]))
    unreviewed = m.project(g.References(root), changed)
    check(not unreviewed['comparison_ready'] and any(row['status'] == 'needs_review' for row in unreviewed['fields']),
          'unequal source and test-JAR facts remain unreviewed')
    stale = copy.deepcopy(policy)
    stale['entries'][0]['hashes']['B'] = 'f' * 64
    changed['review_policy'] = save('stale-policy.json', stale)
    rejected(lambda: m.project(g.References(root), changed), 'stale two-variant delta policy', 'delta policy values differ')
    for component, field in [('P', 'production'), ('D', 'resolved_dependency_identities'), ('spec', 'observed'),
                             ('U', 'owned_recipe_digest'), ('I', 'test_resource_files')]:
        forbidden = {'schema': 1, 'type': 'declared-mechanism-delta-policy', 'entries': [{
            'component': component, 'field': field, 'hashes': {'A': '1' * 64, 'B': '2' * 64},
            'reason': 'Unsupported hard-input exception', 'evidence': [request['variants']['A']['archive_cases']]}]}
        reference = save('forbidden-' + component + '.json', forbidden)
        rejected(lambda: m.delta.mechanism_policy_entries(g.References(root), reference),
                 'hard ' + component + ' cannot be waived', 'unsupported or duplicate')
    cold = dict(request, cache_condition='cold')
    claimed = m.project(g.References(root), cold)
    check(claimed['cache_condition_claim'] == 'cold' and not claimed['cache_condition_verified'],
          'changing a cache claim does not prove a cold execution')
    resources = {name: {**value['payloads'], 'I': {**value['payloads']['I'], 'test_resource_files': {
        'fixture/target/test-classes/generated.properties': ('1' if name == 'A' else '2') * 64}}} for name, value in facts.items()}
    parsed = m.delta.mechanism_policy_entries(g.References(root), request['review_policy'])
    rejected(lambda: m.delta.compare_payloads(resources, answer['case_partition'], parsed),
             'different original prepared resources', 'I.test_resource_files')
    manifest = root / 'manual-inputs-request.json'
    manifest.write_bytes(fmod.encode(request))
    output = root / 'manual-inputs.json'
    command = ['bash', str(Path(__file__).with_name('ci-mechanism-inputs.sh')), '--manifest', str(manifest), '--evidence-root', str(root), '--output', str(output)]
    child = subprocess.run(command, capture_output=True, text=True)
    check(child.returncode == 0 and g.load(output) == answer, 'actual manual CLI replays the full original archive chain: ' + child.stderr)
    raw = output.read_bytes()
    check(subprocess.run(command, capture_output=True).returncode != 0 and output.read_bytes() == raw, 'exclusive output preserves prior evidence')


def pr_boundary(root, contexts, unused_complete, unused_added):
    request, unused, unused_policy, unused_save = prepare_pair(root, contexts)
    rejected(lambda: m.project(g.References(root), request), 'PR evidence cannot become a manual mechanism sample',
             'mechanism variant/event/attempt differs')


def legacy_boundary(root, contexts, unused_complete, unused_added):
    request, unused, unused_policy, unused_save = prepare_pair(root, contexts)
    answer = m.project(g.References(root), request)
    check(answer['case_partition']['original_case_partition_verified'] and answer['fixed_four_workers_verified'],
          'legacy literal source and four recorded workers remain readable')
    check(not any(answer['current_native_dependency_packets_present'].values()) and not answer['comparison_ready'],
          'missing native dependency packets cannot qualify a current pair')


def unresolved_boundary(root, contexts, unused_complete, unused_added):
    request, unused, unused_policy, unused_save = prepare_pair(root, contexts)
    answer = m.project(g.References(root), request)
    check(not answer['case_partition']['original_case_partition_verified'] and not answer['comparison_ready'],
          'unresolved computed source cannot qualify a manual pair')
    check(answer['case_partition']['original_cases'] is None and answer['case_partition']['additions'] is None
          and all(value['unresolved'] for value in answer['source_inventories'].values()),
          'unknown provider identities remain explicit without invented original/addition rows')


def stale_layout_boundary(root, contexts, unused_complete, unused_added):
    def mutate(name, context):
        if name == 'A':
            context['plan']['layout_source']['sha256'] = 'f' * 64
    request, unused, unused_policy, unused_save = prepare_pair(root, contexts, mutate)
    rejected(lambda: m.project(g.References(root), request), 'layout facts cannot replace original source bytes',
             'mechanism layout differs from original source')


def missing_layout_boundary(root, contexts, unused_complete, unused_added):
    def mutate(name, context):
        if name == 'A':
            context['plan'].pop('layout')
            context['plan'].pop('layout_source')
    request, unused, unused_policy, unused_save = prepare_pair(root, contexts, mutate)
    answer = m.project(g.References(root), request)
    check(answer['case_partition']['original_case_partition_verified'] and not answer['fixed_four_workers_verified']
          and not answer['comparison_ready'], 'four worker names without original layout cannot qualify a pair')


if __name__ == '__main__':
    metadata = module('mechanism_dependency_fixture', '_ci-group-delta-smoke.py').dependency_metadata
    fixture.cohort(added=True, callback=controls, test_jar=True, resolved_artifacts=True, maven_schema=4,
                   event_name='workflow_dispatch', variants='AB', extra_source_files=EXTRA, dependency_metadata=metadata, source_setup=source_setup)
    fixture.cohort(added=True, callback=pr_boundary, test_jar=True, resolved_artifacts=True, maven_schema=4,
                   event_name='pull_request', variants='AB', extra_source_files=EXTRA, dependency_metadata=metadata, source_setup=source_setup)
    fixture.cohort(added=True, callback=legacy_boundary, test_jar=True, event_name='workflow_dispatch',
                   variants='AB', extra_source_files=EXTRA, dependency_metadata=metadata, source_setup=source_setup)
    fixture.cohort(source_case='''package io.tapstate.fixture;
class SampleTest {
 @Test void holds() {}
 @ParameterizedTest @MethodSource("computed") void dynamic(int value) {}
 static Stream<Integer> computed() { return external(); }
}
''', complete=False, callback=unresolved_boundary, test_jar=True, resolved_artifacts=True, maven_schema=4,
                   event_name='workflow_dispatch', variants='AB', extra_source_files=EXTRA, dependency_metadata=metadata, source_setup=source_setup)
    for boundary in (stale_layout_boundary, missing_layout_boundary):
        fixture.cohort(added=True, callback=boundary, test_jar=True, resolved_artifacts=True, maven_schema=4,
                       event_name='workflow_dispatch', variants='AB', extra_source_files=EXTRA,
                       dependency_metadata=metadata, source_setup=source_setup)
    print('CI manual mechanism input controls passed:', count)
