#!/usr/bin/env python3
"""Independent source and original XML/hash controls over the shared real-Git fixture."""
import copy
import importlib.util
import io
import json
from contextlib import redirect_stdout
from pathlib import Path
import subprocess
import sys
import tempfile
from types import SimpleNamespace
import xml.etree.ElementTree as ET

sys.dont_write_bytecode = True


def load(name, filename):
    spec = importlib.util.spec_from_file_location(name, Path(__file__).with_name(filename))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


fixture = load('archive_fixture', '_ci-archive-inputs-smoke.py')
m = load('case_reader', '_ci-archive-cases.py')
g, ev = m.g, m.ev
count = 0


def check(value, message):
    global count
    assert value, message
    count += 1


def rejected(call, message):
    try:
        call()
    except (ValueError, OSError, KeyError, TypeError) as error:
        check(message in str(error), 'wrong rejection: ' + str(error))
    else:
        raise AssertionError('expected rejection: ' + message)


def prepare_cases(f, transform_inventory=None):
    data = f['root'] / 'producer-data'
    data.mkdir()
    inv_path = data / 'inventory.json'
    inv, unused = m.source_inventory(f['work'], f['merge'], g.checksum(fixture.v.git(f['work'], 'archive', '--format=tar', f['merge']).stdout), 'ci', inv_path)
    if transform_inventory is not None:
        inv = transform_inventory(inv)
        ev.output(inv_path, inv)
    expected_path = data / 'expected.json'
    with redirect_stdout(io.StringIO()):
        ev.export_hashes(SimpleNamespace(input=str(inv_path), output=str(expected_path)))
    target = data / 'rest'
    target.mkdir()
    for name, raw in f['result_files'].items():
        m.data_file(target, 'files/' + name, raw)
    manifest_path = m.data_file(target, 'manifest.json', fixture.encode(f['manifest']))
    input_path = m.data_file(data, 'inputs.json', fixture.encode(f['pre']))
    binding_path = m.data_file(data, 'binding.json', fixture.encode({'source_sha': f['merge'], 'run_id': f['scope']['run_id'], 'attempt': 1}))
    gates = data / 'gates'
    gates.mkdir()
    obs_path = data / 'observations.json'
    with redirect_stdout(io.StringIO()):
        ev.reports(SimpleNamespace(root=str(target / 'files'), inventory=str(inv_path), lane='ci', inputs=str(input_path),
                                  binding=str(binding_path), class_gates=str(gates), artifact_manifest=str(manifest_path),
                                  shard_plan=None, shard=None, output=str(obs_path)))
    hashes_path = data / 'observed.json'
    observation = ev.load(obs_path)
    observation['cases'] = [row for row in observation['cases'] if row.get('parameter') is not None]
    ev.output(obs_path, observation)
    with redirect_stdout(io.StringIO()):
        ev.export_hashes(SimpleNamespace(input=str(obs_path), output=str(hashes_path)))
    artifacts = f['artifacts']
    artifacts['ci-inputs-rest']['expected-case-hashes.json'] = json.loads(expected_path.read_bytes())
    artifacts['ci-inputs-rest']['observed-case-hashes.json'] = json.loads(hashes_path.read_bytes())
    f['reseal']()
    request = {'schema': 1, 'type': 'archive-cases-request', 'archive_inputs': f['save']('input-request.json', f['request'])}
    return request, inv


def controls(f):
    request, inv = prepare_cases(f)
    artifacts = f['artifacts']
    original = copy.deepcopy(artifacts)

    def reseal():
        f['reseal']()
        request['archive_inputs'] = f['save']('input-request.json', f['request'])

    def reset():
        artifacts.clear()
        artifacts.update(copy.deepcopy(original))
        reseal()

    def project():
        return m.project(g.References(f['evidence']), f['work'], request, include_test_observations=True)

    answer = project()
    exported = answer['test_observations']
    exported_inventory = json.loads(exported['source_inventory_json'])
    check(g.checksum(exported['source_inventory_json'].encode()) == exported['source_inventory_sha256']
          and exported_inventory['methods'] == answer['source_inventory']['methods'],
          'full outcomes retain the exact independently reconstructed source inventory')
    worker_export = exported['workers']['rest']
    observation = json.loads(worker_export['observation_json'])
    check(g.checksum(worker_export['observation_json'].encode()) == worker_export['observation_sha256']
          and worker_export['selected_tests'] == f['plan']['shards'][0]['tests']
          and worker_export['result_manifest_sha256'] == g.checksum(fixture.encode(f['manifest'])),
          'full outcomes retain exact raw replay bytes and actual selected manifest')
    check(observation['source_inventory_sha256'] == exported['source_inventory_sha256']
          and observation['input_binding']['source_sha'] == f['merge']
          and observation['inputs_sha256'] == g.checksum(fixture.encode(artifacts['ci-inputs-rest']['build-inputs.json'])),
          'full outcomes retain their actual worker source and prepared input binding')
    if not inv['complete']:
        check(answer['source_case_subset_verified'] and not answer['all_source_cases_statically_parsed'],
              'computed provider stays outside the verified static subset')
        check('computed_provider_identities' in answer['unverified_components']
              and not answer['whole_input_projection_verified'], 'computed provider cannot become complete from matching declarations')
        check(answer['source_inventory']['unresolved'], 'actual computed source gap is retained')
        check(not observation['complete'] and not exported_inventory['complete'] and exported_inventory['unresolved'],
              'full export preserves source incompleteness even when XML has only known methods')
        print('CI archive computed provider controls passed: 7')
        return
    check(answer['source_class_cohort_verified'] and answer['source_case_subset_verified'], 'source and raw XML are independently joined')
    check(answer['all_source_cases_statically_parsed'], 'simple literal source methods resolve completely')
    check(len(answer['source_inventory']['cases']) == 1, 'original source case count is derived')
    check(not answer['whole_input_projection_verified'] and not answer['first_complete_PR_sample']
          and not answer['performance_accepted'], 'case subset readback cannot certify the other input components')
    full_inventory = f['evidence'] / 'full-inventory.json'
    full_inventory.write_text(exported['source_inventory_json'])
    full_observation = f['evidence'] / 'full-observation.json'
    full_observation.write_text(json.dumps(observation))
    comparison = f['evidence'] / 'full-comparison.json'
    args = SimpleNamespace(inventory=str(full_inventory), candidate_inventory=None, additions=None,
                           before=str(full_observation), after=str(full_observation), output=str(comparison))
    ev.compare_tests(args)
    check(json.loads(comparison.read_text())['complete'], 'full export supports the status and skip comparator')
    changed_observation = copy.deepcopy(observation)
    changed_observation['cases'][0].update(status='skipped', skip={'type': None, 'reason': 'changed outcome'})
    changed_path = f['evidence'] / 'changed-outcome.json'
    changed_path.write_text(json.dumps(changed_observation))
    args.after = str(changed_path)
    rejected(lambda: ev.compare_tests(args), 'per-case skip status or reason changed')
    foreign = copy.deepcopy(artifacts['ci-inputs-rest']['expected-case-hashes.json']['cases'][0])
    foreign['method'] = 'foreign'
    artifacts['ci-inputs-rest']['expected-case-hashes.json']['cases'].append(foreign)
    reseal()
    rejected(project, 'case row has no source method')
    reset()
    foreign = copy.deepcopy(artifacts['ci-inputs-rest']['observed-case-hashes.json']['cases'][0])
    foreign['method'] = 'foreign'
    artifacts['ci-inputs-rest']['observed-case-hashes.json']['cases'].append(foreign)
    reseal()
    rejected(project, 'case row has no source method')
    reset()
    expected = artifacts['ci-inputs-rest']['expected-case-hashes.json']
    expected['methods'] = []
    reseal()
    rejected(project, 'method set differs from source')
    reset()
    artifacts['ci-inputs-rest']['expected-case-hashes.json']['cases'][0]['case_digest'] = 'f' * 64
    reseal()
    rejected(project, 'static case identities/skip policy differ')
    reset()
    artifacts['ci-inputs-rest']['expected-case-hashes.json']['methods'][0]['source_sha256'] = 'f' * 64
    reseal()
    rejected(project, 'declaration differs from source')
    reset()
    artifacts['ci-inputs-rest']['observed-case-hashes.json']['input_binding']['attempt'] = 2
    reseal()
    rejected(project, 'input/inventory binding differs')
    reset()
    artifacts['ci-inputs-rest']['observed-case-hashes.json']['cases'][0]['status'] = 'skipped'
    reseal()
    rejected(project, 'observations differ from raw XML/ledger replay')
    reset()
    artifacts['ci-inputs-rest']['observed-case-hashes.json']['actual_xml_case_count'] = 2
    reseal()
    rejected(project, 'counters differ from raw replay')
    reset()
    artifacts['ci-inputs-rest']['observed-case-hashes.json']['class_gate_files'] = {'omitted.json': 'f' * 64}
    reseal()
    rejected(project, 'class gate/ledger digests differ')
    reset()
    changed = f['xml'].replace(b'name="holds"', b'name="changed"')
    artifacts['ci-test-results-rest']['rest/files/' + f['xml_path']] = changed
    artifacts['ci-test-results-rest']['rest/manifest.json']['files'][f['xml_path']] = g.checksum(changed)
    reseal()
    rejected(project, 'observations differ from raw XML/ledger replay')
    reset()
    empty = b'<testsuite name="io.tapstate.fixture.SampleTest" tests="0" skipped="0" errors="0" failures="0"/>'
    artifacts['ci-test-results-rest']['rest/files/' + f['xml_path']] = empty
    artifacts['ci-test-results-rest']['rest/manifest.json']['files'][f['xml_path']] = g.checksum(empty)
    artifacts['ci-test-results-rest']['rest/manifest.json']['statistics'].update(tests=0, zero_case_classes=1)
    actual = artifacts['ci-inputs-rest']['observed-case-hashes.json']
    actual['cases'] = []
    actual['actual_xml_case_count'] = 0
    reseal()
    rejected(project, 'source case multiset differs')
    reset()
    output = f['evidence'] / 'cases-cli.json'
    manifest = f['evidence'] / 'case-request.json'
    manifest.write_bytes(fixture.encode(request))
    command = ['bash', str(Path(__file__).with_name('ci-archive-cases.sh')), '--manifest', str(manifest),
               '--evidence-root', str(f['evidence']), '--root', str(f['work']), '--output', str(output)]
    child = subprocess.run(command, capture_output=True, text=True)
    check(child.returncode == 0 and json.loads(output.read_bytes())['source_inventory_digest'] == answer['source_inventory_digest'],
          'case CLI replays original members: ' + child.stderr)
    check('test_observations' not in json.loads(output.read_bytes()), 'default output retains its existing scope')
    before = output.read_bytes()
    check(subprocess.run(command, capture_output=True).returncode != 0 and output.read_bytes() == before,
          'case output is created exclusively')
    full_output = f['evidence'] / 'cases-cli-full.json'
    command[command.index('--output') + 1] = str(full_output)
    child = subprocess.run(command + ['--include-test-observations'], capture_output=True, text=True)
    check(child.returncode == 0 and json.loads(full_output.read_bytes())['test_observations'] == exported,
          'CLI opt-in exports the same validated full outcomes: ' + child.stderr)
    print('CI archive case controls passed: ' + str(count))


def two_worker_controls(f, verify=True):
    data = f['root'] / 'two-worker-producer'
    data.mkdir()
    source_path = data / 'source.json'
    inv, unused = m.source_inventory(f['work'], f['merge'], g.checksum(fixture.v.git(
        f['work'], 'archive', '--format=tar', f['merge']).stdout), 'ci', source_path)
    inventory_digests = []
    for worker in f['plan']['shards']:
        name = worker['id']
        members = f['artifacts']['ci-inputs-' + name]
        prepared = fixture.encode(members['build-inputs.json'])
        producer_inv = copy.deepcopy(inv)
        producer_inv['provider_probe'] = {'schema': 1, 'actual_source_sha': f['merge'],
                                          'original_source_sha': f['merge'], 'inputs_sha256': g.checksum(prepared)}
        inv_path = data / (name + '-inventory.json')
        ev.output(inv_path, producer_inv)
        expected_path = data / (name + '-expected.json')
        with redirect_stdout(io.StringIO()):
            ev.export_hashes(SimpleNamespace(input=str(inv_path), output=str(expected_path)))
        target = data / name
        target.mkdir()
        for path, raw in f['artifacts']['ci-test-results-' + name].items():
            m.data_file(target, path, raw if isinstance(raw, bytes) else fixture.encode(raw))
        input_path = m.data_file(target, 'inputs.json', prepared)
        binding_path = m.data_file(target, 'binding.json', fixture.encode(
            {'source_sha': f['merge'], 'run_id': int(members['build-inputs.json']['run_id']),
             'attempt': int(members['build-inputs.json']['run_attempt'])}))
        gates = target / 'gates'
        gates.mkdir()
        obs_path, hashes_path = target / 'observations.json', target / 'hashes.json'
        with redirect_stdout(io.StringIO()):
            ev.reports(SimpleNamespace(root=str(target / name / 'files'), inventory=str(inv_path), lane='ci',
                        inputs=str(input_path), binding=str(binding_path), class_gates=str(gates),
                        artifact_manifest=str(target / name / 'manifest.json'), shard_plan=None, shard=None,
                        output=str(obs_path)))
            ev.export_hashes(SimpleNamespace(input=str(obs_path), output=str(hashes_path)))
        members['expected-case-hashes.json'] = json.loads(expected_path.read_bytes())
        members['observed-case-hashes.json'] = json.loads(hashes_path.read_bytes())
        inventory_digests.append(members['expected-case-hashes.json']['source_inventory_sha256'])
    check(len(set(inventory_digests)) == 2, 'worker-specific inputs produce distinct inventory digests')
    f['reseal']()
    request = {'schema': 1, 'type': 'archive-cases-request',
               'archive_inputs': f['save']('two-worker-request.json', f['request'])}
    if not verify:
        return request
    answer = m.project(g.References(f['evidence']), f['work'], request, include_test_observations=True)
    check(answer['all_source_cases_statically_parsed'] and len(answer['workers']) == 2,
          'same source and different worker capture metadata are valid')
    check(len({v['source_inventory_sha256'] for v in answer['workers'].values()}) == 2,
          'each original worker inventory binding is retained')
    exported = answer['test_observations']
    check(set(exported['workers']) == {'rest', 'engine'}
          and len({json.loads(v['observation_json'])['inputs_sha256'] for v in exported['workers'].values()}) == 2
          and all(json.loads(v['observation_json'])['inputs_sha256'] == v['prepared_input_member_sha256']
                  for v in exported['workers'].values()),
          'full export preserves distinct worker inputs without inventing an aggregate invocation')
    print('CI archive two-worker controls passed: 4')


def unresolved_xml_controls(f):
    suite = ET.fromstring(f['xml'])
    ET.SubElement(suite, 'testcase', classname='io.tapstate.fixture.SampleTest', name='dynamic(int)[1]')
    suite.set('tests', '2')
    raw = ET.tostring(suite)
    path = f['xml_path']
    f['result_files'][path] = raw
    f['manifest']['files'][path] = g.checksum(raw)
    f['manifest']['statistics']['tests'] = 2
    f['artifacts']['ci-test-results-rest']['rest/files/' + path] = raw
    f['artifacts']['ci-test-results-rest']['rest/manifest.json'] = copy.deepcopy(f['manifest'])
    f['artifacts']['ci-aggregate'][path] = raw
    f['reseal']()
    request, unused = prepare_cases(f)
    answer = m.project(g.References(f['evidence']), f['work'], request, include_test_observations=True)
    observation = json.loads(answer['test_observations']['workers']['rest']['observation_json'])
    check(len(observation['cases']) == 2 and sum(row['parameter'] is None for row in observation['cases']) == 1,
          'full export retains the actual unresolved XML case before hash filtering')
    check(observation['unresolved'] and not observation['complete']
          and len(answer['workers']['rest']['replayed_cases']) == 1,
          'unresolved XML remains incomplete while the hash subset contains only the resolved case')
    print('CI archive unresolved XML controls passed: 2')


def published_controls():
    identity = {'lane': 'ci', 'module': 'e2e', 'kind': 'it', 'class': 'io.tapstate.e2e.PublishedExamplesIT', 'method': 'runs'}
    policies = {'one.e2e.yml on IN_PROCESS': {'expected_status': 'executed', 'skip_message': None},
                'one.e2e.yml on REAL_PROCESS': {'expected_status': 'skipped', 'skip_message': 'required connector unavailable'}}
    inv = {'methods': [{**identity, 'published_policy': {'cases': policies}}]}
    cases = [{**identity, 'parameter': name, 'status': policy['expected_status'],
              'skip': None if policy['skip_message'] is None else {'reason': policy['skip_message']}}
             for name, policy in policies.items()]
    ledger = ['one.e2e.yml on IN_PROCESS']
    m.published_observations(inv, cases, ledger)
    check(True, 'source executed/skip and ledger contracts agree')
    changed = copy.deepcopy(cases)
    changed[0]['status'] = 'skipped'
    rejected(lambda: m.published_observations(inv, changed, []), 'outcome differs from source applicability')
    changed = copy.deepcopy(cases)
    changed[1]['skip']['reason'] = 'different skip reason'
    rejected(lambda: m.published_observations(inv, changed, ledger), 'skip reason differs')
    rejected(lambda: m.published_observations(inv, cases, []), 'ledger differs')
    rejected(lambda: m.published_observations(inv, cases, ledger + ledger), 'ledger differs')
    print('CI archive published source-policy controls passed: 5')


def source_entry_controls():
    with tempfile.TemporaryDirectory(prefix='ci-published-entry-data-') as temporary:
        root = Path(temporary).resolve()
        m.data_file(root, 'pom.xml', b'<project><modules><module>e2e</module></modules></project>')
        m.data_file(root, 'e2e/pom.xml', b'<project><build><plugins><plugin><artifactId>maven-failsafe-plugin</artifactId><executions><execution><goals><goal>integration-test</goal></goals></execution></executions></plugin></plugins></build></project>')
        actual = Path(__file__).resolve().parents[2] / 'e2e/src/test/java/io/tapstate/e2e'
        for name in ['PublishedExamplesIT.java', 'E2eConnectorJar.java', 'RealConnectorGate.java',
                     'WitnessLedger.java', 'PublishedExampleSelection.java']:
            m.data_file(root, 'e2e/src/test/java/io/tapstate/e2e/' + name, (actual / name).read_bytes())
        m.data_file(root, 'e2e/examples/one/case.e2e.yml', b'setup:\n  connectors: [mysql]\n')
        workflow = m.data_file(root, '.github/workflows/ci.yml', b'jobs:\n  test:\n    run: |\n      .github/scripts/ci-evidence.sh inventory --root . --lane ci --published-connectors unknown --native-cli-presence absent --output source-inventory.json\n')
        inv_path = root / 'inventory.json'
        try:
            rejected(lambda: m.source_inventory(root, 'a' * 40, 'a' * 64, 'ci', inv_path),
                     'published connector availability is unknown')
            workflow.write_text('jobs:\n  test:\n    run: |\n      .github/scripts/ci-evidence.sh inventory --root . --lane ci --native-cli-presence absent --published-connectors absent --output source-inventory.json\n')
            rejected(lambda: m.source_inventory(root, 'a' * 40, 'a' * 64, 'ci', inv_path),
                     'published connector availability is unknown')
            workflow.write_text('jobs:\n  test:\n    run: |\n      .github/scripts/ci-evidence.sh inventory --root . --lane ci --published-connectors absent --native-cli-presence absent --output source-inventory.json\n')
            inv, entry = m.source_inventory(root, 'a' * 40, 'a' * 64, 'ci', inv_path)
            policies = [p for method in inv['methods'] for p in (method.get('published_policy') or {}).get('cases', {}).values()]
            check(policies and all(p['expected_status'] == 'skipped' for p in policies)
                  and entry['published_connectors'] == 'absent', 'actual source gate with frozen absent availability remains skipped')
        finally:
            inv_path.unlink(missing_ok=True)
    print('CI archive source-entry controls passed: 3')


def cloud_source_controls(context):
    fixture.mark_cloud_plan(context)
    request, unused = prepare_cases(context)

    def project():
        return m.project(g.References(context['evidence']), context['work'], request,
                         include_test_observations=True)

    answer = project()
    check(answer['source_case_subset_verified']
          and answer['test_observations']['workers']['rest']['selected_tests'][0]
          ['requires_cloud_release_inputs'] is True,
          'a source-only input marker remains metadata on an executed source case')
    plan = context['artifacts']['ci-shard-plan']['shards.json']
    rows = plan['expected'] + [test for worker in plan['shards'] for test in worker['tests']]
    rows += context['artifacts']['ci-test-results-rest']['rest/manifest.json']['tests']
    for row in rows:
        row.pop('requires_cloud_release_inputs', None)
    context['reseal']()
    request['archive_inputs'] = context['save']('input-request.json', context['request'])
    rejected(project, 'source Cloud input metadata differs from archived plan')


def foreign_cloud_marker_controls(context):
    fixture.mark_cloud_plan(context)
    request, unused = prepare_cases(context)
    rejected(lambda: m.project(g.References(context['evidence']), context['work'], request),
             'source Cloud input metadata differs from archived plan')


if __name__ == '__main__':
    fixture.run(cloud_source_controls, source_case=fixture.CLOUD_SOURCE_CASE,
                source_setup=fixture.cloud_source_setup, only_callback=True)
    fixture.run(foreign_cloud_marker_controls, only_callback=True)
    fixture.run(controls)
    fixture.run(controls, source_case='''package io.tapstate.fixture;
class SampleTest {
 @Test void holds() {}
 @ParameterizedTest @MethodSource("computed") void dynamic(int value) {}
 static Stream<Integer> computed() { return external(); }
}
''')
    fixture.run(two_worker_controls, two_workers=True, only_callback=True)
    fixture.run(unresolved_xml_controls, source_case='''package io.tapstate.fixture;
class SampleTest {
 @Test void holds() {}
 @ParameterizedTest @MethodSource("computed") void dynamic(int value) {}
 static Stream<Integer> computed() { return external(); }
}
''', only_callback=True)
    published_controls()
    source_entry_controls()
    print('CI archive case total controls passed: ' + str(count))
