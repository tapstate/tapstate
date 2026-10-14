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


def controls(f):
    data = f['root'] / 'producer-data'
    data.mkdir()
    inv_path = data / 'inventory.json'
    inv, unused = m.source_inventory(f['work'], f['merge'], g.checksum(fixture.v.git(f['work'], 'archive', '--format=tar', f['merge']).stdout), 'ci', inv_path)
    expected_path = data / 'expected.json'
    with redirect_stdout(io.StringIO()):
        ev.export_hashes(SimpleNamespace(input=str(inv_path), output=str(expected_path)))
    target = data / 'rest'
    target.mkdir()
    for name, raw in f['result_files'].items():
        m.data_file(target, 'files/' + name, raw)
    manifest_path = m.data_file(target, 'manifest.json', fixture.encode(f['manifest']))
    input_path = m.data_file(data, 'inputs.json', fixture.encode(f['pre']))
    binding_path = m.data_file(data, 'binding.json', fixture.encode({'source_sha': f['merge'], 'run_id': 42, 'attempt': 1}))
    gates = data / 'gates'
    gates.mkdir()
    obs_path = data / 'observations.json'
    with redirect_stdout(io.StringIO()):
        ev.reports(SimpleNamespace(root=str(target / 'files'), inventory=str(inv_path), lane='ci', inputs=str(input_path),
                                  binding=str(binding_path), class_gates=str(gates), artifact_manifest=str(manifest_path),
                                  shard_plan=None, shard=None, output=str(obs_path)))
    hashes_path = data / 'observed.json'
    with redirect_stdout(io.StringIO()):
        ev.export_hashes(SimpleNamespace(input=str(obs_path), output=str(hashes_path)))
    artifacts = f['artifacts']
    artifacts['ci-inputs-rest']['expected-case-hashes.json'] = json.loads(expected_path.read_bytes())
    artifacts['ci-inputs-rest']['observed-case-hashes.json'] = json.loads(hashes_path.read_bytes())
    f['reseal']()
    original = copy.deepcopy(artifacts)
    request = {'schema': 1, 'type': 'archive-cases-request', 'archive_inputs': f['save']('input-request.json', f['request'])}

    def reseal():
        f['reseal']()
        request['archive_inputs'] = f['save']('input-request.json', f['request'])

    def reset():
        artifacts.clear()
        artifacts.update(copy.deepcopy(original))
        reseal()

    def project():
        return m.project(g.References(f['evidence']), f['work'], request)

    answer = project()
    if not inv['complete']:
        check(answer['source_case_subset_verified'] and not answer['all_source_cases_statically_parsed'],
              'computed provider stays outside the verified static subset')
        check('computed_provider_identities' in answer['unverified_components']
              and not answer['whole_input_projection_verified'], 'computed provider cannot become complete from matching declarations')
        check(answer['source_inventory']['unresolved'], 'actual computed source gap is retained')
        print('CI archive computed provider controls passed: 3')
        return
    check(answer['source_class_cohort_verified'] and answer['source_case_subset_verified'], 'source and raw XML are independently joined')
    check(answer['all_source_cases_statically_parsed'], 'simple literal source methods resolve completely')
    check(len(answer['source_inventory']['cases']) == 1, 'original source case count is derived')
    check(not answer['whole_input_projection_verified'] and not answer['first_complete_PR_sample']
          and not answer['performance_accepted'], 'case subset readback cannot certify the other input components')
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
    before = output.read_bytes()
    check(subprocess.run(command, capture_output=True).returncode != 0 and output.read_bytes() == before,
          'case output is created exclusively')
    print('CI archive case controls passed: ' + str(count))


def two_worker_controls(f):
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
            {'source_sha': f['merge'], 'run_id': 42, 'attempt': 1}))
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
    answer = m.project(g.References(f['evidence']), f['work'], request)
    check(answer['all_source_cases_statically_parsed'] and len(answer['workers']) == 2,
          'same source and different worker capture metadata are valid')
    check(len({v['source_inventory_sha256'] for v in answer['workers'].values()}) == 2,
          'each original worker inventory binding is retained')
    print('CI archive two-worker controls passed: 3')


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


fixture.run(controls)
fixture.run(controls, source_case='''package io.tapstate.fixture;
class SampleTest {
 @Test void holds() {}
 @ParameterizedTest @MethodSource("computed") void dynamic(int value) {}
 static Stream<Integer> computed() { return external(); }
}
''')
fixture.run(two_worker_controls, two_workers=True, only_callback=True)
published_controls()
source_entry_controls()
print('CI archive case total controls passed: ' + str(count))
