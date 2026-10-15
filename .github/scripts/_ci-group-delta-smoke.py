#!/usr/bin/env python3
"""Archived group deltas retain hard inputs and bind source-derived test partitions."""
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


m = module('group_delta', '_ci-group-delta.py')
fixture = module('delta_fixture', '_ci-case-partition-smoke.py')
g, count = m.g, 0


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


def controls(root, contexts, unused_complete, unused_added):
    def original_event(name, context, case_request):
        if case_request is None:
            scope = context['scope']
            context['event'].update(schema=1, event_payload_complete=True, ref='refs/pull/7/merge', pull_request={
                'number': 7, 'action': 'synchronize', 'head_repository': scope['repository'],
                'base_repository': scope['repository'], 'head_ref': scope['branch'], 'base_ref': 'main',
                'head_sha': scope['head_sha'], 'base_sha': scope['base_sha']})
            context['artifacts']['ci-shard-plan']['ci-event-inputs.json'] = copy.deepcopy(context['event'])
            return
        run = g.load(context['evidence'] / 'run/run.json')
        index = 'ABC'.index(name)
        run.update(status='completed', conclusion='success', path='.github/workflows/ci.yml',
                   created_at='2026-10-14T00:0' + str(index * 2) + ':00Z',
                   updated_at='2026-10-14T00:0' + str(index * 2 + 1) + ':00Z')
        context['save']('run/run.json', run)
        context['source_request']['archive'] = context['seal']()
        context['request']['source'] = context['save']('source-request.json', context['source_request'])
        case_request['archive_inputs'] = context['save']('input-request.json', context['request'])

    partition_request, inventories, unused = fixture.prepare_request(root, contexts, original_event)

    def save(name, value):
        path = root / name
        raw = fixture.fixture.encode(value)
        path.write_bytes(raw)
        return {'path': name, 'sha256': g.checksum(raw)}

    partition_ref = save('partition-request.json', partition_request)
    facts = {name: m.components.project(g.References(root), entry['source_root'],
        {'schema': 1, 'type': 'archived-group-components-request', 'archive_cases': entry['archive_cases']})
        for name, entry in partition_request['variants'].items()}
    payloads = {name: value['payloads'] for name, value in facts.items()}
    entries = []
    for component, field in sorted(m.POLICY_FIELDS):
        values = {name: (value['I']['source_inventory'][field] if component == 'I' else value[component][field])
                  for name, value in payloads.items()}
        observed = m.hashes(values)
        if len(set(observed.values())) != 1:
            entries.append({'component': component, 'field': field, 'hashes': observed,
                            'reason': 'Declared source addition; original assertion semantics remain unverified',
                            'evidence': [partition_ref]})
    policy = {'schema': 1, 'type': 'declared-group-delta-policy', 'entries': entries}
    request = {'schema': 1, 'type': 'archived-group-delta-request', 'partition_request': partition_ref,
               'review_policy': save('policy.json', policy)}
    report = m.project(g.References(root), request)
    check(report['comparison_ready'] and not report['full_component_payloads_equal'],
          'actual source additions reconcile without replacing unequal full fingerprints')
    check(all(len(cases) == 1 for cases in report['case_partition']['additions'].values()),
          'actual archived source and XML additions remain separate')
    check(not report['original_assertions_verified'] and not report['declared_delta_semantics_verified']
          and not report['normalized_original_inputs_verified'] and not report['whole_build_provenance_complete']
          and not report['group_equivalence_proven'] and not report['first_complete_PR_sample']
          and not report['performance_accepted'], 'declared differences cannot become semantic or acceptance proof')
    rows = m.compare_payloads(payloads, report['case_partition'], {})
    check(any(row['status'] == 'needs_review' for row in rows), 'unreviewed source changes remain visible')
    rejected(lambda: m.bind_group(dict(report, comparison_ready=False), {}, {}), 'pending deltas block group reconciliation')
    parsed_policy = m.policy_entries(g.References(root), request['review_policy'])
    for component, field in [('P', 'production'), ('P', 'packaged_outputs'), ('P', 'preserved_source_digest'),
                             ('U', 'owned_recipe_digest'), ('spec', 'toolchain'), ('R', 'original_labels'), ('R', 'ruleset_records')]:
        changed = copy.deepcopy(payloads)
        changed['B'][component][field] = {'different': 'archived fact'}
        rejected(lambda: m.compare_payloads(changed, report['case_partition'], parsed_policy),
                 'hard ' + component + '.' + field)
    changed = copy.deepcopy(payloads)
    changed['B']['D']['resolved_test_classpaths']['fixture'].append({'identity': 'unproven test dependency'})
    check(any(row['status'] == 'needs_review' for row in m.compare_payloads(changed, report['case_partition'], parsed_policy)),
          'new test dependency cannot be declared original-input equality')
    changed = copy.deepcopy(payloads)
    changed['B']['D']['peer_dependency_observations']['build']['dependency_repository_files']['unused.pom'] = 'a' * 64
    check(any(row['status'] == 'side_observation_delta' for row in m.compare_payloads(changed, report['case_partition'], parsed_policy)),
          'unused available cache observations remain explicit side facts')
    for label, mutate in [
        ('missing models', lambda x: x['dependency_coordinate_models'].clear()),
        ('missing producers', lambda x: x['dependency_producer_facts'].clear()),
        ('unverified coordinate', lambda x: x['dependency_coordinate_models']['example/lib/1/lib-1.pom'].update(coordinate_type_verified=False)),
        ('unverified producer', lambda x: x['dependency_producer_facts']['example/lib/1/lib-1.jar'].update(producer_identity_verified=False)),
        ('unbound model bytes', lambda x: x['dependency_coordinate_models']['example/lib/1/lib-1.pom'].update(sha256='f' * 64)),
    ]:
        changed = copy.deepcopy(payloads)
        for value in changed.values():
            for peer in value['D']['peer_dependency_observations'].values():
                mutate(peer)
        check(any(row['status'] == 'unknown' for row in m.compare_payloads(changed, report['case_partition'], parsed_policy)),
              label + ' remains unknown even with equal resolved classpath bytes')
    changed = copy.deepcopy(payloads)
    changed['B']['D']['peer_dependency_observations']['build']['dependency_coordinate_models']['example/lib/1/lib-1.pom']['packaging'] = 'different'
    check(any(row['field'] == 'resolved_dependency_identities' and row['status'] == 'needs_review'
              for row in m.compare_payloads(changed, report['case_partition'], parsed_policy)),
          'changed metadata for a used dependency is not an unused-cache difference')
    reactor = copy.deepcopy(payloads['A'])
    identity = 'repository/io/tapstate/fixture/1/fixture-1-tests.jar'
    reactor['D']['resolved_test_classpaths']['fixture'] = [{'identity': identity, 'sha256': 'b' * 64, 'size': 10, 'own_classes': {}}]
    reactor['P']['packaged_outputs'] = {'fixture/target/fixture-1-tests.jar': {'sha256': 'b' * 64, 'size': 10, 'mode': 420}}
    check(not m.resolved_dependency_facts(reactor)['unknown'], 'reactor test-JAR binds its unique recorded packaged output')
    for label, mutate in [
        ('missing reactor output', lambda x: x['P']['packaged_outputs'].clear()),
        ('different reactor bytes', lambda x: x['P']['packaged_outputs']['fixture/target/fixture-1-tests.jar'].update(sha256='f' * 64)),
        ('ambiguous reactor producer', lambda x: x['P']['packaged_outputs'].update({'other/target/fixture-1-tests.jar': {'sha256': 'b' * 64, 'size': 10, 'mode': 420}})),
        ('conflicting module bindings', lambda x: x['D']['resolved_test_classpaths'].update({'other': [{'identity': identity, 'sha256': 'b' * 64, 'size': 10, 'own_classes': {'io/tapstate/fixture/Unexpected': {'class_id': 'a' * 16, 'sha256': 'a' * 64}}}]})),
    ]:
        changed = copy.deepcopy(reactor); mutate(changed)
        check(m.resolved_dependency_facts(changed)['unknown'], label + ' remains unknown')
    unknown = dict(report['case_partition'], original_case_partition_verified=False)
    check(any(row['status'] == 'unknown' for row in m.compare_payloads(payloads, unknown, parsed_policy)),
          'unknown original identities never become a complete partition')
    changed_policy = copy.deepcopy(policy)
    changed_policy['entries'][0]['hashes']['B'] = 'f' * 64
    stale = m.policy_entries(g.References(root), save('stale-policy.json', changed_policy))
    rejected(lambda: m.compare_payloads(payloads, report['case_partition'], stale), 'stale declared delta')
    changed_policy = copy.deepcopy(policy)
    changed_policy['entries'][0]['component'] = 'P'
    rejected(lambda: m.policy_entries(g.References(root), save('hard-policy.json', changed_policy)), 'policy waives a hard input')
    changed_policy = copy.deepcopy(policy)
    changed_policy['entries'].append(dict(changed_policy['entries'][0]))
    rejected(lambda: m.policy_entries(g.References(root), save('duplicate-policy.json', changed_policy)), 'duplicate policy entry')
    variants = {name: {'component_mode': 'archive-replayed', 'binding': value['binding'],
        'components': value['component_hashes'], 'identity': {key: value['scope'][key] for key in ('head_sha', 'base_sha', 'run_id')}}
        for name, value in facts.items()}
    scope = {key: facts['A']['scope'][key] for key in ('repository', 'pr', 'branch')}
    m.bind_group(report, scope, variants)
    check(True, 'group hook binds full hashes and original event identities')
    for label, mutate in [('foreign event', lambda x: x['B']['identity'].update(base_sha='f' * 40)),
                          ('foreign fingerprint', lambda x: x['C']['components'].update(I='f' * 64)),
                          ('declared capture', lambda x: x['A'].update(component_mode='declared'))]:
        changed = copy.deepcopy(variants); mutate(changed)
        rejected(lambda: m.bind_group(report, scope, changed), label)
    for candidate in ('B', 'C'):
        comparison = {'inputs': {'inventory': save('inventory-' + candidate + '.json', inventories['A']),
            'candidate_inventory': save('candidate-' + candidate + '.json', inventories[candidate]),
            'additions': save('additions-' + candidate + '.json', report['case_partition']['additions'][candidate])}}
        m.bind_test_inputs(g.References(root), comparison, report, candidate)
        check(True, 'comparison source/additions bound for ' + candidate)
        changed = copy.deepcopy(comparison)
        changed['inputs']['additions'] = save('missing-additions-' + candidate + '.json', [])
        rejected(lambda: m.bind_test_inputs(g.References(root), changed, report, candidate), 'omitted additions ' + candidate)
        changed = copy.deepcopy(comparison)
        incomplete = dict(inventories['A'], cases=[])
        changed['inputs']['inventory'] = save('omitted-original-' + candidate + '.json', incomplete)
        rejected(lambda: m.bind_test_inputs(g.References(root), changed, report, candidate), 'caller omitted original ' + candidate)
    group = {'schema': 1, 'type': 'pr-group-inputs', 'scope': scope, 'cache_condition': 'warm',
             'order': ['A', 'B', 'C'], 'variants': {}, 'comparisons': [],
             'normalization': {key: request[key] for key in ('partition_request', 'review_policy')}}
    observations = {}
    for name, entry in partition_request['variants'].items():
        component_request = save('group-components-' + name + '.json',
            {'schema': 1, 'type': 'archived-group-components-request', 'archive_cases': entry['archive_cases']})
        typed = {field: save('group-' + name + '-' + field + '.json', {
            'schema': 1, 'type': 'archived-group-component', 'name': field, 'binding': facts[name]['binding'],
            'payload': payloads[name][field], 'request': component_request, 'source_root': entry['source_root'],
            'normalized_original_inputs_verified': False}) for field in sorted(g.COMPONENTS)}
        case_request, unused = g.References(root).read(entry['archive_cases'])
        input_request, unused = g.References(root).read(case_request['archive_inputs'])
        source_request, unused = g.References(root).read(input_request['source'])
        group['variants'][name] = {'head_sha': facts[name]['scope']['head_sha'],
                                  'archive': source_request['archive'], 'components': typed}
        observations[name] = {}
        for filename, label in [('inventory.json', 'inventory'), ('observations.json', 'tests')]:
            raw = (contexts[name]['root'] / 'producer-data' / filename).read_bytes()
            path = root / ('group-' + name + '-' + filename); path.write_bytes(raw)
            observations[name][label] = {'path': path.name, 'sha256': g.checksum(raw)}
        binding = facts[name]['binding']
        observations[name]['coverage'] = save('group-' + name + '-coverage.json', {
            'schema': 1, 'type': 'coverage-observation', 'complete': True, 'binding': binding,
            'production': {'fixture/target/classes/io/tapstate/fixture/Product': {'class_id': 'a' * 16, 'sha256': 'a' * 64}},
            'covered_lines_branches': [['fixture', 'io/tapstate/fixture', 'Product.java', 1, 2, 1]],
            'executions': {'fixture/target/jacoco.exec': {'classes': [{'name': 'io/tapstate/fixture/Product',
                'class_id': 'a' * 16, 'probes': 2, 'covered_probes': [0, 1]}]}}})
        observations[name]['gates'] = save('group-' + name + '-gates.json', {
            'schema': 1, 'input_binding': binding, 'gates': [{'id': 'ci:build', 'required': True,
                'applicable': True, 'scope': 'always', 'contract_sha256': '1' * 64, 'conclusion': 'success'}]})
    for candidate in ('B', 'C'):
        for kind in ('tests', 'coverage', 'gates'):
            inputs = {'before': observations['A'][kind], 'after': observations[candidate][kind]}
            if kind == 'tests':
                inputs.update(inventory=observations['A']['inventory'], candidate_inventory=observations[candidate]['inventory'],
                              additions=save('group-additions-' + candidate + '.json', report['case_partition']['additions'][candidate]))
            group['comparisons'].append({'before': 'A', 'after': candidate, 'lane': 'ci',
                'request': save('group-compare-' + candidate + '-' + kind + '.json', {'schema': 1, 'kind': kind, 'inputs': inputs})})
    reconciled = g.group_inputs(g.References(root), group)
    check(not reconciled['declared_component_payloads_equal'] and len(reconciled['comparisons_replayed']) == 6,
          'actual group consumer replays unequal archived components and all existing comparators')
    check(not reconciled['group_equivalence_proven'] and reconciled['archived_group_delta']['comparison_ready'],
          'integrated group reconciliation remains bounded protocol evidence')
    manifest = root / 'delta-request.json'; manifest.write_bytes(fixture.fixture.encode(request))
    output = root / 'delta-cli.json'
    command = ['bash', str(Path(__file__).with_name('ci-group-delta.sh')), '--manifest', str(manifest),
               '--evidence-root', str(root), '--output', str(output)]
    result = subprocess.run(command, capture_output=True, text=True)
    check(result.returncode == 0, 'actual delta CLI succeeds: ' + result.stderr)
    check(json.loads(output.read_bytes()) == report, 'CLI replays original facts and retains full fingerprints')


dependency_metadata = {
    'dependency_repository_files': {'example/lib/1/lib-1.jar': 'c' * 64, 'example/lib/1/lib-1.pom': 'd' * 64},
    'dependency_coordinate_models': {'example/lib/1/lib-1.pom': {
        'gav': ['example', 'lib', '1'], 'packaging': 'jar', 'sha256': 'd' * 64, 'parent': None,
        'coordinate_type_verified': True, 'effective_dependency_model_proven': False}},
    'dependency_producer_facts': {'example/lib/1/lib-1.jar': {
        'gav': ['example', 'lib', '1'], 'sha256': 'c' * 64, 'pom_path': 'example/lib/1/lib-1.pom', 'pom_sha256': 'd' * 64,
        'archive_type_verified': True, 'producer_identity_verified': True,
        'producer_identity_source': 'synthetic-protocol-fixture', 'runtime_realm_proven': False}}}
fixture.cohort(added=True, callback=controls, dependency_metadata=dependency_metadata)
print('CI archived group delta controls passed:', count)
