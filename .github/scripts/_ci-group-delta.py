#!/usr/bin/env python3
"""Reconcile archived variant differences without replacing their full fingerprints."""
import argparse
import importlib.util
from pathlib import Path
import re
import sys

sys.dont_write_bytecode = True
spec = importlib.util.spec_from_file_location('delta_partition', Path(__file__).with_name('_ci-case-partition.py'))
partition = importlib.util.module_from_spec(spec)
spec.loader.exec_module(partition)
g, require = partition.g, partition.require
components = g.module('delta_components', '_ci-group-components.py')
test_jars = g.module('delta_test_jars', '_ci-test-jar-inputs.py')
maven_artifacts = g.module('delta_maven_artifacts', '_ci-maven-artifacts.py')
POLICY_FIELDS = {('I', field) for field in ('methods', 'class_contracts', 'sources', 'entry_contract', 'test_packaged_outputs')} | {('R', 'workflows')}


def hashes(values):
    return {name: g.checksum(g.canonical(value)) for name, value in values.items()}


def policy_entries(refs, reference):
    value, unused = refs.read(reference)
    require(isinstance(value, dict) and set(value) == {'schema', 'type', 'entries'}
            and type(value['schema']) is int and value['schema'] == 1
            and value['type'] == 'declared-group-delta-policy' and isinstance(value['entries'], list),
            'invalid group delta policy')
    entries = {}
    for entry in value['entries']:
        require(isinstance(entry, dict) and set(entry) == {'component', 'field', 'hashes', 'reason', 'evidence'},
                'invalid delta policy entry')
        key = (entry['component'], entry['field'])
        require(key in POLICY_FIELDS and key not in entries, 'unsupported or duplicate delta policy field')
        require(isinstance(entry['hashes'], dict) and set(entry['hashes']) == {'A', 'B', 'C'}
                and all(isinstance(v, str) and re.fullmatch(r'[0-9a-f]{64}', v) for v in entry['hashes'].values()),
                'invalid delta policy hashes')
        require(isinstance(entry['reason'], str) and entry['reason'].strip() and len(entry['reason']) <= 4096
                and isinstance(entry['evidence'], list) and entry['evidence'], 'delta policy rationale or evidence missing')
        for reference in entry['evidence']:
            refs.blob(reference)
        entries[key] = entry
    return entries


def original_resolved_identities(snapshot, consumers, entry, gav, kind):
    packet, native = snapshot.get('resolved_maven_artifacts'), []
    if packet is None:
        return None
    reactor = packet.get('reactor')
    if not isinstance(reactor, dict) or reactor.get('unresolved') or maven_artifacts.is_reactor(reactor, gav):
        return None
    for consumer in sorted(consumers):
        observed = maven_artifacts.consumer_identity(packet, consumer, entry['identity'])
        artifact = observed.get('artifact') if observed is not None else None
        if not (isinstance(artifact, dict) and artifact.get('gav') == gav
                and artifact.get('type') == kind and artifact.get('classifier') is None
                and artifact.get('sha256') == entry['sha256'] and artifact.get('size') == entry['size']
                and isinstance(artifact.get('scopes'), list) and artifact['scopes']
                and all(scope in maven_artifacts.SCOPES for scope in artifact['scopes'])):
            return None
        native.append({'consumer': consumer, **observed})
    return native or None


def resolved_dependency_facts(payload, test_outputs=None):
    dependencies, outputs = payload['D'], payload['P']['packaged_outputs']
    used, facts, gaps, consumers = {}, {}, [], {}
    for consumer, entries in dependencies['resolved_test_classpaths'].items():
        for entry in entries:
            identity = entry['identity']
            if identity.startswith('repository/'):
                if identity in used and g.canonical(used[identity]) != g.canonical(entry):
                    gaps.append(identity + ': conflicting module classpath bindings')
                used[identity] = entry
                consumers.setdefault(identity, set()).add(consumer)
    has_native = any('resolved_maven_artifacts' in peer for peer in dependencies['peer_dependency_observations'].values())
    for identity, entry in sorted(used.items()):
        path = identity.removeprefix('repository/')
        kind = 'pom' if path.endswith('.pom') else 'jar' if path.endswith('.jar') else None
        if kind is None or kind == 'pom' and not has_native:
            gaps.append(identity + ': unsupported repository classpath entry')
            continue
        role = test_jars.classpath_role(entry, test_outputs or {})
        if role is not None:
            record = test_outputs[role]
            facts[identity] = {'recorded_test_fixture_output': {'path': role, 'model': record['model'],
                'metadata': record['metadata'], 'mode': record['output']['mode'],
                'producer_runtime_inputs': record['producer_runtime_inputs']}}
            continue
        first_party = path.startswith('io/tapstate/')
        if first_party:
            matches = {name: value for name, value in outputs.items()
                       if Path(name).name == Path(path).name and value['sha256'] == entry['sha256']
                       and value['size'] == entry['size']}
            gav = maven_artifacts.repository_coordinates(path)
            declared = (gav is not None and all(isinstance(value.get('resolved_maven_artifacts'), dict)
                and maven_artifacts.is_reactor(value['resolved_maven_artifacts']['reactor'], gav)
                for value in dependencies['peer_dependency_observations'].values())) if has_native else True
            if len(matches) == 1 and declared:
                facts[identity] = {'recorded_reactor_output': matches}
                continue
            if not has_native:
                gaps.append(identity + ': unique recorded reactor output missing')
                continue
        peers = []
        for peer, snapshot in sorted(dependencies['peer_dependency_observations'].items()):
            producer = snapshot['dependency_producer_facts'].get(path) if kind == 'jar' else None
            pom_path = producer.get('pom_path') if isinstance(producer, dict) else path if kind == 'pom' else None
            model = snapshot['dependency_coordinate_models'].get(pom_path)
            available = snapshot['dependency_repository_files']
            model_bound = (isinstance(model, dict) and model.get('coordinate_type_verified') is True
                and isinstance(model.get('gav'), list) and len(model['gav']) == 3
                and all(isinstance(part, str) and re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.+-]*', part) for part in model['gav'])
                and pom_path == '/'.join([model['gav'][0].replace('.', '/'), model['gav'][1], model['gav'][2],
                                         model['gav'][1] + '-' + model['gav'][2] + '.pom'])
                and available.get(pom_path) == model.get('sha256')
                and isinstance(model.get('sha256'), str) and re.fullmatch(r'[0-9a-f]{64}', model['sha256']))
            parent = model.get('parent') if isinstance(model, dict) else None
            parent_bound = parent is None or (isinstance(parent, dict) and isinstance(parent.get('gav'), list)
                and len(parent['gav']) == 3 and all(isinstance(v, str) and re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.+-]*', v) for v in parent['gav'])
                and parent.get('path') == '/'.join([parent['gav'][0].replace('.', '/'), parent['gav'][1], parent['gav'][2],
                    parent['gav'][1] + '-' + parent['gav'][2] + '.pom'])
                and isinstance(parent.get('sha256'), str) and re.fullmatch(r'[0-9a-f]{64}', parent['sha256'])
                and available.get(parent['path']) == parent['sha256'])
            model_bound = model_bound and parent_bound
            if kind == 'pom':
                verified = model_bound and model.get('packaging') == 'pom' and model['sha256'] == entry['sha256']
            else:
                verified = (model_bound and isinstance(producer, dict) and producer.get('archive_type_verified') is True
                    and producer.get('gav') == model['gav'] and available.get(path) == producer.get('sha256') == entry['sha256']
                    and producer.get('pom_sha256') == model['sha256'])
            native = None
            if verified and (has_native or kind == 'pom' or first_party or producer.get('producer_identity_verified') is not True):
                native = original_resolved_identities(snapshot, consumers[identity], entry, model['gav'], kind)
                verified = native is not None
            if not verified:
                reason = ('coordinate/producer or original resolved identity missing or unverified'
                          if 'resolved_maven_artifacts' in snapshot else 'coordinate/producer identity missing or unverified')
                gaps.append(identity + ': ' + peer + ' ' + reason)
            fact = {'producer': producer, 'model': model}
            if verified and native:
                fact['original_maven_resolved_identities'] = native
            peers.append(fact)
        if not peers:
            gaps.append(identity + ': dependency peers missing')
        facts[identity] = sorted({g.canonical(value) for value in peers})
        facts[identity] = [g.json_bytes(value) for value in facts[identity]]
    return {'resolved': facts, 'unknown': gaps}


def test_output_projection(payload):
    """This receives independently replayed component facts, never caller ignore paths."""
    fact = payload['I'].get('test_packaged_outputs')
    if fact is None or fact.get('original_owner_bytes_replayed') is not True or fact.get('unverified'):
        return None
    require(isinstance(fact.get('outputs'), dict), 'replayed test-JAR outputs unavailable')
    records = fact['outputs']
    for path, record in records.items():
        require(payload['P']['packaged_outputs'].get(path) == record.get('output')
                and isinstance(record.get('producer_runtime_inputs'), dict), 'replayed test-JAR output/runtime binding differs')
    production = {name: value for name, value in payload['P']['packaged_outputs'].items() if name not in records}
    metadata = {name: {'model': value['model'], 'metadata': value['metadata'], 'mode': value['output']['mode'],
                       'producer_runtime_inputs': value['producer_runtime_inputs']} for name, value in records.items()}
    classpaths = {}
    for consumer, entries in payload['D']['resolved_test_classpaths'].items():
        classpaths[consumer] = []
        for entry in entries:
            role = test_jars.classpath_role(entry, records)
            classpaths[consumer].append(entry if role is None else
                {'identity': entry['identity'], 'replayed_test_fixture_output': role, 'producer': metadata[role]})
    return {'outputs': records, 'production': production, 'metadata': metadata, 'classpaths': classpaths}


def compare_payloads(payloads, case_partition, policy):
    """Hard constraints cannot be waived by a declared source-delta policy."""
    rows, used = [], set()

    def record(component, field, values, unequal='reject'):
        observed = hashes(values)
        status = 'equal' if len(set(observed.values())) == 1 else unequal
        key = (component, field)
        row = {'component': component, 'field': field, 'hashes': observed, 'status': status}
        if status == 'needs_review' and key in policy:
            entry = policy[key]
            require(entry['hashes'] == observed, 'delta policy values differ from archived facts')
            row.update(status='declared_expected_delta', reason=entry['reason'], evidence=entry['evidence'])
            used.add(key)
        require(status != 'reject', 'hard group input differs: ' + component + '.' + field)
        rows.append(row)

    projections = {name: test_output_projection(value) for name, value in payloads.items()}
    replayed_test_inputs = all(value is not None for value in projections.values())
    for field in ('preserved_source_digest', 'production'):
        record('P', field, {name: value['P'][field] for name, value in payloads.items()})
    if replayed_test_inputs:
        record('P', 'production_packaged_outputs', {name: value['production'] for name, value in projections.items()})
        record('P', 'test_producer_metadata', {name: value['metadata'] for name, value in projections.items()})
        record('P', 'packaged_outputs', {name: value['P']['packaged_outputs'] for name, value in payloads.items()},
               'replayed_test_input_partition')
        record('I', 'test_packaged_outputs', {name: value['I']['test_packaged_outputs'] for name, value in payloads.items()},
               'needs_review')
    else:
        record('P', 'packaged_outputs', {name: value['P']['packaged_outputs'] for name, value in payloads.items()})
        if any('test_packaged_outputs' in value['I'] for value in payloads.values()):
            rows.append({'component': 'I', 'field': 'test_packaged_outputs', 'status': 'unknown',
                         'hashes': hashes({name: value['I'].get('test_packaged_outputs') for name, value in payloads.items()})})
    record('U', 'owned_recipe_digest', {name: value['U']['owned_recipe_digest'] for name, value in payloads.items()})
    record('spec', 'observed', {name: value['spec'] for name, value in payloads.items()})
    normalized = {name: value['classpaths'] for name, value in projections.items()} if replayed_test_inputs else None
    if normalized is not None:
        record('D', 'resolved_production_classpaths', normalized, 'needs_review')
    raw_status = ('replayed_test_input_partition' if normalized is not None and len(set(hashes(normalized).values())) == 1
                  else 'needs_review')
    record('D', 'resolved_test_classpaths', {name: value['D']['resolved_test_classpaths'] for name, value in payloads.items()}, raw_status)
    identities = {name: resolved_dependency_facts(value, projections[name]['outputs'] if replayed_test_inputs else None)
                  for name, value in payloads.items()}
    if any(value['unknown'] for value in identities.values()):
        rows.append({'component': 'D', 'field': 'resolved_dependency_identities', 'status': 'unknown',
                     'hashes': hashes(identities), 'facts': identities})
    else:
        record('D', 'resolved_dependency_identities', identities, 'needs_review')
    record('D', 'peer_dependency_observations', {name: value['D']['peer_dependency_observations'] for name, value in payloads.items()}, 'side_observation_delta')
    for field in ('original_labels', 'ruleset_records'):
        record('R', field, {name: value['R'][field] for name, value in payloads.items()})
    record('R', 'workflows', {name: value['R']['workflows'] for name, value in payloads.items()}, 'needs_review')
    inventories = {name: value['I']['source_inventory'] for name, value in payloads.items()}
    for field in ('methods', 'class_contracts', 'sources', 'entry_contract'):
        record('I', field, {name: value[field] for name, value in inventories.items()}, 'needs_review')
    rows.append({'component': 'I', 'field': 'cases', 'status': 'source_partition' if case_partition['original_case_partition_verified'] else 'unknown',
                 'hashes': hashes({name: value['cases'] for name, value in inventories.items()})})
    require(set(policy) == used, 'unused delta policy entry')
    return rows


def project(refs, request):
    require(isinstance(request, dict) and set(request) == {'schema', 'type', 'partition_request', 'review_policy'}
            and type(request['schema']) is int and request['schema'] == 1
            and request['type'] == 'archived-group-delta-request', 'invalid group delta request')
    partition_request, unused = refs.read(request['partition_request'])
    case_partition = partition.project(refs, partition_request)
    facts = {}
    for name, entry in partition_request['variants'].items():
        facts[name] = components.project(refs, entry['source_root'],
            {'schema': 1, 'type': 'archived-group-components-request', 'archive_cases': entry['archive_cases']})
        require(facts[name]['scope'] == case_partition['variant_bindings'][name]['scope']
                and g.checksum(g.canonical(facts[name]['payloads']['I']['source_inventory']))
                    == case_partition['variant_bindings'][name]['source_inventory_digest'], 'delta component/partition source differs')
    rows = compare_payloads({name: value['payloads'] for name, value in facts.items()}, case_partition,
                            policy_entries(refs, request['review_policy']))
    ready = all(row['status'] in ('equal', 'source_partition', 'declared_expected_delta', 'side_observation_delta', 'replayed_test_input_partition') for row in rows)
    return {'schema': 1, 'type': 'archived-group-delta-report', 'fields': rows,
            'variant_bindings': {name: {'scope': value['scope'], 'binding': value['binding'],
                'component_hashes': value['component_hashes']} for name, value in facts.items()},
            'source_inventories': {name: value['payloads']['I']['source_inventory'] for name, value in facts.items()},
            'case_partition': case_partition, 'comparison_ready': ready,
            'full_component_payloads_equal': all(value['component_hashes'] == facts['A']['component_hashes'] for value in facts.values()),
            'declared_delta_semantics_verified': False, 'original_assertions_verified': False,
            'normalized_original_inputs_verified': False, 'whole_build_provenance_complete': False,
            'group_equivalence_proven': False, 'first_complete_PR_sample': False, 'performance_accepted': False,
            'input_files': refs.finish()}


def bind_group(report, scope, variants):
    require(report['comparison_ready'], 'group delta still has unknown or unreviewed fields')
    for name, variant in variants.items():
        derived = report['variant_bindings'][name]
        require(variant.get('component_mode') == 'archive-replayed'
                and variant['binding'] == derived['binding'] and variant['components'] == derived['component_hashes'],
                'group delta belongs to another component capture')
        identity = variant['identity']
        require(all(derived['scope'][key] == scope[key] for key in ('repository', 'pr', 'branch'))
                and all(derived['scope'][key] == identity[key] for key in ('head_sha', 'base_sha', 'run_id')),
                'group delta belongs to another original event')


def bind_test_inputs(refs, request, report, candidate):
    for label, name in [('inventory', 'A'), ('candidate_inventory', candidate)]:
        value, unused = refs.read(request['inputs'][label])
        expected = report['source_inventories'][name]
        require(all(g.canonical(value.get(field)) == g.canonical(expected[field])
                    for field in ('methods', 'cases', 'class_contracts', 'sources', 'unresolved')),
                'comparison inventory differs from archived source')
    additions, unused = refs.read(request['inputs']['additions'])
    require(g.canonical(additions) == g.canonical(report['case_partition']['additions'][candidate]),
            'comparison additions differ from archived source partition')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for field in ('manifest', 'evidence-root', 'output'):
        parser.add_argument('--' + field, required=True)
    args = parser.parse_args()
    manifest = Path(args.manifest)
    raw = g.file_bytes(manifest)
    refs = g.References(args.evidence_root)
    result = project(refs, g.json_bytes(raw))
    require(g.file_bytes(manifest) == raw and refs.finish() == result['input_files'], 'group delta inputs changed during replay')
    g.write_new(Path(args.output), result)
    print('archived full differences retained; comparison_ready=' + str(result['comparison_ready']).lower()
          + '; source-delta declarations do not prove semantics or acceptance')


if __name__ == '__main__':
    try:
        main()
    except (ValueError, OSError, KeyError, TypeError) as error:
        print('group delta rejected: ' + str(error), file=sys.stderr)
        sys.exit(1)
