#!/usr/bin/env python3
"""Replay two original manual mechanism archives without claiming PR acceptance."""
import argparse
import importlib.util
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace

sys.dont_write_bytecode = True
spec = importlib.util.spec_from_file_location('mechanism_delta', Path(__file__).with_name('_ci-group-delta.py'))
delta = importlib.util.module_from_spec(spec)
spec.loader.exec_module(delta)
g, require, partition = delta.g, delta.require, delta.partition
WORKERS = {'e2e-1', 'e2e-2', 'engine', 'rest'}
READY = {'equal', 'source_partition', 'declared_expected_delta', 'side_observation_delta', 'replayed_test_input_partition'}


def fixed_control_plan(root, plan, checkout):
    if {worker['id'] for worker in plan['shards']} != WORKERS or len(plan['shards']) != 4:
        return False
    if 'layout' not in plan or 'layout_source' not in plan:
        return False
    source = plan['layout_source']
    path = partition.reader.s.LAYOUT_PATH
    require(isinstance(source, dict) and set(source) == {'path', 'sha256', 'legacy_defaults'}
            and source['path'] == path and source['legacy_defaults'] is False, 'mechanism layout source missing')
    raw = partition.reader.inputs.source.variants.git(root, 'show', checkout + ':' + path).stdout
    require(g.checksum(raw) == source['sha256'] and g.json_bytes(raw) == plan['layout'],
            'mechanism layout differs from original source')
    with tempfile.TemporaryDirectory(prefix='ci-mechanism-layout-') as temporary:
        data = Path(temporary).resolve()
        partition.reader.data_file(data, path, raw)
        layout, bound = partition.reader.s.layout_config(SimpleNamespace(), data)
    require(layout == plan['layout'] and bound == source, 'mechanism layout declaration differs')
    partition.reader.s.matrix(plan)
    return plan['layout']['e2e_shards'] == 2 and not plan['layout']['rest_modules'] and 'app_shards' not in plan['layout']


def project(refs, request):
    require(isinstance(request, dict) and set(request) == {'schema', 'type', 'variants', 'review_policy', 'order', 'cache_condition'}
            and type(request['schema']) is int and request['schema'] == 1
            and request['type'] == 'archived-mechanism-inputs-request'
            and isinstance(request['variants'], dict) and set(request['variants']) == {'A', 'B'}
            and request['order'] in (['A', 'B'], ['B', 'A'])
            and request['cache_condition'] in ('warm', 'cold'), 'invalid mechanism inputs request')
    cases, sources, facts, runs, plans, native, resources, fixed_plans = {}, {}, {}, {}, {}, {}, {}, {}
    for name, entry in request['variants'].items():
        require(isinstance(entry, dict) and set(entry) == {'archive_cases', 'source_root'}
                and isinstance(entry['source_root'], str), 'invalid mechanism variant reference')
        case_request, unused = refs.read(entry['archive_cases'])
        cases[name] = partition.reader.project(refs, entry['source_root'], case_request)
        scope = cases[name]['scope']
        require(scope['variant'] == name and scope['event'] == 'workflow_dispatch' and scope['run_attempt'] == 1,
                'mechanism variant/event/attempt differs')
        input_request, unused = refs.read(case_request['archive_inputs'])
        source_request, unused = refs.read(input_request['source'])
        sources[name] = partition.reader.inputs.source.project(refs, entry['source_root'], source_request)
        archive = partition.reader.inputs.Archive(refs, source_request['archive'], scope)
        run = archive.json('run.json')
        require(archive.receipt['archive_complete'] is True and run.get('status') == 'completed'
                and run.get('conclusion') == 'success' and run.get('path') == '.github/workflows/ci.yml',
                'mechanism original workflow is incomplete or unsuccessful')
        require(g.timestamp(run['created_at']) <= g.timestamp(run['updated_at']), 'invalid mechanism run timeline')
        runs[name] = run
        plans[name] = g.json_bytes(archive.artifact('ci-shard-plan')['shards.json'])
        fixed_plans[name] = fixed_control_plan(entry['source_root'], plans[name], cases[name]['checkout']['sha'])
        facts[name] = delta.components.project(refs, entry['source_root'], {
            'schema': 1, 'type': 'archived-group-components-request', 'archive_cases': entry['archive_cases']})
        require(facts[name]['scope'] == scope and g.checksum(g.canonical(facts[name]['payloads']['I']['source_inventory']))
                == cases[name]['source_inventory_digest'], 'mechanism component/partition source differs')
        peers = facts[name]['payloads']['D']['peer_dependency_observations']
        native[name] = all(isinstance(value.get('resolved_maven_artifacts'), dict) for value in peers.values())
        resources[name] = isinstance(facts[name]['payloads']['I'].get('test_resource_files'), dict)
    scopes = [value['scope'] for value in cases.values()]
    require(len({(value['repository'], value['branch']) for value in scopes}) == 1,
            'mechanism repository/branch differs')
    require(len({value['head_sha'] for value in scopes}) == len({value['run_id'] for value in scopes}) == 2,
            'mechanism head or run is reused')
    require(len({value['preserved_source_digest'] for value in sources.values()}) == 1
            and len({value['owned_recipe_digest'] for value in sources.values()}) == 1,
            'mechanism preserved source or recipe differs')
    first, last = request['order']
    require(g.timestamp(runs[first]['updated_at']) <= g.timestamp(runs[last]['created_at']),
            'mechanism run order differs or executions overlap')
    inventories = {name: value['source_inventory'] for name, value in cases.items()}
    complete = all(value.get('all_source_cases_resolved', value['all_source_cases_statically_parsed'])
                   and not inventories[name]['unresolved'] for name, value in cases.items())
    case_partition = partition.mechanism_partition(inventories) if complete else {'original_cases': None, 'additions': None}
    case_partition['original_case_partition_verified'] = complete
    rows = delta.compare_payloads({name: value['payloads'] for name, value in facts.items()}, case_partition,
                                 delta.mechanism_policy_entries(refs, request['review_policy']))
    fixed = all(fixed_plans.values())
    return {'schema': 1, 'type': 'archived-mechanism-inputs', 'order': request['order'],
            'variant_bindings': {name: {'scope': value['scope'], 'binding': value['binding'],
                'component_hashes': value['component_hashes']} for name, value in facts.items()},
            'fields': rows, 'case_partition': case_partition,
            'source_inventories': inventories, 'fixed_four_workers_verified': fixed,
            'current_native_dependency_packets_present': native,
            'prepared_test_resources_present': resources,
            'comparison_ready': complete and fixed and all(native.values()) and all(resources.values())
                and all(row['status'] in READY for row in rows),
            'cache_condition_claim': request['cache_condition'], 'cache_condition_verified': False,
            'declared_delta_semantics_verified': False, 'original_assertions_verified': False,
            'normalized_original_inputs_verified': False, 'whole_build_provenance_complete': False,
            'group_equivalence_proven': False, 'first_complete_PR_sample': False,
            'performance_accepted': False, 'latest_tip_verified': False, 'input_files': refs.finish()}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('manifest', 'evidence-root', 'output'):
        parser.add_argument('--' + name, required=True)
    args = parser.parse_args()
    manifest = Path(args.manifest)
    raw = g.file_bytes(manifest)
    refs = g.References(args.evidence_root)
    result = project(refs, g.json_bytes(raw))
    require(g.file_bytes(manifest) == raw and refs.finish() == result['input_files'], 'mechanism inputs changed during replay')
    g.write_new(Path(args.output), result)
    print('manual A/B input facts replayed; PR, cache conditions and performance acceptance remain unproven')


if __name__ == '__main__':
    try:
        main()
    except (ValueError, OSError, KeyError, TypeError) as error:
        print('mechanism inputs rejected: ' + str(error), file=sys.stderr)
        sys.exit(1)
