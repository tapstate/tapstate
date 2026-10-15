#!/usr/bin/env python3
"""Derive original and added case identities from independently replayed archives."""
import argparse
from collections import Counter
import importlib.util
from pathlib import Path
import sys

sys.dont_write_bytecode = True
spec = importlib.util.spec_from_file_location('partition_cases', Path(__file__).with_name('_ci-archive-cases.py'))
reader = importlib.util.module_from_spec(spec)
spec.loader.exec_module(reader)
g, require, ev = reader.g, reader.require, reader.ev


def _partition(inventories, variants):
    require(set(inventories) == set(variants), 'case partition variant set differs')
    original = Counter(ev.identity(case) for case in inventories['A']['cases'])
    require(original and all(count == 1 for count in original.values()), 'empty or duplicate original source cases')
    original_methods = {ev.method_key(row) for row in inventories['A']['methods']}
    result = {'original_cases': inventories['A']['cases'], 'additions': {}}
    for name in variants[1:]:
        candidate = Counter(ev.identity(case) for case in inventories[name]['cases'])
        require(all(count == 1 for count in candidate.values()), 'duplicate candidate source cases')
        require(not original - candidate, 'candidate source omits original cases')
        require(original_methods <= {ev.method_key(row) for row in inventories[name]['methods']},
                'candidate source omits original methods')
        added = candidate - original
        result['additions'][name] = [case for case in inventories[name]['cases'] if ev.identity(case) in added]
        require(candidate == original + Counter(ev.identity(case) for case in result['additions'][name]),
                'source partition is incomplete')
    return result


def partition(inventories):
    require(set(inventories) == {'A', 'B', 'C'}, 'case partition requires A/B/C')
    return _partition(inventories, ('A', 'B', 'C'))


def mechanism_partition(inventories):
    require(set(inventories) == {'A', 'B'}, 'mechanism case partition requires A/B')
    return _partition(inventories, ('A', 'B'))


def project(refs, request):
    require(isinstance(request, dict) and set(request) == {'schema', 'type', 'variants'}
            and type(request['schema']) is int and request['schema'] == 1
            and request['type'] == 'archived-original-case-partition-request'
            and isinstance(request['variants'], dict) and set(request['variants']) == {'A', 'B', 'C'},
            'invalid original case partition request')
    facts, source_facts = {}, {}
    for name, entry in request['variants'].items():
        require(isinstance(entry, dict) and set(entry) == {'archive_cases', 'source_root'}
                and isinstance(entry['source_root'], str), 'invalid variant case reference')
        case_request, unused = refs.read(entry['archive_cases'])
        facts[name] = reader.project(refs, entry['source_root'], case_request)
        scope = facts[name]['scope']
        require(scope['variant'] == name and scope['event'] == 'pull_request' and scope['run_attempt'] == 1,
                'case partition variant/event/attempt differs')
        input_request, unused = refs.read(case_request['archive_inputs'])
        source_request, unused = refs.read(input_request['source'])
        source_facts[name] = reader.inputs.source.project(refs, entry['source_root'], source_request)
    scopes = [value['scope'] for value in facts.values()]
    require(len({tuple(value[key] for key in ('repository', 'pr', 'branch', 'base_sha')) for value in scopes}) == 1,
            'case partition repository/PR/branch/base differs')
    require(len({value['head_sha'] for value in scopes}) == len({value['run_id'] for value in scopes}) == 3,
            'reused case partition head or run')
    require(len({value['preserved_source_digest'] for value in source_facts.values()}) == 1
            and len({value['owned_recipe_digest'] for value in source_facts.values()}) == 1,
            'case partition preserved source or recipe differs')
    inventories = {name: value['source_inventory'] for name, value in facts.items()}
    complete = all(value.get('all_source_cases_resolved', value['all_source_cases_statically_parsed']) and not inventories[name]['unresolved']
                   for name, value in facts.items())
    result = partition(inventories) if complete else {'original_cases': None, 'additions': None}
    return {'schema': 1, 'type': 'archived-original-case-partition',
            'variant_bindings': {name: {'scope': value['scope'], 'checkout': value['checkout'],
                'source_inventory_digest': value['source_inventory_digest']} for name, value in facts.items()},
            **result, 'known_source_cases': {name: value['cases'] for name, value in inventories.items()},
            'known_static_subset_partitioned': all(value['all_source_cases_statically_parsed'] for value in facts.values()), 'original_case_partition_verified': complete,
            'unresolved': {name: value['unresolved'] for name, value in inventories.items()},
            'original_assertions_verified': False, 'normalized_original_inputs_verified': False,
            'whole_build_provenance_complete': False, 'first_complete_PR_sample': False,
            'performance_accepted': False, 'input_files': refs.finish()}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for field in ('manifest', 'evidence-root', 'output'):
        parser.add_argument('--' + field, required=True)
    args = parser.parse_args()
    manifest = Path(args.manifest)
    raw = g.file_bytes(manifest)
    refs = g.References(args.evidence_root)
    result = project(refs, g.json_bytes(raw))
    require(g.checksum(g.file_bytes(manifest)) == g.checksum(raw), 'case partition request changed during replay')
    require(refs.finish() == result['input_files'], 'case partition inputs changed during replay')
    g.write_new(Path(args.output), result)
    print('original and addition identities derived; unresolved source and other proof gaps remain explicit')


if __name__ == '__main__':
    try:
        main()
    except (ValueError, OSError, KeyError, TypeError) as error:
        print('case partition rejected: ' + str(error), file=sys.stderr)
        sys.exit(1)
