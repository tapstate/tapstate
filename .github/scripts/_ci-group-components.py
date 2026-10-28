#!/usr/bin/env python3
"""Replay full variant component facts from original archives without normalization."""
import argparse
import importlib.util
from pathlib import Path
import sys

sys.dont_write_bytecode = True
spec = importlib.util.spec_from_file_location('component_cases', Path(__file__).with_name('_ci-archive-cases.py'))
cases = importlib.util.module_from_spec(spec)
spec.loader.exec_module(cases)
g, require = cases.g, cases.require
triggers = g.module('component_triggers', '_ci-trigger-basis.py')


def project(refs, root, request):
    require(isinstance(request, dict) and set(request) == {'schema', 'type', 'archive_cases'}
            and type(request['schema']) is int and request['schema'] == 1
            and request['type'] == 'archived-group-components-request', 'invalid group component request')
    case_request, unused = refs.read(request['archive_cases'])
    case_facts = cases.project(refs, root, case_request)
    input_request, unused = refs.read(case_request['archive_inputs'])
    input_facts = cases.inputs.project(refs, root, input_request)
    source_request, unused = refs.read(input_request['source'])
    trigger_facts = triggers.project(refs, root,
        {'schema': 1, 'type': 'trigger-basis-request', 'source': input_request['source']})
    scope = case_facts['scope']
    require(input_facts['scope'] == trigger_facts['scope'] == scope
            and case_facts['checkout'] == input_facts['checkout'] == trigger_facts['checkout'],
            'component source scopes differ')
    archive = cases.inputs.Archive(refs, source_request['archive'], scope)
    snapshots = {name: g.json_bytes(archive.artifact('ci-inputs-' + name)['build-inputs.json'])
                 for name in input_facts['input_member_hashes']}
    observed = snapshots['build']
    dependency_fields = ('dependency_repository_files', 'dependency_coordinate_models', 'dependency_producer_facts')
    if 'observed_resolved_maven_artifacts' in input_facts:
        dependency_fields += ('resolved_maven_artifacts',)
    require(all(all(isinstance(value.get(field), dict) for field in dependency_fields) for value in snapshots.values()),
            'component dependency fields missing')
    # Available cache extras can differ. The original input comparator above
    # independently reconciles resolved classpaths and their actual byte bindings.
    dependencies = {'resolved_test_classpaths': observed['resolved_test_classpaths'],
        'peer_dependency_observations': {name: {field: value[field] for field in dependency_fields}
                                         for name, value in snapshots.items()}}
    payloads = {
        'P': {'preserved_source_digest': input_facts['source_basis']['preserved_source_digest'],
              'production': input_facts['observed_production'], 'packaged_outputs': observed['packaged_outputs']},
        'I': {'source_inventory': case_facts['source_inventory'],
              'all_source_cases_statically_parsed': case_facts['all_source_cases_statically_parsed'],
              'original_case_partition_verified': False},
        'D': dependencies,
        'R': {'workflows': trigger_facts['workflows'], 'original_labels': trigger_facts['original_labels'],
              'ruleset_records': trigger_facts['ruleset_records'], 'applicability_verified': False},
        'U': {'owned_recipe_digest': input_facts['source_basis']['owned_recipe_digest']},
        'spec': {'toolchain': input_facts['observed_toolchain'], 'jacoco_version': observed['jacoco_version'],
                 'runtime_spec_complete': False},
    }
    if 'all_source_cases_resolved' in case_facts:
        payloads['I']['all_source_cases_resolved'] = case_facts['all_source_cases_resolved']
        payloads['I']['provider_case_inputs'] = case_facts['provider_case_inputs']
    if 'observed_packaged_test_outputs' in input_facts:
        payloads['I']['test_packaged_outputs'] = input_facts['observed_packaged_test_outputs']
    if 'observed_test_resource_files' in input_facts:
        payloads['I']['test_resource_files']=input_facts['observed_test_resource_files']
    binding = {'run_id': scope['run_id'], 'run_attempt': scope['run_attempt'],
               'source_sha': input_facts['checkout']['sha']}
    return {'schema': 1, 'type': 'archived-full-variant-components', 'scope': scope, 'binding': binding,
            'payloads': payloads, 'component_hashes': {name: g.checksum(g.canonical(value)) for name, value in payloads.items()},
            'source_case_input_replayed': True, 'normalized_original_inputs_verified': False,
            'whole_build_provenance_complete': False, 'group_equivalence_proven': False,
            'first_complete_PR_sample': False, 'performance_accepted': False,
            'unverified_components': sorted(set(case_facts['unverified_components'])
                | set(trigger_facts['unverified_components']) | {'original_case_and_owned_change_normalization'}),
            'input_files': refs.finish()}


def component(refs, root, request_reference, name):
    require(name in g.COMPONENTS, 'unknown group component')
    request, unused = refs.read(request_reference)
    replay = project(refs, root, request)
    return {'schema': 1, 'type': 'archived-group-component', 'name': name,
            'binding': replay['binding'], 'payload': replay['payloads'][name],
            'request': request_reference, 'source_root': str(Path(root).absolute()),
            'normalized_original_inputs_verified': False}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for field in ('manifest', 'evidence-root', 'root', 'output'):
        parser.add_argument('--' + field, required=True)
    parser.add_argument('--component', choices=sorted(g.COMPONENTS))
    args = parser.parse_args()
    manifest = Path(args.manifest)
    raw = g.file_bytes(manifest)
    refs = g.References(args.evidence_root)
    request = g.json_bytes(raw)
    replay = project(refs, args.root, request)
    expected_inputs = replay['input_files']
    if args.component:
        # The request must itself be an immutable, hash-bound input under the evidence root.
        absolute = manifest.absolute()
        require(absolute.is_relative_to(refs.root), 'component request must be under the evidence root')
        reference = {'path': absolute.relative_to(refs.root).as_posix(), 'sha256': g.checksum(raw)}
        refs.blob(reference)
        expected_inputs = refs.finish()
        result = {'schema': 1, 'type': 'archived-group-component', 'name': args.component,
            'binding': replay['binding'], 'payload': replay['payloads'][args.component],
            'request': reference, 'source_root': str(Path(args.root).absolute()),
            'normalized_original_inputs_verified': False}
    else:
        result = replay
    require(g.checksum(g.file_bytes(manifest)) == g.checksum(raw), 'component request changed during replay')
    require(refs.finish() == expected_inputs, 'component input chain changed during replay')
    g.write_new(Path(args.output), result)
    print('full variant component facts replayed; original normalization and acceptance remain unverified')


if __name__ == '__main__':
    try:
        main()
    except (ValueError, OSError, KeyError, TypeError) as error:
        print('group components rejected: ' + str(error), file=sys.stderr)
        sys.exit(1)
