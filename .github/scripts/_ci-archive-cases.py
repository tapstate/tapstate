#!/usr/bin/env python3
"""Independently parse archived source and replay original case report members.

Only trusted reader code runs. Archived regular files are copied as non-executable
data into a fresh temporary directory; links and archived code execution are forbidden.
Computed providers remain unknown without separately bound replay result records.
"""
import argparse
import copy
from collections import Counter
from contextlib import redirect_stdout
import importlib.util
import io
from pathlib import Path
import re
import sys
import tarfile
import tempfile
from types import SimpleNamespace

sys.dont_write_bytecode = True
spec = importlib.util.spec_from_file_location('archive_inputs', Path(__file__).with_name('_ci-archive-inputs.py'))
inputs = importlib.util.module_from_spec(spec)
spec.loader.exec_module(inputs)
g, require, s = inputs.g, inputs.require, inputs.shards
ev = g.evidence


def data_file(root, name, raw):
    inputs.source.variants.path_name(name)
    path = root / name
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('xb') as stream:
        stream.write(raw)
    path.chmod(0o600)
    return path


def parsed_source(raw, root):
    with tarfile.open(fileobj=io.BytesIO(raw), mode='r:') as archive:
        for member in archive:
            if member.isdir():
                continue
            require(member.isfile(), 'case source parser does not follow source links')
            data_file(root, member.name, archive.extractfile(member).read())


def source_inventory(root, checkout, tar_sha, lane, output):
    workflow = root / '.github/workflows/ci.yml'
    matches = re.findall(r'^\s*\.github/scripts/ci-evidence\.sh inventory --root \. --lane ci '
                         r'--published-connectors (unknown|absent|available) '
                         r'--native-cli-presence (unknown|absent|present) --output ', workflow.read_text(), re.M)
    connectors, native = matches[0] if len(matches) == 1 else ('unknown', 'unknown')
    args = SimpleNamespace(root=str(root), source_archive=None, source_receipt=None, lane=lane,
                           parameters=None, kind=None, published_connectors=connectors,
                           native_cli_presence=native, output=str(output))
    # The temporary data tree has no Git origin. The already verified original
    # archive supplies its origin; inherited Git environment must not invent one.
    original_sha = ev.current_sha
    try:
        ev.current_sha = lambda unused: None
        with redirect_stdout(io.StringIO()):
            ev.inventory(args)
    finally:
        ev.current_sha = original_sha
    value = ev.load(output)
    real_published = [policy for method in value['methods']
                      for policy in (method.get('published_policy') or {}).get('cases', {}).values()
                      if policy.get('requires_real')]
    require(connectors != 'unknown' or not real_published, 'published connector availability is unknown')
    value.update(source_sha=checkout, source_origin={'kind': 'verified-archived-checkout', 'source_sha': checkout,
                                                   'source_archive_sha256': tar_sha})
    value['original_product_sources'] = {p.relative_to(root).as_posix(): g.checksum(p.read_bytes())
                                         for p in sorted(root.rglob('*')) if p.is_file()
                                         and ('/src/main/' in p.relative_to(root).as_posix() or p.name == 'pom.xml')}
    ev.output(output, value)
    return value, {'published_connectors': connectors, 'native_cli_presence': native,
                   'workflow_sha256': g.checksum(workflow.read_bytes())}


def methods(value):
    rows = value['methods']
    require(isinstance(rows, list) and len({ev.method_key(r) for r in rows}) == len(rows), 'duplicate or invalid case methods')
    return {ev.method_key(r): r for r in rows}


def source_rows(value, source_methods):
    rows = value.get('cases')
    require(isinstance(rows, list), 'invalid producer case rows')
    for row in rows:
        require(isinstance(row, dict) and ev.method_key(row) in source_methods, 'case row has no source method')
        require(all(isinstance(row.get(k), str) and re.fullmatch(r'[0-9a-f]{64}', row[k])
                    for k in ('case_digest', 'parameter_digest')), 'invalid producer case digest')
    return rows


def published_observations(inv, cases, ledger):
    declared = methods(inv)
    expected_ledger = []
    for case in cases:
        method = declared.get(ev.method_key(case))
        policy = (method.get('published_policy') or {}) if method else {}
        if not policy or case.get('parameter') is None:
            continue
        entry = policy['cases'].get(case['parameter'])
        require(entry is not None and case['status'] == entry['expected_status'],
                'published outcome differs from source applicability')
        if case['status'] == 'skipped':
            require(case.get('skip') is not None and case['skip']['reason'] == entry['skip_message'],
                    'published skip reason differs from source applicability')
        elif case['status'] == 'executed':
            expected_ledger.append(case['parameter'])
    require(Counter(ledger) == Counter(expected_ledger), 'published ledger differs from source executed cases')


def provider_inventory(refs, root, request, inventory, scope, workers):
    references = request.get('provider_replays')
    if references is None:
        return inventory, None
    require(isinstance(references, dict) and set(references) == set(workers), 'provider case replay worker set differs')
    replay = g.module('case_provider_replay', '_ci-provider-replay-readback.py')
    methods, bindings = None, {}
    for worker, reference in sorted(references.items()):
        audit_request, unused = refs.read(reference)
        audited = replay.audit(refs, root, audit_request)
        value, unused = refs.read(audit_request['replay'])
        original, unused = refs.read(value['request'])
        require(audited['original_scope'] == scope and audited['worker'] == worker
                and original['archive_inputs']['sha256'] == request['archive_inputs']['sha256'],
                'provider case replay belongs to another source/run/input request')
        current = audited['reconstructed_methods']
        require(methods is None or g.canonical(methods) == g.canonical(current), 'provider case identities differ between workers')
        methods = current
        bindings[worker] = {'request': reference, 'replay_receipt_sha256': audited['replay_receipt_sha256'],
                            'method_count': audited['method_count'], 'case_count': audited['case_count']}
    result = copy.deepcopy(inventory)
    by_key = {ev.method_key(row): row for row in result['methods']}
    require(len(by_key) == len(result['methods']), 'duplicate provider source method identity')
    resolved = set()
    for method in methods or []:
        key = ev.method_key(method)
        require(key in by_key and key not in resolved and by_key[key]['parameters'] is None
                and by_key[key]['source'] == method['source'], 'provider case replay replaces a static or foreign method')
        by_key[key].update({field: value for field, value in method.items() if field not in ('lane','module','kind','class','method','source')})
        resolved.add(key)
    pairs = {(by_key[key]['source'], by_key[key]['method']) for key in resolved}
    pairs = {pair for pair in pairs if all(row['parameters'] is not None for row in result['methods']
             if (row['source'], row['method']) == pair)}
    result['unresolved'] = [gap for gap in result['unresolved'] if (gap.get('source'), gap.get('method')) not in pairs]
    result['cases'] = sorted([{**{field: method[field] for field in ('lane','module','kind','class','method')}, 'parameter': parameter}
        for method in result['methods'] for parameter in method['parameters'] or []], key=ev.identity)
    result['complete'] = not result['unresolved'] and all(row['parameters'] is not None for row in result['methods'])
    return result, bindings


def project(refs, root, request, include_test_observations=False):
    require(type(include_test_observations) is bool, 'invalid full observation export option')
    require(isinstance(request, dict) and set(request) in ({'schema', 'type', 'archive_inputs'}, {'schema', 'type', 'archive_inputs', 'provider_replays'})
            and type(request['schema']) is int and request['schema'] == 1
            and request['type'] == 'archive-cases-request', 'invalid archived case request')
    input_request, unused = refs.read(request['archive_inputs'])
    readback = inputs.project(refs, root, input_request)
    source_request, unused = refs.read(input_request['source'])
    archive = inputs.Archive(refs, source_request['archive'], readback['scope'])
    checkout = readback['checkout']['sha']
    source_raw = archive.raw('checkout-source.tar')
    plan = readback['plan']
    observed, gaps = [], []
    reader_paths = ['_ci-archive-cases.py', '_ci-archive-inputs.py', '_ci-source-projection.py',
                    '_ci-variants.py', '_ci-pr-groups.py', '_ci-evidence.py', '_ci-shards.py', '_ci-receipt.py', '_ci-provider-readback.py']
    if 'provider_replays' in request: reader_paths.append('_ci-provider-replay-readback.py')
    reader_hashes = {name: g.checksum(Path(__file__).with_name(name).read_bytes()) for name in reader_paths}
    worker_reads, test_observations = {}, {}
    with tempfile.TemporaryDirectory(prefix='ci-case-data-') as temporary:
        data_root = Path(temporary).resolve()
        tree = data_root / 'source'
        tree.mkdir()
        parsed_source(source_raw, tree)
        # Discover classes from the verified default reactor independently of
        # both the plan's union and the producer's expected-case hash document.
        require(plan['selection'] == 'default-reactor', 'case reader requires default reactor selection')
        require(Counter(s.key(t) for t in s.discover(tree)) == Counter(s.key(t) for t in plan['expected']),
                'source-discovered class cohort differs from archived plan')
        inv_path = data_root / 'source-inventory.json'
        inv, environment = source_inventory(tree, checkout, g.checksum(source_raw), 'ci', inv_path)
        statically_complete = inv['complete']
        inv, provider_bindings = provider_inventory(refs, root, request, inv, readback['scope'], [row['id'] for row in plan['shards']])
        if provider_bindings is not None: ev.output(inv_path, inv)
        hash_path = data_root / 'source-case-hashes.json'
        with redirect_stdout(io.StringIO()):
            ev.export_hashes(SimpleNamespace(input=str(inv_path), output=str(hash_path)))
        derived = ev.load(hash_path)
        known_methods = methods(derived)
        source_methods = methods(inv)
        expected_reference = None
        for worker in plan['shards']:
            name = worker['id']
            members = archive.artifact('ci-inputs-' + name)
            expected = g.json_bytes(members['expected-case-hashes.json'])
            actual = g.json_bytes(members['observed-case-hashes.json'])
            require(type(expected.get('schema')) is int and expected['schema'] == 1
                    and expected.get('type') == 'expected-case-hashes' and expected.get('source_sha') == checkout,
                    'producer case inventory source differs')
            shared_fields = ('schema', 'type', 'complete', 'source_sha', 'source_origin', 'actual_source_sha',
                             'original_source_sha', 'methods', 'cases', 'case_count', 'unresolved_count')
            shared = {field: expected.get(field) for field in shared_fields}
            require(expected_reference is None or shared == expected_reference, 'worker case inventory semantics differ')
            expected_reference = shared
            recorded = methods(expected)
            source_rows(expected, source_methods)
            require(set(recorded) == set(known_methods), 'producer case method set differs from source')
            for identity, row in known_methods.items():
                fields = ('lane', 'module', 'kind', 'class', 'method', 'source', 'source_sha256',
                          'declaration_sha256', 'signature_types')
                require(all(row.get(k) == recorded[identity].get(k) for k in fields), 'producer case declaration differs from source')
                if source_methods[identity]['parameters'] is not None:
                    require(row.get('mapping') == recorded[identity].get('mapping'), 'producer static case mapping differs')
            known = {key for key, method in source_methods.items() if method['parameters'] is not None}
            producer_known = [r for r in expected['cases'] if ev.method_key(r) in known]
            require(Counter(g.canonical(r) for r in producer_known) == Counter(g.canonical(r) for r in derived['cases']),
                    'producer static case identities/skip policy differ from source')
            prepared = members['build-inputs.json']
            binding = {'run_id': readback['scope']['run_id'],
                       'run_attempt': readback['worker_producers'][name]['run_attempt'], 'source_sha': checkout}
            require(type(actual.get('schema')) is int and actual['schema'] == 1 and actual.get('type') == 'observed-case-hashes'
                    and g.origin(actual.get('input_binding')) == binding
                    and actual.get('inputs_sha256') == g.checksum(prepared)
                    and actual.get('source_inventory_sha256') == expected.get('source_inventory_sha256'),
                    'producer observations input/inventory binding differs')
            source_rows(actual, source_methods)
            target = data_root / name
            target.mkdir()
            result = archive.artifact('ci-test-results-' + name)
            for path, raw in result.items():
                data_file(target, path, raw)
            input_path = data_file(target, 'build-inputs.json', prepared)
            binding_path = data_file(target, 'binding.json', g.canonical(
                {'source_sha': checkout, 'run_id': binding['run_id'], 'attempt': binding['run_attempt']}))
            gates = target / 'class-gates'
            gates.mkdir()
            for path, raw in members.items():
                if path.startswith('class-gates/'):
                    data_file(target, path, raw)
            observation_path = target / 'observation.json'
            args = SimpleNamespace(root=str(target / name / 'files'), inventory=str(inv_path), lane='ci',
                                   inputs=str(input_path), binding=str(binding_path), class_gates=str(gates),
                                   artifact_manifest=str(target / name / 'manifest.json'), shard_plan=None, shard=None,
                                   output=str(observation_path))
            with redirect_stdout(io.StringIO()):
                ev.reports(args)
            observation = ev.load(observation_path)
            published_observations(inv, observation['cases'], observation['canonical_ledger'])
            if include_test_observations:
                test_observations[name] = {
                    'observation_json': observation_path.read_text(),
                    'observation_sha256': g.checksum(observation_path.read_bytes()),
                    'selected_tests': worker['tests'],
                    'prepared_input_member_sha256': g.checksum(prepared),
                    'result_manifest_sha256': g.checksum(result[name + '/manifest.json'])}
            replay_path = target / 'replayed-hashes.json'
            # Unknown parameters cannot be hashed as resolved identities. Retain
            # them in the observation, compare only independently parsed methods.
            projected = dict(observation)
            projected['cases'] = [r for r in observation['cases'] if r.get('parameter') is not None]
            ev.output(observation_path, projected)
            with redirect_stdout(io.StringIO()):
                ev.export_hashes(SimpleNamespace(input=str(observation_path), output=str(replay_path)))
            replay = ev.load(replay_path)
            require(actual.get('class_gate_files') == replay['class_gate_files']
                    and actual.get('ledger_digests') == replay['ledger_digests']
                    and actual.get('raw_ledger_digests') == replay['raw_ledger_digests'],
                    'producer class gate/ledger digests differ from raw replay')
            recorded_cases = [r for r in actual['cases'] if ev.method_key(r) in known]
            replay_cases = [r for r in replay['cases'] if ev.method_key(r) in known]
            require(Counter(g.canonical(r) for r in recorded_cases) == Counter(g.canonical(r) for r in replay_cases),
                    'producer observations differ from raw XML/ledger replay')
            require(actual.get('actual_xml_case_count') == observation['actual_xml_case_count']
                    and actual.get('source_not_applicable_count') == observation['source_not_applicable_count'],
                    'producer observation counters differ from raw replay')
            observed.extend(observation['cases'] + observation['source_not_applicable'])
            gaps.extend(observation['unresolved'])
            worker_reads[name] = {'source_inventory_sha256': expected['source_inventory_sha256'],
                                  'expected_member_sha256': g.checksum(members['expected-case-hashes.json']),
                                  'observed_member_sha256': g.checksum(members['observed-case-hashes.json']),
                                  'replayed_cases': replay_cases, 'complete': observation['complete'],
                                  'unresolved': observation['unresolved']}
        expected_known = Counter(ev.identity(r) for r in inv['cases'])
        observed_known = Counter(ev.identity(r) for r in observed if r.get('parameter') is not None and ev.method_key(r) in known)
        require(expected_known == observed_known, 'source case multiset differs from all original workers')
        fully_parsed = inv['complete'] and not gaps and all(v['complete'] for v in worker_reads.values())
        inventory = {'methods': inv['methods'], 'cases': inv['cases'], 'class_contracts': inv['class_contracts'],
                     'sources': inv['sources'], 'unresolved': inv['unresolved'], 'entry_contract': environment}
        exported = ({'test_observations': {'source_inventory_json': inv_path.read_text(),
                     'source_inventory_sha256': g.checksum(inv_path.read_bytes()),
                     'workers': test_observations}} if include_test_observations else {})
    require(reader_hashes == {name: g.checksum(Path(__file__).with_name(name).read_bytes()) for name in reader_paths},
            'trusted case reader changed during replay')
    return {'schema': 1, 'type': 'archived-case-readback', 'scope': readback['scope'], 'checkout': readback['checkout'],
            'source_class_cohort_verified': True, 'source_case_subset_verified': bool(inv['cases']),
            'all_source_cases_statically_parsed': fully_parsed and statically_complete,
            **({'all_source_cases_resolved': fully_parsed, 'provider_case_inputs': provider_bindings} if provider_bindings is not None else {}),
            'source_inventory': inventory, 'source_inventory_digest': g.checksum(g.canonical(inventory)),
            'workers': worker_reads, 'unverified_components': sorted(set(readback['unverified_components'])
                           | ({'computed_provider_identities'} if not fully_parsed else set())),
            'whole_input_projection_verified': False, 'first_complete_PR_sample': False, 'performance_accepted': False,
            **exported,
            'trusted_reader_hashes': reader_hashes, 'input_files': refs.finish()}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for field in ('manifest', 'evidence-root', 'root', 'output'):
        parser.add_argument('--' + field, required=True)
    parser.add_argument('--include-test-observations', action='store_true',
                        help='Retain full independently replayed worker outcomes and their exact source inventory')
    args = parser.parse_args()
    answer = project(g.References(args.evidence_root), args.root, g.load(Path(args.manifest)),
                     include_test_observations=args.include_test_observations)
    g.write_new(Path(args.output), answer)
    print('archived source case subset replayed; unknown providers and whole input acceptance remain unverified')


if __name__ == '__main__':
    try:
        main()
    except (ValueError, OSError, KeyError, TypeError, tarfile.TarError) as error:
        print('archive case projection rejected: ' + str(error), file=sys.stderr)
        sys.exit(1)
