#!/usr/bin/env python3
"""Join archived PR identities and declared group inputs without certifying performance.

Existing case, coverage and gate comparators are replayed on hash-bound inputs.
Declared component equality does not establish whole-build provenance, all applicable
workflow results, a complete first PR sample or latest-tip acceptance.
"""
import argparse
from contextlib import redirect_stdout
from datetime import datetime
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path, PurePosixPath
import re
from types import SimpleNamespace
import sys
import tempfile
from urllib.parse import urlencode

sys.dont_write_bytecode = True
COMPONENTS = {'P', 'I', 'D', 'R', 'U', 'spec'}
REQUIRED_CHECKS = {'build', 'no-cjk', 'no-agent-footprint', 'sonarqube', 'e2e-admission',
                   'no-binary-bytes', 'dco', 'docs-classification', 'docs-impact'}


def module(name, filename):
    spec = importlib.util.spec_from_file_location(name, Path(__file__).with_name(filename))
    value = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(value)
    return value


receipt = module('ci_group_receipt', '_ci-receipt.py')
evidence = module('ci_group_evidence', '_ci-evidence.py')
require = receipt.require


def checksum(value):
    return hashlib.sha256(value).hexdigest()


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()


def unique_object(pairs):
    value = {}
    for name, entry in pairs:
        require(name not in value, 'duplicate JSON key')
        value[name] = entry
    return value


def file_bytes(path):
    require(path.is_file() and not path.is_symlink() and path.stat().st_size <= 64 * 1024 * 1024,
            'missing, symlink or oversized JSON input')
    with path.open('rb') as stream:
        raw = stream.read(64 * 1024 * 1024 + 1)
    require(len(raw) <= 64 * 1024 * 1024, 'oversized JSON input changed while reading')
    return raw


def json_bytes(raw):
    return json.loads(raw, object_pairs_hook=unique_object,
                      parse_constant=lambda value: (_ for _ in ()).throw(ValueError('nonfinite JSON input')))


def load(path):
    return json_bytes(file_bytes(path))


def write_new(path, value):
    payload = (json.dumps(value, indent=2, sort_keys=True, allow_nan=False) + '\n').encode()
    write_new_bytes(path, payload)


def write_new_bytes(path, payload):
    # Walk directory descriptors without following symlinks, then create exclusively.
    require(isinstance(payload, bytes), 'exclusive archive payload must be bytes')
    path = path.absolute()
    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
    directory = os.open(path.anchor, flags)
    try:
        for part in path.parent.parts[1:]:
            try:
                child = os.open(part, flags, dir_fd=directory)
            except FileNotFoundError:
                try:
                    os.mkdir(part, mode=0o700, dir_fd=directory)
                except FileExistsError:
                    pass
                child = os.open(part, flags, dir_fd=directory)
            os.close(directory)
            directory = child
        descriptor = os.open(path.name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                             0o600, dir_fd=directory)
        with os.fdopen(descriptor, 'wb') as stream:
            stream.write(payload)
    finally:
        os.close(directory)


class References:
    def __init__(self, root):
        root = Path(root).absolute()
        require(root.is_dir() and root.resolve() == root, 'evidence root must be a real directory')
        self.root = root
        self.files = {}

    def blob(self, reference):
        require(isinstance(reference, dict) and set(reference) == {'path', 'sha256'}, 'invalid file reference')
        name, expected = reference['path'], reference['sha256']
        require(isinstance(name, str) and name and '\\' not in name, 'invalid reference path')
        parts = PurePosixPath(name)
        require(not parts.is_absolute() and '..' not in parts.parts and parts.as_posix() == name,
                'reference path escapes evidence root')
        require(isinstance(expected, str) and re.fullmatch(r'[0-9a-f]{64}', expected), 'invalid reference digest')
        path = self.root
        for part in parts.parts:
            path /= part
            require(not path.is_symlink(), 'symlink reference path')
        raw = file_bytes(path)
        actual = checksum(raw)
        require(actual == expected, 'referenced input hash differs')
        require(name not in self.files or self.files[name] == actual, 'reference changed while reading')
        self.files[name] = actual
        return raw, path

    def read(self, reference):
        raw, path = self.blob(reference)
        return json_bytes(raw), path

    def finish(self):
        for name, expected in self.files.items():
            path = self.root
            for part in PurePosixPath(name).parts:
                path /= part
                require(not path.is_symlink(), 'symlink reference path changed during comparison')
            require(checksum((self.root / name).read_bytes()) == expected, 'input changed during comparison')
        return {name: {'sha256': value} for name, value in sorted(self.files.items())}


def component(refs, name, reference, binding=None):
    value, unused = refs.read(reference)
    if value.get('type') == 'archived-group-component':
        require(set(value) == {'schema', 'type', 'name', 'binding', 'payload', 'request', 'source_root',
                'normalized_original_inputs_verified'} and type(value['schema']) is int and value['schema'] == 1
                and value['name'] == name and value['normalized_original_inputs_verified'] is False,
                'invalid archived group component')
        reader = module('archived_component_replay', '_ci-group-components.py')
        expected = reader.component(refs, value['source_root'], value['request'], name)
        require(canonical(value) == canonical(expected), 'component payload differs from original archive replay')
        require(binding is None or value['binding'] == binding, 'component belongs to another variant run')
        return checksum(canonical(value['payload']))
    require(type(value.get('schema')) is int and value['schema'] == 1
            and value.get('type') == 'declared-group-component' and value.get('name') == name,
            'invalid declared component')
    payload = value.get('payload')
    require(isinstance(payload, (dict, list)) and payload, 'empty or unknown component payload')
    if binding is not None:
        require(value.get('binding') == binding, 'component belongs to another variant run')
    return checksum(canonical(payload))


def component_mode(refs, references):
    modes = {refs.read(reference)[0].get('type') for reference in references.values()}
    require(modes in ({'declared-group-component'}, {'archived-group-component'}),
            'mixed or unsupported group component capture')
    return 'archive-replayed' if modes == {'archived-group-component'} else 'declared'


def timestamp(value):
    result = datetime.fromisoformat(value.replace('Z', '+00:00'))
    require(result.tzinfo is not None, 'timestamp has no timezone')
    return result


def origin(value):
    require(isinstance(value, dict), 'observation run binding is missing')
    run, attempt = value.get('run_id'), value.get('run_attempt', value.get('attempt'))
    require(type(run) in (str, int) and re.fullmatch(r'[1-9][0-9]*', str(run))
            and type(attempt) in (str, int) and re.fullmatch(r'[1-9][0-9]*', str(attempt)),
            'invalid observation run binding')
    if 'run_attempt' in value and 'attempt' in value:
        require(str(value['run_attempt']) == str(value['attempt']), 'contradictory run attempts')
    return {'run_id': int(run), 'run_attempt': int(attempt),
            'source_sha': receipt.sha(value['source_sha'], 'observation source')}


def variant(refs, scope, value):
    archived, path = refs.read(value['archive'])
    require(path.name == 'receipt.json', 'variant archive must reference its receipt')
    with redirect_stdout(io.StringIO()):
        receipt.verify(SimpleNamespace(directory=str(path.parent)))
    require(archived.get('repository') == scope['repository'] and archived.get('run_attempt') == 1
            and archived.get('event') == 'pull_request', 'archive belongs to another scope or attempt')
    require(archived.get('archive_root') == str(path.parent), 'archive root differs')
    def raw(name):
        require(name in archived['files'], 'original archived input missing')
        return refs.read({'path': (path.parent / name).relative_to(refs.root).as_posix(),
                          'sha256': archived['files'][name]['sha256']})[0]
    run, event, commit = (raw(name) for name in ('run.json', 'event-inputs.json', 'checkout-commit.json'))
    identity = receipt.reconcile_pr_identity(run, event, commit, scope['repository'], scope['pr'],
                                             value['head_sha'], scope['branch'])
    require(run['conclusion'] == 'success', 'failed CI first attempt is diagnostic only')
    require(archived.get('run_id') == run['id'] and archived.get('source_sha') == run['head_sha']
            and archived.get('checkout_sha') == identity['checkout_sha'], 'archive input binding differs')
    require(timestamp(run['created_at']) <= timestamp(run['updated_at']), 'reversed CI run times')
    binding = {'run_id': run['id'], 'run_attempt': 1, 'source_sha': identity['checkout_sha']}
    require(set(value['components']) == COMPONENTS, 'group component set differs')
    mode = component_mode(refs, value['components'])
    hashes = {name: component(refs, name, value['components'][name], binding) for name in sorted(COMPONENTS)}
    result = {'identity': identity, 'binding': binding, 'components': hashes,
            'ci_created_at': run['created_at'], 'ci_terminal_observed_at': run['updated_at']}
    if mode == 'archive-replayed':
        result['component_mode'] = mode
    return result


def comparison(refs, request, before_binding=None, after_binding=None, lane=None):
    require(type(request.get('schema')) is int and request['schema'] == 1, 'unsupported comparison request')
    kind, inputs = request['kind'], request['inputs']
    wanted = {'before', 'after'} if kind in ('coverage', 'gates') else {'inventory', 'candidate_inventory', 'before', 'after', 'additions'}
    require(kind in ('tests', 'coverage', 'gates') and set(inputs) == wanted, 'comparison inputs differ')
    values, paths = {}, {}
    for name, reference in inputs.items():
        values[name], paths[name] = refs.read(reference)
    if before_binding is not None:
        for label, binding in [('before', before_binding), ('after', after_binding)]:
            actual = values[label].get('binding' if kind == 'coverage' else 'input_binding')
            require(origin(actual) == binding, 'comparison observation belongs to another run')
        if kind == 'tests':
            require(values['inventory']['source_sha'] == before_binding['source_sha']
                    and values['candidate_inventory']['source_sha'] == after_binding['source_sha'],
                    'comparison inventory source differs')
            require(lane is None or all(case['lane'] == lane for label in ('inventory', 'candidate_inventory')
                                       for case in values[label]['cases']), 'comparison lane differs')
    with tempfile.TemporaryDirectory(prefix='ci-group-comparison-') as temporary:
        # Comparators consume immutable copies of the exact bytes bound above.
        for name, path in paths.items():
            raw = file_bytes(path)
            require(checksum(raw) == inputs[name]['sha256'], 'comparison input changed before replay')
            snapshot = Path(temporary) / (name + '.json')
            snapshot.write_bytes(raw)
            paths[name] = snapshot
        target = Path(temporary) / 'comparison.json'
        if kind == 'tests':
            evidence.compare_tests(SimpleNamespace(**{name: str(path) for name, path in paths.items()},
                                                   output=str(target)))
        elif kind == 'coverage':
            evidence.compare_coverage(SimpleNamespace(before=str(paths['before']), after=str(paths['after']), output=str(target)))
        else:
            evidence.gates(SimpleNamespace(baseline=str(paths['before']), candidate=str(paths['after']), output=str(target)))
        result = load(target)
    require(result.get('complete') is True, 'existing evidence comparator is incomplete')
    return {'kind': kind, 'input_files': inputs, 'result': result}


def group_inputs(refs, value):
    require(type(value.get('schema')) is int and value['schema'] == 1 and value.get('type') == 'pr-group-inputs',
            'unsupported PR group input schema')
    scope = value['scope']
    require(type(scope['pr']) is int and scope['pr'] > 0
            and re.fullmatch(r'[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+', scope['repository'])
            and scope['branch'].startswith('ws/'), 'invalid experiment scope')
    require(value['cache_condition'] in ('warm', 'cold'), 'unknown group cache claim')
    require(set(value['variants']) == {'A', 'B', 'C'} and sorted(value['order']) == ['A', 'B', 'C'],
            'group must contain A/B/C exactly once')
    variants = {name: variant(refs, scope, entry) for name, entry in value['variants'].items()}
    require(len({entry.get('component_mode', 'declared') for entry in variants.values()}) == 1,
            'mixed variant component capture')
    require(len({entry['identity']['head_sha'] for entry in variants.values()}) == 3
            and len({entry['identity']['run_id'] for entry in variants.values()}) == 3, 'reused variant head or run')
    equal = all(entry['components'] == variants['A']['components'] for entry in variants.values())
    delta = None
    if 'normalization' in value:
        request = value['normalization']
        require(isinstance(request, dict) and set(request) == {'partition_request', 'review_policy'},
                'invalid group normalization request')
        adapter = module('group_delta_replay', '_ci-group-delta.py')
        delta = adapter.project(refs, {'schema': 1, 'type': 'archived-group-delta-request', **request})
        adapter.bind_group(delta, scope, variants)
    else:
        require(equal, 'group P/I/D/R/U/spec declaration differs')
    for previous, current in zip(value['order'], value['order'][1:]):
        require(timestamp(variants[previous]['ci_terminal_observed_at']) <= timestamp(variants[current]['ci_created_at']),
                'CI event runs overlap or were reordered')
    comparisons, seen = [], set()
    for entry in value['comparisons']:
        require(entry['before'] == 'A' and entry['after'] in ('B', 'C') and isinstance(entry['lane'], str)
                and entry['lane'], 'invalid group comparison pair or lane')
        request, unused = refs.read(entry['request'])
        key = (entry['after'], entry['lane'], request['kind'])
        require(key not in seen, 'duplicate group comparison')
        seen.add(key)
        if delta is not None and request['kind'] == 'tests':
            adapter.bind_test_inputs(refs, request, delta, entry['after'])
        result = comparison(refs, request, variants['A']['binding'], variants[entry['after']]['binding'], entry['lane'])
        comparisons.append({'before': 'A', 'after': entry['after'], 'lane': entry['lane'], **result})
    require(seen and all({kind for after, observed_lane, kind in seen if after == candidate and observed_lane == lane}
                            == {'tests', 'coverage', 'gates'}
                        for candidate in ('B', 'C') for lane in {entry['lane'] for entry in value['comparisons']}),
            'missing tests/coverage/gates comparison for a declared lane')
    result = {'schema': 1, 'type': 'pr-group-input-reconciliation', 'scope': scope, 'variants': variants,
            'cache_condition_claim': value['cache_condition'], 'cache_condition_verified': False,
            'declared_component_payloads_equal': equal, 'comparisons_replayed': comparisons,
            'first_complete_pr_sample_proven': False, 'group_equivalence_proven': False,
            'whole_build_provenance_complete': False, 'latest_tip_verified': False, 'performance_accepted': False,
            'limits': ['component payloads are declared normalized inputs, not independent producer proof',
                       'CI event order does not prove every applicable workflow was awaited',
                       'all-workflow applicability, byte provenance, cache and original assertion proof remain required']}
    if delta is not None:
        result['archived_group_delta'] = delta
        result['limits'][0] = 'full archived fingerprints retained; declared source deltas are not semantic equivalence proof'
    return result


def serial_group_inputs(refs, value):
    """Join the existing comparisons with mandatory source-bound serial evidence."""
    require(isinstance(value.get('serial_readbacks'), dict) and set(value['serial_readbacks']) == {'A', 'B', 'C'},
            'group requires serial readbacks for A/B/C')
    result = group_inputs(refs, value)
    serial = module('group_serial_readback', '_ci-serial-readback.py')
    result['serial_readbacks'] = {name: serial.bind(refs, reference, value['scope'],
            result['variants'][name], value['variants'][name]['archive'])
            for name, reference in value['serial_readbacks'].items()}
    for previous, current in zip(value['order'], value['order'][1:]):
        require(timestamp(result['serial_readbacks'][previous]['workflow_terminal_observed_at'])
                <= timestamp(result['serial_readbacks'][current]['workflow_created_at']),
                'applicable workflow timelines overlap or were reordered')
    result['archived_all_workflow_timeline_consistent'] = True
    result['source_bound_serial_transitions_replayed'] = True
    result['backend_response_attestation_proven'] = False
    result['limits'][1] = 'recorded source-bound serial traces are replayed; backend attestation and full sample acceptance remain unknown'
    return result


def impact(refs, value):
    require(value['group_state'] in ('pending', 'completed'), 'unknown group state')
    require(set(value['before']) == set(value['after']) == COMPONENTS, 'impact component set differs')
    require(component_mode(refs, value['before']) == component_mode(refs, value['after']),
            'mixed impact component capture')
    hashes = {label: {name: component(refs, name, reference) for name, reference in value[label].items()}
              for label in ('before', 'after')}
    changed = sorted(name for name in COMPONENTS if hashes['before'][name] != hashes['after'][name])
    return {'schema': 1, 'type': 'declared-input-impact', 'component_hashes': hashes, 'changed': changed,
            'pending_group_declarations_comparable': not changed,
            'completed_group_disposition': 'review-and-retest-affected-metrics' if changed else 'retain-versioned-declarations',
            'latest_tip_verified': False, 'performance_accepted': False}


def page_values(request, endpoint, key):
    rows, ids, page = [], set(), 1
    while True:
        value = request(endpoint + ('&' if '?' in endpoint else '?') + f'per_page=100&page={page}')
        require(isinstance(value.get(key), list) and type(value.get('total_count')) is int, 'invalid API page')
        for row in value[key]:
            require(type(row['id']) is int and row['id'] not in ids, 'duplicate API row')
            ids.add(row['id']); rows.append(row)
        require(len(rows) <= value['total_count'], 'API count differs')
        if len(rows) == value['total_count']:
            return rows
        require(value[key] and page < 100, 'incomplete API pagination')
        page += 1


def head_status(scope, contract, request):
    require(type(scope['pr']) is int and scope['pr'] > 0
            and re.fullmatch(r'[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+', scope['repository']), 'invalid PR scope')
    receipt.sha(scope['head_sha'], 'expected head')
    prefix = 'repos/' + scope['repository']
    pr = request(f'{prefix}/pulls/{scope["pr"]}')
    require(pr['number'] == scope['pr'] and pr['state'] == 'open' and pr['draft'] is True,
            'experiment PR is not the expected open draft')
    require(pr['head']['sha'] == scope['head_sha'] and pr['head']['ref'] == scope['branch']
            and pr['head']['repo']['full_name'] == pr['base']['repo']['full_name'] == scope['repository']
            and pr['base']['ref'] == 'main', 'current PR head or branch differs')
    paths, checks = contract['applicable_workflows'], contract['required_checks']
    require(isinstance(paths, list) and paths and len(set(paths)) == len(paths)
            and all(re.fullmatch(r'\.github/workflows/[A-Za-z0-9_.-]+\.ya?ml', path) for path in paths)
            and '.github/workflows/ci.yml' in paths, 'invalid applicable workflow contract')
    require(isinstance(checks, list) and len(set(checks)) == len(checks) and REQUIRED_CHECKS <= set(checks),
            'required check contract is incomplete')
    runs = page_values(request, f'{prefix}/actions/runs?' + urlencode({'event': 'pull_request', 'head_sha': scope['head_sha']}), 'workflow_runs')
    require(all(run['head_sha'] == scope['head_sha'] and run['head_branch'] == scope['branch']
                and run['event'] == 'pull_request' and run['repository']['full_name'] == scope['repository']
                for run in runs), 'foreign workflow run in head snapshot')
    for run in runs:
        associations = run['pull_requests']
        require(isinstance(associations, list), 'invalid workflow PR associations')
        require(all(association['number'] == scope['pr']
                    and association.get('head', {}).get('sha') == scope['head_sha']
                    and association.get('base', {}).get('sha') == pr['base']['sha'] for association in associations),
                'foreign PR run association')
    merge = receipt.sha(pr['merge_commit_sha'], 'current merge')
    commit = request(f'{prefix}/git/commits/{merge}')
    require(commit['sha'] == merge and [parent['sha'] for parent in commit['parents']]
            == [pr['base']['sha'], scope['head_sha']], 'current merge parents differ')
    observed, check_ids = [], set()
    for source in sorted({scope['head_sha'], merge}):
        for check in page_values(request, f'{prefix}/commits/{source}/check-runs?filter=all', 'check_runs'):
            require(check['head_sha'] == source, 'foreign check source')
            associations = check.get('pull_requests', [])
            require(isinstance(associations, list), 'invalid check PR associations')
            require(all(association['number'] == scope['pr']
                        and association['head']['sha'] == scope['head_sha']
                        and association['base']['sha'] == pr['base']['sha'] for association in associations),
                    'foreign PR check association')
            if check['id'] not in check_ids:
                check_ids.add(check['id']); observed.append(check)
    reasons = []
    observed_paths = {run['path'] for run in runs}
    if observed_paths != set(paths): reasons.append('applicable workflow set is missing or differs')
    if any(run['status'] != 'completed' for run in runs): reasons.append('workflow still running or queued')
    if not set(checks) <= {check['name'] for check in observed}: reasons.append('required check instance missing')
    if any(check['status'] != 'completed' for check in observed): reasons.append('check still running or queued')
    final_pr = request(f'{prefix}/pulls/{scope["pr"]}')
    require(all(final_pr.get(key) == pr.get(key) for key in ('number', 'state', 'draft', 'merge_commit_sha'))
            and all(final_pr[side][key] == pr[side][key] for side in ('head', 'base') for key in ('sha', 'ref'))
            and all(final_pr[side]['repo']['full_name'] == pr[side]['repo']['full_name'] for side in ('head', 'base')),
            'PR head or merge scope changed during inspection')
    return {'schema': 1, 'type': 'read-only-pr-head-status', 'scope': scope, 'merge_sha': merge,
            'base_sha': pr['base']['sha'],
            'can_advance_after_archiving': not reasons, 'reasons': reasons,
            'runs': [{key: run[key] for key in ('id', 'path', 'run_attempt', 'status', 'conclusion')} for run in runs],
            'checks': [{key: check[key] for key in ('id', 'name', 'head_sha', 'status', 'conclusion')} for check in observed],
            'unassociated_check_ids': [check['id'] for check in observed if not check.get('pull_requests')],
            'unassociated_run_ids': [run['id'] for run in runs if not run['pull_requests']],
            'archive_readback_verified': False, 'first_complete_pr_sample_proven': False,
            'limits': ['terminal failures and retries are diagnostic; archive before making any ordinary commit',
                       'this command never pushes, dispatches, cancels or changes a PR']}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='command', required=True)
    for name in ('group-inputs', 'serial-group-inputs', 'compare-evidence', 'impact'):
        p = commands.add_parser(name)
        p.add_argument('--manifest', required=True)
        p.add_argument('--evidence-root', required=True)
        p.add_argument('--output', required=True)
    p = commands.add_parser('head-status')
    p.add_argument('--manifest', required=True, help='Explicit scope and applicable workflow/check contract')
    p.add_argument('--output', required=True)
    args = parser.parse_args()
    output = Path(args.output)
    require(not output.exists() and not output.is_symlink(), 'output exists; never overwrite evidence')
    manifest = Path(args.manifest)
    raw = file_bytes(manifest)
    manifest_hash = checksum(raw)
    value = json_bytes(raw)
    if args.command == 'head-status':
        result = head_status(value['scope'], value['contract'], receipt.api)
    else:
        refs = References(args.evidence_root)
        operation = {'group-inputs': group_inputs, 'serial-group-inputs': serial_group_inputs, 'compare-evidence': comparison, 'impact': impact}[args.command]
        result = operation(refs, value)
        result['input_files'] = refs.finish()
    require(checksum(manifest.read_bytes()) == manifest_hash, 'manifest changed during comparison')
    result['manifest_sha256'] = manifest_hash
    write_new(output, result)


if __name__ == '__main__':
    try:
        main()
    except (ValueError, KeyError, TypeError, OSError) as error:
        print('ci-pr-groups: ' + str(error), file=sys.stderr)
        sys.exit(1)
