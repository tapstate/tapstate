#!/usr/bin/env python3
"""Derive a source basis from archived bytes and reviewed Git recipes.

This is source readback, not whole-build input or first-PR-sample acceptance.
No archive member is extracted or executed, and no checkout/ref is changed.
"""
import argparse
from contextlib import redirect_stdout
import hashlib
import importlib.util
import io
from pathlib import Path
import re
import sys
import tarfile
from types import SimpleNamespace

sys.dont_write_bytecode = True
spec = importlib.util.spec_from_file_location('ci_source_variants', Path(__file__).with_name('_ci-variants.py'))
variants = importlib.util.module_from_spec(spec)
spec.loader.exec_module(variants)
groups = variants.groups
require = groups.require
MAX_MEMBER = 32 * 1024 * 1024
MAX_FILES = 20000


def archived_tree(raw, expected_commit):
    """Read the Git archive's complete path/mode/blob map without extracting it."""
    files, seen, directories = {}, set(), set()
    with tarfile.open(fileobj=io.BytesIO(raw), mode='r:') as archive:
        require(archive.pax_headers.get('comment') == expected_commit, 'source archive commit differs')
        for member in archive:
            name = member.name.rstrip('/') if member.isdir() else member.name
            variants.path_name(name)
            require(name not in seen, 'duplicate source archive entry')
            seen.add(name)
            require(len(seen) <= MAX_FILES, 'source archive has too many entries')
            require(member.mode & 0o7000 == 0, 'source archive has special permissions')
            require(member.sparse is None and not any(k.startswith('GNU.sparse') for k in member.pax_headers),
                    'unsupported sparse source archive entry')
            if member.isdir():
                require(member.size == 0, 'source directory contains data')
                directories.add(name)
                continue
            if member.issym():
                require(member.size == 0 and '\x00' not in member.linkname, 'invalid source symlink')
                data, mode = member.linkname.encode(), '120000'
            else:
                require(member.isfile() and 0 <= member.size <= MAX_MEMBER, 'unsupported source archive entry')
                stream = archive.extractfile(member)
                require(stream is not None, 'source archive entry is unreadable')
                data = stream.read(MAX_MEMBER + 1)
                require(len(data) == member.size, 'source archive size differs')
                mode = '100755' if member.mode & 0o111 else '100644'
            blob = hashlib.sha1(b'blob ' + str(len(data)).encode() + b'\0' + data).hexdigest()
            files[name] = {'mode': mode, 'blob': blob}
    require(files, 'source archive is empty')
    expected_directories = {
        '/'.join(parts[:i]) for name in files for parts in [name.split('/')] for i in range(1, len(parts))}
    require(directories == expected_directories, 'source archive directory set differs')
    for name in files:
        parts = name.split('/')
        require(not any('/'.join(parts[:i]) in files for i in range(1, len(parts))), 'source file is also a parent')
    return files


def project(refs, root, request):
    require(type(request.get('schema')) is int and request['schema'] == 1
            and request.get('type') == 'source-projection-request', 'unsupported source projection request')
    scope = request['scope']
    require(isinstance(scope, dict), 'invalid source projection scope')
    fields = {'repository', 'branch', 'variant', 'event', 'run_id', 'run_attempt', 'head_sha'}
    if scope.get('event') == 'pull_request':
        fields |= {'pr', 'base_sha'}
    require(set(scope) == fields, 'source scope fields differ')
    require(scope['variant'] in {'A', 'B', 'C'} and scope['event'] in {'pull_request', 'workflow_dispatch'},
            'unsupported source projection scope')
    require(type(scope['run_id']) is int and scope['run_id'] > 0
            and type(scope['run_attempt']) is int and 1 <= scope['run_attempt'] <= 100,
            'invalid source run/attempt')
    groups.receipt.sha(scope['head_sha'], 'source head')
    recipe, unused = refs.read(request['recipe'])
    require(recipe['scope']['repository'] == scope['repository'] and recipe['scope']['branch'] == scope['branch'],
            'recipe belongs to another source scope')
    root = variants.validate_recipe(refs, root, recipe)
    archive, receipt_path = refs.read(request['archive'])
    require(receipt_path.name == 'receipt.json' and archive.get('archive_root') == str(receipt_path.parent),
            'archive location differs')
    with redirect_stdout(io.StringIO()):
        groups.receipt.verify(SimpleNamespace(directory=str(receipt_path.parent)))
    require(type(archive.get('run_id')) is int and type(archive.get('run_attempt')) is int, 'invalid receipt run/attempt')
    require(archive.get('repository') == scope['repository'] and archive.get('run_id') == scope['run_id']
            and archive.get('run_attempt') == scope['run_attempt'] and archive.get('event') == scope['event']
            and archive.get('source_sha') == scope['head_sha'], 'receipt source scope differs')

    def raw(name):
        require(name in archive['files'], 'mandatory source archive input missing: ' + name)
        value = {'path': (receipt_path.parent / name).relative_to(refs.root).as_posix(),
                 'sha256': archive['files'][name]['sha256']}
        return refs.blob(value)[0]

    run = groups.json_bytes(raw('run.json'))
    require(isinstance(run, dict) and isinstance(run.get('repository'), dict), 'invalid archived run source')
    require(type(run.get('id')) is int and type(run.get('run_attempt')) is int
            and run['id'] == scope['run_id'] and run['run_attempt'] == scope['run_attempt']
            and run.get('head_sha') == scope['head_sha'] and run.get('head_branch') == scope['branch']
            and run.get('event') == scope['event'] and run.get('repository', {}).get('full_name') == scope['repository'],
            'archived run source differs')
    missing = ['production_P', 'I', 'D', 'R', 'runtime_spec', 'G_v', 'live_PR_identity']
    if scope['event'] == 'pull_request':
        require(type(scope.get('pr')) is int and scope['pr'] > 0, 'invalid source PR')
        groups.receipt.sha(scope['base_sha'], 'source base')
        associations = run.get('pull_requests', [])
        require(isinstance(associations, list), 'invalid archived PR associations')
        for value in associations:
            require(isinstance(value, dict) and isinstance(value.get('head'), dict)
                    and isinstance(value.get('base'), dict), 'invalid archived source PR')
            require(type(value.get('number')) is int and value['number'] == scope['pr']
                    and value.get('head', {}).get('sha') == scope['head_sha']
                    and value.get('base', {}).get('sha') == scope['base_sha'], 'archived source PR differs')
        if not associations:
            missing.append('archived_PR_association')
    if scope['run_attempt'] != 1:
        missing.append('first_attempt')
    if scope['event'] != 'pull_request':
        missing.append('pull_request_event')

    def source(commit_name, tar_name, commit_sha, tree_sha):
        commit = groups.json_bytes(raw(commit_name))
        require(isinstance(commit, dict), 'invalid archived source commit')
        nested = commit.get('commit', {})
        require(isinstance(nested, dict) and isinstance(commit.get('tree', nested.get('tree', {})), dict),
                'invalid archived source tree')
        if 'tree' in commit and 'tree' in nested:
            require(commit['tree'] == nested['tree'], 'contradictory archived source trees')
        recorded_tree = commit.get('tree', nested.get('tree', {})).get('sha')
        require(commit.get('sha') == commit_sha and recorded_tree == tree_sha, 'archived source commit/tree differs')
        variants.commit_sha(root, commit_sha)
        require(variants.text(root, 'rev-parse', commit_sha + '^{tree}') == tree_sha, 'Git source object differs')
        parents = commit.get('parents')
        require(isinstance(parents, list) and all(isinstance(p, dict) and isinstance(p.get('sha'), str) for p in parents),
                'invalid archived source parents')
        actual_parents = variants.text(root, 'rev-list', '--parents', '-n', '1', commit_sha).split()[1:]
        require([p['sha'] for p in parents] == actual_parents, 'archived source parents differ')
        files = archived_tree(raw(tar_name), commit_sha)
        require(files == variants.tree_files(root, tree_sha), 'archived source path/mode/blob map differs')
        return files

    tree = recipe['variant_trees'][scope['variant']]
    files = source('commit.json', 'source.tar', scope['head_sha'], tree)
    owned = set(recipe['owned_paths'])
    preserved = {name: entry for name, entry in files.items() if name not in owned}
    require(variants.source_digest(preserved) == recipe['preserved_source_digest'], 'preserved source differs')
    checkout = None
    if scope['event'] == 'pull_request':
        event = groups.json_bytes(raw('event-inputs.json'))
        require(isinstance(event, dict), 'invalid checkout event source')
        require(event.get('repository') == scope['repository'] and event.get('event') == scope['event']
                and type(event.get('run_id')) is int and event['run_id'] == scope['run_id']
                and type(event.get('run_attempt')) is int and event['run_attempt'] == scope['run_attempt'],
                'checkout event source differs')
        checkout_sha = groups.receipt.sha(event['checkout_sha'], 'source checkout')
        if archive.get('checkout_sha') is None:
            missing.append('receipt_checkout_binding')
        else:
            require(groups.receipt.sha(archive['checkout_sha'], 'receipt checkout') == checkout_sha,
                    'receipt checkout differs')
        require(event.get('checkout_tree') == tree, 'checkout changes the reviewed variant tree')
        expected_parents = [scope['base_sha'], scope['head_sha']]
        require(event.get('checkout_parents') == expected_parents, 'checkout PR parents differ')
        require(variants.text(root, 'rev-list', '--parents', '-n', '1', checkout_sha).split()[1:] == expected_parents,
                'Git checkout PR parents differ')
        checkout_files = source('checkout-commit.json', 'checkout-source.tar', checkout_sha, tree)
        require(checkout_files == files, 'head and checkout source differ')
        checkout = {'sha': checkout_sha, 'tree': tree, 'source_digest': variants.source_digest(checkout_files)}
    else:
        if archive.get('checkout_sha') is None:
            missing.append('receipt_checkout_binding')
        else:
            require(groups.receipt.sha(archive['checkout_sha'], 'dispatch checkout') == scope['head_sha'],
                    'dispatch receipt checkout differs')
        if 'event-inputs.json' in archive['files']:
            event = groups.json_bytes(raw('event-inputs.json'))
            require(isinstance(event, dict) and event.get('repository') == scope['repository']
                    and event.get('event') == scope['event'] and type(event.get('run_id')) is int
                    and event['run_id'] == scope['run_id'] and type(event.get('run_attempt')) is int
                    and event['run_attempt'] == scope['run_attempt'] and event.get('checkout_sha') == scope['head_sha']
                    and event.get('checkout_tree') == tree, 'dispatch checkout event differs')
            parents = variants.text(root, 'rev-list', '--parents', '-n', '1', scope['head_sha']).split()[1:]
            require(event.get('checkout_parents') == parents, 'dispatch checkout parents differ')
        else:
            missing.append('archived_checkout_event')
        pair = {'checkout-commit.json', 'checkout-source.tar'} & set(archive['files'])
        require(not pair or len(pair) == 2, 'dispatch checkout source is incomplete')
        if pair:
            checkout_files = source('checkout-commit.json', 'checkout-source.tar', scope['head_sha'], tree)
            require(checkout_files == files, 'dispatch checkout source differs')
            checkout = {'sha': scope['head_sha'], 'tree': tree,
                        'source_digest': variants.source_digest(checkout_files)}
        else:
            missing.append('archived_checkout_source')
    owned_variants = {}
    for name, candidate in recipe['variant_trees'].items():
        entries = variants.tree_files(root, candidate)
        owned_variants[name] = {path: entries.get(path) for path in sorted(owned)}
    basis = {'owned_paths': sorted(owned), 'variants': owned_variants}
    inputs = refs.finish()
    return {'schema': 1, 'type': 'archived-source-projection', 'scope': scope,
            'head': {'sha': scope['head_sha'], 'tree': tree, 'source_digest': variants.source_digest(files),
                     'file_count': len(files)}, 'checkout': checkout,
            'preserved_source_digest': variants.source_digest(preserved),
            'owned_recipe_digest': groups.checksum(groups.canonical(basis)), 'owned_recipe_basis': basis,
            'archived_source_readback_verified': True, 'unverified_components': sorted(missing),
            'whole_input_projection_verified': False, 'first_complete_PR_sample': False,
            'performance_accepted': False, 'input_files': inputs}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for field in ('manifest', 'evidence-root', 'root', 'output'):
        parser.add_argument('--' + field, required=True)
    args = parser.parse_args()
    request = groups.json_bytes(groups.file_bytes(Path(args.manifest)))
    answer = project(groups.References(args.evidence_root), args.root, request)
    groups.write_new(Path(args.output), answer)
    print('archived source readback verified; whole input and PR acceptance remain unverified')


if __name__ == '__main__':
    try:
        main()
    except (ValueError, OSError, KeyError, TypeError, tarfile.TarError) as error:
        print('source projection rejected: ' + str(error), file=sys.stderr)
        sys.exit(1)
