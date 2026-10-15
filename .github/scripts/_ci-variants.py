#!/usr/bin/env python3
"""Prepare bounded source recipes, make ordinary variant commits and recover owned changes.

No command pushes, dispatches, cancels, merges, resets or switches a checkout.
Source recovery does not establish behavior, coverage or performance acceptance.
"""
import argparse
from contextlib import redirect_stdout
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path, PurePosixPath
import re
import subprocess
import sys
import tempfile

sys.dont_write_bytecode = True
spec = importlib.util.spec_from_file_location('ci_variant_groups', Path(__file__).with_name('_ci-pr-groups.py'))
groups = importlib.util.module_from_spec(spec); spec.loader.exec_module(groups)
require = groups.require
MAX_PATCH = 4 * 1024 * 1024


def git(root, *args, index=None, checked=True):
    environment = dict(os.environ)
    for key in list(environment):
        if key in {'GIT_DIR', 'GIT_WORK_TREE', 'GIT_INDEX_FILE', 'GIT_PREFIX', 'GIT_COMMON_DIR',
                   'GIT_QUARANTINE_PATH', 'GIT_NAMESPACE', 'GIT_OBJECT_DIRECTORY',
                   'GIT_ALTERNATE_OBJECT_DIRECTORIES', 'GIT_CONFIG_COUNT', 'GIT_CONFIG_PARAMETERS'} \
                or key.startswith(('GIT_CONFIG_KEY_', 'GIT_CONFIG_VALUE_')):
            environment.pop(key, None)
    if index is not None:
        environment['GIT_INDEX_FILE'] = str(index)
    environment['GIT_NO_REPLACE_OBJECTS'] = '1'
    result = subprocess.run(['git', '-c', 'gc.auto=0', '-c', 'maintenance.auto=false', '-C', str(root), *args],
                            env=environment, capture_output=True)
    if checked:
        require(result.returncode == 0, 'Git command failed: ' + args[0])
    return result


def text(root, *args, index=None):
    return git(root, *args, index=index).stdout.decode().strip()


def commit_sha(root, value):
    groups.receipt.sha(value, 'commit')
    require(text(root, 'rev-parse', value + '^{commit}') == value, 'commit object differs')
    return value


def path_name(value):
    require(isinstance(value, str) and value and '\\' not in value and '\x00' not in value, 'unsafe owned path')
    path = PurePosixPath(value)
    require(not path.is_absolute() and '..' not in path.parts and path.as_posix() == value
            and '.git' not in path.parts, 'owned path escapes checkout')
    return value


def tree_files(root, tree):
    result = {}
    for raw in git(root, 'ls-tree', '-r', '-z', tree).stdout.split(b'\0'):
        if not raw: continue
        metadata, name = raw.split(b'\t', 1)
        mode, kind, oid = metadata.decode().split()
        name = path_name(name.decode())
        require(kind == 'blob' and mode in ('100644', '100755', '120000'), 'unsupported source tree entry')
        result[name] = {'mode': mode, 'blob': oid}
    require(result, 'empty source tree')
    return result


def source_digest(files):
    return groups.checksum(groups.canonical(files))


def diff_patch(root, before, after):
    return git(root, 'diff', '--no-ext-diff', '--no-textconv', '--binary', '--no-renames',
               '--full-index', before, after, '--').stdout


def apply_tree(root, anchor_tree, raw):
    require(len(raw) <= MAX_PATCH and b'\0' not in raw and b'GIT binary patch' not in raw,
            'oversized or binary variant patch')
    with tempfile.TemporaryDirectory(prefix='ci-variant-index-') as temporary:
        index = Path(temporary) / 'index'
        patch = Path(temporary) / 'change.patch'; patch.write_bytes(raw)
        git(root, 'read-tree', anchor_tree, index=index)
        if raw:
            git(root, 'apply', '--cached', '--whitespace=error', str(patch), index=index)
        tree = text(root, 'write-tree', index=index)
    require(raw == diff_patch(root, anchor_tree, tree), 'patch is not the canonical reviewed anchor diff')
    return tree


def checkout(root, scope, mutate=False):
    root = Path(root).absolute()
    require(root.is_dir() and root.resolve() == root, 'checkout must be a real directory')
    require(Path(text(root, 'rev-parse', '--show-toplevel')).resolve() == root, 'not the actual checkout root')
    require(str(root) == scope['worktree'], 'recipe belongs to another worktree')
    if mutate:
        require(text(root, 'symbolic-ref', '--short', 'HEAD') == scope['branch']
                and scope['branch'].startswith('ws/'), 'not the expected experiment branch')
        actual_git = Path(text(root, 'rev-parse', '--absolute-git-dir')).resolve()
        common = Path(text(root, 'rev-parse', '--path-format=absolute', '--git-common-dir')).resolve()
        require(actual_git != common, 'mutations require an independent linked worktree')
        remote = text(root, 'remote', 'get-url', 'origin')
        allowed = {f'https://github.com/{scope["repository"]}.git', f'https://github.com/{scope["repository"]}',
                   f'git@github.com:{scope["repository"]}.git', f'git@github.com:{scope["repository"]}'}
        require(remote in allowed, 'origin does not match the scoped repository')
    return root


def clean(root):
    require(not git(root, 'status', '--porcelain', '--untracked-files=all').stdout.strip(), 'checkout is not clean')


def prepare(refs, root, request):
    require(type(request.get('schema')) is int and request['schema'] == 1
            and request.get('type') == 'variant-recipe-request', 'unsupported recipe request')
    scope = request['scope']
    require(re.fullmatch(r'[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+', scope['repository'])
            and scope['branch'].startswith('ws/'), 'invalid recipe scope')
    root = checkout(root, scope)
    clean(root)
    anchor = commit_sha(root, request['anchor_commit'])
    require(text(root, 'rev-parse', 'HEAD') == anchor, 'recipe anchor is not the current reviewed C head')
    anchor_tree = text(root, 'rev-parse', anchor + '^{tree}')
    original = tree_files(root, anchor_tree)
    paths = request['owned_paths']
    require(isinstance(paths, list) and paths and len(set(paths)) == len(paths), 'empty or duplicate owned paths')
    owned = {path_name(value) for value in paths}
    require(set(request['patches']) == {'A', 'B', 'C'}, 'recipe must declare A/B/C patches')
    trees, digests = {}, {}
    for variant, reference in request['patches'].items():
        raw, unused = refs.blob(reference)
        tree = apply_tree(root, anchor_tree, raw)
        files = tree_files(root, tree)
        changed = {name for name in original.keys() | files.keys() if original.get(name) != files.get(name)}
        require(changed <= owned, 'patch changes a non-owned path')
        require(all(files.get(name, {}).get('mode') != '120000' for name in changed), 'owned patch creates a symlink')
        trees[variant], digests[variant] = tree, source_digest(files)
    require(trees['C'] == anchor_tree and len(set(trees.values())) == 3,
            'C must restore the anchor and A/B/C must be distinct source trees')
    refs.finish()
    return {'schema': 1, 'type': 'source-variant-recipe', 'scope': scope, 'anchor_commit': anchor,
            'anchor_tree': anchor_tree, 'owned_paths': sorted(owned), 'patch_files': request['patches'],
            'variant_trees': trees, 'source_digests': digests,
            'preserved_source_digest': source_digest({name: value for name, value in original.items() if name not in owned}),
            'behavior_verified': False, 'performance_accepted': False}


def validate_recipe(refs, root, recipe):
    require(type(recipe.get('schema')) is int and recipe['schema'] == 1
            and recipe.get('type') == 'source-variant-recipe', 'unsupported prepared recipe')
    root = checkout(root, recipe['scope'], mutate=True)
    commit_sha(root, recipe['anchor_commit'])
    require(text(root, 'rev-parse', recipe['anchor_commit'] + '^{tree}') == recipe['anchor_tree'], 'anchor tree differs')
    require(set(recipe['variant_trees']) == set(recipe['source_digests']) == {'A', 'B', 'C'}, 'prepared variant set differs')
    require(len(set(recipe['variant_trees'].values())) == 3, 'prepared source trees must be distinct')
    paths = recipe['owned_paths']
    require(isinstance(paths, list) and paths and len(set(paths)) == len(paths), 'empty or duplicate owned paths')
    owned = {path_name(value) for value in paths}
    original = tree_files(root, recipe['anchor_tree'])
    for name, tree in recipe['variant_trees'].items():
        raw, unused = refs.blob(recipe['patch_files'][name])
        require(apply_tree(root, recipe['anchor_tree'], raw) == tree, 'variant tree differs from its reviewed patch')
        groups.receipt.sha(tree, 'variant tree')
        files = tree_files(root, tree)
        require(source_digest(files) == recipe['source_digests'][name], 'prepared source digest differs')
        preserved = {path: entry for path, entry in files.items() if path not in owned}
        require(source_digest(preserved) == recipe['preserved_source_digest'], 'non-owned source changed')
        require(all(files.get(path, {}).get('mode') != '120000' for path in owned), 'owned variant contains a symlink')
    require(recipe['variant_trees']['C'] == recipe['anchor_tree'], 'final C differs from the anchor')
    return root


def archive_run(refs, reference, expected, scope, request):
    archive, receipt_path = refs.read(reference)
    require(receipt_path.name == 'receipt.json' and archive.get('archive_root') == str(receipt_path.parent), 'archive location differs')
    with redirect_stdout(io.StringIO()):
        groups.receipt.verify(type('Args', (), {'directory': str(receipt_path.parent)})())
    def raw(name):
        require(name in archive['files'], 'mandatory raw archive input missing')
        reference = {'path': (receipt_path.parent / name).relative_to(refs.root).as_posix(),
                     'sha256': archive['files'][name]['sha256']}
        data, path = refs.blob(reference)
        return data, path
    current = groups.json_bytes(raw('run.json')[0])
    def associations(value):
        values = value.get('pull_requests', [])
        require(isinstance(values, list), 'invalid archived PR associations')
        for association in values:
            require(isinstance(association, dict) and type(association.get('number')) is int
                    and isinstance(association.get('head'), dict) and isinstance(association.get('base'), dict)
                    and association['number'] == scope['pr'] and association['head'].get('sha') == scope['head_sha']
                    and association['base'].get('sha') == scope['base_sha'], 'archived PR association differs')
        return bool(values)
    root_associated = associations(current)
    require(all(current.get(key) == expected[key] for key in ('id', 'path', 'run_attempt', 'status', 'conclusion'))
            and current['head_sha'] == scope['head_sha'] and current['head_branch'] == scope['branch']
            and current['event'] == 'pull_request' and current['repository']['full_name'] == scope['repository'],
            'archived workflow run differs from current head snapshot')
    require(type(current['run_attempt']) is int and 1 <= current['run_attempt'] <= 100, 'invalid archived attempt count')
    unassociated = []
    for attempt in range(1, current['run_attempt'] + 1):
        prefix = 'attempt-' + str(attempt) + '/'
        observed = groups.json_bytes(raw(prefix + 'run.json')[0])
        actual = request(f'repos/{scope["repository"]}/actions/runs/{current["id"]}/attempts/{attempt}')
        for value in (observed, actual):
            if not associations(value) and attempt not in unassociated: unassociated.append(attempt)
            require(value['id'] == current['id'] and value['run_attempt'] == attempt and value['status'] == 'completed'
                    and value['head_sha'] == scope['head_sha'] and value['head_branch'] == scope['branch']
                    and value['event'] == 'pull_request' and value['repository']['full_name'] == scope['repository'],
                    'old, missing or unfinished archived attempt')
        require(observed['conclusion'] == actual['conclusion'], 'archived attempt conclusion differs')
        pages = sorted((name for name in archive['files'] if re.fullmatch(re.escape(prefix) + r'jobs-[0-9]+\.json', name)),
                       key=lambda name: int(name.rsplit('-', 1)[1].split('.')[0]))
        require(pages and pages == [prefix + f'jobs-{i}.json' for i in range(1, len(pages) + 1)], 'archived job pages are incomplete')
        jobs, total = [], None
        for name in pages:
            value = groups.json_bytes(raw(name)[0])
            require(type(value['total_count']) is int and isinstance(value['jobs'], list)
                    and (total is None or value['total_count'] == total), 'archived job page count differs')
            total = value['total_count']; jobs.extend(value['jobs'])
        require(len(jobs) == total and len({job['id'] for job in jobs}) == total, 'archived jobs missing or duplicated')
        live = groups.page_values(request, f'repos/{scope["repository"]}/actions/runs/{current["id"]}/attempts/{attempt}/jobs', 'jobs')
        fields = ('id', 'run_id', 'run_attempt', 'name', 'status', 'conclusion', 'started_at', 'completed_at', 'steps')
        def projection(values):
            for job in values:
                require(job['run_id'] == current['id'] and job['run_attempt'] == attempt and job['status'] == 'completed',
                        'foreign or unfinished archived job')
            return sorted(({key: job[key] for key in fields} for job in values), key=lambda job: job['id'])
        require(projection(jobs) == projection(live), 'archived job timeline differs from original API')
        name = prefix + 'logs.zip'
        require(name in archive['files'], 'mandatory raw archive input missing')
        log = receipt_path.parent / name
        require(groups.receipt.digest(log) == archive['files'][name]['sha256'], 'raw logs changed')
        groups.receipt.zip_members(log)
    with redirect_stdout(io.StringIO()):
        groups.receipt.verify(type('Args', (), {'directory': str(receipt_path.parent)})())
    return {'run_id': current['id'], 'attempts_read_back': current['run_attempt'], 'receipt': reference,
            'root_PR_association_unknown': not root_associated, 'unassociated_attempts': unassociated}


def transition(refs, value, request):
    require(type(value.get('schema')) is int and value['schema'] == 1
            and value.get('type') == 'serial-transition-request', 'unsupported transition request')
    scope = value['scope']
    before = groups.head_status(scope, value['contract'], request)
    require(before['can_advance_after_archiving'] and before['base_sha'] == value['expected_base_sha'],
            'previous published head is unfinished or its base changed')
    policy_reader, policy, policy_before = None, None, None
    if 'trigger_policy' in value:
        reference = value['trigger_policy']
        require(isinstance(reference, dict) and set(reference) == {'request', 'source_root'}
                and isinstance(reference['source_root'], str), 'invalid transition trigger policy reference')
        policy_reader = groups.module('transition_trigger_contract', '_ci-trigger-contract.py')
        policy_request, unused = refs.read(reference['request'])
        policy = policy_reader.project(refs, reference['source_root'], policy_request)
        require(all(policy['scope'][name] == scope[name] for name in ('repository', 'pr', 'branch', 'head_sha'))
                and policy['scope']['base_sha'] == value['expected_base_sha'], 'transition trigger policy scope differs')
        require(set(policy['ordinary_applicable_workflows']) == set(value['contract']['applicable_workflows']),
                'declared workflow set differs from original trigger policy')
        policy_before = policy_reader.policy_head_status(policy, request)
        require(policy_before['can_advance_after_archiving'], 'original trigger policy is unknown or unfinished')
        require(policy_before['runs'] == before['runs'] and policy_before['checks'] == before['checks'],
                'head instances changed during trigger policy join')
    archives = value['archives']
    require(isinstance(archives, list) and len({entry['run_id'] for entry in archives}) == len(archives)
            and {entry['run_id'] for entry in archives} == {run['id'] for run in before['runs']},
            'workflow archive set differs from the current head')
    indexed = {entry['run_id']: entry['receipt'] for entry in archives}
    readback = [archive_run(refs, indexed[run['id']], run, dict(scope, base_sha=before['base_sha']), request)
                for run in before['runs']]
    frames = value['check_pages']
    require(set(frames) == {scope['head_sha'], before['merge_sha']}, 'head/merge check page set differs')
    checks = []
    for source, references in frames.items():
        require(isinstance(references, list) and references, 'raw check pages are missing')
        records, total = [], None
        for reference in references:
            raw, unused = refs.read(reference)
            require(type(raw['total_count']) is int and isinstance(raw['check_runs'], list)
                    and (total is None or total == raw['total_count']), 'raw check page count differs')
            total = raw['total_count']; records.extend(raw['check_runs'])
        require(len(records) == total and len({check['id'] for check in records}) == total
                and all(check['head_sha'] == source for check in records), 'raw check pages incomplete or foreign')
        for check in records:
            require(all(association['number'] == scope['pr'] and association['head']['sha'] == scope['head_sha']
                        and association['base']['sha'] == before['base_sha'] for association in check.get('pull_requests', [])),
                    'raw check PR association differs')
        checks.extend({key: check[key] for key in ('id', 'name', 'head_sha', 'status', 'conclusion')} for check in records)
    require(len({check['id'] for check in checks}) == len(checks)
            and sorted(checks, key=lambda check: check['id']) == sorted(before['checks'], key=lambda check: check['id']),
            'raw check instances differ from original API')
    after = groups.head_status(scope, value['contract'], request)
    require(before == after, 'published head changed during archive readback')
    if policy is not None:
        require(policy_reader.policy_head_status(policy, request) == policy_before,
                'head trigger policy instances changed during archive readback')
    refs.finish()
    result = {'schema': 1, 'type': 'serial-transition-readback', 'snapshot': after, 'archives': readback,
            'archive_readback_verified': True, 'first_complete_pr_sample_proven': False,
            'performance_accepted': False}
    if policy_before is not None:
        result['source_trigger_policy'] = policy_before
    return result


def reserve_journal(directory, plan):
    directory = Path(directory).absolute()
    require(not directory.exists() and not directory.is_symlink(), 'journal already exists')
    require(directory.parent.is_dir() and directory.parent.resolve() == directory.parent, 'journal parent must be a real directory')
    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
    descriptor = os.open(directory.anchor, flags)
    try:
        for part in directory.parent.parts[1:]:
            child = os.open(part, flags, dir_fd=descriptor)
            os.close(descriptor); descriptor = child
        os.mkdir(directory.name, mode=0o700, dir_fd=descriptor)
    finally:
        os.close(descriptor)
    groups.write_new(directory / 'planned.json', plan)
    return directory


def operation(refs, root, manifest, variant, journal, request, unpublished=None):
    recipe, unused = refs.read(manifest['recipe'])
    root = validate_recipe(refs, root, recipe)
    clean(root)
    before = commit_sha(root, manifest['from_head'])
    require(text(root, 'rev-parse', 'HEAD') == before, 'current local head differs')
    current_tree = text(root, 'rev-parse', 'HEAD^{tree}')
    require(current_tree in recipe['variant_trees'].values(), 'current source is not a reviewed variant')
    target_tree = recipe['variant_trees'][variant]
    require(target_tree != current_tree, 'source already matches; do not manufacture an empty event')
    value, unused = refs.read(manifest['transition'])
    require(all(value['scope'][key] == recipe['scope'][key] for key in ('repository', 'branch')), 'transition belongs to another recipe scope')
    if unpublished is None:
        require(value['scope']['head_sha'] == before, 'local head is not the previous published head')
    else:
        require(value['scope']['head_sha'] == unpublished['before_head'] and before == unpublished['after_head']
                and text(root, 'rev-list', '--parents', '-n', '1', before).split() == [before, unpublished['before_head']],
                'local recovery is not the recorded unpublished commit')
    readback = transition(refs, value, request)
    clean(root)
    require(text(root, 'rev-parse', 'HEAD') == before, 'local head changed during transition readback')
    patch = diff_patch(root, current_tree, target_tree)
    require(len(patch) <= MAX_PATCH and b'GIT binary patch' not in patch, 'unsupported transition patch')
    changed = {path_name(name) for name in git(root, 'diff', '--name-only', '-z', current_tree, target_tree).stdout.decode().split('\0') if name}
    require(changed and changed <= set(recipe['owned_paths']), 'transition changes non-owned source')
    directory = Path(journal).absolute()
    common = Path(text(root, 'rev-parse', '--path-format=absolute', '--git-common-dir')).resolve()
    worktrees = [Path(entry.decode()[len('worktree '):]).resolve()
                 for entry in git(root, 'worktree', 'list', '--porcelain', '-z').stdout.split(b'\0')
                 if entry.startswith(b'worktree ')]
    require(not directory.is_relative_to(common) and all(not directory.is_relative_to(path) for path in worktrees),
            'journal must be outside every product worktree and Git source')
    plan = {'schema': 1, 'type': 'variant-operation', 'scope': recipe['scope'], 'recipe': manifest['recipe'],
            'before_head': before, 'before_tree': current_tree, 'target_tree': target_tree, 'variant': variant,
            'patch_sha256': groups.checksum(patch), 'readback': readback, 'input_files': refs.finish(),
            'request_snapshot': manifest, 'request_sha256': groups.checksum(groups.canonical(manifest))}
    directory = reserve_journal(directory, plan)
    patch_file = directory / 'forward.patch'
    with patch_file.open('xb') as stream: stream.write(patch)
    state = {'schema': 1, 'type': 'variant-operation-result', 'before_head': before, 'phase': 'planned',
             'journal': str(directory), 'completed': False, 'source_recovered': False,
             'behavior_verified': False, 'performance_accepted': False}
    try:
        git(root, 'apply', '--check', '--index', '--whitespace=error', str(patch_file))
        git(root, 'apply', '--index', '--whitespace=error', str(patch_file))
        state['phase'] = 'staged'
        require(text(root, 'write-tree') == target_tree and not git(root, 'diff', '--name-only').stdout,
                'staged source changed before commit')
        groups.write_new(directory / 'staged.json', {'target_tree': target_tree, 'before_head': before})
        require(text(root, 'rev-parse', 'HEAD') == before, 'head changed before ordinary commit')
        message = 'Restore reviewed candidate C source' if variant == 'C' else 'Select reviewed CI variant ' + variant
        git(root, 'commit', '-m', message)
        after = text(root, 'rev-parse', 'HEAD'); state.update(after_head=after, phase='committed')
        require(text(root, 'rev-list', '--parents', '-n', '1', after).split() == [after, before]
                and text(root, 'rev-parse', after + '^{tree}') == target_tree, 'ordinary commit source or parent differs')
        clean(root)
        state.update(completed=True, source_recovered=variant == 'C', source_digest=recipe['source_digests'][variant])
        return state
    finally:
        state['observed_head'] = text(root, 'rev-parse', 'HEAD')
        groups.write_new(directory / 'result.json', state)


def rollback(refs, root, manifest):
    recipe, unused = refs.read(manifest['recipe'])
    root = validate_recipe(refs, root, recipe)
    planned, planned_path = refs.read(manifest['planned'])
    require(planned['type'] == 'variant-operation' and planned['scope'] == recipe['scope']
            and planned['recipe'] == manifest['recipe'], 'recovery journal belongs to another recipe')
    require(planned['variant'] in recipe['variant_trees']
            and planned['target_tree'] == recipe['variant_trees'][planned['variant']]
            and text(root, 'rev-parse', planned['before_head'] + '^{tree}') == planned['before_tree']
            and planned['before_tree'] in recipe['variant_trees'].values(), 'recovery journal source differs')
    require(text(root, 'rev-parse', 'HEAD') == planned['before_head'], 'journal head already advanced')
    require(text(root, 'write-tree') == planned['target_tree'] and not git(root, 'diff', '--name-only').stdout
            and not git(root, 'ls-files', '--others', '--exclude-standard').stdout, 'interrupted source contains unrecorded edits')
    patch = planned_path.parent / 'forward.patch'
    require(patch.is_file() and not patch.is_symlink() and groups.checksum(patch.read_bytes()) == planned['patch_sha256'], 'journal patch changed')
    require(patch.read_bytes() == diff_patch(root, planned['before_tree'], planned['target_tree']), 'journal patch is not the reviewed source change')
    refs.finish()
    git(root, 'apply', '--check', '--index', '--reverse', '--whitespace=error', str(patch))
    git(root, 'apply', '--index', '--reverse', '--whitespace=error', str(patch))
    require(text(root, 'write-tree') == planned['before_tree'] and text(root, 'rev-parse', 'HEAD') == planned['before_head'],
            'rollback source differs from recorded preimage')
    clean(root)
    return {'schema': 1, 'type': 'uncommitted-owned-patch-rollback', 'head': planned['before_head'],
            'tree': planned['before_tree'], 'ordinary_commit_created': False, 'behavior_verified': False,
            'performance_accepted': False}


def restore_local(refs, root, value, journal, request):
    recipe, unused = refs.read(value['recipe'])
    previous, unused = refs.read(value['previous_result'])
    planned, unused = refs.read(value['previous_plan'])
    require(previous.get('completed') is True and previous['phase'] == 'committed'
            and previous['before_head'] == planned['before_head'] and previous.get('after_head') == value['from_head']
            and previous.get('observed_head') == previous.get('after_head'), 'local recovery has no verified ordinary commit')
    require(planned['scope'] == recipe['scope'] and planned['recipe'] == value['recipe']
            and planned['target_tree'] == text(Path(root), 'rev-parse', value['from_head'] + '^{tree}'),
            'local recovery journal scope or source differs')
    return operation(refs, root, value, 'C', journal, request, unpublished=previous)


def verify_source(refs, root, manifest, variant):
    recipe, unused = refs.read(manifest['recipe'])
    root = validate_recipe(refs, root, recipe); clean(root)
    tree = text(root, 'rev-parse', 'HEAD^{tree}')
    require(tree == recipe['variant_trees'][variant], 'source is not the selected reviewed variant')
    refs.finish()
    return {'schema': 1, 'type': 'source-variant-verification', 'variant': variant,
            'head': text(root, 'rev-parse', 'HEAD'), 'tree': tree, 'source_digest': recipe['source_digests'][variant],
            'source_matches_recipe': True, 'behavior_verified': False, 'performance_accepted': False}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='command', required=True)
    for name in ('prepare', 'verify-transition', 'switch', 'restore-local', 'rollback', 'verify-source'):
        p = commands.add_parser(name)
        p.add_argument('--manifest', required=True); p.add_argument('--evidence-root', required=True)
        if name != 'verify-transition': p.add_argument('--root', required=True)
        if name in ('switch', 'restore-local'):
            p.add_argument('--journal', required=True)
        else: p.add_argument('--output', required=True)
        if name in ('switch', 'verify-source'): p.add_argument('--variant', choices=['A', 'B', 'C'], required=True)
    args = parser.parse_args()
    refs = groups.References(args.evidence_root)
    manifest = Path(args.manifest); raw = groups.file_bytes(manifest); value = groups.json_bytes(raw)
    if args.command == 'prepare': result = prepare(refs, args.root, value)
    elif args.command == 'verify-transition': result = transition(refs, value, groups.receipt.api)
    elif args.command == 'switch': result = operation(refs, args.root, value, args.variant, args.journal, groups.receipt.api)
    elif args.command == 'restore-local':
        result = restore_local(refs, args.root, value, args.journal, groups.receipt.api)
    elif args.command == 'rollback': result = rollback(refs, args.root, value)
    else: result = verify_source(refs, args.root, value, args.variant)
    require(groups.checksum(manifest.read_bytes()) == groups.checksum(raw), 'manifest changed during operation')
    if args.command not in ('switch', 'restore-local'):
        result['input_files'] = refs.finish(); result['manifest_sha256'] = groups.checksum(raw)
        groups.write_new(Path(args.output), result)
    print(json.dumps(result, sort_keys=True))


if __name__ == '__main__':
    try: main()
    except (ValueError, KeyError, TypeError, OSError) as error:
        print('ci-variants: ' + str(error), file=sys.stderr); sys.exit(1)
