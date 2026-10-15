#!/usr/bin/env python3
"""Original/addition partition controls over independent original Git/ZIP chains."""
import copy
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
from unittest.mock import patch

sys.dont_write_bytecode = True


def module(name, filename):
    spec = importlib.util.spec_from_file_location(name, Path(__file__).with_name(filename))
    value = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(value)
    return value


m = module('case_partition', '_ci-case-partition.py')
case_fixture = module('partition_fixture', '_ci-archive-cases-smoke.py')
fixture, g = case_fixture.fixture, m.g
count = 0


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


def rebase_reference(reference, local, root, rewritten):
    """Rebase request graph paths; preserve original receipt and ZIP bytes."""
    key = (reference['path'], reference['sha256'])
    if key in rewritten:
        return rewritten[key]
    raw, path = g.References(local).blob(reference)

    def transform(value):
        if isinstance(value, dict):
            if set(value) == {'path', 'sha256'}:
                return rebase_reference(value, local, root, rewritten)
            return {name: transform(entry) for name, entry in value.items()}
        if isinstance(value, list):
            return [transform(entry) for entry in value]
        return value

    if path.suffix == '.json':
        original = g.json_bytes(raw)
        changed = transform(original)
        if g.canonical(changed) != g.canonical(original):
            raw = fixture.encode(changed)
            path.write_bytes(raw)
    result = {'path': path.relative_to(root).as_posix(), 'sha256': g.checksum(raw)}
    rewritten[key] = result
    return result


def pure_controls(inventories):
    original = {name: copy.deepcopy(inventories['A']) for name in 'ABC'}
    added = dict(original['A']['cases'][0], parameter='additional parameter')
    original['B']['cases'].append(added)
    original['C']['cases'].append(added)
    result = m.partition(original)
    check(result['original_cases'] == inventories['A']['cases'], 'A defines the original identities')
    check(result['additions'] == {'B': [added], 'C': [added]}, 'candidate source differences define additions')
    for name in ('A', 'B', 'C'):
        changed = copy.deepcopy(original)
        changed[name]['cases'].append(changed[name]['cases'][0])
        rejected(lambda: m.partition(changed), 'duplicate ' + name + ' identity')
    for name in ('B', 'C'):
        changed = copy.deepcopy(original)
        changed[name]['cases'].pop(0)
        rejected(lambda: m.partition(changed), 'missing original ' + name + ' identity')
        changed = copy.deepcopy(original)
        changed[name]['methods'].clear()
        rejected(lambda: m.partition(changed), 'missing original ' + name + ' method')
    changed = copy.deepcopy(original)
    changed['A']['cases'].clear()
    rejected(lambda: m.partition(changed), 'empty original inventory')


def prepare_request(root, contexts, customize=None):
    request = {'schema': 1, 'type': 'archived-original-case-partition-request', 'variants': {}}
    inventories = {}
    original_archive_hashes = {}
    for name, context in contexts.items():
        if customize:
            customize(name, context, None)
        case_request, inventories[name] = case_fixture.prepare_cases(context)
        if customize:
            customize(name, context, case_request)
        reference = context['save']('partition-cases-request.json', case_request)
        for path in (context['evidence'] / 'run').rglob('*'):
            if path.is_file():
                original_archive_hashes[path] = g.checksum(path.read_bytes())
        reference = rebase_reference(reference, context['evidence'], root, {})
        request['variants'][name] = {'archive_cases': reference, 'source_root': str(context['work'])}
    return request, inventories, original_archive_hashes


def controls(root, contexts, complete, added):
    request, inventories, original_archive_hashes = prepare_request(root, contexts)
    answer = m.project(g.References(root), request)
    check(answer['original_case_partition_verified'] is complete, 'only fully parsed source authorizes a partition')
    check(answer['known_static_subset_partitioned'] is complete, 'partial inventories never claim a partition')
    check(all(g.checksum(path.read_bytes()) == digest for path, digest in original_archive_hashes.items()),
          'request rebasing preserves every original archive byte')
    check(not answer['original_assertions_verified'] and not answer['normalized_original_inputs_verified']
          and not answer['whole_build_provenance_complete'] and not answer['first_complete_PR_sample']
          and not answer['performance_accepted'], 'identity partition does not claim assertions or acceptance')
    if not complete:
        check(answer['original_cases'] is None and answer['additions'] is None, 'unresolved source has no authoritative partition')
        check(all(answer['unresolved'].values()) and all(answer['known_source_cases'].values()),
              'known and unresolved source identities both survive')
        return
    check(len(answer['original_cases']) == 1 and all(len(cases) == int(added) for cases in answer['additions'].values()),
          'three independently replayed variants retain originals and derive additions')
    if added:
        check(all(cases[0]['method'] == 'added' for cases in answer['additions'].values()),
              'added method identity comes from actual candidate source and original XML')
    pure_controls(inventories)
    changed = copy.deepcopy(request)
    changed['variants']['B'] = changed['variants']['A']
    rejected(lambda: m.project(g.References(root), changed), 'reused original as candidate')
    changed = copy.deepcopy(request)
    changed['variants']['A']['source_root'] = str(contexts['B']['work'])
    rejected(lambda: m.project(g.References(root), changed), 'foreign source root')
    changed = copy.deepcopy(request)
    changed['variants']['A']['additions'] = []
    rejected(lambda: m.project(g.References(root), changed), 'caller supplied additions')
    changed = copy.deepcopy(request)
    changed['schema'] = True
    rejected(lambda: m.project(g.References(root), changed), 'boolean schema')
    manifest, output = root / 'request.json', root / 'partition.json'
    manifest.write_bytes(fixture.encode(request))
    command = ['bash', str(Path(__file__).with_name('ci-case-partition.sh')), '--manifest', str(manifest),
               '--evidence-root', str(root), '--output', str(output)]
    result = subprocess.run(command, capture_output=True, text=True)
    check(result.returncode == 0, 'actual partition CLI succeeds: ' + result.stderr)
    check(json.loads(output.read_bytes()) == answer, 'CLI independently reproduces all archived facts')
    raw = output.read_bytes()
    check(subprocess.run(command, capture_output=True).returncode != 0 and output.read_bytes() == raw,
          'exclusive output preserves prior evidence')


def cohort(source_case=None, complete=True, added=False, callback=controls, dependency_metadata=False, test_jar=False, resolved_artifacts=False):
    with tempfile.TemporaryDirectory(prefix='ci-case-partition-') as temporary:
        root, contexts = Path(temporary).resolve(), {}

        def enter(index):
            if index == 3:
                callback(root, contexts, complete, added)
                return
            name = 'ABC'[index]

            def accept_context(context):
                contexts[name] = context
                enter(index + 1)

            options = {} if source_case is None else {'source_case': source_case}
            if added:
                original = 'package io.tapstate.fixture; class SampleTest { @Test void holds() {} }\n'
                candidate = original.replace('void holds() {}', 'void holds() {} @Test void added() {}')
                options['case_variants'] = {'A': original, 'B': candidate, 'C': candidate}
            fixture.run(accept_context, only_callback=True, variant=name, run_id=40 + index,
                        temporary_parent=root, dependency_metadata=dependency_metadata, test_jar=test_jar, resolved_artifacts=resolved_artifacts, **options)

        # Independent Git fixtures must share the same original base commit.
        with patch.dict(os.environ, GIT_AUTHOR_DATE='2026-10-14T00:00:00+00:00',
                        GIT_COMMITTER_DATE='2026-10-14T00:00:00+00:00'):
            enter(0)


if __name__ == '__main__':
    cohort()
    cohort(added=True)
    cohort('''package io.tapstate.fixture;
class SampleTest {
 @Test void holds() {}
 @ParameterizedTest @MethodSource("computed") void dynamic(int value) {}
 static Stream<Integer> computed() { return external(); }
}
''', complete=False)
    print('CI archived original case partition controls passed:', count)
