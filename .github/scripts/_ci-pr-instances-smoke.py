#!/usr/bin/env python3
"""Offline identity, multi-attempt, same-name failure and original archive controls."""
import copy
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
from urllib.parse import parse_qs, urlsplit

sys.dont_write_bytecode = True


def module(name, file):
    spec = importlib.util.spec_from_file_location(name, Path(__file__).with_name(file))
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


m = module('instances', '_ci-pr-instances.py')
fixture = module('instances_fixture', '_ci-archive-inputs-smoke.py')
count = 0


def check(value, text):
    global count
    assert value, text
    count += 1


def rejected(call, text):
    try:
        call()
    except (ValueError, OSError, KeyError, TypeError) as error:
        check(text in str(error), 'unexpected rejection: ' + str(error))
    else:
        raise AssertionError('expected rejection: ' + text)


def data(anchor):
    scope = anchor['scope']
    association = {'number': scope['pr'], 'head': {'sha': scope['head_sha']}, 'base': {'sha': scope['base_sha']}}
    runs = []
    for identity, event, conclusion, attempts, associations in [
            (42, 'pull_request', 'success', 1, [association]),
            (43, 'pull_request', 'failure', 1, [association]),
            (44, 'pull_request', None, 2, []),
            (45, 'pull_request_target', 'success', 1, [association]),
            (46, 'pull_request_target', 'failure', 1, [{**association, 'number': 8}])]:
        runs.append({'id': identity, 'workflow_id': 900, 'check_suite_id': identity * 10 + attempts,
                     'run_attempt': attempts, 'event': event, 'head_sha': scope['head_sha'] if event == 'pull_request' else scope['base_sha'],
                     'path': '.github/workflows/ci.yml', 'repository': {'full_name': scope['repository']},
                     'pull_requests': copy.deepcopy(associations), 'status': 'completed' if conclusion else 'queued', 'conclusion': conclusion,
                     'created_at': '2026-10-14T10:00:00Z', 'updated_at': '2026-10-14T10:01:00Z'})
    endpoints, check_rows = {}, {sha: [] for sha in (scope['head_sha'], scope['base_sha'], anchor['checkout']['sha'])}
    for run in runs:
        base = 'repos/' + scope['repository'] + '/actions/runs/' + str(run['id'])
        if run['id'] == 46:
            continue
        endpoints[base] = run
        for attempt in range(1, run['run_attempt'] + 1):
            item = {**run, 'run_attempt': attempt, 'check_suite_id': run['id'] * 10 + attempt}
            endpoints[base + f'/attempts/{attempt}'] = item
            identity = run['id'] * 100 + attempt
            endpoints[base + f'/attempts/{attempt}/jobs'] = {'total_count': 1, 'jobs': [
                {'id': identity, 'run_id': run['id'], 'run_attempt': attempt, 'name': 'build',
                 'status': run['status'], 'conclusion': run['conclusion'],
                 'check_run_url': 'https://api.github.com/repos/' + scope['repository'] + '/check-runs/' + str(identity)}]}
            sha = scope['base_sha'] if run['event'] == 'pull_request_target' else anchor['checkout']['sha']
            check_rows[sha].append({'id': identity, 'name': 'build', 'head_sha': sha, 'status': run['status'],
                                    'conclusion': run['conclusion'], 'check_suite': {'id': item['check_suite_id']},
                                    'app': {'id': 15368}, 'pull_requests': run['pull_requests']})

    def request(endpoint):
        parts = urlsplit(endpoint)
        query = parse_qs(parts.query)
        require = m.require
        if parts.path.endswith('/actions/runs'):
            event = query['event'][0]
            if event == 'pull_request':
                require(query['head_sha'] == [scope['head_sha']], 'ordinary query must freeze head')
            else:
                require('head_sha' not in query and query.get('created'), 'target query must not filter by ordinary PR head')
            selected = [row for row in runs if row['event'] == event]
            return {'total_count': len(selected), 'workflow_runs': copy.deepcopy(selected)}
        if '/check-runs' in parts.path:
            require(query.get('filter') == ['all'], 'check queries must retain all same-name instances')
            sha = parts.path.split('/')[-2]
            return {'total_count': len(check_rows[sha]), 'check_runs': copy.deepcopy(check_rows[sha])}
        return copy.deepcopy(endpoints[parts.path])

    return request, runs, endpoints, check_rows


def controls(f):
    event = copy.deepcopy(f['event'])
    event.update(event_payload_complete=True, pull_request={
        'number': 7, 'action': 'synchronize', 'head_sha': f['anchor'], 'base_sha': f['base'], 'head_ref': 'ws/fixture',
        'base_ref': 'main', 'head_repository': 'example/project', 'base_repository': 'example/project'})
    f['save']('run/event-inputs.json', event)
    run = json.loads((f['evidence'] / 'run/run.json').read_text())
    run['created_at'] = '2026-10-14T10:00:00Z'
    f['save']('run/run.json', run)
    record = next(item for item in f['records'] if item['name'] == 'ci-shard-plan')
    f['artifacts']['ci-shard-plan']['ci-event-inputs.json'] = event
    f['save']('run/artifacts/' + str(record['id']) + '-ci-shard-plan.zip', fixture.zipped(f['artifacts']['ci-shard-plan']))
    f['source_request']['archive'] = f['seal']()
    source_ref = f['save']('instance-source.json', f['source_request'])
    request = {'schema': 1, 'type': 'pr-instance-request', 'source': source_ref,
               'target_created': {'start': '2026-10-14T09:00:00Z', 'end': '2026-10-14T11:00:00Z'}}
    original = m.anchor(m.g.References(f['evidence']), f['work'], request)
    check(original['original_action'] == 'synchronize', 'original action comes from indexed event artifact')
    check(original['original_labels']['snapshot'] == 'unknown', 'legacy missing labels remain unknown')
    bad = copy.deepcopy(request)
    bad['target_created']['end'] = '2026-10-15T11:00:00Z'
    rejected(lambda: m.anchor(m.g.References(f['evidence']), f['work'], bad), 'bounded to one day')
    bad['target_created'] = {'start': '2026-10-14T08:00:00Z', 'end': '2026-10-14T09:00:00Z'}
    rejected(lambda: m.anchor(m.g.References(f['evidence']), f['work'], bad), 'excludes original CI')
    api, runs, endpoints, checks = data(original)
    files = {}
    m.capture(original, api, lambda name, value: files.__setitem__(name, copy.deepcopy(value)))
    answer = m.summarize(original, files.__getitem__)
    check(len(answer['runs']) == 5 and len(answer['jobs']) == len(answer['checks']) == 5, 'all candidates, attempts, jobs and checks retained')
    check([item['conclusion'] for item in answer['checks']].count('failure') == 1, 'same-name green never hides another run failure')
    check(all(item['name'] == 'build' for item in answer['checks']), 'same-name instances stay distinct by ID')
    check(any(item['run_attempt'] == 2 for item in answer['jobs']), 'attempt2 preserved separately, never called first sample')
    check(next(item for item in answer['runs'] if item['run_id'] == 46)['association'] == 'foreign-pr', 'foreign target query record retained without fetching its details')
    check(all(item['original_event_membership'] == 'unknown' for item in answer['runs']), 'PR refs never prove same original action membership')
    check(all(len(item['job_instances']) == 1 for item in answer['checks']), 'job URL joins exact suite and run attempt IDs')
    check(not answer['R_G_complete'] and not answer['first_complete_PR_sample'] and not answer['performance_accepted'], 'candidate archive never certifies R/G or performance')
    modified = copy.deepcopy(files)
    modified['runs/44/attempt-1/jobs-1.json']['jobs'][0]['run_attempt'] = 2
    rejected(lambda: m.summarize(original, modified.__getitem__), 'archived job binding differs')
    modified = copy.deepcopy(files)
    stem = 'checks/' + original['checkout']['sha'] + '-1.json'
    modified[stem]['check_runs'][0]['head_sha'] = '0' * 40
    rejected(lambda: m.summarize(original, modified.__getitem__), 'archived check source differs')
    modified = copy.deepcopy(files)
    modified['runs/42/attempt-1/jobs-1.json']['jobs'][0]['check_run_url'] = 'https://api.github.com/repos/example/project/check-runs/4401'
    rejected(lambda: m.summarize(original, modified.__getitem__), 'run attempt suite disagree')
    modified = copy.deepcopy(files)
    modified['after/pull_request/runs-1.json']['workflow_runs'].pop()
    modified['after/pull_request/runs-1.json']['total_count'] -= 1
    rejected(lambda: m.summarize(original, modified.__getitem__), 'candidate sets differ')
    modified = copy.deepcopy(files)
    modified['checks/' + original['scope']['base_sha'] + '-1.json']['check_runs'].append(modified[stem]['check_runs'][0])
    modified['checks/' + original['scope']['base_sha'] + '-1.json']['total_count'] += 1
    rejected(lambda: m.summarize(original, modified.__getitem__), 'check source differs')
    modified = copy.deepcopy(files)
    modified['before/pull_request/runs-1.json']['workflow_runs'].append(copy.deepcopy(runs[0]))
    modified['before/pull_request/runs-1.json']['total_count'] += 1
    rejected(lambda: m.summarize(original, modified.__getitem__), 'duplicate archived API identity')
    def rows_api(value):
        return lambda endpoint: copy.deepcopy(value)
    rejected(lambda: m.query_rows(rows_api({'total_count': 1000, 'workflow_runs': []}), 'runs', 'workflow_runs', lambda *args: None, 'test'), 'search result cap')
    rejected(lambda: m.query_rows(rows_api({'total_count': 1, 'jobs': []}), 'jobs', 'jobs', lambda *args: None, 'test'), 'incomplete API pagination')
    rejected(lambda: m.query_rows(rows_api({'total_count': 2, 'jobs': [{'id': 1}, {'id': 1}]}), 'jobs', 'jobs', lambda *args: None, 'test'), 'duplicate API identity')
    responses = iter([{'total_count': 2, 'jobs': [{'id': 1}]}, {'total_count': 3, 'jobs': [{'id': 2}]}])
    rejected(lambda: m.query_rows(lambda endpoint: next(responses), 'jobs', 'jobs', lambda *args: None, 'test'), 'API totals changed')
    responses = iter([{'total_count': 2, 'jobs': [{'id': 1}]}, {'total_count': 2, 'jobs': [{'id': 2}]}])
    short_pages = {}
    check(len(m.query_rows(lambda endpoint: next(responses), 'jobs', 'jobs', short_pages.__setitem__, 'short')) == 2,
          'short API pages are counted by records, not assumed full pages')
    saved = runs[0]['head_sha']
    runs[0]['head_sha'] = '0' * 40
    rejected(lambda: m.capture(original, api, lambda *args: None), 'foreign ordinary PR head')
    runs[0]['head_sha'] = saved
    saved = runs[3]['created_at']
    runs[3]['created_at'] = '2026-10-14T12:00:00Z'
    rejected(lambda: m.capture(original, api, lambda *args: None), 'target run outside observation window')
    runs[3]['created_at'] = saved
    removed = runs.pop(0)
    rejected(lambda: m.capture(original, api, lambda *args: None), 'original CI anchor absent')
    runs.insert(0, removed)
    key = 'repos/example/project/actions/runs/44/attempts/1'
    endpoints[key]['run_attempt'] = 2
    rejected(lambda: m.capture(original, api, lambda *args: None), 'attempt run binding differs')
    endpoints[key]['run_attempt'] = 1
    modified = copy.deepcopy(files)
    modified['runs/44/attempt-2/run.json']['check_suite_id'] = 9999
    stem = 'checks/' + original['checkout']['sha'] + '-1.json'
    for item in modified[stem]['check_runs']:
        if item['id'] == 4402:
            item['check_suite']['id'] = 9999
    rejected(lambda: m.summarize(original, modified.__getitem__), 'latest attempt check suite differs')
    key = 'repos/example/project/actions/runs/44/attempts/2'
    endpoints[key]['check_suite_id'] = 9999
    rejected(lambda: m.capture(original, api, lambda *args: None), 'latest attempt check suite differs')
    endpoints[key]['check_suite_id'] = 442
    from unittest.mock import patch
    manifest = f['evidence'] / 'unsafe-output-request.json'
    manifest.write_bytes(fixture.encode(request))
    outside = f['work'].parent / 'outside-component'
    outside.mkdir()
    unsafe = outside / '..' / f['work'].name / 'candidate-archive'
    argv = ['ci-pr-instances', 'collect', '--manifest', str(manifest), '--evidence-root', str(f['evidence']),
        '--root', str(f['work']), '--output', str(unsafe)]
    with patch.object(sys, 'argv', argv), patch.object(m.r, 'api', api):
        rejected(m.main, 'collection output path must be canonical')
    check(not (f['work'] / 'candidate-archive').exists(), 'unsafe collection never writes product source')
    rejected(lambda: m.collection_output('/private/escape-component/../tmp/instance-output', f['work']), 'path must be canonical')
    rejected(lambda: m.collection_output('/private/tmp/instance-output', f['work']), 'persistent archive outside product')
    rejected(lambda: m.collection_output(f['work'] / 'candidate-archive', f['work']), 'persistent archive outside product')
    check(m.collection_output('/private/allowed-persistent-fixture/instance-output', f['work']) == Path('/private/allowed-persistent-fixture/instance-output'),
          'canonical persistent boundary check is read-only')
    for name, value in files.items():
        f['save']('instances/' + name, value)
    f['save']('instances/anchor.json', original)
    f['save']('instances/observation.json', answer)
    directory = f['evidence'] / 'instances'
    m.r.manifest(directory, {'type': 'PR-instance-candidate-archive', 'archive_complete': True})
    reference = f['save']('instances-reference.json', f['save']('instances/receipt.json', (directory / 'receipt.json').read_bytes()))
    manifest = f['evidence'] / 'instances-request.json'
    manifest.write_bytes(fixture.encode(request))
    output = f['evidence'] / 'instance-replay.json'
    command = ['bash', str(Path(__file__).with_name('ci-pr-instances.sh')), 'project', '--manifest', str(manifest),
               '--archive', str(f['evidence'] / reference['path']), '--evidence-root', str(f['evidence']),
               '--root', str(f['work']), '--output', str(output)]
    child = subprocess.run(command, capture_output=True, text=True)
    check(child.returncode == 0 and json.loads(output.read_bytes())['raw_JSON_readback_verified'], 'independent CLI original archive replay: ' + child.stderr)
    before = output.read_bytes()
    check(subprocess.run(command, capture_output=True).returncode != 0 and output.read_bytes() == before, 'replay output is exclusive')
    (directory / 'runs/42/run.json').write_text('{}')
    command[-1] = str(f['evidence'] / 'damaged-replay.json')
    child = subprocess.run(command, capture_output=True, text=True)
    check(child.returncode != 0 and 'file set or hash differs' in child.stderr, 'damaged durable raw record rejects')
    f['save']('instances/runs/42/run.json', files['runs/42/run.json'])
    f['save']('instances/observation.json', {**answer, 'R_G_complete': True})
    m.r.manifest(directory, {'type': 'PR-instance-candidate-archive', 'archive_complete': True})
    f['save']('instances-reference.json', f['save']('instances/receipt.json', (directory / 'receipt.json').read_bytes()))
    child = subprocess.run(command, capture_output=True, text=True)
    check(child.returncode != 0 and 'independent replay' in child.stderr, 'resealed fabricated applicability cannot replace raw instance replay')


fixture.run(controls, only_callback=True)
print('pr-instance controls passed: ' + str(count))
