#!/usr/bin/env python3
"""Offline native log/artifact identity, byte-retention and independent replay controls."""
import copy
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
from urllib.parse import urlsplit

sys.dont_write_bytecode = True


def module(name, filename):
    spec = importlib.util.spec_from_file_location(name, Path(__file__).with_name(filename))
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


m = module('native_instances', '_ci-instance-evidence.py')
shared = module('native_candidate_fixture', '_ci-pr-instances-smoke.py')
count = 0


def check(value, message):
    global count
    assert value, message
    count += 1


def rejected(call, text):
    try:
        call()
    except (ValueError, OSError, KeyError, TypeError) as error:
        check(text in str(error), 'wrong rejection: ' + str(error))
    else:
        raise AssertionError('expected rejection: ' + text)


def controls(f):
    source_request = shared.prepare_anchor(f)
    anchor = m.instances.anchor(m.g.References(f['evidence']), f['work'], source_request)
    api, runs, endpoints, check_rows = shared.data(anchor)
    for run in runs:
        run.update(status='completed', conclusion=run['conclusion'] or 'failure', head_branch='ws/fixture' if run['event'] == 'pull_request' else 'main')
    for key, value in endpoints.items():
        if 'jobs' in value:
            for job in value['jobs']:
                job.update(status='completed', conclusion=job['conclusion'] or 'failure')
        else:
            value.update(status='completed', conclusion=value['conclusion'] or 'failure', head_branch='ws/fixture' if value['event'] == 'pull_request' else 'main')
    for rows in check_rows.values():
        for row in rows:
            row.update(status='completed', conclusion=row['conclusion'] or 'failure')
    candidate_files = {}
    m.instances.capture(anchor, api, lambda name, value: candidate_files.__setitem__(name, copy.deepcopy(value)))
    candidates = m.instances.summarize(anchor, candidate_files.__getitem__)
    candidate_record = candidate_files.__getitem__
    prefix = 'repos/example/project'
    event = json.loads((f['evidence'] / 'run/event-inputs.json').read_bytes())
    artifacts, byte_endpoints = {}, {}
    for run in runs:
        if run['id'] == 46:
            continue
        artifacts[run['id']] = []
        for attempt in range(1, run['run_attempt'] + 1):
            byte_endpoints[prefix + f'/actions/runs/{run["id"]}/attempts/{attempt}/logs'] = shared.fixture.zipped(
                {'job.log': ('native outcome ' + str(run['conclusion']) + ' attempt ' + str(attempt)).encode()})
    for identity, run_id, name, content in [(7001, 42, 'ci-shard-plan', {'ci-event-inputs.json': event}),
                                           (7002, 44, 'case-results', {'native.txt': b'first attempt failed'}),
                                           (7003, 44, 'case-results', {'native.txt': b'last attempt failed'})]:
        run = next(row for row in runs if row['id'] == run_id)
        artifacts[run_id].append({'id': identity, 'name': name, 'expired': False,
                                  'workflow_run': {'id': run_id, 'head_sha': run['head_sha'], 'head_branch': run['head_branch']}})
        byte_endpoints[prefix + f'/actions/artifacts/{identity}/zip'] = shared.fixture.zipped(content)

    def native_api(endpoint):
        path = urlsplit(endpoint).path
        if path.endswith('/artifacts'):
            identity = int(path.split('/')[-2])
            return {'total_count': len(artifacts[identity]), 'artifacts': copy.deepcopy(artifacts[identity])}
        return api(endpoint)

    files = {}
    m.capture(candidates, candidate_record, native_api, byte_endpoints.__getitem__,
              lambda name, value: files.__setitem__(name, shared.fixture.encode(value)), files.__setitem__)
    answer = m.replay(candidates, candidate_record, event, files.__getitem__, set(files))
    check(answer['raw_snapshot_complete'] and len(answer['runs']) == 4, 'native candidates include ordinary, target and every attempt')
    logs = next(run for run in answer['runs'] if run['run_id'] == 44)['attempt_logs']
    check(set(logs) == {'1', '2'} and logs['1']['sha256'] != logs['2']['sha256'], 'attempt-specific log bytes retained separately')
    repeated = next(run for run in answer['runs'] if run['run_id'] == 44)['artifacts']
    check(len(repeated) == 2 and {item['artifact_id'] for item in repeated} == {7002, 7003}, 'same-name run-level artifacts retain distinct IDs')
    check(all(item['artifact_attempt'] == 'unproven-run-level-API' for item in repeated), 'run-level API never invents artifact attempt')
    check(next(run for run in answer['runs'] if run['run_id'] == 42)['artifacts'][0]['ZIP']['embedded_event']['declaration_only'],
          'embedded event retained as bounded declaration')
    check(not answer['R_G_complete'] and not answer['first_complete_PR_sample'] and not answer['performance_accepted'], 'native bytes never certify all events or performance')
    modified = copy.deepcopy(files)
    del modified['runs/43/attempt-1/logs.zip']
    modified['fetch-failures.json'] = shared.fixture.encode({'runs/43/attempt-1/logs.zip': {'kind': 'ValueError', 'raw_bytes_retained': False}})
    partial = m.replay(candidates, candidate_record, event, modified.__getitem__, set(modified))
    check(not partial['raw_snapshot_complete'] and len(partial['gaps']) == 1, 'missing native log is diagnostic, never zero cost success')
    modified = copy.deepcopy(files)
    modified['runs/43/attempt-1/logs.zip'] = b'not a ZIP'
    partial = m.replay(candidates, candidate_record, event, modified.__getitem__, set(modified))
    check(not partial['raw_snapshot_complete'] and partial['runs'][1]['attempt_logs']['1']['valid'] is False, 'invalid raw bytes retained without false completeness')
    modified = copy.deepcopy(files)
    for phase in ('before', 'after'):
        name = 'runs/44/' + phase + '-artifacts-1.json'
        value = m.g.json_bytes(modified[name])
        value['artifacts'][0]['expired'] = True
        modified[name] = shared.fixture.encode(value)
    del modified['runs/44/artifacts/7002.zip']
    check(not m.replay(candidates, candidate_record, event, modified.__getitem__, set(modified))['raw_snapshot_complete'], 'expired native artifact stays explicit')
    modified['runs/44/artifacts/7002.zip'] = files['runs/44/artifacts/7002.zip']
    rejected(lambda: m.replay(candidates, candidate_record, event, modified.__getitem__, set(modified)), 'expired artifact has substituted')
    modified = copy.deepcopy(files)
    name = 'runs/44/before-artifacts-1.json'
    value = m.g.json_bytes(modified[name])
    value['artifacts'][0]['workflow_run']['id'] = 42
    modified[name] = shared.fixture.encode(value)
    modified[name.replace('before', 'after')] = modified[name]
    rejected(lambda: m.replay(candidates, candidate_record, event, modified.__getitem__, set(modified)), 'artifact run/source differs')
    modified = copy.deepcopy(files)
    value = m.g.json_bytes(modified['runs/42/after-run.json'])
    value['run_attempt'] = 2
    modified['runs/42/after-run.json'] = shared.fixture.encode(value)
    rejected(lambda: m.replay(candidates, candidate_record, event, modified.__getitem__, set(modified)), 'run identity differs')
    modified = copy.deepcopy(files)
    value = m.g.json_bytes(modified['runs/44/after-artifacts-1.json'])
    value['artifacts'].pop()
    value['total_count'] -= 1
    modified['runs/44/after-artifacts-1.json'] = shared.fixture.encode(value)
    rejected(lambda: m.replay(candidates, candidate_record, event, modified.__getitem__, set(modified)), 'artifact snapshots differ')
    modified = copy.deepcopy(files)
    modified['foreign.log'] = b'unbound native bytes'
    rejected(lambda: m.replay(candidates, candidate_record, event, modified.__getitem__, set(modified)), 'unused or foreign native')
    modified = copy.deepcopy(files)
    modified['runs/42/artifacts/7001.zip'] = shared.fixture.zipped({'ci-event-inputs.json': {**event, 'run_id': 43}})
    rejected(lambda: m.replay(candidates, candidate_record, event, modified.__getitem__, set(modified)), 'embedded event run binding differs')
    modified = copy.deepcopy(files)
    modified['runs/42/artifacts/7001.zip'] = shared.fixture.zipped({'ci-event-inputs.json': {**event, 'checkout_sha': '0' * 40}})
    rejected(lambda: m.replay(candidates, candidate_record, event, modified.__getitem__, set(modified)), 'original CI event differs')
    modified = copy.deepcopy(files)
    modified['runs/43/attempt-1/logs.zip'] = shared.fixture.zipped({'../escaped.txt': b'not extracted'})
    rejected(lambda: m.replay(candidates, candidate_record, event, modified.__getitem__, set(modified)), 'path')
    modified = copy.deepcopy(files)
    modified['runs/43/attempt-1/logs.zip'] = shared.fixture.zipped({})
    diagnostic = m.replay(candidates, candidate_record, event, modified.__getitem__, set(modified))
    check(not diagnostic['raw_snapshot_complete'] and diagnostic['runs'][1]['attempt_logs']['1']['empty'],
          'valid empty native logs retained only as incomplete diagnostics')
    modified = copy.deepcopy(files)
    modified['runs/44/artifacts/7002.zip'] = shared.fixture.zipped({})
    diagnostic = m.replay(candidates, candidate_record, event, modified.__getitem__, set(modified))
    check(not diagnostic['raw_snapshot_complete'] and diagnostic['runs'][2]['artifacts'][0]['ZIP']['empty'],
          'valid empty artifact has no fabricated execution or complete snapshot')
    from unittest.mock import patch
    retained = f['evidence'] / 'retained-receipt-target'
    retained.write_bytes(b'user retained private content')
    for kind in ('regular', 'symlink'):
        output = f['evidence'] / ('receipt-existing-' + kind)
        output.mkdir()
        if kind == 'regular':
            (output / 'receipt.json').write_bytes(b'existing receipt')
        else:
            (output / 'receipt.json').symlink_to(retained)
        rejected(lambda: m.instances.seal_snapshot(output, {'schema': 1}), 'receipt already exists')
    check(retained.read_bytes() == b'user retained private content', 'existing receipt symlink target never changed')
    writer = m.g.write_new
    for kind in ('regular', 'symlink'):
        output = f['evidence'] / ('receipt-late-' + kind)
        output.mkdir()
        def intervene(path, payload):
            if kind == 'regular':
                path.write_bytes(b'late retained receipt')
            else:
                path.symlink_to(retained)
            writer(path, payload)
        with patch.object(m.g, 'write_new', intervene):
            rejected(lambda: m.instances.seal_snapshot(output, {'schema': 1}), 'File exists')
        if kind == 'regular':
            check((output / 'receipt.json').read_bytes() == b'late retained receipt', 'atomic seal never overwrites late regular receipt')
        else:
            check(retained.read_bytes() == b'user retained private content', 'atomic seal never follows late symlink')
    output = f['evidence'] / 'receipt-fresh'
    output.mkdir()
    m.instances.seal_snapshot(output, {'schema': 1})
    check((output / 'receipt.json').stat().st_mode & 0o777 == 0o600, 'fresh native receipt explicitly private 0600')
    for name, value in candidate_files.items():
        f['save']('native-candidates/' + name, value)
    f['save']('native-candidates/anchor.json', anchor)
    f['save']('native-candidates/observation.json', candidates)
    m.r.manifest(f['evidence'] / 'native-candidates', {'type': 'PR-instance-candidate-archive', 'archive_complete': True})
    candidate_ref = f['save']('native-candidates/receipt.json', (f['evidence'] / 'native-candidates/receipt.json').read_bytes())
    request = {'schema': 1, 'type': 'instance-evidence-request', 'instances_request': f['save']('native-instance-source.json', source_request),
               'candidate_archive': candidate_ref}
    for name, value in files.items():
        f['save']('native/' + name, value)
    f['save']('native/observation.json', answer)
    directory = f['evidence'] / 'native'
    m.r.manifest(directory, {'type': 'native-instance-snapshot-archive', 'archive_complete': True})
    f['save']('native-reference.json', f['save']('native/receipt.json', (directory / 'receipt.json').read_bytes()))
    manifest = f['evidence'] / 'native-request.json'
    manifest.write_bytes(shared.fixture.encode(request))
    output = f['evidence'] / 'native-readback.json'
    command = ['bash', str(Path(__file__).with_name('ci-instance-evidence.sh')), 'project', '--manifest', str(manifest),
               '--archive', str(f['evidence'] / 'native-reference.json'), '--evidence-root', str(f['evidence']),
               '--root', str(f['work']), '--output', str(output)]
    child = subprocess.run(command, capture_output=True, text=True)
    check(child.returncode == 0 and json.loads(output.read_bytes())['raw_snapshot_complete'], 'CLI replays original candidate/source/native chain: ' + child.stderr)
    previous = output.read_bytes()
    check(subprocess.run(command, capture_output=True).returncode != 0 and output.read_bytes() == previous, 'native output creation exclusive')
    (directory / 'runs/42/artifacts/7001.zip').write_bytes(b'corrupted ZIP')
    command[-1] = str(f['evidence'] / 'bad-native-readback.json')
    child = subprocess.run(command, capture_output=True, text=True)
    check(child.returncode != 0 and 'file set or hash differs' in child.stderr, 'native durable byte corruption rejects')
    diagnostic_files = copy.deepcopy(files)
    del diagnostic_files['runs/43/attempt-1/logs.zip']
    diagnostic_files['fetch-failures.json'] = shared.fixture.encode({'runs/43/attempt-1/logs.zip': {'kind': 'ValueError', 'raw_bytes_retained': False}})
    diagnostic = m.replay(candidates, candidate_record, event, diagnostic_files.__getitem__, set(diagnostic_files))
    for name, value in diagnostic_files.items():
        f['save']('native-diagnostic/' + name, value)
    f['save']('native-diagnostic/observation.json', diagnostic)
    partial_directory = f['evidence'] / 'native-diagnostic'
    m.r.manifest(partial_directory, {'type': 'native-instance-snapshot-archive', 'archive_complete': False})
    f['save']('native-diagnostic-reference.json', f['save']('native-diagnostic/receipt.json', (partial_directory / 'receipt.json').read_bytes()))
    command[command.index('--archive') + 1] = str(f['evidence'] / 'native-diagnostic-reference.json')
    command[-1] = str(f['evidence'] / 'native-diagnostic-readback.json')
    child = subprocess.run(command, capture_output=True, text=True)
    check(child.returncode == 2 and not json.loads(Path(command[-1]).read_bytes())['raw_snapshot_complete'],
          'actual CLI retains valid incomplete diagnostic with exit2, distinct from invalid input')
    m.g.write_new_bytes(f['evidence'] / 'binary-roundtrip', b'\x00\xff\x01native bytes\r\n')
    check((f['evidence'] / 'binary-roundtrip').read_bytes() == b'\x00\xff\x01native bytes\r\n', 'native binary retention has no JSON/text conversion')
    rejected(lambda: m.g.write_new_bytes(f['evidence'] / 'binary-roundtrip', b'overwrite'), 'File exists')


shared.fixture.run(controls, only_callback=True)
print('native instance evidence controls passed: ' + str(count))
