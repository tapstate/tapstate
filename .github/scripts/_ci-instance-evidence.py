#!/usr/bin/env python3
"""Retain bounded native attempt logs and run-level artifact ZIPs for archived candidates."""
import argparse
from datetime import datetime, timezone
import importlib.util
import json
import io
from pathlib import Path
import subprocess
import sys
import time
import zipfile

sys.dont_write_bytecode = True
spec = importlib.util.spec_from_file_location('raw_instances', Path(__file__).with_name('_ci-pr-instances.py'))
instances = importlib.util.module_from_spec(spec)
spec.loader.exec_module(instances)
g, r, require = instances.g, instances.r, instances.require
MAX_RAW = 64 * 1024 * 1024


def binary(endpoint):
    # Bound memory while downloading; terminate only this owned GET if it exceeds the budget.
    process = subprocess.Popen(['gh', 'api', endpoint], stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
    try:
        data = process.stdout.read(MAX_RAW + 1)
        require(len(data) <= MAX_RAW, 'native ZIP download exceeds byte budget')
        require(process.wait() == 0, 'native ZIP download unavailable')
        return data
    finally:
        process.stdout.close()
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()


def original(refs, root, request):
    require(isinstance(request, dict) and set(request) == {'schema', 'type', 'instances_request', 'candidate_archive'}
            and type(request['schema']) is int and request['schema'] == 1
            and request['type'] == 'instance-evidence-request', 'invalid native instance request')
    source_request, unused = refs.read(request['instances_request'])
    anchor = instances.anchor(refs, root, source_request)
    candidates, path, archive = instances.read_candidate(refs, anchor, request['candidate_archive'])
    source, unused = refs.read(source_request['source'])
    original_archive = instances.trigger.archive_inputs.Archive(refs, source['archive'], anchor['scope'])
    event = original_archive.json('event-inputs.json')

    def record(name):
        require(name in archive['files'], 'original candidate record missing')
        return refs.read({'path': (path.parent / name).relative_to(refs.root).as_posix(),
                          'sha256': archive['files'][name]['sha256']})[0]

    return candidates, record, event


def stable(run, expected, repository):
    require(instances.run_identity(run, repository, expected['event'])
            == instances.run_identity(expected, repository, expected['event']), 'native run identity differs from candidate')
    require(run.get('head_branch') == expected.get('head_branch') and isinstance(run.get('head_branch'), str),
            'native run branch differs or is absent')
    require(run.get('status') == expected.get('status') == 'completed'
            and run.get('conclusion') == expected.get('conclusion'), 'native run terminal outcome differs or is incomplete')


def artifact(record, run):
    instances.positive(record.get('id'), 'artifact ID')
    require(isinstance(record.get('name'), str) and record['name'] and type(record.get('expired')) is bool,
            'invalid native artifact metadata')
    workflow = record.get('workflow_run')
    require(isinstance(workflow, dict) and type(workflow.get('id')) is int and workflow.get('id') == run['id']
            and workflow.get('head_sha') == run['head_sha'] and workflow.get('head_branch') == run['head_branch'],
            'native artifact run/source differs')


def capture(candidates, candidate_record, request, download, put, put_bytes):
    repository = candidates['anchor']['scope']['repository']
    prefix = 'repos/' + repository
    failures, seen_artifacts = {}, set()

    def fetch(endpoint, path):
        try:
            data = download(endpoint)
            require(isinstance(data, bytes) and len(data) <= MAX_RAW, 'native ZIP download exceeds byte budget')
        except (ValueError, OSError) as error:
            failures[path] = {'kind': type(error).__name__, 'raw_bytes_retained': False}
            return
        put_bytes(path, data)

    for candidate in candidates['runs']:
        if not candidate['details_captured']:
            continue
        identity = candidate['run_id']
        base = 'runs/' + str(identity)
        expected = candidate_record(base + '/run.json')
        run = request(f'{prefix}/actions/runs/{identity}')
        stable(run, expected, repository)
        put(base + '/before-run.json', run)
        for attempt in range(1, run['run_attempt'] + 1):
            fetch(f'{prefix}/actions/runs/{identity}/attempts/{attempt}/logs', base + f'/attempt-{attempt}/logs.zip')
        records = instances.query_rows(request, f'{prefix}/actions/runs/{identity}/artifacts', 'artifacts', put,
                                       base + '/before-artifacts')
        for row in records:
            artifact(row, run)
            require(row['id'] not in seen_artifacts, 'artifact ID appears in different runs')
            seen_artifacts.add(row['id'])
            if not row['expired']:
                fetch(f'{prefix}/actions/artifacts/{row["id"]}/zip', base + f'/artifacts/{row["id"]}.zip')
        after = instances.query_rows(request, f'{prefix}/actions/runs/{identity}/artifacts', 'artifacts', put,
                                     base + '/after-artifacts')
        require({row['id']: row for row in records} == {row['id']: row for row in after}, 'native artifact snapshot changed')
        final = request(f'{prefix}/actions/runs/{identity}')
        stable(final, expected, repository)
        put(base + '/after-run.json', final)
    put('fetch-failures.json', failures)


def zip_fact(raw, run, event):
    require(len(raw) <= MAX_RAW, 'native ZIP exceeds byte budget')
    with zipfile.ZipFile(io.BytesIO(raw)) as archive:
        if not archive.infolist():
            return {'sha256': g.checksum(raw), 'empty': True, 'members': {}}
    files, unused_members = instances.trigger.archive_inputs.zip_read(raw)
    fact = {'sha256': g.checksum(raw), 'members': {name: {'size': len(data), 'sha256': g.checksum(data)}
                                               for name, data in sorted(files.items())}}
    if 'ci-event-inputs.json' in files:
        declared = g.json_bytes(files['ci-event-inputs.json'])
        require(isinstance(declared, dict) and declared.get('repository') == event['repository']
                and type(declared.get('run_id')) is int and declared.get('run_id') == run['id'] and declared.get('event') == run['event']
                and type(declared.get('run_attempt')) is int and 1 <= declared['run_attempt'] <= run['run_attempt'],
                'native artifact embedded event run binding differs')
        if run['id'] == event['run_id']:
            require(declared == event, 'native artifact original CI event differs')
        fact['embedded_event'] = {'member_sha256': g.checksum(files['ci-event-inputs.json']),
                                  'run_id': declared['run_id'], 'run_attempt': declared['run_attempt'],
                                  'event': declared['event'], 'declaration_only': True}
    return fact


def replay(candidates, candidate_record, event, read, names):
    repository = candidates['anchor']['scope']['repository']
    used, gaps, runs, seen_artifacts = set(), [], [], set()

    def raw(name):
        require(name in names, 'mandatory native record missing: ' + name)
        used.add(name)
        return read(name)

    def value(name):
        return g.json_bytes(raw(name))

    def pages(stem):
        first = value(stem + '-1.json')
        total = first.get('total_count')
        require(type(total) is int and total >= 0, 'invalid native artifact page')
        records, ids = [], set()
        for number in range(1, 102):
            page = first if number == 1 else value(stem + f'-{number}.json')
            require(page.get('total_count') == total and isinstance(page.get('artifacts'), list), 'native artifact page totals differ')
            for row in page['artifacts']:
                identity = instances.positive(row.get('id'), 'native artifact page ID')
                require(identity not in ids, 'duplicate native artifact API identity')
                ids.add(identity)
                records.append(row)
            require(len(records) <= total, 'native artifact API count differs')
            if len(records) == total:
                return records
            require(page['artifacts'], 'native artifact page set incomplete')
        raise ValueError('native artifact page budget exceeded')

    def zipped(name, run):
        if name not in names:
            gaps.append('native bytes unavailable: ' + name)
            return None
        data = raw(name)
        try:
            facts = zip_fact(data, run, event)
            if facts.get('empty'):
                gaps.append('native ZIP empty: ' + name)
            return facts
        except zipfile.BadZipFile:
            gaps.append('native ZIP invalid: ' + name)
            return {'sha256': g.checksum(data), 'valid': False}

    failures = value('fetch-failures.json')
    require(isinstance(failures, dict), 'invalid native fetch diagnostics')
    for candidate in candidates['runs']:
        if not candidate['details_captured']:
            continue
        identity = candidate['run_id']
        base = 'runs/' + str(identity)
        expected = candidate_record(base + '/run.json')
        run, after = value(base + '/before-run.json'), value(base + '/after-run.json')
        stable(run, expected, repository)
        stable(after, expected, repository)
        record = {'run_id': identity, 'event': run['event'], 'attempt_logs': {}, 'artifacts': []}
        for attempt in range(1, run['run_attempt'] + 1):
            record['attempt_logs'][str(attempt)] = zipped(base + f'/attempt-{attempt}/logs.zip', run)
        before, after = pages(base + '/before-artifacts'), pages(base + '/after-artifacts')
        require({row['id']: row for row in before} == {row['id']: row for row in after}, 'archived native artifact snapshots differ')
        for row in before:
            artifact(row, run)
            require(row['id'] not in seen_artifacts, 'archived artifact ID appears in different runs')
            seen_artifacts.add(row['id'])
            file = base + f'/artifacts/{row["id"]}.zip'
            if row['expired']:
                require(file not in names, 'expired artifact has substituted native bytes')
                gaps.append('native artifact expired: ' + str(row['id']))
                facts = None
            else:
                facts = zipped(file, run)
            record['artifacts'].append({'artifact_id': row['id'], 'name': row['name'], 'run_id': identity,
                                        'artifact_attempt': 'unproven-run-level-API', 'ZIP': facts})
        runs.append(record)
    require(set(names) == used, 'unused or foreign native archive files')
    missing = {gap.removeprefix('native bytes unavailable: ') for gap in gaps if gap.startswith('native bytes unavailable: ')}
    require(set(failures) == missing and all(isinstance(value, dict) and value.get('raw_bytes_retained') is False for value in failures.values()),
            'native fetch diagnostics differ from missing bytes')
    return {'schema': 1, 'type': 'archived-native-instance-evidence', 'anchor': candidates['anchor'],
            'runs': runs, 'raw_snapshot_complete': not gaps, 'gaps': gaps, 'used_files': sorted(used),
            'R_G_complete': False, 'first_complete_PR_sample': False, 'performance_accepted': False,
            'unverified_components': ['original_event_membership_and_applicability', 'artifact_attempt_without_source_bound_producer',
                                      'complete_execution_input_and_test_provenance', 'API_history_limits_and_snapshot_atomicity']}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=('collect', 'project'))
    for field in ('manifest', 'evidence-root', 'root', 'output'):
        parser.add_argument('--' + field, required=True)
    parser.add_argument('--archive', help='JSON path/SHA-256 reference to native snapshot receipt')
    args = parser.parse_args()
    refs = g.References(Path(args.evidence_root))
    request = g.load(Path(args.manifest))
    candidates, candidate_record, event = original(refs, Path(args.root), request)
    output = Path(args.output).absolute()
    if args.command == 'collect':
        output = instances.collection_output(args.output, args.root)
        started = time.monotonic()
        capture(candidates, candidate_record, r.api, binary,
                lambda name, data: g.write_new(output / name, data),
                lambda name, data: g.write_new_bytes(output / name, data))
        files = {p.relative_to(output).as_posix() for p in r.safe_files(output)}
        answer = replay(candidates, candidate_record, event, lambda name: g.file_bytes(output / name), files)
        g.write_new(output / 'observation.json', answer)
        instances.seal_snapshot(output, {'schema': 1, 'type': 'native-instance-snapshot-archive', 'archive_complete': answer['raw_snapshot_complete'],
                            'R_G_complete': False, 'first_complete_PR_sample': False, 'performance_accepted': False,
                            'collected_at': datetime.now(timezone.utc).isoformat(), 'collection_seconds_before_receipt_seal': time.monotonic() - started,
                            'outer_collection_cost_complete': False,
                            'input_files': refs.finish()})
        print(json.dumps({'archive': str(output), 'raw_snapshot_complete': answer['raw_snapshot_complete'], 'R_G_complete': False,
                          'collection_and_seal_seconds': time.monotonic() - started, 'last_stdout_write_unmeasured': True}))
        return 0 if answer['raw_snapshot_complete'] else 2
    require(args.archive is not None, 'native replay requires archive reference')
    archived, path = refs.read(g.load(Path(args.archive)))
    require(archived.get('type') == 'native-instance-snapshot-archive', 'invalid native snapshot archive')
    actual = {p.relative_to(path.parent).as_posix(): {'sha256': r.digest(p), 'size': p.stat().st_size}
              for p in r.safe_files(path.parent) if p != path}
    require(actual == archived['files'], 'native archive file set or hash differs')

    def read(name):
        require(name in archived['files'], 'native archive record absent')
        return refs.blob({'path': (path.parent / name).relative_to(refs.root).as_posix(),
                          'sha256': archived['files'][name]['sha256']})[0]

    names = set(archived['files']) - {'observation.json'}
    answer = replay(candidates, candidate_record, event, read, names)
    require(g.json_bytes(read('observation.json')) == answer and archived.get('archive_complete') is answer['raw_snapshot_complete'],
            'native observation differs from independent raw replay')
    answer['input_files'] = refs.finish()
    g.write_new(output, answer)
    print(json.dumps({'output': str(output), 'raw_snapshot_complete': answer['raw_snapshot_complete'], 'R_G_complete': False}))
    return 0 if answer['raw_snapshot_complete'] else 2


if __name__ == '__main__':
    try:
        sys.exit(main())
    except (ValueError, OSError, KeyError, TypeError, json.JSONDecodeError, zipfile.BadZipFile) as error:
        print('ci-instance-evidence: ' + str(error), file=sys.stderr)
        sys.exit(1)
