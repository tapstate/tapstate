#!/usr/bin/env python3
"""Archive PR run/job/check candidates by identity without inventing event membership."""
import argparse
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import re
import sys
import tempfile
import time
from urllib.parse import urlencode

sys.dont_write_bytecode = True
spec = importlib.util.spec_from_file_location('instance_trigger', Path(__file__).with_name('_ci-trigger-basis.py'))
trigger = importlib.util.module_from_spec(spec)
spec.loader.exec_module(trigger)
g = trigger.g
r = g.receipt
require = r.require


def instant(value):
    require(isinstance(value, str), 'invalid timestamp')
    result = datetime.fromisoformat(value.replace('Z', '+00:00'))
    require(result.tzinfo is not None and result.utcoffset().total_seconds() == 0, 'timestamp must be UTC')
    return result


def anchor(refs, root, request):
    require(isinstance(request, dict) and set(request) == {'schema', 'type', 'source', 'target_created'}
            and type(request['schema']) is int and request['schema'] == 1
            and request['type'] == 'pr-instance-request', 'invalid PR instance request')
    basis = trigger.project(refs, root, {'schema': 1, 'type': 'trigger-basis-request', 'source': request['source']})
    scope = basis['scope']
    require(scope['event'] == 'pull_request' and scope['run_attempt'] == 1, 'ordinary original PR anchor required')
    source, unused = refs.read(request['source'])
    original = trigger.archive_inputs.Archive(refs, source['archive'], scope)
    event = original.json('event-inputs.json')
    require(isinstance(event.get('pull_request'), dict) and event.get('event_payload_complete') is True,
            'complete original nested PR event required')
    pr = event['pull_request']
    window = request['target_created']
    require(isinstance(window, dict) and set(window) == {'start', 'end'}, 'invalid target observation window')
    start, end = instant(window['start']), instant(window['end'])
    require(0 < (end - start).total_seconds() <= 86400, 'target observation window must be bounded to one day')
    run = original.json('run.json')
    require(start <= instant(run['created_at']) <= end, 'target observation window excludes original CI creation')
    return {'scope': scope, 'checkout': basis['checkout'], 'original_action': pr['action'],
            'original_labels': basis['original_labels'], 'target_created': window,
            'source_request_sha256': request['source']['sha256']}


def collection_output(value, root):
    output = Path(value).absolute()
    require(output.resolve() == output, 'collection output path must be canonical')
    require(not output.exists() and not output.is_symlink(), 'collection output already exists')
    temporary = {Path('/tmp').resolve(), Path('/private/tmp').resolve(), Path(tempfile.gettempdir()).resolve()}
    require(not output.is_relative_to(Path(root).resolve())
            and not any(output.is_relative_to(path) for path in temporary),
            'collection must stay in a persistent archive outside product source')
    return output


def positive(value, label):
    require(type(value) is int and value > 0, 'invalid ' + label)
    return value


def associations(value, scope):
    require(isinstance(value, list), 'invalid PR associations')
    for item in value:
        require(isinstance(item, dict) and type(item.get('number')) is int, 'invalid PR association identity')
    if not value:
        return 'unassociated'
    matching = [item for item in value if item['number'] == scope['pr']]
    if not matching:
        return 'foreign-pr'
    if len(matching) != len(value):
        return 'mixed-pr'
    if any(item.get('head', {}).get('sha') != scope['head_sha']
           or item.get('base', {}).get('sha') != scope['base_sha'] for item in matching):
        return 'different-head-or-base'
    return 'matching-pr-refs-event-unproven'


def run_identity(run, repository, event):
    require(isinstance(run, dict) and run.get('repository', {}).get('full_name') == repository
            and run.get('event') == event, 'foreign run repository or event')
    positive(run.get('id'), 'run ID')
    positive(run.get('workflow_id'), 'workflow ID')
    positive(run.get('check_suite_id'), 'run check suite ID')
    attempt = positive(run.get('run_attempt'), 'run attempt')
    require(attempt <= 100, 'run attempts exceed archive budget')
    r.sha(run.get('head_sha'), 'run head')
    require(isinstance(run.get('path'), str) and run['path'].startswith('.github/workflows/'), 'invalid workflow path')
    return {key: run[key] for key in ('id', 'workflow_id', 'check_suite_id', 'run_attempt', 'event', 'head_sha', 'path')}


def query_rows(request, endpoint, key, put, stem):
    rows, seen, total = [], set(), None
    for page in range(1, 102):
        value = request(endpoint + ('&' if '?' in endpoint else '?') + f'per_page=100&page={page}')
        put(f'{stem}-{page}.json', value)
        require(isinstance(value, dict) and type(value.get('total_count')) is int
                and value['total_count'] >= 0 and isinstance(value.get(key), list), 'invalid API page')
        require(total is None or total == value['total_count'], 'API totals changed during pagination')
        total = value['total_count']
        require(key != 'workflow_runs' or total < 1000, 'workflow search result cap prevents complete enumeration')
        for row in value[key]:
            identity = positive(row.get('id'), 'API row ID')
            require(identity not in seen, 'duplicate API identity')
            seen.add(identity)
            rows.append(row)
        require(len(rows) <= total, 'API page count differs')
        if len(rows) == total:
            return rows
        require(value[key], 'incomplete API pagination')
    raise ValueError('API page budget exceeded')


def enumeration(anchor_value, request, put, phase):
    scope = anchor_value['scope']
    prefix = 'repos/' + scope['repository']
    window = anchor_value['target_created']
    parameters = {'pull_request': {'event': 'pull_request', 'head_sha': scope['head_sha']},
                  'pull_request_target': {'event': 'pull_request_target',
                                          'created': window['start'] + '..' + window['end']}}
    values = {}
    for event, query in parameters.items():
        rows = query_rows(request, prefix + '/actions/runs?' + urlencode(query), 'workflow_runs', put,
                          f'{phase}/{event}/runs')
        for row in rows:
            run_identity(row, scope['repository'], event)
            if event == 'pull_request':
                require(row['head_sha'] == scope['head_sha'], 'foreign ordinary PR head in query')
            else:
                require(instant(window['start']) <= instant(row['created_at']) <= instant(window['end']),
                        'target run outside observation window')
        values[event] = rows
    return values


def capture(anchor_value, request, put):
    """Only GET JSON endpoints; preserve every candidate and every attempt's jobs."""
    scope = anchor_value['scope']
    prefix = 'repos/' + scope['repository']
    first = enumeration(anchor_value, request, put, 'before')
    rows = [run for event in first.values() for run in event]
    require(len({row['id'] for row in rows}) == len(rows), 'run ID appears in different event namespaces')
    require(any(row['id'] == scope['run_id'] for row in first['pull_request']), 'original CI anchor absent from run candidates')
    checks_at = {scope['head_sha'], scope['base_sha'], anchor_value['checkout']['sha']}
    for candidate in rows:
        status = associations(candidate.get('pull_requests'), scope)
        if candidate['event'] == 'pull_request_target' and status in ('foreign-pr', 'mixed-pr', 'different-head-or-base'):
            continue
        identity = run_identity(candidate, scope['repository'], candidate['event'])
        base = 'runs/' + str(candidate['id'])
        run = request(f'{prefix}/actions/runs/{candidate["id"]}')
        put(base + '/run.json', run)
        require(run_identity(run, scope['repository'], candidate['event']) == identity, 'run identity advanced during collection')
        checks_at.add(run['head_sha'])
        for attempt in range(1, run['run_attempt'] + 1):
            item = request(f'{prefix}/actions/runs/{run["id"]}/attempts/{attempt}')
            put(base + f'/attempt-{attempt}/run.json', item)
            require(attempt != run['run_attempt'] or item.get('check_suite_id') == identity['check_suite_id'],
                    'latest attempt check suite differs from frozen run')
            expected = {**identity, 'run_attempt': attempt, 'check_suite_id': item.get('check_suite_id')}
            require(run_identity(item, scope['repository'], run['event']) == expected, 'attempt run binding differs')
            jobs = query_rows(request, f'{prefix}/actions/runs/{run["id"]}/attempts/{attempt}/jobs', 'jobs', put,
                              base + f'/attempt-{attempt}/jobs')
            require(all(job.get('run_id') == run['id'] and job.get('run_attempt') == attempt for job in jobs),
                    'job run or attempt binding differs')
    for sha in sorted(checks_at):
        checks = query_rows(request, f'{prefix}/commits/{sha}/check-runs?filter=all', 'check_runs', put, 'checks/' + sha)
        require(all(check.get('head_sha') == sha for check in checks), 'check source differs from query')
    last = enumeration(anchor_value, request, put, 'after')
    for event in first:
        require({row['id']: run_identity(row, scope['repository'], event) for row in first[event]}
                == {row['id']: run_identity(row, scope['repository'], event) for row in last[event]},
                'workflow candidate set changed during collection')


def summarize(anchor_value, read):
    """Replay pages and joins; no check-name collapse, success or cohort claims."""
    scope = anchor_value['scope']
    used = set()

    def get(path):
        used.add(path)
        return read(path)

    def page_set(stem, key):
        first = get(stem + '-1.json')
        total = first.get('total_count')
        require(type(total) is int and total >= 0, 'invalid archived API count')
        values, seen = [], set()
        for page in range(1, 102):
            value = first if page == 1 else get(stem + f'-{page}.json')
            require(value.get('total_count') == total and isinstance(value.get(key), list), 'archived API totals differ')
            for row in value[key]:
                identity = positive(row.get('id'), 'archived API row ID')
                require(identity not in seen, 'duplicate archived API identity')
                seen.add(identity)
                values.append(row)
            require(len(values) <= total, 'archived API count differs')
            if len(values) == total:
                break
            require(value[key], 'archived API page set incomplete')
        require(len(values) == total, 'archived API page set incomplete')
        require(key != 'workflow_runs' or total < 1000, 'workflow search result cap prevents complete enumeration')
        return values

    runs, jobs, checks, suites = [], [], {}, {}
    checks_at = {scope['head_sha'], scope['base_sha'], anchor_value['checkout']['sha']}
    seen_runs = set()
    for event in ('pull_request', 'pull_request_target'):
        before, after = (page_set(phase + '/' + event + '/runs', 'workflow_runs') for phase in ('before', 'after'))
        identities = [{row['id']: run_identity(row, scope['repository'], event) for row in rows} for rows in (before, after)]
        require(identities[0] == identities[1], 'archived workflow candidate sets differ')
        for row in before:
            require(row['id'] not in seen_runs, 'archived run ID appears in different event namespaces')
            seen_runs.add(row['id'])
            if event == 'pull_request':
                require(row['head_sha'] == scope['head_sha'], 'foreign archived ordinary PR head')
            else:
                window = anchor_value['target_created']
                require(instant(window['start']) <= instant(row['created_at']) <= instant(window['end']), 'archived target outside observation window')
            association = associations(row.get('pull_requests'), scope)
            captured = event != 'pull_request_target' or association not in ('foreign-pr', 'mixed-pr', 'different-head-or-base')
            record = {'run_id': row['id'], 'event': event, 'workflow_id': row['workflow_id'], 'path': row['path'],
                      'check_suite_id': row['check_suite_id'], 'head_sha': row['head_sha'], 'association': association,
                      'attempts': [], 'original_event_membership': 'unknown', 'details_captured': captured}
            runs.append(record)
            if not captured:
                continue
            base = 'runs/' + str(row['id'])
            detail = get(base + '/run.json')
            require(run_identity(detail, scope['repository'], event) == identities[0][row['id']], 'archived run detail binding differs')
            checks_at.add(detail['head_sha'])
            for attempt in range(1, row['run_attempt'] + 1):
                item = get(base + f'/attempt-{attempt}/run.json')
                require(attempt != row['run_attempt'] or item.get('check_suite_id') == row['check_suite_id'],
                        'latest attempt check suite differs from frozen run')
                require(run_identity(item, scope['repository'], event) == {**identities[0][row['id']], 'run_attempt': attempt, 'check_suite_id': item.get('check_suite_id')},
                        'archived attempt run binding differs')
                suites.setdefault(item['check_suite_id'], []).append((row['id'], attempt))
                record['attempts'].append({key: item.get(key) for key in ('run_attempt', 'check_suite_id', 'status', 'conclusion', 'created_at', 'updated_at')})
                for job in page_set(base + f'/attempt-{attempt}/jobs', 'jobs'):
                    require(job.get('run_id') == row['id'] and job.get('run_attempt') == attempt, 'archived job binding differs')
                    check_url = job.get('check_run_url')
                    match = re.fullmatch(r'https://api\.github\.com/repos/' + re.escape(scope['repository']) + r'/check-runs/([1-9][0-9]*)', check_url or '')
                    jobs.append({'job_id': job['id'], 'run_id': row['id'], 'run_attempt': attempt, 'name': job.get('name'),
                                 'status': job.get('status'), 'conclusion': job.get('conclusion'),
                                 'check_run_id': int(match[1]) if match else None})
    require(scope['run_id'] in seen_runs, 'archived original CI anchor absent')
    for sha in sorted(checks_at):
        for check in page_set('checks/' + sha, 'check_runs'):
            require(check.get('head_sha') == sha, 'archived check source differs')
            require(check['id'] not in checks, 'check ID appears in different source queries')
            suite_id = positive(check.get('check_suite', {}).get('id'), 'check suite ID')
            checks[check['id']] = {'check_id': check['id'], 'head_sha': sha, 'name': check.get('name'),
                                  'status': check.get('status'), 'conclusion': check.get('conclusion'),
                                  'app_id': positive(check.get('app', {}).get('id'), 'check app ID'),
                                  'check_suite_id': suite_id, 'run_ids': sorted({item[0] for item in suites.get(suite_id, [])}),
                                  'run_attempts': [{'run_id': run, 'run_attempt': attempt} for run, attempt in sorted(suites.get(suite_id, []))],
                                  'association': associations(check.get('pull_requests', []), scope),
                                  'job_instances': [], 'original_event_membership': 'unknown'}
    for job in jobs:
        check = checks.get(job['check_run_id'])
        if check:
            require({'run_id': job['run_id'], 'run_attempt': job['run_attempt']} in check['run_attempts'],
                    'job check URL and run attempt suite disagree')
            check['job_instances'].append({'job_id': job['job_id'], 'run_id': job['run_id'], 'run_attempt': job['run_attempt']})
    require(len({job['job_id'] for job in jobs}) == len(jobs), 'job ID repeated across run attempts')
    return {'schema': 1, 'type': 'archived-pr-instance-candidates', 'anchor': anchor_value,
            'runs': runs, 'jobs': jobs, 'checks': list(checks.values()), 'raw_JSON_readback_verified': True,
            'R_G_complete': False, 'first_complete_PR_sample': False, 'performance_accepted': False,
            'unverified_components': ['per_workflow_original_action_and_event_membership', 'target_event_and_base_workflow_source',
                                      'trigger_guard_path_and_historical_ruleset_applicability', 'API_history_limits_and_snapshot_atomicity',
                                      'raw_logs_artifacts_and_complete_execution_provenance'], 'used_files': sorted(used)}


def seal_snapshot(directory, metadata):
    destination = directory / 'receipt.json'
    require(not destination.exists() and not destination.is_symlink(), 'snapshot receipt already exists')
    files = {path.relative_to(directory).as_posix(): {'sha256': r.digest(path), 'size': path.stat().st_size}
             for path in r.safe_files(directory) if path != destination}
    g.write_new(destination, {**metadata, 'files': files})


def read_candidate(refs, expected, reference):
    archived, path = refs.read(reference)
    require(archived.get('type') == 'PR-instance-candidate-archive' and archived.get('archive_complete') is True,
            'invalid instance candidate archive')
    actual = {p.relative_to(path.parent).as_posix(): {'sha256': r.digest(p), 'size': p.stat().st_size}
              for p in r.safe_files(path.parent) if p != path}
    require(actual == archived['files'], 'candidate archive file set or hash differs')

    def read(name):
        require(name in archived['files'], 'mandatory candidate record missing')
        return refs.read({'path': (path.parent / name).relative_to(refs.root).as_posix(),
                          'sha256': archived['files'][name]['sha256']})[0]

    require(read('anchor.json') == expected, 'candidate anchor differs from original source')
    answer = summarize(expected, read)
    require(read('observation.json') == answer, 'candidate observation differs from independent replay')
    require(set(archived['files']) == set(answer['used_files']) | {'anchor.json', 'observation.json'}, 'unused or foreign candidate archive files')
    return answer, path, archived


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=('collect', 'project'))
    for field in ('manifest', 'evidence-root', 'root', 'output'):
        parser.add_argument('--' + field, required=True)
    parser.add_argument('--archive', help='path/SHA-256 JSON reference to the collected receipt')
    args = parser.parse_args()
    refs = g.References(Path(args.evidence_root))
    request = g.load(Path(args.manifest))
    expected = anchor(refs, Path(args.root), request)
    output = Path(args.output).absolute()
    if args.command == 'collect':
        output = collection_output(args.output, args.root)
        started = time.monotonic()
        g.write_new(output / 'anchor.json', expected)
        capture(expected, r.api, lambda name, value: g.write_new(output / name, value))
        answer = summarize(expected, lambda name: g.load(output / name))
        g.write_new(output / 'observation.json', answer)
        metadata = {'schema': 1, 'type': 'PR-instance-candidate-archive', 'archive_complete': True,
                    'R_G_complete': False, 'first_complete_PR_sample': False, 'performance_accepted': False,
                    'collected_at': datetime.now(timezone.utc).isoformat(), 'external_collection_seconds': time.monotonic() - started,
                    'input_files': refs.finish()}
        seal_snapshot(output, metadata)
        print(json.dumps({'archive': str(output), 'candidate_JSON_archive_complete': True, 'R_G_complete': False}))
    else:
        require(args.archive is not None, 'project requires archive reference')
        reference = g.load(Path(args.archive))
        answer, unused_path, unused_archive = read_candidate(refs, expected, reference)
        answer['input_files'] = refs.finish()
        g.write_new(output, answer)
        print(json.dumps({'output': str(output), 'raw_JSON_readback_verified': True, 'R_G_complete': False}))
    return 0


if __name__ == '__main__':
    try:
        sys.exit(main())
    except (ValueError, OSError, KeyError, TypeError, json.JSONDecodeError) as error:
        print('ci-pr-instances: ' + str(error), file=sys.stderr)
        sys.exit(1)
