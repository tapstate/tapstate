#!/usr/bin/env bash
# Read-only Actions timing report. Requires gh authentication and Python 3 (stdlib only).
# Run --help for selection and measurement definitions. No logs or credentials are persisted.
set -euo pipefail
exec python3 - "$@" <<'PY'
import argparse
from collections import defaultdict
from datetime import datetime
import json
import os
import re
import statistics
import subprocess
import sys
from urllib.parse import quote, urlencode


class ReportError(Exception):
    pass


def command(args):
    try:
        result = subprocess.run(args, capture_output=True, text=True)
        # New gh versions refuse ANSI-bearing Maven logs unless explicitly permitted.
        if result.returncode and 'pass --allow-escape-sequences' in result.stderr:
            result = subprocess.run(args + ['--allow-escape-sequences'], capture_output=True, text=True)
    except OSError as exc:
        raise ReportError(f'could not execute {args[0]}: {exc}') from exc
    if result.returncode:
        raise ReportError(f'{args[0]} failed: {result.stderr.strip()}')
    return result.stdout


def api(endpoint):
    try:
        value = json.loads(command(['gh', 'api', endpoint]))
    except json.JSONDecodeError as exc:
        raise ReportError(f'invalid JSON from {endpoint}') from exc
    if not isinstance(value, dict):
        raise ReportError(f'expected an API object from {endpoint}')
    return value


def pages(endpoint, key, filters=None):
    page, count, seen = 1, 0, set()
    while True:
        query = urlencode({**(filters or {}), 'per_page': 100, 'page': page})
        data = api(f'{endpoint}?{query}')
        rows, total = data.get(key), data.get('total_count')
        if not isinstance(rows, list) or not isinstance(total, int) or total < 0:
            raise ReportError(f'invalid {key} page from {endpoint}')
        if not rows:
            if count < total:
                raise ReportError(f'incomplete pagination from {endpoint}: {count} of {total}')
            return
        for row in rows:
            if not isinstance(row, dict) or not isinstance(row.get('id'), int) or row['id'] in seen:
                raise ReportError(f'invalid or duplicate {key} id from {endpoint}')
            seen.add(row['id'])
            count += 1
            yield row
        if count >= total:
            return
        page += 1


def timestamp(value, context):
    if not isinstance(value, str):
        raise ReportError(f'missing timestamp: {context}')
    try:
        result = datetime.fromisoformat(value.replace('Z', '+00:00'))
    except ValueError as exc:
        raise ReportError(f'invalid timestamp: {context}: {value}') from exc
    if result.tzinfo is None:
        raise ReportError(f'timestamp has no timezone: {context}')
    return result


def duration(start, end, context):
    seconds = (timestamp(end, context) - timestamp(start, context)).total_seconds()
    if seconds < 0:
        raise ReportError(f'reversed timestamps: {context}')
    return seconds


def summary(values):
    return {'count': len(values), 'median': statistics.median(values) if values else None,
            'max': max(values) if values else None}


def required(row, name, context):
    value = row.get(name)
    if value is None or value == '':
        raise ReportError(f'missing {name}: {context}')
    return value


def parse_logs(content, context):
    # Only Maven log records count. XML snippets and aggregate test summaries must not
    # become class timings; repeated invocations remain separate observations.
    ansi = re.compile(r'\x1b\[[0-?]*[ -/]*[@-~]')
    module = re.compile(r'^(.+?)\s+\.{2,}\s+(SUCCESS|FAILURE)\s+\[\s*([\d.,:]+)\s+(s|min|h)\s*\]$')
    test = re.compile(r'^Tests run: (\d+), Failures: (\d+), Errors: (\d+), Skipped: (\d+), '
                      r'Time elapsed: ([\d,]+(?:\.\d+)?) s\s+--?\s+in (\S+)\s*$')
    modules, classes = [], []
    in_reactor, reactor_index = False, 0
    for line_number, raw in enumerate(content.splitlines(), 1):
        clean = ansi.sub('', raw)
        match = re.search(r'\[(?:INFO|WARNING|ERROR)\]\s+(.*)$', clean)
        if not match:
            continue
        line = match.group(1).strip()
        logged_at = clean.split(' ', 1)[0] if re.match(r'^\d{4}-\d{2}-\d{2}T', clean) else None
        if line.startswith('Reactor Summary'):
            in_reactor = True
            reactor_index += 1
            continue
        if in_reactor and re.fullmatch(r'-{5,}', line):
            in_reactor = False
        match = module.fullmatch(line) if in_reactor else None
        if match:
            name, state, value, unit = match.groups()
            pieces = value.replace(',', '').split(':')
            if len(pieces) > 3:
                raise ReportError(f'invalid Maven duration in {context}: {line}')
            try:
                seconds = float(pieces[0])
                for piece in pieces[1:]:
                    seconds = seconds * 60 + float(piece)
                seconds *= {'s': 1, 'min': 60, 'h': 3600}[unit] / (60 ** (len(pieces) - 1))
            except ValueError as exc:
                raise ReportError(f'invalid Maven duration in {context}: {line}') from exc
            modules.append({'name': name, 'conclusion': state, 'seconds': seconds,
                            'reactor_index': reactor_index, 'log_line': line_number, 'logged_at': logged_at})
        match = test.fullmatch(line)
        if match:
            tests, failures, errors, skipped, elapsed, name = match.groups()
            classes.append({'name': name, 'seconds': float(elapsed.replace(',', '')),
                            'log_line': line_number, 'logged_at': logged_at,
                            'tests': int(tests), 'failures': int(failures),
                            'errors': int(errors), 'skipped': int(skipped)})
    if not modules or not classes:
        raise ReportError(f'no Maven reactor or per-class timings in logs for {context}')
    return modules, classes


def main():
    parser = argparse.ArgumentParser(description='Measure recent successful Actions runs, jobs and steps.',
        epilog='Workflow wall = original creation to last job completion, including queue and dependencies. '
               'For reruns it also includes earlier attempts and time before retry; compare attempt 1 '
               'samples for push-to-answer baselines. Job runtime = started_at to completed_at. '
               'Step medians exclude skipped steps. --modules reads selected job logs for each run, '
               'preserving every reactor and test-class occurrence; it does not infer uncovered build time.')
    parser.add_argument('workflow', help='workflow file name, for example ci.yml')
    parser.add_argument('--runs', type=int, default=5, help='required number of successful completed runs (default: 5)')
    parser.add_argument('--event', choices=['push', 'pull_request'])
    parser.add_argument('--branch', help='exact head branch filter')
    parser.add_argument('--repo', help='owner/repository (default: GITHUB_REPOSITORY or gh repo view)')
    parser.add_argument('--job', help='exact job name for step and optional Maven breakdown')
    parser.add_argument('--modules', action='store_true', help='parse Maven logs; requires --job')
    parser.add_argument('--json', action='store_true', help='emit all measurements and provenance as JSON')
    args = parser.parse_args()
    if args.runs < 1 or args.runs > 1000:
        parser.error('--runs must be between 1 and 1000 (Actions filtered search limit)')
    if args.modules and not args.job:
        parser.error('--modules requires --job')
    repo = args.repo or os.environ.get('GITHUB_REPOSITORY') or command(
        ['gh', 'repo', 'view', '--json', 'nameWithOwner', '--jq', '.nameWithOwner']).strip()
    if not re.fullmatch(r'[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+', repo):
        parser.error('repository must be owner/name')
    filters = {'status': 'success'}
    if args.event:
        filters['event'] = args.event
    if args.branch:
        filters['branch'] = args.branch
    prefix = f'repos/{repo}/actions'
    runs = []
    for run in pages(f'{prefix}/workflows/{quote(args.workflow, safe="")}/runs', 'workflow_runs', filters):
        if run.get('status') != 'completed' or run.get('conclusion') != 'success':
            continue
        if args.event and run.get('event') != args.event:
            raise ReportError('API run does not match requested event')
        if args.branch and run.get('head_branch') != args.branch:
            raise ReportError('API run does not match requested branch')
        runs.append(run)
        if len(runs) == args.runs:
            break
    if len(runs) != args.runs:
        raise ReportError(f'requested {args.runs} successful runs, found {len(runs)}')
    records, jobs_by_name, steps_by_name = [], defaultdict(list), defaultdict(list)
    for run in runs:
        run_id, attempt = run['id'], required(run, 'run_attempt', 'run')
        if not isinstance(attempt, int) or attempt < 1:
            raise ReportError(f'invalid attempt for run {run_id}')
        context = f'run {run_id} attempt {attempt}'
        jobs = list(pages(f'{prefix}/runs/{run_id}/attempts/{attempt}/jobs', 'jobs'))
        if not jobs:
            raise ReportError(f'no jobs for {context}')
        record = {'id': run_id, 'attempt': attempt, 'head_sha': required(run, 'head_sha', context),
                  'event': required(run, 'event', context), 'branch': run.get('head_branch'),
                  'url': required(run, 'html_url', context), 'created_at': run.get('created_at'),
                  'run_started_at': run.get('run_started_at'), 'jobs': []}
        selected = []
        ends, starts = [], []
        for job in jobs:
            name = required(job, 'name', context)
            if job.get('run_id') != run_id or job.get('run_attempt') != attempt:
                raise ReportError(f'job belongs to a different run or attempt: {context}: {name}')
            if job.get('status') != 'completed':
                raise ReportError(f'job is not completed: {context}: {name}')
            conclusion = required(job, 'conclusion', context)
            seconds = None
            if conclusion != 'skipped':
                seconds = duration(job.get('started_at'), job.get('completed_at'), f'{context}: {name}')
                starts.append(timestamp(job['started_at'], context))
                ends.append(timestamp(job['completed_at'], context))
            item = {'id': job['id'], 'name': name, 'conclusion': conclusion,
                    'url': required(job, 'html_url', context), 'runtime_seconds': seconds,
                    'started_at': job.get('started_at'), 'completed_at': job.get('completed_at'),
                    'runner_labels': job.get('labels', []), 'runner_name': job.get('runner_name')}
            record['jobs'].append(item)
            jobs_by_name[name].append(item)
            if name == args.job:
                selected.append(job)
        if not ends:
            raise ReportError(f'no timed jobs for {context}')
        end = max(ends).isoformat()
        record['completed_at'] = end
        record['workflow_wall_seconds'] = duration(run.get('created_at'), end, context)
        record['reported_start_to_completion_seconds'] = duration(run.get('run_started_at'), end, context)
        # First-job delay includes scheduler/startup time, not a claim about queue alone.
        record['creation_to_first_job_seconds'] = duration(run.get('created_at'), min(starts).isoformat(), context)
        if args.job:
            if len(selected) != 1 or selected[0].get('conclusion') == 'skipped':
                raise ReportError(f'expected one executed job named {args.job!r} in {context}, found {len(selected)}')
            job = selected[0]
            steps = job.get('steps')
            if not isinstance(steps, list) or not steps:
                raise ReportError(f'no steps for {context}: {args.job}')
            record['steps'] = []
            for step in steps:
                name = required(step, 'name', context)
                number = required(step, 'number', context)
                conclusion = required(step, 'conclusion', context)
                if step.get('status') != 'completed':
                    raise ReportError(f'step is not completed: {context}: {name}')
                seconds = None if conclusion == 'skipped' else duration(
                    step.get('started_at'), step.get('completed_at'), f'{context}: {name}')
                item = {'number': number, 'name': name, 'conclusion': conclusion, 'runtime_seconds': seconds}
                steps_by_name[f'{number}: {name}'].append(item)
                record['steps'].append(item)
            if args.modules:
                logs = command(['gh', 'api', f'{prefix}/jobs/{job["id"]}/logs'])
                record['modules'], record['test_classes'] = parse_logs(logs, context)
        records.append(record)
    def aggregates(groups):
        return {name: {'runtime_seconds': summary([r['runtime_seconds'] for r in rows
                                                   if r['runtime_seconds'] is not None]),
                       'skipped': sum(r['conclusion'] == 'skipped' for r in rows)}
                for name, rows in sorted(groups.items())}
    report = {'repository': repo, 'workflow': args.workflow, 'event': args.event, 'branch': args.branch,
              'selected_job': args.job, 'sample_count': len(records),
              'workflow_wall_seconds': summary([r['workflow_wall_seconds'] for r in records]),
              'jobs': aggregates(jobs_by_name), 'steps': aggregates(steps_by_name), 'runs': records}
    if args.json:
        print(json.dumps(report, indent=2, allow_nan=False))
        return
    def row(name, stats):
        def fmt(value):
            return 'n/a' if value is None else f'{value:.3f}'
        print(f'{name}: n={stats["count"]} median={fmt(stats["median"])}s max={fmt(stats["max"])}s')
    print(f'{repo} / {args.workflow} event={args.event or "all"} branch={args.branch or "all"}')
    row('Workflow wall (created -> last job completed, includes queue)', report['workflow_wall_seconds'])
    if any(r['attempt'] > 1 for r in records):
        print('Rerun samples include time before retry in workflow wall; compare attempt 1 for push-to-answer baselines.')
    for name, values in report['jobs'].items():
        row(f'Job {name} (skipped={values["skipped"]})', values['runtime_seconds'])
    for name, values in report['steps'].items():
        row(f'Step {args.job} / {name} (skipped={values["skipped"]})', values['runtime_seconds'])
    for record in records:
        print(f'Run {record["id"]} attempt={record["attempt"]} sha={record["head_sha"]} '
              f'event={record["event"]} branch={record["branch"]} {record["url"]}')
        print(f'  created={record["created_at"]} reported_start={record["run_started_at"]} '
              f'completed={record["completed_at"]} first_job_delay={record["creation_to_first_job_seconds"]:.3f}s')
        for job in record['jobs']:
            print(f'  {job["name"]}: labels={job["runner_labels"]} runner={job["runner_name"]} {job["url"]}')
        for module in record.get('modules', []):
            print(f'  Reactor {module["reactor_index"]} / Module {module["name"]}: '
                  f'{module["seconds"]:.3f}s ({module["conclusion"]}) line={module["log_line"]}')
        for test in record.get('test_classes', []):
            print(f'  Class {test["name"]}: {test["seconds"]:.3f}s tests={test["tests"]} skipped={test["skipped"]}')


try:
    main()
except ReportError as error:
    print(f'ci-timing: {error}', file=sys.stderr)
    sys.exit(1)
PY
