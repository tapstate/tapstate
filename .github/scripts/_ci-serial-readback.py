#!/usr/bin/env python3
"""Capture and passively replay a source-bound serial PR transition.

Recorded API data is evidence of consistency, not cryptographic backend attestation.
The passive reader never queries GitHub or changes a checkout.
"""
import importlib.util
import json
from pathlib import Path
import sys

sys.dont_write_bytecode = True
spec = importlib.util.spec_from_file_location('serial_variants', Path(__file__).with_name('_ci-variants.py'))
v = importlib.util.module_from_spec(spec)
spec.loader.exec_module(v)
g, require = v.groups, v.require
MAX_TRACE = 64 * 1024 * 1024


def capture_bytes(value):
    payload = (json.dumps(value, indent=2, sort_keys=True, allow_nan=False) + '\n').encode()
    require(len(payload) <= MAX_TRACE, 'serial capture serialized payload exceeds its bound')
    return payload


def capture(refs, reference, api):
    chain = g.References(refs.root)
    request, unused = chain.read(reference)
    require('trigger_policy' in request, 'serial capture requires source-derived trigger policy')
    trace, size = [], 0

    def record(endpoint):
        nonlocal size
        value = api(endpoint)
        # Copy the response before a parser can mutate it. No headers or tokens enter the trace.
        row = {'endpoint': endpoint, 'response': value}
        raw = g.canonical(row)
        size += len(raw)
        require(size <= MAX_TRACE and len(trace) < 4096, 'serial API trace exceeds its bound')
        row = g.json_bytes(raw)
        trace.append(row)
        return g.json_bytes(g.canonical(row['response']))

    result = v.transition(chain, request, record)
    require(result.get('source_trigger_policy', {}).get('trigger_policy_source_bound') is True,
            'serial transition lacks source policy binding')
    files = chain.finish()
    for name, entry in files.items():
        refs.blob({'path': name, 'sha256': entry['sha256']})
    value = {'schema': 1, 'type': 'serial-transition-capture', 'request': reference,
             'readback': result, 'api_trace': trace, 'input_files': files,
             'backend_response_attestation_proven': False, 'first_complete_pr_sample_proven': False,
             'performance_accepted': False}
    capture_bytes(value)
    return value


def readback(refs, value):
    require(isinstance(value, dict) and set(value) == {'schema', 'type', 'request', 'readback', 'api_trace',
            'input_files', 'backend_response_attestation_proven', 'first_complete_pr_sample_proven', 'performance_accepted'}
            and type(value['schema']) is int and value['schema'] == 1
            and value['type'] == 'serial-transition-capture', 'invalid serial transition capture')
    require(all(value[key] is False for key in ('backend_response_attestation_proven',
            'first_complete_pr_sample_proven', 'performance_accepted')), 'serial capture claims unsupported proof')
    trace = value['api_trace']
    require(isinstance(trace, list) and 0 < len(trace) <= 4096 and len(g.canonical(value)) <= MAX_TRACE,
            'invalid serial API trace')
    position = 0

    def replay(endpoint):
        nonlocal position
        require(position < len(trace), 'serial API trace is incomplete')
        row = trace[position]
        require(isinstance(row, dict) and set(row) == {'endpoint', 'response'} and row['endpoint'] == endpoint,
                'serial API trace endpoint/order differs')
        position += 1
        return g.json_bytes(g.canonical(row['response']))

    expected = capture(refs, value['request'], replay)
    require(position == len(trace), 'serial API trace has unused responses')
    require(g.canonical(expected) == g.canonical(value), 'serial capture differs from independent replay')
    return expected['readback']



def workflow_window(refs, captured, result):
    """Read terminal workflow intervals from the exact archives and recorded API data."""
    intervals = []
    for entry in result['archives']:
        archive, path = refs.read(entry['receipt'])

        def raw(name):
            require(name in archive['files'], 'serial workflow timeline input is missing')
            return refs.read({'path': (path.parent / name).relative_to(refs.root).as_posix(),
                              'sha256': archive['files'][name]['sha256']})[0]

        run, attempt = raw('run.json'), raw('attempt-1/run.json')
        endpoint = f"repos/{result['snapshot']['scope']['repository']}/actions/runs/{entry['run_id']}/attempts/1"
        responses = [row['response'] for row in captured['api_trace'] if row['endpoint'] == endpoint]
        listings = [run for row in captured['api_trace'] if '/actions/runs?' in row['endpoint']
                    for run in row['response']['workflow_runs'] if run['id'] == entry['run_id']]
        require(responses and listings, 'serial workflow timeline lacks recorded original API data')
        for value, originals in ((run, listings), (attempt, responses)):
            require(all(isinstance(value.get(key), str) for key in ('created_at', 'updated_at')),
                    'serial workflow timeline timestamp is missing')
            require(all(all(value[key] == original.get(key) for key in ('created_at', 'updated_at'))
                        for original in originals), 'serial workflow timeline differs from recorded original API')
            require(g.timestamp(value['created_at']) <= g.timestamp(value['updated_at']),
                    'serial workflow timeline is reversed')
        require(run['created_at'] == attempt['created_at'], 'serial workflow creation differs across original views')
        start = run['created_at']
        end = max((run['updated_at'], attempt['updated_at']), key=g.timestamp)
        for name in archive['files']:
            if not name.startswith('attempt-1/jobs-') or not name.endswith('.json'):
                continue
            for job in raw(name)['jobs']:
                begin, finish = job['started_at'], job['completed_at']
                if begin is None and finish is None and job['conclusion'] == 'skipped':
                    continue
                require(isinstance(begin, str) and isinstance(finish, str),
                        'serial job timeline timestamp is missing')
                require(g.timestamp(start) <= g.timestamp(begin) <= g.timestamp(finish)
                        and all(g.timestamp(finish) <= g.timestamp(view['updated_at']) for view in (run, attempt)),
                        'serial job timeline is outside its workflow interval')
        intervals.append({'run_id': run['id'], 'workflow': run['path'],
                          'created_at': start, 'terminal_observed_at': end})
    require(intervals, 'serial workflow timeline is empty')
    return {'workflow_created_at': min((row['created_at'] for row in intervals), key=g.timestamp),
            'workflow_terminal_observed_at': max((row['terminal_observed_at'] for row in intervals), key=g.timestamp),
            'workflow_intervals': sorted(intervals, key=lambda row: row['run_id'])}


def bind(refs, reference, scope, variant, archive_reference):
    value, unused = refs.read(reference)
    result = readback(refs, value)
    snapshot, identity = result['snapshot'], variant['identity']
    require(snapshot['scope'] == dict(scope, head_sha=identity['head_sha'])
            and snapshot['base_sha'] == identity['base_sha'] and snapshot['merge_sha'] == identity['checkout_sha'],
            'serial readback belongs to another variant scope')
    require(snapshot['can_advance_after_archiving'] is True and result['archive_readback_verified'] is True
            and result['source_trigger_policy']['can_advance_after_archiving'] is True,
            'serial readback is not source-policy eligible and terminal')
    matches = [entry for entry in result['archives'] if entry['run_id'] == identity['run_id']]
    require(len(matches) == 1 and matches[0]['receipt'] == archive_reference and matches[0]['attempts_read_back'] == 1,
            'serial readback belongs to another archive or rerun')
    require(all(run['status'] == 'completed' and run['conclusion'] == 'success' and run['run_attempt'] == 1
                for run in snapshot['runs'])
            and all(check['status'] == 'completed' and check['conclusion'] in ('success', 'neutral', 'skipped')
                    for check in snapshot['checks']), 'serial group has a failed or retried original instance')
    return {'capture': reference, 'snapshot': snapshot, 'source_trigger_policy': result['source_trigger_policy'],
            **workflow_window(refs, value, result),
            'recorded_transition_replayed': True, 'backend_response_attestation_proven': False}
