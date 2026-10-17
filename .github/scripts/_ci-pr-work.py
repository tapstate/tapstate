#!/usr/bin/env python3
"""Stop only this worker's owned process group after confirming an obsolete PR head."""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import signal
import select
import subprocess
import sys
import time

sys.dont_write_bytecode = True
HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location('ci_work_receipt', HERE / '_ci-receipt.py')
receipt = importlib.util.module_from_spec(spec)
spec.loader.exec_module(receipt)
OBSOLETE = 78


def scope(root, original, environment):
    payload = json.loads(Path(environment['GITHUB_EVENT_PATH']).read_text())
    actual = receipt.event_snapshot(root, payload, environment)
    # The planner and worker have different job IDs; their event and checkout must agree.
    comparable = lambda value: {k: v for k, v in value.items() if k != 'workflow_source'}
    receipt.require(comparable(original) == comparable(actual), 'worker event differs from original plan event')
    before = receipt.event_workflow_source(original, actual['repository'])
    now = receipt.event_workflow_source(actual, actual['repository'])
    without_job = lambda value: {k: v for k, v in value.items() if k != 'job'}
    receipt.require(without_job(before) == without_job(now), 'worker workflow differs from planner workflow')
    if before['snapshot'] != 'captured':
        return None
    receipt.require(before['job'] == 'shard-plan' and now['job'] == 'test-shards', 'foreign planner or worker job')
    if actual['event'] != 'pull_request' or actual.get('event_payload_complete') is not True:
        return None
    pr = actual['pull_request']
    if pr['head_repository'] != actual['repository']:
        return None
    return {'repository': actual['repository'], **pr}


def live_identity(value, expected):
    receipt.require(isinstance(value, dict) and type(value['number']) is int
                    and value['number'] == expected['number'] and value['state'] == 'open',
                    'live PR identity unavailable')
    for role in ('head', 'base'):
        observed = value[role]
        receipt.require(observed['repo']['full_name'] == expected['repository']
                        and observed['ref'] == expected[role + '_ref'], 'live PR scope changed')
    return {role + '_sha': receipt.sha(value[role]['sha'], 'live ' + role) for role in ('head', 'base')}


def query(expected):
    endpoint = f"repos/{expected['repository']}/pulls/{expected['number']}"
    result = subprocess.run(['gh', 'api', '--method', 'GET', endpoint],
                            capture_output=True, text=True, timeout=10)
    receipt.require(result.returncode == 0 and len(result.stdout) <= 2 * 1024 * 1024,
                    'live PR read unavailable')
    return live_identity(json.loads(result.stdout), expected)


def probe(expected):
    if expected is None:
        return {'state': 'not-eligible'}
    try:
        first, second = query(expected), query(expected)
        if first != second:
            return {'state': 'unknown', 'reason': 'PR changed during readback'}
        same = all(first[k] == expected[k] for k in ('head_sha', 'base_sha'))
        return {'state': 'current' if same else 'obsolete', **first}
    except (ValueError, OSError, KeyError, TypeError, json.JSONDecodeError, subprocess.TimeoutExpired):
        # Unknown state never authorizes terminating work. Do not echo API bodies or credentials.
        return {'state': 'unknown', 'reason': 'live PR identity unavailable'}


class OwnedCommand:
    def __init__(self, command, cwd, env):
        status_read, status_write = os.pipe()
        release_read, release_write = os.pipe()
        try:
            self.leader = subprocess.Popen([sys.executable, str(HERE / '_ci-owned-command.py'),
                str(status_write), str(release_read), '--', *command], cwd=cwd, env=env,
                pass_fds=(status_write, release_read), start_new_session=True)
        except BaseException:
            os.close(status_read)
            os.close(release_write)
            raise
        finally:
            os.close(status_write)
            os.close(release_read)
        self.pid = self.leader.pid
        self.status_read, self.release_write = status_read, release_write
        self.returncode, self.buffer = None, b''

    def poll(self):
        if self.returncode is not None:
            return self.returncode
        ready, _, _ = select.select([self.status_read], [], [], 0)
        if ready:
            data = os.read(self.status_read, 64)
            self.buffer += data
            if b'\n' in self.buffer:
                line, _, self.buffer = self.buffer.partition(b'\n')
                self.returncode = int(line)
            elif not data:
                # A missing status is failure, never an empty success.
                self.returncode = 125
        return self.returncode

    def wait(self, timeout=None):
        deadline = None if timeout is None else time.monotonic() + timeout
        while self.poll() is None:
            if deadline is not None and time.monotonic() >= deadline:
                raise subprocess.TimeoutExpired('owned-command', timeout)
            time.sleep(0.01)
        return self.returncode

    def close(self):
        normal_release = not getattr(self, 'stopped', False)
        release_error = None
        try:
            if normal_release:
                try:
                    os.write(self.release_write, b'R')
                    deadline = time.monotonic() + 5
                    while b'\n' not in self.buffer:
                        remaining = deadline - time.monotonic()
                        receipt.require(remaining > 0, 'held session release acknowledgement timed out')
                        ready, _, _ = select.select([self.status_read], [], [], remaining)
                        receipt.require(ready, 'held session release acknowledgement timed out')
                        data = os.read(self.status_read, 64)
                        receipt.require(data, 'held session leader lost before release acknowledgement')
                        self.buffer += data
                    receipt.require(self.buffer == b'READY\n', 'invalid held session release acknowledgement')
                except (ValueError, OSError) as error:
                    release_error = error
                # Cleanup precedes reaping even when the command/leader already
                # exited. The direct child's PID cannot be reused in this gap.
                stop_owned(self)
            result = self.leader.wait(timeout=2.5)
            if normal_release:
                receipt.require(release_error is None and result == -signal.SIGKILL,
                                'held session leader failed before normal release')
        finally:
            os.close(self.release_write)
            os.close(self.status_read)


def stop_owned(process, graces=(7.5, 2.5)):
    # Keep the direct child unreaped until the final group signal so its PID/group
    # cannot be reused. Descendants still receive TERM/KILL if their leader exits.
    def send(action):
        try:
            os.killpg(process.pid, action)
            return True
        except ProcessLookupError:
            return False
        except PermissionError:
            # Darwin reports EPERM for a group containing only an unreaped zombie.
            # Reap only on this terminal path, then never signal that group again.
            if sys.platform == 'darwin' and process.leader.poll() is not None:
                return False
            raise
    if process.poll() is not None:
        try:
            inventory = subprocess.run(['ps', '-axo', 'pid=,pgid=,stat='], capture_output=True, text=True, timeout=2.5)
        except (OSError, subprocess.TimeoutExpired):
            inventory = None
        rows = [line.split() for line in inventory.stdout.splitlines()] if inventory is not None else []
        if inventory is not None and inventory.returncode == 0 and rows and all(len(row) == 3 and row[0].isdigit() and row[1].isdigit() for row in rows):
            others = [row for row in rows if int(row[1]) == process.pid and int(row[0]) != process.pid and not row[2].startswith('Z')]
            if not others:
                graces = (0, 0)
    for action, grace in ((signal.SIGINT, graces[0]), (signal.SIGTERM, graces[1])):
        if not send(action):
            break
        time.sleep(grace)
    else:
        send(signal.SIGKILL)
    process.stopped = True
    process.wait(timeout=2.5)


def supervise(command, root, expected, output, phase, interval=15.0):
    started = time.monotonic_ns()
    value = {'schema': 1, 'type': 'ci-pr-owned-work', 'phase': phase,
             'run_id': os.environ['GITHUB_RUN_ID'], 'run_attempt': os.environ['GITHUB_RUN_ATTEMPT'],
             'checkout_sha': receipt.git(root, 'rev-parse', 'HEAD'),
             'command_sha256': hashlib.sha256(json.dumps(command).encode()).hexdigest(),
             'pr_scope': None if expected is None else {k: expected[k] for k in ('repository', 'number', 'head_sha', 'base_sha')},
             'termination_scope': 'spawned-process-group-only', 'github_job_cancelled': False,
             'ability_proven': False, 'native_scheduler_proven': False,
             'observations': []}
    output = Path(output)
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open('x') as stream:
        json.dump({'schema': 1, 'type': value['type'], 'status': 'starting'}, stream)
    process, interrupted, prior = None, [], {}

    def observe():
        result = probe(expected)
        value['last_observation'] = result
        value['probe_count'] = value.get('probe_count', 0) + 1
        # Bound artifact size; every observation still reaches the original workflow log.
        if len(value['observations']) < 512:
            value['observations'].append(result)
        print('CI_PR_WORK_JSON ' + json.dumps({'phase': phase, **result}, sort_keys=True), flush=True)
        return result

    def interrupted_by(number, _frame):
        interrupted.append(number)

    try:
        if observe()['state'] == 'obsolete':
            value['status'] = 'obsolete-before-start'
            return OBSOLETE
        for number in (signal.SIGINT, signal.SIGTERM):
            prior[number] = signal.signal(number, interrupted_by)
        child_environment = dict(os.environ)
        child_environment.pop('GH_TOKEN', None)
        child_environment.pop('GITHUB_TOKEN', None)
        process = OwnedCommand(command, cwd=root, env=child_environment)
        next_probe = time.monotonic() + interval
        while process.poll() is None:
            if interrupted:
                value['status'] = 'interrupted'
                stop_owned(process)
                return 128 + interrupted[0]
            if expected is not None and time.monotonic() >= next_probe:
                current = observe()
                if current['state'] == 'obsolete':
                    value['status'] = 'obsolete-during-work'
                    stop_owned(process)
                    return OBSOLETE
                next_probe = time.monotonic() + interval
            try:
                process.wait(timeout=min(interval, 0.2))
            except subprocess.TimeoutExpired:
                pass
        if interrupted:
            value['status'] = 'interrupted'
            stop_owned(process)
            return 128 + interrupted[0]
        value['status'] = 'success' if process.returncode == 0 else 'command-failed'
        return process.returncode if process.returncode >= 0 else 128 - process.returncode
    finally:
        cleanup_error = None
        try:
            if process is not None and not getattr(process, 'stopped', False) and value.get('status') != 'success':
                stop_owned(process)
            if process is not None:
                process.close()
        except (ValueError, OSError, subprocess.TimeoutExpired) as error:
            cleanup_error = error
            value['status'] = 'supervisor-failed'
        if interrupted and cleanup_error is None:
            value['status'] = 'interrupted'
            value['interruption_signal'] = interrupted[0]
        for number, handler in prior.items():
            signal.signal(number, handler)
        value['elapsed_ns'] = time.monotonic_ns() - started
        value['command_exit_code'] = process.returncode if process is not None else None
        value.setdefault('status', 'supervisor-failed')
        output.write_text(json.dumps(value, indent=2, sort_keys=True) + '\n')
        print('CI_PR_WORK_JSON ' + json.dumps({k: v for k, v in value.items() if k != 'observations'}, sort_keys=True), flush=True)
        if cleanup_error is not None:
            raise cleanup_error
        if interrupted:
            return 128 + interrupted[0]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', required=True)
    parser.add_argument('--event-inputs', required=True)
    parser.add_argument('--output', required=True)
    parser.add_argument('--phase', choices=('prepare', 'verify'), required=True)
    parser.add_argument('command', nargs=argparse.REMAINDER)
    args = parser.parse_args()
    receipt.require(args.command[:1] == ['--'] and len(args.command) > 1, 'explicit command separator required')
    root = Path(args.root).resolve(strict=True)
    original_path = Path(args.event_inputs)
    expected = scope(root, json.loads(original_path.read_text()), os.environ)
    return supervise(args.command[1:], root, expected, args.output, args.phase)


if __name__ == '__main__':
    try:
        sys.exit(main())
    except (ValueError, OSError, KeyError, TypeError, json.JSONDecodeError) as error:
        print('ci-pr-work: ' + str(error), file=sys.stderr)
        sys.exit(1)
