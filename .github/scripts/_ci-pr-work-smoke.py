#!/usr/bin/env python3
"""Exercise original checkout binding, API readback and owned-process cancellation."""
import copy
import importlib.util
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import threading
import time

sys.dont_write_bytecode = True
HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location('ci_pr_work', HERE / '_ci-pr-work.py')
w = importlib.util.module_from_spec(spec)
spec.loader.exec_module(w)
checks = 0


def check(value, message):
    global checks
    assert value, message
    checks += 1


def rejected(call, message):
    try:
        call()
    except (ValueError, KeyError, TypeError, OSError):
        check(True, message)
    else:
        raise AssertionError(message)


with tempfile.TemporaryDirectory(prefix='ci-pr-work-') as temporary:
    root = Path(temporary)
    repo = root / 'repo'
    repo.mkdir()
    def git(*args):
        return subprocess.check_output(['git', '-C', str(repo), *args], text=True, stderr=subprocess.DEVNULL).strip()
    git('init', '-b', 'main')
    git('config', 'user.email', 'ci@example.invalid')
    git('config', 'user.name', 'CI fixture')
    (repo / 'one').write_text('base')
    git('add', '.')
    git('commit', '-m', 'Base')
    base = git('rev-parse', 'HEAD')
    git('checkout', '-b', 'feature')
    (repo / 'two').write_text('head')
    git('add', '.')
    git('commit', '-m', 'Head')
    head = git('rev-parse', 'HEAD')
    git('checkout', 'main')
    git('merge', '--no-ff', 'feature', '-m', 'Merge fixture')
    merge = git('rev-parse', 'HEAD')
    expected = {'repository': 'sample/repo', 'number': 12, 'head_sha': head, 'base_sha': base,
                'head_ref': 'feature', 'base_ref': 'main'}
    def live(head_sha=head, base_sha=base):
        return {'number': 12, 'state': 'open',
                'head': {'sha': head_sha, 'ref': 'feature', 'repo': {'full_name': 'sample/repo'}},
                'base': {'sha': base_sha, 'ref': 'main', 'repo': {'full_name': 'sample/repo'}}}
    payload = {'number': 12, 'action': 'synchronize', 'repository': {'full_name': 'sample/repo'},
               'pull_request': {'number': 12, 'head': live()['head'], 'base': live()['base'], 'labels': []}}
    event = root / 'event.json'
    event.write_text(json.dumps(payload))
    environment = {**os.environ, 'GITHUB_REPOSITORY': 'sample/repo', 'GITHUB_EVENT_NAME': 'pull_request',
                   'GITHUB_SHA': merge, 'GITHUB_RUN_ID': '123', 'GITHUB_RUN_ATTEMPT': '1',
                   'GITHUB_REF': 'refs/pull/12/merge', 'GITHUB_HEAD_REF': 'feature',
                   'GITHUB_BASE_REF': 'main', 'GITHUB_EVENT_PATH': str(event)}
    # Existing fixture environments must not accidentally supply a different workflow identity.
    for key in ('GITHUB_WORKFLOW_REF', 'GITHUB_WORKFLOW_SHA', 'GITHUB_JOB'):
        environment.pop(key, None)
    environment.update(GITHUB_WORKFLOW_REF='sample/repo/.github/workflows/ci.yml@refs/pull/12/merge',
                       GITHUB_WORKFLOW_SHA=merge,GITHUB_JOB='shard-plan')
    original = w.receipt.event_snapshot(repo, payload, environment)
    environment['GITHUB_JOB'] = 'test-shards'
    def planner_snapshot(body, env=environment):
        return w.receipt.event_snapshot(repo, body, {**env, 'GITHUB_JOB': 'shard-plan'})
    check(w.scope(repo, original, environment)['head_sha'] == head, 'actual merge checkout binds the original head')
    missing_environment = {k: v for k, v in environment.items() if k not in ('GITHUB_WORKFLOW_REF', 'GITHUB_WORKFLOW_SHA', 'GITHUB_JOB')}
    missing_original = w.receipt.event_snapshot(repo, payload, missing_environment)
    check(w.scope(repo, missing_original, missing_environment) is None, 'missing workflow binding never authorizes termination')
    for field, replacement in [('workflow_sha','f'*40), ('job','foreign')]:
        altered_original = copy.deepcopy(original)
        altered_original['workflow_source'][field] = replacement
        rejected(lambda: w.scope(repo, altered_original, environment), 'workflow/job provenance must agree')
    changed = copy.deepcopy(original)
    changed['pull_request']['head_sha'] = 'f' * 40
    rejected(lambda: w.scope(repo, changed, environment), 'changed captured source cannot authorize supervision')
    altered = {**environment, 'GITHUB_SHA': head}
    rejected(lambda: w.scope(repo, original, altered), 'actual checkout mismatch rejects before work')
    fork = copy.deepcopy(payload)
    fork['pull_request']['head']['repo']['full_name'] = 'foreign/repo'
    event.write_text(json.dumps(fork))
    fork_original = planner_snapshot(fork)
    check(w.scope(repo, fork_original, environment) is None, 'fork is never eligible to terminate work')
    incomplete = {**payload, 'pull_request': None}
    event.write_text(json.dumps(incomplete))
    incomplete_original = planner_snapshot(incomplete)
    check(w.scope(repo, incomplete_original, environment) is None, 'missing event payload cannot authorize termination')
    push = {'repository': {'full_name': 'sample/repo'}, 'ref': 'refs/heads/main', 'after': merge}
    event.write_text(json.dumps(push))
    push_env = {**environment, 'GITHUB_EVENT_NAME': 'push', 'GITHUB_REF': 'refs/heads/main'}
    push_original = planner_snapshot(push, push_env)
    check(w.scope(repo, push_original, push_env) is None, 'main push remains outside PR supervision')
    event.write_text(json.dumps(payload))
    check(w.live_identity(live(), expected) == {'head_sha': head, 'base_sha': base}, 'current API head and base parsed')
    for key, value in [('number', True), ('number', 13), ('state', 'closed')]:
        changed = live(); changed[key] = value
        rejected(lambda: w.live_identity(changed, expected), 'foreign or unavailable live PR rejected')
    for role in ('head', 'base'):
        for field, value in [('ref', 'other'), ('repo', {'full_name': 'foreign/repo'}), ('sha', 'bad')]:
            changed = live(); changed[role][field] = value
            rejected(lambda: w.live_identity(changed, expected), 'live repository/ref/SHA is scoped')
    original_query = w.query
    for values, state in [([{'head_sha': head, 'base_sha': base}] * 2, 'current'),
                          ([{'head_sha': 'f'*40, 'base_sha': base}] * 2, 'obsolete'),
                          ([{'head_sha': head, 'base_sha': 'f'*40}] * 2, 'obsolete'),
                          ([{'head_sha': head, 'base_sha': base}, {'head_sha': 'f'*40, 'base_sha': base}], 'unknown')]:
        iterator = iter(values)
        w.query = lambda _: next(iterator)
        check(w.probe(expected)['state'] == state, 'two reads must agree before obsolete work may stop')
    w.query = lambda _: (_ for _ in ()).throw(OSError('private response must not escape'))
    check(w.probe(expected) == {'state': 'unknown', 'reason': 'live PR identity unavailable'}, 'API failure is unknown without raw diagnostics')
    check(w.probe(None)['state'] == 'not-eligible', 'ineligible scope performs no API read')
    w.query = original_query
    prior_environment = dict(os.environ)
    os.environ.update(environment)
    original_probe, original_stop = w.probe, w.stop_owned
    def short_stop(process):
        # Coordinate on the private command-status pipe before shortening the
        # remaining descendant cleanup grace; do not assume a scheduler delay.
        os.killpg(process.pid, signal.SIGINT)
        try:
            process.wait(timeout=3)
        except subprocess.TimeoutExpired:
            pass
        return original_stop(process, graces=(0.05, 0.05))
    w.stop_owned = short_stop
    unrelated = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(30)'], start_new_session=True)
    try:
        marker = root / 'started'
        argv = [sys.executable, '-c', 'from pathlib import Path; import sys; Path(sys.argv[1]).write_text("ran")', str(marker)]
        w.probe = lambda _: {'state': 'obsolete', 'head_sha': 'f'*40, 'base_sha': base}
        out = root / 'stale-before.json'
        check(w.supervise(argv, repo, expected, out, 'prepare', interval=0.02) == w.OBSOLETE
              and not marker.exists(), 'delayed old admission never starts its command or touches the newer worker')
        check(json.loads(out.read_text())['status'] == 'obsolete-before-start', 'pre-start rejection is explicit')
        rejected(lambda: w.supervise(argv, repo, expected, out, 'prepare', interval=0.02), 'previous work receipt cannot be reused')
        w.probe = lambda _: {'state': 'unknown'}
        check(w.supervise(argv, repo, expected, root / 'unknown.json', 'prepare', interval=0.02) == 0
              and marker.read_text() == 'ran', 'unknown API state never terminates work')
        w.probe = lambda _: {'state': 'current'}
        check(w.supervise([sys.executable, '-c', 'raise SystemExit(23)'], repo, expected,
                           root / 'failed.json', 'verify', interval=0.02) == 23, 'original command failure is preserved')
        child = root / 'owned-child.py'
        child.write_text('''import os, signal, subprocess, sys, time
from pathlib import Path
signal.signal(signal.SIGINT, lambda *_: sys.exit(0))
leaf = subprocess.Popen([sys.executable, '-c', 'import os,signal,time; from pathlib import Path; signal.signal(signal.SIGINT, signal.SIG_IGN); signal.signal(signal.SIGTERM, signal.SIG_IGN); Path(os.environ[\"LEAF_READY\"]).write_text(str(os.getpid())); time.sleep(30)'])
while not Path(os.environ['LEAF_READY']).exists(): time.sleep(0.01)
Path(sys.argv[1]).write_text(str(os.getpid()))
time.sleep(30)
''')
        def after_start(_):
            return {'state': 'obsolete' if marker.exists() else 'current'}
        marker.unlink()
        leaf_ready = root / 'leaf-ready'
        os.environ['LEAF_READY'] = str(leaf_ready)
        w.probe = after_start
        before = time.monotonic()
        out = root / 'stale-during.json'
        check(w.supervise([sys.executable, str(child), str(marker)], repo, expected, out, 'verify', interval=0.02) == w.OBSOLETE,
              'a fresh obsolete-head observation stops active owned work')
        check(time.monotonic() - before < 5, 'owned process cleanup is bounded')
        saved = json.loads(out.read_text())
        check(saved['status'] == 'obsolete-during-work' and saved['command_exit_code'] == 0,
              'obsolete worker remains failure even if its command handles interrupt and exits zero')
        check(saved['github_job_cancelled'] is False and saved['ability_proven'] is False,
              'process interruption is not reported as native job cancellation or capability proof')
        leaf_pid = int(leaf_ready.read_text())
        leaf_state = subprocess.run(['ps', '-o', 'stat=', '-p', str(leaf_pid)], capture_output=True, text=True).stdout.strip()
        check(not leaf_state or leaf_state.startswith('Z'), 'descendant ignoring INT and TERM cannot survive leader exit')
        check(unrelated.poll() is None, 'another process group remains alive')
        exited = root / 'command-exited'
        leaf = root / 'exit-leaf-ready'
        os.environ['LEAF_READY'] = str(leaf)
        child.write_text(child.read_text().replace('time.sleep(30)\n', 'time.sleep(30)\n', 1).rsplit('time.sleep(30)\n',1)[0] + 'Path(sys.argv[2]).touch()\n')
        query_count = 0
        def stale_after_command_exit(_):
            global query_count
            query_count += 1
            if query_count == 1: return {'state': 'current'}
            deadline = time.monotonic() + 5
            while not exited.exists() and time.monotonic() < deadline: time.sleep(0.01)
            check(exited.exists(), 'actual command exited during PR readback')
            time.sleep(0.05)
            return {'state': 'obsolete'}
        w.probe = stale_after_command_exit
        out = root / 'real-descendant-exit.json'
        check(w.supervise([sys.executable, str(child), str(root/'exit-started'), str(exited)], repo, expected, out, 'verify', interval=0) == w.OBSOLETE,
              'confirmed obsolescence survives actual command exit during API readback')
        leaf_pid = int(leaf.read_text())
        leaf_state = subprocess.run(['ps', '-o', 'stat=', '-p', str(leaf_pid)], capture_output=True, text=True).stdout.strip()
        check(not leaf_state or leaf_state.startswith('Z'), 'held session leader permits cleanup after command exit')
        # Deterministically place exit zero at the end of a wait/readback boundary.
        saved_owned, saved_git, saved_stop = w.OwnedCommand, w.receipt.git, w.stop_owned
        holder = {}
        class FinishedAtBoundary:
            def __init__(self, *args, **kwargs):
                self.returncode = None
                holder['process'] = self
            def close(self): pass
            def poll(self): return self.returncode
            def wait(self, timeout=None):
                self.returncode = 0
                signal.getsignal(signal.SIGTERM)(signal.SIGTERM, None)
                return 0
        try:
            w.OwnedCommand = FinishedAtBoundary
            w.stop_owned = lambda process: None
            w.receipt.git = lambda *_: merge
            w.probe = lambda _: {'state': 'current'}
            out = root / 'interrupt-exit-zero.json'
            check(w.supervise(['owned-fixture'], repo, expected, out, 'verify', interval=1) == 128 + signal.SIGTERM,
                  'interruption dominates an exit-zero child at the end of wait')
            check(json.loads(out.read_text())['status'] == 'interrupted', 'exit-zero interruption never publishes success')
            class InterruptedAtClose(FinishedAtBoundary):
                def wait(self, timeout=None):
                    self.returncode = 0
                    return 0
                def close(self):
                    signal.getsignal(signal.SIGTERM)(signal.SIGTERM, None)
            w.OwnedCommand = InterruptedAtClose
            out = root / 'interrupt-at-close.json'
            check(w.supervise(['owned-fixture'], repo, expected, out, 'verify', interval=1) == 128 + signal.SIGTERM,
                  'interrupt received during release cannot preserve pending exit zero')
            check(json.loads(out.read_text())['status'] == 'interrupted', 'release interruption is recorded as interrupted')
            w.OwnedCommand = FinishedAtBoundary
            calls = 0
            def obsolete_at_exit(_):
                global calls
                calls += 1
                if calls > 1:
                    holder['process'].returncode = 0
                    return {'state': 'obsolete'}
                return {'state': 'current'}
            w.probe = obsolete_at_exit
            out = root / 'obsolete-exit-zero.json'
            check(w.supervise(['owned-fixture'], repo, expected, out, 'verify', interval=0) == w.OBSOLETE,
                  'obsolete observation dominates concurrent exit zero')
            check(json.loads(out.read_text())['status'] == 'obsolete-during-work', 'obsolete exit zero never publishes success')
        finally:
            w.OwnedCommand, w.receipt.git, w.stop_owned = saved_owned, saved_git, saved_stop
        saved_owned = w.OwnedCommand
        def capture_owned(*args, **kwargs):
            command = saved_owned(*args, **kwargs)
            holder['owned'] = command
            return command
        w.OwnedCommand = capture_owned
        probe_count = 0
        def lose_leader_after_status(_):
            global probe_count
            probe_count += 1
            if probe_count > 1:
                command = holder['owned']
                check(command.wait(timeout=5) == 0, 'leader-loss control reached command status zero')
                os.kill(command.pid, signal.SIGKILL)
            return {'state': 'current'}
        w.probe = lose_leader_after_status
        out = root / 'lost-leader.json'
        lost_leaf = root / 'lost-leader-leaf'
        os.environ['LEAF_READY'] = str(lost_leaf)
        try:
            rejected(lambda: w.supervise([sys.executable,str(child),str(root/'lost-leader-started'),str(root/'lost-leader-command-exit')], repo, expected, out, 'verify', interval=0),
                     'leader loss after command status zero cannot publish success')
            check(json.loads(out.read_text())['status'] == 'supervisor-failed', 'failed leader release has an explicit failed receipt')
            leaf_state = subprocess.run(['ps','-o','stat=','-p',lost_leaf.read_text()],capture_output=True,text=True).stdout.strip()
            check(not leaf_state or leaf_state.startswith('Z'), 'abnormal leader release also cleans its surviving descendant')
        finally:
            w.OwnedCommand = saved_owned
        # Credentials are available to the read-only API client, not the supervised command.
        os.environ['GH_TOKEN'] = 'fixture-private-token'
        os.environ['GITHUB_TOKEN'] = 'fixture-private-token'
        w.probe = lambda _: {'state': 'current'}
        token_check = 'import os; assert "GH_TOKEN" not in os.environ and "GITHUB_TOKEN" not in os.environ'
        out = root / 'tokens.json'
        check(w.supervise([sys.executable, '-c', token_check], repo, expected, out, 'verify', interval=0.02) == 0,
              'API credentials are not passed to the build/test command')
        check('fixture-private-token' not in out.read_text(), 'hash-only receipt excludes raw credentials')
        ready = root / 'interrupt-ready'
        def interrupt_when_ready():
            deadline = time.monotonic() + 5
            while not ready.exists() and time.monotonic() < deadline:
                time.sleep(0.01)
            if ready.exists():
                os.kill(os.getpid(), signal.SIGTERM)
        interrupter = threading.Thread(target=interrupt_when_ready)
        interrupter.start()
        out = root / 'interrupted.json'
        command = [sys.executable, '-c', 'import sys,time; from pathlib import Path; Path(sys.argv[1]).touch(); time.sleep(10)', str(ready)]
        check(w.supervise(command, repo, expected, out, 'prepare', interval=0.02) == 128 + signal.SIGTERM,
              'external supervisor interruption propagates to the owned command')
        interrupter.join(timeout=5)
        check(json.loads(out.read_text())['status'] == 'interrupted', 'external interruption is never recorded as success')
    finally:
        os.killpg(unrelated.pid, signal.SIGKILL)
        unrelated.wait()
        w.probe, w.stop_owned = original_probe, original_stop
        os.environ.clear(); os.environ.update(prior_environment)
    # Losing the supervisor must not leave its separate command session alive.
    parent_driver = root / 'parent-driver.py'
    leaf = root / 'abandoned-command'
    leader = root / 'abandoned-leader'
    parent_driver.write_text('import importlib.util,os,sys,time\nsys.dont_write_bytecode=True\nfrom pathlib import Path\n'
        + 'spec=importlib.util.spec_from_file_location("work",'+repr(str(HERE/'_ci-pr-work.py'))+')\n'
        + 'work=importlib.util.module_from_spec(spec); spec.loader.exec_module(work)\n'
        + 'command=[sys.executable,"-c","import os,sys,time; from pathlib import Path; Path(sys.argv[1]).write_text(str(os.getpid())); time.sleep(30)",sys.argv[1]]\n'
        + 'owned=work.OwnedCommand(command,cwd=Path.cwd(),env=dict(os.environ))\n'
        + 'Path(sys.argv[2]).write_text(str(owned.pid))\ntime.sleep(30)\n')
    parent = subprocess.Popen([sys.executable,str(parent_driver),str(leaf),str(leader)],cwd=repo,start_new_session=True)
    try:
        deadline=time.monotonic()+5
        while not (leaf.exists() and leader.exists()) and time.monotonic()<deadline: time.sleep(0.01)
        check(leaf.exists() and leader.exists(), 'abandonment control reached actual command readiness')
        os.kill(parent.pid, signal.SIGKILL)
        parent.wait()
        deadline=time.monotonic()+13
        while time.monotonic()<deadline:
            states=[subprocess.run(['ps','-o','stat=','-p',path.read_text()],capture_output=True,text=True).stdout.strip() for path in (leaf,leader)]
            if all(not state or state.startswith('Z') for state in states): break
            time.sleep(0.05)
        check(all(not state or state.startswith('Z') for state in states), 'abandoned session stops after supervisor loss')
    finally:
        if parent.poll() is None:
            os.kill(parent.pid,signal.SIGKILL); parent.wait()
    # Close the status reader after sending release, before READY can be read.
    status_read, status_write = os.pipe()
    release_read, release_write = os.pipe()
    ack_leaf = root / 'ack-loss-leaf'
    ack_env = {**os.environ, 'LEAF_READY': str(ack_leaf)}
    ack_leader = subprocess.Popen([sys.executable,str(HERE/'_ci-owned-command.py'),str(status_write),str(release_read),'--',
        sys.executable,str(child),str(root/'ack-started'),str(root/'ack-command-exit')],
        env=ack_env,pass_fds=(status_write,release_read),start_new_session=True)
    os.close(status_write); os.close(release_read)
    try:
        with os.fdopen(status_read,'rb') as status_stream:
            check(status_stream.readline().strip() == b'0', 'READY-loss control reached actual command exit zero')
        os.write(release_write,b'R')
        os.close(release_write)
        release_write = None
        result = ack_leader.wait(timeout=13)
        state = subprocess.run(['ps','-o','stat=','-p',ack_leaf.read_text()],capture_output=True,text=True).stdout.strip()
        check(result == -signal.SIGKILL and (not state or state.startswith('Z')),
              'supervisor loss during READY acknowledgement stops remaining descendants')
    finally:
        if release_write is not None: os.close(release_write)
        if ack_leader.poll() is None:
            os.killpg(ack_leader.pid,signal.SIGKILL); ack_leader.wait()
        if ack_leaf.exists():
            leaf_pid=int(ack_leaf.read_text())
            state=subprocess.run(['ps','-o','stat=','-p',str(leaf_pid)],capture_output=True,text=True).stdout.strip()
            if state and not state.startswith('Z'):
                os.kill(leaf_pid,signal.SIGKILL)
    api_file = root / 'live.json'
    api_file.write_text(json.dumps(live()))
    gh = root / 'gh'
    gh.write_text('#!' + sys.executable + '\nimport json,os,sys\nfrom pathlib import Path\n'
                  'with open(os.environ["FAKE_API_LOG"],"a") as out: out.write(json.dumps(sys.argv[1:])+"\\n")\n'
                  'if os.environ.get("FAKE_API_FAIL") == "true": raise SystemExit(5)\n'
                  'print(Path(os.environ["FAKE_API_FILE"]).read_text())\n')
    gh.chmod(0o755)
    event_file = root / 'original.json'
    event_file.write_text(json.dumps(original))
    api_log = root / 'api.log'
    cli_env = {**environment, 'PATH': str(root) + os.pathsep + os.environ['PATH'],
               'FAKE_API_LOG': str(api_log), 'FAKE_API_FILE': str(api_file), 'GH_TOKEN': 'fixture-only-secret'}
    for name, data, extra, expected_exit in [
        ('current', live(), {}, 0),
        ('obsolete', live(head_sha='f'*40), {}, w.OBSOLETE),
        ('base-advanced', live(base_sha='f'*40), {}, w.OBSOLETE),
        ('unavailable', live(), {'FAKE_API_FAIL': 'true'}, 0),
        ('malformed', {'private_body': 'must-not-leak'}, {}, 0),
    ]:
        api_file.write_text(json.dumps(data))
        marker = root / ('cli-marker-' + name)
        command = [sys.executable, '-c', 'from pathlib import Path; import sys; Path(sys.argv[1]).touch()', str(marker)]
        out = root / ('cli-' + name + '.json')
        result = subprocess.run(['bash', str(HERE / 'ci-pr-work.sh'), '--root', str(repo),
                                 '--event-inputs', str(event_file), '--output', str(out), '--phase', 'prepare',
                                 '--', *command], env={**cli_env, **extra}, capture_output=True, text=True, timeout=10)
        check(result.returncode == expected_exit and marker.exists() == (expected_exit == 0),
              'actual CLI preserves current, obsolete and unknown API behavior: '+name+' exit='+str(result.returncode)+' '+result.stdout+' '+result.stderr)
        check('fixture-only-secret' not in result.stdout + result.stderr + out.read_text()
              and 'must-not-leak' not in result.stdout + result.stderr + out.read_text(), 'CLI logs and artifact contain no raw API body/token')
    calls = [json.loads(line) for line in api_log.read_text().splitlines()]
    check(calls and all(row == ['api', '--method', 'GET', 'repos/sample/repo/pulls/12'] for row in calls),
          'actual GH client performs only scoped read-only PR calls')
print('CI owned PR work controls passed:', checks)
