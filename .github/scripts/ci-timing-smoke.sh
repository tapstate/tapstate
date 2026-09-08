#!/usr/bin/env bash
# Offline contract cases: the fake API insists on query filters, pagination and attempt pinning.
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
export TIMING_GATE="$here/ci-timing.sh"
scratch="$(mktemp -d)"
trap 'rm -rf "$scratch"' EXIT
mkdir -p "$scratch/bin"
cat > "$scratch/bin/gh" <<'PY'
#!/usr/bin/env python3
import json
import os
import sys
from urllib.parse import parse_qs, urlsplit
mode = os.environ.get('TIMING_CASE', 'ok')
if mode == 'api-error':
    print('permission denied', file=sys.stderr)
    sys.exit(1)
if sys.argv[1:3] == ['repo', 'view']:
    print('example/project')
    sys.exit(0)
assert sys.argv[1] == 'api', sys.argv
url = urlsplit(sys.argv[2])
path, query = url.path, parse_qs(url.query)
def time(seconds):
    return f'2026-09-08T00:{seconds // 60:02}:{seconds % 60:02}Z'
def run(i):
    return dict(id=i, run_attempt=2 if i == 1 else 1, status='completed',
                conclusion='success', head_sha=str(i) * 40, event='push',
                head_branch='main', created_at=time(0), run_started_at=time(10),
                html_url=f'https://github.com/example/project/actions/runs/{i}')
def job(i, name='build'):
    return dict(id=100+i, run_id=i, run_attempt=2 if i == 1 else 1,
                name=name, status='completed', conclusion='success',
                started_at=time(20), completed_at=time(80 if i == 1 else 120),
                html_url=f'https://github.com/example/project/actions/runs/{i}/job/{100+i}',
                labels=['ubuntu-latest'], runner_name='Hosted Agent',
                steps=[dict(number=1, name='Build', status='completed', conclusion='success',
                            started_at=time(25), completed_at=time(70 if i == 1 else 100)),
                       dict(number=2, name='Optional', status='completed', conclusion='skipped',
                            started_at=None, completed_at=None)])
if '/workflows/' in path:
    assert path == 'repos/example/project/actions/workflows/ci.yml/runs', path
    assert query.get('status') == ['success'], query
    assert query.get('event') == ['push'], query
    assert query.get('branch') == ['main'], query
    page = int(query['page'][0])
    data = [run(1)] if page == 1 else ([run(2)] if page == 2 else [])
    if mode == 'empty': data = []
    if mode == 'bad-json':
        print('invalid JSON')
        sys.exit(0)
    print(json.dumps(dict(total_count=0 if mode == 'empty' else 2, workflow_runs=data)))
elif '/attempts/' in path:
    i = int(path.split('/runs/')[1].split('/')[0])
    attempt = 2 if i == 1 else 1
    assert path == f'repos/example/project/actions/runs/{i}/attempts/{attempt}/jobs', path
    page = int(query['page'][0])
    data = [job(i)] if page == 1 else ([job(i, 'lint')] if page == 2 else [])
    if mode == 'missing-time' and page == 1: data[0]['started_at'] = None
    if mode == 'wrong-attempt' and page == 1: data[0]['run_attempt'] = 99
    if mode == 'reversed-time' and page == 1: data[0]['completed_at'] = time(1)
    if mode == 'empty-jobs': data = []
    # Distinct ids matter even when job names vary between runs.
    if page == 2 and data: data[0]['id'] += 1000
    print(json.dumps(dict(total_count=0 if mode == 'empty-jobs' else 2, jobs=data)))
elif path.endswith('/logs'):
    if mode == 'ansi' and '--allow-escape-sequences' not in sys.argv:
        print('the response contains terminal escape sequences; pass --allow-escape-sequences to output it anyway', file=sys.stderr)
        sys.exit(1)
    assert path in ['repos/example/project/actions/jobs/101/logs',
                    'repos/example/project/actions/jobs/102/logs'], path
    if mode != 'empty-log':
        print('2026-09-08T00:00:30Z [INFO] Reactor Summary for Example 1.0:')
        print('2026-09-08T00:00:30Z \x1b[32m[INFO] Example :: Core ........ SUCCESS [  1.250 s]\x1b[0m')
        print('2026-09-08T00:00:30Z [INFO] Example :: Engine ...... SUCCESS [ 01:02 min]')
        print('2026-09-08T00:00:30Z [INFO] -------------------------------------------------')
        print('2026-09-08T00:00:30Z [INFO] Tests run: 5, Failures: 0, Errors: 0, Skipped: 1, Time elapsed: 1,234.567 s -- in io.example.HeavyTest')
        print('2026-09-08T00:00:31Z [INFO] Tests run: 2, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 0.012 s - in io.example.TinyTest')
        # Aggregate summaries and echoed XML are not per-class test results.
        print('2026-09-08T00:00:32Z [INFO] Tests run: 7, Failures: 0, Errors: 0, Skipped: 1')
        print('<testsuite name="io.example.XMLTest" time="9000"/>')
        print('2026-09-08T00:00:33Z [INFO] Reactor Summary for Example 1.0:')
        print('2026-09-08T00:00:33Z [INFO] Example :: Core ........ SUCCESS [  0.001 s]')
        print('2026-09-08T00:00:33Z [INFO] Example :: Long ........ SUCCESS [01:02 h]')
        print('2026-09-08T00:00:33Z [INFO] -------------------------------------------------')
else:
    raise AssertionError(sys.argv)
PY
chmod +x "$scratch/bin/gh"
export PATH="$scratch/bin:$PATH"
python3 - <<'PY'
import json
import os
import subprocess
base = ['bash', os.environ['TIMING_GATE'], 'ci.yml', '--repo', 'example/project', '--runs', '2', '--event', 'push',
        '--branch', 'main', '--job', 'build', '--modules', '--json']
passed = 0
def call(case='ok', argv=None):
    return subprocess.run(base if argv is None else argv, text=True, capture_output=True,
                          env={**os.environ, 'TIMING_CASE': case})
def check(condition, message):
    global passed
    assert condition, message
    passed += 1
result = call()
check(result.returncode == 0, result.stderr)
data = json.loads(result.stdout)
check(data['sample_count'] == 2, 'pagination must collect the requested sample count')
check(data['workflow_wall_seconds'] == {'count': 2, 'median': 100.0, 'max': 120.0}, 'wall includes initial queue')
check(data['jobs']['build']['runtime_seconds'] == {'count': 2, 'median': 80.0, 'max': 100.0}, 'job runtime is separate')
check(data['jobs']['lint']['runtime_seconds']['count'] == 2, 'job pagination')
check(data['steps']['1: Build']['runtime_seconds']['median'] == 60.0, 'step median')
check(data['steps']['2: Optional']['runtime_seconds']['count'] == 0 and
      data['steps']['2: Optional']['skipped'] == 2, 'skipped steps are not zero-duration samples')
check(data['runs'][0]['attempt'] == 2 and data['runs'][0]['head_sha'] == '1'*40,
      'attempt and SHA provenance')
check(data['runs'][0]['jobs'][0]['runner_labels'] == ['ubuntu-latest'], 'runner provenance')
check(data['runs'][0]['modules'][1]['seconds'] == 62.0, 'minute duration')
check(data['runs'][0]['test_classes'][0]['seconds'] == 1234.567, 'decimal precision and grouping commas')
check(data['runs'][0]['modules'][2]['reactor_index'] == 2 and
      data['runs'][0]['modules'][0]['reactor_index'] == 1, 'keep repeated reactor summaries separate')
check(data['runs'][0]['modules'][3]['seconds'] == 3720.0, 'hour and minute duration units differ')
check(len(data['runs'][0]['test_classes']) == 2, 'do not count XML or aggregate summaries')
for case in ['api-error', 'empty', 'bad-json', 'missing-time', 'wrong-attempt',
             'reversed-time', 'empty-jobs', 'empty-log']:
    result = call(case)
    check(result.returncode != 0 and not result.stdout, f'{case} must fail without a partial report: {result}')
for tail in [['--runs', '0'], ['--runs', 'x'], ['--event', 'schedule'], ['--runs'],
             ['--job', 'absent'], ['--runs', '3']]:
    result = call(argv=base+tail)
    check(result.returncode != 0, f'invalid or unavailable selection: {tail}')
result = call('ansi')
check(result.returncode == 0 and json.loads(result.stdout)['runs'][0]['modules'][0]['seconds'] == 1.25,
      'gh escape-sequence refusal retries with explicit allowance; ANSI is stripped')
result = call(argv=base[:-1])
check(result.returncode == 0 and 'Workflow wall' in result.stdout and '1234.567' in result.stdout,
      'human-readable timings preserve useful precision')
print(f'ci-timing smoke: {passed} assertions passed')
PY
