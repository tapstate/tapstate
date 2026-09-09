#!/usr/bin/env bash
# Cache identity follows freshly read server engine and analyzer content hashes.
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
python3 - "$here" <<'PY'
from pathlib import Path
import json
import os
import re
import subprocess
import sys
import tempfile

scripts = Path(sys.argv[1])
with tempfile.TemporaryDirectory() as temporary:
    root = Path(temporary)
    plugins = root / 'plugins.json'
    engine = root / 'engine.txt'
    calls = root / 'calls'
    baseline = {'plugins': [{'key': 'java', 'hash': 'a' * 32}, {'key': 'branch', 'hash': 'b' * 32}]}
    plugins.write_text(json.dumps(baseline))
    engine.write_text('scanner-engine.jar|' + 'c' * 32 + '\n')
    curl = root / 'curl'
    curl.write_text('''#!/usr/bin/env python3
import os, pathlib, sys
args = sys.argv[1:]
assert '--fail' in args and '--max-time' in args
assert args[args.index('--user') + 1] == 'fixture-token:'
url = args[-1]
endpoint = 'plugins' if url.endswith('/api/plugins/installed') else 'engine'
assert url.endswith('/api/plugins/installed') or url.endswith('/batch/index')
with open(os.environ['CALLS'], 'a') as stream: stream.write(endpoint + '\\n')
if os.environ.get('FAIL_ENDPOINT') == endpoint: sys.exit(7)
print(pathlib.Path(os.environ[endpoint.upper()]).read_text(), end='')
''')
    curl.chmod(0o755)
    env = dict(os.environ, PATH=str(root) + os.pathsep + os.environ['PATH'],
               SONAR_HOST_URL='https://sonar.example.invalid', SONAR_TOKEN='fixture-token',
               PLUGINS=str(plugins), ENGINE=str(engine), CALLS=str(calls))

    def key(ok=True, **extra):
        result = subprocess.run(['bash', str(scripts / 'sonar-cache-key.sh')],
                                env=dict(env, **extra), capture_output=True, text=True)
        assert (result.returncode == 0) == ok, result.stdout + result.stderr
        assert 'fixture-token' not in result.stdout + result.stderr
        if not ok:
            assert not result.stdout, 'failed input must not emit a usable cache key'
            return None
        values = dict(line.split('=', 1) for line in result.stdout.splitlines())
        assert set(values) == {'server', 'manifest'}
        assert all(len(value) == 64 and all(c in '0123456789abcdef' for c in value) for value in values.values())
        return values

    first = key()
    assert first == key(), 'identical content must reuse cache'
    assert calls.read_text().splitlines() == ['plugins', 'engine'] * 2, 'inputs must be fetched on every run'
    assert first == key(SONAR_HOST_URL=env['SONAR_HOST_URL'] + '/'), 'trailing slash changes server identity'
    assert first['server'] != key(SONAR_HOST_URL='https://other.example.invalid')['server']
    plugins.write_text(json.dumps({'plugins': list(reversed(baseline['plugins']))}))
    assert first == key(), 'server list ordering is not a content change'
    plugins.write_text(json.dumps({'plugins': [{'key': 'java', 'hash': 'd' * 32}, baseline['plugins'][1]]}))
    assert first['manifest'] != key()['manifest'], 'analyzer update must create a writable new cache key'
    plugins.write_text(json.dumps(baseline))
    engine.write_text('scanner-engine.jar|' + 'e' * 32 + '\n')
    assert first['manifest'] != key()['manifest'], 'scanner engine update must invalidate the key'
    engine.write_text('scanner-engine.jar|' + 'c' * 32 + '\n')
    key(False, FAIL_ENDPOINT='plugins')
    key(False, FAIL_ENDPOINT='engine')
    for invalid in ['broken json', '{}', '{"plugins":[]}', json.dumps({'plugins': [baseline['plugins'][0]] * 2}), '{"plugins":[{"key":"java"}]}']:
        plugins.write_text(invalid)
        key(False)
    plugins.write_text(json.dumps(baseline))
    engine.write_text('')
    key(False)
    engine.write_text('<html>proxy error</html>')
    key(False)

workflow = (scripts.parent / 'workflows/ci.yml').read_text()
build = workflow.split('\n  build:\n', 1)[1].split('\n  sonarqube:\n', 1)[0]
assert 'run: .github/scripts/sonar-cache-key.sh >> "$GITHUB_OUTPUT"' in build
assert 'path: ~/.sonar/cache' in build, 'Maven cache does not contain analyzer binaries'
assert 'steps.sonar-cache-key.outputs.server' in build and 'steps.sonar-cache-key.outputs.manifest' in build
assert 'steps.sonar-cache-key.outcome' in build, 'cache lookup failure must fall back to normal analysis'
assert build.index('Identify Sonar cache inputs') < build.index('Restore Sonar analyzer binaries') < build.index('Analyze admitted coverage')
def step(name):
    body = build.split('      - name: ' + name + '\n', 1)[1]
    return body.split('\n      - ', 1)[0]
identify = step('Identify Sonar cache inputs')
restore = step('Restore Sonar analyzer binaries')
analyze = step('Analyze admitted coverage')
assert '        continue-on-error: true' in identify.splitlines()
assert '        continue-on-error: true' in restore.splitlines()
assert '        if: steps.guard.outputs.run == \'true\'' in identify.splitlines()
assert '          SONAR_TOKEN: ${{ secrets.SONAR_TOKEN }}' in identify.splitlines()
assert '          SONAR_HOST_URL: ${{ secrets.SONAR_HOST_URL }}' in identify.splitlines()
assert '        if: steps.guard.outputs.run == \'true\'' in analyze.splitlines(), 'a cache hit must never skip analysis'
assert '-Dsonar.qualitygate.wait=true' in analyze
assert re.search(r'^          restore-keys: \|\n\s+sonar-v1-.*outputs.server.*-$', restore, re.M), 'updated plugins should reuse unchanged binaries'
assert 'bash .github/scripts/sonar-cache-smoke.sh' in workflow
assert '.github/scripts/sonar-cache-key.sh .github/scripts/sonar-cache-smoke.sh' in workflow
print('sonar cache smoke: fresh server inputs, analyzer/engine invalidation, failure fallback and workflow wiring passed')
PY
