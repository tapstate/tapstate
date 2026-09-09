#!/usr/bin/env bash
# Exercise executable calls and the event policy, including the live workflow wiring.
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
python3 - "$here" <<'PY'
from pathlib import Path
import subprocess
import sys
import tempfile

scripts = Path(sys.argv[1])
gate = scripts / 'one-verify.sh'


def workflow(command='mvn -B verify', trigger='[push]', extra=''):
    return f'''name: Fixture
on: {trigger}
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - run: |
{''.join('          ' + line + chr(10) for line in command.splitlines())}{extra}'''


cases = [
    ('one full call', {'one.yml': workflow()}, True, ['one.yml:test']),
    ('two workflows', {'one.yml': workflow(), 'two.yml': workflow()}, False, ['one.yml:test', 'two.yml:test']),
    ('two calls in one step', {'one.yml': workflow('mvn verify; mvn -B verify')}, False, ['found 2']),
    ('two jobs', {'one.yml': workflow() + '  other:\n    steps:\n      - run: mvn verify\n'}, False, ['one.yml:test', 'one.yml:other']),
    ('comments and text', {'one.yml': workflow('# mvn -B verify\necho "mvn -B verify"\nmvn -B verify # mvn verify')}, True, ['found 1']),
    ('comments retain the reverse assertion', {'one.yml': workflow('# mvn verify\nmvn verify\nmvn verify')}, False, ['found 2']),
    ('inline comments preserve command boundaries', {'one.yml': workflow('mvn verify # first suite\nmvn verify # second suite')}, False, ['found 2']),
    ('continued invocation', {'one.yml': workflow('mvn -B \\\n  verify')}, True, ['found 1']),
    ('install is a test lifecycle', {'one.yml': workflow('mvn -B install')}, True, ['found 1']),
    ('test lifecycle', {'one.yml': workflow('./mvnw -B test')}, True, ['found 1']),
    ('skip false runs tests', {'one.yml': workflow('mvn -DskipTests=false verify')}, True, ['found 1']),
    ('skipped install is not another suite', {'one.yml': workflow('mvn -DskipTests install\nmvn verify')}, True, ['found 1']),
    ('selected tests are not another suite', {'one.yml': workflow('mvn -pl runtime/engine -am test -Dtest=JoinPerformanceGateTest\nmvn verify')}, True, ['found 1']),
    ('manual is outside automatic budget', {'one.yml': workflow(), 'manual.yml': workflow(trigger='[workflow_dispatch]')}, True, ['found 1']),
    ('name is not a command', {'one.yml': workflow(extra='      - name: mvn verify\n        run: echo done\n')}, True, ['found 1']),
    ('empty fails closed', {'one.yml': workflow('echo done')}, False, ['found 0']),
    ('quoted run', {'one.yml': workflow().replace('run: |\n          mvn -B verify', 'run: "mvn -B verify"')}, True, ['found 1']),
    ('folded run', {'one.yml': workflow().replace('run: |', 'run: >-')}, True, ['found 1']),
    ('logical shard suite', {'one.yml': workflow('.github/scripts/ci-shard.sh run --plan plan.json --shard "$SHARD"')}, True, ['found 1']),
    ('second logical shard suite', {'one.yml': workflow('.github/scripts/ci-shard.sh run --plan plan.json --shard "$SHARD"\nmvn verify')}, False, ['found 2']),
    ('push and PR overlap', {'one.yml': workflow(trigger='[push, pull_request]')}, False, ['overlap']),
    ('wildcard branch and PR overlap', {'one.yml': workflow(trigger='\n  push:\n    branches: [main, "ws/**"]\n  pull_request:\n    branches: [main]')}, False, ['overlap']),
    ('main push and PR disjoint', {'one.yml': workflow(trigger='\n  push:\n    branches:\n      - main\n    tags:\n      - "v*"\n  pull_request:\n    branches:\n      - main\n  workflow_dispatch:')}, True, ['found 1']),
    ('flow jobs fail closed', {'one.yml': workflow(), 'two.yml': 'on: push\njobs: {duplicate: {steps: [{run: mvn verify}]}}\n'}, False, ['unsupported']),
    ('flow steps fail closed', {'one.yml': workflow(), 'two.yml': 'on: push\njobs:\n  duplicate:\n    steps: [{run: mvn verify}]\n'}, False, ['unsupported']),
    ('module working directory is not a full suite', {'one.yml': workflow(extra='      - run: mvn verify\n        working-directory: e2e\n')}, True, ['found 1']),
]
with tempfile.TemporaryDirectory() as temporary:
    directory = Path(temporary)
    for name, files, success, messages in cases:
        for path in directory.iterdir():
            path.unlink()
        for filename, content in files.items():
            (directory / filename).write_text(content)
        result = subprocess.run(['bash', str(gate), str(directory)], capture_output=True, text=True)
        output = result.stdout + result.stderr
        assert (result.returncode == 0) == success, (name, result.returncode, output)
        assert all(message in output for message in messages), (name, output)

# The real entry point must run both the gate and its tests from catalog-scripts.
ci = (scripts.parent / 'workflows/ci.yml').read_text()
catalog = ci.split('\n  catalog-scripts:\n', 1)[1]
import re
catalog = re.split(r'\n  [\w-]+:\n', catalog, maxsplit=1)[0]
assert re.search(r'^\s+run: (?:bash )?\.github/scripts/one-verify\.sh\s*$', catalog, re.M), 'catalog-scripts does not invoke the structural gate'
assert re.search(r'^\s+(?:run: )?bash \.github/scripts/one-verify-smoke\.sh\s*$', catalog, re.M), 'catalog-scripts does not invoke its regression'
result = subprocess.run(['bash', str(gate)], capture_output=True, text=True)
assert result.returncode == 0, result.stdout + result.stderr
template = (scripts.parent / 'PULL_REQUEST_TEMPLATE.md').read_text()
assert '**CI cost delta and lane placement:**' in template
assert '.github/workflows/README.md' in template and 'ci-timing.sh' in template
placement = (scripts.parent / 'workflows/README.md').read_text()
assert 'gh workflow run ci.yml --ref <branch>' in placement
assert 'path- or label-gated' in placement and 'scheduled lane' in placement
print(f'one-verify smoke: {len(cases)} admission cases and live workflow wiring passed')
PY
