#!/usr/bin/env bash
# Read the conventional workflow job blocks, excluding comments and nested YAML keys.
# Optional context file comes from the protected branch's required-status-check rules.
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
python3 - "$here/../workflows" "${1:-}" <<'PY'
from pathlib import Path
import re
import sys


def jobs(text):
    result, current, inside = {}, None, False
    for line in text.splitlines():
        if line.lstrip().startswith('#'):
            continue
        if line == 'jobs:':
            inside = True
            continue
        if inside and re.match(r'^\S', line):
            break
        match = re.fullmatch(r'  ([\w-]+):\s*', line) if inside else None
        if match:
            current = match[1]
            result[current] = []
        elif current:
            result[current].append(line)
    return {k: '\n'.join(v) for k, v in result.items()}


def check(workflows, contexts):
    ci = jobs(workflows['ci.yml'])
    build = ci.get('build', '')
    assert build, 'required check build must exist'
    assert not re.search(r'^\s+matrix:', build, re.M), 'build must not be a matrix'
    assert not re.search(r'^    name:', build, re.M), 'build must retain its check name'
    assert re.search(r'^    needs:.*test-shards', build, re.M), 'build must wait for all shards'
    assert re.search(r'^    if:.*always\(\)', build, re.M), 'build must run when a shard fails'
    assert 'ci-aggregate.sh verify' in build, 'build must admit the global report set'
    shards = ci.get('test-shards', '')
    assert 'fail-fast: false' in shards, 'collect evidence from every shard on failure'
    assert 'ci-shard.sh run' in shards and 'ci-aggregate.sh pack' in shards
    assert 'install' in shards and '-DskipTests' in shards and 'RUNNER_TEMP/m2' in shards
    assert 'if-no-files-found: error' in shards, 'missing shard artifact must fail'
    names = set()
    for text in workflows.values():
        for key, body in jobs(text).items():
            # A matrix or custom display name no longer provides the unqualified job id.
            if not re.search(r'^\s+matrix:', body, re.M) and not re.search(r'^    name:', body, re.M):
                names.add(key)
    assert set(contexts) <= names, f'missing exact required contexts: {set(contexts) - names}'


root = Path(sys.argv[1])
workflows = {p.name: p.read_text() for p in root.glob('*.yml')}
# Snapshot of the protected branch's context names; the live list can be supplied as argv[1].
contexts = Path(sys.argv[2]).read_text().splitlines() if sys.argv[2] else [
    'build', 'no-cjk', 'no-agent-footprint', 'sonarqube', 'e2e-admission',
    'no-binary-bytes', 'dco', 'docs-classification', 'docs-impact']
assert contexts, 'required context list is empty'
check(workflows, contexts)
for original, replacement in [
    ('  build:', '  build-renamed:'),
    ('  build:', '  build:\n    strategy:\n      matrix: {part: [1, 2]}'),
    ('ci-aggregate.sh verify', 'echo verification-removed'),
]:
    assert original in workflows['ci.yml'], f'mutation target absent: {original}'
    mutated = {**workflows, 'ci.yml': workflows['ci.yml'].replace(original, replacement, 1)}
    try:
        check(mutated, contexts)
    except AssertionError:
        continue
    raise AssertionError(f'workflow mutation escaped: {replacement}')
print('ci-shape smoke: workflow contracts and 3 mutations passed')
PY
