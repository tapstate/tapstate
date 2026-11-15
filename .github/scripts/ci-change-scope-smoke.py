#!/usr/bin/env python3
"""Exercise Git identities, unsafe mixed changes and every expensive workflow step."""
import importlib.util
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile

sys.dont_write_bytecode = True
HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location('scope', HERE / 'ci-change-scope.py')
scope = importlib.util.module_from_spec(spec)
spec.loader.exec_module(scope)


def run(root, *args):
    environment = {key: value for key, value in os.environ.items() if key not in (
        'GIT_DIR', 'GIT_WORK_TREE', 'GIT_INDEX_FILE', 'GIT_PREFIX', 'GIT_QUARANTINE_PATH', 'GIT_COMMON_DIR')}
    return subprocess.check_output(['git', '-c', 'core.hooksPath=/dev/null', '-C', str(root), *args],
                                   env=environment, stderr=subprocess.PIPE).decode().strip()


with tempfile.TemporaryDirectory() as directory:
    root = Path(directory)
    run(root, 'init', '-q')
    assert Path(run(root, 'rev-parse', '--show-toplevel')).resolve() == root.resolve()
    run(root, 'config', 'user.name', 'Fixture')
    run(root, 'config', 'user.email', 'fixture@example.invalid')
    run(root, 'config', 'commit.gpgsign', 'false')
    (root / 'README.md').write_text('Original prose\n')
    run(root, 'add', '.')
    run(root, 'commit', '-qm', 'Base')
    base = run(root, 'rev-parse', 'HEAD')

    def check(name, expected, files=(), event_name='pull_request', mutate=None):
        run(root, 'checkout', '-q', '--detach', base)
        # Every scenario owns its whole temporary repository.
        run(root, 'clean', '-fdq')
        for path in files:
            target = root / path
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text('Changed prose\n')
        if mutate:
            mutate(root)
        run(root, 'add', '-A')
        run(root, 'commit', '-qm', name, '--allow-empty')
        head = run(root, 'rev-parse', 'HEAD')
        event = {'pull_request': {'base': {'sha': base}, 'head': {'sha': head}}}
        actual = scope.classify(root, event_name, event)[0]
        assert actual == expected, (name, actual, expected)
        return event

    event = check('README only', True, ['README.md'])
    assert not scope.classify(root, 'pull_request', {'pull_request': {}})[0]
    missing = {'pull_request': {'base': {'sha': 'f' * 40}, 'head': event['pull_request']['head']}}
    assert not scope.classify(root, 'pull_request', missing)[0]
    check('docs pages', True, ['docs/guide/page.md', 'README.md'])
    check('root policies', True, ['CONTRIBUTING.md', 'SECURITY.md'])
    check('deleted page', True, mutate=lambda root: (root / 'README.md').unlink())
    check('empty diff', False)
    for name, files in [
        ('mixed code', ['README.md', 'app/src/main/java/Main.java']),
        ('workflow', ['.github/workflows/ci.yml']),
        ('build file', ['pom.xml']),
        ('test fixture', ['app/src/test/resources/README.md']),
        ('script', ['scripts/README.md']),
        ('hidden docs', ['docs/.config.md']),
        ('executable example', ['docs/example.sh']),
        ('unknown root', ['unknown.md']),
        ('newline path', ['docs/guide\nworkflow.yml']),
    ]:
        check(name, False, files)
    check('symlink page', False, mutate=lambda root: (
        (root / 'README.md').unlink(), (root / 'README.md').symlink_to('/tmp/outside')))
    check('executable page', False, ['README.md'], mutate=lambda root: (root / 'README.md').chmod(0o755))
    check('rename from code', False, mutate=lambda root: (root / 'README.md').rename(root / 'Main.java'))
    for event_name in ('push', 'workflow_dispatch', 'pull_request_target', ''):
        check('other event ' + event_name, False, ['README.md'], event_name=event_name)
    # A real merge checkout must bind both original parents, with divergent base history.
    event = check('PR head', True, ['README.md'])
    head = run(root, 'rev-parse', 'HEAD')
    run(root, 'checkout', '-q', '--detach', base)
    (root / 'base.txt').write_text('base advanced')
    run(root, 'add', '.')
    run(root, 'commit', '-qm', 'Base advanced')
    new_base = run(root, 'rev-parse', 'HEAD')
    run(root, 'merge', '--no-ff', '-qm', 'PR merge', head)
    event['pull_request']['base']['sha'] = new_base
    assert scope.classify(root, 'pull_request', event)[0]
    event['pull_request']['base']['sha'] = base
    assert not scope.classify(root, 'pull_request', event)[0], 'foreign checkout must retain full CI'


def workflow_contract(text):
    blocks = re.split(r'(?m)(?=^  [\w-]+:\s*$)', text.split('jobs:\n', 1)[1])
    jobs = {block.split(':', 1)[0].strip(): block for block in blocks if block.strip()}
    assert 'docs-only: ${{ steps.scope.outputs.docs-only }}' in jobs['shard-plan']
    assert 'steps.scope.outputs.matrix || steps.plan.outputs.matrix' in jobs['shard-plan']
    for role in ('shard-plan', 'test-shards', 'build', 'installer', 'catalog-scripts', 'image-smoke'):
        block = jobs[role]
        selector = 'steps.scope.outputs.docs-only' if role == 'shard-plan' else 'needs.shard-plan.outputs.docs-only'
        if role not in ('shard-plan', 'build'):
            assert 'needs: shard-plan' in block, role
        for step in re.split(r'(?m)(?=^      - )', block.split('    steps:\n', 1)[1]):
            if not step.strip():
                continue
            if any(marker in step for marker in ('actions/checkout@v4', 'id: scope', 'CI change-scope controls', 'Validate CI routing', 'Documentation-only CI')):
                continue
            assert f"{selector} != 'true'" in step, (role, step.splitlines()[0])
    assert 'test "$PLAN_RESULT" = success' in jobs['build']
    assert 'test "$SHARD_RESULT" = success' in jobs['build']
    assert "if [ \"$DOCS_ONLY\" = true ]; then" in jobs['sonarqube']
    assert 'not evaluated for documentation-only changes' in jobs['sonarqube']


workflow = (HERE.parent / 'workflows/ci.yml').read_text()
workflow_contract(workflow)
bad = workflow.replace("needs.shard-plan.outputs.docs-only != 'true'", 'true', 1)
try:
    workflow_contract(bad)
except AssertionError:
    pass
else:
    raise AssertionError('unguarded expensive step escaped')
print('CI scope smoke: Git fixtures, fail-closed cases and workflow guards passed')
