#!/usr/bin/env python3
"""Exercise Git identities, unsafe mixed changes and every expensive workflow step."""
import importlib.util
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import textwrap

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


# Additional predicates already used by full CI; the route guard must always apply to the whole condition.
FULL_CI_WRAPPERS = (
    "startsWith(matrix.shard, 'e2e-')", 'always()', 'failure()', "github.event_name == 'pull_request'",
    "steps.guard.outputs.run == 'true'",
    "steps.guard.outputs.run == 'true' && steps.sonar-cache-key.outcome == 'success'",
    "always() && steps.guard.outputs.run == 'true' && steps.analyze.outcome != 'skipped' "
    "&& steps.analyze.outcome != '' && github.event_name == 'pull_request'",
    "always() && steps.sonar-comment.outputs.findings_path != ''",
)


def workflow_contract(text):
    blocks = re.split(r'(?m)(?=^  [\w-]+:\s*$)', text.split('jobs:\n', 1)[1])
    jobs = {block.split(':', 1)[0].strip(): block for block in blocks if block.strip()}
    assert 'docs-only: ${{ steps.scope.outputs.docs-only }}' in jobs['shard-plan']
    assert 'steps.scope.outputs.matrix || steps.plan.outputs.matrix' in jobs['shard-plan']
    for role in ('shard-plan', 'snapshot-lock', 'test-shards', 'build', 'installer', 'catalog-scripts', 'image-smoke'):
        block = jobs[role]
        selector = 'steps.scope.outputs.docs-only' if role == 'shard-plan' else 'needs.shard-plan.outputs.docs-only'
        if role == 'test-shards':
            assert 'needs: [shard-plan, snapshot-lock]' in block, role
        elif role not in ('shard-plan', 'build'):
            assert 'needs: shard-plan' in block, role
        required = f"{selector} != 'true'"
        accepted = {required}
        accepted.update("${{ " + required + " && (" + wrapper + ") }}" for wrapper in FULL_CI_WRAPPERS)
        for step in re.split(r'(?m)(?=^      - )', block.split('    steps:\n', 1)[1]):
            if not step.strip():
                continue
            header = step.splitlines()[0].strip()
            if header == '- uses: actions/checkout@v4':
                continue
            if role == 'shard-plan' and header == '- name: Classify the original pull request diff':
                continue
            conditions = re.findall(r'^        if: (.+)$', step, re.MULTILINE)
            if role in ('build', 'installer') and header == '- name: Documentation-only CI':
                assert conditions == [f"{selector} == 'true'"], (role, header, conditions)
                if role == 'installer':
                    assert 'Installer checks were not evaluated' in step and '$GITHUB_STEP_SUMMARY' in step
                continue
            assert len(conditions) == 1 and conditions[0] in accepted, (role, header, conditions)
    assert '- name: Documentation-only CI' in jobs['installer']
    # A failed full shard must still reach checkout and the aggregate evidence steps.
    pre_checkout = jobs['build'].split('- uses: actions/checkout@v4', 1)[0]
    for step in pre_checkout.split('      - ')[1:]:
        if 'test "$SHARD_RESULT" = success' in step:
            assert "if: needs.shard-plan.outputs.docs-only == 'true'" in step, 'full shard failure skips diagnostic collection'
    assert 'test "$PLAN_RESULT" = success' in pre_checkout
    assert 'test "$SHARD_RESULT" = success' in pre_checkout
    after_evidence = jobs['build'].split('- name: Admit the complete report and coverage set', 1)[1]
    assert 'test "$SHARD_RESULT" = success' in after_evidence
    assert "if [ \"$DOCS_ONLY\" = true ]; then" in jobs['sonarqube']
    assert 'not evaluated for documentation-only changes' in jobs['sonarqube']


def check_planner_checkout(workflow):
    block = workflow.split('      - name: Classify the original pull request diff\n', 1)[1]
    script = textwrap.dedent(block.split('        run: |\n', 1)[1].split('\n      - ', 1)[0])
    with tempfile.TemporaryDirectory() as directory:
        tmp = Path(directory)
        repo = tmp / 'repo'
        repo.mkdir()
        run(repo, 'init', '-q')
        run(repo, 'config', 'user.name', 'Fixture')
        run(repo, 'config', 'user.email', 'fixture@example.invalid')
        run(repo, 'config', 'commit.gpgsign', 'false')
        run(repo, 'commit', '--allow-empty', '-qm', 'Before the classifier exists')
        bootstrap = run(repo, 'rev-parse', 'HEAD')
        scripts = repo / '.github/scripts'
        scripts.mkdir(parents=True)
        (scripts / 'ci-change-scope.py').write_bytes((HERE / 'ci-change-scope.py').read_bytes())
        # The stub records which revision supplied the smoke without nesting this fixture.
        (scripts / 'ci-change-scope-smoke.py').write_text(
            'import os\nfrom pathlib import Path\nPath(os.environ["SMOKE_MARKER"]).touch()\n')
        (repo / '.github/workflows').mkdir()
        (repo / '.github/workflows/ci.yml').write_text(workflow)
        run(repo, 'add', '.')
        run(repo, 'commit', '-qm', 'Reviewed routing rules')
        base = run(repo, 'rev-parse', 'HEAD')
        (repo / 'README.md').write_text('Documentation update\n')
        run(repo, 'add', '.')
        run(repo, 'commit', '-qm', 'Documentation')
        head = run(repo, 'rev-parse', 'HEAD')
        # The planner must still work independently of the copies in the checkout.
        for path in scripts.glob('*.py'):
            path.unlink()
        marker = tmp / 'smoke-ran'
        for revision, expected in ((bootstrap, False), (base, True)):
            output = tmp / 'output'
            event = tmp / 'event'
            output.write_text('')
            event.write_text(json.dumps({'pull_request': {'base': {'sha': revision}, 'head': {'sha': head}}}))
            environment = os.environ | dict(GITHUB_EVENT_NAME='pull_request', SCOPE_BASE_SHA=revision,
                GITHUB_EVENT_PATH=str(event), GITHUB_OUTPUT=str(output), RUNNER_TEMP=str(tmp), SMOKE_MARKER=str(marker))
            subprocess.run(['bash', '-c', script], cwd=repo, env=environment, capture_output=True, check=True)
            assert f'docs-only={str(expected).lower()}' in output.read_text().splitlines()
            assert marker.exists() == expected


workflow = (HERE.parent / 'workflows/ci.yml').read_text()
workflow_contract(workflow)
check_planner_checkout(workflow)
bad = workflow.replace("needs.shard-plan.outputs.docs-only != 'true'", 'true', 1)
try:
    workflow_contract(bad)
except AssertionError:
    pass
else:
    raise AssertionError('unguarded expensive step escaped')
print('CI scope smoke: Git fixtures, fail-closed cases and workflow guards passed')
