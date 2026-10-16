#!/usr/bin/env bash
# Read the conventional workflow job blocks, excluding comments and nested YAML keys.
# Optional context file comes from the protected branch's required-status-check rules.
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
python3 - "$here/../workflows" "${1:-}" <<'PY'
from pathlib import Path
import re
import sys
import xml.etree.ElementTree as ET


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


def check(workflows, contexts, inputs):
    ci = jobs(workflows['ci.yml'])
    plan = ci.get('shard-plan', '')
    assert 'ci-receipt.sh event-inputs' in plan and '${{ runner.temp }}/ci-event-inputs.json' in plan, \
        'the original event and actual checkout must be retained with the shard plan'
    assert 'matrix: ${{ steps.plan.outputs.matrix }}' in plan and 'id: plan' in plan, \
        'the planner must publish its exact matrix output'
    assert 'ci-shard.sh matrix --root . --plan "$RUNNER_TEMP/shards.json"' in plan, \
        'matrix IDs must come from the actual emitted plan'
    build = ci.get('build', '')
    assert build, 'required check build must exist'
    assert not re.search(r'^\s+matrix:', build, re.M), 'build must not be a matrix'
    assert not re.search(r'^    name:', build, re.M), 'build must retain its check name'
    assert re.search(r'^    needs:.*test-shards', build, re.M), 'build must wait for all shards'
    assert re.search(r'^    if:.*always\(\)', build, re.M), 'build must run when a shard fails'
    assert 'ci-aggregate.sh verify' in build, 'build must admit the global report set'
    assert 'ci-receipt.sh compare-inputs' in build and 'pattern: ci-inputs-*' in build, \
        'build must compare every independent worker input snapshot before report admission'
    shards = ci.get('test-shards', '')
    assert 'matrix: ${{ fromJSON(needs.shard-plan.outputs.matrix) }}' in shards, \
        'workers must consume the planner matrix without a second shard list'
    assert 'name: test-shards (${{ matrix.shard }})' in shards, \
        'worker display names must not include auxiliary matrix metadata'
    assert 'if: matrix.prepare_cloud_inputs' in shards, \
        'every residual module group must retain Cloud test inputs'
    for role in ('test-shards', 'build'):
        block = ci[role]
        assert block.count('ci-build-observation.sh') == 1, 'each original prepare must capture one observation'
        assert ' -- mvn -B -T 2 -Dmaven.repo.local="$RUNNER_TEMP/m2" -DskipTests install' in block, 'complete prepare must retain bounded two-thread compilation and skipped test execution'
        assert '--maven-observation "$RUNNER_TEMP/maven-observation/summary.json"' in block, 'prepare observation is not bound to the input snapshot'
    assert shards.count('--maven-observation "$RUNNER_TEMP/maven-observation/summary.json"') == 2, 'both worker snapshots must retain the same prepare observation'
    assert build.count('--shard-id build') == 2, 'aggregate prepare and input snapshot must both declare build'

    assert 'python3 .github/scripts/_ci-maven-artifacts-smoke.py' in ci.get('catalog-scripts', ''), 'original Maven artifact identity controls must run in every recipe'
    assert 'python3 -m py_compile .github/scripts/_ci-maven-artifacts.py .github/scripts/_ci-maven-artifacts-smoke.py' in ci.get('catalog-scripts', ''), 'Maven artifact capture and controls must compile'
    assert 'python3 .github/scripts/_ci-test-jar-inputs-smoke.py' in ci.get('catalog-scripts', ''), 'test-JAR role controls must run in every recipe'
    assert 'python3 -m py_compile .github/scripts/_ci-test-jar-inputs.py .github/scripts/_ci-test-jar-inputs-smoke.py' in ci.get('catalog-scripts', ''), 'test-JAR capture and controls must compile'
    assert 'python3 .github/scripts/_ci-mechanism-inputs-smoke.py' in ci.get('catalog-scripts', ''), 'manual mechanism archive controls must run in every recipe'
    assert 'python3 -m py_compile .github/scripts/_ci-mechanism-inputs.py .github/scripts/_ci-mechanism-inputs-smoke.py' in ci.get('catalog-scripts', ''), 'manual mechanism archive readers and controls must compile'
    assert '--probe-receipt "$RUNNER_TEMP/ci-inputs/provider-probe-receipt.json"' in shards, 'provider capture must stay in the original input artifact'
    assert 'python3 .github/scripts/_ci-provider-receipt-smoke.py' in ci.get('catalog-scripts', ''), 'provider capture controls must run in CI'
    assert 'python3 .github/scripts/_ci-provider-readback-smoke.py' in ci.get('catalog-scripts', ''), 'provider archive controls must run in CI'
    assert 'python3 .github/scripts/_ci-provider-replay-smoke.py' in ci.get('catalog-scripts', ''), 'active provider replay controls must run in CI'
    assert 'python3 .github/scripts/_ci-provider-replay-readback-smoke.py' in ci.get('catalog-scripts', ''), 'passive replay readback controls must run in CI'
    assert 'python3 .github/scripts/_ci-provider-cases-smoke.py' in ci.get('catalog-scripts', ''), 'computed provider case partition controls must run in CI'
    assert 'fail-fast: false' in shards, 'collect evidence from every shard on failure'
    assert shards.count('--shard-id "${{ matrix.shard }}"') == 3, 'both input snapshots and the preparation observer must declare the actual matrix shard'
    assert '--shard-id build' in build, 'aggregate inputs must declare their own producer role'
    assert 'ci-shard.sh run' in shards and 'ci-aggregate.sh pack' in shards
    assert 'install' in shards and '-DskipTests' in shards and 'RUNNER_TEMP/m2' in shards
    assert 'if-no-files-found: error' in shards, 'missing shard artifact must fail'
    namespace = {'m': 'http://maven.apache.org/POM/4.0.0'}
    reactor = ET.fromstring(inputs['pom.xml'])
    version = reactor.findtext('m:properties/m:cloud.sdk.version', namespaces=namespace)
    assert version and re.fullmatch(r'[0-9]+\.[0-9]+\.[0-9]+-[0-9a-f]{8}-SNAPSHOT', version), \
        'the reactor must declare the published Cloud SDK revision'
    app = ET.fromstring(inputs['app/pom.xml'])
    sdk = [dependency for dependency in app.findall('m:dependencies/m:dependency', namespace)
           if dependency.findtext('m:groupId', namespaces=namespace) == 'io.tapstate'
           and dependency.findtext('m:artifactId', namespaces=namespace) == 'cloud-control-plane-sdk']
    assert len(sdk) == 1 and sdk[0].findtext('m:version', namespaces=namespace) == '${cloud.sdk.version}', \
        'the application must resolve the declared Cloud SDK dependency'
    assert sdk[0].findtext('m:scope', namespaces=namespace) in (None, 'compile'), \
        'the published Cloud SDK must remain an application dependency'
    snapshots = [repository for repository in reactor.findall('m:repositories/m:repository', namespace)
                 if repository.findtext('m:id', namespaces=namespace) == 'nexus-snapshots']
    assert len(snapshots) == 1 \
        and snapshots[0].findtext('m:url', namespaces=namespace) == \
        'https://nexus.tapdata.net/repository/maven-snapshots/' \
        and snapshots[0].findtext('m:snapshots/m:enabled', namespaces=namespace) == 'true', \
        'the published Cloud SDK must resolve through the configured Nexus snapshots repository'
    for job in ('test-shards', 'build', 'image-smoke'):
        body = ci.get(job, '')
        assert not any(value in body for value in (
            'repository: tapstate/tapstate-cloud', 'CLOUD_SDK_REVISION',
            '-DskipSdkGeneration=true', 'client/java', 'install:install-file')), \
            f'{job} must not inject an unpublished or fake Cloud SDK'
    for job in ('test-shards', 'build'):
        assert re.search(r'(?:run: | -- )mvn -B -T 2 -Dmaven\.repo\.local="\$RUNNER_TEMP/m2" -DskipTests install',
                         ci[job]), f'{job} must resolve published dependencies in its isolated Maven build'
    assert 'bash deploy/docker/image-smoke.sh --build' in ci['image-smoke'] \
        and 'mvn -q -pl app -am -DskipTests package' in inputs['deploy/docker/image-smoke.sh'], \
        'image-smoke must build the actual application through Maven dependency resolution'
    sonar = ci.get('sonarqube', '')
    sonar_name = '    name: sonarqube'
    assert sonar_name in sonar.splitlines(), 'sonarqube must keep its exact name on analysis events'
    assert not re.search(r'^\s+matrix:', sonar, re.M), 'sonarqube must not be a matrix'
    assert 'needs: build' in sonar and 'always()' in sonar, 'sonarqube must report failed builds'
    names = {'sonarqube'}
    for text in workflows.values():
        for key, body in jobs(text).items():
            # A matrix or custom display name no longer provides the unqualified job id.
            if not re.search(r'^\s+matrix:', body, re.M) and not re.search(r'^    name:', body, re.M):
                names.add(key)
    assert set(contexts) <= names, f'missing exact required contexts: {set(contexts) - names}'


root = Path(sys.argv[1]).resolve()
workflows = {p.name: p.read_text() for p in root.glob('*.yml')}
repository = root.parents[1]
inputs = {name: (repository / name).read_text()
          for name in ('pom.xml', 'app/pom.xml', 'deploy/docker/image-smoke.sh')}
# Snapshot of the protected branch's context names; the live list can be supplied as argv[1].
contexts = Path(sys.argv[2]).read_text().splitlines() if sys.argv[2] else [
    'build', 'no-cjk', 'no-agent-footprint', 'sonarqube', 'e2e-admission',
    'no-binary-bytes', 'dco', 'docs-classification', 'docs-impact']
assert contexts, 'required context list is empty'
check(workflows, contexts, inputs)
mutations = 0
for original, replacement in [
    ('ci-receipt.sh event-inputs', 'echo event-capture-removed'),
    ('${{ runner.temp }}/ci-event-inputs.json', '${{ runner.temp }}/missing-event.json'),
    ('matrix: ${{ steps.plan.outputs.matrix }}', 'matrix: wrong-output'),
    ('ci-shard.sh matrix --root . --plan "$RUNNER_TEMP/shards.json"', 'echo unrelated-matrix'),
    ('matrix: ${{ fromJSON(needs.shard-plan.outputs.matrix) }}', 'matrix: {shard: [rest]}'),
    ('name: test-shards (${{ matrix.shard }})', 'name: unrelated-worker'),
    ('if: matrix.prepare_cloud_inputs', "if: matrix.shard == 'rest'"),
    ('--probe-receipt "$RUNNER_TEMP/ci-inputs/provider-probe-receipt.json"', '--probe-receipt unrelated-file.json'),
    ('python3 .github/scripts/_ci-maven-artifacts-smoke.py', 'echo artifact-identity-controls-removed'),
    ('python3 .github/scripts/_ci-test-jar-inputs-smoke.py', 'echo test-jar-controls-removed'),
    ('python3 .github/scripts/_ci-mechanism-inputs-smoke.py', 'echo mechanism-input-controls-removed'),
    ('python3 -m py_compile .github/scripts/_ci-mechanism-inputs.py .github/scripts/_ci-mechanism-inputs-smoke.py', 'echo mechanism-input-compilation-removed'),
    ('python3 .github/scripts/_ci-provider-receipt-smoke.py', 'echo provider-controls-removed'),
    ('python3 .github/scripts/_ci-provider-readback-smoke.py', 'echo archive-provider-controls-removed'),
    ('python3 .github/scripts/_ci-provider-replay-smoke.py', 'echo active-provider-controls-removed'),
    ('python3 .github/scripts/_ci-provider-replay-readback-smoke.py', 'echo passive-replay-controls-removed'),
    ('python3 .github/scripts/_ci-provider-cases-smoke.py', 'echo provider-case-controls-removed'),
    ('  build:', '  build-renamed:'),
    ('  build:', '  build:\n    strategy:\n      matrix: {part: [1, 2]}'),
    ('ci-aggregate.sh verify', 'echo verification-removed'),
    ('ci-receipt.sh compare-inputs', 'echo input-comparison-removed'),
    ('--shard-id "${{ matrix.shard }}"', '--shard-id omitted'),
    ('--shard-id build', '--shard-id omitted'),
    ('pattern: ci-inputs-*', 'pattern: no-worker-inputs'),
    ('    name: sonarqube', '    name: sonar-renamed'),
    ('  test-shards:', '  test-shards:\n    env:\n      CLOUD_SDK_REVISION: unpublished'),
    (' -- mvn -B -T 2 -Dmaven.repo.local="$RUNNER_TEMP/m2" -DskipTests install',
     ' -- echo Maven-resolution-removed'),
    (' -- mvn -B -T 2 ', ' -- mvn -B -T 4 '),
    ('-DskipTests install', '-Dmaven.test.skip=true install'),
    ('ci-build-observation.sh', 'observation-removed.sh'),
    ('--maven-observation "$RUNNER_TEMP/maven-observation/summary.json"', '--maven-observation omitted'),
]:
    assert original in workflows['ci.yml'], f'mutation target absent: {original}'
    mutated = {**workflows, 'ci.yml': workflows['ci.yml'].replace(original, replacement, 1)}
    try:
        check(mutated, contexts, inputs)
    except AssertionError:
        mutations += 1
        continue
    raise AssertionError(f'workflow mutation escaped: {replacement}')
sdk_version = ET.fromstring(inputs['pom.xml']).findtext(
    './/{http://maven.apache.org/POM/4.0.0}cloud.sdk.version')
for name, original, replacement in [
    ('pom.xml', sdk_version, 'LATEST'),
    ('app/pom.xml', '<artifactId>cloud-control-plane-sdk</artifactId>',
     '<artifactId>missing-cloud-sdk</artifactId>'),
    ('app/pom.xml', '${cloud.sdk.version}', 'LATEST'),
    ('pom.xml', 'https://nexus.tapdata.net/repository/maven-snapshots/', 'https://invalid.example.test/'),
    ('deploy/docker/image-smoke.sh', 'mvn -q -pl app -am -DskipTests package',
     'echo Maven-resolution-removed'),
]:
    assert original in inputs[name], f'SDK mutation target absent: {original}'
    mutated = {**inputs, name: inputs[name].replace(original, replacement, 1)}
    try:
        check(workflows, contexts, mutated)
    except AssertionError:
        mutations += 1
        continue
    raise AssertionError(f'SDK input mutation escaped: {replacement}')
print(f'ci-shape smoke: workflow contracts and {mutations} mutations passed')
PY
