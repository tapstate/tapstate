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


MAVEN_ARGS = ('          MAVEN_ARGS: >-\n'
              '            -Dapi.version=1.44\n'
              '            -Dtapstate.it.jvmArgs=-Djava.io.tmpdir=${{ runner.temp }}/test-tmp')


def check(workflows, contexts, inputs):
    assert not re.search(r'^(?: {2}| {6})MAVEN_ARGS:', workflows['ci.yml'], re.M), \
        'Docker API configuration must not enter unrelated job or connector environments'
    execute = re.search(r'(?ms)^      - name: Execute exactly this shard\n(.*?)(?=^      - |\Z)', workflows['ci.yml'])
    assert execute and re.search(r'^        env:\n(?:          #[^\n]*\n)*' + re.escape(MAVEN_ARGS) + r'\n', execute[1], re.M), \
        'Docker API configuration must reach only the selected Maven verify and its test forks'
    assert 'mkdir -p "$RUNNER_TEMP/test-tmp"' in execute[1], \
        'the selected test step must prepare its runner-backed JVM temporary directory'
    assert workflows['ci.yml'].count('MAVEN_ARGS:') == 1 and 'JAVA_TOOL_OPTIONS:' not in workflows['ci.yml'], \
        'Docker API configuration must not leak into other steps or JVM child output'
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
    input_command = re.search(r'^        run: (.*ci-receipt\.sh compare-inputs .*)$', build, re.M)
    assert input_command and '--artifacts "$RUNNER_TEMP/ci-artifacts"' in input_command[1], 'worker inputs must bind the selected successful result producers'
    for step in re.finditer(r'(?ms)^      - [^\n]+\n.*?(?=^      - |^  [A-Za-z]|\Z)', workflows['ci.yml']):
        if not re.search(r'^        uses: actions/upload-artifact@v4$|^      - uses: actions/upload-artifact@v4$', step[0], re.M):
            continue
        settings = dict(re.findall(r'^          ([\w-]+): (.+)$', step[0], re.M))
        if settings.get('name', '').startswith('ci-'):
            assert settings.get('overwrite') == 'true', 'same-run retries must replace only their named role artifact'
    shards = ci.get('test-shards', '')
    assert 'matrix: ${{ fromJSON(needs.shard-plan.outputs.matrix) }}' in shards, \
        'workers must consume the planner matrix without a second shard list'
    assert 'name: test-shards (${{ matrix.shard }})' in shards, \
        'worker display names must not include auxiliary matrix metadata'
    assert 'PREPARE_CLOUD_INPUTS: ${{ matrix.prepare_cloud_inputs }}' in shards \
        and 'if [ "${PREPARE_CLOUD_INPUTS:-false}" = true ]; then' in shards, \
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
    assert 'ci-pr-work.sh' not in build, 'aggregate and Sonar must not be interrupted by the test supervisor'
    assert shards.count('ci-pr-work.sh') == 2, 'both worker preparation and selected verify must supervise their owned command'
    assert '--phase prepare -- .github/scripts/ci-build-observation.sh' in shards
    assert '--phase verify -- .github/scripts/ci-shard.sh run' in shards
    assert shards.count('--event-inputs "$RUNNER_TEMP/ci-event-inputs.json"') == 2, 'both phases must bind the original event'
    worker_lock = shards.split('    concurrency:\n', 1)[1].split('    strategy:', 1)[0]
    assert '      cancel-in-progress: false' in worker_lock and '      queue: max' in worker_lock
    assert "format('ci-test-{0}-{1}'" in worker_lock and 'matrix.shard' in worker_lock
    assert "format('pr-{0}', github.event.pull_request.number)" in worker_lock
    assert "format('run-{0}-{1}', github.run_id, github.run_attempt)" in worker_lock
    assert 'github.event.pull_request.head.repo.full_name == github.repository' in worker_lock
    assert 'python3 .github/scripts/_ci-pr-work-smoke.py' in ci.get('catalog-scripts', ''), 'owned work controls must run in CI'
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
    assert 'Prepare pinned public inputs for Cloud startup witnesses' in shards
    assert 'if [ "$SHARD" = rest ]; then' in shards, 'the REST shard must prepare pinned Cloud inputs'
    assert 'cloud_context_test = "io.tapstate.e2e.CloudClusterContextCarriesTheCurrentEgressAddressIT"' in shards
    assert 'test["class"] == cloud_context_test for test in shard["tests"]' in shards, \
        'any shard assigned the Cloud context test must prepare its pinned inputs'
    assert 'prepare-test-inputs.py --output "$RUNNER_TEMP/cloud-test-release"' in shards
    assert 'TAPSTATE_TEST_CLOUD_RELEASE_DIR=$RUNNER_TEMP/cloud-test-release' in shards
    namespace = {'m': 'http://maven.apache.org/POM/4.0.0'}
    reactor = ET.fromstring(inputs['pom.xml'])
    version = reactor.findtext('m:properties/m:cloud.sdk.version', namespaces=namespace)
    # The Cloud SDK snapshot publisher preserves either a major.minor or major.minor.patch tag.
    assert version and re.fullmatch(r'[0-9]+\.[0-9]+(?:\.[0-9]+)?-[0-9a-f]{8}-SNAPSHOT', version), \
        'the reactor must declare a published Cloud SDK snapshot revision'
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
    cloud_context_test = 'io.tapstate.e2e.CloudClusterContextCarriesTheCurrentEgressAddressIT'
    assert cloud_context_test in shards, 'the Cloud context e2e shard must prepare its release inputs'
    cloud_context_source = inputs['e2e/src/test/java/io/tapstate/e2e/CloudClusterContextCarriesTheCurrentEgressAddressIT.java']
    assert 'package io.tapstate.e2e;' in cloud_context_source \
        and re.search(r'\bclass\s+CloudClusterContextCarriesTheCurrentEgressAddressIT\b', cloud_context_source), \
        'the Cloud context test class named by CI must still exist at its declared package'
    cloud_dev = jobs(workflows['cloud-cluster-dev.yml']).get('image', '')
    assert cloud_dev, 'the Cloud dev image job must exist'
    assert '  workflow_call:' in workflows['cloud-cluster-dev.yml'], \
        'release must be able to call the Cloud dev build'
    assert 'ref: ${{ inputs.source-sha || github.ref }}' in cloud_dev \
        and 'ref: ${{ inputs.web-sha || github.ref_name }}' in cloud_dev, \
        'release calls must build their frozen source commits'
    assert cloud_dev.count('RELEASE_VERSION: ${{ inputs.version }}') == 2 \
        and 'echo "version=$RELEASE_VERSION" >> "$GITHUB_OUTPUT"' in cloud_dev, \
        'release calls must retain the selected release version'
    assert 'environment: cloud-ghcr-dev' in cloud_dev \
        and 'secrets.CLOUD_CLUSTER_VERSION_RELEASE_TOKEN_DEV' in cloud_dev \
        and 'secrets.CLOUD_CLUSTER_VERSION_RELEASE_TOKEN_PROD' not in cloud_dev, \
        'dev validation must use its own environment and registration token'
    assert "github.repository == 'tapstate/tapstate'" in cloud_dev \
        and "github.ref_type == 'branch'" in cloud_dev, 'Cloud dev image dispatch must exclude tag refs'
    assert '.github/scripts/next-version.sh "$BUMP" origin/main' in cloud_dev, \
        'Cloud dev version calculation must use the tagged main history'
    assert '--dev-source-sha "$SOURCE_SHA"' in cloud_dev, \
        'feature builds must publish under a source-specific dev tag'
    assert cloud_dev.count("if: inputs.version != '' || github.ref == 'refs/heads/main'") == 2 \
        and 'DEV_BRANCH: main' in cloud_dev, \
        'only main builds and pinned release candidates may register in the dev catalog'
    assert 'if [[ -z "$RELEASE_VERSION" && "$GITHUB_REF" != refs/heads/main ]]; then' in cloud_dev, \
        'pinned releases retain the canonical dev tag even when dispatched from an allowed branch'
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
          for name in ('pom.xml', 'app/pom.xml', 'deploy/docker/image-smoke.sh',
                       'e2e/src/test/java/io/tapstate/e2e/CloudClusterContextCarriesTheCurrentEgressAddressIT.java')}
# Snapshot of the protected branch's context names; the live list can be supplied as argv[1].
contexts = Path(sys.argv[2]).read_text().splitlines() if sys.argv[2] else [
    'build', 'no-cjk', 'no-agent-footprint', 'sonarqube', 'e2e-admission',
    'no-binary-bytes', 'dco', 'docs-classification', 'docs-impact']
assert contexts, 'required context list is empty'
check(workflows, contexts, inputs)
mutations = 0
for original, replacement in [
    (MAVEN_ARGS, ''),
    (MAVEN_ARGS, MAVEN_ARGS.replace('1.44', '1.32')),
    (MAVEN_ARGS, '    env:\n' + '\n'.join('      ' + line[10:] for line in MAVEN_ARGS.splitlines())),
    (MAVEN_ARGS, '          MAVEN_ARGS: "-Dapi.version=1.44"'),
    (MAVEN_ARGS, MAVEN_ARGS.replace('${{ runner.temp }}/test-tmp', '/tmp')),
    ('mkdir -p "$RUNNER_TEMP/test-tmp"', 'echo missing-test-tmp'),
    ('permissions:\n', 'env:\n  MAVEN_ARGS: "-Dapi.version=1.44"\npermissions:\n'),
    ('ci-receipt.sh event-inputs', 'echo event-capture-removed'),
    ('${{ runner.temp }}/ci-event-inputs.json', '${{ runner.temp }}/missing-event.json'),
    ('matrix: ${{ steps.plan.outputs.matrix }}', 'matrix: wrong-output'),
    ('ci-shard.sh matrix --root . --plan "$RUNNER_TEMP/shards.json"', 'echo unrelated-matrix'),
    ('matrix: ${{ fromJSON(needs.shard-plan.outputs.matrix) }}', 'matrix: {shard: [rest]}'),
    ('name: test-shards (${{ matrix.shard }})', 'name: unrelated-worker'),
    ('PREPARE_CLOUD_INPUTS: ${{ matrix.prepare_cloud_inputs }}', 'PREPARE_CLOUD_INPUTS: false'),
    ('if [ "${PREPARE_CLOUD_INPUTS:-false}" = true ]; then', 'if false; then'),
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
    ('--phase prepare -- .github/scripts/ci-build-observation.sh', '--phase prepare -- unrelated-observer.sh'),
    ('--phase verify -- .github/scripts/ci-shard.sh run', '--phase verify -- unrelated-shards.sh run'),
    ('--event-inputs "$RUNNER_TEMP/ci-event-inputs.json"', '--event-inputs unrelated-event.json'),
    ("format('ci-test-{0}-{1}'", "format('unrelated-{0}-{1}'"),
    ('matrix.shard) }}', 'matrix.other) }}'),
    ("format('run-{0}-{1}', github.run_id, github.run_attempt)", "format('shared-unknown')"),
    ('python3 .github/scripts/_ci-pr-work-smoke.py', 'echo work-controls-removed'),
    ('  build:', '  build-renamed:'),
    ('  build:', '  build:\n    strategy:\n      matrix: {part: [1, 2]}'),
    ('ci-aggregate.sh verify', 'echo verification-removed'),
    ('ci-receipt.sh compare-inputs', 'echo input-comparison-removed'),
    (' --artifacts "$RUNNER_TEMP/ci-artifacts"', ''),
    ('          overwrite: true', '          overwrite: false'),
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
patch_sdk_version = '0.1.1-' + sdk_version.split('-', 1)[1]
accepted_patch_version = {**inputs, 'pom.xml': inputs['pom.xml'].replace(sdk_version, patch_sdk_version, 1)}
check(workflows, contexts, accepted_patch_version)
for name, original, replacement in [
    ('pom.xml', sdk_version, 'LATEST'),
    ('pom.xml', sdk_version, sdk_version.replace('-', '.1.2-', 1)),
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
mutated_test = {**inputs, 'e2e/src/test/java/io/tapstate/e2e/CloudClusterContextCarriesTheCurrentEgressAddressIT.java':
                inputs['e2e/src/test/java/io/tapstate/e2e/CloudClusterContextCarriesTheCurrentEgressAddressIT.java']
                .replace('CloudClusterContextCarriesTheCurrentEgressAddressIT', 'RenamedCloudContextIT')}
try:
    check(workflows, contexts, mutated_test)
except AssertionError:
    mutations += 1
else:
    raise AssertionError('renaming the Cloud context integration test escaped CI-shape validation')
for original, replacement in [
    ('&& github.ref_type == \'branch\'', ''),
    ('  workflow_call:', '  unused_call:'),
    ('inputs.source-sha || github.ref', 'github.ref'),
    ('inputs.web-sha || github.ref_name', 'github.ref_name'),
    ('RELEASE_VERSION: ${{ inputs.version }}', 'RELEASE_VERSION: ""'),
    ('environment: cloud-ghcr-dev', 'environment: cloud-ghcr-release'),
    ('secrets.CLOUD_CLUSTER_VERSION_RELEASE_TOKEN_DEV', 'secrets.CLOUD_CLUSTER_VERSION_RELEASE_TOKEN_PROD'),
    ("inputs.version != '' || github.ref == 'refs/heads/main'", "github.ref == 'refs/heads/main'"),
    ('-z "$RELEASE_VERSION" && "$GITHUB_REF"', '"$GITHUB_REF"'),
    ('.github/scripts/next-version.sh "$BUMP" origin/main', '.github/scripts/next-version.sh "$BUMP" HEAD'),
    ('--dev-source-sha "$SOURCE_SHA"', ''),
    ('DEV_BRANCH: main', 'DEV_BRANCH: ${{ github.ref_name }}'),
]:
    assert original in workflows['cloud-cluster-dev.yml'], f'Cloud dev mutation target absent: {original}'
    mutated_workflows = {**workflows,
                         'cloud-cluster-dev.yml': workflows['cloud-cluster-dev.yml'].replace(original, replacement, 1)}
    try:
        check(mutated_workflows, contexts, inputs)
    except AssertionError:
        mutations += 1
        continue
    raise AssertionError(f'Cloud dev workflow mutation escaped: {replacement}')
print(f'ci-shape smoke: workflow contracts and {mutations} mutations passed')
PY
