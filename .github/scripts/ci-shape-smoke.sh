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
    assert 'Prepare pinned public inputs for Cloud startup witnesses' in shards
    assert "if: matrix.shard == 'rest'" in shards
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
        assert re.search(r'run: mvn -B -Dmaven\.repo\.local="\$RUNNER_TEMP/m2" -DskipTests install',
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
    assert "github.repository == 'tapstate/tapstate'" in cloud_dev \
        and "github.ref_type == 'branch'" in cloud_dev, 'Cloud dev image dispatch must exclude tag refs'
    assert '.github/scripts/next-version.sh "$BUMP" origin/main' in cloud_dev, \
        'Cloud dev version calculation must use the tagged main history'
    assert '--dev-source-sha "$SOURCE_SHA"' in cloud_dev, \
        'feature builds must publish under a source-specific dev tag'
    assert "if: github.ref == 'refs/heads/main'" in cloud_dev and 'DEV_BRANCH: main' in cloud_dev, \
        'only main may register the image in the dev version catalog'
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
    ('  build:', '  build-renamed:'),
    ('  build:', '  build:\n    strategy:\n      matrix: {part: [1, 2]}'),
    ('ci-aggregate.sh verify', 'echo verification-removed'),
    ('    name: sonarqube', '    name: sonar-renamed'),
    ('  test-shards:', '  test-shards:\n    env:\n      CLOUD_SDK_REVISION: unpublished'),
    ('run: mvn -B -Dmaven.repo.local="$RUNNER_TEMP/m2" -DskipTests install',
     'run: echo Maven-resolution-removed'),
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
