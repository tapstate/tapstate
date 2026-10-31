#!/usr/bin/env bash
# Preserve the release check name and the complete connector artifact path.
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
CONNECTOR_WORKFLOW_ROOT="$(cd "$here/../.." && pwd)"
export CONNECTOR_WORKFLOW_ROOT
python3 - <<'PY'
import os,pathlib,re
root=pathlib.Path(os.environ['CONNECTOR_WORKFLOW_ROOT'])
workflow=(root/'.github/workflows/nightly-real-connectors.yml').read_text()
release=(root/'.github/workflows/release.yml').read_text()
def check(text):
    # Conventional top-level job blocks; comments cannot supply an executable gate.
    text='\n'.join(line.split('#',1)[0] for line in text.splitlines() if not line.lstrip().startswith('#'))
    starts=list(re.finditer(r'^  ([\w-]+):\s*$',text,re.M))
    jobs={m.group(1):text[m.end():starts[i+1].start() if i+1<len(starts) else len(text)] for i,m in enumerate(starts)}
    aggregate=jobs['real-connectors']; witness=jobs['connector-witnesses']; jars=jobs['connector-jars']; plan=jobs['connector-plan']
    assert 'needs: connector-plan' in jars
    assert "if: steps.plan.outputs.requires_atlas == 'true'" in plan
    assert "jq -r '.requires_atlas_fixtures'" in plan and '.connector_deferrals' in plan
    assert plan.index('connector-witnesses.sh plan --input-mode') < plan.index('test -n "$TAPSTATE_ATLAS_TEST_URI"')
    for name in ('TAPSTATE_ATLAS_TEST_URI', 'TAPSTATE_ATLAS_STANDARD_URI'):
        binding = name + ': ${{ secrets.' + name + ' }}'
        assert binding in plan and binding in witness
        assert 'test -n "$' + name + '"' in plan
    assert 'prepare-test-inputs.py --output "$RUNNER_TEMP/cloud-test-release"' in witness
    assert 'echo "TAPSTATE_TEST_CLOUD_RELEASE_DIR=$RUNNER_TEMP/cloud-test-release" >> "$GITHUB_ENV"' in witness
    assert witness.index('prepare-test-inputs.py') < witness.index('connector-witnesses.sh run --input-mode')
    assert 'matrix:' not in aggregate and 'if: always()' in aggregate
    assert 'needs: [connector-plan, connector-witnesses]' in aggregate
    assert 'connector-witnesses.sh verify --input-mode' in aggregate and 'merge-multiple: true' in aggregate
    assert 'SHARD_RESULT: ${{ needs.connector-witnesses.result }}' in aggregate
    assert 'run: test "$SHARD_RESULT" = success' in aggregate
    assert 'run: .github/scripts/witness-gate.sh' in aggregate
    assert 'shard: [shard-1, shard-2, shard-3, shard-4, shard-5, shard-6, shard-7, shard-8, shard-9, shard-10]' in witness and 'fail-fast: false' in witness
    assert 'needs: [connector-plan, connector-jars]' in witness
    assert '-DskipTests install' in witness and '-Dmaven.repo.local="$RUNNER_TEMP/m2"' in witness
    assert 'connector-witnesses.sh run --input-mode' in witness and 'connector-witnesses.sh pack --input-mode' in witness
    assert '-Dapi.version=1.44 -Dtapstate.e2e.connectors-dir=' in witness
    assert ' -am ' not in witness
    assert 'restore-keys:' not in jars
    assert 'cache-dependency-path: |' in jars
    for dependency in ['**/pom.xml', '.github/scripts/connector-cache.sh', 'scripts/build-real-connectors.sh', '.github/maven-settings-connectors.xml']:
        assert re.search(r'^            ' + re.escape(dependency) + r'$', jars, re.M)
    assert jars.count('path: ${{ runner.temp }}/connector-m2/io/tapdata') == 2
    pdk_key="key: pdk-inputs-${{ runner.os }}-${{ steps.source.outputs.oss_sha }}-${{ steps.source.outputs.enterprise_sha }}-${{ hashFiles('.github/scripts/connector-cache.sh', 'scripts/build-real-connectors.sh', '.github/maven-settings-connectors.xml') }}"
    assert jars.count(pdk_key) == 2
    assert jars.index('id: pdk') < jars.index('connector-cache.sh prepare')
    assert jars.index('connector-cache.sh verify') < jars.index('name: Cache verified remote PDK dependencies')
    assert "if: env.CONNECTOR_INPUT_MODE == 'upstream' && (steps.pdk.outputs.cache-hit != 'true')" in jars
    assert 'connector-cache.sh prepare' in jars and 'connector-cache.sh seal' in jars and 'connector-cache.sh verify' in jars
    selected = re.findall(r'^      CONNECTOR_MODULES: (.+)$', jars, re.M)
    assert len(selected) == 1
    assert set(selected[0].split(',')) == {
        'mysql=connectors/mysql-connector', 'mongodb=connectors/mongodb-connector',
        'postgres=connectors/postgres-connector', 'oracle=connectors/oracle-connector',
        'sqlserver=connectors/mssql-connector', 'mongodb-atlas=connectors/mongodb-atlas-connector',
        'aws-rds-mysql=connectors/aws-rds-mysql-connector'}
    for command in ('connector-cache.sh prepare', 'scripts/build-real-connectors.sh'):
        assert command + ' --modules "$CONNECTOR_MODULES" --checkout ' in jars
    assert '-nsu -Dmaven.repo.local=' in jars
    assert 'repository: tapdata/tapdata-connectors-enterprise' in jars
    assert 'token: ${{ secrets.ENTERPRISE_CONNECTORS_READ_TOKEN }}' in jars
    assert 'persist-credentials: false' in jars
    assert 'mv "$GITHUB_WORKSPACE/.connector-enterprise-source" "$RUNNER_TEMP/connector-enterprise-source"' in jars
    for line in jars.splitlines():
        if 'connector-cache.sh prepare ' in line or 'connector-cache.sh seal ' in line or 'scripts/build-real-connectors.sh --modules ' in line:
            assert '--checkout "$RUNNER_TEMP/connector-source" --checkout "$RUNNER_TEMP/connector-enterprise-source"' in line
    assert "options: [upstream, release]" in text and "default: upstream" in text
    assert "name: ${{ inputs.connector-inputs == 'release' && 'real-connectors-release' || 'real-connectors' }}" in aggregate
    assert 'name: connector-input-identity' in jars and 'name: connector-input-identity' in aggregate
    assert '_connector-inputs.py prepare-release' in jars and '_connector-inputs.py verify' in jars
    for command in ['run', 'pack']:
        assert 'connector-witnesses.sh '+command+' --input-mode "$CONNECTOR_INPUT_MODE" --inputs "$RUNNER_TEMP/connectors/connector-inputs.json"' in witness
    assert 'connector-witnesses.sh verify --input-mode "$CONNECTOR_INPUT_MODE" --inputs "$RUNNER_TEMP/connector-input-identity/connector-inputs.json"' in aggregate
    upstream = jars[:jars.index('      - name: Prepare verified published connector inputs')]
    starts=list(re.finditer(r'^      - ',upstream,re.M))
    for i,match in enumerate(starts):
        block=upstream[match.start():starts[i+1].start() if i+1<len(starts) else len(upstream)]
        if 'actions/checkout@' not in block:
            assert "if: env.CONNECTOR_INPUT_MODE == 'upstream'" in block, 'upstream build step can run in Release mode'
    assert '-Dit.test=' not in text  # No second selector list can drift from source inventory.
assert "group: real-connectors-sharded-${{ github.ref }}-${{ inputs.connector-inputs || 'upstream' }}" in workflow
assert 'cancel-in-progress: true' in workflow
check(workflow)
assert '-f connector-inputs=release' in release
assert '--required real-connectors-release && break' in release
assert '--required real-connectors && break' not in release
assert release.count('sleep 20')==2 and 'sleep 60' not in release
for before,after in [
    ('  real-connectors:', '  real-connectors-renamed:'),
    ('real-connectors-release', 'unbound-release-verdict'),
    ('_connector-inputs.py verify', '_connector-inputs.py record'),
    ('--inputs "$RUNNER_TEMP/connectors/connector-inputs.json"', ''),
    ('--inputs "$RUNNER_TEMP/connector-input-identity/connector-inputs.json"', ''),
    ("if: env.CONNECTOR_INPUT_MODE == 'upstream'", "if: true"),
    ('    if: always()', '    if: success()'),
    ('connector-witnesses.sh verify --input-mode','true # connector-witnesses.sh verify --input-mode'),
    ('shard: [shard-1, shard-2, shard-3, shard-4, shard-5, shard-6, shard-7, shard-8, shard-9, shard-10]','shard: [shard-1, shard-2, shard-3]'),
    ('connector-cache.sh verify','connector-cache.sh key'),
    ('connector-cache.sh prepare --modules "$CONNECTOR_MODULES"', 'connector-cache.sh prepare'),
    ('scripts/build-real-connectors.sh --modules "$CONNECTOR_MODULES"', 'scripts/build-real-connectors.sh'),
    (',mongodb-atlas=connectors/mongodb-atlas-connector,aws-rds-mysql=connectors/aws-rds-mysql-connector', ''),
    ('path: ${{ runner.temp }}/connector-m2/io/tapdata', 'path: ~/.m2/repository'),
    ('pdk-inputs-${{ runner.os }}-${{ steps.source.outputs.oss_sha }}-${{ steps.source.outputs.enterprise_sha }}-', 'pdk-inputs-${{ runner.os }}-'),
    ('-${{ steps.source.outputs.enterprise_sha }}-', '-'),
    (' --checkout "$RUNNER_TEMP/connector-enterprise-source"', ''),
    ('    needs: connector-plan', '    needs: []'),
    ("if: steps.plan.outputs.requires_atlas == 'true'", "if: false"),
    ("jq -r '.requires_atlas_fixtures'", "jq -r 'false'"),
    ('TAPSTATE_ATLAS_TEST_URI: ${{ secrets.TAPSTATE_ATLAS_TEST_URI }}', 'TAPSTATE_ATLAS_TEST_URI: ""'),
    ('TAPSTATE_ATLAS_STANDARD_URI: ${{ secrets.TAPSTATE_ATLAS_STANDARD_URI }}', 'TAPSTATE_ATLAS_STANDARD_URI: ""'),
    ('prepare-test-inputs.py --output', 'true --output'),
    ('echo "TAPSTATE_TEST_CLOUD_RELEASE_DIR=', 'echo "UNUSED_CLOUD_RELEASE_DIR=')]:
    mutated=workflow.replace(before,after)
    assert mutated!=workflow
    try: check(mutated)
    except (AssertionError,KeyError,ValueError): pass
    else: raise AssertionError('workflow mutation survived: '+before)
print('connector-workflow smoke: fixed check name, explicit artifact selection/Atlas deferral, complete active admission and 19 mutations passed')
PY
