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
    aggregate=jobs['real-connectors']; witness=jobs['connector-witnesses']; jars=jobs['connector-jars']
    assert 'matrix:' not in aggregate and 'if: always()' in aggregate
    assert 'needs: [connector-plan, connector-witnesses]' in aggregate
    assert 'connector-witnesses.sh verify --plan' in aggregate and 'merge-multiple: true' in aggregate
    assert 'SHARD_RESULT: ${{ needs.connector-witnesses.result }}' in aggregate
    assert 'run: test "$SHARD_RESULT" = success' in aggregate
    assert 'run: .github/scripts/witness-gate.sh' in aggregate
    assert 'shard: [shard-1, shard-2, shard-3, shard-4, shard-5, shard-6, shard-7, shard-8, shard-9, shard-10]' in witness and 'fail-fast: false' in witness
    assert 'needs: [connector-plan, connector-jars]' in witness
    assert '-DskipTests install' in witness and '-Dmaven.repo.local="$RUNNER_TEMP/m2"' in witness
    assert 'connector-witnesses.sh run --plan' in witness and 'connector-witnesses.sh pack --plan' in witness
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
    assert "if: steps.pdk.outputs.cache-hit != 'true'" in jars
    assert 'connector-cache.sh prepare' in jars and 'connector-cache.sh seal' in jars and 'connector-cache.sh verify' in jars
    assert '-nsu -Dmaven.repo.local=' in jars
    assert 'repository: tapdata/tapdata-connectors-enterprise' in jars
    assert 'token: ${{ secrets.ENTERPRISE_CONNECTORS_READ_TOKEN }}' in jars
    assert 'persist-credentials: false' in jars
    assert 'mv "$GITHUB_WORKSPACE/.connector-enterprise-source" "$RUNNER_TEMP/connector-enterprise-source"' in jars
    for line in jars.splitlines():
        if 'connector-cache.sh prepare ' in line or 'connector-cache.sh seal ' in line or 'scripts/build-real-connectors.sh --checkout ' in line:
            assert '--checkout "$RUNNER_TEMP/connector-source" --checkout "$RUNNER_TEMP/connector-enterprise-source"' in line
    assert '-Dit.test=' not in text  # No second selector list can drift from source inventory.
assert 'group: real-connectors-sharded-${{ github.ref }}' in workflow
assert 'cancel-in-progress: true' in workflow
check(workflow)
assert release.count('sleep 20')==2 and 'sleep 60' not in release
for before,after in [
    ('  real-connectors:', '  real-connectors-renamed:'),
    ('    if: always()', '    if: success()'),
    ('connector-witnesses.sh verify --plan','true # connector-witnesses.sh verify --plan'),
    ('shard: [shard-1, shard-2, shard-3, shard-4, shard-5, shard-6, shard-7, shard-8, shard-9, shard-10]','shard: [shard-1, shard-2, shard-3]'),
    ('connector-cache.sh verify','connector-cache.sh key'),
    ('path: ${{ runner.temp }}/connector-m2/io/tapdata', 'path: ~/.m2/repository'),
    ('pdk-inputs-${{ runner.os }}-${{ steps.source.outputs.oss_sha }}-${{ steps.source.outputs.enterprise_sha }}-', 'pdk-inputs-${{ runner.os }}-'),
    ('-${{ steps.source.outputs.enterprise_sha }}-', '-'),
    (' --checkout "$RUNNER_TEMP/connector-enterprise-source"', '')]:
    mutated=workflow.replace(before,after)
    assert mutated!=workflow
    try: check(mutated)
    except (AssertionError,KeyError,ValueError): pass
    else: raise AssertionError('workflow mutation survived: '+before)
print('connector-workflow smoke: fixed check name, complete admission and 9 mutations passed')
PY
