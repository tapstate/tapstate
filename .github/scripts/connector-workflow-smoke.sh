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
    assert 'shard: [shard-1, shard-2, shard-3, shard-4]' in witness and 'fail-fast: false' in witness
    assert 'needs: [connector-plan, connector-jars]' in witness
    assert '-DskipTests install' in witness and '-Dmaven.repo.local="$RUNNER_TEMP/m2"' in witness
    assert 'connector-witnesses.sh run --plan' in witness and 'connector-witnesses.sh pack --plan' in witness
    assert '-Dapi.version=1.44 -Dtapstate.e2e.connectors-dir=' in witness
    assert ' -am ' not in witness
    assert 'restore-keys:' not in jars
    assert 'connector-cache.sh prepare' in jars and 'connector-cache.sh seal' in jars and 'connector-cache.sh verify' in jars
    assert '-nsu -Dmaven.repo.local=' in jars
    assert '-Dit.test=' not in text  # No second selector list can drift from source inventory.
check(workflow)
assert release.count('sleep 20')==2 and 'sleep 60' not in release
for before,after in [
    ('  real-connectors:', '  real-connectors-renamed:'),
    ('    if: always()', '    if: success()'),
    ('connector-witnesses.sh verify --plan','true # connector-witnesses.sh verify --plan'),
    ('shard: [shard-1, shard-2, shard-3, shard-4]','shard: [shard-1, shard-2, shard-3]'),
    ('connector-cache.sh verify','connector-cache.sh key')]:
    mutated=workflow.replace(before,after)
    assert mutated!=workflow
    try: check(mutated)
    except (AssertionError,KeyError): pass
    else: raise AssertionError('workflow mutation survived: '+before)
print('connector-workflow smoke: fixed check name, complete admission and 5 mutations passed')
PY
