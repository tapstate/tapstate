#!/usr/bin/env bash
# The scanner must never run when even one declared shard input is missing.
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
export SHARD_GATE="$here/ci-shard.sh" AGGREGATE_GATE="$here/ci-aggregate.sh" SONAR_GATE="$here/sonar-inputs.sh"
for layout_count in 2 4 6; do
CI_LAYOUT_E2E_COUNT="$layout_count" python3 - <<'PY'
import json, os, pathlib, shutil, subprocess, tempfile
with tempfile.TemporaryDirectory() as tmp:
    root=pathlib.Path(tmp); artifacts=root/'artifacts'
    def put(path,text):
        p=root/path; p.parent.mkdir(parents=True,exist_ok=True); p.write_text(text)
    put('pom.xml','<project><modules><module>e2e</module><module>runtime/engine</module><module>core</module></modules></project>')
    for module in ['e2e','runtime/engine','core']:
        put(module+'/pom.xml','<project><build><plugins><plugin><artifactId>maven-failsafe-plugin</artifactId><executions><execution><goals><goal>integration-test</goal></goals></execution></executions></plugin></plugins></build></project>')
    for module,name in [('e2e',name) for name in ['OneIT','TwoIT','ThreeIT','FourIT','FiveIT','SixIT']]+[('runtime/engine','EngineTest'),('core','CoreTest')]:
        put(module+'/src/test/java/sample/'+name+'.java','package sample; class '+name+' {}')
    workers=int(os.environ['CI_LAYOUT_E2E_COUNT'])
    put('.github/ci-shard-layout.json',json.dumps({'schema':1,'e2e_shards':workers,'rest_modules':{},'new_test_seconds':30}))
    subprocess.run([os.environ['SHARD_GATE'],'plan','--root',tmp,'--output',str(root/'plan.json')],check=True)
    plan=json.loads((root/'plan.json').read_text())
    e2e_ids=[shard['id'] for shard in plan['shards'] if all(t['module']=='e2e' and t['kind']=='it' for t in shard['tests'])]
    assert len(e2e_ids)==workers
    put('bin/mvn','#!/bin/sh\nexit 0\n'); (root/'bin/mvn').chmod(0o755)
    os.environ['PATH']=str(root/'bin')+':'+os.environ['PATH']
    for shard in plan['shards']:
        for module in ['e2e','runtime/engine','core']:
            shutil.rmtree(root/module/'target',ignore_errors=True)
            put(module+'/target/classes/sample/Production.class','bytecode')
        # Exercise the real reset/verify freshness protocol before fixture reports are written.
        subprocess.run([os.environ['SHARD_GATE'],'run','--root',tmp,'--plan',str(root/'plan.json'),
                        '--shard',shard['id'],'--repo-local',str(root/'m2')],check=True,capture_output=True)
        for test in shard['tests']:
            phase='failsafe' if test['kind']=='it' else 'surefire'; name=test['class']
            put(test['module']+'/target/test-classes/'+name.replace('.', '/')+'.class','test-bytecode')
            put(test['module']+'/target/'+phase+'-reports/TEST-'+name+'.xml',f'<testsuite name="{name}" tests="1" failures="0" errors="0" skipped="0"><testcase classname="{name}" name="ok"/></testsuite>')
        for path in shard['required_exec']: put(path,'execution-data')
        subprocess.run([os.environ['AGGREGATE_GATE'],'pack','--root',tmp,'--plan',str(root/'plan.json'),'--shard',shard['id'],'--output',str(artifacts/shard['id'])],check=True)
    backup=root/'backup'; shutil.copytree(artifacts,backup)
    def reset(): shutil.rmtree(artifacts); shutil.copytree(backup,artifacts)
    def check(expect):
        # The marker represents a scanner that would otherwise accept zero coverage.
        marker=root/'scanner-ran'; marker.unlink(missing_ok=True)
        result=subprocess.run(['bash','-e','-c','"$SONAR_GATE" --root "$1" --plan "$1/plan.json" --artifacts "$1/artifacts" --restore "$1/restored"; touch "$1/scanner-ran"','smoke',tmp],capture_output=True,text=True)
        assert (result.returncode==0)==expect, result.stdout+result.stderr
        assert marker.exists()==expect, 'scanner ran after failed input admission'
    check(True) # Every E2E IT shard deliberately has no execution data.
    for path in artifacts.rglob('*.exec'): path.unlink()
    check(False); reset()
    # Another shard still has coverage: a global "any exec" check would miss this.
    (artifacts/'engine/files/runtime/engine/target/jacoco.exec').unlink()
    manifest=artifacts/'engine/manifest.json'; data=json.loads(manifest.read_text())
    data['files'].pop('runtime/engine/target/jacoco.exec'); manifest.write_text(json.dumps(data))
    check(False); reset()
    for shard in plan['shards']:
        shutil.rmtree(artifacts/shard['id']); check(False); reset()
    next((artifacts/e2e_ids[0]/'files/e2e/target/failsafe-reports').glob('TEST-*.xml')).unlink(); check(False); reset()
    manifest=artifacts/'rest/manifest.json'; data=json.loads(manifest.read_text()); data['source_sha']='another-commit'; manifest.write_text(json.dumps(data)); check(False); reset()
    path=artifacts/'engine/files/runtime/engine/target/test-classes/sample/EngineTest.class'; path.unlink(); check(False)
print('sonar-inputs smoke: input admission passed for '+str(workers)+' E2E workers, including every missing worker')
PY
done
