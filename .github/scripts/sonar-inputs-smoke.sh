#!/usr/bin/env bash
# The scanner must never run when even one declared shard input is missing.
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
export SHARD_GATE="$here/ci-shard.sh" AGGREGATE_GATE="$here/ci-aggregate.sh" SONAR_GATE="$here/sonar-inputs.sh"
python3 - <<'PY'
import json, os, pathlib, shutil, subprocess, tempfile
with tempfile.TemporaryDirectory() as tmp:
    root=pathlib.Path(tmp); artifacts=root/'artifacts'
    def put(path,text):
        p=root/path; p.parent.mkdir(parents=True,exist_ok=True); p.write_text(text)
    put('pom.xml','<project><modules><module>e2e</module><module>runtime/engine</module><module>core</module></modules></project>')
    for module in ['e2e','runtime/engine','core']:
        put(module+'/pom.xml','<project><build><plugins><plugin><artifactId>maven-failsafe-plugin</artifactId><executions><execution><goals><goal>integration-test</goal></goals></execution></executions></plugin></plugins></build></project>')
    for module,name in [('e2e','OneIT'),('e2e','TwoIT'),('runtime/engine','EngineTest'),('core','CoreTest')]:
        put(module+'/src/test/java/sample/'+name+'.java','package sample; class '+name+' {}')
    subprocess.run([os.environ['SHARD_GATE'],'plan','--root',tmp,'--output',str(root/'plan.json')],check=True)
    plan=json.loads((root/'plan.json').read_text())
    for shard in plan['shards']:
        for module in ['e2e','runtime/engine','core']:
            shutil.rmtree(root/module/'target',ignore_errors=True)
            put(module+'/target/classes/sample/Production.class','bytecode')
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
    check(True) # The two e2e IT shards deliberately have no execution data.
    for path in artifacts.rglob('*.exec'): path.unlink()
    check(False); reset()
    # Another shard still has coverage: a global "any exec" check would miss this.
    (artifacts/'engine/files/runtime/engine/target/jacoco.exec').unlink()
    manifest=artifacts/'engine/manifest.json'; data=json.loads(manifest.read_text())
    data['files'].pop('runtime/engine/target/jacoco.exec'); manifest.write_text(json.dumps(data))
    check(False); reset()
    shutil.rmtree(artifacts/'e2e-2'); check(False); reset()
    next((artifacts/'e2e-1/files/e2e/target/failsafe-reports').glob('TEST-*.xml')).unlink(); check(False); reset()
    manifest=artifacts/'rest/manifest.json'; data=json.loads(manifest.read_text()); data['source_sha']='another-commit'; manifest.write_text(json.dumps(data)); check(False); reset()
    path=artifacts/'engine/files/runtime/engine/target/test-classes/sample/EngineTest.class'; path.unlink(); check(False)
print('sonar-inputs smoke: 7 cases passed')
PY
