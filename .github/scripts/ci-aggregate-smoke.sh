#!/usr/bin/env bash
# Admission must fail closed when an entire successful-looking shard disappears.
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
export SHARD_GATE="$here/ci-shard.sh" AGGREGATE_GATE="$here/ci-aggregate.sh"
python3 - <<'PY'
import hashlib, json, os, pathlib, shutil, subprocess, tempfile
with tempfile.TemporaryDirectory() as tmp:
    root=pathlib.Path(tmp); artifacts=root/'artifacts'
    def put(path,text):
        p=root/path; p.parent.mkdir(parents=True,exist_ok=True); p.write_text(text)
    put('pom.xml','<project><modules><module>e2e</module><module>runtime/engine</module><module>core</module></modules></project>')
    for module in ['e2e','runtime/engine','core']:
        put(module+'/pom.xml','<project><build><plugins><plugin><artifactId>maven-failsafe-plugin</artifactId><executions><execution><goals><goal>integration-test</goal></goals></execution></executions></plugin></plugins></build></project>')
    for module,name in [('e2e','OneIT'),('e2e','TwoIT'),('e2e','UnitTest'),('runtime/engine','EngineTest'),('core','DisabledTest')]:
        put(module+'/src/test/java/sample/'+name+'.java','package sample; class '+name+' {}')
    subprocess.run([os.environ['SHARD_GATE'],'plan','--root',tmp,'--output',str(root/'plan.json')],check=True)
    plan=json.loads((root/'plan.json').read_text())
    for shard in plan['shards']:
        # Each runner has only its own fresh reports, despite having compiled the full reactor.
        for module in ['e2e','runtime/engine','core']:
            shutil.rmtree(root/module/'target',ignore_errors=True)
            put(module+'/target/classes/sample/Production.class','bytecode')
        for test in shard['tests']:
            phase='failsafe' if test['kind']=='it' else 'surefire'
            name=test['class']; count='0' if name.endswith('DisabledTest') else '1'
            put(test['module']+'/target/test-classes/'+name.replace('.', '/')+'.class','test-bytecode')
            report=f'<testsuite name="{name}" tests="{count}" failures="0" errors="0" skipped="0"><testcase classname="{name}" name="ok"/></testsuite>' if count=='1' else f'<testsuite name="{name}" tests="0" failures="0" errors="0" skipped="0"/>'
            put(test['module']+'/target/'+phase+'-reports/TEST-'+name+'.xml',report)
            if name.endswith('EngineTest'):
                put(test['module']+'/target/'+phase+'-reports/TEST-'+name+'$Nested.xml',f'<testsuite name="{name}$Nested" tests="1" failures="0" errors="0"><testcase classname="{name}$Nested" name="nested"/></testsuite>')
        for path in shard['required_exec']: put(path,'execution-data')
        subprocess.run([os.environ['AGGREGATE_GATE'],'pack','--root',tmp,'--plan',str(root/'plan.json'),'--shard',shard['id'],'--output',str(artifacts/shard['id'])],check=True)
    def verify(expect=True, rehash=False):
        # A self-consistent but incomplete manifest must still fail report admission.
        if rehash:
            for manifest in artifacts.glob('*/manifest.json'):
                m=json.loads(manifest.read_text()); files=manifest.parent/'files'
                m['files']={p.relative_to(files).as_posix():hashlib.sha256(p.read_bytes()).hexdigest() for p in files.rglob('*') if p.is_file()}
                manifest.write_text(json.dumps(m))
        result=subprocess.run([os.environ['AGGREGATE_GATE'],'verify','--root',tmp,'--plan',str(root/'plan.json'),'--artifacts',str(artifacts),'--restore',str(root/'restored')],capture_output=True,text=True)
        assert (result.returncode==0)==expect, result.stdout+result.stderr
    verify(); assert (root/'restored/target/ci-shards/rest/e2e/target/jacoco.exec').is_file()
    backup=root/'backup'; shutil.copytree(artifacts,backup)
    def reset(): shutil.rmtree(artifacts); shutil.copytree(backup,artifacts)
    shutil.rmtree(artifacts/'e2e-2'); verify(False); reset()
    manifest=artifacts/'rest/manifest.json'; m=json.loads(manifest.read_text()); m['source_sha']='wrong'; manifest.write_text(json.dumps(m)); verify(False); reset()
    (artifacts/'rest/files/e2e/target/jacoco.exec').unlink(); verify(False); reset()
    report=next((artifacts/'engine/files/runtime/engine/target/surefire-reports').glob('TEST-*.xml'))
    report.write_text(report.read_text().replace('failures="0"','failures="1"')); verify(False, rehash=True); reset()
    reports=artifacts/'engine/files/runtime/engine/target/surefire-reports'; report=next(reports.glob('TEST-*.xml')); shutil.copy(report,reports/'TEST-duplicate.xml'); verify(False, rehash=True); reset()
    reports=artifacts/'rest/files/core/target/surefire-reports'; next(reports.glob('TEST-*.xml')).unlink(); verify(False, rehash=True); reset()
    reports=artifacts/'rest/files/core/target/surefire-reports'; (reports/'TEST-extra.xml').write_text('<testsuite name="sample.ExtraTest" tests="0" errors="0" failures="0"/>'); verify(False, rehash=True); reset()
    shutil.copytree(artifacts/'engine',artifacts/'unexpected'); verify(False); reset()
    manifest=artifacts/'engine/manifest.json'; m=json.loads(manifest.read_text()); m['required_exec']=[]; manifest.write_text(json.dumps(m)); verify(False); reset()
    (artifacts/'engine/files/runtime/engine/target/test-classes/sample/EngineTest.class').unlink(); verify(False, rehash=True); reset()
    alien=artifacts/'engine/files/other/target/surefire-reports'; alien.mkdir(parents=True)
    (alien/'TEST-extra.xml').write_text('<testsuite name="sample.ExtraTest" tests="1" errors="0" failures="0"/>'); verify(False, rehash=True); reset()
    # A test added after planning invalidates the source-selected cohort.
    put('core/src/test/java/sample/NewTest.java','package sample; class NewTest {}'); verify(False)
print('ci-aggregate smoke: 14 admission cases passed')
PY
