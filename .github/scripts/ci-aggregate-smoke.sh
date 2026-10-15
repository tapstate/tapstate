#!/usr/bin/env bash
# Admission must fail closed when an entire successful-looking shard disappears.
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
export SHARD_GATE="$here/ci-shard.sh" AGGREGATE_GATE="$here/ci-aggregate.sh"
for layout_count in 2 4 6; do
CI_LAYOUT_E2E_COUNT="$layout_count" python3 - <<'PY'
import hashlib, json, os, pathlib, shutil, subprocess, tempfile
from collections import Counter
import xml.etree.ElementTree as ET
with tempfile.TemporaryDirectory() as tmp:
    root=pathlib.Path(tmp); artifacts=root/'artifacts'
    def put(path,text):
        p=root/path; p.parent.mkdir(parents=True,exist_ok=True); p.write_text(text)
    put('pom.xml','<project><modules><module>e2e</module><module>runtime/engine</module><module>core</module><module>app</module></modules></project>')
    for module in ['e2e','runtime/engine','core','app']:
        put(module+'/pom.xml','<project><build><plugins><plugin><artifactId>maven-failsafe-plugin</artifactId><executions><execution><goals><goal>integration-test</goal></goals></execution></executions></plugin></plugins></build></project>')
    seeded = [('e2e', name, 'it') for name in ['OneIT','TwoIT','ThreeIT','FourIT','FiveIT','SixIT']] + [
        ('e2e','UnitTest','unit'),('runtime/engine','EngineTest','unit'),('core','DisabledTest','unit'),('app','AppTest','unit')]
    for module,name,kind in seeded:
        put(module+'/src/test/java/sample/'+name+'.java','package sample; class '+name+' {}')
    workers=int(os.environ['CI_LAYOUT_E2E_COUNT'])
    put('.github/ci-shard-layout.json',json.dumps({'schema':1,'e2e_shards':workers,'rest_modules':{'app':['app']} if workers==4 else {},'new_test_seconds':30}))
    subprocess.run([os.environ['SHARD_GATE'],'plan','--root',tmp,'--output',str(root/'plan.json')],check=True)
    plan=json.loads((root/'plan.json').read_text())
    assert Counter((t['module'],t['kind'],t['class']) for t in plan['expected'])==Counter((m,k,'sample.'+n) for m,n,k in seeded)
    e2e_ids=[shard['id'] for shard in plan['shards'] if all(t['module']=='e2e' and t['kind']=='it' for t in shard['tests'])]
    assert len(e2e_ids)==workers
    put('bin/mvn','#!/bin/sh\nexit 0\n'); (root/'bin/mvn').chmod(0o755)
    os.environ['PATH']=str(root/'bin')+':'+os.environ['PATH']
    for shard in plan['shards']:
        # Each runner has only its own fresh reports, despite having compiled the full reactor.
        for module in ['e2e','runtime/engine','core','app']:
            shutil.rmtree(root/module/'target',ignore_errors=True)
            put(module+'/target/classes/sample/Production.class','bytecode')
            put(module+'/target/test-classes/sample/Helper.class','helper-bytecode')
        # The real helper owns reset and records a successful selected verify.
        subprocess.run([os.environ['SHARD_GATE'],'run','--root',tmp,'--plan',str(root/'plan.json'),
                        '--shard',shard['id'],'--repo-local',str(root/'m2')],check=True,capture_output=True)
        if any(t['module']=='e2e' and t['kind']=='it' for t in shard['tests']):
            put('e2e/target/witness-ledger.txt','current ordinary witness\n')
            put('e2e/target/witness-ledgers/123.txt','current selected witness\n')
        for test in shard['tests']:
            phase='failsafe' if test['kind']=='it' else 'surefire'
            name=test['class']; count='0' if name.endswith('DisabledTest') else '1'
            put(test['module']+'/target/test-classes/'+name.replace('.', '/')+'.class','test-bytecode')
            report=f'<testsuite name="{name}" tests="{count}" failures="0" errors="0" skipped="0"><testcase classname="{name}" name="ok"/></testsuite>' if count=='1' else f'<testsuite name="{name}" tests="0" failures="0" errors="0" skipped="0"/>'
            put(test['module']+'/target/'+phase+'-reports/TEST-'+name+'.xml',report)
            if name.endswith(('EngineTest', 'OneIT', 'TwoIT')):
                put(test['module']+'/target/'+phase+'-reports/TEST-'+name+'$Nested.xml',f'<testsuite name="{name}$Nested" tests="1" failures="0" errors="0"><testcase classname="{name}$Nested" name="nested"/></testsuite>')
        for path in shard['required_exec']: put(path,'execution-data')
        subprocess.run([os.environ['AGGREGATE_GATE'],'pack','--root',tmp,'--plan',str(root/'plan.json'),'--shard',shard['id'],'--output',str(artifacts/shard['id'])],check=True)
        packed=json.loads((artifacts/shard['id']/'manifest.json').read_text())['files']
        assert not any('/target/classes/' in p for p in packed), 'production bytecode is rebuilt by the aggregate'
        owners={t['module'] for t in shard['tests']}
        assert {p.split('/target/')[0] for p in packed if '/test-classes/' in p}==owners, 'test bytecode must be limited to this shard modules'
    def verify(expect=True, rehash=False):
        # A self-consistent but incomplete manifest must still fail report admission.
        if rehash:
            for manifest in artifacts.glob('*/manifest.json'):
                m=json.loads(manifest.read_text()); files=manifest.parent/'files'
                m['files']={p.relative_to(files).as_posix():hashlib.sha256(p.read_bytes()).hexdigest() for p in files.rglob('*') if p.is_file()}
                manifest.write_text(json.dumps(m))
        shutil.rmtree(root/'restored',ignore_errors=True)
        result=subprocess.run([os.environ['AGGREGATE_GATE'],'verify','--root',tmp,'--plan',str(root/'plan.json'),'--artifacts',str(artifacts),'--restore',str(root/'restored')],capture_output=True,text=True)
        assert (result.returncode==0)==expect, result.stdout+result.stderr
        if not expect: assert not (root/'restored').exists()
    verify(); assert (root/'restored/target/ci-shards/rest/e2e/target/jacoco.exec').is_file()
    def case_counts(paths):
        return Counter((ET.parse(p).getroot().get('name'),c.get('classname'),c.get('name'))
                       for p in paths for c in ET.parse(p).getroot().findall('testcase'))
    assert case_counts(artifacts.rglob('TEST-*.xml'))==case_counts((root/'restored').rglob('TEST-*.xml'))
    assert len(list((root/'restored').rglob('TEST-*.xml')))==len(seeded)+3
    assert len(list((root/'restored/target/ci-shards').rglob('run.json')))==len(plan['shards'])
    for shard_id in e2e_ids:
        raw=root/'restored/target/ci-shards'/shard_id/'e2e/target'
        assert (raw/'witness-ledger.txt').read_text()=='current ordinary witness\n'
        assert (raw/'witness-ledgers/123.txt').read_text()=='current selected witness\n'
    assert not (root/'restored/e2e/target/witness-ledger.txt').exists(), 'raw shard ledgers must not collide'
    backup=root/'backup'; shutil.copytree(artifacts,backup)
    def reset(): shutil.rmtree(artifacts); shutil.copytree(backup,artifacts)
    for path in ['engine/files/runtime/engine/target/classes/sample/Production.class',
                 'engine/files/core/target/test-classes/sample/Helper.class']:
        put('artifacts/'+path,'unexpected-bytecode')
        verify(False, rehash=True); reset()
    # Distinct ordinary and nested suites cannot silently collide in the restored namespace.
    for pattern in ['TEST-sample.*IT.xml', 'TEST-sample.*IT$Nested.xml']:
        for p in artifacts.rglob(pattern): p.rename(p.with_name('TEST-shared.xml'))
        verify(False, rehash=True); reset()
    p=next(artifacts.rglob('TEST-sample.EngineTest$Nested.xml')); p.rename(p.with_name('TEST-renamed.xml'))
    verify(False, rehash=True); reset()
    for shard in plan['shards']:
        shutil.rmtree(artifacts/shard['id']); verify(False); reset()
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
    # A declared raw ledger and successful source/run-bound verify are mandatory evidence.
    ledger=artifacts/'e2e-1/files/e2e/target/witness-ledger.txt'
    ledger.unlink(); verify(False, rehash=True); reset()
    put('artifacts/'+e2e_ids[0]+'/files/e2e/target/witness-ledgers/extra.txt','unknown ledger')
    verify(False, rehash=True); reset()
    for field,value in [('source_sha','old'), ('status','failure'), ('run_id','old-run'), ('run_attempt',999)]:
        manifest=artifacts/'engine/manifest.json'; m=json.loads(manifest.read_text())
        m['provenance'][field]=value; manifest.write_text(json.dumps(m))
        raw=artifacts/'engine/files'/m['provenance_path']; raw.write_text(json.dumps(m['provenance']))
        verify(False, rehash=True); reset()
    # Inner phase metadata is a versioned run observation, not arbitrary truthy JSON.
    for field,value in [('schema',999), ('command','pack'), ('run_attempt',True)]:
        manifest=artifacts/'engine/manifest.json'; m=json.loads(manifest.read_text())
        m['provenance']['verify'][field]=value; manifest.write_text(json.dumps(m))
        (artifacts/'engine/files'/m['provenance_path']).write_text(json.dumps(m['provenance']))
        verify(False, rehash=True); reset()
    manifest=artifacts/'engine/manifest.json'; m=json.loads(manifest.read_text())
    for phase in ['reset','verify']:
        for field in ['started_monotonic_ns','monotonic_ns','elapsed_ns']:
            m['provenance'][phase][field]=False
    manifest.write_text(json.dumps(m)); (artifacts/'engine/files'/m['provenance_path']).write_text(json.dumps(m['provenance']))
    verify(False, rehash=True); reset()
    manifest=artifacts/'engine/manifest.json'; m=json.loads(manifest.read_text())
    m['provenance']['verify']['started_monotonic_ns']=0
    m['provenance']['verify']['elapsed_ns']=m['provenance']['verify']['monotonic_ns']
    manifest.write_text(json.dumps(m)); (artifacts/'engine/files'/m['provenance_path']).write_text(json.dumps(m['provenance']))
    verify(False, rehash=True); reset()
    manifest=artifacts/'engine/manifest.json'; m=json.loads(manifest.read_text()); m['schema']=1; manifest.write_text(json.dumps(m))
    verify(False); reset()
    marker=artifacts/'engine/files/target/ci-shards/run-engine.json'; marker.unlink()
    verify(False, rehash=True); reset()
    # Admission must preflight every restore path before any shard changes the destination.
    with tempfile.TemporaryDirectory() as external_tmp:
        outside=pathlib.Path(external_tmp)
        foreign=outside/'ci-shards/e2e-1/run.json'; foreign.parent.mkdir(parents=True); foreign.write_text('external receipt')
        destination=root/'linked-restore'; destination.mkdir()
        (destination/'target').symlink_to(outside,target_is_directory=True)
        result=subprocess.run([os.environ['AGGREGATE_GATE'],'verify','--root',tmp,'--plan',str(root/'plan.json'),
                               '--artifacts',str(artifacts),'--restore',str(destination)],capture_output=True,text=True)
        assert result.returncode!=0 and 'unsafe restore path' in result.stderr, 'linked restore destination was admitted'
        assert foreign.read_text()=='external receipt', 'restore overwrote external metadata'
        assert {p.name for p in destination.iterdir()}=={'target'}, 'unsafe restore partially wrote ordinary evidence'
    # A test added after planning invalidates the source-selected cohort.
    put('core/src/test/java/sample/NewTest.java','package sample; class NewTest {}'); verify(False)
print('ci-aggregate smoke: admission controls, every planned worker and shard-local payload passed for '+str(workers)+' E2E workers')
PY
done
