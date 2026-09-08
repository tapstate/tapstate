#!/usr/bin/env bash
# Source patterns, complete shard evidence, and the sweep ledger fail closed.
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
export WITNESS_GATE="$here/connector-witnesses.sh"
python3 - <<'PY'
import hashlib, json, os, pathlib, shutil, subprocess, tempfile
import xml.etree.ElementTree as ET
with tempfile.TemporaryDirectory() as tmp:
    root=pathlib.Path(tmp); artifacts=root/'artifacts'
    def put(path,text):
        p=root/path; p.parent.mkdir(parents=True,exist_ok=True); p.write_text(text)
    put('pom.xml','<project><modules><module>e2e</module></modules></project>')
    put('e2e/pom.xml','<project><build><plugins><plugin><artifactId>maven-failsafe-plugin</artifactId><executions><execution><goals><goal>integration-test</goal></goals></execution></executions></plugin></plugins></build></project>')
    names=['PublishedExamplesIT','RealMysqlToMongoFreshIT','NestFreshIT','DataBrowserFreshIT','WatchFreshIT','AnObjectIdReadsBackTheSameThroughBothFacesIT','SinkValueRoundTripIT']
    for name in names+['UnrelatedIT']:
        put('e2e/src/test/java/sample/'+name+'.java','package sample; class '+name+' {}')
    put('e2e/examples/sample/pipeline.yaml','fixture')
    put('e2e/witness-manifest.txt','sample on IN_PROCESS\nsample on REAL_PROCESS\n')
    def call(command,*extra,ok=True):
        result=subprocess.run([os.environ['WITNESS_GATE'],command,'--root',tmp,*map(str,extra)],capture_output=True,text=True)
        assert (result.returncode==0)==ok,result.stdout+result.stderr
        return result
    call('plan','--output',root/'plan.json')
    plan=json.loads((root/'plan.json').read_text()); assert len(plan['shards'])==4
    assert {t['class'] for t in plan['expected']}=={'sample.'+n for n in names}
    sweep=next(s['id'] for s in plan['shards'] if any(t['class'].endswith('.PublishedExamplesIT') for t in s['tests']))
    for shard in plan['shards']:
        shutil.rmtree(root/'e2e/target',ignore_errors=True)
        for test in shard['tests']:
            name=test['class']; put('e2e/target/test-classes/'+name.replace('.','/')+'.class','bytecode')
            put('e2e/target/failsafe-reports/TEST-'+name+'.xml',f'<testsuite name="{name}" tests="1" failures="0" errors="0" skipped="0"><testcase classname="{name}" name="ok"/></testsuite>')
        if shard['id']==sweep: put('e2e/target/witness-ledger.txt',(root/'e2e/witness-manifest.txt').read_text())
        call('pack','--plan',root/'plan.json','--shard',shard['id'],'--output',artifacts/shard['id'])
    def verify(ok=True): call('verify','--plan',root/'plan.json','--artifacts',artifacts,'--restore',root/'restored',ok=ok)
    verify(); assert (root/'restored/e2e/target/witness-ledger.txt').read_text()==(root/'e2e/witness-manifest.txt').read_text()
    backup=root/'backup'; shutil.copytree(artifacts,backup)
    def reset(): shutil.rmtree(artifacts); shutil.copytree(backup,artifacts)
    shutil.rmtree(artifacts/'shard-4'); verify(False); reset()
    (artifacts/sweep/'witness-ledger.txt').unlink(); verify(False); reset()
    (artifacts/sweep/'witness-ledger.txt').write_text(''); verify(False); reset()
    ledger=artifacts/sweep/'witness-ledger.txt'; ledger.write_text('sample on IN_PROCESS\n')
    identity=artifacts/sweep/'ledger.json'; m=json.loads(identity.read_text()); m['sha256']=hashlib.sha256(ledger.read_bytes()).hexdigest(); identity.write_text(json.dumps(m)); verify(False); reset()
    # Rehash every semantic mutation so content validation, not a checksum, catches it.
    def rehash():
        for path in artifacts.glob('*/manifest.json'):
            m=json.loads(path.read_text()); files=path.parent/'files'
            m['files']={p.relative_to(files).as_posix():hashlib.sha256(p.read_bytes()).hexdigest() for p in files.rglob('*') if p.is_file()}
            m['statistics']['tests']=sum(int(ET.parse(p).getroot().get('tests','0')) for p in files.rglob('TEST-*.xml'))
            m['statistics']['skipped']=sum(int(ET.parse(p).getroot().get('skipped','0')) for p in files.rglob('TEST-*.xml'))
            m['statistics']['zero_case_classes']=sum(int(ET.parse(p).getroot().get('tests','0'))==0 for p in files.rglob('TEST-*.xml'))
            path.write_text(json.dumps(m))
    report=next(artifacts.rglob('TEST-*DataBrowser*.xml')); report.write_text(report.read_text().replace('tests="1"','tests="0"').replace('<testcase classname="sample.DataBrowserFreshIT" name="ok"/>','')); rehash(); verify(False); reset()
    report=next(artifacts.rglob('TEST-*Watch*.xml')); report.write_text(report.read_text().replace('skipped="0"','skipped="1"').replace('name="ok"/>','name="ok"><skipped/></testcase>')); rehash(); verify(False); reset()
    other=next(s['id'] for s in plan['shards'] if s['id']!=sweep); shutil.copy(artifacts/sweep/'witness-ledger.txt',artifacts/other/'witness-ledger.txt'); verify(False); reset()
    put('e2e/src/test/java/sample/WatchAddedIT.java','package sample; class WatchAddedIT {}'); verify(False)
    (root/'e2e/src/test/java/sample/WatchAddedIT.java').unlink()
    # A stale ledger cannot survive the shard runner, even if Maven fails before discovery.
    put('e2e/target/witness-ledger.txt','stale')
    put('bin/mvn','#!/bin/sh\nexit 1\n'); (root/'bin/mvn').chmod(0o755)
    old=os.environ['PATH']; os.environ['PATH']=str(root/'bin')+':'+old
    call('run','--plan',root/'plan.json','--shard',sweep,'--repo-local',root/'m2',ok=False)
    assert not (root/'e2e/target/witness-ledger.txt').exists()
print('connector-witnesses smoke: source selection and 10 admission/run cases passed')
PY
