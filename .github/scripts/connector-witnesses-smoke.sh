#!/usr/bin/env bash
# Source selection, stable execution identities, and raw shard evidence fail closed.
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
export WITNESS_GATE="$here/connector-witnesses.sh"
python3 - <<'PY'
import copy, hashlib, json, os, pathlib, shutil, subprocess, tempfile
from collections import Counter
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
    for n in range(12): put(f'e2e/examples/sample{n}/case.e2e.yml','fixture')
    put('e2e/witness-manifest.txt',''.join(f'sample{n} on {tier}\n' for n in range(12) for tier in ['IN_PROCESS','REAL_PROCESS']))
    def call(command,*extra,ok=True):
        result=subprocess.run([os.environ['WITNESS_GATE'],command,'--root',tmp,*map(str,extra)],capture_output=True,text=True)
        assert (result.returncode==0)==ok,result.stdout+result.stderr
        return result
    put('durations.json',json.dumps({'sample.PublishedExamplesIT':10000, 'sample.PublishedExamplesIT#examples/sample0/case.e2e.yml':100}))
    call('plan','--durations',root/'durations.json','--output',root/'plan.json')
    plan=json.loads((root/'plan.json').read_text()); assert len(plan['shards'])==10
    assert {t['class'] for t in plan['expected']}=={'sample.'+n for n in names}
    assert len(plan['expected'])==18
    assert plan['shards'][0]['estimated_seconds']==100 and len(plan['shards'][0]['tests'])==1
    assert all(s['estimated_seconds']==len(s['tests']) for s in plan['shards'][1:])
    expected=Counter(t['example']+' on '+tier for t in plan['expected'] if 'example' in t for tier in ['IN_PROCESS','REAL_PROCESS'])
    sweep=[s['id'] for s in plan['shards'] if any('example' in t for t in s['tests'])]
    assert len(sweep)>1
    def pack(shard,ok=True): call('pack','--plan',root/'plan.json','--shard',shard['id'],'--output',artifacts/shard['id'],ok=ok)
    for shard in plan['shards']:
        shutil.rmtree(root/'e2e/target',ignore_errors=True)
        suites={}; ledger=[]
        for test in shard['tests']:
            name=test['class']; put('e2e/target/test-classes/'+name.replace('.','/')+'.class','bytecode')
            suite=suites.setdefault(name,ET.Element('testsuite',name=name,failures='0',errors='0',skipped='0',time='1'))
            for tier in (['IN_PROCESS','REAL_PROCESS'] if 'example' in test else [None]):
                case=ET.SubElement(suite,'testcase',classname=name,name='run[1]',time='1')
                if tier:
                    identity=test['example']+' on '+tier; ledger.append(identity)
                    ET.SubElement(case,'system-out').text='tapstate.published-case='+identity+'\n'
        for name,suite in suites.items():
            suite.set('tests',str(len(suite.findall('testcase')))); put('e2e/target/failsafe-reports/TEST-'+name+'.xml',ET.tostring(suite,encoding='unicode'))
        if ledger:
            put('e2e/target/witness-ledgers/123.txt','\n'.join(ledger)+'\n')
            # Another JVM must not replace or hide a previous sweep's file.
            put('e2e/target/witness-ledgers/456.txt','\n'.join(ledger)+'\n'); pack(shard,False)
            (root/'e2e/target/witness-ledgers/456.txt').unlink()
            ledger_path=root/'e2e/target/witness-ledgers/123.txt'; saved_ledger=ledger_path.read_text()
            ledger_path.write_text(saved_ledger+ledger[0]+'\n'); pack(shard,False); ledger_path.write_text(saved_ledger)
            report=root/'e2e/target/failsafe-reports/TEST-sample.PublishedExamplesIT.xml'; saved_report=report.read_text()
            suite=ET.parse(report).getroot(); suite.append(copy.deepcopy(suite.find('testcase'))); suite.set('tests',str(len(suite.findall('testcase'))))
            report.write_text(ET.tostring(suite,encoding='unicode')); pack(shard,False); report.write_text(saved_report)
        pack(shard)
    def verify(ok=True): call('verify','--plan',root/'plan.json','--artifacts',artifacts,'--restore',root/'restored',ok=ok)
    verify()
    assert Counter((root/'restored/e2e/target/witness-ledger.txt').read_text().splitlines())==Counter((root/'e2e/witness-manifest.txt').read_text().splitlines())
    merged=ET.parse(root/'restored/e2e/target/failsafe-reports/TEST-sample.PublishedExamplesIT.xml').getroot()
    assert len(merged.findall('testcase'))==24 and int(merged.get('tests'))==24
    assert Counter(c.find('system-out').text.strip().split('=',1)[1] for c in merged.findall('testcase'))==expected
    assert len(list((root/'restored/target/ci-shards').rglob('TEST-sample.PublishedExamplesIT.xml')))==len(sweep)
    backup=root/'backup'; shutil.copytree(artifacts,backup)
    def reset(): shutil.rmtree(artifacts); shutil.copytree(backup,artifacts)
    def rehash():
        for path in artifacts.glob('*/manifest.json'):
            m=json.loads(path.read_text()); files=path.parent/'files'
            m['files']={p.relative_to(files).as_posix():hashlib.sha256(p.read_bytes()).hexdigest() for p in files.rglob('*') if p.is_file()}
            suites=[ET.parse(p).getroot() for p in files.rglob('TEST-*.xml')]
            for field,attr in [('tests','tests'),('skipped','skipped')]: m['statistics'][field]=sum(int(s.get(attr,'0')) for s in suites)
            m['statistics']['zero_case_classes']=sum(int(s.get('tests','0'))==0 for s in suites)
            path.write_text(json.dumps(m))
        for path in artifacts.glob('*/ledger.json'):
            m=json.loads(path.read_text()); m['sha256']=hashlib.sha256((path.parent/'witness-ledger.txt').read_bytes()).hexdigest(); path.write_text(json.dumps(m))
    def published(): return next(artifacts.rglob('TEST-*Published*.xml'))
    # Missing shards, raw byte corruption, stale SHA and stale cohort are distinct failures.
    shutil.rmtree(artifacts/'shard-10'); verify(False); reset()
    published().write_text('corrupt'); verify(False); reset()
    for filename in ['manifest.json','ledger.json']:
        for field in ['source_sha','cohort_hash']:
            p=next(artifacts.rglob(filename)); m=json.loads(p.read_text()); m[field]='stale'; p.write_text(json.dumps(m)); verify(False); reset()
    # Every semantic mutation is rehashed so checksums cannot masquerade as case admission.
    for mutation in ['missing-tier','duplicate-case','ordinal','missing-marker','duplicate-marker','wrong-example']:
        p=published(); suite=ET.parse(p).getroot(); case=suite.find('testcase')
        if mutation=='missing-tier': suite.remove(case)
        if mutation=='duplicate-case': suite.append(copy.deepcopy(case))
        if mutation=='ordinal': case.find('system-out').text='tapstate.published-case=run[1]\n'
        if mutation=='missing-marker': case.remove(case.find('system-out'))
        if mutation=='duplicate-marker': case.find('system-out').text*=2
        if mutation=='wrong-example': case.find('system-out').text='tapstate.published-case=examples/unknown/case.e2e.yml on IN_PROCESS\n'
        suite.set('tests',str(len(suite.findall('testcase')))); p.write_text(ET.tostring(suite,encoding='unicode')); rehash(); verify(False); reset()
    # Simulate same-name XML copied over its sibling before admission.
    reports=list(artifacts.rglob('TEST-*Published*.xml')); shutil.copyfile(reports[0],reports[1]); rehash(); verify(False); reset()
    for mutation in ['missing','empty','duplicate','omitted','residual']:
        p=artifacts/sweep[0]/'witness-ledger.txt'; lines=p.read_text().splitlines()
        if mutation=='missing': p.unlink()
        else:
            value=[] if mutation=='empty' else lines+lines[:1] if mutation=='duplicate' else lines[1:] if mutation=='omitted' else lines+['stale on IN_PROCESS']
            p.write_text('\n'.join(value)+'\n'); rehash()
        verify(False); reset()
    p=next(artifacts.rglob('TEST-*DataBrowser*.xml')); suite=ET.parse(p).getroot(); suite.remove(suite.find('testcase')); suite.set('tests','0'); p.write_text(ET.tostring(suite,encoding='unicode')); rehash(); verify(False); reset()
    p=next(artifacts.rglob('TEST-*Watch*.xml')); suite=ET.parse(p).getroot(); ET.SubElement(suite.find('testcase'),'skipped'); suite.set('skipped','1'); p.write_text(ET.tostring(suite,encoding='unicode')); rehash(); verify(False); reset()
    put('e2e/examples/added/case.e2e.yml','fixture'); verify(False)
    call('plan','--output',root/'added.json'); assert len(json.loads((root/'added.json').read_text())['expected'])==19
    shutil.rmtree(root/'e2e/examples/added')
    put('e2e/src/test/java/sample/WatchAddedIT.java','package sample; class WatchAddedIT {}'); verify(False)
    (root/'e2e/src/test/java/sample/WatchAddedIT.java').unlink()
    put('e2e/target/witness-ledger.txt','stale'); put('e2e/target/witness-ledgers/old.txt','stale')
    put('bin/mvn','#!/bin/sh\nprintf "%s\\n" "$@" > "'+str(root/'mvn-args')+'"\nexit 1\n'); (root/'bin/mvn').chmod(0o755)
    os.environ['PATH']=str(root/'bin')+':'+os.environ['PATH']
    call('run','--plan',root/'plan.json','--shard',sweep[0],'--repo-local',root/'m2',ok=False)
    assert not (root/'e2e/target/witness-ledger.txt').exists() and not (root/'e2e/target/witness-ledgers').exists()
    args=(root/'mvn-args').read_text().splitlines(); selection=next(a.split('=',1)[1] for a in args if a.startswith('-Dtapstate.e2e.published-examples='))
    selected=next(s for s in plan['shards'] if s['id']==sweep[0])
    assert Counter(selection.split(','))==Counter(t['example'] for t in selected['tests'] if 'example' in t)
    assert '-DforkCount=1' in args and '-DreuseForks=true' in args and '-Djunit.jupiter.execution.parallel.enabled=false' in args
print('connector-witnesses smoke: example scheduling, XML preservation and 26 adverse admission/run scenario types passed')
PY
