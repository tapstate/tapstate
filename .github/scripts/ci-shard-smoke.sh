#!/usr/bin/env bash
# Source discovery and deterministic balancing, without Maven or network access.
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
export SHARD_GATE="$here/ci-shard.sh"
python3 - <<'PY'
import json, os, pathlib, subprocess, tempfile
from collections import Counter
with tempfile.TemporaryDirectory() as tmp:
    root = pathlib.Path(tmp)
    def put(path, text):
        p=root/path; p.parent.mkdir(parents=True, exist_ok=True); p.write_text(text)
    put('pom.xml', '<project><modules><module>e2e</module><module>runtime/engine</module><module>core</module><module>arch-tests</module></modules><profiles><profile><modules><module>tools/catalog-derive</module></modules></profile></profiles></project>')
    for module in ['e2e','runtime/engine','core','arch-tests','tools/catalog-derive']:
        put(module+'/pom.xml', '<project><build><plugins><plugin><artifactId>maven-failsafe-plugin</artifactId><executions><execution><goals><goal>integration-test</goal></goals></execution></executions></plugin></plugins></build></project>')
    for module, name, body in [('e2e','LongIT','class LongIT {}'),('e2e','SmallIT','class SmallIT {}'),('e2e','FixedSleepGateTest','class FixedSleepGateTest {}'),('runtime/engine','EngineTest','class EngineTest {}'),('core','NewTest','class NewTest { /* abstract class NewTest */ @Nested class Nested {} }'),('arch-tests','ErrorCodeGatesTest','class ErrorCodeGatesTest {}'),('arch-tests','NestStateSerialFormGatesTest','class NestStateSerialFormGatesTest {}'),('core','AbstractTest','abstract class AbstractTest {}'),('core','Helper','class Helper {}'),('core','TestUtility','class TestUtility { private TestUtility() {} }'),('tools/catalog-derive','HiddenTest','class HiddenTest {}')]:
        put(module+'/src/test/java/sample/'+name+'.java','package sample; '+body)
    put('durations.json', json.dumps({'sample.LongIT':100, 'sample.SmallIT':20}))
    cmd=[os.environ['SHARD_GATE'],'plan','--root',tmp,'--durations',str(root/'durations.json'),'--output',str(root/'plan.json')]
    subprocess.run(cmd,check=True)
    plan=json.loads((root/'plan.json').read_text())
    assert [s['id'] for s in plan['shards']]==['e2e-1','e2e-2','engine','rest']
    assert len(plan['expected'])==7, plan['expected']
    ident=lambda t:(t['module'],t['kind'],t['class'])
    assert Counter(ident(t) for s in plan['shards'] for t in s['tests'])==Counter(ident(t) for t in plan['expected'])
    assert next(s for s in plan['shards'] if s['id']=='rest')['required_exec']==['arch-tests/target/jacoco.exec','core/target/jacoco.exec','e2e/target/jacoco.exec']
    assert all(not s['required_exec'] for s in plan['shards'][:2])
    assert plan['shards'][0]['tests'][0]['class']=='sample.LongIT'
    first=(root/'plan.json').read_bytes(); subprocess.run(cmd,check=True)
    assert first==(root/'plan.json').read_bytes()
    put('core/src/test/java/sample/AddedTest.java','package sample; class AddedTest {}')
    subprocess.run(cmd,check=True)
    added=json.loads((root/'plan.json').read_text()); assert len(added['expected'])==8 and added['cohort_hash']!=plan['cohort_hash']
    put('inventory.json',json.dumps([{'module':'e2e','kind':'it','class':'sample.'+name} for name in ['OneIT','TwoIT','ThreeIT','FourIT','FiveIT','SixIT']]))
    put('durations.json',json.dumps(dict(zip(['sample.'+name for name in ['OneIT','TwoIT','ThreeIT','FourIT','FiveIT','SixIT']],[100,80,60,40,20,10]))))
    generic=cmd[:-2]+['--output',str(root/'generic.json'),'--inventory',str(root/'inventory.json'),'--count','3']
    subprocess.run(generic,check=True)
    p=json.loads((root/'generic.json').read_text()); assert len(p['shards'])==3 and len(p['expected'])==6
    loads=[s['estimated_seconds'] for s in p['shards']]; assert max(loads)<=1.5*sum(loads)/len(loads)
    put('bin/mvn', '#!/bin/sh\nprintf "%s\\n" "$@" > "'+str(root/'mvn-args')+'"\n')
    (root/'bin/mvn').chmod(0o755)
    subprocess.run([os.environ['SHARD_GATE'],'run','--root',tmp,'--plan',str(root/'plan.json'),'--shard','rest','--repo-local',str(root/'m2')],check=True,env={**os.environ,'PATH':str(root/'bin')+':'+os.environ['PATH']})
    args=(root/'mvn-args').read_text().splitlines(); assert '-am' not in args and 'verify' in args and '-pl' in args
    assert any(a.startswith('-Dtest=') and all(name in a for name in ['FixedSleepGateTest','ErrorCodeGatesTest','NestStateSerialFormGatesTest']) for a in args)
    assert 'arch-tests' in args[args.index('-pl')+1]
print('ci-shard smoke: 14 assertions passed')
PY
