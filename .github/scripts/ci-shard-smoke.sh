#!/usr/bin/env bash
# Source discovery and deterministic balancing, without Maven or network access.
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
export SHARD_GATE="$here/ci-shard.sh"
python3 - <<'PY'
import json, os, pathlib, shutil, subprocess, tempfile
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
    owners={t['class']: (s['id'], t['module']) for s in plan['shards'] for t in s['tests']}
    assert owners['sample.FixedSleepGateTest']==('rest', 'e2e')
    for name in ['ErrorCodeGatesTest', 'NestStateSerialFormGatesTest']:
        assert owners['sample.'+name]==('rest', 'arch-tests')
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
    seeded=json.loads((root/'inventory.json').read_text())
    assert Counter(ident(t) for s in p['shards'] for t in s['tests'])==Counter(ident(t) for t in seeded), 'generic shard assignments must cover the independent seed exactly once'
    # Recompute from assigned classes; the sharder cannot certify its own load totals.
    hints=json.loads((root/'durations.json').read_text())
    loads=[sum(hints[t['class']] for t in s['tests']) for s in p['shards']]
    assert loads==[s['estimated_seconds'] for s in p['shards']]
    assert max(loads)<=1.5*sum(loads)/len(loads)
    # New XML must never borrow old coverage or either witness ledger namespace.
    cleanup_modules=['.', 'e2e', 'runtime/engine', 'core', 'arch-tests']
    for module in cleanup_modules:
        for phase in ['surefire', 'failsafe']:
            put(module+'/target/'+phase+'-reports/TEST-stale.xml','old XML')
        for name in ['jacoco.exec', 'jacoco-it.exec']:
            put(module+'/target/'+name,'old coverage')
        put(module+'/target/classes/Keep.class','production bytecode')
        put(module+'/target/test-classes/Keep.class','test bytecode')
    put('e2e/target/witness-ledger.txt','old ordinary ledger')
    put('e2e/target/witness-ledgers/123.txt','old selected ledger')
    put('bin/mvn', '#!/bin/sh\nprintf "%s\\n" "$@" > "'+str(root/'mvn-args')+'"\n'
        +'mkdir -p "'+str(root/'core/target/surefire-reports')+'"\n'
        +'printf new > "'+str(root/'core/target/surefire-reports/TEST-new.xml')+'"\n'
        +'printf new >> "'+str(root/'core/target/jacoco.exec')+'"\n')
    (root/'bin/mvn').chmod(0o755)
    environment={**os.environ,'PATH':str(root/'bin')+':'+os.environ['PATH']}
    run_command=[os.environ['SHARD_GATE'],'run','--root',tmp,'--plan',str(root/'plan.json'),'--shard','rest','--repo-local',str(root/'m2')]
    executed=subprocess.run(run_command,check=True,env=environment,capture_output=True,text=True)
    args=(root/'mvn-args').read_text().splitlines()
    assert not (root/'e2e/target/witness-ledger.txt').exists(), 'old ordinary ledger survived'
    assert not (root/'e2e/target/witness-ledgers').exists(), 'old selected ledger survived'
    assert (root/'core/target/jacoco.exec').read_text()=='new', 'coverage must be from this verify, without append residue'
    assert (root/'core/target/surefire-reports/TEST-new.xml').read_text()=='new'
    for module in cleanup_modules:
        assert not list((root/module/'target').glob('*-reports/TEST-stale.xml'))
        assert not (root/module/'target/jacoco-it.exec').exists()
        assert (root/module/'target/classes/Keep.class').read_text()=='production bytecode'
        assert (root/module/'target/test-classes/Keep.class').read_text()=='test bytecode'
    timing=[json.loads(line.split(' ',1)[1]) for line in executed.stdout.splitlines() if line.startswith('CI_PHASE_JSON ')]
    assert [(r['phase'],r['event']) for r in timing]==[(phase,event) for phase in ['reset-test-evidence','selected-verify'] for event in ['start','end']]
    for start,end in zip(timing[::2],timing[1::2]):
        assert end['status']=='success' and end['monotonic_ns']>=start['monotonic_ns']
        assert end['elapsed_ns']==end['monotonic_ns']-start['monotonic_ns']
        assert start['source_sha']==end['source_sha']==added['source_sha'] and end['shard_id']=='rest'
    # Validate every removal first: even a later unsafe path must leave all evidence untouched.
    outside_context=tempfile.TemporaryDirectory()
    outside=pathlib.Path(outside_context.name)
    external={'sentinel':'foreign', 'surefire-reports/TEST-foreign.xml':'foreign XML',
              'jacoco.exec':'foreign coverage', 'jacoco-it.exec':'foreign IT coverage'}
    for name,value in external.items():
        path=outside/name; path.parent.mkdir(parents=True,exist_ok=True); path.write_text(value)
    for relative in ['core/target', 'core/target/surefire-reports', 'core/target/jacoco.exec', 'e2e/target/witness-ledgers']:
        path=root/relative
        if path.is_dir(): shutil.rmtree(path)
        elif path.exists(): path.unlink()
        path.parent.mkdir(parents=True,exist_ok=True); path.symlink_to(outside,target_is_directory=True)
        put('e2e/target/failsafe-reports/TEST-retain.xml','must survive failed preflight')
        (root/'mvn-args').unlink(missing_ok=True)
        rejected=subprocess.run(run_command,env=environment,capture_output=True,text=True)
        assert rejected.returncode!=0 and 'unsafe cleanup path' in rejected.stderr, (relative,rejected.stdout,rejected.stderr)
        assert all((outside/name).read_text()==value for name,value in external.items()) and not (root/'mvn-args').exists()
        assert (root/'e2e/target/failsafe-reports/TEST-retain.xml').read_text()=='must survive failed preflight'
        path.unlink()
    outside_context.cleanup()
    # A failed verify gets an explicit failed timing end and cannot retain a success receipt.
    put('bin/mvn','#!/bin/sh\nexit 7\n'); (root/'bin/mvn').chmod(0o755)
    failed=subprocess.run(run_command,env=environment,capture_output=True,text=True)
    assert failed.returncode!=0
    failed_timing=[json.loads(line.split(' ',1)[1]) for line in failed.stdout.splitlines() if line.startswith('CI_PHASE_JSON ')]
    assert failed_timing[-1]['phase']=='selected-verify' and failed_timing[-1]['status']=='failure'
    receipt=json.loads((root/'target/ci-shards/run-rest.json').read_text())
    assert receipt['status']=='failure'

    assert '-am' not in args and 'verify' in args and '-pl' in args
    assert any(a.startswith('-Dtest=') and all(name in a for name in ['FixedSleepGateTest','ErrorCodeGatesTest','NestStateSerialFormGatesTest']) for a in args)
    assert set(args[args.index('-pl')+1].split(','))=={'arch-tests', 'core', 'e2e'}, 'source-scanning gates must run in their owning modules'
    # A custom Maven selector must not disappear from both sides of admission.
    original=(root/'core/pom.xml').read_text()
    put('core/src/test/java/sample/CustomSpec.java','package sample; class CustomSpec {}')
    for plugin in ['maven-surefire-plugin', 'maven-failsafe-plugin']:
        for selector in ['includes', 'excludes', 'includesFile', 'excludesFile']:
            config=f'<configuration><{selector}>custom-selection</{selector}></configuration>'
            for placement in ['plugin', 'execution', 'managed', 'profile']:
                body=f'<executions><execution>{config}</execution></executions>' if placement=='execution' else config
                plugins=f'<plugins><plugin><artifactId>{plugin}</artifactId>{body}</plugin></plugins>'
                build='<build>'+('<pluginManagement>'+plugins+'</pluginManagement>' if placement=='managed' else plugins)+'</build>'
                pom='<project>'+('<profiles><profile>'+build+'</profile></profiles>' if placement=='profile' else build)+'</project>'
                put('core/pom.xml',pom)
                result=subprocess.run(cmd,capture_output=True,text=True)
                assert result.returncode!=0 and 'unsupported test selection' in result.stderr, (plugin,selector,placement,result.stdout,result.stderr)
    put('core/pom.xml',original)
    # The same guard must run on consumption, after a plan was already produced.
    put('core/pom.xml',original.replace('</plugins>','<plugin><artifactId>maven-surefire-plugin</artifactId><configuration><includes/></configuration></plugin></plugins>'))
    result=subprocess.run([os.environ['SHARD_GATE'],'run','--root',tmp,'--plan',str(root/'plan.json'),'--shard','rest','--repo-local',str(root/'m2')],capture_output=True,text=True)
    assert result.returncode!=0 and 'unsupported test selection' in result.stderr, result.stdout+result.stderr
    put('core/pom.xml',original)
    # Non-test plugins may legitimately have selection fields with the same names.
    put('core/pom.xml',original.replace('</plugins>','<plugin><artifactId>maven-enforcer-plugin</artifactId><configuration><includes/></configuration></plugin></plugins>'))
    subprocess.run(cmd,check=True)
print('ci-shard smoke: source discovery, exact assignment, balance and source-gate placement passed')
PY
