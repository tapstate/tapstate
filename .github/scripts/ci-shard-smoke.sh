#!/usr/bin/env bash
# Source discovery and deterministic balancing, without Maven or network access.
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
export SHARD_GATE="$here/ci-shard.sh"
python3 - <<'PY'
import hashlib, json, os, pathlib, shutil, subprocess, sys, tempfile, zipfile
sys.dont_write_bytecode = True
from collections import Counter
with tempfile.TemporaryDirectory() as tmp:
    root = pathlib.Path(tmp)
    def put(path, text):
        p=root/path; p.parent.mkdir(parents=True, exist_ok=True); p.write_text(text)
    put('pom.xml', '<project><modules><module>e2e</module><module>runtime/engine</module><module>core</module><module>arch-tests</module></modules><profiles><profile><modules><module>tools/catalog-derive</module></modules></profile></profiles></project>')
    for module in ['e2e','runtime/engine','core','arch-tests','tools/catalog-derive']:
        put(module+'/pom.xml', '<project><build><plugins><plugin><artifactId>maven-failsafe-plugin</artifactId><executions><execution><goals><goal>integration-test</goal></goals></execution></executions></plugin></plugins></build></project>')
    for module, name, body in [('e2e','LongIT','class LongIT {}'),('e2e','SmallIT','class SmallIT {}'),('e2e','CloudFixtureIT','@RequiresCloudReleaseInputs class CloudFixtureIT {}'),('e2e','FixedSleepGateTest','class FixedSleepGateTest {}'),('runtime/engine','EngineTest','class EngineTest {}'),('core','NewTest','class NewTest { /* abstract class NewTest */ @Nested class Nested {} }'),('arch-tests','ErrorCodeGatesTest','class ErrorCodeGatesTest {}'),('arch-tests','NestStateSerialFormGatesTest','class NestStateSerialFormGatesTest {}'),('core','AbstractTest','abstract class AbstractTest {}'),('core','Helper','class Helper {}'),('core','TestUtility','class TestUtility { private TestUtility() {} }'),('tools/catalog-derive','HiddenTest','class HiddenTest {}')]:
        put(module+'/src/test/java/sample/'+name+'.java','package sample; '+body)
    put('durations.json', json.dumps({'sample.LongIT':100, 'sample.SmallIT':20}))
    cmd=[os.environ['SHARD_GATE'],'plan','--root',tmp,'--durations',str(root/'durations.json'),'--output',str(root/'plan.json')]
    subprocess.run(cmd,check=True)
    plan=json.loads((root/'plan.json').read_text())
    assert [s['id'] for s in plan['shards']]==['e2e-1','e2e-2','engine','rest']
    assert len(plan['expected'])==8, plan['expected']
    ident=lambda t:(t['module'],t['kind'],t['class'])
    assert Counter(ident(t) for s in plan['shards'] for t in s['tests'])==Counter(ident(t) for t in plan['expected'])
    owners={t['class']: (s['id'], t['module']) for s in plan['shards'] for t in s['tests']}
    assert owners['sample.FixedSleepGateTest']==('rest', 'e2e')
    assert owners['sample.CloudFixtureIT']==('rest', 'e2e'), 'Cloud startup witnesses need their locked release inputs'
    for name in ['ErrorCodeGatesTest', 'NestStateSerialFormGatesTest']:
        assert owners['sample.'+name]==('rest', 'arch-tests')
    assert next(s for s in plan['shards'] if s['id']=='rest')['required_exec']==['arch-tests/target/jacoco.exec','core/target/jacoco.exec','e2e/target/jacoco.exec']
    assert all(not s['required_exec'] for s in plan['shards'][:2])
    assert plan['shards'][0]['tests'][0]['class']=='sample.LongIT'
    first=(root/'plan.json').read_bytes(); subprocess.run(cmd,check=True)
    assert first==(root/'plan.json').read_bytes()
    put('core/src/test/java/sample/AddedTest.java','package sample; class AddedTest {}')
    subprocess.run(cmd,check=True)
    added=json.loads((root/'plan.json').read_text()); assert len(added['expected'])==9 and added['cohort_hash']!=plan['cohort_hash']
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
        for name in ['jacoco.exec', 'jacoco-it.exec', 'site/jacoco/jacoco.xml', 'site/jacoco-it/jacoco.xml']:
            put(module+'/target/'+name,'old coverage')
        put(module+'/target/classes/Keep.class','production bytecode')
        put(module+'/target/test-classes/Keep.class','test bytecode')
    put('target/jacoco-reactor.exec','old merged coverage')
    put('e2e/target/witness-ledger.txt','old ordinary ledger')
    put('e2e/target/witness-ledgers/123.txt','old selected ledger')
    put('bin/mvn', '#!/bin/sh\nprintf "%s\\n" "$@" > "'+str(root/'mvn-args')+'"\n'
        +'printf "%s" "${JAVA_TOOL_OPTIONS:-}" > "'+str(root/'mvn-env')+'"\n'
        +'mkdir -p "'+str(root/'core/target/surefire-reports')+'"\n'
        +'printf new > "'+str(root/'core/target/surefire-reports/TEST-new.xml')+'"\n'
        +'printf new >> "'+str(root/'core/target/jacoco.exec')+'"\n')
    (root/'bin/mvn').chmod(0o755)
    runner_temp=root/'runner-temp'
    environment={**os.environ,'PATH':str(root/'bin')+':'+os.environ['PATH'],'RUNNER_TEMP':str(runner_temp),'JAVA_TOOL_OPTIONS':'-Dfile.encoding=UTF-8'}
    run_command=[os.environ['SHARD_GATE'],'run','--root',tmp,'--plan',str(root/'plan.json'),'--shard','rest','--repo-local',str(root/'m2')]
    executed=subprocess.run(run_command,check=True,env=environment,capture_output=True,text=True)
    args=(root/'mvn-args').read_text().splitlines()
    assert '-am' not in args and 'verify' in args and '-pl' in args
    assert '-Dapi.version=1.44' in args, 'Docker 29 requires a supported Testcontainers API version'
    assert '-Djava.io.tmpdir='+str(runner_temp.resolve()/'tapstate-java-tmp') in args
    assert '-Dtapstate.it.jvmArgs=-Djava.io.tmpdir='+str(runner_temp.resolve()/'tapstate-java-tmp') in args, 'Failsafe forks must keep JUnit temp files on the runner temp filesystem'
    assert (root/'mvn-env').read_text()=='-Dfile.encoding=UTF-8', 'do not inject JVM options into child Java processes'
    assert (runner_temp/'tapstate-java-tmp').is_dir(), 'runner-backed Java temporary directory must exist before Maven starts'
    assert not (root/'target/jacoco-reactor.exec').exists(), 'old merged coverage survived'
    assert not (root/'e2e/target/witness-ledger.txt').exists(), 'old ordinary ledger survived'
    assert not (root/'e2e/target/witness-ledgers').exists(), 'old selected ledger survived'
    assert (root/'core/target/jacoco.exec').read_text()=='new', 'coverage must be from this verify, without append residue'
    assert (root/'core/target/surefire-reports/TEST-new.xml').read_text()=='new'
    for module in cleanup_modules:
        assert not list((root/module/'target').glob('*-reports/TEST-stale.xml'))
        assert not (root/module/'target/jacoco-it.exec').exists()
        assert not (root/module/'target/site/jacoco/jacoco.xml').exists()
        assert not (root/module/'target/site/jacoco-it/jacoco.xml').exists()
        assert (root/module/'target/classes/Keep.class').read_text()=='production bytecode'
        assert (root/module/'target/test-classes/Keep.class').read_text()=='test bytecode'
    timing=[json.loads(line.split(' ',1)[1]) for line in executed.stdout.splitlines() if line.startswith('CI_PHASE_JSON ')]
    assert [(r['phase'],r['event']) for r in timing]==[(phase,event) for phase in ['reset-test-evidence','selected-verify'] for event in ['start','end']]
    for start,end in zip(timing[::2],timing[1::2]):
        assert end['status']=='success' and end['monotonic_ns']>=start['monotonic_ns']
        assert end['elapsed_ns']==end['monotonic_ns']-start['monotonic_ns']
        assert start['source_sha']==end['source_sha']==added['source_sha'] and end['shard_id']=='rest'
    # Observer activation audits the real declared classpath, not every available cached JAR.
    assert not any('additionalClasspath' in a or 'autodetection' in a or 'case-evidence' in a for a in args)
    ordinary=root/'m2/example/ordinary/1/ordinary-1.jar'; ordinary.parent.mkdir(parents=True)
    service_name='META-INF/services/org.junit.jupiter.api.extension.Extension'
    observer_prefix='test-support/target/test-classes/'
    def jar(path,service=None,shadow=False):
        with zipfile.ZipFile(path,'w') as archive:
            archive.writestr('sample/Fixture.class','fixture bytecode')
            if service is not None: archive.writestr(service_name,service)
            if shadow: archive.writestr('io/tapstate/testsupport/CiCaseEvidence.class','foreign observer bytecode')
    jar(ordinary)
    unrelated=root/'m2/example/unrelated/1/unrelated-1.jar'; unrelated.parent.mkdir(parents=True)
    jar(unrelated,'foreign.CachedExtension\n')
    classpaths={module:[ordinary] for module in ['arch-tests','core','e2e']}
    def build_inputs():
        value=dict(schema=1,phase='pre-test-after-install',source_sha=added['source_sha'],
                   run_id=os.environ.get('GITHUB_RUN_ID','local'),run_attempt=os.environ.get('GITHUB_RUN_ATTEMPT','1'),
                   source_files={path.relative_to(root).as_posix():hashlib.sha256(path.read_bytes()).hexdigest()
                                 for path in root.rglob('*') if path.is_file() and (path.name=='pom.xml' or '/src/' in path.as_posix())},
                   test_resource_files={path.relative_to(root).as_posix():hashlib.sha256(path.read_bytes()).hexdigest()
                                 for path in root.rglob('*') if path.is_file() and '/target/test-classes/' in path.as_posix() and path.suffix!='.class'},
                   test_files={path.relative_to(root).as_posix():hashlib.sha256(path.read_bytes()).hexdigest()
                               for path in root.rglob('*.class') if '/target/test-classes/' in path.as_posix()},
                   resolved_test_classpaths={module:[dict(identity='repository/'+path.relative_to(root/'m2').as_posix(),
                       sha256=hashlib.sha256(path.read_bytes()).hexdigest(),size=path.stat().st_size) for path in paths]
                       for module,paths in classpaths.items()})
        put('build-inputs.json',json.dumps(value)); return value
    def reject_observer(command,message):
        put('e2e/target/failsafe-reports/TEST-observer-sentinel.xml','preserve before audit')
        put('e2e/target/witness-ledger.txt','preserve ordinary ledger')
        (root/'mvn-args').unlink(missing_ok=True)
        result=subprocess.run(command,env=environment,capture_output=True,text=True)
        assert result.returncode!=0 and message in result.stderr,(message,result.stdout,result.stderr)
        assert not (root/'mvn-args').exists(), 'observer audit must finish before Maven'
        assert (root/'e2e/target/failsafe-reports/TEST-observer-sentinel.xml').read_text()=='preserve before audit'
        assert (root/'e2e/target/witness-ledger.txt').read_text()=='preserve ordinary ledger'
        assert not any(json.loads(line.split(' ',1)[1])['phase']=='reset-test-evidence'
                       for line in result.stdout.splitlines() if line.startswith('CI_PHASE_JSON ')), 'observer audit must finish before reset'
    inputs=build_inputs()
    input_args=['--build-inputs',str(root/'build-inputs.json')]
    opted=run_command+['--case-evidence']+input_args
    reject_observer(run_command+['--case-evidence'],'requires --build-inputs')
    reject_observer(run_command+input_args,'requires --case-evidence')
    reject_observer(opted,'must be built')
    put(observer_prefix+'io/tapstate/testsupport/CiCaseEvidence.class','observer bytecode')
    put(observer_prefix+'io/tapstate/testsupport/CiCaseEvidence$Canonical.class','canonical bytecode')
    inputs=build_inputs(); reject_observer(opted,'must be built')
    put(observer_prefix+service_name,'other.Extension\n')
    inputs=build_inputs(); reject_observer(opted,'registration differs')
    put(observer_prefix+service_name,'io.tapstate.testsupport.CiCaseEvidence\n')
    inputs=build_inputs()
    subprocess.run(opted,check=True,env=environment,capture_output=True)
    observed=(root/'mvn-args').read_text().splitlines()
    extra=['-Dmaven.test.additionalClasspath='+str((root/observer_prefix).resolve()),
           '-Djunit.jupiter.extensions.autodetection.enabled=true','-Dtapstate.ci.case-evidence=true']
    assert all(observed.count(flag)==1 for flag in extra)
    assert [a for a in observed if a not in extra]==args, 'observer must retain the original selectors and verify lifecycle'
    receipt=json.loads((root/'target/ci-shards/run-rest.json').read_text())['case_observer']
    assert receipt['enabled'] is True and receipt['providers']==['io.tapstate.testsupport.CiCaseEvidence']
    assert receipt['build_inputs_sha256']==hashlib.sha256((root/'build-inputs.json').read_bytes()).hexdigest()
    assert {(s['module'],s['kind']) for s in receipt['checked_scopes']}=={('arch-tests','unit'),('core','unit'),('e2e','unit'),('e2e','it')}, 'observer must audit the residual Cloud integration scope too'
    # Class-abort metadata has one fresh task-owned directory, never a reusable sink.
    gate_dir=root/'class-gates'
    gated=opted+['--class-gate-dir',str(gate_dir)]
    reject_observer(run_command+['--class-gate-dir',str(gate_dir)], 'requires --case-evidence')
    reject_observer(opted+['--class-gate-dir','class-gates'], 'must be absolute')
    reject_observer(opted+['--class-gate-dir',str(root/'foreign-gates')], 'declared build input sibling')
    gate_dir.mkdir(); (gate_dir/'retained.jsonl').write_text('foreign retained metadata')
    reject_observer(gated,'must be fresh')
    assert (gate_dir/'retained.jsonl').read_text()=='foreign retained metadata'
    (gate_dir/'retained.jsonl').unlink(); gate_dir.rmdir()
    with tempfile.TemporaryDirectory() as gate_outside:
        sentinel=pathlib.Path(gate_outside)/'retained'; sentinel.write_text('outside metadata')
        gate_dir.symlink_to(gate_outside,target_is_directory=True)
        reject_observer(gated,'unsafe observer ledger path')
        assert sentinel.read_text()=='outside metadata'
        gate_dir.unlink()
    subprocess.run(gated,check=True,env=environment,capture_output=True)
    gated_args=(root/'mvn-args').read_text().splitlines()
    gate_flag='-Dtapstate.ci.class-gate-dir='+str(gate_dir.resolve())
    assert gated_args.count(gate_flag)==1 and gate_dir.is_dir()
    assert [a for a in gated_args if a not in extra+[gate_flag]]==args
    gated_receipt=json.loads((root/'target/ci-shards/run-rest.json').read_text())['case_observer']
    assert gated_receipt['class_gate_ledger_enabled'] is True
    reject_observer(gated,'must be fresh')
    gate_dir.rmdir()
    # Maven resolves the language aggregator as a nonarchive POM alongside its real JARs.
    aggregator=root/'m2/org/graalvm/polyglot/js-community/24.1.2/js-community-24.1.2.pom'
    aggregator.parent.mkdir(parents=True)
    language_pom='<project><modelVersion>4.0.0</modelVersion><groupId>org.graalvm.polyglot</groupId><artifactId>js-community</artifactId><version>24.1.2</version><packaging>pom</packaging></project>'
    aggregator.write_text(language_pom)
    classpaths['core']=[ordinary,aggregator]; build_inputs()
    pom_result=subprocess.run(opted,env=environment,capture_output=True,text=True)
    assert pom_result.returncode==0, pom_result.stdout+pom_result.stderr
    for before,after in [('<packaging>pom</packaging>','<packaging>jar</packaging>'),
                         ('<artifactId>js-community</artifactId>','<artifactId>other</artifactId>'),
                         ('<version>24.1.2</version>','<version>24.1.3</version>')]:
        aggregator.write_text(language_pom.replace(before,after)); build_inputs()
        reject_observer(opted,'nonarchive classpath POM')
    aggregator.write_text('not XML'); build_inputs()
    reject_observer(opted,'nonarchive classpath POM')
    classpaths['core']=[ordinary]
    # Renaming POM XML to a declared JAR must not become an escape from archive admission.
    ordinary.write_text(language_pom); build_inputs()
    reject_observer(opted,'File is not a zip file')
    jar(ordinary); build_inputs()
    for field,value in [('source_sha','wrong-source'),('run_id','another-run'),('run_attempt','999')]:
        changed=build_inputs(); changed[field]=value; put('build-inputs.json',json.dumps(changed))
        reject_observer(opted,'source/run/attempt differs')
    changed=build_inputs(); changed['resolved_test_classpaths'].pop('core'); put('build-inputs.json',json.dumps(changed))
    reject_observer(opted,'every selected module')
    changed=build_inputs(); changed['resolved_test_classpaths']['core'][0]['sha256']='0'*64; put('build-inputs.json',json.dumps(changed))
    reject_observer(opted,'bytes changed')
    for directory in ['classes','test-classes']:
        path='core/target/'+directory+'/'+service_name
        put(path,'foreign.ProjectExtension\n'); build_inputs()
        reject_observer(opted,'unexpected extension provider')
        (root/path).unlink()
    jar(ordinary,'foreign.ActiveExtension\n'); build_inputs()
    reject_observer(opted,'unexpected extension provider')
    jar(ordinary,shadow=True); build_inputs()
    reject_observer(opted,'shadowed in active dependency')
    jar(ordinary)
    # These two module-owned callbacks observe fixtures; they are not third-party extensions.
    fixture_providers=['io.tapstate.e2e.ClusterFixtureTimingExtension','io.tapstate.e2e.ClusterHealthProbeExtension']
    fixture_service='e2e/src/test/resources/'+service_name
    copied_service='e2e/target/test-classes/'+service_name
    fixture_bytes='\n'.join(fixture_providers)+'\n'
    put(fixture_service,fixture_bytes);put(copied_service,fixture_bytes)
    fixture_sources=[];fixture_classes=[]
    for provider in fixture_providers:
        relative=provider.replace('.','/')
        source='e2e/src/test/java/'+relative+'.java';fixture_sources.append(source)
        put(source,'package io.tapstate.e2e; class '+provider.rsplit('.',1)[1]+' {}')
        for suffix in ['', '$Capture']:
            compiled='e2e/target/test-classes/'+relative+suffix+'.class';fixture_classes.append(compiled)
            put(compiled,'owned fixture callback '+provider+suffix)
    build_inputs()
    fixture_result=subprocess.run(opted,env=environment,capture_output=True,text=True)
    assert fixture_result.returncode==0, 'owned E2E fixture extensions must pass source-bound observer admission: '+fixture_result.stdout+fixture_result.stderr
    fixture_receipt=json.loads((root/'target/ci-shards/run-rest.json').read_text())['case_observer']['fixture_extensions']['e2e']
    assert fixture_receipt['providers']==sorted(fixture_providers) and len(fixture_receipt['files'])==8
    extension_checks=1
    def stale_fixture(path,reason):
        global extension_checks
        build_inputs(); original=(root/path).read_bytes(); (root/path).write_bytes(original+b'changed')
        try:
            reject_observer(opted,reason);extension_checks+=1
        finally:
            (root/path).write_bytes(original);build_inputs()
    for path in fixture_sources:stale_fixture(path,'captured source_files')
    for path in fixture_classes:stale_fixture(path,'captured test_files')
    stale_fixture(copied_service,'prepared service differs')
    for path in [name for name in fixture_classes if '$' in name]:
        build_inputs();original=(root/path).read_bytes();(root/path).unlink()
        try:
            reject_observer(opted,'fixture extension class family differs from captured');extension_checks+=1
        finally:(root/path).write_bytes(original);build_inputs()
    build_inputs();source_bytes=(root/fixture_service).read_bytes();copied_bytes=(root/copied_service).read_bytes()
    (root/fixture_service).unlink();(root/copied_service).unlink()
    try:
        reject_observer(opted,'captured fixture extension registration is missing');extension_checks+=1
    finally:
        (root/fixture_service).write_bytes(source_bytes);(root/copied_service).write_bytes(copied_bytes);build_inputs()
    original_source_service=(root/fixture_service).read_bytes()
    put(fixture_service,fixture_bytes+'foreign.ProjectExtension\n');put(copied_service,fixture_bytes+'foreign.ProjectExtension\n');build_inputs()
    reject_observer(opted,'fixture extension source service differs');extension_checks+=1
    put(fixture_service,original_source_service.decode());put(copied_service,fixture_bytes);build_inputs()
    # Equal copied bytes still need their recorded prepared-resource binding.
    changed=build_inputs();del changed['test_resource_files'][copied_service];put('build-inputs.json',json.dumps(changed))
    reject_observer(opted,'prepared service differs from captured source');extension_checks+=1;build_inputs()
    copied_production='e2e/target/classes/'+fixture_providers[0].replace('.','/')+'.class'
    put(copied_production,'shadow callback');build_inputs()
    reject_observer(opted,'shadowed in production output');extension_checks+=1;(root/copied_production).unlink();build_inputs()
    jar(ordinary,fixture_bytes);build_inputs()
    reject_observer(opted,'unexpected extension provider');extension_checks+=1
    # The shared JAR is inspected in arch-tests before e2e; cached membership must still reject shadows.
    for suffix in ['', '$Capture']:
        jar(ordinary)
        with zipfile.ZipFile(ordinary,'a') as archive:
            archive.writestr(fixture_providers[0].replace('.','/')+suffix+'.class','shadow callback')
        build_inputs();reject_observer(opted,'fixture extension class shadowed in active dependency');extension_checks+=1
    jar(ordinary);build_inputs()
    subprocess.run(opted,check=True,env=environment,capture_output=True)
    # Real pack preflight must retain every source/service/primary/inner-class binding after verify.
    import importlib.util
    from types import SimpleNamespace
    spec=importlib.util.spec_from_file_location('extension_pack_controls',str(pathlib.Path(os.environ['SHARD_GATE']).with_name('_ci-shards.py')))
    pack_reader=importlib.util.module_from_spec(spec);spec.loader.exec_module(pack_reader)
    from unittest.mock import patch
    # A phase may fail before its context manager yields any timing record.
    # Preserve that error and leave a failed receipt rather than masking it.
    from contextlib import contextmanager
    actual_timing = pack_reader.phase_timing
    @contextmanager
    def invalid_verify_identity(phase, *args, **kwargs):
        if phase == 'selected-verify':
            with patch.dict(os.environ, {'GITHUB_RUN_ID': '123', 'GITHUB_RUN_ATTEMPT': 'invalid'}):
                with actual_timing(phase, *args, **kwargs) as observation:
                    yield observation
        else:
            with actual_timing(phase, *args, **kwargs) as observation:
                yield observation
    with patch.object(pack_reader, 'phase_timing', invalid_verify_identity):
        try:
            pack_reader.run_shard(SimpleNamespace(shard='rest', repo_local=str(root/'m2'),
                case_evidence=False, build_inputs=None, runtime_class_dir=None, class_gate_dir=None),
                root.resolve(), json.loads((root/'plan.json').read_text()))
        except ValueError as error:
            assert str(error) == 'invalid Actions execution identity', str(error)
        else:
            raise AssertionError('invalid verify identity was accepted')
    failed_origin = json.loads((root/'target/ci-shards/run-rest.json').read_text())
    assert failed_origin['status'] == 'failure' and 'verify' not in failed_origin
    subprocess.run(opted, check=True, env=environment, capture_output=True)
    hash_target=(root/fixture_sources[0]).resolve();original_hash_source=hash_target.read_bytes()
    real_digest=pack_reader.file_digest;first_hash=True
    def mutate_after_validated_hash(path):
        global first_hash
        result=real_digest(path)
        if path.resolve()==hash_target and first_hash:
            first_hash=False;hash_target.write_bytes(original_hash_source+b' changed after validated hash')
        return result
    try:
        with patch.object(pack_reader,'file_digest',mutate_after_validated_hash):
            try:fact=pack_reader.fixture_extensions(root.resolve(),'e2e',json.loads((root/'build-inputs.json').read_bytes()))
            except ValueError:pass
            else:assert fact['files'][fixture_sources[0]]==hashlib.sha256(original_hash_source).hexdigest(), 'provenance must store the hash validated against captured inputs'
        extension_checks+=1
    finally:hash_target.write_bytes(original_hash_source)
    for path in fixture_receipt['files']:
        original=(root/path).read_bytes();(root/path).write_bytes(original+b'post-verify mutation')
        try:
            pack_reader.pack(SimpleNamespace(shard='rest',output=str(root/'never-packed')),root.resolve(),json.loads((root/'plan.json').read_bytes()))
        except ValueError as error:
            assert 'changed after selected verify' in str(error),str(error)
            assert not (root/'never-packed').exists();extension_checks+=1
        else:raise AssertionError('pack accepted changed fixture extension input '+path)
        finally:(root/path).write_bytes(original)
    print('CI module-local fixture extension controls passed:',extension_checks)
    # The same foreign MCP provider is inactive for an explicitly excluded IT and active for unit tests.
    mcp=root/'m2/io/tapstate/mcp-server/1/mcp-server-1.jar'; mcp.parent.mkdir(parents=True)
    jar(mcp,'foreign.McpExtension\n')
    saved_pom=(root/'e2e/pom.xml').read_text()
    exclusion='<configuration><classpathDependencyExcludes><classpathDependencyExclude>io.tapstate:mcp-server</classpathDependencyExclude></classpathDependencyExcludes></configuration>'
    put('e2e/pom.xml',saved_pom.replace('<artifactId>maven-failsafe-plugin</artifactId>','<artifactId>maven-failsafe-plugin</artifactId>'+exclusion))
    classpaths['e2e']=[ordinary,mcp]; build_inputs()
    it_command=opted.copy(); it_command[it_command.index('--shard')+1]='e2e-1'
    subprocess.run(it_command,check=True,env=environment,capture_output=True)
    it_receipt=json.loads((root/'target/ci-shards/run-e2e-1.json').read_text())['case_observer']
    assert it_receipt['checked_scopes']==[dict(module='e2e',kind='it',dependency_excludes=['io.tapstate:mcp-server'])]
    reject_observer(opted,'e2e/unit')
    put('e2e/pom.xml',saved_pom); classpaths['e2e']=[ordinary]; build_inputs()
    # Existing agent dumps stay in a fresh private worker sibling, outside metadata/artifact inputs.
    with tempfile.TemporaryDirectory() as worker_tmp:
        worker=pathlib.Path(worker_tmp); capture_inputs=worker/'ci-inputs'; capture_inputs.mkdir()
        captured=capture_inputs/'build-inputs.json'; captured.write_text((root/'build-inputs.json').read_text())
        runtime_command=opted.copy(); runtime_command[runtime_command.index('--build-inputs')+1]=str(captured)
        runtime=worker/'ci-runtime-bytecode'
        runtime_command+=['--runtime-class-dir',str(runtime)]
        runtime_env={**environment,'RUNNER_TEMP':str(worker)}
        success=subprocess.run(runtime_command,env=runtime_env,capture_output=True,text=True)
        assert success.returncode==0,success.stdout+success.stderr
        runtime_args=(root/'mvn-args').read_text().splitlines()
        runtime_flag='-Djacoco.classDumpDir='+str(runtime.resolve())
        assert runtime_args.count(runtime_flag)==1
        original_tmp=[a for a in observed if a.startswith(('-Djava.io.tmpdir=', '-Dtapstate.it.jvmArgs='))]
        current_tmp=[a for a in runtime_args if a.startswith(('-Djava.io.tmpdir=', '-Dtapstate.it.jvmArgs='))]
        def tmp_args(directory):
            option='-Djava.io.tmpdir='+str(directory.resolve()/'tapstate-java-tmp')
            return [option, '-Dtapstate.it.jvmArgs='+option]
        assert original_tmp==tmp_args(runner_temp) and current_tmp==tmp_args(worker)
        assert [a for a in runtime_args if a!=runtime_flag and a not in current_tmp]==[a for a in observed if a not in original_tmp], 'runtime capture must retain the original agent/selector/lifecycle settings'
        marker=json.loads((root/'target/ci-shards/run-rest.json').read_text())
        origin=runtime/'capture-origin.json'
        assert marker['runtime_class_capture']['capture_origin_sha256']==hashlib.sha256(origin.read_bytes()).hexdigest()
        assert marker['runtime_class_capture']['build_inputs_sha256']==hashlib.sha256(captured.read_bytes()).hexdigest()
        assert marker['runtime_class_capture']['cohort_hash']==added['cohort_hash']
        assert json.loads(origin.read_text())['status']=='success'
        def runtime_reject(command,message,env=runtime_env):
            put('e2e/target/failsafe-reports/TEST-runtime-preserve.xml','must survive runtime preflight')
            (root/'mvn-args').unlink(missing_ok=True)
            result=subprocess.run(command,env=env,capture_output=True,text=True)
            assert result.returncode!=0 and message in result.stderr,(message,result.stdout,result.stderr)
            assert not (root/'mvn-args').exists()
            assert (root/'e2e/target/failsafe-reports/TEST-runtime-preserve.xml').read_text()=='must survive runtime preflight'
        runtime_reject(runtime_command,'must be fresh')
        shutil.rmtree(runtime)
        runtime.symlink_to(capture_inputs,target_is_directory=True)
        runtime_reject(runtime_command,'unsafe runtime dump path')
        runtime.unlink()
        bad=runtime_command.copy(); bad[-1]=str(capture_inputs/'ci-runtime-bytecode')
        runtime_reject(bad,'worker input sibling')
        wrong_worker={**runtime_env,'RUNNER_TEMP':str(worker/'foreign')}
        runtime_reject(runtime_command,'current worker',env=wrong_worker)
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
    assert any(a.startswith('-Dit.test=') and 'CloudFixtureIT' in a for a in args)
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
python3 -B "$here/_ci-layout-smoke.py"
