#!/usr/bin/env python3
"""Exercise provider capture with an explicit JVM boundary recorder, never native proof."""
import copy
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile

sys.dont_write_bytecode = True
for key in ('GIT_DIR', 'GIT_WORK_TREE', 'GIT_INDEX_FILE', 'GIT_PREFIX', 'GIT_QUARANTINE_PATH'):
    os.environ.pop(key, None)
HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location('provider_evidence', HERE / '_ci-evidence.py')
e = importlib.util.module_from_spec(spec)
spec.loader.exec_module(e)
CLASS = 'io.tapstate.core.event.HowManyBytesARowIsTest'
PROVIDER = 'eachRuleChargesWhatTheDefinitionSays'
MODULE = 'core/core-event'
SOURCE = MODULE + '/src/test/java/' + CLASS.replace('.', '/') + '.java'
CONTRACT = '.github/scripts/ci-provider-contracts.json'
CHECKS = 0


def check(ok, reason):
    global CHECKS
    assert ok, reason
    CHECKS += 1


def prepare(root, source_kind='dynamic'):
    root.mkdir()
    def put(name, value):
        path = root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(value if isinstance(value, bytes) else value.encode())
        return path
    put('pom.xml', '<project><modules><module>'+MODULE+'</module></modules></project>')
    put(MODULE+'/pom.xml', '<project/>')
    code = '''package io.tapstate.core.event;
import java.util.stream.Stream;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
class HowManyBytesARowIsTest {
  @TestFactory Stream<DynamicTest> eachRuleChargesWhatTheDefinitionSays() {
    return Stream.of(DynamicTest.dynamicTest("first", () -> { throw new AssertionError("must not execute"); }),
        DynamicTest.dynamicTest("second", () -> { throw new AssertionError("must not execute"); }));
  }
}
'''
    if source_kind == 'source':
        code = '''package io.tapstate.core.event;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
class HowManyBytesARowIsTest {
  @ParameterizedTest @MethodSource("eachRuleChargesWhatTheDefinitionSays") void checks(String value) {}
  static Stream<Arguments> eachRuleChargesWhatTheDefinitionSays() {
    return java.util.stream.IntStream.range(0, 2).mapToObj(index -> Arguments.of(index == 0 ? "first" : "second"));
  }
}
'''
    put(SOURCE, code)
    put(MODULE+'/src/test/resources/provider-fixture.txt', 'fixed source fixture')
    contract = {'schema':1, 'classes':{CLASS:{'module':MODULE,'source':SOURCE,
        'source_sha256':e.digest(code.encode()),'providers':{PROVIDER:{'kind':source_kind,'projection':'strict'}}}}}
    put(CONTRACT, json.dumps(contract))
    for command in (['init','-b','main'], ['config','user.email','fixture@example.invalid'], ['config','user.name','CI fixture'], ['add','.'], ['commit','-m','Provider fixture']):
        subprocess.run(['git','-C',str(root),*command],check=True,capture_output=True)
    put(MODULE+'/target/test-classes/'+CLASS.replace('.','/')+'.class', b'protocol fixture class, not native bytecode')
    put('test-support/target/test-classes/io/tapstate/testsupport/CiProviderInventory.class', b'protocol fixture helper, not native bytecode')
    repository = root.parent / (root.name+'-repository')
    dependency = repository / 'fixture/dependency.jar'
    dependency.parent.mkdir(parents=True)
    dependency.write_bytes(b'protocol fixture dependency, not a native JAR')
    inventory = root.parent / (root.name+'-inventory.json')
    e.inventory(type('Args', (), {'root':str(root),'source_archive':None,'source_receipt':None,'lane':'ci',
        'kind':None,'parameters':None,'published_connectors':'absent','native_cli_presence':'absent','output':str(inventory)})())
    inputs = {'schema':1,'phase':'pre-test-after-install','source_sha':e.current_sha(root),
        'source_tree':subprocess.check_output(['git','-C',str(root),'rev-parse','HEAD^{tree}'],text=True).strip(),
        'run_id':'local','run_attempt':'1','event':'local',
        'runner_context':{'repository':None,'shard_id':'rest','workflow_source':{'snapshot':'unknown','source_bytes_verified':False}},
        'source_files':{name:e.digest((root/name).read_bytes()) for name in subprocess.check_output(['git','-C',str(root),'ls-files'],text=True).splitlines()},
        'test_files':{path.relative_to(root).as_posix():e.digest(path.read_bytes()) for path in root.rglob('*.class')},
        'production_files':{},'resolved_test_classpaths':{MODULE:[{'identity':'repository/fixture/dependency.jar','sha256':e.digest(dependency.read_bytes())}]}}
    input_path = root.parent / (root.name+'-inputs.json')
    e.output(input_path, inputs)
    java = root.parent / (root.name+'-java')
    java.write_text('#!'+sys.executable+'\n'+'''import hashlib,json,os,sys
from pathlib import Path
requests,result=map(Path,sys.argv[-2:])
if os.environ.get('FIXTURE_PROVIDER_CALLS'):
    with open(os.environ['FIXTURE_PROVIDER_CALLS'],'a') as out: out.write('called\\n')
rows=[]
for line in requests.read_text().splitlines():
    classname,method,provider,kind=line.split('\\t')
    class_file=Path('target/test-classes')/(classname.replace('.','/')+'.class')
    rows.append({'schema':1,'class':classname,'test_method':method,'provider':provider,'kind':kind,
        'provider_class_sha256':hashlib.sha256(class_file.read_bytes()).hexdigest(),
        'cases':[{'stable':True,'display_name_digest':'1'*64},{'stable':True,'display_name_digest':'2'*64}]})
for row in rows:
    if row['kind']=='source':
        row['cases']=[{'stable':True,'argument_digest':str(i)*64,'first_string_digest':str(i)*64,
            'argument_descriptors':[{'position':0,'stable':True,'digest':str(i)*64}]} for i in (1,2)]
mode=os.environ.get('FIXTURE_PROVIDER_MODE','valid')
if mode=='unstable-descriptor':
    rows[0]['cases'][0]['argument_descriptors'][0]['stable']=False
    rows[0]['cases'][0]['first_string_digest']=None
if mode=='wrong-first-string': rows[0]['cases'][0]['first_string_digest']='3'*64
if mode=='failure': raise SystemExit(9)
if mode=='wrong-provider': rows[0]['provider']='differentProvider'
if mode=='wrong-kind': rows[0]['kind']='source'
if mode=='boolean-schema': rows[0]['schema']=True
if mode=='duplicate': rows.append(rows[0])
if mode=='empty': rows=[]
result.write_text(''.join(json.dumps(row,separators=(',',':'))+'\\n' for row in rows))
''')
    java.chmod(0o755)
    return {'root':root,'repository':repository,'inventory':inventory,'inputs':input_path,'java':java,'contracts':root/CONTRACT}


def fixture_environment():
    return {key: value for key, value in os.environ.items() if not key.startswith('GITHUB_') and key != 'GH_TOKEN'}


def execute(f, suffix, mode='valid', extra=(), environment=None, include_receipt=True):
    output = f['root'].parent / (suffix+'-resolved.json')
    capture = f['root'].parent / (suffix+'-receipt.json')
    command = [str(HERE/'ci-evidence.sh'),'resolve','--root',str(f['root']),'--inventory',str(f['inventory']),
        '--inputs',str(f['inputs']),'--repo-local',str(f['repository']),'--contracts',str(f['contracts']),
        '--java',str(f['java']),'--output',str(output)]
    if include_receipt:
        command += ['--probe-receipt',str(capture)]
    command += list(extra)
    result = subprocess.run(command,env={**fixture_environment(),'FIXTURE_PROVIDER_MODE':mode,**(environment or {})},capture_output=True,text=True)
    return result, output, capture


def main():
    with tempfile.TemporaryDirectory(prefix='ci-provider-capture-') as temporary:
        f = prepare(Path(temporary)/'source')
        result, resolved, capture = execute(f,'valid')
        check(result.returncode==0, result.stdout+result.stderr)
        inv, record = e.load(resolved), e.load(capture)
        check(inv['complete'] and record['capture_status']=='success', 'bounded successful invocation is captured')
        check(record['resolved_method_count']==1 and record['resolved_case_count']==2, 'method and case counts derive from actual result')
        check(record['binding']['source_sha']==e.current_sha(f['root']) and record['binding']['run_id']=='local', 'local fixture is bound without inventing native origin')
        check(record['independent_provider_replay_proven'] is False and record['ability_proven'] is False and record['whole_build_provenance_complete'] is False, 'capture never self-certifies semantic replay or whole provenance')
        call = record['invocations'][0]
        check(call['exit_code']==0 and call['requested_method_count']==1 and call['elapsed_ns']>0, 'receipt records completed bounded process')
        check(call['request_sha256']==e.digest((CLASS+'\t'+PROVIDER+'\t'+PROVIDER+'\tdynamic\n').encode()), 'request digest binds actual helper TSV')
        check(record['build_inputs_sha256']==e.digest(f['inputs'].read_bytes()) and record['contracts']['sha256']==e.digest(f['contracts'].read_bytes()), 'prepared inputs and reviewed contract bytes are bound')
        check('CI_PROVIDER_PROBE_RECEIPT_SHA256 '+e.digest(capture.read_bytes()) in result.stdout, 'actual CLI emits the archived receipt digest')
        hashes = f['root'].parent/'expected.json'
        e.export_hashes(type('Args',(),{'input':str(resolved),'output':str(hashes)})())
        expected = e.load(hashes)
        check(expected['provider_probe_receipt_sha256']==e.digest(capture.read_bytes()), 'expected hashes retain exact receipt binding')
        check(sorted(row['case_digest'] for row in expected['cases'])==record['resolved_methods'][0]['case_digests'], 'receipt case digests join independently exported expected identities')
        for mode in ('failure','empty','duplicate','wrong-provider','wrong-kind','boolean-schema'):
            run, _, failed = execute(f,mode,mode)
            check(run.returncode!=0 and not failed.exists(), mode+' provider response must fail before receipt publication')
        calls = f['root'].parent/'calls.log'
        prior_calls = os.environ.get('FIXTURE_PROVIDER_CALLS')
        os.environ['FIXTURE_PROVIDER_CALLS']=str(calls)
        try:
            run, _, _ = execute(f,'valid')
            check(run.returncode!=0, 'receipt cannot overwrite an earlier capture')
            check(not calls.exists(), 'reused receipt fails before invoking any provider')
        finally:
            if prior_calls is None: os.environ.pop('FIXTURE_PROVIDER_CALLS')
            else: os.environ['FIXTURE_PROVIDER_CALLS']=prior_calls
        run, diagnostic, diagnostic_capture = execute(f,'diagnostic',extra=('--diagnostic',))
        check(run.returncode==0 and e.load(diagnostic_capture)['capture_status']=='diagnostic'
              and e.load(diagnostic)['complete'] is False, 'diagnostic capture remains explicitly incomplete')
        for label, frozen in [('inputs',f['inputs']),('inventory',f['inventory']),('contracts',f['contracts'])]:
            original_bytes=frozen.read_bytes()
            try:
                run, _, created = execute(f,'alias-'+label,extra=('--output',str(frozen)))
                check(run.returncode!=0 and not created.exists() and frozen.read_bytes()==original_bytes,
                      'receipt output alias must not replace frozen '+label)
            finally:
                frozen.write_bytes(original_bytes)
        alias=f['root'].parent/'hardlink-inputs.json'
        os.link(f['inputs'],alias)
        original_bytes=f['inputs'].read_bytes()
        try:
            run, _, created=execute(f,'hardlink-alias',extra=('--output',str(alias)))
            check(run.returncode!=0 and not created.exists() and f['inputs'].read_bytes()==original_bytes,
                  'receipt output hardlink must not replace frozen inputs')
        finally:
            f['inputs'].write_bytes(original_bytes)
        original_inventory=f['inventory'].read_bytes()
        try:
            run, _, absent=execute(f,'legacy',extra=('--output',str(f['inventory'])),include_receipt=False)
            check(run.returncode==0 and not absent.exists() and e.load(f['inventory'])['complete'], 'legacy no-receipt in-place output remains supported')
        finally:
            f['inventory'].write_bytes(original_inventory)
        run, _, absent=execute(f,'wrong-native-run',environment={'GITHUB_RUN_ID':'999'})
        check(run.returncode!=0 and not absent.exists(), 'captured input run cannot impersonate another native invocation')
        altered=e.load(f['inputs']);prior_inputs=f['inputs'].read_bytes();altered['source_tree']='f'*40;e.output(f['inputs'],altered)
        try:
            run, _, absent=execute(f,'wrong-tree')
            check(run.returncode!=0 and not absent.exists(), 'capture binds the actual checkout tree')
        finally:
            f['inputs'].write_bytes(prior_inputs)
        source_fixture = prepare(Path(temporary)/'source-provider', source_kind='source')
        run, _, source_capture = execute(source_fixture,'source-valid')
        check(run.returncode==0, 'stable source provider capture succeeds: '+run.stderr)
        check(e.load(source_capture)['invocations'][0]['requested_method_count']==1, 'source response controls exercised a computed JVM provider')
        for mode in ('unstable-descriptor','wrong-first-string'):
            run, _, bad = execute(source_fixture,mode,mode)
            check(run.returncode!=0 and not bad.exists(), mode+' source response must not create a complete capture')
    print('CI provider capture controls passed:',CHECKS)


if __name__=='__main__':
    main()
