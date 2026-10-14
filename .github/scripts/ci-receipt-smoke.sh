#!/usr/bin/env bash
# Negative controls for persistent raw evidence, unsafe archives and provenance.
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
python3 - "$here" <<'PY'
import argparse
import copy
import os
import struct
import subprocess
from types import SimpleNamespace
import importlib.util
import io
import json
from pathlib import Path
import tempfile
import zipfile
import sys
sys.dont_write_bytecode = True
spec = importlib.util.spec_from_file_location('receipt', Path(sys.argv[1]) / '_ci-receipt.py')
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)
checks = 0

def check(value, message):
    global checks
    assert value, message
    checks += 1

def rejected(fn, message):
    try:
        fn()
    except (ValueError, OSError, zipfile.BadZipFile):
        check(True, message)
    else:
        raise AssertionError(message)

with tempfile.TemporaryDirectory() as tmp:
    root = Path(tmp)
    raw = root / 'XML'
    raw.mkdir()
    (raw / 'TEST-A.xml').write_text('<testsuite/>')
    nested = raw / 'receipt.json'
    nested.write_text('original nested receipt')
    m.manifest(root, {'archive_complete': True, 'gaps': []})
    m.verify(argparse.Namespace(directory=str(root)))
    check(True, 'complete raw evidence reads back')
    nested.write_text('tampered nested receipt')
    rejected(lambda: m.verify(argparse.Namespace(directory=str(root))), 'nested receipt basename is included in archive hashes')
    nested.write_text('original nested receipt')
    (raw / 'TEST-A.xml').write_text('<testsuite tests="0"/>')
    rejected(lambda: m.verify(argparse.Namespace(directory=str(root))), 'same file identity with tampered bytes rejects')
    (raw / 'TEST-A.xml').write_text('<testsuite/>')
    (raw / 'EXTRA.xml').write_text('unexpected')
    rejected(lambda: m.verify(argparse.Namespace(directory=str(root))), 'extra evidence rejects')
    (raw / 'EXTRA.xml').unlink()
    (raw / 'TEST-A.xml').unlink()
    rejected(lambda: m.verify(argparse.Namespace(directory=str(root))), 'missing original evidence rejects')
    (raw / 'TEST-A.xml').write_text('<testsuite/>')
    m.manifest(root, {'archive_complete': False, 'gaps': ['only expired URL']})
    rejected(lambda: m.verify(argparse.Namespace(directory=str(root))), 'hash-valid but incomplete receipt rejects')
    (raw / 'LINK.xml').symlink_to(raw / 'TEST-A.xml')
    rejected(lambda: m.manifest(root, {'archive_complete': True}), 'symlink evidence rejects')
    (raw / 'LINK.xml').unlink()
    for member in ['../escape.xml', '/absolute.xml', 'unsafe\\name.xml']:
        path = root / 'unsafe.zip'
        with zipfile.ZipFile(path, 'w') as archive:
            archive.writestr(member, 'wrong')
        rejected(lambda: m.zip_members(path), 'archive traversal rejects: ' + member)
    path = root / 'unsafe.zip'
    with zipfile.ZipFile(path, 'w') as archive:
        link = zipfile.ZipInfo('link.xml')
        link.create_system = 3
        link.external_attr = 0o120777 << 16
        archive.writestr(link, '../outside')
    rejected(lambda: m.zip_members(path), 'ZIP symlink rejects')
    with zipfile.ZipFile(path, 'w') as archive:
        archive.writestr('data.xml', '<testsuite/>')
    check(m.zip_members(path) == ['data.xml'], 'normal raw ZIP reads back')
    path.write_bytes(b'not zip')
    rejected(lambda: m.zip_members(path), 'corrupt ZIP rejects')
    classfile = b'\xca\xfe\xba\xbe\x00\x00\x00\x41original'
    check(m.crc64(classfile) != m.crc64(classfile + b'changed'), 'changed class bytes have different identity')
    check(m.crc64(b'123456789') == '46a5a9388a5beffe', 'CRC64 known polynomial vector')
    classbytes = b'\xca\xfe\xba\xbe\x00\x00\x00\x41\x00\x01\x10\x01'
    check(m.class_access_flags(classbytes) == 0x1001, 'actual synthetic access flags do not depend on observed XML')
    rejected(lambda: m.class_access_flags(classbytes[:-1]), 'truncated class access metadata rejects')
    # Only whitelisted coordinate/type producer facts escape a dependency model.
    repository=root/'repository'; repository.mkdir()
    parent='example/parent/1/parent-1.pom'
    child='example/child/1/child-1.pom'
    binary='example/child/1/child-1.jar'
    def model_file(relative, content):
        path=repository/relative; path.parent.mkdir(parents=True,exist_ok=True); path.write_text(content); return path
    model_file(parent,'<project><modelVersion>4.0.0</modelVersion><groupId>example</groupId><artifactId>parent</artifactId><version>1</version><packaging>pom</packaging></project>')
    model_file(child,'<project><modelVersion>4.0.0</modelVersion><parent><groupId>example</groupId><artifactId>parent</artifactId><version>1</version></parent><artifactId>child</artifactId><properties><credential>private-model-sentinel</credential></properties><url>https://private-model-sentinel.invalid</url></project>')
    with zipfile.ZipFile(repository/binary,'w') as archive:
        archive.writestr('META-INF/maven/example/child/pom.properties','groupId=example\nartifactId=child\nversion=1\n')
    hashes={name:m.digest(repository/name) for name in (parent,child,binary)}
    models,producers=m.dependency_models(repository,hashes)
    check(models[child]['gav']==['example','child','1'] and models[child]['parent']['sha256']==hashes[parent], 'literal inherited coordinates bind their parent bytes')
    check(producers[binary]['producer_identity_verified'] and producers[binary]['producer_identity_source']=='embedded-pom-properties', 'embedded Maven producer identity is verified')
    check('private-model-sentinel' not in json.dumps([models,producers]), 'arbitrary properties and URLs are never emitted')
    check(not models[child]['effective_dependency_model_proven'] and not producers[binary]['runtime_realm_proven'], 'model coordinates never pretend to establish effective dependencies or runtime realms')
    with zipfile.ZipFile(repository/binary,'w') as archive:
        archive.writestr('META-INF/MANIFEST.MF','Manifest-Version: 1.0\r\nImplementation-Title: child\r\nImplementation-Version: 1\r\n\r\n')
    hashes[binary]=m.digest(repository/binary); models,producers=m.dependency_models(repository,hashes)
    check(producers[binary]['producer_identity_verified'] and not producers[binary]['pom_properties_present'] and producers[binary]['producer_identity_source']=='embedded-implementation-manifest', 'manifest evidence verifies a producer without inventing pom.properties')
    with zipfile.ZipFile(repository/binary,'w') as archive:
        archive.writestr('META-INF/MANIFEST.MF','Implementation-Title: child\nImplementation-Version: 2\n')
    hashes[binary]=m.digest(repository/binary); models,producers=m.dependency_models(repository,hashes)
    check(not producers[binary]['producer_identity_verified'], 'wrong embedded producer version does not verify')
    missing_parent={name:checksum for name,checksum in hashes.items() if name!=parent}
    models,producers=m.dependency_models(repository,missing_parent)
    check(not models[child]['coordinate_type_verified'] and not producers, 'parent outside actual byte snapshot remains unknown')
    model_file(child,'<project><groupId>example</groupId><artifactId>child</artifactId><version>${revision}</version></project>'); hashes[child]=m.digest(repository/child)
    models,producers=m.dependency_models(repository,hashes)
    check(not models[child]['coordinate_type_verified'], 'unresolved coordinate expressions remain unknown')
    # A producer without Maven properties can still report exact literal OSGi coordinates.
    model_file(child,'<project><parent><groupId>example</groupId><artifactId>parent</artifactId><version>1</version></parent><artifactId>child</artifactId></project>'); hashes[child]=m.digest(repository/child)
    for symbolic,version,expected in [('example.child','1',True),('wrong.child','1',False),('example.child','2',False)]:
        with zipfile.ZipFile(repository/binary,'w') as archive:
            archive.writestr('META-INF/MANIFEST.MF','Bundle-SymbolicName: '+symbolic+'\nBundle-Version: '+version+'\n')
        hashes[binary]=m.digest(repository/binary); models,producers=m.dependency_models(repository,hashes)
        check(producers[binary]['producer_identity_verified']==expected,'OSGi producer requires exact GAV and version')
        if expected: check(producers[binary]['producer_identity_source']=='embedded-osgi-manifest' and not producers[binary]['pom_properties_present'],'OSGi never invents Maven properties')
    # Read genuine dump-format class metadata, preserving variants and hiding non-Class literals.
    worker=root/'worker'; worker.mkdir(); checkout=worker/'checkout'; checkout.mkdir(); capture_inputs=worker/'ci-inputs'; capture_inputs.mkdir(); dumps=worker/'ci-runtime-bytecode'; dumps.mkdir()
    agent_name='org/jacoco/org.jacoco.agent/0.8.13/org.jacoco.agent-0.8.13-runtime.jar'
    agent=model_file(agent_name,'only an offline digest fixture; actual-agent proof is separate')
    value=dict(source_sha='a'*40,run_id='local',run_attempt='1',jacoco_version='0.8.13',production_class_ids={},production_files={},test_class_ids={},test_files={},resolved_test_classpaths={},dependency_repository_files={agent_name:m.digest(agent)},dependency_coordinate_models={})
    before=capture_inputs/'build-inputs.json'; before.write_text(json.dumps(value))
    def observation(phase,start,end,utc,ended):
        return dict(schema=1,command='run',execution='local',phase=phase,event='end',status='success',source_sha='a'*40,shard_id='fixture',cohort_hash='b'*64,run_id=None,run_attempt=None,started_monotonic_ns=start,monotonic_ns=end,elapsed_ns=end-start,utc=utc,ended_utc=ended)
    origin=dict(schema=1,execution='local',source_sha='a'*40,run_id=None,run_attempt=None,shard_id='fixture',cohort_hash='b'*64,invocation_id='c'*32,status='success',case_observer={'build_inputs_sha256':m.digest(before)},runtime_class_capture=dict(enabled=True,jacoco_version='0.8.13',raw_scope='worker-private-input-sibling'),reset=observation('reset-test-evidence',1,2,'2026-10-11T18:00:00+00:00','2026-10-11T18:00:01+00:00'),verify=observation('selected-verify',3,4,'2026-10-11T18:00:02+00:00','2026-10-11T18:00:03+00:00'))
    marker=dumps/'capture-origin.json'; marker.write_text(json.dumps(origin))
    args=argparse.Namespace(runtime_class_dir=str(dumps),phase='post-test-before-pack',output=str(capture_inputs/'post-test-inputs.json'),root=str(checkout),repo_local=str(repository))
    def bytecode(name,literal='private-runtime-sentinel'):
        def utf(text):
            data=text.encode(); return b'\x01'+struct.pack('>H',len(data))+data
        pool=utf(name)+b'\x07\x00\x01'+utf('java/lang/Object')+b'\x07\x00\x03'+utf(literal)+b'\x08\x00\x05'
        return b'\xca\xfe\xba\xbe\x00\x00\x00\x41\x00\x07'+pool+struct.pack('>7H',1,2,4,0,0,0,0)
    def put_dump(data):
        name=m.class_structure(data)['name']; path=dumps/(name+'.'+m.crc64(data)+'.class');path.parent.mkdir(parents=True,exist_ok=True);path.write_bytes(data);return path
    saved_worker=os.environ.get('RUNNER_TEMP');os.environ['RUNNER_TEMP']=str(worker)
    try:
        package_bytes=bytecode('io/tapstate/fixture/package-info');package_dump=dumps/('io/tapstate/fixture/package-info.'+m.crc64(package_bytes)+'.class');package_dump.parent.mkdir(parents=True);package_dump.write_bytes(package_bytes)
        known_package=copy.deepcopy(value);package_path='fixture/target/classes/io/tapstate/fixture/package-info.class';known_package['production_class_ids'][package_path]=m.crc64(package_dump.read_bytes());known_package['production_files'][package_path]=m.digest(package_dump)
        check(not m.runtime_class_metadata(args,known_package)[1], 'compiler package metadata stays known bytecode and never becomes generated')
        package_dump.unlink()
        check(m.class_structure(bytecode('module-info'))['name']=='module-info', 'compiler module metadata is recognized')
        rejected(lambda:m.class_structure(bytecode('io/tapstate/fixture/unsafe-name')), 'arbitrary hyphenated names remain unsafe')
        first=put_dump(bytecode('io/tapstate/GeneratedFixture'))
        capture,rows=m.runtime_class_metadata(args,value)
        check(len(rows)==1 and rows[0]['name']=='io/tapstate/GeneratedFixture' and 'private-runtime-sentinel' not in json.dumps(rows),'runtime structure emits only Class constants')
        check(capture['capture_origin_sha256']==m.digest(marker) and capture['build_inputs_sha256']==m.digest(before) and capture['dump_class_count']==1,'runtime origin/input/counts bind actual bytes')
        second=put_dump(bytecode('io/tapstate/GeneratedFixture','different-private-literal'))
        check(len(m.runtime_class_metadata(args,value)[1])==2,'same runtime name with distinct CRCs is never collapsed')
        known=copy.deepcopy(value); relative='fixture/target/classes/io/tapstate/GeneratedFixture.class'
        known['production_class_ids'][relative]='0'*16;known['production_files'][relative]=m.digest(first)
        rejected(lambda:m.runtime_class_metadata(args,known),'compiled same-name different CRC cannot become generated')
        for phase,field,bad in [('verify','schema',999),('verify','elapsed_ns',False),('verify','event','start'),('verify','execution','unknown'),('verify','utc','2026-10-11T18:00:02')]:
            changed=copy.deepcopy(origin);changed[phase][field]=bad;marker.write_text(json.dumps(changed))
            rejected(lambda:m.runtime_class_metadata(args,value),'malformed runtime successful-verify provenance rejects')
        marker.write_text(json.dumps(origin))
        first.rename(first.with_name('GeneratedFixture.'+'0'*16+'.class'))
        rejected(lambda:m.runtime_class_metadata(args,value),'dump filename and actual CRC must agree')
        wrong=argparse.Namespace(**vars(args));wrong.runtime_class_dir=str(capture_inputs/'ci-runtime-bytecode')
        rejected(lambda:m.runtime_class_metadata(wrong,value),'runtime raw cannot be inside uploaded inputs')
    finally:
        if saved_worker is None:os.environ.pop('RUNNER_TEMP',None)
        else:os.environ['RUNNER_TEMP']=saved_worker
    # Deterministic clock accounting must include the runtime collector, without duration gates.
    timing_root=root/'timing-checkout'; timing_root.mkdir(); (timing_root/'pom.xml').write_text('<project><properties><plugin.jacoco.version>0.8.13</plugin.jacoco.version></properties></project>')
    compiled=timing_root/'target/classes/sample/Fixture.class';compiled.parent.mkdir(parents=True);compiled.write_bytes(b'\xca\xfe\xba\xbe\x00\x00\x00\x41\x00\x01\x00\x01')
    originals=(m.git,m.command,m.subprocess.run,m.time.monotonic_ns,m.runtime_class_metadata)
    clock=[1_000_000_000]
    try:
        def git_boundary(directory,*arguments):
            if arguments==('rev-parse','--show-toplevel'):return str(timing_root)
            if arguments in [('status','--porcelain','--untracked-files=no'),('ls-files','--others','--exclude-standard')]:return ''
            if arguments==('ls-files',):return 'pom.xml'
            return 'a'*40
        m.git=git_boundary;m.command=lambda arguments,**kwargs:'fixture Maven version'
        m.subprocess.run=lambda *arguments,**kwargs:SimpleNamespace(returncode=0,stderr='fixture JDK21',stdout='')
        m.time.monotonic_ns=lambda:clock[0]
        def collector_boundary(arguments,value):
            clock[0]+=500_000_000;return dict(enabled=False),[]
        m.runtime_class_metadata=collector_boundary
        out=timing_root/'inputs.json'
        m.inputs(argparse.Namespace(root=str(timing_root),repo_local=str(repository),output=str(out),resolve_classpath=False,phase='post-test-before-pack',runtime_class_dir=None))
        check(json.loads(out.read_text())['elapsed_seconds']==0.5,'phase timing includes all runtime metadata collection')
    finally:
        m.git,m.command,m.subprocess.run,m.time.monotonic_ns,m.runtime_class_metadata=originals
    # Equal source labels and totals do not excuse different current build bytes.
    input_plan = dict(source_sha='a'*40, cohort_hash='b'*64,
                      shards=[dict(id='e2e-1'), dict(id='engine'), dict(id='rest')])
    prepared = dict(schema=1, phase='pre-test-after-install', source_sha='a'*40,
                    source_tree='c'*40, run_id='123', run_attempt='1',
                    source_files={'pom.xml': 'a'*64}, production_files={'core/target/classes/A.class': 'b'*64},
                    production_class_ids={'core/target/classes/A.class': '1'*16},
                    production_class_access_flags={'core/target/classes/A.class': 33},
                    test_files={'core/target/test-classes/ATest.class': 'c'*64},
                    test_class_ids={'core/target/test-classes/ATest.class': '2'*16},
                    packaged_outputs={'test-support/target/test-support-tests.jar': dict(sha256='d'*64, size=1, mode=420),
                                      'app/target/app-boot.jar': dict(sha256='e'*64, size=1, mode=420)},
                    resolved_test_classpaths={'core': [dict(identity='repository/io/tapstate/cloud-sdk/1/sdk.jar',
                                                           sha256='f'*64, size=1, own_classes={})]},
                    jacoco_version='0.8.13', toolchain=dict(java='JDK21', maven='Maven3', architecture='x86_64'),
                    dependency_repository_files={'io/tapdata/pdk/2.0/pdk.jar': 'd'*64})
    worker_inputs = {shard['id']: copy.deepcopy(prepared) for shard in input_plan['shards']}
    original_workers = copy.deepcopy(worker_inputs)
    def compare_current(): return m.compare_input_snapshots(input_plan, prepared, worker_inputs)
    result = compare_current()
    check(result['declared_input_hashes_equal'] and not result['whole_build_provenance_complete']
          and not result['ability_proven'] and not result['raw_bytes_uploaded'], 'current declared hashes agree without inventing complete provenance')
    for field in m.BUILD_INPUT_FIELDS:
        worker_inputs['engine'][field] = {} if isinstance(prepared[field], dict) else 'wrong'
        rejected(compare_current, 'missing or different projection rejects: ' + field)
        worker_inputs = copy.deepcopy(original_workers)
    for field, value in [('source_sha', 'd'*40), ('run_id', 'old'), ('run_attempt', '2'),
                         ('phase', 'post-test-before-pack'), ('schema', 2)]:
        worker_inputs['rest'][field] = value
        rejected(compare_current, 'old source/run/attempt/schema/phase rejects: ' + field)
        worker_inputs = copy.deepcopy(original_workers)
    for key in ['test-support/target/test-support-tests.jar', 'app/target/app-boot.jar']:
        worker_inputs['engine']['packaged_outputs'].pop(key)
        rejected(compare_current, 'missing required current package rejects: ' + key)
        worker_inputs = copy.deepcopy(original_workers)
    worker_inputs['rest']['production_class_ids']['core/target/classes/A.class'] = '3'*16
    rejected(compare_current, 'same source and class count with different production class ID rejects')
    worker_inputs = copy.deepcopy(original_workers)
    worker_inputs['rest']['resolved_test_classpaths']['core'][0]['sha256'] = 'e'*64
    rejected(compare_current, 'different same-group published SDK bytes reject')
    worker_inputs = copy.deepcopy(original_workers)
    worker_inputs['rest']['dependency_repository_files']['io/tapdata/pdk/2.0/pdk.jar'] = 'e'*64
    rejected(compare_current, 'different available PDK bytes reject')
    worker_inputs = copy.deepcopy(original_workers)
    worker_inputs.pop('engine'); rejected(compare_current, 'missing input shard rejects')
    worker_inputs = copy.deepcopy(original_workers)
    worker_inputs['foreign'] = copy.deepcopy(prepared); rejected(compare_current, 'foreign input shard rejects')
    worker_inputs = copy.deepcopy(original_workers)
    worker_inputs['rest']['dependency_repository_files']['unused/provider.jar'] = 'e'*64
    check(compare_current()['declared_input_hashes_equal'], 'unused incoming cache extras are disclosed by scope and do not pretend to be resolved inputs')
    worker_inputs = copy.deepcopy(original_workers)
    # File selection must distinguish a retained aggregate from current workers.
    worker_root = root / 'worker-inputs'; worker_root.mkdir()
    for shard in input_plan['shards']:
        directory = worker_root / ('ci-inputs-' + shard['id']); directory.mkdir()
        (directory / 'build-inputs.json').write_text(json.dumps(prepared))
    paths = m.worker_input_paths(input_plan, worker_root)
    check(set(paths) == set(original_workers), 'all dynamic planned worker files are selected')
    previous_build = worker_root / 'ci-inputs-build'; previous_build.mkdir()
    (previous_build / 'build-inputs.json').write_text('unreadable old aggregate must never be used')
    check(m.worker_input_paths(input_plan, worker_root) == paths, 'retained aggregate cannot masquerade as a worker on reruns')
    unknown = worker_root / 'ci-inputs-foreign'; unknown.mkdir()
    rejected(lambda: m.worker_input_paths(input_plan, worker_root), 'foreign worker directory still rejects')
    unknown.rmdir()
    path = paths['engine']; content = path.read_text(); path.unlink()
    rejected(lambda: m.worker_input_paths(input_plan, worker_root), 'missing planned input file rejects')
    path.symlink_to(paths['rest'])
    rejected(lambda: m.worker_input_paths(input_plan, worker_root), 'symlink planned input file rejects')
    path.unlink(); path.write_text(content)
    # Exercise the complete command with a real independent Git source and plan.
    fixture = root / 'input-comparison-checkout'; (fixture / 'core/src/test/java/sample').mkdir(parents=True)
    (fixture / 'pom.xml').write_text('<project><modules><module>core</module></modules></project>')
    (fixture / 'core/pom.xml').write_text('<project/>')
    (fixture / 'core/src/test/java/sample/OneTest.java').write_text('package sample; class OneTest {}')
    git_env = {key: value for key, value in os.environ.items() if key not in
               ('GIT_DIR', 'GIT_WORK_TREE', 'GIT_INDEX_FILE', 'GIT_PREFIX', 'GIT_QUARANTINE_PATH')}
    def fixture_git(*args):
        return m.command(['git', '-C', str(fixture), *args], cwd=fixture).strip()
    import subprocess
    subprocess.run(['git', 'init', '-q', str(fixture)], env=git_env, check=True)
    subprocess.run(['git', '-C', str(fixture), 'add', '.'], env=git_env, check=True)
    subprocess.run(['git', '-C', str(fixture), '-c', 'user.name=Fixture', '-c', 'user.email=fixture@example.test',
                    '-c', 'core.hooksPath=/dev/null', 'commit', '-qm', 'Fixture input tree'], env=git_env, check=True)
    plan_path = root / 'current-plan.json'
    subprocess.run(['bash', str(Path(sys.argv[1]) / 'ci-shard.sh'), 'plan', '--root', str(fixture), '--output', str(plan_path)],
                   env=git_env, check=True, capture_output=True)
    plan = json.loads(plan_path.read_text()); current = copy.deepcopy(prepared)
    current.update(source_sha=fixture_git('rev-parse', 'HEAD'), source_tree=fixture_git('rev-parse', 'HEAD^{tree}'),
                   run_id=os.getenv('GITHUB_RUN_ID', 'local'), run_attempt=os.getenv('GITHUB_RUN_ATTEMPT', '1'))
    aggregate_path = root / 'current-aggregate.json'; aggregate_path.write_text(json.dumps(current))
    command_workers = root / 'command-worker-inputs'; command_workers.mkdir()
    for shard in plan['shards']:
        directory = command_workers / ('ci-inputs-' + shard['id']); directory.mkdir()
        (directory / 'build-inputs.json').write_text(json.dumps(current))
    directory = command_workers / 'ci-inputs-build'; directory.mkdir()
    (directory / 'build-inputs.json').write_text('old aggregate is not current')
    arguments = argparse.Namespace(root=str(fixture), plan=str(plan_path), inputs_root=str(command_workers),
                                   aggregate_inputs=str(aggregate_path), output=str(root / 'comparison.json'))
    m.compare_inputs(arguments)
    check(json.loads(Path(arguments.output).read_text())['declared_input_hashes_equal'], 'real Git/plan/file/current-run comparison accepts fresh workers with old aggregate artifact')
    current['run_attempt'] = str(int(current['run_attempt']) + 1)
    aggregate_path.write_text(json.dumps(current))
    for shard in plan['shards']:
        (command_workers / ('ci-inputs-' + shard['id']) / 'build-inputs.json').write_text(json.dumps(current))
    try:
        m.compare_inputs(arguments)
    except ValueError as error:
        check('differs from the current run/attempt' in str(error),
              'equal wrong attempts must reject against current execution identity, not worker disagreement')
    else:
        raise AssertionError('equal wrong attempts escaped current execution identity admission')
    # Event-time merge input comes from the original runner payload, not current PR state.
    checkout=root/'event-checkout'; checkout.mkdir()
    environment=dict(os.environ)
    for key in ('GIT_DIR','GIT_WORK_TREE','GIT_INDEX_FILE','GIT_PREFIX','GIT_QUARANTINE_PATH'):
        environment.pop(key,None)
    environment.update(GIT_AUTHOR_NAME='Fixture',GIT_COMMITTER_NAME='Fixture',
                       GIT_AUTHOR_EMAIL='fixture@example.test',GIT_COMMITTER_EMAIL='fixture@example.test')
    def fixture_git(*args, input=None):
        result=subprocess.run(['git','-C',str(checkout),*args],input=input,env=environment,
                              capture_output=True,text=True)
        assert result.returncode==0,result.stderr
        return result.stdout.strip()
    fixture_git('init','-q')
    (checkout/'source.txt').write_text('base')
    fixture_git('add','source.txt'); fixture_git('commit','-qm','Fixture base')
    base=fixture_git('rev-parse','HEAD')
    (checkout/'source.txt').write_text('head')
    fixture_git('commit','-qam','Fixture head'); head=fixture_git('rev-parse','HEAD')
    tree=fixture_git('rev-parse','HEAD^{tree}')
    merge=fixture_git('commit-tree',tree,'-p',base,'-p',head,input='Fixture merge\n')
    fixture_git('checkout','-q','--detach',merge)
    # The real default checkout is shallow. Parent proof must read the commit object.
    (checkout/'.git/shallow').write_text(merge+'\n')
    check(fixture_git('show','-s','--format=%P','HEAD')=='','depth-one fixture hides traversal parents')
    runner=dict(environment,GITHUB_REPOSITORY='example/project',GITHUB_EVENT_NAME='pull_request',
                GITHUB_SHA=merge,GITHUB_RUN_ID='100',GITHUB_RUN_ATTEMPT='1',
                GITHUB_REF='refs/pull/7/merge',GITHUB_HEAD_REF='ws/fixture',GITHUB_BASE_REF='main')
    payload=dict(repository={'full_name':'example/project'},number=7,action='synchronize',
                 pull_request=dict(number=7,title='private-event-sentinel',body='private-event-sentinel',
                    head=dict(sha=head,ref='ws/fixture',repo={'full_name':'example/project'}),
                    base=dict(sha=base,ref='main',repo={'full_name':'example/project'})))
    captured=m.event_snapshot(checkout,payload,runner)
    run=dict(id=100,repository={'full_name':'example/project'},event='pull_request',run_attempt=1,
             status='completed',path='.github/workflows/ci.yml',head_branch='ws/fixture',
             head_sha=head,pull_requests=[],conclusion='success')
    commit=dict(sha=merge,tree={'sha':tree},parents=[{'sha':base},{'sha':head}])
    def reconcile(value=captured, actual=run):
        return m.reconcile_pr_identity(actual,value,commit,'example/project',7,head,'ws/fixture')
    identity=reconcile()
    check(identity['checkout_sha']==merge and identity['head_sha']==head
          and identity['base_sha']==base,'original PR identity binds distinct head/base/merge in a shallow checkout')
    check(not identity['first_complete_pr_sample_proven'] and not identity['group_equivalence_proven'],
          'identity consistency alone never proves a complete first sample or group equivalence')
    check('private-event-sentinel' not in json.dumps(captured),'event text and user data are excluded')
    failed=copy.deepcopy(run); failed['conclusion']='failure'
    check(reconcile(actual=failed)['conclusion']=='failure','failed first attempts retain identity instead of being selected away')
    def rejects_identity(value, actual, reason):
        try: reconcile(value,actual)
        except ValueError as error:
            check(reason in str(error),'identity rejects for expected reason: '+reason)
        else: raise AssertionError('identity mutation escaped: '+reason)
    for field,wrong,reason in [('checkout_sha','f'*40,'archived Git commit'),
            ('checkout_parents',[base,'f'*40],'original base/head'),
            ('ref','refs/pull/8/merge','PR merge ref'),('run_id',101,'run ID'),
            ('run_attempt',2,'attempt 1'),('event','workflow_dispatch','pull_request'),
            ('repository','foreign/project','repository')]:
        bad=copy.deepcopy(captured);bad[field]=wrong;rejects_identity(bad,run,reason)
    for field,wrong,reason in [('number',8,'foreign PR'),('head_sha','f'*40,'PR head'),
            ('base_sha','f'*40,'original PR input'),('head_repository','foreign/project','expected repository'),
            ('head_ref','ws/foreign','branch'),('base_ref','release','branch')]:
        bad=copy.deepcopy(captured);bad['pull_request'][field]=wrong;rejects_identity(bad,run,reason)
    for field,wrong,reason in [('head_sha','f'*40,'PR head'),('run_attempt',2,'attempt 1'),
            ('status','in_progress','not complete'),('event','workflow_dispatch','pull_request')]:
        bad=copy.deepcopy(run);bad[field]=wrong;rejects_identity(captured,bad,reason)
    association=dict(number=7,head={'sha':head},base={'sha':base})
    populated=copy.deepcopy(run);populated['pull_requests']=[association]
    check(reconcile(actual=populated)['pr_event_identity_consistent'],'matching API association corroborates original event')
    populated['pull_requests'][0]['base']['sha']='f'*40
    rejects_identity(captured,populated,'association differs')
    missing=m.event_snapshot(checkout,{},runner)
    check(missing['event_payload_complete'] is False,'missing fork or historical payload is recorded without changing original CI execution')
    rejects_identity(missing,run,'payload is incomplete')
    for event,ref in [('push','refs/heads/main'),('push','refs/tags/v1.0.0'),('workflow_dispatch','refs/heads/ws/fixture')]:
        nonpr=dict(runner,GITHUB_EVENT_NAME=event,GITHUB_REF=ref)
        original=dict(repository={'full_name':'example/project'},ref=ref,after=merge,inputs={'ignored':'private-event-sentinel'})
        saved=m.event_snapshot(checkout,original,nonpr)
        check(saved['event_payload_complete'] and saved['event']==event and saved['checkout_sha']==merge,
              'original '+event+' '+ref+' preserves its checkout identity')
        check('private-event-sentinel' not in json.dumps(saved),'arbitrary dispatch inputs never enter event receipt')
    deletion=m.event_snapshot(checkout,dict(repository={'full_name':'example/project'},deleted=True,after='0'*40),
                             dict(runner,GITHUB_EVENT_NAME='push',GITHUB_REF='refs/tags/v1.0.0'))
    check(deletion['ref_deleted'] and not deletion['event_payload_complete'] and deletion['checkout_sha']==merge,
          'deleted push keeps checkout identity without treating an absent after commit as the PR source')
    wrong_payload=copy.deepcopy(payload);wrong_payload['pull_request']['base']['sha']='f'*40
    rejected(lambda:m.event_snapshot(checkout,wrong_payload,runner),'capture rejects wrong base against real Git commit parents')
    for field,wrong in [('sha','f'*40),('tree',{'sha':'f'*40}),('parents',[{'sha':'f'*40},{'sha':head}])]:
        changed=copy.deepcopy(commit);changed[field]=wrong
        rejected(lambda:m.reconcile_pr_identity(run,captured,changed,'example/project',7,head,'ws/fixture'),
                 'independent Git commit mutation rejects: '+field)
    event_file=root/'event.json';event_file.write_text(json.dumps(payload))
    runner['GITHUB_EVENT_PATH']=str(event_file)
    snapshot_file=root/'event-inputs.json';run_file=root/'pr-run.json';run_file.write_text(json.dumps(run))
    commit_file=root/'checkout-commit.json';commit_file.write_text(json.dumps(commit))
    script=str(Path(sys.argv[1])/'_ci-receipt.py')
    result=subprocess.run([sys.executable,script,'event-inputs','--root',str(checkout),'--output',str(snapshot_file)],
                          env=runner,capture_output=True,text=True)
    check(result.returncode==0,result.stderr)
    result=subprocess.run([sys.executable,script,'pr-identity','--run-json',str(run_file),
        '--event-inputs',str(snapshot_file),'--checkout-commit',str(commit_file),'--repo','example/project','--pr','7','--head',head,
        '--branch','ws/fixture','--output',str(root/'pr-identity.json')],env=environment,capture_output=True,text=True)
    check(result.returncode==0,result.stderr)
    check(json.loads((root/'pr-identity.json').read_text())['event_inputs_sha256']==m.digest(snapshot_file),
          'CLI identity binds the exact archived event bytes')
    # The collector keeps head source and actual merge source distinct.
    archive_bytes=io.BytesIO()
    with zipfile.ZipFile(archive_bytes,'w') as archive:
        archive.writestr('ci-event-inputs.json',json.dumps(captured))
        archive.writestr('shards.json',json.dumps(dict(source_sha=merge,shards=[{'id':'rest'}])))
    log_bytes=io.BytesIO()
    with zipfile.ZipFile(log_bytes,'w') as archive: archive.writestr('job.txt','original log fixture')
    collected_run=dict(run,created_at='2026-10-14T03:00:00Z')
    original_api,original_command=m.api,m.command
    def collector_api(endpoint):
        if endpoint.endswith('/rulesets'): return []
        if '/git/commits/' in endpoint: return commit
        if '/commits/' in endpoint and '/check-runs?' not in endpoint: return {'sha':head}
        if '/artifacts?' in endpoint:
            return {'total_count':1,'artifacts':[dict(id=1,name='ci-shard-plan',expired=False)]}
        if '/jobs?' in endpoint: return {'total_count':0,'jobs':[]}
        if '/check-runs?' in endpoint: return {'total_count':0,'check_runs':[]}
        return collected_run
    def collector_command(args,binary=False,cwd=None):
        if args[:2]==['gh','api']:
            return archive_bytes.getvalue() if '/artifacts/' in args[2] else log_bytes.getvalue()
        return original_command(args,binary=binary,cwd=cwd)
    m.api,m.command=collector_api,collector_command
    try:
        scoped=argparse.Namespace(repo='example/project',run=100,archive_root=str(root/'scoped-archive'),
            source_root=str(checkout),cache_condition='unknown',pr=7,head=head,branch='ws/fixture')
        check(m.collect(scoped)==2,'missing full artifacts keep collector receipt incomplete')
        archived=root/'scoped-archive/run-100/attempt-1'
        result=json.loads((archived/'receipt.json').read_text())
        check(result['source_sha']==head and result['checkout_sha']==merge
              and (archived/'checkout-source.tar').is_file(),'collector archives both PR head and actual merge checkout')
        check(result['pr_event_identity_verified'] and not result['first_complete_pr_sample_proven'],
              'collector joins explicit PR scope without claiming full first-attempt acceptance')
        scoped.archive_root=str(root/'unscoped-archive');scoped.pr=None
        check(m.collect(scoped)==2,'unscoped PR collection cannot become complete')
        incomplete=json.loads((root/'unscoped-archive/run-100/attempt-1/receipt.json').read_text())
        check(not incomplete['pr_event_identity_verified']
              and any('explicit --pr/--head/--branch' in gap for gap in incomplete['gaps']),
              'collector never infers the experiment PR from a potentially foreign artifact')
        wrong_event=copy.deepcopy(captured);wrong_event['run_attempt']=2
        for label,member,reason in [('missing',None,'missing or ambiguous'),
                ('malformed','not JSON','event capture unreadable'),('nonobject','[]','event capture must be an object'),
                ('old-attempt',json.dumps(wrong_event),'current run/attempt')]:
            archive_bytes=io.BytesIO()
            with zipfile.ZipFile(archive_bytes,'w') as archive:
                archive.writestr('shards.json',json.dumps(dict(source_sha=merge,shards=[{'id':'rest'}])))
                if member is not None: archive.writestr('ci-event-inputs.json',member)
            scoped.archive_root=str(root/(label+'-archive'));scoped.pr=7
            check(m.collect(scoped)==2,label+' event must retain an incomplete receipt')
            incomplete=json.loads((root/(label+'-archive')/'run-100/attempt-1/receipt.json').read_text())
            check(not incomplete['pr_event_identity_verified'] and not incomplete['archive_complete']
                  and incomplete['checkout_sha'] is None and any(reason in gap for gap in incomplete['gaps']),
                  label+' event never falls back to PR head or loses its final manifest')
            rejected(lambda:m.verify(argparse.Namespace(directory=str(root/(label+'-archive')/'run-100/attempt-1'))),
                     label+' archived raw bytes remain readable but cannot establish completeness')
        for label,ids,reason in [('null-id',[None],'invalid shard ID'),
                ('unsafe-id',['../rest'],'invalid shard ID'),('duplicate-id',['rest','rest'],'duplicate shard ID')]:
            archive_bytes=io.BytesIO()
            with zipfile.ZipFile(archive_bytes,'w') as archive:
                archive.writestr('ci-event-inputs.json',json.dumps(captured))
                archive.writestr('shards.json',json.dumps(dict(source_sha=merge,shards=[{'id':n} for n in ids])))
            scoped.archive_root=str(root/(label+'-archive'))
            check(m.collect(scoped)==2,label+' plan must retain an incomplete receipt')
            incomplete=json.loads((root/(label+'-archive')/'run-100/attempt-1/receipt.json').read_text())
            check(not incomplete['archive_complete'] and any(reason in gap for gap in incomplete['gaps']),
                  label+' cannot escape plan parsing recovery')
    finally: m.api,m.command=original_api,original_command
    # First-attempt provenance is validated independently of successful conclusion.
    original_api = m.api
    m.api = lambda endpoint: {'id': 10, 'run_attempt': 1, 'status': 'in_progress', 'repository': {'full_name': 'example/project'}}
    rejected(lambda: m.collect(argparse.Namespace(repo='example/project', run=10, archive_root=str(root / 'archive'),
                                                  source_root=str(root), cache_condition='unknown')), 'unfinished run cannot publish a receipt')
    m.api = original_api
print(f'ci-receipt smoke: {checks} assertions passed')
PY
