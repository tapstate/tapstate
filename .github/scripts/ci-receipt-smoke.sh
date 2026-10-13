#!/usr/bin/env bash
# Negative controls for persistent raw evidence, unsafe archives and provenance.
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
python3 - "$here" <<'PY'
import argparse
import copy
import os
import struct
from types import SimpleNamespace
import importlib.util
import io
import json
from pathlib import Path
import tempfile
import zipfile
import sys
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
    # First-attempt provenance is validated independently of successful conclusion.
    original_api = m.api
    m.api = lambda endpoint: {'id': 10, 'run_attempt': 1, 'status': 'in_progress', 'repository': {'full_name': 'example/project'}}
    rejected(lambda: m.collect(argparse.Namespace(repo='example/project', run=10, archive_root=str(root / 'archive'),
                                                  source_root=str(root), cache_condition='unknown')), 'unfinished run cannot publish a receipt')
    m.api = original_api
print(f'ci-receipt smoke: {checks} assertions passed')
PY
