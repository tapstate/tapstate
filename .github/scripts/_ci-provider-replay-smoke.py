#!/usr/bin/env python3
"""Active replay protocol controls use an explicit process-boundary recorder."""
import copy
import importlib.util
import io
import json
import os
import shutil
import signal
import time
from pathlib import Path
import subprocess
import sys
import tempfile
import zipfile

sys.dont_write_bytecode = True
HERE = Path(__file__).resolve().parent


def module(name, filename):
    spec = importlib.util.spec_from_file_location(name, HERE / filename)
    value = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(value)
    return value


fixture = module('replay_capture_fixture', '_ci-provider-readback-smoke.py')
r = module('active_provider_replay', '_ci-provider-replay.py')
g = r.g
CHECKS = 0


def check(ok, message):
    global CHECKS
    assert ok, message
    CHECKS += 1


def prepare(context):
    f = context['f']
    artifacts = f['artifacts']
    pre = artifacts['ci-inputs-rest']['build-inputs.json']
    post = artifacts['ci-inputs-rest']['post-test-inputs.json']
    capture = artifacts['ci-inputs-rest'][fixture.r.MEMBER]
    checkout = f['work']
    repository = f['root'] / 'replay-repository'
    repository.mkdir()
    binaries = {f['compiled']: fixture.class_header(fixture.CLASS.replace('.', '/')),
                f['prod']: fixture.class_header('io/tapstate/fixture/Product'),
                f['observer_class']: fixture.class_header('io/tapstate/testsupport/CiCaseEvidence'),
                context['helper']: fixture.class_header('io/tapstate/testsupport/CiProviderInventory'),
                fixture.COPIED_RESOURCE: fixture.RESOURCE_BYTES}
    for name, raw in binaries.items():
        path = checkout / name; path.parent.mkdir(parents=True, exist_ok=True); path.write_bytes(raw)
    output = io.BytesIO()
    with zipfile.ZipFile(output, 'w') as archive:
        archive.writestr('fixture.txt', 'protocol dependency, not a native provider fixture')
    dependency = repository / f['dep']
    dependency.parent.mkdir(parents=True); dependency.write_bytes(output.getvalue())
    for value in (pre, post):
        value['production_files'][f['prod']] = g.checksum(binaries[f['prod']])
        value['production_class_ids'][f['prod']] = g.receipt.crc64(binaries[f['prod']])
        value['production_class_access_flags'][f['prod']] = 0x21
        for name in value['test_files']:
            value['test_files'][name] = g.checksum(binaries[name])
            value['test_class_ids'][name] = g.receipt.crc64(binaries[name])
        value['test_resource_files'] = {fixture.COPIED_RESOURCE: g.checksum(fixture.RESOURCE_BYTES)}
        value['dependency_repository_files'] = {f['dep']: g.checksum(output.getvalue())}
        value['resolved_test_classpaths']['fixture'] = [{'identity': 'repository/' + f['dep'],
            'sha256': g.checksum(output.getvalue()), 'size': len(output.getvalue()), 'own_classes': {}}]
    aggregate = copy.deepcopy(pre)
    aggregate['runner_context']['shard_id'] = 'build'
    aggregate['runner_context']['workflow_source']['job'] = 'build'
    artifacts['ci-inputs-build']['build-inputs.json'] = aggregate
    equality = g.receipt.compare_input_snapshots(f['plan'], aggregate, {'rest': pre})
    equality['input_files'] = {'build': g.checksum(fixture.fmod.encode(aggregate)), 'rest': g.checksum(fixture.fmod.encode(pre))}
    artifacts['ci-inputs-build']['build-input-equivalence.json'] = equality
    capture['build_inputs_sha256'] = g.checksum(fixture.fmod.encode(pre))
    capture['helper_class_hashes'] = {name: checksum for name, checksum in pre['test_files'].items() if name.startswith('test-support/')}
    capture['invocations'][0]['helper_entry_hashes_sha256'] = fixture.r.canonical_hash(capture['helper_class_hashes'])
    capture['resolved_classpaths'] = copy.deepcopy(pre['resolved_test_classpaths'])
    capture['resource_hashes'][f['prod']] = pre['production_files'][f['prod']]
    manifest = artifacts['ci-test-results-rest']['rest/manifest.json']
    manifest['provenance']['case_observer']['build_inputs_sha256'] = g.checksum(fixture.fmod.encode(pre))
    marker = fixture.fmod.encode(manifest['provenance'])
    artifacts['ci-test-results-rest']['rest/files/' + f['marker']] = marker
    manifest['files'][f['marker']] = g.checksum(marker)
    artifacts['ci-aggregate']['target/ci-shards/rest/run.json'] = marker
    context['reseal']()
    request = {'schema': 1, 'type': 'provider-replay-request', 'worker': 'rest',
               'archive_inputs': f['save']('replay-inputs-request.json', f['request'])}
    request_path = f['evidence'] / 'replay-request.json'
    request_path.write_bytes(fixture.fmod.encode(request))
    java = f['root'] / 'java-boundary-recorder'
    java.write_text('#!' + sys.executable + '\n' + '''import hashlib,json,os,sys
from pathlib import Path
counter=Path(__file__).with_suffix('.calls')
with counter.open('a') as out:out.write('called\\n')
if any(key in os.environ for key in ('GH_TOKEN','GITHUB_TOKEN','CUSTOM_REPLAY_SECRET','JAVA_TOOL_OPTIONS','JDK_JAVA_OPTIONS','_JAVA_OPTIONS')):
    raise SystemExit(87)
requests,result=map(Path,sys.argv[-2:])
mode_file=Path(__file__).with_suffix('.mode')
mode=mode_file.read_text() if mode_file.exists() else 'valid'
if mode=='failure':raise SystemExit(9)
rows=[]
for line in requests.read_text().splitlines():
    classname,method,provider,kind=line.split('\\t')
    raw=(Path('target/test-classes')/(classname.replace('.','/')+'.class')).read_bytes()
    row={'schema':1,'class':classname,'test_method':method,'provider':provider,'kind':kind,
         'provider_class_sha256':hashlib.sha256(raw).hexdigest(),'cases':[{'stable':True,'argument_digest':str(i)*64,
          'first_string_digest':str(i)*64,'argument_descriptors':[{'position':0,'stable':True,'digest':str(i)*64}]} for i in (1,2)]}
    if mode=='wrong-case':row['cases'][0]['argument_digest']='3'*64
    if mode=='wrong-provider':row['provider']='unapproved'
    rows.append(row)
if mode=='empty':rows=[]
if mode=='duplicate':rows.append(rows[0])
result.write_text(''.join(json.dumps(row)+'\\n' for row in rows))
if mode=='change-reader':
    target=Path(Path(__file__).with_suffix('.reader-target').read_text());target.write_text(target.read_text()+'\\n# controlled reader mutation\\n')
if mode=='mutate':
    resource=Path('target/test-classes/corpus/golden.txt');resource.chmod(0o600);resource.write_text('changed')
''')
    java.chmod(0o755)
    return {'context': context, 'root': checkout, 'repository': repository, 'java': java,
            'request': request, 'request_path': request_path, 'pre': pre, 'capture': capture}


def execute(f, name, environment=None, script_root=None):
    output = f['context']['f']['evidence'] / (name + '.json')
    command = ['bash', str((script_root or HERE) / 'ci-provider-replay.sh'), '--manifest', str(f['request_path']),
        '--evidence-root', str(f['context']['f']['evidence']), '--root', str(f['root']),
        '--repo-local', str(f['repository']), '--java', str(f['java']), '--output', str(output)]
    result = subprocess.run(command, env={**os.environ, **(environment or {})}, capture_output=True, text=True)
    return result, output


def reader_change_control(f, controller_source=None):
    shadow=f['context']['f']['root']/'reader-copy'
    shutil.copytree(HERE,shadow,ignore=shutil.ignore_patterns('__pycache__'))
    if controller_source is not None:
        (shadow/'_ci-provider-replay.py').write_bytes(controller_source)
    target=shadow/'_ci-archive-cases.py'
    f['java'].with_suffix('.reader-target').write_text(str(target))
    f['java'].with_suffix('.mode').write_text('change-reader')
    result,output=execute(f,'reader-change',script_root=shadow)
    check(result.returncode!=0 and not output.exists() and 'reader code changed' in result.stderr,
          'executed transitive reader change must prevent receipt: '+result.stderr)
    f['java'].with_suffix('.mode').unlink()


def controls(context):
    f = prepare(context)
    counter = f['java'].with_suffix('.calls')
    before_tree = fixture.fmod.v.text(f['root'], 'rev-parse', 'HEAD^{tree}')
    result, output = execute(f, 'valid', {'GH_TOKEN': 'fixture-secret', 'CUSTOM_REPLAY_SECRET': 'fixture-secret',
                                       'JAVA_TOOL_OPTIONS': 'must-not-reach-child'})
    check(result.returncode == 0, 'active protocol command succeeds: ' + result.stderr)
    value = g.load(output)
    check(value['case_digest_multiset_reproduced'] and value['recorded_byte_hashes_equal']
          and value['method_count'] == 1 and value['case_count'] == 2, 'private staged bytes reproduce the original fixture case multiset')
    check(value['local_source_sha'] != value['original_binding']['source_sha'] and value['local_source_tree'] == before_tree,
          'local source identity stays distinct from original synthetic PR checkout identity')
    check(not value['historical_runtime_byte_equivalence_proven'] and not value['independent_provider_replay_proven']
          and not value['original_assertions_proven'] and not value['whole_build_provenance_complete']
          and not value['native_CI_proven'], 'a recorder process cannot certify native JVM/runtime/assertions/whole provenance')
    check(counter.read_text().splitlines() == ['called'], 'only the explicit bounded invocation runs after all byte checks')
    prior = output.read_bytes()
    result, unused = execute(f, 'valid')
    check(result.returncode != 0 and output.read_bytes() == prior and len(counter.read_text().splitlines()) == 1,
          'exclusive output fails before a repeated invocation')
    for label, path in [('production', f['root'] / context['f']['prod']),
                        ('test-resource', f['root'] / fixture.COPIED_RESOURCE),
                        ('provider', f['root'] / context['f']['compiled']),
                        ('dependency', f['repository'] / context['f']['dep'])]:
        raw = path.read_bytes(); start = counter.read_text()
        path.write_bytes(raw + b'changed')
        try:
            result, rejected_output = execute(f, 'wrong-' + label)
            check(result.returncode != 0 and not rejected_output.exists() and counter.read_text() == start
                  and 'local replay byte hash/size differs' in result.stderr, 'mismatched ' + label + ' cannot start the process')
        finally:
            path.write_bytes(raw)
    foreign = context['f']['root'] / 'foreign-identity'
    fixture.fmod.v.git(f['root'], 'clone', '-q', str(f['root']), str(foreign))
    fixture.fmod.v.git(foreign,'-c','user.name=Fixture','-c','user.email=fixture@example.test',
                       '-c','commit.gpgsign=false','commit','--allow-empty','-qm','Foreign same-tree identity')
    result, identity_output = execute(f,'inherited-git', {'GIT_DIR': str(foreign/'.git'), 'GIT_WORK_TREE': str(f['root'])})
    check(result.returncode == 0 and g.load(identity_output)['local_source_sha'] == fixture.fmod.v.text(f['root'],'rev-parse','HEAD'),
          'inherited Git state cannot replace the local source identity')
    for mode, reason in [('failure', 'bounded provider replay process failed'), ('empty', 'method set is incomplete'),
                         ('duplicate', 'duplicate or foreign replay result'), ('wrong-provider', 'source-approved request'),
                         ('wrong-case', 'case multiset differs'), ('mutate', 'changed staged bytes')]:
        f['java'].with_suffix('.mode').write_text(mode)
        result, rejected_output = execute(f, mode)
        check(result.returncode != 0 and not rejected_output.exists() and reason in result.stderr,
              mode + ' must fail for its intended replay boundary: ' + result.stderr)
    f['java'].with_suffix('.mode').unlink()
    reader_change_control(f)
    check(fixture.fmod.v.text(f['root'], 'rev-parse', 'HEAD^{tree}') == before_tree
          and not fixture.fmod.v.git(f['root'], 'status', '--porcelain', '--untracked-files=no').stdout,
          'active replay leaves source history and tracked bytes unchanged')


def local_controls():
    with tempfile.TemporaryDirectory(prefix='ci-replay-boundaries-') as temporary:
        base=Path(temporary);source=base/'source';destination=base/'staged'
        source.mkdir();destination.mkdir();(source/'dependency.jar').write_bytes(b'fixture')
        checksum=g.checksum(b'fixture');staged={};budget=[0]
        r.stage_file(source,'dependency.jar',checksum,destination,staged,budget,size=7)
        try:
            r.stage_file(source,'dependency.jar',checksum,destination,staged,budget,size=8)
        except ValueError as error:
            check('inconsistent repeated replay input' in str(error),'duplicate size rejects for its intended boundary')
        else:
            raise AssertionError('repeated staged identity must reject a conflicting recorded size')
        timeout_dir=base/'timeout';timeout_dir.mkdir();marker=base/'timeout.pid'
        code='import os,time;from pathlib import Path;Path('+repr(str(marker))+').write_text(str(os.getpid()));time.sleep(60)'
        try:
            r.bounded_command([sys.executable,'-c',code],base,timeout_dir,timeout=5)
        except ValueError as error:
            check('timed out' in str(error),'owned command observes its bounded deadline')
        else:
            raise AssertionError('bounded replay command must time out')
        check(marker.is_file(),'timeout control actually started its owned command')
        state=subprocess.run(['ps','-p',marker.read_text(),'-o','stat='],capture_output=True,text=True)
        check(not state.stdout.strip() or state.stdout.strip().startswith('Z'),'timeout leaves no live owned command')
        interrupt_dir=base/'interrupt';interrupt_dir.mkdir()
        code='import os,signal;os.kill('+str(os.getpid())+',signal.SIGTERM)'
        try:
            r.bounded_command([sys.executable,'-c',code],base,interrupt_dir,timeout=5)
        except ValueError as error:
            check('interrupted' in str(error),'parent interruption cannot become an exit-zero success')
        else:
            raise AssertionError('interrupted replay command cannot report success')
        log_dir=base/'log-budget';log_dir.mkdir()
        code='import sys;sys.stdout.buffer.write(b"x"*'+str(r.MAX_LOG_BYTES+1)+')'
        try:
            r.bounded_command([sys.executable,'-c',code],base,log_dir,timeout=5)
        except ValueError as error:
            check('log budget exceeded' in str(error),'owned command log budget remains bounded')
        else:
            raise AssertionError('oversized replay log cannot report success')


def writer_command(marker, result=None):
    writer=('import os,signal,time;from pathlib import Path;'
            'signal.signal(signal.SIGINT,signal.SIG_IGN);signal.signal(signal.SIGTERM,signal.SIG_IGN);'
            'signal.signal(signal.SIGPIPE,signal.SIG_IGN);Path('+repr(str(marker))+').write_text(str(os.getpid()));time.sleep(1);')
    writer+='\nfor _ in range(40):\n try:os.write(1,b"x"*200)\n except BrokenPipeError:pass\n'
    if result is not None:
        writer+=' try:\n  with open('+repr(str(result))+',"ab",buffering=0) as out:out.write(b"r"*200)\n except OSError:pass\n'
    writer+=' time.sleep(0.05)\n'
    return ('import subprocess,sys,time;from pathlib import Path;subprocess.Popen([sys.executable,"-c",'+repr(writer)+']);'
            '\nwhile not Path('+repr(str(marker))+').exists():time.sleep(0.01)\n')


def descendant_controls():
    with tempfile.TemporaryDirectory(prefix='ci-replay-descendants-') as temporary:
        base=Path(temporary);directory=base/'cleanup';directory.mkdir();marker=base/'writer.pid'
        saved=r.MAX_LOG_BYTES,r.MAX_RESULT_BYTES
        try:
            r.MAX_LOG_BYTES,r.MAX_RESULT_BYTES=1024,2048
            try:r.bounded_command([sys.executable,'-c',writer_command(marker)],base,directory)
            except ValueError as error:check('log budget exceeded' in str(error),'exited-parent writer is rejected for output budget')
            else:raise AssertionError('exited-parent writer cannot produce a success receipt')
        finally:r.MAX_LOG_BYTES,r.MAX_RESULT_BYTES=saved
        check((directory/'stdout').stat().st_size<=1024,'descendant cleanup never expands stdout beyond the hard collector limit')
        state=subprocess.run(['ps','-p',marker.read_text(),'-o','stat='],capture_output=True,text=True)
        check(not state.stdout.strip() or state.stdout.strip().startswith('Z'),'exited-parent writer is no longer live')
        lost=base/'parent-loss';lost.mkdir();marker=base/'lost-writer.pid';result=lost/'result.jsonl';success=base/'unexpected-success'
        code=writer_command(marker,result)
        driver=('import importlib.util,sys;from pathlib import Path;sys.dont_write_bytecode=True;'
                'spec=importlib.util.spec_from_file_location("lost_replay",'+repr(str(HERE/'_ci-provider-replay.py'))+');'
                'r=importlib.util.module_from_spec(spec);spec.loader.exec_module(r);r.MAX_LOG_BYTES=1024;r.MAX_RESULT_BYTES=2048;'
                'r.bounded_command([sys.executable,"-c",'+repr(code)+'],'+repr(str(base))+',Path('+repr(str(lost))+'));'
                'Path('+repr(str(success))+').write_text("unexpected")')
        controller=subprocess.Popen([sys.executable,'-c',driver],stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        try:
            deadline=time.monotonic()+10
            while not marker.exists() and controller.poll() is None and time.monotonic()<deadline:time.sleep(0.01)
            check(marker.exists() and controller.poll() is None,'parent-loss control reached its owned descendant barrier')
            controller.kill();controller.communicate(timeout=30)
            check(not success.exists() and (lost/'stdout').stat().st_size<=1024,'caller loss cannot publish success or expand inherited stdout')
            check(result.exists() and result.stat().st_size==2048,'inherited file-size limit survives caller loss for result writers')
            state=subprocess.run(['ps','-p',marker.read_text(),'-o','stat='],capture_output=True,text=True)
            check(not state.stdout.strip() or state.stdout.strip().startswith('Z'),'caller-loss writer is no longer live')
        finally:
            if controller.poll() is None:controller.kill()
            controller.communicate(timeout=30)


def main():
    local_controls()
    descendant_controls()
    workflow = (HERE.parent / 'workflows/ci.yml').read_text()
    contract = {'schema': 1, 'classes': {fixture.CLASS: {'module': 'fixture', 'source': fixture.SOURCE,
        'source_sha256': g.checksum(fixture.CODE.encode()), 'providers': {'values': {'kind': 'source', 'projection': 'strict'}}}}}
    def capture(f):
        fixture.controls(f, callback=controls, only_callback=True)
    fixture.fmod.run(callback=capture, only_callback=True, source_case=fixture.CODE, workflow_sources={'ci.yml': workflow},
        extra_source_files={fixture.CONTRACT: json.dumps(contract), fixture.PRODUCER: (HERE / '_ci-evidence.py').read_bytes(),
                            fixture.RESOURCE: fixture.RESOURCE_BYTES})
    print('CI active provider replay protocol controls passed:', CHECKS)


if __name__ == '__main__':
    main()
