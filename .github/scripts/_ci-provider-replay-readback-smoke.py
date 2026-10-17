#!/usr/bin/env python3
"""Hash-only local replay receipt joins, without JVM invocation by the consumer."""
import copy
import importlib.util
import json
from pathlib import Path
import subprocess
import sys

sys.dont_write_bytecode=True
HERE=Path(__file__).resolve().parent


def module(name,filename):
    spec=importlib.util.spec_from_file_location(name,HERE/filename)
    value=importlib.util.module_from_spec(spec);spec.loader.exec_module(value)
    return value


producer=module('passive_replay_fixture','_ci-provider-replay-smoke.py')
r=module('passive_replay_reader','_ci-provider-replay-readback.py')
g=r.g
CHECKS=0


def check(ok,message):
    global CHECKS
    assert ok,message
    CHECKS+=1


def controls(context):
    f=producer.prepare(context)
    result,path=producer.execute(f,'audited-replay')
    check(result.returncode==0,'active fixture produced its original receipt: '+result.stderr)
    value=g.load(path);original=copy.deepcopy(value)
    request={'schema':1,'type':'provider-replay-readback-request','replay':{
        'path':path.relative_to(context['f']['evidence']).as_posix(),'sha256':g.checksum(path.read_bytes())}}
    root=context['f']['evidence']
    counter=f['java'].with_suffix('.calls');calls=counter.read_bytes()
    f['java'].unlink()
    for field in ('production_files','test_files','test_resource_files'):
        for name in f['pre'][field]:(f['root']/name).unlink()
    for source in f['repository'].rglob('*'):
        if source.is_file():source.unlink()

    def project():return r.audit(g.References(root),f['root'],request)
    def reseal():
        path.write_bytes(producer.fixture.fmod.encode(value))
        request['replay']['sha256']=g.checksum(path.read_bytes())
    def rejected(change,reason):
        nonlocal value
        change(value);reseal()
        try:project()
        except (ValueError,OSError,KeyError,TypeError) as error:
            check(reason in str(error),'wrong passive replay rejection: '+str(error))
        else:raise AssertionError('accepted forged local replay record: '+reason)
        value=copy.deepcopy(original);reseal()

    answer=project()
    check(answer['receipt_reference_chain_consistent'] and answer['original_capture_case_multisets_equal']
          and answer['method_count']==1 and answer['case_count']==2,'independent projection joins original request, capture and per-case result records')
    check(answer['local_result_records_read_back'] and not answer['local_result_bytes_read_back']
          and not answer['local_process_execution_proven'] and not answer['local_stage_byte_attestation_proven']
          and not answer['active_reader_code_bytes_read_back'] and not answer['whole_build_provenance_complete'],
          'data consistency leaves local execution, historical bytes and whole provenance unknown')
    rejected(lambda value:value.update(worker='engine'),'worker differs')
    rejected(lambda value:value['original_scope'].update(run_id=999),'original run/source/input binding differs')
    rejected(lambda value:value['original_binding'].update(source_sha='f'*40),'original run/source/input binding differs')
    rejected(lambda value:value.update(original_receipt_sha256='f'*64),'original run/source/input binding differs')
    rejected(lambda value:value.update(prepared_inputs_sha256='f'*64),'original run/source/input binding differs')
    rejected(lambda value:value['request'].update(sha256='f'*64),'referenced input hash differs')
    rejected(lambda value:value['input_files'].clear(),'input reference chain differs')
    rejected(lambda value:value.update(local_source_tree='f'*40),'local commit tree differs')
    rejected(lambda value:value.update(staged_bytes=True),'invalid provider replay staged-byte count')
    rejected(lambda value:value['reader_code_hashes'].pop('_ci-archive-cases.py'),'reader version set differs')
    rejected(lambda value:value['invocations'][0]['result_records'][0]['cases'][0].update(argument_digest='3'*64),'case multiset differs')
    rejected(lambda value:value['invocations'][0].update(result_records=[]),'method set is incomplete')
    rejected(lambda value:value['invocations'][0]['result_records'].append(copy.deepcopy(value['invocations'][0]['result_records'][0])),
             'duplicate or foreign replay result')
    rejected(lambda value:value['invocations'][0]['case_multisets'][0].update(case_digests=['f'*64]),'result detail differs')
    rejected(lambda value:value['invocations'][0].update(result_records_sha256='f'*64),'result detail differs')
    rejected(lambda value:value['invocations'][0].update(original_result_records_hash_equal=True),'original result-hash declaration differs')
    rejected(lambda value:value['invocations'][0].update(inherited_environment=True),'invalid provider replay invocation declaration')
    rejected(lambda value:value['invocations'][0].update(elapsed_ns=False),'invalid provider replay invocation declaration')
    rejected(lambda value:value['invocations'][0].update(method_count=2),'result detail differs')
    rejected(lambda value:value.update(case_count=3),'summary counts differ')
    rejected(lambda value:value.update(ability_proven=True),'unsupported strong proof')
    rejected(lambda value:value['invocations'][0].pop('result_records'),'invalid provider replay invocation declaration')
    check(counter.read_bytes()==calls,'all passive reads and rejections execute no additional process')
    manifest=root/'readback-request.json';manifest.write_bytes(producer.fixture.fmod.encode(request))
    output=root/'readback-output.json'
    command=['bash',str(HERE/'ci-provider-replay-readback.sh'),'--manifest',str(manifest),
        '--evidence-root',str(root),'--root',str(f['root']),'--output',str(output)]
    result=subprocess.run(command,capture_output=True,text=True)
    check(result.returncode==0 and g.load(output)['case_count']==2,'actual passive CLI succeeds with build inputs and Java executable removed: '+result.stderr)
    prior=output.read_bytes()
    result=subprocess.run(command,capture_output=True,text=True)
    check(result.returncode!=0 and output.read_bytes()==prior,'passive CLI preserves an existing evidence file')


def main():
    fixture=producer.fixture
    workflow=(HERE.parent/'workflows/ci.yml').read_text()
    contract={'schema':1,'classes':{fixture.CLASS:{'module':'fixture','source':fixture.SOURCE,
        'source_sha256':g.checksum(fixture.CODE.encode()),'providers':{'values':{'kind':'source','projection':'strict'}}}}}
    def capture(f):fixture.controls(f,callback=controls,only_callback=True)
    fixture.fmod.run(callback=capture,only_callback=True,source_case=fixture.CODE,workflow_sources={'ci.yml':workflow},
        extra_source_files={fixture.CONTRACT:json.dumps(contract),fixture.PRODUCER:(HERE/'_ci-evidence.py').read_bytes(),fixture.RESOURCE:fixture.RESOURCE_BYTES})
    print('CI passive provider replay readback controls passed:',CHECKS)


if __name__=='__main__':main()
