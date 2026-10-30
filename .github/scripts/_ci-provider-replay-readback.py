#!/usr/bin/env python3
"""Passively audit local hash-only provider result records against original captures.

Record consistency cannot attest a local process, a historical runtime or original
assertions. This reader never invokes the active replay command or any JVM.
"""
import argparse
import importlib.util
from pathlib import Path
import sys

sys.dont_write_bytecode = True
HERE=Path(__file__).resolve().parent
spec=importlib.util.spec_from_file_location('replay_audit_inputs',HERE/'_ci-archive-inputs.py')
inputs=importlib.util.module_from_spec(spec);spec.loader.exec_module(inputs)
g,require=inputs.g,inputs.require
records_reader=g.module('replay_audit_records','_ci-provider-readback.py')
variants=inputs.source.variants
FALSE_FLAGS={'historical_runtime_byte_equivalence_proven','independent_provider_replay_proven',
             'original_assertions_proven','whole_build_provenance_complete','ability_proven',
             'native_CI_proven','first_complete_PR_sample','performance_accepted'}
TOP_FIELDS={'schema','type','request','original_scope','worker','original_receipt_sha256','original_binding',
            'prepared_inputs_sha256','local_source_sha','local_source_tree','staged_bytes',
            'recorded_byte_hashes_equal','staged_inputs_unchanged','invocations','method_count','case_count',
            'case_digest_multiset_reproduced','java_executable_sha256','reader_code_hashes','input_files'}|FALSE_FLAGS
CALL_FIELDS={'module','exit_code','elapsed_ns','started_at','ended_at','stdout_sha256','stderr_sha256',
             'command_sha256','termination_scope','inherited_environment','method_count','case_count',
             'result_sha256','result_records_sha256','case_multisets','result_records','original_result_records_hash_equal'}


def audit(refs,root,request):
    require(isinstance(request,dict) and set(request)=={'schema','type','replay'} and type(request['schema']) is int
            and request['schema']==1 and request['type']=='provider-replay-readback-request','invalid provider replay readback request')
    value,unused=refs.read(request['replay'])
    require(isinstance(value,dict) and set(value)==TOP_FIELDS and type(value['schema']) is int and value['schema']==1
            and value['type']=='provider-byte-replay','provider replay detail is missing or has an invalid schema')
    require(all(value[key] is False for key in FALSE_FLAGS),'provider replay claims unsupported strong proof')
    require(all(value[key] is True for key in ('recorded_byte_hashes_equal','staged_inputs_unchanged','case_digest_multiset_reproduced')),
            'provider replay success declarations differ')
    chain=g.References(refs.root)
    original_request,unused=chain.read(value['request'])
    require(isinstance(original_request,dict) and set(original_request)=={'schema','type','archive_inputs','worker'}
            and type(original_request['schema']) is int and original_request['schema']==1
            and original_request['type']=='provider-replay-request','replay receipt is not bound to its original request')
    input_request,unused=chain.read(original_request['archive_inputs'])
    basis=inputs.project(chain,root,input_request)
    worker=value['worker']
    require(worker==original_request['worker'] and worker in basis.get('provider_captures',{}),'replay worker differs from original request or capture')
    source_request,unused=chain.read(input_request['source'])
    archive=inputs.Archive(chain,source_request['archive'],basis['scope'])
    members=archive.artifact('ci-inputs-'+worker)
    before=g.json_bytes(members['build-inputs.json'])
    capture=g.json_bytes(members['provider-probe-receipt.json'])
    require(value['original_scope']==basis['scope'] and value['original_binding']==capture['binding']
            and value['original_receipt_sha256']==basis['provider_captures'][worker]['receipt_sha256']
            and value['prepared_inputs_sha256']==g.checksum(members['build-inputs.json']),'provider replay original run/source/input binding differs')
    require('test_resource_files' in before and basis['provider_captures'][worker]['prepared_resource_hashes_complete'],'provider replay original resource inputs are incomplete')
    local=g.receipt.sha(value['local_source_sha'],'local replay source')
    variants.commit_sha(root,local)
    require(value['local_source_tree']==before['source_tree']==variants.text(root,'rev-parse',local+'^{tree}'),
            'provider replay local commit tree differs from original checkout')
    require(type(value['staged_bytes']) is int and 0<value['staged_bytes']<=2*1024*1024*1024,'invalid provider replay staged-byte count')
    records_reader.digest(value['java_executable_sha256'])
    hashes=value['reader_code_hashes']
    required={'_ci-provider-replay.py','_ci-provider-command.py','_ci-archive-inputs.py','_ci-archive-cases.py',
              '_ci-provider-readback.py','_ci-pr-groups.py','_ci-receipt.py','_ci-evidence.py','_ci-source-projection.py',
              '_ci-variants.py','_ci-shards.py','_ci-pr-work.py','_ci-owned-command.py'}
    require(isinstance(hashes,dict) and set(hashes)==required,'provider replay reader version set differs')
    for checksum in hashes.values():records_reader.digest(checksum)
    calls=value['invocations']
    require(isinstance(calls,list) and len(calls)==len(capture['invocations'])>0,'provider replay invocation set differs')
    method_count,case_count=0,0
    reconstructed=[]
    for original,call in zip(capture['invocations'],calls):
        require(isinstance(call,dict) and set(call)==CALL_FIELDS and call['module']==original['module']
                and type(call['exit_code']) is int and call['exit_code']==0
                and type(call['elapsed_ns']) is int and call['elapsed_ns']>=0
                and call['termination_scope']=='owned-held-process-group' and call['inherited_environment'] is False,
                'invalid provider replay invocation declaration')
        require(g.timestamp(call['started_at'])<=g.timestamp(call['ended_at']),'invalid provider replay invocation timeline')
        for key in ('stdout_sha256','stderr_sha256','command_sha256','result_sha256','result_records_sha256'):
            records_reader.digest(call[key])
        checked=records_reader.reproduce_records(call['result_records'],original,capture,before)
        require(all(type(call[key]) is int and call[key]==checked[key] for key in ('method_count','case_count'))
                and call['case_multisets']==checked['case_multisets']
                and call['result_records_sha256']==checked['result_records_sha256'], 'provider replay result detail differs from independent projection')
        require(type(call['original_result_records_hash_equal']) is bool and call['original_result_records_hash_equal']
                ==(checked['result_records_sha256']==original['result_records_sha256']),'provider replay original result-hash declaration differs')
        original_methods={(row['class'],row['method']):row for row in capture['resolved_methods'] if row['module']==call['module']}
        for row in call['result_records']:
            method=original_methods[(row['class'],row['test_method'])]
            mapping=method['mapping']
            field={'dynamic-marker':'display_name_digest','named-carrier-marker':'first_string_digest',
                   'argument-marker':'argument_digest'}[mapping]
            parameters=sorted(case[field] for case in row['cases'])
            projected={key:method[key] for key in ('lane','module','kind','class','method','source','mapping','provider_contract')}
            projected.update(parameters=parameters,argument_digests=parameters if mapping=='argument-marker' else None)
            if mapping=='named-carrier-marker':projected['projection_types']=['java.lang.String','java.util.function.BiFunction']
            reconstructed.append(projected)
        method_count+=checked['method_count'];case_count+=checked['case_count']
    require(type(value['method_count']) is int and value['method_count']==method_count and type(value['case_count']) is int
            and value['case_count']==case_count,'provider replay summary counts differ')
    chain_files=chain.finish()
    require(value['input_files']==chain_files,'provider replay original input reference chain differs')
    for name,entry in chain_files.items():refs.blob({'path':name,'sha256':entry['sha256']})
    return {'schema':1,'type':'provider-replay-readback','original_scope':basis['scope'],'worker':worker,
            'replay_receipt_sha256':request['replay']['sha256'],
            'reconstructed_methods':sorted(reconstructed,key=lambda row:tuple(row[key] for key in ('lane','module','kind','class','method'))),
            'method_count':method_count,'case_count':case_count,'original_capture_case_multisets_equal':True,
            'local_result_records_read_back':True,'receipt_reference_chain_consistent':True,
            'local_process_execution_proven':False,'local_stage_byte_attestation_proven':False,
            'local_result_bytes_read_back':False,'active_reader_code_bytes_read_back':False,
            **{key:False for key in FALSE_FLAGS},'input_files':refs.finish()}


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    for key in ('manifest','evidence-root','root','output'):parser.add_argument('--'+key,required=True)
    args=parser.parse_args()
    value=audit(g.References(args.evidence_root),args.root,g.load(Path(args.manifest)))
    g.write_new(Path(args.output),value)
    print('provider replay records independently read back; local execution and whole CI remain unproven')


if __name__=='__main__':
    try:main()
    except (ValueError,OSError,KeyError,TypeError) as error:
        print('provider replay readback rejected: '+str(error),file=sys.stderr)
        sys.exit(1)
