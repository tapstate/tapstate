#!/usr/bin/env python3
"""Offline provider receipt joins over real Git/ZIP data, without provider execution."""
import copy
from contextlib import redirect_stdout
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import struct
import subprocess
import sys
from types import SimpleNamespace

sys.dont_write_bytecode = True
for key in ('GIT_DIR', 'GIT_WORK_TREE', 'GIT_INDEX_FILE', 'GIT_PREFIX', 'GIT_QUARANTINE_PATH'):
    os.environ.pop(key, None)
HERE = Path(__file__).resolve().parent


def load(name, filename):
    spec = importlib.util.spec_from_file_location(name, HERE / filename)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


fmod = load('provider_archive_fixture', '_ci-archive-inputs-smoke.py')
r = load('provider_archive_reader', '_ci-provider-readback.py')
g, ev = r.g, r.ev
CHECKS = 0
CLASS = 'io.tapstate.fixture.SampleTest'
SOURCE = 'fixture/src/test/java/io/tapstate/fixture/SampleTest.java'
CONTRACT = '.github/scripts/ci-provider-contracts.json'
PRODUCER = '.github/scripts/_ci-evidence.py'
RESOURCE = 'fixture/src/test/resources/corpus/golden.txt'
COPIED_RESOURCE = 'fixture/target/test-classes/corpus/golden.txt'
RESOURCE_BYTES = b'original resource protocol fixture\n'
CODE = '''package io.tapstate.fixture;
class SampleTest {
  @Test void holds() {}
  @ParameterizedTest @MethodSource("values") void computed(int value) {}
  static Stream<Arguments> values() {
    return java.util.stream.IntStream.range(0, 2).mapToObj(index -> Arguments.of(index));
  }
}
'''


def check(ok, message):
    global CHECKS
    assert ok, message
    CHECKS += 1


def rejected(call, message):
    try:
        call()
    except (ValueError, OSError, KeyError, TypeError) as error:
        check(message in str(error), 'wrong rejection: ' + str(error))
    else:
        raise AssertionError('expected rejection: ' + message)


def class_header(name):
    # A minimal class-file header exercises the passive class identity reader.
    # It has no executable methods and is never loaded by a JVM.
    def utf8(value):
        raw = value.encode()
        return b'\x01' + struct.pack('>H', len(raw)) + raw
    return (b'\xca\xfe\xba\xbe' + struct.pack('>HHH', 0, 65, 5)
            + utf8(name) + b'\x07\x00\x01' + utf8('java/lang/Object') + b'\x07\x00\x03'
            + struct.pack('>HHHHHHH', 0x21, 2, 4, 0, 0, 0, 0))


def combined_log_controls(context):
    logs, reset = context['logs'], context['reset']
    step_name = context['log_name']
    job = context['pages']['jobs'][0]
    capture_raw = fmod.encode(context['artifacts']['ci-inputs-rest'][r.MEMBER])
    receipt_sha = g.checksum(capture_raw)
    invocations = context['artifacts']['ci-inputs-rest'][r.MEMBER]['invocations']
    name = '4_' + job['name'] + '.txt'
    raw = (b'\xef\xbb\xbf2026-10-14T09:59:00Z Runner setup\n' + logs[step_name]
           + b'2026-10-14T10:00:02Z Job cleanup\n')
    combined = {name: raw}

    def bind(values):
        return r.step_binding(None, 'rest', [job], values, receipt_sha, invocations)

    bound = bind(combined)
    check(bound['job_id'] == job['id'] and bound['step_number'] == 9 and bound['log_member'] == name
          and bound['log_member_sha256'] == g.checksum(raw) and bound['archived_step_log_binding_consistent'],
          'combined original job log keeps its full raw hash and API step binding')
    for foreign in ('combined-job.txt', '4_test-shards (engine).txt', '-1_' + job['name'] + '.txt',
                    '04_' + job['name'] + '.txt', 'nested/' + name):
        rejected(lambda: bind({foreign: raw}), 'original provider step log member missing')
    rejected(lambda: bind({name: raw, '8_' + job['name'] + '.txt': raw}), 'original provider job log member ambiguous')
    rejected(lambda: bind({name: raw.replace(receipt_sha.encode(), b'f' * 64)}), 'log marker missing, duplicated or different')
    rejected(lambda: bind({name: raw + logs[step_name]}), 'log marker missing, duplicated or different')
    rejected(lambda: bind({name: raw.replace(b'10:00:00.3000000', b'10:00:09.3000000')}), 'marker outside original step')
    rejected(lambda: bind({name: raw.replace(b'CI_PROVIDER_PROBE_RECEIPT_SHA256 ', b'CI_PROVIDER_PROBE_RECEIPT_SHA256 malformed ')}),
             'malformed provider receipt log marker')
    rejected(lambda: bind({name: b'2026-10-14T10:00:00Z No receipt marker\n'}), 'log marker missing, duplicated or different')
    rejected(lambda: bind({**combined, step_name: logs[step_name].replace(receipt_sha.encode(), b'f' * 64)}),
             'log marker missing, duplicated or different')
    logs.clear()
    logs.update(combined)
    context['reseal'](False)
    answer = context['project']()
    found = answer['provider_captures']['rest']
    check(found['step'] == bound and found['receipt_hash_links_consistent']
          and not found['independent_provider_replay_proven'] and not answer['whole_input_projection_verified'],
          'full archive chain accepts a combined job log without upgrading provenance or provider semantics')
    reset()


def controls(f, callback=None, only_callback=False):
    artifacts = f['artifacts']
    pre = artifacts['ci-inputs-rest']['build-inputs.json']
    post = artifacts['ci-inputs-rest']['post-test-inputs.json']
    binary = class_header(CLASS.replace('.', '/'))
    pre['test_files'][f['compiled']] = g.checksum(binary)
    pre['test_class_ids'][f['compiled']] = g.receipt.crc64(binary)
    helper = 'test-support/target/test-classes/io/tapstate/testsupport/CiProviderInventory.class'
    for value in (pre, post):
        value['test_files'][helper] = g.checksum(b'helper protocol fixture')
        value['test_class_ids'][helper] = g.receipt.crc64(b'helper protocol fixture')
    post['test_files'][f['compiled']] = pre['test_files'][f['compiled']]
    post['test_class_ids'][f['compiled']] = pre['test_class_ids'][f['compiled']]
    context = {'repository': f['scope']['repository'], 'shard_id': 'rest', 'workflow_source': {
        'snapshot': 'captured', 'workflow_ref': ('example/project/.github/workflows/ci.yml@refs/pull/7/merge'
            if f['scope']['event'] == 'pull_request' else 'example/project/.github/workflows/ci.yml@refs/heads/ws/fixture'),
        'workflow_sha': f['merge'], 'workflow_path': '.github/workflows/ci.yml', 'job': 'test-shards',
        'source_bytes_verified': False}}
    pre['runner_context'] = copy.deepcopy(context)
    post['runner_context'] = copy.deepcopy(context)
    aggregate = copy.deepcopy(pre)
    aggregate['runner_context']['shard_id'] = 'build'
    aggregate['runner_context']['workflow_source']['job'] = 'build'
    equality = g.receipt.compare_input_snapshots(f['plan'], aggregate, {'rest': pre})
    equality['input_files'] = {'build': g.checksum(fmod.encode(aggregate)), 'rest': g.checksum(fmod.encode(pre))}
    artifacts['ci-inputs-build'] = {'build-inputs.json': aggregate, 'build-input-equivalence.json': equality}
    provenance = copy.deepcopy(f['provenance'])
    provenance['case_observer']['build_inputs_sha256'] = g.checksum(fmod.encode(pre))
    files = {**f['result_files'], f['compiled']: binary, f['marker']: fmod.encode(provenance), COPIED_RESOURCE: RESOURCE_BYTES}
    manifest = copy.deepcopy(f['manifest'])
    manifest.update(provenance=provenance, files={name: g.checksum(raw) for name, raw in files.items()})
    artifacts['ci-test-results-rest'] = {'rest/manifest.json': manifest,
                                        **{'rest/files/' + name: raw for name, raw in files.items()}}
    artifacts['ci-aggregate']['target/ci-shards/rest/run.json'] = fmod.encode(provenance)

    # Derive the pre-resolution source document directly, independently of the
    # archive reader's reconstruction. The temporary checkout has the same tree.
    inv_path = f['root'] / 'direct-inventory.json'
    with redirect_stdout(io.StringIO()):
        ev.inventory(SimpleNamespace(root=str(f['work']), source_archive=None, source_receipt=None, lane='ci',
            parameters=None, kind=None, published_connectors='absent', native_cli_presence='absent', output=str(inv_path)))
    inv = ev.load(inv_path)
    inv.update(source_sha=f['merge'], source_origin={'kind': 'checkout', 'source_sha': f['merge']})
    original_sha = g.checksum((json.dumps(inv, indent=2, sort_keys=True) + '\n').encode())
    check(any(row['method'] == 'computed' and row['parameters'] is None for row in inv['methods']),
          'archive fixture contains an actual computed source gap')
    provider_contract = {'source_sha256': g.checksum(CODE.encode()), 'provider': 'values',
                         'provider_class_sha256': g.checksum(binary)}
    case_digests = [hashlib.sha256(json.dumps(['ci', 'fixture', 'unit', CLASS, 'computed', parameter],
                    separators=(',', ':')).encode()).hexdigest() for parameter in ('1' * 64, '2' * 64)]
    identity = {'lane': 'ci', 'module': 'fixture', 'kind': 'unit', 'class': CLASS, 'method': 'computed'}
    method = {**identity, 'source': SOURCE, 'mapping': 'argument-marker', 'provider_contract': provider_contract,
              'case_digests': sorted(case_digests)}
    helper_map = {name: checksum for name, checksum in pre['test_files'].items() if name.startswith('test-support/')}
    requested = {'class': CLASS, 'method': 'computed', 'kind': 'unit', 'provider': 'values', 'provider_kind': 'source'}
    call = {'module': 'fixture', 'requested_methods': [requested], 'requested_method_count': 1,
            'request_sha256': g.checksum((CLASS + '\tcomputed\tvalues\tsource\n').encode()),
            **{key: 'a' * 64 for key in ('result_sha256', 'helper_jar_sha256', 'stdout_sha256', 'stderr_sha256',
                                       'java_command_sha256', 'result_records_sha256')},
            'helper_entry_hashes_sha256': r.canonical_hash(helper_map), 'exit_code': 0, 'elapsed_ns': 1000,
            'started_at': '2026-10-14T10:00:00.100000+00:00', 'ended_at': '2026-10-14T10:00:00.200000+00:00',
            'result_record_count': 1}
    capture = {'schema': 1, 'type': 'provider-probe-receipt',
        'binding': {key: pre[key] for key in ('source_sha', 'source_tree', 'run_id', 'run_attempt', 'event')},
        'runner_context': context, 'lane': 'ci', 'original_source_sha': f['merge'], 'original_inventory_sha256': original_sha,
        'producer_script_sha256': pre['source_files'][PRODUCER], 'producer_prepared_source_bound': True,
        'build_inputs_sha256': g.checksum(fmod.encode(pre)), 'contracts': {'path': CONTRACT, 'sha256': pre['source_files'][CONTRACT]},
        'helper_class_hashes': helper_map, 'resolved_classpaths': copy.deepcopy(pre['resolved_test_classpaths']), 'resource_hashes': {f['compiled']: g.checksum(binary), f['prod']: pre['production_files'][f['prod']],
                            RESOURCE: g.checksum(RESOURCE_BYTES), COPIED_RESOURCE: g.checksum(RESOURCE_BYTES)},
        'invocations': [call], 'resolved_methods': [method], 'resolved_method_count': 1, 'resolved_case_count': 2,
        'capture_status': 'success', 'inventory_complete': True, 'before_test_selection': True, 'project_original': False,
        'independent_provider_replay_proven': False, 'ability_proven': False, 'whole_build_provenance_complete': False}
    expected = {'schema': 1, 'type': 'expected-case-hashes', 'source_sha': f['merge'], 'complete': True,
        'original_inventory_sha256': original_sha, 'methods': [{**identity, 'provider_contract_sha256': r.canonical_hash(provider_contract)}],
        'cases': [{**identity, 'case_digest': value} for value in case_digests], 'ability_proven': False}
    target = artifacts['ci-inputs-rest']
    target[r.MEMBER] = capture
    target['expected-case-hashes.json'] = expected
    step = {'name': r.STEP, 'number': 9, 'status': 'completed', 'conclusion': 'success',
            'started_at': '2026-10-14T10:00:00Z', 'completed_at': '2026-10-14T10:00:01Z'}
    later = {**step, 'name': r.EXECUTE, 'number': 10, 'started_at': '2026-10-14T10:00:01Z', 'completed_at': '2026-10-14T10:00:02Z'}
    pages = {'total_count': 1, 'jobs': [{'id': 401, 'run_id': f['scope']['run_id'], 'run_attempt': 1,
             'name': 'test-shards (rest)', 'status': 'completed', 'steps': [step, later]}]}
    log_name = 'test-shards (rest)/9_' + r.STEP + '.txt'
    extras = f['archive_extra_files']
    extras['attempt-1/jobs-1.json'] = pages
    logs = {}

    def reseal(update=True):
        if update:
            raw = fmod.encode(artifacts['ci-inputs-rest'][r.MEMBER])
            artifacts['ci-inputs-rest']['expected-case-hashes.json']['provider_probe_receipt_sha256'] = g.checksum(raw)
            logs[log_name] = ('2026-10-14T10:00:00.3000000Z CI_PROVIDER_PROBE_RECEIPT_SHA256 ' + g.checksum(raw) + '\n').encode()
        extras['attempt-1/logs.zip'] = fmod.zipped(logs)
        f['reseal']()

    reseal()
    original = copy.deepcopy(artifacts), copy.deepcopy(extras), copy.deepcopy(logs)

    def reset():
        artifacts.clear(); artifacts.update(copy.deepcopy(original[0]))
        extras.clear(); extras.update(copy.deepcopy(original[1]))
        logs.clear(); logs.update(copy.deepcopy(original[2]))
        reseal()

    def project():
        return fmod.m.project(g.References(f['evidence']), f['work'], f['request'])

    if callback is not None:
        callback(locals())
        if only_callback:
            return
    answer = project()
    found = answer['provider_captures']['rest']
    check(found['receipt_hash_links_consistent'] and found['step']['archived_step_log_binding_consistent'], 'original step marker joins the actual receipt member')
    check(found['provider_class_bytes_read_back'] and not found['helper_class_bytes_read_back'], 'raw provider header is joined while absent helper bytes stay unknown')
    check(found['resources_without_prepared_hash']==[COPIED_RESOURCE] and not found['prepared_resource_hashes_complete']
          and 'provider_prepared_resource_hashes' in answer['unverified_components'], 'copied test resource is read without inventing a prepared-input hash')
    check(found['method_count'] == 1 and found['case_count'] == 2 and found['workflow_source_is_archived_checkout'], 'source, method and case counts join original checkout')
    check(not found['provider_result_bytes_read_back'] and not found['independent_provider_replay_proven']
          and not found['ability_proven'] and not answer['whole_input_projection_verified'], 'protocol joins cannot certify provider semantics, original execution or whole provenance')
    combined_log_controls(locals())
    before = fmod.v.text(f['work'], 'rev-parse', 'HEAD'), fmod.v.git(f['work'], 'write-tree').stdout

    def edit(change, message):
        change(artifacts['ci-inputs-rest'][r.MEMBER])
        reseal(); rejected(project, message); reset()

    edit(lambda value: value['binding'].update(run_id='999'), 'capture source/run/tree differs')
    edit(lambda value: value.update(producer_script_sha256='f' * 64), 'producer/prepared input byte binding differs')
    edit(lambda value: value.update(original_inventory_sha256='f' * 64), 'expected identities are not bound')
    artifacts['ci-inputs-rest'][r.MEMBER]['original_inventory_sha256'] = 'f' * 64
    artifacts['ci-inputs-rest']['expected-case-hashes.json']['original_inventory_sha256'] = 'f' * 64
    reseal(); rejected(project, 'independent source replay'); reset()
    edit(lambda value: value['contracts'].update(sha256='f' * 64), 'contract source is unbound')
    edit(lambda value: value['helper_class_hashes'].update({helper: 'f' * 64}), 'helper class set differs')
    edit(lambda value: value['invocations'][0].update(exit_code=1), 'invocation unsuccessful or partial')
    edit(lambda value: value['invocations'][0].update(request_sha256='f' * 64), 'request TSV hash differs')
    edit(lambda value: value['invocations'][0].update(helper_entry_hashes_sha256='f' * 64), 'helper invocation digest differs')
    edit(lambda value: value['invocations'][0].update(result_record_count=0), 'invocation unsuccessful or partial')
    edit(lambda value: value['invocations'][0].update(ended_at='2026-10-14T10:00:08Z'), 'invocation outside original step')
    edit(lambda value: value['invocations'][0].update(ended_at='2026-10-14T10:00:00.400000Z'), 'invocation ends after its receipt marker')
    edit(lambda value: value.update(invocations=[], resolved_methods=[], resolved_method_count=0, resolved_case_count=0, capture_status='not-required', resolved_classpaths={}), 'omits or adds an original computed method')
    edit(lambda value: value['resolved_classpaths'].update(fixture=[]), 'ordered classpath differs')
    edit(lambda value: value['resolved_methods'][0].update(case_digests=['f' * 64]), 'case multiset differs')
    edit(lambda value: value.update(resource_hashes={}), 'resource set differs')
    edit(lambda value: value['resource_hashes'].update({'foreign/target/test-classes/file.txt':'f'*64}), 'extra resource is outside class-only test input gap')
    edit(lambda value: value['resource_hashes'].update({COPIED_RESOURCE:'f'*64}), 'resource archive bytes differ')
    edit(lambda value: value.update(resolved_method_count=2), 'invalid provider method/call counts')
    edit(lambda value: value.update(resolved_case_count=3), 'method/case set incomplete')
    edit(lambda value: value.update(ability_proven=True), 'unsupported completeness or projection')
    edit(lambda value: value.update(capture_status='diagnostic'), 'diagnostic/empty status differs')
    artifacts['ci-inputs-rest']['post-test-inputs.json']['test_files'][helper] = 'f' * 64
    reseal(); rejected(project, 'bytecode changed after enumeration'); reset()
    artifacts['ci-inputs-rest']['expected-case-hashes.json']['provider_probe_receipt_sha256'] = 'f' * 64
    reseal(False); rejected(project, 'expected identities are not bound'); reset()
    logs[log_name] = b'2026-10-14T10:00:00.3000000Z CI_PROVIDER_PROBE_RECEIPT_SHA256 ' + b'f' * 64 + b'\n'
    reseal(False); rejected(project, 'log marker missing, duplicated or different'); reset()
    logs[log_name] += logs[log_name]
    reseal(False); rejected(project, 'log marker missing, duplicated or different'); reset()
    logs[log_name] = logs[log_name].replace(b'10:00:00.3000000', b'10:00:09.3000000')
    reseal(False); rejected(project, 'marker outside original step'); reset()
    logs['combined-job.txt'] = logs.pop(log_name)
    reseal(False); rejected(project, 'original provider step log member missing'); reset()
    extras['attempt-1/jobs-1.json']['jobs'][0]['run_id'] = 999
    reseal(False); rejected(project, 'foreign or unfinished provider job'); reset()
    extras['attempt-1/jobs-1.json']['jobs'][0]['steps'][0]['conclusion'] = 'skipped'
    reseal(False); rejected(project, 'provider step not completed successfully'); reset()
    extras['attempt-1/jobs-1.json']['jobs'][0]['steps'][1]['number'] = 8
    reseal(False); rejected(project, 'provider step not completed successfully'); reset()
    artifacts['ci-inputs-rest'].pop(r.MEMBER)
    reseal(False); rejected(project, 'receipt set differs'); reset()
    # A new prepared capture binds copied test resources without changing the
    # legacy fixture above or claiming complete build/provider semantics.
    for artifact_name in ('ci-inputs-rest','ci-inputs-build'):
        for member_name in ('build-inputs.json','post-test-inputs.json'):
            if member_name in artifacts[artifact_name]:
                artifacts[artifact_name][member_name]['test_resource_files']={COPIED_RESOURCE:g.checksum(RESOURCE_BYTES)}
    current_pre=artifacts['ci-inputs-rest']['build-inputs.json']
    aggregate_pre=artifacts['ci-inputs-build']['build-inputs.json']
    equality=g.receipt.compare_input_snapshots(f['plan'],aggregate_pre,{'rest':current_pre})
    equality['input_files']={'build':g.checksum(fmod.encode(aggregate_pre)),'rest':g.checksum(fmod.encode(current_pre))}
    artifacts['ci-inputs-build']['build-input-equivalence.json']=equality
    artifacts['ci-inputs-rest'][r.MEMBER]['build_inputs_sha256']=g.checksum(fmod.encode(current_pre))
    current_manifest=artifacts['ci-test-results-rest']['rest/manifest.json']
    current_manifest['provenance']['case_observer']['build_inputs_sha256']=g.checksum(fmod.encode(current_pre))
    raw_marker=fmod.encode(current_manifest['provenance'])
    artifacts['ci-test-results-rest']['rest/files/'+f['marker']]=raw_marker
    current_manifest['files'][f['marker']]=g.checksum(raw_marker)
    artifacts['ci-aggregate']['target/ci-shards/rest/run.json']=raw_marker
    reseal()
    modern=copy.deepcopy(artifacts),copy.deepcopy(extras),copy.deepcopy(logs)
    def reset_modern():
        artifacts.clear();artifacts.update(copy.deepcopy(modern[0]))
        extras.clear();extras.update(copy.deepcopy(modern[1]))
        logs.clear();logs.update(copy.deepcopy(modern[2]));reseal()
    modern_answer=project();modern_capture=modern_answer['provider_captures']['rest']
    check(modern_capture['resources_without_prepared_hash']==[] and modern_capture['prepared_resource_hashes_complete']
          and 'provider_prepared_resource_hashes' not in modern_answer['unverified_components'], 'new prepared input binds the copied test resource hash')
    check(modern_answer['observed_test_resource_files']=={COPIED_RESOURCE:g.checksum(RESOURCE_BYTES)}
          and not modern_capture['independent_provider_replay_proven'] and not modern_answer['whole_input_projection_verified'],
          'recorded resource bytes do not certify provider semantics or whole provenance')
    artifacts['ci-inputs-rest']['post-test-inputs.json']['test_resource_files'][COPIED_RESOURCE]='f'*64
    reseal();rejected(project,'post-test resource hashes differ');reset_modern()
    artifacts['ci-test-results-rest']['rest/files/'+COPIED_RESOURCE]=b'changed copied resource'
    artifacts['ci-test-results-rest']['rest/manifest.json']['files'][COPIED_RESOURCE]=g.checksum(b'changed copied resource')
    reseal();rejected(project,'result test resource differs');reset_modern()
    artifacts['ci-test-results-rest'].pop('rest/files/'+COPIED_RESOURCE)
    artifacts['ci-test-results-rest']['rest/manifest.json']['files'].pop(COPIED_RESOURCE)
    reseal();rejected(project,'result test resource set differs');reset_modern()
    artifacts['ci-inputs-rest'][r.MEMBER]['resource_hashes']['fixture/target/test-classes/unbound.txt']='f'*64
    reseal();rejected(project,'provider resource missing from prepared resource capture');reset_modern()
    check(before == (fmod.v.text(f['work'], 'rev-parse', 'HEAD'), fmod.v.git(f['work'], 'write-tree').stdout)
          and not fmod.v.git(f['work'], 'status', '--porcelain').stdout, 'all passive archive reads leave the worktree unchanged')


def dispatch_fixture():
    def dispatch_control(context):
        artifacts = context['artifacts']
        project, reseal, reset = context['project'], context['reseal'], context['reset']
        value = project()
        check(value['scope']['event'] == 'workflow_dispatch'
              and not value['first_complete_PR_sample'] and not value['performance_accepted'],
              'original native dispatch provider input replay keeps PR and performance claims false')
        combined_log_controls(context)
        artifacts['ci-inputs-rest'][r.MEMBER]['binding']['event'] = 'pull_request'
        reseal(); rejected(project, 'provider capture source/run/tree differs'); reset()
        artifacts['ci-inputs-rest']['build-inputs.json']['event'] = 'pull_request'
        reseal(); rejected(project, 'phase/event'); reset()
        artifacts['ci-inputs-rest'][r.MEMBER]['binding']['event'] = 'push'
        reseal(); rejected(project, 'provider capture source/run/tree differs'); reset()
        expected = project()
        output, manifest = context['f']['evidence'] / 'dispatch-readback.json', context['f']['evidence'] / 'dispatch-request.json'
        manifest.write_bytes(fmod.encode(context['f']['request']))
        child = subprocess.run(['bash',str(HERE/'ci-archive-inputs.sh'),'--manifest',str(manifest),
            '--evidence-root',str(context['f']['evidence']),'--root',str(context['f']['work']),'--output',str(output)],capture_output=True,text=True)
        check(child.returncode == 0 and json.loads(output.read_bytes()) == expected,
              'original dispatch provider archive CLI: '+child.stderr)
    contract = {'schema': 1, 'classes': {CLASS: {'module': 'fixture', 'source': SOURCE,
        'source_sha256': g.checksum(CODE.encode()), 'providers': {'values': {'kind': 'source', 'projection': 'strict'}}}}}
    def receive(f):
        controls(f, callback=dispatch_control, only_callback=True)
    fmod.run(callback=receive, only_callback=True, source_case=CODE,
             workflow_sources={'ci.yml': (HERE.parent/'workflows/ci.yml').read_text()},
             extra_source_files={CONTRACT: json.dumps(contract), PRODUCER: (HERE/'_ci-evidence.py').read_bytes(), RESOURCE:RESOURCE_BYTES},
             event_name='workflow_dispatch')


def main():
    workflow = (HERE.parent / 'workflows/ci.yml').read_text()
    check(r.workflow_recipe(workflow), 'actual producer workflow has the exact bounded capture recipe')
    check(not r.workflow_recipe(workflow.replace('ci-evidence.sh resolve --root', 'echo ci-evidence.sh resolve --root')), 'echoed commands cannot claim a capture recipe')
    check(not r.workflow_recipe(workflow.replace('name: ' + r.STEP, 'name: Unrelated')), 'unrelated step cannot claim pre-selection capture')
    legacy=workflow.replace(' --probe-receipt "$RUNNER_TEMP/ci-inputs/provider-probe-receipt.json"','')
    check(not r.workflow_capture_expected(legacy+'\n# --probe-receipt is mentioned in a comment\n'), 'a legacy comment cannot require a receipt')
    check(not r.workflow_capture_expected(legacy.replace('      - name: '+r.STEP, '      # --probe-receipt\n      - name: '+r.STEP)), 'an unrelated worker comment cannot require a receipt')
    check(r.workflow_capture_expected(workflow) and r.workflow_capture_expected(workflow.replace('name: '+r.STEP,'name: Unsupported')), 'a real capture command requires receipts even with unsupported step shape')
    continued=workflow.replace(' --probe-receipt "$RUNNER_TEMP/ci-inputs/provider-probe-receipt.json"', ' \\\n          --probe-receipt "$RUNNER_TEMP/ci-inputs/provider-probe-receipt.json"')
    def missing_capture(f):
        rejected(lambda: fmod.m.project(g.References(f['evidence']),f['work'],f['request']), 'receipt set differs')
    fmod.run(callback=missing_capture,only_callback=True,workflow_sources={'ci.yml':continued})
    inline='jobs:\n  test-shards:\n    steps:\n      - name: '+r.STEP+'\n        run: .github/scripts/ci-evidence.sh resolve --probe-receipt capture.json\n'
    fmod.run(callback=missing_capture,only_callback=True,workflow_sources={'ci.yml':inline})
    check(r.workflow_capture_expected(continued) and r.workflow_capture_expected(inline), 'unsupported active layouts stay fail-closed')
    quoted=workflow.replace('  test-shards:', '  "test-shards":')
    fmod.run(callback=missing_capture,only_callback=True,workflow_sources={'ci.yml':quoted})
    indented=workflow.replace('  test-shards:', '    test-shards:')
    check(all(r.workflow_capture_expected(value) and not r.workflow_recipe(value) for value in (quoted,indented)), 'capture detection does not depend on job-key formatting')
    check(r.workflow_recipe(legacy,capture=False), 'the exact original recipe is recognized as legacy')
    def legacy_capture(f):
        value=fmod.m.project(g.References(f['evidence']),f['work'],f['request'])
        check('provider_captures' not in value,'original legacy recipe reads without invented provider capture')
    fmod.run(callback=legacy_capture,only_callback=True,workflow_sources={'ci.yml':legacy})
    contract = {'schema': 1, 'classes': {CLASS: {'module': 'fixture', 'source': SOURCE,
        'source_sha256': g.checksum(CODE.encode()), 'providers': {'values': {'kind': 'source', 'projection': 'strict'}}}}}
    fmod.run(callback=controls, only_callback=True, source_case=CODE, workflow_sources={'ci.yml': workflow},
             extra_source_files={CONTRACT: json.dumps(contract), PRODUCER: (HERE / '_ci-evidence.py').read_bytes(), RESOURCE: RESOURCE_BYTES})
    dispatch_fixture()
    print('CI provider archive readback controls passed:', CHECKS)


if __name__ == '__main__':
    main()
