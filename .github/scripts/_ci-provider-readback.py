#!/usr/bin/env python3
"""Join hash-only provider captures to original source, step logs and input packets.

These are archive consistency checks. No archived code runs and computed provider
semantics remain unproven until a separate approved replay uses matching bytes.
"""
from collections import Counter
import io
import json
from pathlib import Path, PurePosixPath
import re
import sys
import tarfile
import tempfile

sys.dont_write_bytecode = True
import importlib.util
spec = importlib.util.spec_from_file_location('provider_groups', Path(__file__).with_name('_ci-pr-groups.py'))
g = importlib.util.module_from_spec(spec)
spec.loader.exec_module(g)
require, ev = g.require, g.evidence
variants = g.module('provider_variants', '_ci-variants.py')
STEP = 'Derive complete test identities before selection'
EXECUTE = 'Execute exactly this shard'
MEMBER = 'provider-probe-receipt.json'


def digest(value):
    require(isinstance(value, str) and re.fullmatch(r'[0-9a-f]{64}', value) is not None, 'invalid provider capture hash')
    return value


def hashes(value):
    require(isinstance(value, dict), 'invalid provider hash map')
    for name, checksum in value.items():
        variants.path_name(name)
        digest(checksum)
    return value


def canonical_hash(value):
    return g.checksum(json.dumps(value, sort_keys=True, separators=(',', ':')).encode())


class Source:
    def __init__(self, raw, source_hashes):
        self.archive = tarfile.open(fileobj=io.BytesIO(raw), mode='r:')
        self.members = {}
        self.hashes = source_hashes
        for item in self.archive:
            require(item.name not in self.members, 'duplicate provider source archive member')
            self.members[item.name] = item

    def read(self, name):
        require(name in self.members and name in self.hashes, 'provider source file absent from checkout archive')
        item = self.members[name]
        require(item.isfile() and item.size <= 16 * 1024 * 1024, 'provider source is not a bounded regular file')
        raw = self.archive.extractfile(item).read()
        require(g.checksum(raw) == self.hashes[name], 'provider source bytes differ from checkout snapshot')
        return raw

    def close(self):
        self.archive.close()


def jobs(archive):
    prefix = 'attempt-' + str(archive.scope['run_attempt']) + '/'
    pages = sorted((name for name in archive.receipt['files'] if re.fullmatch(re.escape(prefix)+r'jobs-[1-9][0-9]*\.json', name)),
                   key=lambda name: int(name.rsplit('-', 1)[1].split('.')[0]))
    require(pages and pages == [prefix+'jobs-'+str(i)+'.json' for i in range(1,len(pages)+1)], 'provider step job pages missing')
    found, total = [], None
    for name in pages:
        value = archive.json(name)
        require(type(value.get('total_count')) is int and isinstance(value.get('jobs'),list)
                and (total is None or total == value['total_count']), 'provider step job totals differ')
        total = value['total_count']
        found.extend(value['jobs'])
    require(len(found) == total and len({row['id'] for row in found}) == total, 'provider jobs missing or duplicated')
    for row in found:
        require(type(row.get('id')) is int and row['id'] > 0 and type(row.get('run_id')) is int
                and row['run_id'] == archive.scope['run_id'] and type(row.get('run_attempt')) is int
                and row['run_attempt'] == archive.scope['run_attempt'] and row.get('status') == 'completed', 'foreign or unfinished provider job')
    return found, prefix


def step_binding(archive, worker, all_jobs, log_files, receipt_sha, invocations):
    matches = [row for row in all_jobs if row['name'] == 'test-shards ('+worker+')']
    require(len(matches) == 1, 'provider worker job is absent or ambiguous')
    job = matches[0]
    steps = job['steps']
    require(isinstance(steps,list), 'provider job steps unavailable')
    selected = [step for step in steps if step['name'] == STEP]
    executions = [step for step in steps if step['name'] == EXECUTE]
    require(len(selected) == len(executions) == 1, 'provider or selected-verify step absent or ambiguous')
    step, later = selected[0], executions[0]
    require(type(step['number']) is int and type(later['number']) is int and 0 < step['number'] < later['number']
            and step['status'] == 'completed' and step['conclusion'] == 'success', 'provider step not completed successfully before selection')
    start, end = g.timestamp(step['started_at']), g.timestamp(step['completed_at'])
    require(start <= end and end <= g.timestamp(later['started_at']), 'provider step timeline differs from pre-selection claim')
    name = job['name']+'/'+str(step['number'])+'_'+STEP+'.txt'
    if name not in log_files:
        # A workflow log archive can contain numbered, combined job logs. Its
        # receipt marker must still bind to this job's original API step window.
        combined = [path for path in log_files if re.fullmatch(
            r'(?:0|[1-9][0-9]*)_' + re.escape(job['name']) + r'\.txt', path)]
        require(len(combined) <= 1, 'original provider job log member ambiguous')
        require(combined, 'original provider step log member missing')
        name = combined[0]
    markers = []
    for line in log_files[name].decode('utf-8-sig').splitlines():
        if 'CI_PROVIDER_PROBE_RECEIPT_SHA256' not in line:
            continue
        match = re.fullmatch(r'(\d{4}-\d{2}-\d{2}T[^ ]+) CI_PROVIDER_PROBE_RECEIPT_SHA256 ([0-9a-f]{64})',line)
        require(match is not None, 'malformed provider receipt log marker')
        moment = g.timestamp(match[1])
        require(start <= moment and moment.timestamp() < end.timestamp()+1, 'provider receipt marker outside original step')
        markers.append(match[2])
        marker_time = moment
    require(markers == [receipt_sha], 'provider log marker missing, duplicated or different')
    for call in invocations:
        a,b = g.timestamp(call['started_at']),g.timestamp(call['ended_at'])
        require(start <= a <= b and b.timestamp() < end.timestamp()+1, 'provider invocation outside original step')
        require(b <= marker_time, 'provider invocation ends after its receipt marker')
    return {'job_id':job['id'],'step_number':step['number'],'step_name':STEP,'log_member':name,
            'log_member_sha256':g.checksum(log_files[name]),'archived_step_log_binding_consistent':True}


def source_method(source, contracts, row, requested):
    key = row['class']
    contract = contracts['classes'].get(key)
    require(isinstance(contract,dict) and contract.get('module') == row['module'], 'provider module/class absent from source contract')
    raw = source.read(contract['source'])
    require(g.checksum(raw) == contract['source_sha256'] == row['provider_contract']['source_sha256']
            and row['source'] == contract['source'], 'provider source contract hash differs')
    approved = contract['providers'].get(requested['provider'])
    require(isinstance(approved,dict) and approved['kind'] == requested['provider_kind'], 'provider request outside original whitelist')
    declarations = ev.declarations(raw.decode(),PurePosixPath(contract['source']).stem,key.rsplit('.',1)[0]+'.')
    methods = [method for method in declarations if method['class'] == key and method['method'] == row['method']]
    require(len(methods) == 1, 'provider requested method is absent or overloaded in original source')
    method = methods[0]
    if requested['provider_kind'] == 'dynamic':
        require('TestFactory' in method['annotations'] and requested['provider'] == row['method']
                and row['mapping'] == 'dynamic-marker', 'dynamic provider declaration differs')
    else:
        require(requested['provider_kind'] == 'source' and 'MethodSource' in method['annotations']
                and ev.literals(method['annotations']['MethodSource'],raw.decode()) == [requested['provider']], 'source provider declaration differs')
        wanted = 'named-carrier-marker' if approved['projection']=='named-carrier' else 'argument-marker'
        require(row['mapping'] == wanted,'provider projection differs from original contract')


def original_inventory(raw, checkout):
    reader = g.module('provider_source_inventory', '_ci-archive-cases.py')
    with tempfile.TemporaryDirectory(prefix='provider-source-data-') as temporary:
        root = Path(temporary) / 'source'
        root.mkdir()
        reader.parsed_source(raw, root)
        value, environment = reader.source_inventory(root, checkout, g.checksum(raw), 'ci', Path(temporary)/'inventory.json')
        require(environment['published_connectors']=='absent' and environment['native_cli_presence']=='absent',
                'provider inventory entry conditions differ from the original CI recipe')
        # Reconstruct the direct-checkout document, not the reader's archive-origin wrapper.
        value['source_origin'] = {'kind':'checkout','source_sha':checkout}
        checksum = g.checksum((json.dumps(value,indent=2,sort_keys=True)+'\n').encode())
        return value, checksum


def pending_requests(inventory, contracts, source):
    wanted = set()
    for method in inventory['methods']:
        if method['parameters'] is not None or method['class'] not in contracts['classes']:
            continue
        approved = contracts['classes'][method['class']]
        if 'TestFactory' in method['annotations']:
            name,kind = method['method'],'dynamic'
        elif 'MethodSource' in method['annotations']:
            # The source parser retained the annotation literal; provider names are public metadata.
            names = ev.literals(method['annotations']['MethodSource'],source.read(approved['source']).decode())
            require(len(names)==1,'ambiguous original provider declaration')
            name,kind = names[0],'source'
        else:
            continue
        require(name in approved['providers'] and approved['providers'][name]['kind']==kind,'original provider is outside reviewed contract')
        wanted.add((method['module'],method['kind'],method['class'],method['method']))
    return wanted


def read_capture(archive, worker, members, before, after, source, source_hashes, payloads, all_jobs, log_files, inventory, original_sha):
    raw = members[MEMBER]
    value = g.json_bytes(raw)
    wanted = {'schema','type','binding','runner_context','lane','original_source_sha','original_inventory_sha256',
              'producer_script_sha256','producer_prepared_source_bound','build_inputs_sha256','contracts','helper_class_hashes',
              'resolved_classpaths','resource_hashes','invocations','resolved_methods','resolved_method_count','resolved_case_count',
              'capture_status','inventory_complete','before_test_selection','project_original','independent_provider_replay_proven',
              'ability_proven','whole_build_provenance_complete'}
    require(isinstance(value,dict) and set(value)==wanted and type(value['schema']) is int and value['schema']==1
            and value['type']=='provider-probe-receipt', 'invalid archived provider receipt schema')
    require(value['before_test_selection'] is True and value['project_original'] is False
            and value['independent_provider_replay_proven'] is False and value['ability_proven'] is False
            and value['whole_build_provenance_complete'] is False and type(value['inventory_complete']) is bool,
            'provider capture claims unsupported completeness or projection')
    binding = value['binding']
    require(set(binding)=={'source_sha','source_tree','run_id','run_attempt','event'}
            and g.origin(binding)=={'run_id':archive.scope['run_id'],'run_attempt':archive.scope['run_attempt'],'source_sha':before['source_sha']}
            and binding['source_tree']==before['source_tree']
            and binding['event']==before['event']==archive.scope['event']
            and binding['event'] in ('pull_request','workflow_dispatch')
            and value['original_source_sha']==before['source_sha'] and value['lane']=='ci', 'provider capture source/run/tree differs')
    context = g.receipt.input_runner_context(before,worker,archive.scope['repository'])
    require(context is not None and context==value['runner_context'] and context['workflow_source']['snapshot']=='captured',
            'provider capture runner workflow source is missing or different')
    workflow = context['workflow_source']
    require(workflow['workflow_path']=='.github/workflows/ci.yml' and workflow['job']=='test-shards', 'provider workflow path/job differs from the original worker')
    workflow_same_checkout = workflow['workflow_sha']==before['source_sha']
    require(value['producer_prepared_source_bound'] is True
            and digest(value['producer_script_sha256'])==source_hashes.get('.github/scripts/_ci-evidence.py')
            and value['build_inputs_sha256']==g.checksum(members['build-inputs.json']), 'provider producer/prepared input byte binding differs')
    contract = value['contracts']
    require(isinstance(contract,dict) and set(contract)=={'path','sha256'} and digest(contract['sha256'])==source_hashes.get(contract['path']),
            'provider contract source is unbound')
    contracts = g.json_bytes(source.read(contract['path']))
    require(type(contracts.get('schema')) is int and contracts['schema']==1 and isinstance(contracts.get('classes'), dict), 'invalid original provider contract')
    expected = g.json_bytes(members['expected-case-hashes.json'])
    require(type(expected.get('schema')) is int and expected['schema']==1 and expected.get('type')=='expected-case-hashes'
            and type(expected.get('complete')) is bool and expected['complete']==value['inventory_complete']
            and expected.get('ability_proven') is False, 'invalid provider expected hash document')
    require(expected.get('provider_probe_receipt_sha256')==g.checksum(raw)
            and digest(expected.get('original_inventory_sha256'))==digest(value['original_inventory_sha256'])
            and expected.get('source_sha')==before['source_sha'], 'expected identities are not bound to this provider receipt')
    helper = hashes(value['helper_class_hashes'])
    prefix='test-support/target/test-classes/io/tapstate/testsupport/'
    original_helper={name:checksum for name,checksum in before['test_files'].items() if name.startswith(prefix)
        and PurePosixPath(name).name.startswith(('CiCaseEvidence','CiProviderInventory')) and 'Test' not in PurePosixPath(name).name}
    require(helper==original_helper and prefix+'CiProviderInventory.class' in helper,'provider helper class set differs from prepared inputs')
    require(value['original_inventory_sha256']==original_sha,'provider original inventory digest differs from independent source replay')
    methods, calls = value['resolved_methods'],value['invocations']
    require(isinstance(methods,list) and isinstance(calls,list) and type(value['resolved_method_count']) is int
            and value['resolved_method_count']==len(methods) and type(value['resolved_case_count']) is int, 'invalid provider method/call counts')
    require(value['capture_status']==('success' if calls else 'not-required'), 'provider diagnostic/empty status differs')
    index, requested = {},{}
    for call in calls:
        require(isinstance(call,dict) and set(call)=={'module','requested_methods','requested_method_count','request_sha256','result_sha256',
            'helper_jar_sha256','helper_entry_hashes_sha256','stdout_sha256','stderr_sha256','java_command_sha256','exit_code',
            'started_at','ended_at','elapsed_ns','result_record_count','result_records_sha256'}, 'invalid provider invocation schema')
        module=call['module']; require(module not in index,'duplicate provider module invocation'); index[module]=call
        require(type(call['exit_code']) is int and call['exit_code']==0 and type(call['elapsed_ns']) is int and call['elapsed_ns']>=0
                and isinstance(call['requested_methods'],list) and type(call['requested_method_count']) is int
                and call['requested_method_count']==len(call['requested_methods'])>0 and type(call['result_record_count']) is int
                and call['result_record_count']==call['requested_method_count'], 'provider invocation unsuccessful or partial')
        for field in ('request_sha256','result_sha256','helper_jar_sha256','stdout_sha256','stderr_sha256','java_command_sha256','result_records_sha256'):
            digest(call[field])
        require(call['helper_entry_hashes_sha256']==canonical_hash(helper),'provider helper invocation digest differs')
        lines=[]
        for row in call['requested_methods']:
            require(isinstance(row,dict) and set(row)=={'class','method','kind','provider','provider_kind'}
                    and row['kind'] in ('unit','it') and row['provider_kind'] in ('source','dynamic'),'invalid provider request identity')
            key=(module,row['kind'],row['class'],row['method'])
            require(key not in requested,'duplicate provider source request'); requested[key]=row
            lines.append('\t'.join([row['class'],row['method'],row['provider'],row['provider_kind']])+'\n')
        require(g.checksum(''.join(lines).encode())==call['request_sha256'],'provider request TSV hash differs')
    require(set(requested)==pending_requests(inventory,contracts,source),'provider invocation set omits or adds an original computed method')
    classpaths = value['resolved_classpaths']
    require(isinstance(classpaths,dict) and set(classpaths)==set(index) and all(classpaths[module]==before['resolved_test_classpaths'].get(module) for module in index),
            'provider ordered classpath differs from prepared inputs')
    method_keys, provider_files, count = set(),{},0
    expected_methods={(row['module'],row['kind'],row['class'],row['method']):row for row in expected['methods']}
    require(len(expected_methods)==len(expected['methods']), 'duplicate expected provider method')
    for row in methods:
        require(isinstance(row,dict) and set(row)=={'lane','module','kind','class','method','source','mapping','provider_contract','case_digests'}
                and row['lane']=='ci' and isinstance(row['case_digests'],list) and row['case_digests'], 'invalid resolved provider method')
        key=(row['module'],row['kind'],row['class'],row['method'])
        require(key in requested and key not in method_keys and key in expected_methods,'foreign/duplicate resolved provider method'); method_keys.add(key)
        provider_contract=row['provider_contract']
        require(isinstance(provider_contract,dict) and set(provider_contract)=={'source_sha256','provider','provider_class_sha256'}
                and provider_contract['provider']==requested[key]['provider'], 'resolved provider contract differs from request')
        source_method(source,contracts,row,requested[key])
        path=row['module']+'/target/test-classes/'+row['class'].replace('.','/')+'.class'
        provider_files[path]=digest(provider_contract['provider_class_sha256'])
        require(before['test_files'].get(path)==provider_files[path],'provider class differs from prepared input')
        require(expected_methods[key]['provider_contract_sha256']==canonical_hash(provider_contract), 'expected provider contract differs')
        cases=[case['case_digest'] for case in expected['cases'] if (case['module'],case['kind'],case['class'],case['method'])==key]
        require(Counter(map(digest,row['case_digests']))==Counter(map(digest,cases)), 'provider resolved case multiset differs from expected hashes')
        count+=len(cases)
    require(method_keys==set(requested) and count==value['resolved_case_count'],'provider receipt method/case set incomplete')
    resolved_sources={(row['source'],row['method']) for row in methods}
    remaining=[gap for gap in inventory['unresolved'] if (gap.get('source'),gap.get('method')) not in resolved_sources]
    require(value['inventory_complete']==(not remaining), 'provider inventory completeness differs from independent source gaps')
    byte_readback={}
    for path,checksum in {**helper,**provider_files}.items():
        require(after['test_files'].get(path)==before['test_files'].get(path)==checksum, 'provider/helper bytecode changed after enumeration')
        found=[]
        for name,members0 in payloads.items():
            relative=name+'/files/'+path
            if relative in members0: found.append(members0[relative])
        require(all(g.checksum(data)==checksum for data in found),'archived provider/helper bytes differ')
        if found:
            expected_name=path.split('/target/test-classes/',1)[1].removesuffix('.class')
            require(all(g.receipt.class_structure(data)['name']==expected_name for data in found),'provider/helper binary class identity differs')
        byte_readback[path]=bool(found)
    resource_hashes=hashes(value['resource_hashes'])
    wanted_resources={}
    for module in index:
        prefixes=tuple(module+'/'+folder+'/' for folder in ('src/test/resources','src/main/resources','target/test-classes','target/classes'))
        for field in ('source_files','production_files','test_files','test_resource_files'):
            wanted_resources.update({path:checksum for path,checksum in before.get(field,{}).items() if path.startswith(prefixes)})
    require(all(resource_hashes.get(path)==checksum for path,checksum in wanted_resources.items()),
            'provider resource set differs from prepared source/classes')
    unbound_resources=sorted(set(resource_hashes)-set(wanted_resources))
    require('test_resource_files' not in before or not unbound_resources, 'provider resource missing from prepared resource capture')
    require(all(any(path.startswith(module+'/target/test-classes/') for module in index) and not path.endswith('.class')
                for path in unbound_resources), 'provider extra resource is outside class-only test input gap')
    for path,checksum in resource_hashes.items():
        prepared=before['source_files'].get(path,before['production_files'].get(path,before['test_files'].get(path)))
        if prepared is not None:require(prepared==checksum,'provider resource differs from prepared inputs')
        observed=after['source_files'].get(path,after['production_files'].get(path,after['test_files'].get(path)))
        if observed is not None:require(observed==checksum,'provider resource changed after enumeration')
        for name,packet in payloads.items():
            if name+'/files/'+path in packet:require(g.checksum(packet[name+'/files/'+path])==checksum,'provider resource archive bytes differ')
    step = step_binding(archive,worker,all_jobs,log_files,g.checksum(raw),calls)
    return {'receipt_sha256':g.checksum(raw),'binding':binding,'step':step,'method_count':len(methods),'case_count':count,
            'helper_class_bytes_read_back':all(byte_readback[p] for p in helper),
            'provider_class_bytes_read_back':(all(byte_readback[p] for p in provider_files) if provider_files else None),
            'workflow_source_is_archived_checkout':workflow_same_checkout,
            'receipt_hash_links_consistent':True,'resources_without_prepared_hash':unbound_resources,
            'prepared_resource_hashes_complete':not unbound_resources,'inventory_complete':value['inventory_complete'],'provider_result_bytes_read_back':False,
            'independent_provider_replay_proven':False,'whole_build_provenance_complete':False,'ability_proven':False}


def workflow_job(text):
    job = re.search(r'^  test-shards:[ \t]*\n(?P<body>.*?)(?=^  [\w-]+:|\Z)', text, re.M | re.S)
    return job['body'] if job is not None else ''


def workflow_capture_expected(text):
    # Any active flag keeps the strict path enabled, including unsupported
    # inline, folded or continued commands. Unfamiliar active uses may reject
    # conservatively; full-line comments alone never require a receipt.
    active='\n'.join(line for line in text.splitlines() if not line.lstrip().startswith('#'))
    return '--probe-receipt' in active


def workflow_recipe(text, capture=True):
    # Interpret only the conventional workflow shape this producer emits. Unknown
    # syntax fails closed; comments and unrelated steps cannot enable capture.
    steps = list(re.finditer(r'^      - name: ([^\n]+)\n(?P<body>.*?)(?=^      - |\Z)', workflow_job(text), re.M | re.S))
    selected = [step for step in steps if step[1] == STEP]
    if len(selected) != 1:
        return False
    match = re.fullmatch(r'        run: \|\n(?P<commands>(?:          [^\n]+\n?)+)', selected[0]['body'])
    if match is None:
        return False
    commands = [line.removeprefix('          ') for line in match['commands'].splitlines()]
    wanted = [
        '.github/scripts/ci-evidence.sh inventory --root . --lane ci --published-connectors absent --native-cli-presence absent --output "$RUNNER_TEMP/source-inventory.json"',
        '.github/scripts/ci-evidence.sh resolve --root . --inventory "$RUNNER_TEMP/source-inventory.json" --inputs "$RUNNER_TEMP/ci-inputs/build-inputs.json" --repo-local "$RUNNER_TEMP/m2" --output "$RUNNER_TEMP/resolved-inventory.json" --probe-receipt "$RUNNER_TEMP/ci-inputs/provider-probe-receipt.json"',
        '.github/scripts/ci-evidence.sh export-hashes --input "$RUNNER_TEMP/resolved-inventory.json" --output "$RUNNER_TEMP/ci-inputs/expected-case-hashes.json"']
    if not capture:
        wanted[1]=wanted[1].replace(' --probe-receipt "$RUNNER_TEMP/ci-inputs/provider-probe-receipt.json"','')
    return commands == wanted and any(step[1] == EXECUTE and step.start() > selected[0].start() for step in steps)


def project(archive, plan, pre, post, input_members, payloads, source_hashes, source_raw, zip_reader):
    with_source=Source(source_raw,source_hashes)
    try:
        workflow=with_source.read('.github/workflows/ci.yml').decode()
        required=workflow_recipe(workflow)
        present=[name for name,members in input_members.items() if MEMBER in members]
        if not workflow_capture_expected(workflow) and not present:
            require(STEP not in workflow or workflow_recipe(workflow,capture=False), 'unsupported legacy provider workflow recipe')
            return None
        workers={worker['id'] for worker in plan['shards']}
        require(required and set(present)==workers,'provider receipt set differs from original workflow/worker set')
        inventory, original_sha = original_inventory(source_raw,plan['source_sha'])
        all_jobs,prefix=jobs(archive)
        log_files,_=zip_reader(archive.raw(prefix+'logs.zip'))
        return {name:read_capture(archive,name,input_members[name],pre[name],post[name],with_source,source_hashes,payloads,all_jobs,log_files,inventory,original_sha)
                for name in sorted(workers)}
    finally:
        with_source.close()


def reproduce_records(records, call, capture, before):
    require(isinstance(records,list), 'invalid provider replay result record list')
    wanted = {(row['class'], row['method']): (row['provider'], row['provider_kind']) for row in call['requested_methods']}
    original = {(row['class'], row['method']): row for row in capture['resolved_methods'] if row['module'] == call['module']}
    rows, found, counts, multisets = [], set(), {}, []
    for row in records:
        ev.validate_provider_record(row, wanted, before, call['module'])
        key = row['class'], row['test_method']
        require(key not in found and key in original, 'duplicate or foreign replay result')
        found.add(key)
        method = original[key]
        if method['mapping'] == 'dynamic-marker':
            require(row['kind'] == 'dynamic', 'replay dynamic projection differs')
            parameters = [case['display_name_digest'] for case in row['cases']]
        elif method['mapping'] == 'named-carrier-marker':
            require(row['class'] == 'io.tapstate.e2e.JoinConformanceIT' and row['provider'] == 'carriers', 'unreviewed replay carrier projection')
            require(all(case['first_string_digest'] and len(case['argument_descriptors']) == 2
                        and case['argument_descriptors'][0]['stable'] for case in row['cases']), 'replay carrier lacks stable source identity')
            parameters = [case['first_string_digest'] for case in row['cases']]
        else:
            require(method['mapping'] == 'argument-marker' and row['kind'] == 'source'
                    and all(case['stable'] for case in row['cases']), 'replay arguments lack stable identities')
            parameters = [case['argument_digest'] for case in row['cases']]
        require(len(parameters) == len(set(parameters)), 'duplicate provider replay case')
        digests = [ev.case_hash({**{field: method[field] for field in ('lane', 'module', 'kind', 'class', 'method')},
                                'parameter': parameter}) for parameter in parameters]
        require(Counter(digests) == Counter(method['case_digests']), 'replayed provider case multiset differs')
        multisets.append({**{field:method[field] for field in ('lane','module','kind','class','method')}, 'case_digests':sorted(digests)})
        counts[key] = len(digests)
        rows.append(row)
    require(found == set(wanted) == set(original), 'provider replay method set is incomplete')
    return {'method_count': len(found), 'case_count': sum(counts.values()),
            'result_records_sha256': g.checksum(g.canonical(rows)),
            'case_multisets': sorted(multisets,key=lambda value:tuple(value[key] for key in ('lane','module','kind','class','method')))}
