#!/usr/bin/env bash
# Offline controls for group input joins, comparator replay and read-only head guards.
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
python3 - "$here" <<'PY'
import copy
import hashlib
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import tempfile
from urllib.parse import parse_qs, urlsplit
from unittest.mock import patch
from types import SimpleNamespace
sys.dont_write_bytecode = True
spec = importlib.util.spec_from_file_location('groups', Path(sys.argv[1]) / '_ci-pr-groups.py')
m = importlib.util.module_from_spec(spec); spec.loader.exec_module(m)
checks = 0

def check(value, message):
    global checks
    assert value, message
    checks += 1

def rejected(fn, reason):
    try:
        fn()
    except (ValueError, KeyError, TypeError, OSError) as error:
        check(reason in str(error), 'specific rejection: '+reason+'; got '+str(error))
    else:
        raise AssertionError('accepted mutation: '+reason)

with tempfile.TemporaryDirectory(prefix='ci-pr-group-smoke-') as temporary:
    root = Path(temporary).resolve()
    def save(name, value):
        path=root/name; path.parent.mkdir(parents=True,exist_ok=True)
        path.write_text(json.dumps(value,sort_keys=True,indent=2)+'\n'); return ref(path)
    def ref(path):
        return {'path':path.relative_to(root).as_posix(),'sha256':hashlib.sha256(path.read_bytes()).hexdigest()}
    scope={'repository':'example/project','pr':7,'branch':'ws/fixture'}
    case={'lane':'ci','module':'core','kind':'unit','class':'sample.OriginalTest','method':'original','parameter':''}
    gate={'id':'ci:build','required':True,'applicable':True,'scope':'always','contract_sha256':'1'*64,'conclusion':'success'}
    group={'schema':1,'type':'pr-group-inputs','scope':scope,'cache_condition':'warm','order':['A','B','C'],'variants':{},'comparisons':[]}
    for number,(name,head,checkout,start,end) in enumerate([
            ('A','a'*40,'d'*40,'00:00','00:10'),('B','b'*40,'e'*40,'00:11','00:20'),
            ('C','c'*40,'f'*40,'00:21','00:30')],100):
        directory=root/name; directory.mkdir()
        binding={'run_id':number,'run_attempt':1,'source_sha':checkout}
        run={'id':number,'repository':{'full_name':scope['repository']},'event':'pull_request','run_attempt':1,
             'status':'completed','path':'.github/workflows/ci.yml','head_branch':scope['branch'],
             'head_sha':head,'pull_requests':[],'conclusion':'success',
             'created_at':'2026-01-01T'+start+':00Z','updated_at':'2026-01-01T'+end+':00Z'}
        event={'schema':1,'event_payload_complete':True,'repository':scope['repository'],'event':'pull_request',
               'run_id':number,'run_attempt':1,'checkout_sha':checkout,'checkout_tree':'2'*40,
               'checkout_parents':['1'*40,head],'ref':'refs/pull/7/merge',
               'pull_request':{'number':7,'action':'synchronize','head_repository':scope['repository'],
                   'base_repository':scope['repository'],'head_ref':scope['branch'],'base_ref':'main',
                   'head_sha':head,'base_sha':'1'*40}}
        commit={'sha':checkout,'tree':{'sha':'2'*40},'parents':[{'sha':'1'*40},{'sha':head}]}
        for filename,value in [('run.json',run),('event-inputs.json',event),('checkout-commit.json',commit)]: save(name+'/'+filename,value)
        components={key:save(name+'/'+key+'.json',{'schema':1,'type':'declared-group-component','name':key,
                    'binding':binding,'payload':{'original_input':'unchanged '+key}}) for key in m.COMPONENTS}
        inv={'schema':1,'type':'source-inventory','source_sha':checkout,'complete':True,'methods':[case], 'cases':[case]}
        inventory=save(name+'/inventory.json',inv)
        observation={'schema':1,'type':'test-observation','complete':True,'input_binding':binding,
                     'source_inventory_sha256':inventory['sha256'],'cases':[dict(case,status='executed',skip=None)],
                     'suites':[{'module':'core','kind':'unit','class':case['class']}], 'ledger':[]}
        save(name+'/observation.json',observation)
        coverage={'schema':1,'type':'coverage-observation','complete':True,
                  'binding':{'run_id':number,'attempt':1,'source_sha':checkout},
                  'production':{'core/target/classes/sample/Production':{'class_id':'3'*16,'sha256':'4'*64}},
                  'covered_lines_branches':[['core','sample','Production.java',1,2,1]],
                  'executions':{'core/target/jacoco.exec':{'classes':[{'name':'sample/Production','class_id':'3'*16,
                           'probes':2,'covered_probes':[0,1]}]}}}
        save(name+'/coverage.json',coverage)
        save(name+'/gates.json',{'schema':1,'input_binding':binding,'gates':[gate]})
        m.receipt.manifest(directory,{'archive_complete':True,'gaps':[],'repository':scope['repository'],
                        'run_attempt':1,'event':'pull_request','archive_root':str(directory),'run_id':number,
                        'source_sha':head,'checkout_sha':checkout})
        group['variants'][name]={'head_sha':head,'archive':ref(directory/'receipt.json'),'components':components}
    for candidate in ('B','C'):
        for kind in ('tests','coverage','gates'):
            filename={'tests':'observation.json','coverage':'coverage.json','gates':'gates.json'}[kind]
            inputs={'before':ref(root/'A'/filename),'after':ref(root/candidate/filename)}
            if kind=='tests':
                inputs.update(inventory=ref(root/'A/inventory.json'),candidate_inventory=ref(root/candidate/'inventory.json'),
                              additions=save(candidate+'/additions.json',[]))
            request=save(candidate+'-'+kind+'.json',{'schema':1,'kind':kind,'inputs':inputs})
            group['comparisons'].append({'before':'A','after':candidate,'lane':'ci','request':request})
    # Additions above were generated after sealing the raw fixture. Keep the archive immutable:
    # comparison-only additions belong outside it and do not pretend to be producer artifacts.
    for candidate in ('B','C'):
        additions=root/candidate/'additions.json'
        target=root/(candidate+'-additions.json'); additions.rename(target)
        request_path=root/(candidate+'-tests.json'); value=json.loads(request_path.read_text())
        value['inputs']['additions']=ref(target)
        updated=save(request_path.name,value)
        next(entry for entry in group['comparisons'] if entry['after']==candidate and entry['request']['path']==request_path.name)['request']=updated
    def reconcile(value=group):return m.group_inputs(m.References(root),value)
    good=reconcile()
    rejected(lambda:m.serial_group_inputs(m.References(root),group),'requires serial readbacks')
    check(good['declared_component_payloads_equal'] and len(good['comparisons_replayed'])==6,'A/B/C joins and replays all three existing comparators')
    check(not any(good[key] for key in ['first_complete_pr_sample_proven','group_equivalence_proven','whole_build_provenance_complete','latest_tip_verified','performance_accepted']),
          'declared equality and replayed comparisons never certify a whole sample or performance')
    check(not good['cache_condition_verified'],'a warm label is not independent cache evidence')
    # Comparison and archive bindings have separate controls; isolate only the cross-variant time join here.
    serial_group=copy.deepcopy(group)
    serial_group['serial_readbacks']={name:{'fixture':name} for name in ('A','B','C')}
    timeline={name:{'workflow_created_at':entry['ci_created_at'],
                    'workflow_terminal_observed_at':entry['ci_terminal_observed_at']}
              for name,entry in good['variants'].items()}
    def join_times():
        boundary=SimpleNamespace(bind=lambda refs,reference,*args:copy.deepcopy(timeline[reference['fixture']]))
        with patch.object(m,'group_inputs',return_value=copy.deepcopy(good)), patch.object(m,'module',return_value=boundary):
            return m.serial_group_inputs(m.References(root),serial_group)
    check(join_times()['source_bound_serial_transitions_replayed'],'all-workflow serial windows can join')
    original=copy.deepcopy(timeline)
    timeline['A']['workflow_terminal_observed_at']='2026-01-01T00:12:00Z'
    rejected(join_times,'applicable workflow timelines overlap or were reordered')
    timeline=copy.deepcopy(original)
    timeline['B']['workflow_created_at']='2026-01-01T00:09:00Z'
    rejected(join_times,'applicable workflow timelines overlap or were reordered')
    timeline=original
    for change,reason in [({'cache_condition':'unknown'},'unknown group cache'),({'order':['A','C','B']},'overlap or were reordered'),
                          ({'order':['A','B','B']},'exactly once'),({'schema':True},'unsupported PR group')]:
        rejected(lambda change=change:reconcile(dict(group,**change)),reason)
    value=copy.deepcopy(group);del value['variants']['C'];rejected(lambda:reconcile(value),'exactly once')
    value=copy.deepcopy(group);value['variants']['B']['head_sha']='a'*40;rejected(lambda:reconcile(value),'PR head differs')
    value=copy.deepcopy(group);value['variants']['C']['archive']=value['variants']['A']['archive'];rejected(lambda:reconcile(value),'PR head differs')
    value=copy.deepcopy(group);value['scope']['pr']=8;rejected(lambda:reconcile(value),'foreign PR')
    value=copy.deepcopy(group);value['scope']['branch']='ws/other';rejected(lambda:reconcile(value),'branch differs')
    value=copy.deepcopy(group);value['comparisons'].pop();rejected(lambda:reconcile(value),'missing tests/coverage/gates')
    value=copy.deepcopy(group);value['comparisons'].append(value['comparisons'][0]);rejected(lambda:reconcile(value),'duplicate group comparison')
    original=(root/'B/P.json').read_bytes();(root/'B/P.json').write_text('tampered')
    rejected(lambda:reconcile(),'archive file set/size/SHA-256 differs');(root/'B/P.json').write_bytes(original)
    changed=copy.deepcopy(group)
    binding={'run_id':101,'run_attempt':1,'source_sha':'e'*40}
    changed['variants']['B']['components']['P']=save('changed-P.json',{'schema':1,'type':'declared-group-component','name':'P','binding':binding,'payload':{'new_main_product':'changed'}})
    rejected(lambda:reconcile(changed),'P/I/D/R/U/spec declaration differs')
    for component in m.COMPONENTS:
        changed=copy.deepcopy(group)
        changed['variants']['B']['components'][component]=save('changed-'+component+'.json',{'schema':1,'type':'declared-group-component','name':component,'binding':binding,'payload':{'changed_input':component}})
        rejected(lambda changed=changed:reconcile(changed),'P/I/D/R/U/spec declaration differs')
    changed=copy.deepcopy(group);changed['variants']['B']['components']['P']=save('unknown-P.json',{'schema':1,'type':'declared-group-component','name':'P','binding':binding,'payload':None})
    rejected(lambda:reconcile(changed),'empty or unknown')
    def compare_mutation(kind,filename,mutate,reason):
        request=json.loads((root/('B-'+kind+'.json')).read_text())
        raw=json.loads((root/'B'/filename).read_text());mutate(raw)
        request['inputs']['after']=save('mutated-observation.json',raw)
        rejected(lambda:m.comparison(m.References(root),request,good['variants']['A']['binding'],binding),reason)
    compare_mutation('tests','observation.json',lambda raw:raw['cases'][0].update(method='replacement'),'case multiset differs')
    compare_mutation('tests','observation.json',lambda raw:raw['cases'].append(copy.deepcopy(raw['cases'][0])),'case multiset differs')
    compare_mutation('tests','observation.json',lambda raw:raw.update(input_binding=dict(binding,run_id=100)),'another run')
    compare_mutation('tests','observation.json',lambda raw:raw['cases'][0].update(status='skipped',skip={'type':'fixture','reason':'changed','raw':{}}),'skip status or reason changed')
    request=json.loads((root/'B-tests.json').read_text());raw=json.loads((root/'B/observation.json').read_text())
    raw['input_binding']={'run_id':'101','attempt':'1','source_sha':'e'*40,'required_exec':['core/target/jacoco.exec']}
    request['inputs']['after']=save('native-shaped-observation.json',raw)
    check(m.comparison(m.References(root),request,good['variants']['A']['binding'],binding)['result']['complete'],
          'native observation strings, attempt spelling and extra fields preserve the same origin')
    rejected(lambda:m.origin(dict(binding,run_id=True)),'invalid observation run binding')
    rejected(lambda:m.origin(dict(binding,attempt=2)),'contradictory run attempts')
    value=copy.deepcopy(group)
    for entry in value['comparisons']:entry['lane']='foreign-lane'
    rejected(lambda:reconcile(value),'comparison lane differs')
    compare_mutation('coverage','coverage.json',lambda raw:raw['covered_lines_branches'][0].__setitem__(5,0),'branches decreased')
    compare_mutation('coverage','coverage.json',lambda raw:raw['executions']['core/target/jacoco.exec']['classes'][0].update(covered_probes=[0]),'probe hits decreased')
    compare_mutation('coverage','coverage.json',lambda raw:raw['production']['core/target/classes/sample/Production'].update(class_id='9'*16),'class IDs changed')
    compare_mutation('gates','gates.json',lambda raw:raw['gates'][0].update(conclusion='cancelled'),'unsuccessful')
    compare_mutation('gates','gates.json',lambda raw:raw['gates'].clear(),'empty gate contract')
    bad={'path':'../escape.json','sha256':'0'*64};rejected(lambda:m.References(root).read(bad),'escapes evidence root')
    for name in ['/absolute.json','A/./run.json','A//run.json','A\\run.json']:
        rejected(lambda name=name:m.References(root).read({'path':name,'sha256':'0'*64}),'reference path')
    (root/'link.json').symlink_to(root/'A/run.json');rejected(lambda:m.References(root).read({'path':'link.json','sha256':'0'*64}),'symlink reference')
    (root/'linked-parent').symlink_to(root/'A',target_is_directory=True)
    rejected(lambda:m.References(root).read({'path':'linked-parent/run.json','sha256':'0'*64}),'symlink reference')
    rejected(lambda:m.References(root).read({'path':'A/P.json','sha256':'0'*64}),'referenced input hash differs')
    duplicate=root/'duplicate.json';duplicate.write_text('{"schema":1,"schema":2}')
    rejected(lambda:m.load(duplicate),'duplicate JSON key')
    refs=m.References(root);refs.read(ref(root/'A/P.json'));old=(root/'A/P.json').read_bytes();(root/'A/P.json').write_bytes(old+b' ')
    rejected(refs.finish,'input changed during comparison');(root/'A/P.json').write_bytes(old)
    impact={'group_state':'completed','before':copy.deepcopy(group['variants']['A']['components']),
            'after':copy.deepcopy(group['variants']['A']['components'])}
    kept=m.impact(m.References(root),impact)
    check(not kept['changed'] and kept['completed_group_disposition']=='retain-versioned-declarations' and not kept['latest_tip_verified'], 'same input metadata retains versioned declarations, not latest-tip green')
    impact=copy.deepcopy(impact);impact['group_state']='pending';impact['after']['P']=changed['variants']['B']['components']['P']
    rejected(lambda:m.impact(m.References(root),impact),'empty or unknown')
    impact['after']['P']=ref(root/'changed-P.json')
    affected=m.impact(m.References(root),impact)
    check(affected['changed']==['P'] and not affected['pending_group_declarations_comparable'],'new main product input prevents pending group comparability')
    # Test the actual command entry point; its inputs are deliberately synthetic receipts.
    manifest=root/'group.json';manifest.write_text(json.dumps(group))
    command=['bash',str(Path(sys.argv[1])/'ci-pr-groups.sh'),'group-inputs','--manifest',str(manifest),'--evidence-root',str(root),'--output',str(root/'result.json')]
    completed=subprocess.run(command,text=True,capture_output=True)
    check(completed.returncode==0,'CLI group join executes existing comparators: '+completed.stderr)
    strict=command.copy();strict[strict.index('group-inputs')]='serial-group-inputs';strict[-1]=str(root/'strict.json')
    denied=subprocess.run(strict,text=True,capture_output=True)
    check(denied.returncode!=0 and 'requires serial readbacks' in denied.stderr,'strict CLI rejects a self-consistent group without serial source-policy proof')
    output=json.loads((root/'result.json').read_text())
    check(output['manifest_sha256']==hashlib.sha256(manifest.read_bytes()).hexdigest() and 'A/inventory.json' in output['input_files'],'CLI binds manifest and exact comparison inputs')
    completed=subprocess.run(command,text=True,capture_output=True)
    check(completed.returncode!=0 and 'never overwrite evidence' in completed.stderr,'CLI never overwrites an existing receipt')
    target=root/'symlink-target.json';linked=root/'dangling-output.json';linked.symlink_to(target)
    linked_command=command[:-1]+[str(linked)]
    completed=subprocess.run(linked_command,text=True,capture_output=True)
    check(completed.returncode!=0 and not target.exists(),'CLI rejects a dangling output without creating its target')
    preexisting=root/'exclusive.json';preexisting.write_text('another writer owns this evidence')
    rejected(lambda:m.write_new(preexisting,{}),'File exists')
    check(preexisting.read_text()=='another writer owns this evidence','exclusive creation preserves a concurrent writer')
    directory=root/'output-directory';directory.mkdir();(root/'output-link').symlink_to(directory,target_is_directory=True)
    try:m.write_new(root/'output-link/unsafe.json',{})
    except OSError:check(not (directory/'unsafe.json').exists(),'output directory traversal never follows a symlink')
    else:raise AssertionError('output followed symlink directory')

    head_scope=dict(scope,head_sha='a'*40)
    contract={'applicable_workflows':['.github/workflows/ci.yml','.github/workflows/repo-hygiene.yml'],'required_checks':sorted(m.REQUIRED_CHECKS)}
    pr={'number':7,'state':'open','draft':True,'merge_commit_sha':'d'*40,
        'head':{'sha':'a'*40,'ref':'ws/fixture','repo':{'full_name':'example/project'}},
        'base':{'sha':'1'*40,'ref':'main','repo':{'full_name':'example/project'}}}
    runs=[{'id':i,'head_sha':'a'*40,'head_branch':'ws/fixture','event':'pull_request','repository':{'full_name':'example/project'},
           'pull_requests':[{'number':7,'head':{'sha':'a'*40},'base':{'sha':'1'*40}}],
           'path':path,'status':'completed','conclusion':'success','run_attempt':1} for i,path in enumerate(contract['applicable_workflows'],1)]
    checks_data=[{'id':i,'head_sha':'d'*40,'name':name,'status':'completed','conclusion':'success'} for i,name in enumerate(contract['required_checks'],10)]
    commit={'sha':'d'*40,'parents':[{'sha':'1'*40},{'sha':'a'*40}]}
    def inspect(edit=None):
        snapshot={'pr':copy.deepcopy(pr),'runs':copy.deepcopy(runs),'checks':copy.deepcopy(checks_data),'commit':copy.deepcopy(commit)}
        if edit:edit(snapshot)
        queried=[]
        def request(endpoint):
            queried.append(endpoint)
            if '/pulls/' in endpoint:return copy.deepcopy(snapshot['pr'])
            if '/git/commits/' in endpoint:return snapshot['commit']
            if '/actions/runs?' in endpoint:return {'total_count':len(snapshot['runs']),'workflow_runs':snapshot['runs']}
            data=snapshot['checks'] if '/commits/'+('d'*40)+'/check-runs' in endpoint else []
            if parse_qs(urlsplit(endpoint).query).get('filter')!=['all']:
                data=list({row['name']:row for row in sorted(data,key=lambda row:row['id'])}.values())
            return {'total_count':len(data),'check_runs':data}
        result=m.head_status(head_scope,contract,request)
        check(all('/cancel' not in endpoint and '/dispatch' not in endpoint for endpoint in queried),'head inspection makes read-only requests')
        return result
    terminal=inspect()
    check(terminal['can_advance_after_archiving'] and not terminal['archive_readback_verified'] and not terminal['first_complete_pr_sample_proven'],'terminal snapshot still requires archive and does not certify a sample')
    check(terminal['unassociated_check_ids'],'missing check PR associations remain explicit unknowns')
    for edit,reason in [(lambda s:s['runs'][0].update(status='in_progress'),'workflow still running'),
                        (lambda s:s['runs'].pop(),'workflow set is missing'),
                        (lambda s:s['checks'].pop(),'check instance missing'),
                        (lambda s:s['checks'][0].update(status='queued'),'check still running')]:
        result=inspect(edit);check(not result['can_advance_after_archiving'] and any(reason in item for item in result['reasons']),'head guard: '+reason)
    failed=inspect(lambda s:s['runs'][0].update(conclusion='failure',run_attempt=2))
    check(failed['can_advance_after_archiving'] and not failed['first_complete_pr_sample_proven'],'terminal failure/retry retained for diagnostics, never a first sample')
    def old_instance(snapshot):
        old=copy.deepcopy(snapshot['checks'][0]);old.update(id=2,status='in_progress');snapshot['checks'].append(old)
    repeated=inspect(old_instance)
    check(not repeated['can_advance_after_archiving'] and len(repeated['checks'])==len(checks_data)+1,
          'filter=all keeps an unfinished older instance even when its latest same-name instance completed')
    def foreign_checks(snapshot):
        for row in snapshot['checks']:row['pull_requests']=[{'number':8,'head':{'sha':'a'*40},'base':{'sha':'1'*40}}]
    rejected(lambda:inspect(foreign_checks),'foreign PR check association')
    def own_checks(snapshot):
        for row in snapshot['checks']:row['pull_requests']=[{'number':7,'head':{'sha':'a'*40},'base':{'sha':'1'*40}}]
    check(inspect(own_checks)['can_advance_after_archiving'],'matching populated check associations corroborate the current PR')
    rejected(lambda:inspect(lambda s:s['pr']['head'].update(sha='b'*40)),'current PR head')
    rejected(lambda:inspect(lambda s:s['pr'].update(draft=False)),'expected open draft')
    rejected(lambda:inspect(lambda s:s['runs'][0]['pull_requests'][0].update(number=8)),'foreign PR run')
    rejected(lambda:inspect(lambda s:s['runs'][0]['pull_requests'][0]['head'].update(sha='9'*40)),'foreign PR run')
    rejected(lambda:inspect(lambda s:s['runs'][0]['pull_requests'][0]['base'].update(sha='9'*40)),'foreign PR run')
    rejected(lambda:inspect(lambda s:s['commit']['parents'][0].update(sha='9'*40)),'merge parents differ')
    responses=iter([copy.deepcopy(pr),dict(copy.deepcopy(pr),draft=False)])
    def moving_scope(endpoint):
        if '/pulls/' in endpoint:return next(responses)
        if '/git/commits/' in endpoint:return copy.deepcopy(commit)
        if '/actions/runs?' in endpoint:return {'total_count':len(runs),'workflow_runs':copy.deepcopy(runs)}
        data=copy.deepcopy(checks_data) if '/commits/'+('d'*40)+'/check-runs' in endpoint else []
        return {'total_count':len(data),'check_runs':data}
    rejected(lambda:m.head_status(head_scope,contract,moving_scope),'scope changed during inspection')
    small=copy.deepcopy(contract);small['required_checks'].remove('build')
    rejected(lambda:m.head_status(head_scope,small,lambda endpoint:copy.deepcopy(pr)),'check contract is incomplete')
print('ci-pr-groups smoke: '+str(checks)+' assertions passed')
PY
