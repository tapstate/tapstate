#!/usr/bin/env python3
"""Complete mixed-attempt collection and artifact/case readback protocol controls."""
import argparse
import copy
from contextlib import redirect_stdout
import importlib.util
import io
import json
from pathlib import Path
import sys

sys.dont_write_bytecode = True
HERE = Path(__file__).resolve().parent

def load(name, filename):
    spec = importlib.util.spec_from_file_location(name, HERE / filename)
    module = importlib.util.module_from_spec(spec); spec.loader.exec_module(module)
    return module

fixture = load('retained_input_fixture', '_ci-archive-inputs-smoke.py')
cases = load('retained_case_fixture', '_ci-archive-cases-smoke.py')
g, m = fixture.g, fixture.m
checks = 0

def check(value, message):
    global checks
    assert value, message
    checks += 1

def controls(f):
    artifacts, extra, scope = f['artifacts'], f['archive_extra_files'], f['scope']
    scope['run_attempt'] = 2
    run = dict(id=scope['run_id'], run_attempt=2, head_sha=scope['head_sha'], head_branch=scope['branch'],
               event=scope['event'], repository={'full_name':scope['repository']}, path='.github/workflows/ci.yml',
               status='completed', conclusion='success', pull_requests=[], created_at='2026-10-14T10:00:00Z')
    event = f['event']
    event.update(schema=1, event_payload_complete=True, ref='refs/pull/7/merge',
        workflow_source={'snapshot':'captured','workflow_path':run['path'],
            'workflow_ref':'example/project/.github/workflows/ci.yml@refs/pull/7/merge',
            'workflow_sha':f['merge'],'job':'shard-plan','source_bytes_verified':False},
        pull_request={'number':7,'action':'synchronize','head_repository':scope['repository'],
            'base_repository':scope['repository'],'head_sha':scope['head_sha'],'base_sha':scope['base_sha'],
            'head_ref':scope['branch'],'base_ref':'main'})
    artifacts['ci-shard-plan']['ci-event-inputs.json'] = copy.deepcopy(event)
    for name, attempt in [('engine',2),('build',2)]:
        members = artifacts['ci-inputs-'+name]
        for filename in ('build-inputs.json','post-test-inputs.json'):
            if filename in members: members[filename]['run_attempt'] = str(attempt)
        if name == 'engine':
            packet = artifacts['ci-test-results-engine']; manifest = packet['engine/manifest.json']
            provenance = manifest['provenance']; provenance['run_attempt'] = attempt
            for phase in ('reset','verify'):provenance[phase]['run_attempt'] = attempt
            provenance['case_observer']['build_inputs_sha256'] = g.checksum(fixture.encode(members['build-inputs.json']))
            marker = manifest['provenance_path']; packet['engine/files/'+marker] = fixture.encode(provenance)
            manifest['files'][marker] = g.checksum(packet['engine/files/'+marker])
            artifacts['ci-aggregate']['target/ci-shards/engine/run.json'] = fixture.encode(provenance)
    snapshots = {name:artifacts['ci-inputs-'+name]['build-inputs.json'] for name in ('rest','engine','build')}
    equality = g.receipt.compare_input_snapshots(f['plan'],snapshots['build'],{n:v for n,v in snapshots.items()if n!='build'})
    equality['input_files'] = {n:g.checksum(fixture.encode(v))for n,v in snapshots.items()}
    artifacts['ci-inputs-build']['build-input-equivalence.json'] = equality
    # A raw empty ledger is retained; this unit-only protocol fixture executes no E2E witness.
    artifacts['ci-aggregate']['e2e/target/witness-ledger.txt'] = b''
    def job(identity,name,attempt,conclusion='success'):
        return dict(id=identity,run_id=scope['run_id'],run_attempt=attempt,head_sha=scope['head_sha'],name=name,
            status='completed',conclusion=conclusion,started_at='2026-10-14T10:00:00Z',completed_at='2026-10-14T10:00:01Z',steps=[])
    jobs = {1:[job(1,'shard-plan',1),job(2,'test-shards (rest)',1),job(3,'test-shards (engine)',1,'failure')],
            2:[job(4,'test-shards (engine)',2)]}
    extra['run.json'] = run
    for attempt in (1,2):
        extra[f'attempt-{attempt}/run.json'] = dict(run,run_attempt=attempt)
        extra[f'attempt-{attempt}/jobs-1.json'] = dict(total_count=len(jobs[attempt]),jobs=jobs[attempt])
        extra[f'attempt-{attempt}/logs.zip'] = fixture.zipped({'job.txt':b'original protocol log'})
    f['reseal']()
    request = cases.two_worker_controls(f, verify=False)
    # The real collector retains every original ZIP, event, attempt and source archive.
    receipt_module = g.receipt
    api_before, command_before = receipt_module.api, receipt_module.command
    records = f['records']
    def api(endpoint):
        if endpoint.endswith('/rulesets'):return []
        if '/git/commits/' in endpoint:return f['commit'](endpoint.rsplit('/',1)[1])
        if '/commits/' in endpoint and '/check-runs?' not in endpoint:return f['commit'](scope['head_sha'])
        if '/artifacts?' in endpoint:return dict(total_count=len(records),artifacts=records)
        if '/check-runs?' in endpoint:return dict(total_count=0,check_runs=[])
        for attempt in (1,2):
            if f'/attempts/{attempt}/jobs?' in endpoint:return extra[f'attempt-{attempt}/jobs-1.json']
            if endpoint.endswith(f'/attempts/{attempt}'):return extra[f'attempt-{attempt}/run.json']
        return run
    def command(args,binary=False,cwd=None):
        if args[:2] == ['gh','api']:
            if '/artifacts/' in args[2]:
                identity=int(args[2].split('/artifacts/',1)[1].split('/')[0])
                record=next(r for r in records if r['id']==identity)
                return fixture.zipped(artifacts[record['name']])
            attempt=int(args[2].split('/attempts/',1)[1].split('/')[0])
            return extra[f'attempt-{attempt}/logs.zip']
        return command_before(args,binary=binary,cwd=cwd)
    receipt_module.api,receipt_module.command=api,command
    try:
        target=f['evidence']/'collected'
        with redirect_stdout(io.StringIO()):
            code=receipt_module.collect(argparse.Namespace(repo=scope['repository'],run=scope['run_id'],
                archive_root=str(target),source_root=str(f['work']),cache_condition='unknown',pr=7,
                head=scope['head_sha'],branch=scope['branch']))
        directory=target/f"run-{scope['run_id']}"/'attempt-2'
        collected=json.loads((directory/'receipt.json').read_bytes())
        check(code==0 and collected['archive_complete'],'complete mixed-attempt raw collection succeeds')
        check(collected['run_attempt']==2 and not collected['first_attempt']
              and json.loads((directory/'event-inputs.json').read_bytes())['run_attempt']==1,
              'current and original planner attempts remain distinct without first-attempt credit')
        source_request=f['source_request'];source_request['archive']={'path':(directory/'receipt.json').relative_to(f['evidence']).as_posix(),
            'sha256':g.checksum((directory/'receipt.json').read_bytes())}
        f['request']['source']=f['save']('source-request.json',source_request)
        request['archive_inputs']=f['save']('two-worker-request.json',f['request'])
        answer=cases.m.project(g.References(f['evidence']),f['work'],request,include_test_observations=True)
        check(answer['all_source_cases_statically_parsed'] and not answer['first_complete_PR_sample'],
              'complete mixed-attempt collector artifacts replay through source/input/case readers')
        observed={name:json.loads(row['observation_json'])['input_binding']['attempt']
                  for name,row in answer['test_observations']['workers'].items()}
        check(observed=={'rest':1,'engine':2},'each original worker attempt is retained through raw XML readback')
    finally:receipt_module.api,receipt_module.command=api_before,command_before
    original_artifacts, original_extra = copy.deepcopy(artifacts), copy.deepcopy(extra)
    for label in ('newer-failed','missing','duplicate','foreign-head','foreign-run','wrong-workflow',
                  'pre-attempt','post-attempt','result-attempt','case-attempt','build-retained','missing-page','duplicate-json','duplicate-run-json','nonfinite-json'):
        artifacts.clear();artifacts.update(copy.deepcopy(original_artifacts))
        extra.clear();extra.update(copy.deepcopy(original_extra))
        first=extra['attempt-1/jobs-1.json'];later=extra['attempt-2/jobs-1.json']
        if label=='newer-failed':later['jobs'].append(job(9,'test-shards (rest)',2,'failure'));later['total_count']+=1
        if label=='missing':first['jobs']=[j for j in first['jobs']if j['name']!='test-shards (rest)'];first['total_count']-=1
        if label=='duplicate':first['jobs'].append(job(9,'test-shards (rest)',1));first['total_count']+=1
        if label=='foreign-head':first['jobs'][1]['head_sha']='f'*40
        if label=='foreign-run':first['jobs'][1]['run_id']+=1
        if label=='wrong-workflow':extra['attempt-2/run.json']['path']='.github/workflows/other.yml'
        if label=='pre-attempt':artifacts['ci-inputs-rest']['build-inputs.json']['run_attempt']='2'
        if label=='post-attempt':artifacts['ci-inputs-engine']['post-test-inputs.json']['run_attempt']='1'
        if label=='result-attempt':
            packet=artifacts['ci-test-results-engine'];manifest=packet['engine/manifest.json']
            provenance=manifest['provenance'];provenance['run_attempt']=1
            for phase in ('reset','verify'):provenance[phase]['run_attempt']=1
            marker=manifest['provenance_path'];packet['engine/files/'+marker]=fixture.encode(provenance)
            manifest['files'][marker]=g.checksum(packet['engine/files/'+marker])
        if label=='case-attempt':artifacts['ci-inputs-engine']['observed-case-hashes.json']['input_binding']['attempt']=1
        if label=='build-retained':artifacts['ci-inputs-build']['build-inputs.json']['run_attempt']='1'
        if label=='missing-page':extra.pop('attempt-1/jobs-1.json')
        if label=='duplicate-json':
            visible=fixture.encode(later).decode().strip()
            hidden=dict(later, jobs=later['jobs']+[job(9,'test-shards (rest)',2,'failure')],total_count=2)
            # Duplicate raw keys must not erase a newer failed producer with last-wins parsing.
            extra['attempt-2/jobs-1.json']=(fixture.encode(hidden).decode().strip()[:-1]+','+visible[1:]).encode()
        if label=='duplicate-run-json':
            value=extra['attempt-2/run.json']
            extra['attempt-2/run.json']=(fixture.encode(value).decode().strip()[:-1]+',"run_attempt":2}').encode()
        if label=='nonfinite-json':
            extra['attempt-2/run.json']=(fixture.encode(extra['attempt-2/run.json']).decode().strip()[:-1]+',"unknown":NaN}').encode()
        f['reseal']();request['archive_inputs']=f['save']('two-worker-request.json',f['request'])
        try:cases.m.project(g.References(f['evidence']),f['work'],request)
        except (ValueError,KeyError,TypeError) as error:
            expected={'newer-failed':'latest worker producer','missing':'worker producer job is missing',
                'duplicate':'ambiguous','foreign-head':'job scope differs','foreign-run':'job scope differs',
                'wrong-workflow':'attempt scope differs','pre-attempt':'snapshot checkout/run/attempt',
                'post-attempt':'snapshot checkout/run/attempt','result-attempt':'provenance',
                'case-attempt':'input/inventory binding','build-retained':'snapshot checkout/run/attempt',
                'missing-page':'job pages are incomplete','duplicate-json':'duplicate',
                'duplicate-run-json':'duplicate','nonfinite-json':'nonfinite'}[label]
            check(expected in str(error),label+' rejected for wrong reason: '+str(error))
        else:raise AssertionError('mixed-attempt mutation escaped: '+label)

def retained_provider_controls(context):
    f=context['f'];artifacts=f['artifacts'];extra=f['archive_extra_files'];scope=f['scope'];scope['run_attempt']=2
    run=dict(id=scope['run_id'],run_attempt=2,head_sha=scope['head_sha'],head_branch=scope['branch'],event=scope['event'],
             repository={'full_name':scope['repository']},path='.github/workflows/ci.yml',status='completed')
    event=f['event'];event['workflow_source']={'snapshot':'captured','workflow_path':run['path'],
        'workflow_ref':'example/project/.github/workflows/ci.yml@refs/pull/7/merge','workflow_sha':f['merge'],
        'job':'shard-plan','source_bytes_verified':False}
    artifacts['ci-shard-plan']['ci-event-inputs.json']=copy.deepcopy(event)
    build=artifacts['ci-inputs-build']['build-inputs.json'];build['run_attempt']='2'
    equality=g.receipt.compare_input_snapshots(f['plan'],build,{'rest':artifacts['ci-inputs-rest']['build-inputs.json']})
    equality['input_files']={n:g.checksum(fixture.encode(artifacts['ci-inputs-'+n]['build-inputs.json']))for n in ('rest','build')}
    artifacts['ci-inputs-build']['build-input-equivalence.json']=equality
    first=extra['attempt-1/jobs-1.json'];worker=first['jobs'][0]
    worker.update(head_sha=scope['head_sha'],conclusion='success')
    first['jobs'].append(dict(worker,id=402,name='shard-plan',steps=[]));first['total_count']=2
    extra['run.json']=run
    for attempt in (1,2):extra[f'attempt-{attempt}/run.json']=dict(run,run_attempt=attempt)
    extra['attempt-2/jobs-1.json']={'total_count':0,'jobs':[]}
    extra['attempt-2/logs.zip']=fixture.zipped({'job.txt':b'no retained worker ran here'})
    context['reseal']()
    answer=context['project']()
    check(answer['scope']['run_attempt']==2 and answer['worker_producers']['rest']['run_attempt']==1
          and answer['provider_captures']['rest']['step']['job_id']==worker['id']
          and not answer['first_complete_PR_sample'],'retained provider receipt uses its original attempt jobs and logs')
    original_artifacts,original_extra=copy.deepcopy(artifacts),copy.deepcopy(extra)
    for label in ('capture-attempt','wrong-log','missing-log','failed-newer'):
        artifacts.clear();artifacts.update(copy.deepcopy(original_artifacts));extra.clear();extra.update(copy.deepcopy(original_extra))
        if label=='capture-attempt':artifacts['ci-inputs-rest']['provider-probe-receipt.json']['binding']['run_attempt']='2'
        if label=='wrong-log':extra['attempt-1/logs.zip']=extra['attempt-2/logs.zip']
        if label=='missing-log':extra.pop('attempt-1/logs.zip')
        if label=='failed-newer':
            bad=dict(worker,id=409,run_attempt=2,conclusion='failure')
            extra['attempt-2/jobs-1.json']={'total_count':1,'jobs':[bad]}
        # Keep log/expected receipt hashes consistent when testing the capture's producer itself.
        if label=='capture-attempt':context['reseal']()
        else:f['reseal']()
        try:context['project']()
        except (ValueError,KeyError,TypeError) as error:
            expected={'capture-attempt':'source/run/tree','wrong-log':'log member missing',
                      'missing-log':'mandatory archive input missing','failed-newer':'latest worker producer'}[label]
            check(expected in str(error),label+' provider rejected for wrong reason: '+str(error))
        else:raise AssertionError('retained provider mutation escaped: '+label)


def provider_fixture():
    provider=load('retained_provider_fixture','_ci-provider-readback-smoke.py')
    contract={'schema':1,'classes':{provider.CLASS:{'module':'fixture','source':provider.SOURCE,
        'source_sha256':g.checksum(provider.CODE.encode()),'providers':{'values':{'kind':'source','projection':'strict'}}}}}
    provider.fmod.run(lambda f:provider.controls(f,callback=retained_provider_controls,only_callback=True),
        only_callback=True,source_case=provider.CODE,temporary_parent='/var/tmp',
        workflow_sources={'ci.yml':(HERE.parent/'workflows/ci.yml').read_text()},
        extra_source_files={provider.CONTRACT:json.dumps(contract),provider.PRODUCER:(HERE/'_ci-evidence.py').read_bytes(),
                            provider.RESOURCE:provider.RESOURCE_BYTES})


if __name__=='__main__':
    fixture.run(controls,two_workers=True,only_callback=True,temporary_parent='/var/tmp')
    provider_fixture()
    print('CI retained worker collection/readback controls passed:',checks)
