#!/usr/bin/env bash
# Real Git source/archive controls; no workflow/API/Docker operation occurs.
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
PYTHONDONTWRITEBYTECODE=1 python3 - "$here" <<'PY'
import copy, hashlib, importlib.util, io, json, os, subprocess, sys, tarfile, tempfile
from pathlib import Path
spec=importlib.util.spec_from_file_location('projection',Path(sys.argv[1])/'_ci-source-projection.py')
m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
v=m.variants;g=m.groups;count=0

def check(value,message):
 global count
 assert value,message
 count+=1

def rejected(call,message):
 try:call()
 except (ValueError,OSError,KeyError,TypeError,tarfile.TarError) as error:
  check(message in str(error),'wrong rejection: '+str(error))
 else:raise AssertionError('expected rejection: '+message)

with tempfile.TemporaryDirectory(prefix='ci-source-projection-') as temporary:
 root=Path(temporary).resolve();main=root/'main';work=root/'owned';evidence=root/'evidence';evidence.mkdir()
 subprocess.run(['git','init','-q',str(main)],check=True)
 for key,value in [('user.name','Fixture'),('user.email','fixture@example.test'),('commit.gpgsign','false')]:
  v.git(main,'config',key,value)
 (main/'product.txt').write_text('preserved product fix\n')
 (main/'nested').mkdir();(main/'nested/member.txt').write_text('complete source directory\n')
 (main/'fixture.cfg').write_text('base\n')
 v.git(main,'add','.');v.git(main,'commit','-qm','Initial source')
 base=v.text(main,'rev-parse','HEAD')
 (main/'fixture.cfg').write_text('C\n');v.git(main,'add','.');v.git(main,'commit','-qm','Reviewed candidate source')
 anchor=v.text(main,'rev-parse','HEAD');tree=v.text(main,'rev-parse','HEAD^{tree}')
 v.git(main,'remote','add','origin','https://github.com/example/project.git')
 v.git(main,'worktree','add','-qb','ws/fixture',str(work),anchor)
 scope={'repository':'example/project','branch':'ws/fixture','worktree':str(work)}
 def save(name,value):
  path=evidence/name;path.parent.mkdir(parents=True,exist_ok=True)
  data=value if isinstance(value,bytes) else (json.dumps(value,sort_keys=True)+'\n').encode()
  path.write_bytes(data);return {'path':name,'sha256':g.checksum(data)}
 def patch(label):
  index=root/('index-'+label)
  v.git(work,'read-tree',tree,index=index)
  blob=root/('blob-'+label);blob.write_text(label+'\n')
  oid=v.text(work,'hash-object','-w',str(blob))
  v.git(work,'update-index','--cacheinfo','100644',oid,'fixture.cfg',index=index)
  target=v.text(work,'write-tree',index=index)
  return v.diff_patch(work,tree,target)
 request={'schema':1,'type':'variant-recipe-request','scope':scope,'anchor_commit':anchor,
          'owned_paths':['fixture.cfg'],'patches':{x:save(x+'.patch',patch(x) if x!='C' else b'') for x in 'ABC'}}
 recipe=v.prepare(g.References(evidence),work,request);recipe_ref=save('recipe.json',recipe)
 before=v.text(work,'rev-parse','HEAD');index_before=v.git(work,'write-tree').stdout
 merge=v.text(work,'commit-tree',tree,'-p',base,'-p',anchor,'-m','Fixture PR checkout')
 def commit_json(sha):return {'sha':sha,'tree':{'sha':v.text(work,'rev-parse',sha+'^{tree}')},
                              'parents':[{'sha':p} for p in v.text(work,'rev-list','--parents','-n','1',sha).split()[1:]]}
 def archive():
  directory=evidence/'run';directory.mkdir(exist_ok=True)
  data={'run.json':{'id':42,'run_attempt':1,'head_sha':anchor,'head_branch':'ws/fixture','event':'pull_request',
                    'repository':{'full_name':'example/project'},'pull_requests':[{'number':7,'head':{'sha':anchor},'base':{'sha':base}}]},
        'commit.json':commit_json(anchor),'source.tar':v.git(work,'archive','--format=tar',anchor).stdout,
        'checkout-commit.json':commit_json(merge),'checkout-source.tar':v.git(work,'archive','--format=tar',merge).stdout,
        'event-inputs.json':{'repository':'example/project','event':'pull_request','run_id':42,'run_attempt':1,
                             'checkout_sha':merge,'checkout_tree':tree,'checkout_parents':[base,anchor]}}
  for name,value in data.items():save('run/'+name,value)
  return seal()
 def seal():
  directory=evidence/'run'
  files={p.relative_to(directory).as_posix():{'size':p.stat().st_size,'sha256':g.checksum(p.read_bytes())}
         for p in directory.rglob('*') if p.is_file() and p.name!='receipt.json'}
  receipt={'archive_root':str(directory),'repository':'example/project','run_id':42,'run_attempt':1,
           'event':'pull_request','source_sha':anchor,'archive_complete':True,'gaps':[],
           'purpose':'source-readback fixture only; not full workflow evidence','checkout_sha':merge,'files':files}
  return save('run/receipt.json',receipt)
 archive_ref=archive()
 request={'schema':1,'type':'source-projection-request','recipe':recipe_ref,'archive':archive_ref,
          'scope':{'repository':'example/project','branch':'ws/fixture','variant':'C','head_sha':anchor,
                   'run_id':42,'run_attempt':1,'event':'pull_request','pr':7,'base_sha':base}}
 def project(req=None):return m.project(g.References(evidence),work,request if req is None else req)
 actual=project()
 check(actual['archived_source_readback_verified'],'complete archived source is read back')
 check(actual['preserved_source_digest']==recipe['preserved_source_digest'],'preserved basis derives from source bytes')
 check(actual['head']['source_digest']==recipe['source_digests']['C'],'complete source digest matches the recipe')
 check(actual['checkout']['tree']==tree,'checkout tree is checked independently')
 check(actual['whole_input_projection_verified'] is False and actual['first_complete_PR_sample'] is False
       and actual['performance_accepted'] is False,'source readback cannot certify remaining inputs or PR performance')
 check(v.text(work,'rev-parse','HEAD')==before and v.git(work,'write-tree').stdout==index_before
       and not v.git(work,'status','--porcelain').stdout,'projection leaves checkout/index/ref unchanged')
 manifest=evidence/'request.json';manifest.write_text(json.dumps(request))
 answer=evidence/'cli-result.json'
 child=subprocess.run(['bash',str(Path(sys.argv[1])/'ci-source-projection.sh'),'--manifest',str(manifest),
                       '--evidence-root',str(evidence),'--root',str(work),'--output',str(answer)],capture_output=True,text=True)
 check(child.returncode==0 and json.loads(answer.read_text())['head']==actual['head'],'CLI reads real source records')
 before_output=answer.read_bytes()
 again=subprocess.run(child.args,capture_output=True,text=True)
 check(again.returncode!=0 and answer.read_bytes()==before_output,'CLI cannot overwrite an existing output')
 saved=dict(os.environ)
 try:
  os.environ.update(GIT_DIR=str(main/'.git'),GIT_INDEX_FILE=str(root/'foreign-index'),GIT_CONFIG_COUNT='1',
                    GIT_CONFIG_KEY_0='alias.archive',GIT_CONFIG_VALUE_0='!false')
  check(project()['head']==actual['head'],'source Git ignores inherited worktree/index/config injection')
 finally:
  os.environ.clear();os.environ.update(saved)
 def mutate_tar(name,change):
  path=evidence/'run'/name;raw=path.read_bytes()
  with tarfile.open(fileobj=io.BytesIO(raw),mode='r:') as tar:
   entries=[(copy.copy(t),tar.extractfile(t).read() if t.isfile() else b'') for t in tar]
   pax=dict(tar.pax_headers)
  out=io.BytesIO()
  with tarfile.open(fileobj=out,mode='w',format=tarfile.PAX_FORMAT,pax_headers=pax) as tar:
   change(entries,pax,tar)
  path.write_bytes(out.getvalue());request['archive']=seal()
 def copy_entries(entries,pax,tar):
  for member,data in entries:tar.addfile(member,io.BytesIO(data) if member.isfile() else None)
 def changed_bytes(entries,pax,tar):
  for member,data in entries:
   if member.name=='product.txt':data=b'foreign source bytes\n';member.size=len(data)
   tar.addfile(member,io.BytesIO(data) if member.isfile() else None)
 mutate_tar('source.tar',changed_bytes);rejected(project,'path/mode/blob map differs')
 request['archive']=archive()
 def extra_directory(entries,pax,tar):
  copy_entries(entries,pax,tar)
  member=tarfile.TarInfo('unexpected');member.type=tarfile.DIRTYPE;member.mode=0o755;tar.addfile(member)
 def missing_directory(entries,pax,tar):
  for member,data in entries:
   if member.isdir() and member.name.rstrip('/')=='nested':continue
   tar.addfile(member,io.BytesIO(data) if member.isfile() else None)
 probes={}
 for name,change in [('extra_directory',extra_directory),('missing_directory',missing_directory)]:
  mutate_tar('source.tar',change)
  try:project()
  except ValueError as error:
   check('directory set differs' in str(error),'directory mutation rejected for wrong reason')
   probes[name+'_accepted']=False
  else:probes[name+'_accepted']=True
  request['archive']=archive()
 receipt=json.loads((evidence/'run/receipt.json').read_text());receipt['checkout_sha']='a'*40
 request['archive']=save('run/receipt.json',receipt)
 try:project()
 except ValueError as error:
  check('receipt checkout differs' in str(error),'receipt mutation rejected for wrong reason')
  probes['foreign_receipt_checkout_accepted']=False
 else:probes['foreign_receipt_checkout_accepted']=True
 request['archive']=archive()
 print(json.dumps(probes,sort_keys=True))
 check(not any(probes.values()),'directory or checkout binding gap reproduced')
 receipt=json.loads((evidence/'run/receipt.json').read_text());receipt.pop('checkout_sha')
 request['archive']=save('run/receipt.json',receipt)
 check('receipt_checkout_binding' in project()['unverified_components'],'absent receipt checkout stays unverified')
 request['archive']=archive()
 def changed_mode(entries,pax,tar):
  for member,data in entries:
   if member.name=='product.txt':member.mode=0o755
   tar.addfile(member,io.BytesIO(data) if member.isfile() else None)
 mutate_tar('source.tar',changed_mode);rejected(project,'path/mode/blob map differs')
 request['archive']=archive()
 def special_mode(entries,pax,tar):
  for member,data in entries:
   if member.name=='product.txt':member.mode=0o4755
   tar.addfile(member,io.BytesIO(data) if member.isfile() else None)
 mutate_tar('source.tar',special_mode);rejected(project,'special permissions')
 request['archive']=archive()
 def sparse(entries,pax,tar):
  for member,data in entries:
   if member.name=='product.txt':member.pax_headers={'GNU.sparse.name':'product.txt'}
   tar.addfile(member,io.BytesIO(data) if member.isfile() else None)
 mutate_tar('source.tar',sparse);rejected(project,'sparse source archive entry')
 request['archive']=archive()
 def duplicate(entries,pax,tar):
  copy_entries(entries,pax,tar)
  member,data=next((x,y) for x,y in entries if x.name=='product.txt')
  tar.addfile(member,io.BytesIO(data))
 mutate_tar('source.tar',duplicate);rejected(project,'duplicate source archive entry')
 request['archive']=archive()
 def escape(entries,pax,tar):
  copy_entries(entries,pax,tar);member=tarfile.TarInfo('../outside');member.size=1;tar.addfile(member,io.BytesIO(b'x'))
 mutate_tar('source.tar',escape);rejected(project,'escapes checkout')
 request['archive']=archive()
 def hardlink(entries,pax,tar):
  copy_entries(entries,pax,tar);member=tarfile.TarInfo('alias');member.type=tarfile.LNKTYPE;member.linkname='product.txt';tar.addfile(member)
 mutate_tar('source.tar',hardlink);rejected(project,'unsupported source archive entry')
 request['archive']=archive()
 (evidence/'run'/'source.tar').unlink();request['archive']=seal();rejected(project,'mandatory source archive input missing')
 request['archive']=archive()
 value=commit_json(anchor);value['tree']['sha']='a'*40;save('run/commit.json',value);request['archive']=seal()
 rejected(project,'source commit/tree differs');request['archive']=archive()
 value=json.loads((evidence/'run/run.json').read_text());value['pull_requests'][0]['number']=8
 save('run/run.json',value);request['archive']=seal();rejected(project,'source PR differs');request['archive']=archive()
 value=copy.deepcopy(request);value['scope']['variant']='A';rejected(lambda:project(value),'source commit/tree differs')
 value=json.loads((evidence/'run/checkout-commit.json').read_text());value['parents']=[]
 save('run/checkout-commit.json',value);request['archive']=seal();rejected(project,'source parents differ');request['archive']=archive()
 value=json.loads((evidence/'run/event-inputs.json').read_text());value['checkout_parents']=[anchor,base]
 save('run/event-inputs.json',value);request['archive']=seal();rejected(project,'checkout PR parents differ');request['archive']=archive()
 value=json.loads((evidence/'run/event-inputs.json').read_text());value['run_id']=43
 save('run/event-inputs.json',value);request['archive']=seal();rejected(project,'checkout event source differs');request['archive']=archive()
 value=copy.deepcopy(request);value['scope']['run_id']=True
 rejected(lambda:project(value),'invalid source run/attempt')
 value=copy.deepcopy(request);value['scope']['extra']='ignored'
 rejected(lambda:project(value),'source scope fields differ')
 (evidence/'run'/'source.tar').write_bytes(b'changed without resealing')
 rejected(project,'file set/size/SHA-256 differs');request['archive']=archive()
 collapsed=copy.deepcopy(recipe)
 collapsed['variant_trees']={x:tree for x in 'ABC'};collapsed['source_digests']={x:recipe['source_digests']['C'] for x in 'ABC'}
 collapsed['patch_files']={x:recipe['patch_files']['C'] for x in 'ABC'}
 value=copy.deepcopy(request);value['recipe']=save('collapsed-recipe.json',collapsed)
 rejected(lambda:project(value),'must be distinct')
 duplicate_paths=copy.deepcopy(recipe);duplicate_paths['owned_paths']=['fixture.cfg','fixture.cfg']
 value=copy.deepcopy(request);value['recipe']=save('duplicate-paths.json',duplicate_paths)
 rejected(lambda:project(value),'duplicate owned paths')
 empty_paths=copy.deepcopy(recipe);empty_paths['owned_paths']=[]
 value=copy.deepcopy(request);value['recipe']=save('empty-paths.json',empty_paths)
 rejected(lambda:project(value),'empty or duplicate owned paths')
 value=copy.deepcopy(request);value['scope']['run_attempt']=2
 run=json.loads((evidence/'run/run.json').read_text());run['run_attempt']=2;save('run/run.json',run)
 event=json.loads((evidence/'run/event-inputs.json').read_text());event['run_attempt']=2;save('run/event-inputs.json',event)
 receipt=json.loads((evidence/'run/receipt.json').read_text());receipt['run_attempt']=2
 ref=seal();sealed=json.loads((evidence/'run/receipt.json').read_text());sealed['run_attempt']=2
 value['archive']=save('run/receipt.json',sealed)
 rejected(lambda:project(value),'mandatory source archive input missing: attempt-1/run.json')
 # Keep the event at planner attempt 1 while current/API/receipt remain attempt 2.
 event['run_attempt']=1
 event['workflow_source']={'snapshot':'captured','workflow_path':'.github/workflows/ci.yml',
   'workflow_ref':'example/project/.github/workflows/ci.yml@refs/pull/7/merge',
   'workflow_sha':merge,'job':'shard-plan','source_bytes_verified':False}
 save('run/event-inputs.json',event)
 def retry_pages():
  for attempt in (1,2):
   previous=dict(run,run_attempt=attempt,status='completed',path='.github/workflows/ci.yml')
   save('run/attempt-'+str(attempt)+'/run.json',previous)
   jobs=([{'id':1,'run_id':42,'run_attempt':1,'head_sha':anchor,'name':'shard-plan',
           'status':'completed','conclusion':'success'}] if attempt==1 else [])
   save('run/attempt-'+str(attempt)+'/jobs-1.json',{'total_count':len(jobs),'jobs':jobs})
 def seal_retry():
  seal();record=json.loads((evidence/'run/receipt.json').read_text());record['run_attempt']=2
  return save('run/receipt.json',record)
 run.update(status='completed',path='.github/workflows/ci.yml');save('run/run.json',run)
 retry_pages();value['archive']=seal_retry()
 retained=project(value)
 check(retained['archived_source_readback_verified'] and 'first_attempt' in retained['unverified_components'],
       'PR retry reads back the retained planner without first-sample credit')
 # A newly rerun planner is also valid, with its own actual successful job.
 event['run_attempt']=2;save('run/event-inputs.json',event)
 save('run/attempt-2/jobs-1.json',{'total_count':1,'jobs':[{'id':2,'run_id':42,'run_attempt':2,
   'head_sha':anchor,'name':'shard-plan','status':'completed','conclusion':'success'}]})
 value['archive']=seal_retry();check(project(value)['first_complete_PR_sample'] is False,
                                   'rerun planner still cannot become a first PR sample')
 event['run_attempt']=1;save('run/event-inputs.json',event)
 for label in ('missing','duplicate','failed','newer','future','foreign-head','foreign-run'):
  retry_pages();candidate=copy.deepcopy(event)
  name='run/attempt-1/jobs-1.json';page=json.loads((evidence/name).read_text())
  if label=='missing':(evidence/name).unlink()
  if label=='duplicate':page['jobs'].append(dict(page['jobs'][0],id=9));page['total_count']=2
  if label=='failed':page['jobs'][0]['conclusion']='failure'
  if label=='newer':save('run/attempt-2/jobs-1.json',{'total_count':1,'jobs':[dict(page['jobs'][0],id=9,run_attempt=2)]})
  if label=='future':candidate['run_attempt']=3
  if label=='foreign-head':page['jobs'][0]['head_sha']='f'*40
  if label=='foreign-run':candidate['run_id']=43
  if label!='missing':save(name,page)
  save('run/event-inputs.json',candidate);value['archive']=seal_retry()
  try:project(value)
  except (ValueError,KeyError):check(True,'retry rejects '+label)
  else:raise AssertionError('retained planner mutation escaped: '+label)
 for path in (evidence/'run').glob('attempt-*/*'):path.unlink()
 for path in (evidence/'run').glob('attempt-*'):path.rmdir()
 request['archive']=archive()
 value=copy.deepcopy(request);value['scope']['event']='workflow_dispatch';value['scope'].pop('pr');value['scope'].pop('base_sha')
 run=json.loads((evidence/'run/run.json').read_text());run['event']='workflow_dispatch';save('run/run.json',run)
 event=json.loads((evidence/'run/event-inputs.json').read_text());event.update(event='workflow_dispatch',checkout_sha=anchor,checkout_parents=[base])
 save('run/event-inputs.json',event);save('run/checkout-commit.json',commit_json(anchor))
 save('run/checkout-source.tar',v.git(work,'archive','--format=tar',anchor).stdout)
 ref=seal();sealed=json.loads((evidence/'run/receipt.json').read_text());sealed.update(event='workflow_dispatch',checkout_sha=anchor)
 value['archive']=save('run/receipt.json',sealed)
 dispatched=project(value);check('pull_request_event' in dispatched['unverified_components'] and dispatched['first_complete_PR_sample'] is False,
                              'dispatch source readback cannot become a PR first sample')
 original_dispatch=copy.deepcopy(event);original_run=copy.deepcopy(run)
 run.update(run_attempt=2,status='completed',path='.github/workflows/ci.yml');save('run/run.json',run)
 event['workflow_source']={'snapshot':'captured','workflow_path':'.github/workflows/ci.yml',
   'workflow_ref':'example/project/.github/workflows/ci.yml@refs/heads/ws/fixture',
   'workflow_sha':anchor,'job':'shard-plan','source_bytes_verified':False}
 save('run/event-inputs.json',event);retry_pages()
 value['scope']['run_attempt']=2
 value['archive']=seal_retry();record=json.loads((evidence/'run/receipt.json').read_text())
 record.update(event='workflow_dispatch',checkout_sha=anchor);value['archive']=save('run/receipt.json',record)
 check(project(value)['archived_source_readback_verified'], 'dispatch retry retains its independently proven planner')
 for path in (evidence/'run').glob('attempt-*/*'):path.unlink()
 for path in (evidence/'run').glob('attempt-*'):path.rmdir()
 event=original_dispatch;run=original_run;save('run/event-inputs.json',event);save('run/run.json',run)
 value['scope']['run_attempt']=1
 seal();sealed=json.loads((evidence/'run/receipt.json').read_text());sealed.update(event='workflow_dispatch',checkout_sha=anchor)
 value['archive']=save('run/receipt.json',sealed)
 sealed['checkout_sha']='a'*40;value['archive']=save('run/receipt.json',sealed)
 try:project(value)
 except ValueError as error:
  check('dispatch receipt checkout differs' in str(error),'dispatch mismatch rejected for wrong reason')
  accepted=False
 else:accepted=True
 print(json.dumps({'foreign_dispatch_checkout_accepted':accepted}))
 check(not accepted,'foreign dispatch checkout binding gap reproduced')
 def seal_dispatch(checkout_sha=anchor):
  seal();record=json.loads((evidence/'run/receipt.json').read_text());record['event']='workflow_dispatch'
  if checkout_sha is None:record.pop('checkout_sha')
  else:record['checkout_sha']=checkout_sha
  return save('run/receipt.json',record)
 value['archive']=seal_dispatch()
 event['checkout_sha']='a'*40;save('run/event-inputs.json',event);value['archive']=seal_dispatch()
 rejected(lambda:project(value),'dispatch checkout event differs')
 event['checkout_sha']=anchor;event['checkout_parents']=[];save('run/event-inputs.json',event);value['archive']=seal_dispatch()
 rejected(lambda:project(value),'dispatch checkout parents differ')
 event['checkout_parents']=[base];save('run/event-inputs.json',event)
 (evidence/'run/checkout-source.tar').unlink();value['archive']=seal_dispatch()
 rejected(lambda:project(value),'dispatch checkout source is incomplete')
 (evidence/'run/checkout-commit.json').unlink();(evidence/'run/event-inputs.json').unlink()
 value['archive']=seal_dispatch(None);limited=project(value)
 check({'receipt_checkout_binding','archived_checkout_event','archived_checkout_source'} <= set(limited['unverified_components'])
       and limited['checkout'] is None,'absent dispatch checkout records remain explicitly unverified')
 request['archive']=archive()
 check(not (root/'outside').exists(),'unsafe tar members are never extracted')
 print('ci-source-projection smoke:',count,'assertions passed')
PY
