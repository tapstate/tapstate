#!/usr/bin/env bash
# Real Git controls; only the external Actions API is a small offline fixture.
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
python3 - "$here" <<'PY'
import copy
from contextlib import redirect_stdout
import hashlib
import importlib.util
import json
import io
from pathlib import Path
import sys
import tempfile
import zipfile
sys.dont_write_bytecode=True
spec=importlib.util.spec_from_file_location('variants',Path(sys.argv[1])/'_ci-variants.py')
m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
checks=0
def check(value,message):
 global checks
 assert value,message;checks+=1
def rejected(fn,reason):
 try:fn()
 except (ValueError,KeyError,TypeError,OSError) as error:check(reason in str(error),'specific failure '+reason+': '+str(error))
 else:raise AssertionError('accepted mutation: '+reason)

with tempfile.TemporaryDirectory(prefix='ci-variants-smoke-') as temporary:
 root=Path(temporary).resolve(); repo=root/'repo';repo.mkdir(); work=root/'work'; archive=root/'archive';archive.mkdir()
 m.git(repo,'init','-q','-b','main');m.git(repo,'config','user.name','Fixture');m.git(repo,'config','user.email','fixture@example.invalid')
 m.git(repo,'config','commit.gpgsign','false');m.git(repo,'remote','add','origin','https://github.com/example/project.git')
 original='new_main_setting=retained\nmechanism=original\nschedule=4\nnew_controls=on\n'
 candidate=original.replace('mechanism=original','mechanism=optimized').replace('schedule=4','schedule=6')
 (repo/'fixture.cfg').write_text(original);(repo/'product.txt').write_text('other main product changes and original cases retained\n')
 m.git(repo,'add','.');m.git(repo,'commit','-qm','Product anchor');base=m.text(repo,'rev-parse','HEAD')
 (repo/'fixture.cfg').write_text(candidate);m.git(repo,'add','.');m.git(repo,'commit','-qm','Reviewed candidate')
 anchor=m.text(repo,'rev-parse','HEAD');m.git(repo,'worktree','add','-q','-b','ws/fixture',str(work),anchor)
 scope={'repository':'example/project','branch':'ws/fixture','worktree':str(work)}
 def ref(path):return {'path':path.relative_to(archive).as_posix(),'sha256':hashlib.sha256(path.read_bytes()).hexdigest()}
 def save(name,value):
  path=archive/name;path.parent.mkdir(parents=True,exist_ok=True);path.write_text(json.dumps(value,sort_keys=True,indent=2)+'\n');return ref(path)
 def raw(name,value):
  path=archive/name;path.parent.mkdir(parents=True,exist_ok=True);path.write_bytes(value);return ref(path)
 def patch_for(content,path='fixture.cfg'):
  with tempfile.TemporaryDirectory(prefix='ci-variant-fixture-index-') as temp:
   temp=Path(temp);blob=temp/'blob';blob.write_text(content);index=temp/'index'
   m.git(work,'read-tree',anchor+'^{tree}',index=index)
   oid=m.text(work,'hash-object','-w',str(blob));m.git(work,'update-index','--cacheinfo','100644',oid,path,index=index)
   target=m.text(work,'write-tree',index=index)
   return m.diff_patch(work,anchor+'^{tree}',target)
 patches={'A':raw('A.patch',patch_for(original)),
          'B':raw('B.patch',patch_for(candidate.replace('schedule=6','schedule=4'))),'C':raw('C.patch',b'')}
 request={'schema':1,'type':'variant-recipe-request','scope':scope,'anchor_commit':anchor,'owned_paths':['fixture.cfg'],'patches':patches}
 recipe=m.prepare(m.groups.References(archive),work,request); recipe_ref=save('recipe.json',recipe)
 check(len(set(recipe['variant_trees'].values()))==3,'real Git prepares three distinct reviewed trees')
 check(m.text(work,'rev-parse','HEAD')==anchor and not m.git(work,'status','--porcelain').stdout,'preparation does not modify branch/index/worktree')
 changed=copy.deepcopy(request);changed['anchor_commit']=base;rejected(lambda:m.prepare(m.groups.References(archive),work,changed),'current reviewed C head')
 changed=copy.deepcopy(request);changed['patches']['A']=raw('non-owned.patch',patch_for('unrelated main change lost\n','product.txt'))
 rejected(lambda:m.prepare(m.groups.References(archive),work,changed),'non-owned path')
 changed=copy.deepcopy(request);changed['patches']['A']=raw('abbreviated.patch',m.groups.file_bytes(archive/'A.patch').replace(b'index ',b'index ',1).replace(
  next(line for line in (archive/'A.patch').read_bytes().splitlines() if line.startswith(b'index ')),b'index 1234567..7654321 100644'))
 rejected(lambda:m.prepare(m.groups.References(archive),work,changed),'canonical reviewed anchor diff')
 changed=copy.deepcopy(request);changed['patches']['A']=patches['C'];rejected(lambda:m.prepare(m.groups.References(archive),work,changed),'must be distinct')
 changed=copy.deepcopy(request);changed['owned_paths']=['../product.txt'];rejected(lambda:m.prepare(m.groups.References(archive),work,changed),'escapes checkout')

 state={'published':anchor,'attempts':1,'running':False};generation=0
 contract={'applicable_workflows':['.github/workflows/ci.yml','.github/workflows/repo-hygiene.yml'],
           'required_checks':sorted(m.groups.REQUIRED_CHECKS)}
 def api_data():
  head=state['published'];merge='d'*40
  pr={'number':7,'state':'open','draft':True,'merge_commit_sha':merge,
      'head':{'sha':head,'ref':'ws/fixture','repo':{'full_name':'example/project'}},
      'base':{'sha':base,'ref':'main','repo':{'full_name':'example/project'}}}
  association={'number':7,'head':{'sha':head},'base':{'sha':base}}
  runs=[{'id':100+i,'path':path,'run_attempt':state['attempts'],'head_sha':head,'head_branch':'ws/fixture',
         'event':'pull_request','repository':{'full_name':'example/project'},'pull_requests':[association],
         'status':'in_progress' if state['running'] else 'completed','conclusion':'success'} for i,path in enumerate(contract['applicable_workflows'])]
  checks_data=[{'id':200+i,'name':name,'head_sha':merge,'status':'completed','conclusion':'success','pull_requests':[association]}
               for i,name in enumerate(contract['required_checks'])]
  return pr,runs,checks_data
 def attempt_run(run,attempt):return dict(run,run_attempt=attempt,status='completed',conclusion='failure' if attempt<state['attempts'] else 'success')
 def jobs(run,attempt):
  return [{'id':run['id']*10+attempt,'run_id':run['id'],'run_attempt':attempt,'name':'fixture','status':'completed',
           'conclusion':'success','started_at':'2026-01-01T00:00:00Z','completed_at':'2026-01-01T00:01:00Z',
           'steps':[{'number':1,'name':'original action','status':'completed','conclusion':'success'}]}]
 def api(endpoint):
  pr,runs,checks_data=api_data()
  if '/pulls/' in endpoint:return copy.deepcopy(pr)
  if '/git/commits/' in endpoint:return {'sha':'d'*40,'parents':[{'sha':base},{'sha':state['published']}]}
  if '/actions/runs?' in endpoint:return {'total_count':len(runs),'workflow_runs':copy.deepcopy(runs)}
  if '/attempts/' in endpoint:
   run_id=int(endpoint.split('/runs/')[1].split('/')[0]);attempt=int(endpoint.split('/attempts/')[1].split('/')[0].split('?')[0]);run=next(row for row in runs if row['id']==run_id)
   if '/jobs' in endpoint:return {'total_count':1,'jobs':jobs(run,attempt)}
   return attempt_run(run,attempt)
  data=checks_data if '/commits/'+('d'*40)+'/check-runs' in endpoint else []
  return {'total_count':len(data),'check_runs':copy.deepcopy(data)}
 def transition_file():
  global generation
  generation+=1;directory='transition-'+str(generation)
  pr,runs,checks_data=api_data();archives=[]
  for run in runs:
   name=directory+'/run-'+str(run['id']);path=archive/name;path.mkdir(parents=True)
   save(name+'/run.json',run)
   for attempt in range(1,state['attempts']+1):
    save(name+'/attempt-'+str(attempt)+'/run.json',attempt_run(run,attempt))
    save(name+'/attempt-'+str(attempt)+'/jobs-1.json',{'total_count':1,'jobs':jobs(run,attempt)})
    with zipfile.ZipFile(path/('attempt-'+str(attempt))/'logs.zip','w') as log:log.writestr('original-action.txt','raw original action log')
   m.groups.receipt.manifest(path,{'schema':1,'type':'raw-workflow-evidence','archive_complete':True,'gaps':[],'archive_root':str(path)})
   archives.append({'run_id':run['id'],'receipt':ref(path/'receipt.json')})
  frames={state['published']:[save(directory+'/head-checks.json',{'total_count':0,'check_runs':[]})],
          'd'*40:[save(directory+'/merge-checks.json',{'total_count':len(checks_data),'check_runs':checks_data})]}
  transition={'schema':1,'type':'serial-transition-request','scope':{'repository':'example/project','branch':'ws/fixture','pr':7,'head_sha':state['published']},
              'expected_base_sha':base,'contract':contract,'archives':archives,'check_pages':frames}
  return transition,save(directory+'/request.json',transition)
 trans,trans_ref=transition_file()
 gate=m.transition(m.groups.References(archive),trans,api)
 check(gate['archive_readback_verified'] and len(gate['archives'])==2 and not gate['first_complete_pr_sample_proven'],'terminal raw API/logs readback is operational proof, not sample acceptance')
 def archived_mutation(filename,change,reason=None):
  value=copy.deepcopy(trans);receipt_path=archive/value['archives'][0]['receipt']['path'];metadata=json.loads(receipt_path.read_text())
  path=receipt_path.parent/filename;original_bytes=path.read_bytes();observed=json.loads(original_bytes);change(observed)
  path.write_text(json.dumps(observed));m.groups.receipt.manifest(receipt_path.parent,{key:item for key,item in metadata.items() if key!='files'})
  value['archives'][0]['receipt']=ref(receipt_path)
  try:
   if reason:rejected(lambda:m.transition(m.groups.References(archive),value,api),reason)
   else:
    result=m.transition(m.groups.References(archive),value,api)
    check(result['archives'][0]['root_PR_association_unknown'] or result['archives'][0]['unassociated_attempts'],
          'missing raw associations remain unknown instead of claiming original-event proof')
  finally:
   path.write_bytes(original_bytes);m.groups.receipt.manifest(receipt_path.parent,{key:item for key,item in metadata.items() if key!='files'})
 for filename in ['run.json','attempt-1/run.json']:
  archived_mutation(filename,lambda value:value['pull_requests'][0].update(number=8),'archived PR association differs')
  archived_mutation(filename,lambda value:value['pull_requests'][0]['head'].update(sha='1'*40),'archived PR association differs')
  archived_mutation(filename,lambda value:value['pull_requests'][0]['base'].update(sha='1'*40),'archived PR association differs')
  archived_mutation(filename,lambda value:value.update(pull_requests=[]))
 bad=copy.deepcopy(trans);bad['archives'].pop();rejected(lambda:m.transition(m.groups.References(archive),bad,api),'archive set differs')
 state['running']=True;rejected(lambda:m.transition(m.groups.References(archive),trans,api),'unfinished');state['running']=False
 bad=copy.deepcopy(trans);bad['expected_base_sha']='1'*40;rejected(lambda:m.transition(m.groups.References(archive),bad,api),'base changed')
 state['attempts']=2;two,two_ref=transition_file();two_result=m.transition(m.groups.References(archive),two,api)
 check(all(row['attempts_read_back']==2 for row in two_result['archives']) and not two_result['performance_accepted'],'failed attempt and retry both read back without first-sample credit')
 state['attempts']=1
 def switch(variant,name,transition_ref=None,from_head=None):
  if transition_ref is None:unused,transition_ref=transition_file()
  value={'recipe':recipe_ref,'transition':transition_ref,'from_head':from_head or m.text(work,'rev-parse','HEAD')}
  return m.operation(m.groups.References(archive),work,value,variant,archive/name,api)
 first=switch('A','journal-A');check(m.text(work,'rev-parse','HEAD^')==anchor and first['phase']=='committed','A is an ordinary one-parent real Git commit')
 check((work/'fixture.cfg').read_text()==original and 'retained' in (work/'product.txt').read_text(),'variant patch retains new main setting, product and controls')
 rejected(lambda:switch('B','unpublished-bad',trans_ref),'previous published head')
 # Recovery before any publish: retain both ordinary commits, without moving remote state.
 previous_ref=ref(archive/'journal-A/result.json');plan_ref=ref(archive/'journal-A/planned.json')
 previous=json.loads((archive/'journal-A/result.json').read_text())
 local_value={'recipe':recipe_ref,'transition':trans_ref,'from_head':first['after_head'],
              'previous_result':previous_ref,'previous_plan':plan_ref}
 bad_plan=json.loads((archive/'journal-A/planned.json').read_text());bad_plan['scope']=dict(scope,worktree='/other/worktree')
 bad=copy.deepcopy(local_value);bad['previous_plan']=save('foreign-plan.json',bad_plan)
 rejected(lambda:m.restore_local(m.groups.References(archive),work,bad,archive/'foreign-local-recovery',api),'journal scope or source differs')
 local=m.restore_local(m.groups.References(archive),work,local_value,archive/'journal-local-C',api)
 check(m.text(work,'rev-parse','HEAD^')==first['after_head'] and m.text(work,'rev-parse','HEAD^{tree}')==recipe['anchor_tree'],'unpublished recovery appends C and preserves history')
 state['published']=m.text(work,'rev-parse','HEAD')
 a=switch('A','journal-A2');state['published']=a['after_head']
 b=switch('B','journal-B');state['published']=b['after_head']
 c=switch('C','journal-C');state['published']=c['after_head']
 check(m.text(work,'rev-parse','HEAD^')==b['after_head'] and c['source_recovered'],'published B restores C with a new ordinary commit')
 verified=m.verify_source(m.groups.References(archive),work,{'recipe':recipe_ref},'C')
 check(verified['source_matches_recipe'] and not verified['behavior_verified'],'full C source digest matches while behavior stays unverified')
 rejected(lambda:switch('C','empty-event'),'do not manufacture an empty event')
 def entry(arguments):
  old_arguments=sys.argv;old_api=m.groups.receipt.api
  try:
   sys.argv=['ci-variants',*arguments];m.groups.receipt.api=api
   output=io.StringIO()
   with redirect_stdout(output):m.main()
   return json.loads(output.getvalue())
  finally:sys.argv=old_arguments;m.groups.receipt.api=old_api
 cli_transition,cli_transition_ref=transition_file()
 transition_arguments=['verify-transition','--manifest',str(archive/cli_transition_ref['path']),
                       '--evidence-root',str(archive),'--output',str(archive/'cli-transition-result.json')]
 cli_transition_result=entry(transition_arguments)
 check(cli_transition_result['type']=='serial-transition-readback' and cli_transition_result['archive_readback_verified']
       and not cli_transition_result['first_complete_pr_sample_proven'] and not cli_transition_result['performance_accepted']
       and cli_transition_result['manifest_sha256']==cli_transition_ref['sha256']
       and json.loads((archive/'cli-transition-result.json').read_text())==cli_transition_result,
       'actual CLI verifies and writes the documented transition without a trigger policy')
 incomplete=copy.deepcopy(cli_transition);incomplete['archives'].pop()
 incomplete_ref=save('cli-incomplete-transition.json',incomplete)
 rejected(lambda:entry(['verify-transition','--manifest',str(archive/incomplete_ref['path']),
                       '--evidence-root',str(archive),'--output',str(archive/'must-not-exist.json')]),
          'archive set differs')
 check(not (archive/'must-not-exist.json').exists(),'invalid CLI transition writes no result')
 invalid_policy=dict(cli_transition,trigger_policy=None)
 invalid_policy_ref=save('cli-invalid-policy.json',invalid_policy)
 rejected(lambda:entry(['verify-transition','--manifest',str(archive/invalid_policy_ref['path']),
                       '--evidence-root',str(archive),'--output',str(archive/'must-not-exist.json')]),
          'invalid transition trigger policy reference')
 verify_manifest=save('cli-verify-request.json',{'recipe':recipe_ref})
 result=entry(['verify-source','--manifest',str(archive/verify_manifest['path']),'--evidence-root',str(archive),
               '--root',str(work),'--variant','C','--output',str(archive/'cli-verify-result.json')])
 check(result['source_matches_recipe'] and (archive/'cli-verify-result.json').is_file(),'actual CLI parser writes source verification')
 cli_request=copy.deepcopy(request);cli_request['anchor_commit']=m.text(work,'rev-parse','HEAD')
 cli_manifest=save('cli-prepare-request.json',cli_request)
 cli_recipe=entry(['prepare','--manifest',str(archive/cli_manifest['path']),'--evidence-root',str(archive),
                   '--root',str(work),'--output',str(archive/'cli-recipe.json')])
 check(cli_recipe['variant_trees']==recipe['variant_trees'],'CLI recipe preparation replays exact Git trees')
 cli_trans,cli_trans_ref=transition_file()
 operation_value={'recipe':recipe_ref,'transition':cli_trans_ref,'from_head':m.text(work,'rev-parse','HEAD')}
 op_manifest=save('cli-switch-request.json',operation_value)
 cli_a=entry(['switch','--manifest',str(archive/op_manifest['path']),'--evidence-root',str(archive),'--root',str(work),
              '--variant','A','--journal',str(archive/'cli-journal-A')])
 check(cli_a['completed'] and cli_a['after_head']==m.text(work,'rev-parse','HEAD'),'CLI switch makes an ordinary commit with a recoverable journal')
 local_value={'recipe':recipe_ref,'transition':cli_trans_ref,'from_head':cli_a['after_head'],
              'previous_result':ref(archive/'cli-journal-A/result.json'),'previous_plan':ref(archive/'cli-journal-A/planned.json')}
 local_manifest=save('cli-local-restore-request.json',local_value)
 cli_c=entry(['restore-local','--manifest',str(archive/local_manifest['path']),'--evidence-root',str(archive),
              '--root',str(work),'--journal',str(archive/'cli-journal-C')])
 check(cli_c['source_recovered'] and m.text(work,'rev-parse','HEAD^')==cli_a['after_head'],'CLI local recovery appends exact C instead of resetting history')
 state['published']=cli_c['after_head']
 operation_value={'recipe':recipe_ref,'transition':clean_ref if 'clean_ref' in locals() else transition_file()[1],
                  'from_head':m.text(work,'rev-parse','HEAD')}
 rejected(lambda:m.operation(m.groups.References(archive),work,operation_value,'A',repo/'bad-journal',api),'outside every product worktree')
 (work/'product.txt').write_text('user edit kept\n');rejected(lambda:switch('A','dirty-operation'),'checkout is not clean')
 check((work/'product.txt').read_text()=='user edit kept\n','unknown user edit is preserved')
 (work/'product.txt').write_text('other main product changes and original cases retained\n')
 clean_trans,clean_ref=transition_file()
 common=Path(m.text(work,'rev-parse','--path-format=absolute','--git-common-dir'))
 hook=common/'hooks/pre-commit';hook.write_text('#!/bin/sh\nexit 7\n');hook.chmod(0o755)
 before=m.text(work,'rev-parse','HEAD')
 rejected(lambda:switch('A','journal-hook-failure',clean_ref),'Git command failed: commit')
 check(m.text(work,'rev-parse','HEAD')==before and m.text(work,'write-tree')==recipe['variant_trees']['A'],'normal commit hook failure preserves its staged operation')
 recovery={'recipe':recipe_ref,'planned':ref(archive/'journal-hook-failure/planned.json')}
 rolled=m.rollback(m.groups.References(archive),work,recovery)
 check(rolled['head']==before and not rolled['ordinary_commit_created'] and not m.git(work,'status','--porcelain').stdout,'uncommitted failure reverses only its recorded owned patch')
 rejected(lambda:switch('A','journal-user-interruption',clean_ref),'Git command failed: commit')
 staged=(work/'fixture.cfg').read_text();(work/'fixture.cfg').write_text(staged+'user interruption\n')
 recovery={'recipe':recipe_ref,'planned':ref(archive/'journal-user-interruption/planned.json')}
 rejected(lambda:m.rollback(m.groups.References(archive),work,recovery),'unrecorded edits')
 check('user interruption' in (work/'fixture.cfg').read_text(),'recovery never overwrites an unrecorded edit')
 (work/'fixture.cfg').write_text(staged);m.rollback(m.groups.References(archive),work,recovery);hook.unlink()
 # Adding a new main change makes this frozen recipe require renewed impact review.
 (work/'product.txt').write_text('newer main retained\n');m.git(work,'add','.');m.git(work,'commit','-qm','New main input')
 newer=m.text(work,'rev-parse','HEAD');state['published']=newer
 rejected(lambda:switch('A','stale-recipe'),'not a reviewed variant')
 check((work/'product.txt').read_text()=='newer main retained\n','stale recipe cannot restore an older whole source')
 check(m.text(repo,'rev-parse','HEAD')==anchor,'all operations stay in the independent linked worktree')
 check(m.git(work,'merge-base','--is-ancestor',anchor,'HEAD',checked=False).returncode==0,
       'original anchor and experimental commits remain in append-only history')
print('ci-variants smoke: '+str(checks)+' assertions passed')
PY
