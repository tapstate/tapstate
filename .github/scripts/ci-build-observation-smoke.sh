#!/usr/bin/env bash
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
python3 -B - "$here" <<'PY_TEST'
import copy, hashlib, importlib.util, json, sys
from pathlib import Path
here=Path(sys.argv[1]); count=0

def module(name, path):
    spec=importlib.util.spec_from_file_location(name,path)
    value=importlib.util.module_from_spec(spec); spec.loader.exec_module(value); return value

receipt=module('receipt',here/'_ci-receipt.py')
reader=module('observation',here/'_ci-build-observation.py')
identity='1'*64
def metadata_fixture():
    row={'project':'io.tapstate:fixture:0.0.0','plugin':'org.apache.maven.plugins:maven-install-plugin:3.1.2','execution':'default-install','goal':'install','fully_injected_parameter_values':'not-observed'}
    artifact={'group':'org.example','artifact':'fixture','version':'1','type':'jar','classifier':None,'scope':None,'resolved':True,'file':identity}
    ordinary={**row,'merged_xml_config_sha256':'5'*64,'project_artifacts_at_event':[artifact]}
    root={'path_identity_sha256':'6'*64,'file_consumption_proven':False}
    runtime={'maven_jvm_version':'21','java_vendor':'Fixture','java_home_identity_sha256':'7'*64,'maven_core':{'file':identity},'major_jvm_file_identities':{name:{'file':identity} for name in ['release','bin/java','lib/modules','lib/ct.sym']},'capture_phase':'first-configured-mojo','whole_JDK_installation_proven':False,'selected_or_forked_compiler_proven':False}
    configured={**row,'event':'MojoConfigured','nanos':2,'configured_implementation':'org.example.FixtureMojo','effective_model_sha256':'8'*64,'active_profile_identities':[{'id_sha256':'9'*64}],'registered_compile_source_roots':[root],'registered_test_source_roots':[],'requested_concurrency':1,'runtime_identity':runtime,'compiler_consumed_files':'not-observed','selected_or_forked_compiler':'not-observed'}
    return {'schema':3,'type':'local-configured-build-input-observation','events':None,'failures':0,'dropped_events':0,'whole_build_provenance_complete':False,'hashed_bytes':4,'java_version':'21','java_vendor':'Fixture','os_arch':'fixture','annotation_processor_inputs':'not-observed','external_generator_inputs':'not-observed','elapsed_nanos':4,'files':{identity:{'state':'observed-file','size':4,'sha256':'2'*64,'path_identity_sha256':'3'*64}},'event_records':[{**ordinary,'event':'MojoStarted','nanos':1},configured,{**ordinary,'event':'MojoSucceeded','nanos':3,'plugin_artifact':artifact,'plugin_artifacts':[artifact],'realm':{'state':'not-observed','reason':'no-realm'}}]}

metadata=metadata_fixture()

def reject(name, fn):
    global count
    try: fn()
    except (ValueError, KeyError, TypeError): count+=1
    else: raise AssertionError('accepted '+name)

def snapshot(name):
    files={'.github/scripts/maven-inputs/'+p:hashlib.sha256((here/'maven-inputs'/p).read_bytes()).hexdigest() for p in ['BuildInputSpy.java','ConfiguredBuildInputListener.java','components.xml']}
    environment={'GITHUB_REPOSITORY':'tapstate/tapstate','GITHUB_WORKFLOW_REF':'tapstate/tapstate/.github/workflows/ci.yml@refs/heads/main','GITHUB_WORKFLOW_SHA':'c'*40,'GITHUB_JOB':'build' if name=='build' else 'test-shards'}
    context=receipt.input_runner_snapshot(environment,name)
    value={'source_sha':'a'*40,'source_tree':'b'*40,'run_id':'123','run_attempt':'1','event':'pull_request','source_files':files,'runner_context':context}
    fact={'schema':1,'type':'maven-execution-observation-summary','capture_status':'captured',**{k:value[k] for k in ['source_sha','source_tree','run_id','run_attempt','event','runner_context']},'observer_source_sha256':hashlib.sha256(json.dumps({Path(p).name:s for p,s in files.items()},sort_keys=True).encode()).hexdigest(),'observer_jar_sha256':'4'*64,**reader.observation_summary(json.dumps(metadata).encode())}
    value['maven_execution_observation']=fact
    return value

value=snapshot('e2e-0'); assert receipt.maven_observation_fact(value,'e2e-0')['compiler_inputs_verified'] is False; count+=1
for label,mutation in [('boolean projection',lambda x:x.update(goal_executions=True)),('unknown summary bytes',lambda x:x.update(raw_bytes='private'))]:
    changed=copy.deepcopy(value);mutation(changed['maven_execution_observation'])
    reject(label,lambda:receipt.maven_observation_fact(changed,'e2e-0'))
for field, replacement in [('source_sha','d'*40),('source_tree','d'*40),('run_id','124'),('run_attempt','2'),('event','push'),('observer_source_sha256','0'*64),('goal_executions',0),('observation_metadata_sha256','0'*64),('whole_build_provenance_complete',True),('compiler_inputs_verified',True)]:
    changed=copy.deepcopy(value); changed['maven_execution_observation'][field]=replacement
    reject(field,lambda:receipt.maven_observation_fact(changed,'e2e-0'))
for field,replacement in [('shard_id','engine'),('repository','other/repo')]:
    changed=copy.deepcopy(value);changed['maven_execution_observation']['runner_context'][field]=replacement
    reject(field,lambda:receipt.maven_observation_fact(changed,'e2e-0'))
changed=copy.deepcopy(value);changed['maven_execution_observation']['runner_context']['workflow_source']['job']='build'
reject('foreign job',lambda:receipt.maven_observation_fact(changed,'e2e-0'))
assert receipt.compare_maven_observations({}, {'e2e-0':{}}) is None; count+=1
assert receipt.compare_maven_observations(snapshot('build'),{'e2e-0':value})['whole_build_provenance_complete'] is False; count+=1
reject('mixed legacy/current',lambda:receipt.compare_maven_observations(snapshot('build'),{'e2e-0':{}}))
for name,mutate in [('missing configured',lambda x:x['event_records'].pop(1)),('reordered events',lambda x:x['event_records'].reverse()),('foreign file',lambda x:x['event_records'][-1]['plugin_artifact'].update(file='0'*64)),('dropped',lambda x:x.update(dropped_events=1)),('false whole',lambda x:x.update(whole_build_provenance_complete=True))]:
    changed=copy.deepcopy(metadata);mutate(changed);reject(name,lambda:reader.observation_summary(json.dumps(changed).encode()))
for field in metadata['event_records'][1]:
    changed=copy.deepcopy(metadata);changed['event_records'][1].pop(field)
    reject('missing configured '+field,lambda:reader.observation_summary(json.dumps(changed).encode()))
for field,replacement in [('effective_model_sha256','bad'),('configured_implementation',None),('active_profile_identities',{}),('registered_compile_source_roots',{}),('registered_test_source_roots',None),('requested_concurrency',True),('runtime_identity',{}),('compiler_consumed_files','complete'),('fully_injected_parameter_values','complete'),('selected_or_forked_compiler','complete')]:
    changed=copy.deepcopy(metadata);changed['event_records'][1][field]=replacement
    reject('invalid configured '+field,lambda:reader.observation_summary(json.dumps(changed).encode()))
for name,mutate in [('false JDK',lambda x:x['runtime_identity'].update(whole_JDK_installation_proven=True)),('false forked compiler',lambda x:x['runtime_identity'].update(selected_or_forked_compiler_proven=True)),('missing major JVM file',lambda x:x['runtime_identity']['major_jvm_file_identities'].pop('release')),('false root consumption',lambda x:x['registered_compile_source_roots'][0].update(file_consumption_proven=True)),('unhashed profile',lambda x:x['active_profile_identities'][0].update(id_sha256='bad')),('unknown raw content',lambda x:x.update(raw_parameters='private'))]:
    changed=copy.deepcopy(metadata);mutate(changed['event_records'][1]);reject(name,lambda:reader.observation_summary(json.dumps(changed).encode()))
for name,mutate in [('boolean bytes',lambda x:x.update(hashed_bytes=True)),('negative elapsed',lambda x:x.update(elapsed_nanos=-1)),('missing start facts',lambda x:x['event_records'][0].pop('merged_xml_config_sha256')),('missing success realm',lambda x:x['event_records'][-1].pop('realm')),('generator claim',lambda x:x.update(external_generator_inputs='complete'))]:
    changed=copy.deepcopy(metadata);mutate(changed);reject(name,lambda:reader.observation_summary(json.dumps(changed).encode()))
reject('duplicate keys',lambda:reader.observation_summary(b'{"schema":3,"schema":3}'))
def schema_four():
    value=metadata_fixture();value['schema']=4
    for row in value['event_records']:
        artifacts=list(row.get('project_artifacts_at_event',[]))+list(row.get('plugin_artifacts',[]))
        if 'plugin_artifact' in row:artifacts.append(row['plugin_artifact'])
        for artifact in artifacts:artifact['base_version']=artifact['version']
    return value
current=schema_four()
assert reader.observation_summary(json.dumps(current).encode())['event_count']==3;count+=1
for label,mutation in [('missing base',lambda a:a.pop('base_version')),('empty base',lambda a:a.update(base_version='')),('boolean base',lambda a:a.update(base_version=True)),('unknown field',lambda a:a.update(inferred_base=True))]:
    changed=schema_four();mutation(changed['event_records'][0]['project_artifacts_at_event'][0])
    reject(label,lambda:reader.observation_summary(json.dumps(changed).encode()))
changed=schema_four();changed['schema']=3
reject('schema3 cannot acquire current fields',lambda:reader.observation_summary(json.dumps(changed).encode()))
changed=schema_four();changed['schema']=5
reject('unsupported metadata version',lambda:reader.observation_summary(json.dumps(changed).encode()))
print('build observation smoke:',count,'controls passed')
spec=importlib.util.spec_from_file_location('archive_fixture',here/'_ci-archive-inputs-smoke.py');fixture=importlib.util.module_from_spec(spec);spec.loader.exec_module(fixture)
count=0

def callback(context):
    global count
    g=fixture.g; artifacts=context['artifacts']; plan=context['plan']; scope=context['scope']
    metadata=metadata_fixture()
    for role in ['build','rest']:
        for name,value in artifacts['ci-inputs-'+role].items():
            if name not in ('build-inputs.json','post-test-inputs.json'): continue
            environment={'GITHUB_REPOSITORY':scope['repository'],'GITHUB_WORKFLOW_REF':scope['repository']+'/.github/workflows/ci.yml@refs/pull/7/merge','GITHUB_WORKFLOW_SHA':'e'*40,'GITHUB_JOB':'build' if role=='build' else 'test-shards'}
            value['runner_context']=g.receipt.input_runner_snapshot(environment,role)
            sources={Path(p).name:h for p,h in value['source_files'].items() if p.startswith('.github/scripts/maven-inputs/')}
            value['maven_execution_observation']={'schema':1,'type':'maven-execution-observation-summary','capture_status':'captured',**{k:value[k] for k in ['source_sha','source_tree','run_id','run_attempt','event','runner_context']},'observer_source_sha256':hashlib.sha256(json.dumps(sources,sort_keys=True).encode()).hexdigest(),'observer_jar_sha256':'4'*64,**reader.observation_summary(json.dumps(metadata).encode())}
    # Each role owns a separate snapshot even when older fixtures shared an object.
    artifacts['ci-inputs-build']['build-inputs.json']=copy.deepcopy(artifacts['ci-inputs-build']['build-inputs.json'])
    artifacts['ci-inputs-build']['build-inputs.json']['runner_context']=g.receipt.input_runner_snapshot({'GITHUB_REPOSITORY':scope['repository'],'GITHUB_WORKFLOW_REF':scope['repository']+'/.github/workflows/ci.yml@refs/pull/7/merge','GITHUB_WORKFLOW_SHA':'e'*40,'GITHUB_JOB':'build'},'build')
    artifacts['ci-inputs-build']['build-inputs.json']['maven_execution_observation']['runner_context']=copy.deepcopy(artifacts['ci-inputs-build']['build-inputs.json']['runner_context'])
    artifacts['ci-inputs-rest']['build-inputs.json']['runner_context']=g.receipt.input_runner_snapshot({'GITHUB_REPOSITORY':scope['repository'],'GITHUB_WORKFLOW_REF':scope['repository']+'/.github/workflows/ci.yml@refs/pull/7/merge','GITHUB_WORKFLOW_SHA':'e'*40,'GITHUB_JOB':'test-shards'},'rest')
    artifacts['ci-inputs-rest']['build-inputs.json']['maven_execution_observation']['runner_context']=copy.deepcopy(artifacts['ci-inputs-rest']['build-inputs.json']['runner_context'])
    artifacts['ci-inputs-rest']['post-test-inputs.json']['runner_context']=copy.deepcopy(artifacts['ci-inputs-rest']['build-inputs.json']['runner_context'])
    artifacts['ci-inputs-rest']['post-test-inputs.json']['maven_execution_observation']=copy.deepcopy(artifacts['ci-inputs-rest']['build-inputs.json']['maven_execution_observation'])
    pre=artifacts['ci-inputs-rest']['build-inputs.json']; manifest=artifacts['ci-test-results-rest']['rest/manifest.json']; marker=context['marker']
    manifest['provenance']['case_observer']['build_inputs_sha256']=g.checksum(fixture.encode(pre))
    provenance=fixture.encode(manifest['provenance']);artifacts['ci-test-results-rest']['rest/files/'+marker]=provenance;manifest['files'][marker]=g.checksum(provenance)
    artifacts['ci-aggregate']['target/ci-shards/rest/run.json']=provenance
    equality=g.receipt.compare_input_snapshots(plan,artifacts['ci-inputs-build']['build-inputs.json'],{'rest':pre})
    equality['input_files']={role:g.checksum(fixture.encode(artifacts['ci-inputs-'+role]['build-inputs.json'])) for role in ['build','rest']}
    artifacts['ci-inputs-build']['build-input-equivalence.json']=equality
    context['reseal'](); answer=context['project'](); assert answer['archived_input_members_verified'] and not answer['whole_build_provenance_complete'];count+=1
    original=copy.deepcopy(artifacts)
    for label,change in [('old run',lambda a:a['ci-inputs-rest']['build-inputs.json']['maven_execution_observation'].update(run_id='old')),('missing post capture',lambda a:a['ci-inputs-rest']['post-test-inputs.json'].pop('maven_execution_observation')),('post mismatch',lambda a:a['ci-inputs-rest']['post-test-inputs.json']['maven_execution_observation'].update(observer_jar_sha256='0'*64)),('mixed capture',lambda a:a['ci-inputs-build']['build-inputs.json'].pop('maven_execution_observation'))]:
        artifacts.clear();artifacts.update(copy.deepcopy(original));change(artifacts);context['reseal']()
        try:context['project']()
        except ValueError:count+=1
        else:raise AssertionError('archive accepted '+label)

fixture.run(callback,only_callback=True,maven_source=True)
print('build observation archive controls:',count,'passed')
PY_TEST
