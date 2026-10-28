#!/usr/bin/env bash
# Protect the actual analysis path and its exact native required check name.
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
python3 - "$here/../workflows" <<'PY'
from pathlib import Path
import fnmatch, json, os, re, subprocess, sys, tempfile
root=Path(sys.argv[1]); text=(root/'ci.yml').read_text()
def block(value, start, boundary):
    found=re.search(start, value, re.M)
    assert found, 'missing workflow block: '+start
    remainder=value[found.end():]
    end=re.search(boundary,remainder,re.M)
    return remainder[:end.start()] if end else remainder
def job(name): return block(text,r'^  '+name+r':\n',r'^  [\w-]+:')
def step(body,name): return block(body,r'^      - name: '+re.escape(name)+r'\n',r'^      - ')
def script(body):
    command=block(body,r'^        run: \|\n',r'^ {0,9}\S')
    return '\n'.join(line[10:] for line in command.splitlines())
build=job('build'); reporter=job('sonarqube')
assert not (root/'sonarqube.yml').exists(), 'duplicate Sonar workflow still exists'
assert re.search(r'^    tags:\n      - [\'"]v\*[\'"]',text,re.M)
assert re.search(r'^  workflow_dispatch:',text,re.M)
assert 'pull_request:' in text and '- main' in text
assert 'matrix:' not in build and 'matrix:' not in reporter
assert re.search(r'^    name: sonarqube$',reporter,re.M), 'native check name must be sonarqube'
assert 'SONAR_REQUIRED' not in text and 'ci-summary' not in text, 'dead branch-push analysis path remains'
def analysis_locks(ci_text, manual):
    assert not re.search(r'^concurrency:', ci_text, re.M), 'Sonar lock must not serialize the entire CI workflow'
    ci_build=block(ci_text,r'^  build:\n',r'^  [\w-]+:')
    lock=block(ci_build,r'^    concurrency:\n',r'^    \S')
    assert "      group: ${{ format('sonarqube-{0}', github.event.pull_request.number || github.ref) }}" in lock
    assert re.search(r'^      cancel-in-progress: false$', lock, re.M), 'in-flight server analysis must finish'
    assert re.search(r'^      queue: max$', lock, re.M), 'multiple pending analyses must not replace each other'
    assert not re.search(r'^concurrency:', manual, re.M), 'manual analysis must lock its analysis job'
    manual_job=block(manual,r'^  analyze:\n',r'^  [\w-]+:')
    manual_lock=block(manual_job,r'^    concurrency:\n',r'^    \S')
    assert re.search(r'^      group: sonarqube-\$\{\{ inputs.pr \}\}$', manual_lock, re.M)
    assert re.search(r'^      cancel-in-progress: false$', manual_lock, re.M)
    assert re.search(r'^      queue: max$', manual_lock, re.M), 'manual analysis must retain the same queue policy'
    for name in ('shard-plan', 'test-shards', 'sonarqube'):
        body=block(ci_text,r'^  '+name+r':\n',r'^  [\w-]+:')
        assert 'sonarqube-' not in body, 'tests and outcome reporting must not acquire the analysis lock'
manual=(root/'sonar-pr.yml').read_text()
analysis_locks(text,manual)
lock_mutations=0
for target, original, replacement in [
    ('ci', "      group: ${{ format('sonarqube-{0}', github.event.pull_request.number || github.ref) }}", '      group: unrelated-analysis'),
    ('ci', '      cancel-in-progress: false', '      cancel-in-progress: true'),
    ('ci', '      queue: max', '      queue: single'),
    ('ci', 'permissions:', 'concurrency: sonarqube-global\npermissions:'),
    ('ci', '  test-shards:', '  test-shards:\n    concurrency: sonarqube-workers'),
    ('manual', '      group: sonarqube-${{ inputs.pr }}', '      group: unrelated-analysis'),
    ('manual', '      cancel-in-progress: false', '      cancel-in-progress: true'),
    ('manual', '      queue: max', '      queue: single'),
    ('manual', 'permissions:', 'concurrency: sonarqube-global\npermissions:'),
]:
    original_text=text if target=='ci' else manual
    assert original in original_text
    if target=='ci' and original in ('      cancel-in-progress: false', '      queue: max'):
        before_build, build_and_tail=original_text.split('  build:\n',1)
        changed=before_build+'  build:\n'+build_and_tail.replace(original,replacement,1)
    else:
        changed=original_text.replace(original,replacement,1)
    try:
        analysis_locks(changed if target=='ci' else text,changed if target=='manual' else manual)
    except AssertionError:
        lock_mutations+=1
    else:
        raise AssertionError('escaped analysis lock mutation: '+replacement)
# Bind cases to the actual subscribed event/ref set; ws pushes must not be a test-only lane.
trigger=block(text,r'^on:\n',r'^\S')
assert re.search(r'^  push:\n    branches:\n      - main\n',trigger,re.M)
assert 'ws/' not in trigger
def subscribed(event, ref):
    event_body=block(trigger,r'^  '+event+r':\n',r'^  \S')
    if event=='workflow_dispatch':
        return True
    kind='tags' if ref.startswith('refs/tags/') else 'branches'
    patterns=block(event_body,r'^    '+kind+r':\n',r'^    \S')
    names=re.findall(r'^      - [\'"]?([^\'"\n]+?)[\'"]?$',patterns,re.M)
    return any(fnmatch.fnmatchcase(ref.split('/',2)[2], name) for name in names)
for event,ref,expected in [('push','refs/heads/main',True),('pull_request','refs/heads/main',True),
                           ('push','refs/tags/v1.2.3',True),('workflow_dispatch','refs/heads/main',True),
                           ('workflow_dispatch','refs/heads/ws/example',True),
                           ('push','refs/heads/ws/example',False),('push','refs/tags/other',False)]:
    assert subscribed(event,ref)==expected, (event,ref,'workflow trigger changed')
assert '        if:' not in step(build,'Check secret availability'), 'every subscribed event evaluates secret availability'
for output, source in [('sonar-enabled','steps.guard.outputs.run'),('sonar-outcome','steps.analyze.outcome')]:
    assert '      '+output+': ${{ '+source+' }}' in build.splitlines(), 'analysis output binding changed: '+output
for variable, output in [('SONAR_ENABLED','sonar-enabled'),('SONAR_OUTCOME','sonar-outcome')]:
    assert '          '+variable+': ${{ needs.build.outputs.'+output+' }}' in reporter.splitlines(), 'required check input binding changed: '+variable
assert 'needs: build' in reporter and 'always()' in reporter
assert 'sonarqube-' in text and 'cancel-in-progress: true' not in text
admit=step(build,'Admit the complete report and coverage set'); analyze=step(build,'Analyze admitted coverage')
assert 'ci-aggregate.sh verify' in admit and '--artifacts' in admit and '--restore .' in admit
assert '        if:' not in admit and 'continue-on-error:' not in admit, 'admission must always gate coverage and analysis'
assert len(re.findall(r'ci-aggregate\.sh verify|sonar-inputs\.sh', build))==1, 'artifact admission must execute exactly once'
assert build.index('Admit the complete report and coverage set') < build.index('Merge coverage from every declared shard') < build.index('Analyze admitted coverage')
assert 'mvn -B -Dmaven.repo.local="$RUNNER_TEMP/m2" sonar:sonar' in analyze
assert not re.search(r'\bmvn\b[^\n]*\bverify\b',analyze)
assert 'continue-on-error: true' in analyze
assert "steps.guard.outputs.run == 'true'" in analyze
assert '-Dsonar.projectVersion=${GITHUB_REF_NAME#v}' in analyze
assert 'steps.analyze.outcome' in build and 'steps.guard.outputs.run' in build
comment=step(build,'Comment quality gate on PR')
assert "steps.analyze.outcome != 'skipped'" in comment and 'always()' in comment
assert 'types=BUG' not in comment, 'PR comment must not hide non-BUG findings'
assert 'resolved=false&ps=100' in comment
with tempfile.TemporaryDirectory() as tmp:
    tmp=Path(tmp)
    first_page={
        'paging': {'pageIndex': 1, 'pageSize': 100, 'total': 102},
        'issues': [
            {'rule': 'java:S'+str(1000+i), 'severity': 'MAJOR', 'type': 'CODE_SMELL',
             'component': 'tapstate:runtime/srs/PageOne.java', 'line': i+1,
             'message': 'Page-one maintainability finding.'}
            for i in range(100)
        ],
    }
    second_page={
        'paging': {'pageIndex': 2, 'pageSize': 100, 'total': 102},
        'issues': [
            {'rule': 'java:S2095', 'severity': 'BLOCKER', 'type': 'BUG',
             'component': 'tapstate:runtime/srs/Resource.java', 'line': 20,
             'message': 'Close the resource.'},
            {'rule': 'java:S3649', 'severity': 'CRITICAL', 'type': 'VULNERABILITY',
             'component': 'tapstate:runtime/srs/Query.java', 'line': 30,
             'message': 'Validate this query.'},
        ],
    }
    curl=tmp/'curl'
    curl.write_text("""#!/usr/bin/env bash
url="${*: -1}"
printf '%s\n' "$url" >> "$CAPTURED_CURL_URLS"
case "$url" in
  *project_status*)
    printf '%s' '{"projectStatus":{"status":"ERROR","conditions":[{"status":"ERROR","metricKey":"new_maintainability_rating","actualValue":"3","comparator":"GT","errorThreshold":"1"}]}}'
    ;;
  *issues/search*p=1)
    printf '%s' '"""+json.dumps(first_page,separators=(',',':'))+"""'
    ;;
  *issues/search*p=2)
    printf '%s' '"""+json.dumps(second_page,separators=(',',':'))+"""'
    ;;
  *) exit 2 ;;
esac
""")
    curl.chmod(0o755)
    gh=tmp/'gh'
    gh.write_text('''#!/usr/bin/env bash
if [[ "$*" == *"/pulls/"* ]]; then
  [[ "${LIVE_HEAD_FAIL:-false}" != true ]] || exit 1
  printf '%s\n' "${LIVE_HEAD_SHA:-$EXPECTED_HEAD_SHA}"
  exit 0
fi
for arg in "$@"; do
  case "$arg" in
    body=*) printf '%s' "${arg#body=}" > "$CAPTURED_COMMENT" ;;
  esac
done
''')
    gh.chmod(0o755)
    captured=tmp/'captured-comment'
    curl_urls=tmp/'curl-urls'
    env=dict(os.environ,PATH=str(tmp)+os.pathsep+os.environ['PATH'],SONAR_TOKEN='token',
             SONAR_HOST_URL='https://sonar.example.invalid',GH_TOKEN='token',PR_NUMBER='540',
             GITHUB_REPOSITORY='tapstate/tapstate',EXPECTED_HEAD_SHA='a'*40,GITHUB_RUN_ID='123',GITHUB_RUN_ATTEMPT='1',CAPTURED_COMMENT=str(captured),
             CAPTURED_CURL_URLS=str(curl_urls))
    run=subprocess.run(['bash','-e','-c',script(comment)],env=env,cwd=tmp,capture_output=True,text=True)
    assert run.returncode==0,run.stdout+run.stderr
    body=captured.read_text()
    assert 'Analyzed PR head `'+('a'*40)+'`' in body and '/actions/runs/123/attempts/1' in body
    assert 'Open unresolved issues:' in body
    for finding in ['java:S1000','java:S1099','java:S2095','java:S3649']:
        assert finding in body, 'PR comment omitted '+finding
    issue_requests=[url for url in curl_urls.read_text().splitlines() if '/api/issues/search?' in url]
    assert len(issue_requests)==2, issue_requests
    assert issue_requests[0].endswith('&p=1'), issue_requests
    assert issue_requests[1].endswith('&p=2'), issue_requests
    for extra in ({'LIVE_HEAD_SHA': 'b'*40}, {'LIVE_HEAD_FAIL': 'true'}):
        captured.unlink(missing_ok=True)
        run=subprocess.run(['bash','-e','-c',script(comment)],env={**env,**extra},cwd=tmp,capture_output=True,text=True)
        assert run.returncode==0 and not captured.exists(), 'old or unknown head must not replace the current PR decoration'
command=script(step(reporter,'Report analysis outcome'))
with tempfile.TemporaryDirectory() as tmp:
    for build_result, enabled, outcome, expect in [
        ('success','true','success',0),
        ('success','true','failure',1),
        ('success','true','skipped',1),
        ('success','','',1),
        ('failure','false','skipped',1),
        ('cancelled','true','success',1),
        ('cancelled','false','skipped',1),
        ('skipped','false','skipped',1),
        ('success','true','cancelled',1),
        ('success','false','skipped',0),
    ]:
        summary=Path(tmp)/'summary'; summary.write_text('')
        env=dict(os.environ,BUILD_RESULT=build_result,SONAR_ENABLED=enabled,SONAR_OUTCOME=outcome,GITHUB_STEP_SUMMARY=str(summary))
        run=subprocess.run(['bash','-e','-c',command],env=env,capture_output=True,text=True)
        assert (run.returncode==0)==(expect==0),run.stdout+run.stderr
        if enabled=='false' and build_result=='success':
            assert 'not evaluated' in summary.read_text(), 'missing-secret success must remain explicit'
guard=script(step(build,'Check secret availability'))
with tempfile.TemporaryDirectory() as tmp:
    for token, host, fork, expected in [('', '', 'true', 'false'), ('', '', 'false', 'false'), ('token', '', 'false', 'false'), ('token', 'https://example.invalid', 'false', 'true')]:
        output=Path(tmp)/'output'; output.write_text('')
        summary=Path(tmp)/'summary'; summary.write_text('')
        env=dict(os.environ,SONAR_TOKEN=token,SONAR_HOST_URL=host,IS_FORK_PR=fork,GITHUB_OUTPUT=str(output),GITHUB_STEP_SUMMARY=str(summary))
        run=subprocess.run(['bash','-e','-c',guard],env=env,capture_output=True,text=True)
        assert run.returncode==0,run.stderr
        assert output.read_text().strip()=='run='+expected
        if expected=='false':
            assert '::notice title=SonarQube not evaluated::' in run.stdout
            assert 'not because the quality gate passed' in summary.read_text()
            assert ('comes from a fork' if fork=='true' else 'not configured') in summary.read_text()
        else:
            assert not summary.read_text()
    # Run the real scanner command against a Maven recorder: no test lifecycle is
    # hidden in the command and the release version still reaches the server.
    recorder=Path(tmp)/'mvn'
    recorder.write_text('#!/usr/bin/env bash\nprintf \"%s\\n\" \"$@\" > \"$RECORDED_ARGS\"\n')
    recorder.chmod(0o755)
    for ref_type, ref_name in [('tag','v1.2.3'),('branch','main')]:
        args=Path(tmp)/'args'
        env=dict(os.environ,PATH=tmp+os.pathsep+os.environ['PATH'],SONAR_TOKEN='token',SONAR_HOST_URL='https://example.invalid',RUNNER_TEMP=tmp,GITHUB_REF_TYPE=ref_type,GITHUB_REF_NAME=ref_name,GITHUB_EVENT_NAME='pull_request' if ref_type=='branch' else 'push',GITHUB_REF='refs/pull/1/merge' if ref_type=='branch' else 'refs/tags/'+ref_name,RECORDED_ARGS=str(args))
        run=subprocess.run(['bash','-e','-c',script(analyze)],env=env,capture_output=True,text=True)
        assert run.returncode==0,run.stderr
        values=args.read_text().splitlines()
        assert 'sonar:sonar' in values and 'verify' not in values
        assert '-Dmaven.repo.local='+tmp+'/m2' in values
        assert '-Dsonar.projectKey=tapstate' in values
        assert '-Dsonar.qualitygate.wait=true' in values, 'scanner must wait for the measured quality gate'
        assert ('-Dsonar.projectVersion=1.2.3' in values)==(ref_type=='tag')
    recorder.write_text('#!/usr/bin/env bash\nexit 23\n')
    run=subprocess.run(['bash','-e','-c',script(analyze)],env=env,capture_output=True,text=True)
    assert run.returncode!=0, 'scanner failure was suppressed before the required check could see it'
revision_controls=0
with tempfile.TemporaryDirectory(prefix='sonar-revision-') as temporary:
    directory=Path(temporary); repo=directory/'repo'; repo.mkdir(); commands=directory/'bin'; commands.mkdir()
    git_env={key:value for key,value in os.environ.items() if key not in (
        'GIT_DIR','GIT_WORK_TREE','GIT_INDEX_FILE','GIT_PREFIX','GIT_QUARANTINE_PATH','GIT_COMMON_DIR')}
    def git(*args):
        return subprocess.check_output(['git','-c','core.hooksPath=/dev/null','-c','commit.gpgsign=false',
                                        '-C',str(repo),*args],env=git_env,text=True,stderr=subprocess.DEVNULL).strip()
    git('init','-b','main')
    assert Path(git('rev-parse','--show-toplevel')).resolve()==repo.resolve(), 'not the owned Git fixture'
    git('config','user.email','fixture@example.invalid');git('config','user.name','Fixture')
    (repo/'source').write_text('old');git('add','source');git('commit','-m','Old revision');old=git('rev-parse','HEAD')
    (repo/'source').write_text('new');git('commit','-am','New revision');new=git('rev-parse','HEAD')
    gh=commands/'gh'
    gh.write_text('''#!/usr/bin/env python3
import json,os,sys
with open(os.environ['API_LOG'],'a') as out:out.write(json.dumps(sys.argv[1:])+'\\n')
assert sys.argv[1:]==['api','--method','GET','repos/tapstate/tapstate/git/ref/heads/main','--jq','.object.sha']
if os.environ.get('LIVE_MAIN_MODE')=='failed':raise SystemExit(1)
print(os.environ['LIVE_MAIN_HEAD'])
''');gh.chmod(0o755)
    recorder=commands/'mvn'
    recorder.write_text('''#!/usr/bin/env bash
printf '%s\\n' "$GITHUB_SHA" > "$SONAR_STATE"
printf '%s' "${GH_TOKEN+present}${GITHUB_TOKEN+present}" > "$SCANNER_AUTH"
''');recorder.chmod(0o755)
    state=directory/'sonar-state';auth=directory/'scanner-auth';api_log=directory/'api-log'
    environment=dict(git_env,PATH=str(commands)+os.pathsep+os.environ['PATH'],SONAR_TOKEN='fixture-token',
        SONAR_HOST_URL='https://example.invalid',RUNNER_TEMP=str(directory),GITHUB_REPOSITORY='tapstate/tapstate',
        GITHUB_REF_TYPE='branch',GITHUB_REF_NAME='main',GITHUB_REF='refs/heads/main',GITHUB_EVENT_NAME='push',
        GH_TOKEN='fixture-read-token',GITHUB_TOKEN='fixture-other-token',LIVE_MAIN_HEAD=new,
        SONAR_STATE=str(state),SCANNER_AUTH=str(auth),API_LOG=str(api_log))
    def run_revision(checkout, extra=None):
        git('checkout','--detach',checkout)
        return subprocess.run(['bash','-e','-c',script(analyze)],cwd=repo,
                              env={**environment,'GITHUB_SHA':checkout,**(extra or {})},capture_output=True,text=True)
    # A newer fast run reaches the shared analysis lock before the older slow run.
    run=run_revision(new);assert run.returncode==0,run.stdout+run.stderr
    assert state.read_text().strip()==new;revision_controls+=1
    run=run_revision(old)
    assert run.returncode!=0 and state.read_text().strip()==new, 'a late old main run overwrote newer Sonar state'
    revision_controls+=1
    assert auth.read_text()=='', 'scanner inherited GitHub API credentials'
    for extra in ({'LIVE_MAIN_MODE':'failed'},{'LIVE_MAIN_HEAD':'null'},
                  {'LIVE_MAIN_HEAD':''},{'GITHUB_SHA':old}):
        state.unlink(missing_ok=True)
        run=run_revision(new,extra)
        assert run.returncode!=0 and not state.exists(), 'unknown or inconsistent main identity reached upload'
        revision_controls+=1
    for event,ref in [('pull_request','refs/pull/1/merge'),('push','refs/tags/v1.2.3'),
                      ('workflow_dispatch','refs/heads/ws/feature')]:
        run=run_revision(old,{'GITHUB_EVENT_NAME':event,'GITHUB_REF':ref,'LIVE_MAIN_MODE':'failed'})
        assert run.returncode==0 and state.read_text().strip()==old, 'non-main analysis behavior changed'
        revision_controls+=1
    run=run_revision(old,{'GITHUB_EVENT_NAME':'workflow_dispatch'})
    assert run.returncode!=0 and state.read_text().strip()==old, 'stale manual main upload was allowed'
    revision_controls+=1
    calls=[json.loads(line) for line in api_log.read_text().splitlines()]
    assert calls and all(row==['api','--method','GET','repos/tapstate/tapstate/git/ref/heads/main','--jq','.object.sha'] for row in calls)
print(f'sonar-workflow smoke: 7 event/ref cases, 20 runtime cases, {revision_controls} revision-order controls and {lock_mutations} lock mutations passed, including 102 paginated issues')
PY
