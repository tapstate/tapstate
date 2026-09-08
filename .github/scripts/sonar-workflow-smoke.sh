#!/usr/bin/env bash
# Protect the actual analysis path and its exact native required check name.
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
python3 - "$here/../workflows" <<'PY'
from pathlib import Path
import os, re, subprocess, sys, tempfile
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
name=re.search(r'^    name: (.*)$',reporter,re.M).group(1)
assert name=="${{ github.event_name == 'push' && startsWith(github.ref, 'refs/heads/ws/') && 'ci-summary' || 'sonarqube' }}", 'exact native check names must follow the trigger'
# Read the trigger operands from the workflow, so this table cannot keep passing
# against an invented test-only condition when the real analysis is disabled.
required_value=re.search(r'^      SONAR_REQUIRED: (.*)$',build,re.M).group(1)
required_match=re.fullmatch(r"\$\{\{ !\(github.event_name == '([^']+)' && startsWith\(github.ref, '([^']+)'\)\) \}\}",required_value)
assert required_match, 'analysis trigger must remain bound to the event and ref'
name_match=re.fullmatch(r"\$\{\{ github.event_name == '([^']+)' && startsWith\(github.ref, '([^']+)'\) && '([^']+)' \|\| '([^']+)' \}\}",name)
assert name_match
trigger_cases=[
    ('push','refs/heads/main',True,'sonarqube'),
    ('pull_request','refs/pull/42/merge',True,'sonarqube'),
    ('push','refs/tags/v1.2.3',True,'sonarqube'),
    ('workflow_dispatch','refs/heads/main',True,'sonarqube'),
    ('workflow_dispatch','refs/heads/ws/example',True,'sonarqube'),
    ('push','refs/heads/ws/example',False,'ci-summary'),
]
for event, ref, expected_required, expected_name in trigger_cases:
    actual_required=not(event==required_match[1] and ref.startswith(required_match[2]))
    actual_name=name_match[3] if event==name_match[1] and ref.startswith(name_match[2]) else name_match[4]
    assert actual_required==expected_required, (event,ref,'analysis trigger changed')
    assert actual_name==expected_name, (event,ref,'native check name changed')
assert "        if: env.SONAR_REQUIRED == 'true'" in step(build,'Check secret availability').splitlines()
for output, source in [('sonar-required','env.SONAR_REQUIRED'),('sonar-enabled','steps.guard.outputs.run'),('sonar-outcome','steps.analyze.outcome')]:
    assert '      '+output+': ${{ '+source+' }}' in build.splitlines(), 'analysis output binding changed: '+output
for variable, output in [('SONAR_REQUIRED','sonar-required'),('SONAR_ENABLED','sonar-enabled'),('SONAR_OUTCOME','sonar-outcome')]:
    assert '          '+variable+': ${{ needs.build.outputs.'+output+' }}' in reporter.splitlines(), 'required check input binding changed: '+variable
assert 'needs: build' in reporter and 'always()' in reporter
assert 'sonarqube-' in text and 'cancel-in-progress: true' not in text
admit=step(build,'Admit Sonar inputs'); analyze=step(build,'Analyze admitted coverage')
assert 'sonar-inputs.sh' in admit and '--artifacts' in admit and '--restore .' in admit
assert build.index('Admit Sonar inputs') < build.index('Merge coverage from every declared shard') < build.index('Analyze admitted coverage')
assert 'mvn -B -Dmaven.repo.local="$RUNNER_TEMP/m2" sonar:sonar' in analyze
assert not re.search(r'\bmvn\b[^\n]*\bverify\b',analyze)
assert 'continue-on-error: true' in analyze
assert "steps.guard.outputs.run == 'true'" in analyze
assert '-Dsonar.projectVersion=${GITHUB_REF_NAME#v}' in analyze
assert 'steps.analyze.outcome' in build and 'steps.guard.outputs.run' in build
comment=step(build,'Comment quality gate on PR')
assert "steps.analyze.outcome != 'skipped'" in comment and 'always()' in comment
command=script(step(reporter,'Report analysis outcome'))
with tempfile.TemporaryDirectory() as tmp:
    for build_result, required, enabled, outcome, expect in [
        ('success','true','true','success',0),
        ('success','true','true','failure',1),
        ('success','true','true','skipped',1),
        ('success','true','','',1),
        ('failure','true','false','skipped',1),
        ('success','true','false','skipped',0),
        ('success','false','','',0),
    ]:
        summary=Path(tmp)/'summary'; summary.write_text('')
        env=dict(os.environ,BUILD_RESULT=build_result,SONAR_REQUIRED=required,SONAR_ENABLED=enabled,SONAR_OUTCOME=outcome,GITHUB_STEP_SUMMARY=str(summary))
        run=subprocess.run(['bash','-e','-c',command],env=env,capture_output=True,text=True)
        assert (run.returncode==0)==(expect==0),run.stdout+run.stderr
        if required=='true' and enabled=='false' and build_result=='success':
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
        env=dict(os.environ,PATH=tmp+os.pathsep+os.environ['PATH'],SONAR_TOKEN='token',SONAR_HOST_URL='https://example.invalid',RUNNER_TEMP=tmp,GITHUB_REF_TYPE=ref_type,GITHUB_REF_NAME=ref_name,RECORDED_ARGS=str(args))
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
print('sonar-workflow smoke: 6 trigger cases and 14 runtime cases passed')
PY
