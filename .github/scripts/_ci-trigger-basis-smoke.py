#!/usr/bin/env python3
"""Original Git/archive and source block controls without evaluating workflows."""
import copy
import importlib.util
import json
from pathlib import Path
import subprocess
import sys

sys.dont_write_bytecode = True


def load(name, filename):
    spec = importlib.util.spec_from_file_location(name, Path(__file__).with_name(filename))
    value = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(value)
    return value


m = load('trigger_basis', '_ci-trigger-basis.py')
fixture = load('trigger_fixture', '_ci-archive-inputs-smoke.py')
count = 0


def check(value, message):
    global count
    assert value, message
    count += 1


def rejected(call, message):
    try:
        call()
    except (ValueError, OSError, KeyError, TypeError) as error:
        check(message in str(error), 'wrong rejection: ' + str(error))
    else:
        raise AssertionError('expected rejection: ' + message)


def controls(f):
    request = {'schema': 1, 'type': 'trigger-basis-request',
               'source': f['save']('trigger-source.json', f['source_request'])}

    def project():
        return m.project(m.g.References(f['evidence']), f['work'], request)

    def seal():
        record = next(r for r in f['records'] if r['name'] == 'ci-shard-plan')
        f['artifacts']['ci-shard-plan']['ci-event-inputs.json'] = copy.deepcopy(event)
        f['save']('run/artifacts/' + str(record['id']) + '-ci-shard-plan.zip',
                  fixture.zipped(f['artifacts']['ci-shard-plan']))
        f['source_request']['archive'] = f['seal']()
        request['source'] = f['save']('trigger-source.json', f['source_request'])

    answer = project()
    workflow = '.github/workflows/ci.yml'
    check(answer['workflows'][workflow]['source_sha256'] == f['sources'][workflow], 'workflow bytes come from bound checkout')
    check(answer['source_blocks_readback_verified'] and answer['original_labels']['snapshot'] == 'unknown',
          'missing original PR data remains unknown')
    check(not answer['R_G_complete'] and not answer['whole_input_projection_verified']
          and not answer['first_complete_PR_sample'] and not answer['performance_accepted'],
          'source facts never certify applicability or performance')
    pr = {'number': 7, 'action': 'synchronize', 'head_sha': f['anchor'], 'base_sha': f['base'],
          'head_ref': 'ws/fixture', 'base_ref': 'main', 'head_repository': 'example/project',
          'base_repository': 'example/project'}
    event = copy.deepcopy(f['event'])
    event['pull_request'] = pr
    f['save']('run/event-inputs.json', event)
    seal()
    check(project()['original_labels']['snapshot'] == 'unknown', 'legacy nested PR identities do not invent label captures')
    check(project()['runner_workflow_source']['runner_declaration']['snapshot'] == 'unknown'
          and not project()['runner_workflow_source']['source_bytes_verified'],
          'legacy source archive does not infer workflow provenance from checkout')
    event['workflow_source'] = None
    f['save']('run/event-inputs.json', event); seal()
    rejected(project, 'null runner workflow')
    event['workflow_source'] = {'snapshot': 'captured', 'workflow_ref': 'example/project/.github/workflows/ci.yml@refs/pull/7/merge',
        'workflow_sha': event['checkout_sha'], 'workflow_path': workflow, 'job': 'shard-plan', 'source_bytes_verified': False}
    f['save']('run/event-inputs.json', event); seal()
    readback = project()['runner_workflow_source']
    check(readback['source_bytes_verified'] and readback['source_sha256'] == f['sources'][workflow]
          and readback['runner_declaration']['source_bytes_verified'] is False,
          'exact declared commit and path join independently hash-bound workflow bytes')
    event['workflow_source']['workflow_sha'] = f['base']
    f['save']('run/event-inputs.json', event); seal()
    check(not project()['runner_workflow_source']['source_bytes_verified']
          and project()['checkout']['sha'] == event['checkout_sha'],
          'different workflow commit remains a source gap without changing merge checkout identity')
    event['workflow_source']['workflow_sha'] = 'bad'
    f['save']('run/event-inputs.json', event); seal()
    rejected(project, 'invalid SHA')
    event['workflow_source']['workflow_sha'] = event['checkout_sha']
    event['workflow_source']['workflow_ref'] = 'foreign/project/.github/workflows/ci.yml@refs/heads/main'
    f['save']('run/event-inputs.json', event); seal()
    rejected(project, 'repository/path differs')
    event['workflow_source']['workflow_ref'] = 'example/project/.github/workflows/ci.yml@refs/pull/7/merge'
    event['workflow_source']['source_bytes_verified'] = True
    f['save']('run/event-inputs.json', event); seal()
    rejected(project, 'source fields')
    event['workflow_source']['source_bytes_verified'] = False
    event['workflow_source'].update(workflow_path='.github/workflows/other.yml',
        workflow_ref='example/project/.github/workflows/other.yml@refs/pull/7/merge')
    f['save']('run/event-inputs.json', event); seal()
    rejected(project, 'path differs from original CI')
    event.pop('workflow_source')
    f['save']('run/event-inputs.json', event); seal()
    pr.update(labels_snapshot_complete=True, label_name_sha256=[], label_ascii_lower_sha256=[],
              label_ascii_case_complete=True)
    f['save']('run/event-inputs.json', event)
    seal()
    check(project()['original_labels']['snapshot'] == 'captured', 'observed empty label snapshot is read separately')
    pr['number'] = 8
    f['save']('run/event-inputs.json', event)
    seal()
    rejected(project, 'original PR trigger scope differs')
    pr['number'] = 7
    pr['label_name_sha256'] = ['a' * 64]
    f['save']('run/event-inputs.json', event)
    seal()
    rejected(project, 'inconsistent emptiness')
    pr['label_name_sha256'] = []
    f['save']('run/event-inputs.json', event)
    f['save']('run/ruleset-12.json', {'id': 12, 'enforcement': 'active', 'rules': []})
    seal()
    check(len(project()['ruleset_records']) == 1 and not project()['R_G_complete'], 'raw ruleset record does not invent historical scope or list completeness')
    f['save']('run/ruleset-12.json', {'id': 13, 'rules': []})
    seal()
    rejected(project, 'ruleset identity differs')
    f['save']('run/ruleset-12.json', {'id': 12, 'enforcement': 'active', 'rules': []})
    seal()
    manifest, output = f['evidence'] / 'trigger-request.json', f['evidence'] / 'trigger-cli.json'
    manifest.write_bytes(fixture.encode(request))
    command = ['bash', str(Path(__file__).with_name('ci-trigger-basis.sh')), '--manifest', str(manifest),
               '--evidence-root', str(f['evidence']), '--root', str(f['work']), '--output', str(output)]
    child = subprocess.run(command, capture_output=True, text=True)
    check(child.returncode == 0 and json.loads(output.read_bytes())['ruleset_records'][0]['value']['id'] == 12,
          'CLI reads original indexed source and ruleset records: ' + child.stderr)
    before = output.read_bytes()
    check(subprocess.run(command, capture_output=True).returncode != 0 and output.read_bytes() == before,
          'trigger output is exclusive')


source = b'''name: fixture
on:
  pull_request:
    branches: [main]
    paths:
      - "source/**"
  pull_request_target:
    types: [opened]
jobs:
  guarded:
    if: >-
      contains(github.event.pull_request.labels.*.name, 'request-lane')
    runs-on: fixture
    steps: []
'''
facts = m.workflow_fact(source)
check(set(facts['event_blocks']) == {'pull_request', 'pull_request_target'}, 'event source namespaces stay separate')
check('paths:' in facts['event_blocks']['pull_request']['source'], 'path filter source is retained')
check(facts['job_guard_blocks'][0]['job'] == 'guarded' and 'contains(' in facts['job_guard_blocks'][0]['source'],
      'full multiline guard is retained as source data')
check(not facts['applicability_verified'], 'guard text is not an evaluated Boolean')
plain = m.workflow_fact(b"jobs:\n  plain:\n    if: github.event_name == 'push' ||\n      github.event_name == 'pull_request'\n    steps: []\n")
check("== 'pull_request'" in plain['job_guard_blocks'][0]['source'], 'plain multiline guard is retained completely')
quoted_guard = m.workflow_fact(b'''jobs:
  quoted_guard:
    if: "github.event_name == 'push' ||
      github.event_name == 'pull_request'"
    steps: []
''')
check("== 'pull_request'" in quoted_guard['job_guard_blocks'][0]['source'], 'quoted multiline guard is retained completely')
rejected(lambda: m.workflow_fact(b'on:\n  pull_request:\n  pull_request:\n'), 'duplicate workflow event')
rejected(lambda: m.workflow_fact(b'on:\n  pull_request:\non:\n  push:\n'), 'duplicate workflow top-level')
inline = m.workflow_fact(b'on: [pull_request, push]\njobs: {}\n')
check(inline['declaration_structure'] == 'unparsed-source' and inline['on_source'], 'inline YAML is retained without invented events')
quoted_job = m.workflow_fact(b"jobs:\n  valid:\n    steps: []\n  'quoted':\n    if: always()\n")
check(quoted_job['job_guard_blocks'][0]['job'] is None
      and quoted_job['job_guard_blocks'][0]['job_structure'] == 'unparsed-source',
      'quoted job key retains its guard as unparsed source')
unknown = {'labels_snapshot_complete': False, 'label_name_sha256': None,
           'label_ascii_lower_sha256': None, 'label_ascii_case_complete': False}
check(m.label_fact(unknown)['snapshot'] == 'unknown', 'explicit absent capture remains unknown')
changed = copy.deepcopy(unknown)
changed['label_name_sha256'] = []
rejected(lambda: m.label_fact(changed), 'contradict')
fixture.run(controls, only_callback=True)
print('CI trigger source basis controls passed: ' + str(count))
