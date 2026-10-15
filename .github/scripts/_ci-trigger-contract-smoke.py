#!/usr/bin/env python3
"""Bounded trigger policy controls with real original Git/ZIP readback."""
import copy
import hashlib
import importlib.util
from pathlib import Path


def module(name, filename):
    spec = importlib.util.spec_from_file_location(name, Path(__file__).with_name(filename))
    value = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(value)
    return value


m = module('contract', '_ci-trigger-contract.py')
fixture = module('fixture', '_ci-archive-inputs-smoke.py')
count = 0


def check(value, message):
    global count
    assert value, message
    count += 1


def unsupported(call):
    try:
        call()
    except m.Unsupported:
        check(True, 'unsupported policy is unknown')
    else:
        raise AssertionError('unsupported policy was evaluated')


def workflow(options='', guard=''):
    return 'name: Fixture\non:\n  pull_request:\n' + options + '\njobs:\n  test:\n' + guard + '    steps: []\n'


def fact(options='', guard=''):
    return m.basis_reader.workflow_fact(workflow(options, guard).encode())


check(m.trigger(fact(), 'synchronize', 'main', []), 'default ordinary activity applies')
check(not m.trigger(fact(), 'labeled', 'main', []), 'default does not invent a labeled activity')
check(not m.trigger(fact('    types: [labeled]\n'), 'synchronize', 'main', []), 'explicit activity excludes synchronize')
check(m.trigger(fact('    branches:\n      - main\n    paths:\n      - "src/**"\n'), 'opened', 'main', ['src/a.java']), 'branch and path filters both match')
check(not m.trigger(fact('    branches: [release]\n    paths: ["src/**"]\n'), 'opened', 'main', ['src/a.java']), 'branch mismatch is not applicable')
check(not m.trigger(fact('    paths: ["src/**"]\n'), 'opened', 'main', ['docs/a.md']), 'unmatched path is not applicable')
check(not m.trigger(fact('    paths: ["src/**"]\n'), 'opened', 'main', []), 'empty diff does not match a path filter')
check(not m.included(['src/**', '!src/private/**'], 'src/private/a.java'), 'negative pattern excludes after positive')
check(m.included(['src/**', '!src/private/**', 'src/private/a.java'], 'src/private/a.java'), 'later positive includes again')
for text in ['    paths: ["**/*.java"]\n', '    paths: &paths ["src/**"]\n', '    paths: []\n', '    tags: [main]\n', '    paths: ["src/**"]\n    paths-ignore: ["docs/**"]\n']:
    unsupported(lambda: m.trigger(fact(text), 'opened', 'main', ['src/a.java']))
unsupported(lambda: m.trigger(m.basis_reader.workflow_fact(b'on: [pull_request]\njobs: {}\n'), 'opened', 'main', []))
unsupported(lambda: m.trigger(fact('    paths: ["src/**"]\n'), 'opened', 'main', None))
labels = {'snapshot': 'captured', 'label_ascii_case_complete': True, 'label_ascii_lower_sha256': []}
guard = fact(guard="    if: github.event_name != 'pull_request' || contains(github.event.pull_request.labels.*.name, 'upgrade-lane')\n")['job_guard_blocks'][0]
check(m.guard_fact(guard, labels)['value'] is False, 'original empty labels do not enable a conditional job')
labels['label_ascii_lower_sha256'] = [hashlib.sha256(b'upgrade-lane').hexdigest()]
check(m.guard_fact(guard, labels)['value'] is True, 'original ASCII membership enables a conditional job')
unsupported(lambda: m.guard_fact(guard, {'snapshot': 'unknown'}))
unsupported(lambda: m.guard_fact(fact(guard="    if: needs.other.result == 'success'\n")['job_guard_blocks'][0], labels))
check(m.guard_fact(fact(guard='    if: ${{ always() }}\n')['job_guard_blocks'][0], labels)['predicate'] == 'native-result-required', 'lifecycle predicate never becomes a fabricated result')


def seed(context):
    event = context['event']
    event['pull_request'] = {'number': 7, 'action': 'synchronize', 'head_sha': context['anchor'],
        'base_sha': context['base'], 'head_ref': 'ws/fixture', 'base_ref': 'main',
        'head_repository': 'example/project', 'base_repository': 'example/project',
        'labels_snapshot_complete': True, 'label_name_sha256': [], 'label_ascii_lower_sha256': [],
        'label_ascii_case_complete': True}
    event['workflow_source'] = {'snapshot': 'captured',
        'workflow_ref': 'example/project/.github/workflows/ci.yml@refs/pull/7/merge',
        'workflow_sha': event['checkout_sha'], 'workflow_path': '.github/workflows/ci.yml',
        'job': 'shard-plan', 'source_bytes_verified': False}
    context['artifacts']['ci-shard-plan']['ci-event-inputs.json'] = copy.deepcopy(event)
    context['reseal']()
    request = {'schema': 1, 'type': 'trigger-contract-request',
               'source': context['save']('trigger-contract-source.json', context['source_request'])}
    return request


def controls(context):
    request = seed(context)
    event = context['event']

    def project():
        return m.project(m.g.References(context['evidence']), context['work'], request)

    answer = project()
    check(answer['source_policy_eligible'] and not answer['unknown'], 'bound original source policy has no unsupported facts')
    check(answer['ordinary_applicable_workflows'] == ['.github/workflows/ci.yml', '.github/workflows/conditional.yml', '.github/workflows/label.yml'], 'policy derives workflow set from actual source and Git diff')
    check(answer['changed_paths']['path_count'] == 1 and answer['changed_paths']['status'] == 'source-derived', 'diff uses original base/head objects')
    check(answer['workflows']['.github/workflows/label.yml']['job_guards'][0]['value'] is False, 'original label skip remains visible')
    check(not answer['R_G_complete'] and not answer['backend_applicability_observed'] and not answer['first_complete_PR_sample'] and not answer['performance_accepted'], 'source policy does not certify backend or first sample')
    scope = answer['scope']
    pr = {'number': 7, 'state': 'open', 'draft': True, 'merge_commit_sha': answer['checkout']['sha'],
        'head': {'sha': scope['head_sha'], 'ref': scope['branch'], 'repo': {'full_name': scope['repository']}},
        'base': {'sha': scope['base_sha'], 'ref': 'main', 'repo': {'full_name': scope['repository']}}}
    association = {'number': 7, 'head': {'sha': scope['head_sha']}, 'base': {'sha': scope['base_sha']}}
    runs = [{'id': number + 42, 'path': path, 'run_attempt': 1, 'status': 'completed', 'conclusion': 'success',
        'head_sha': scope['head_sha'], 'head_branch': scope['branch'], 'event': 'pull_request',
        'repository': {'full_name': scope['repository']}, 'pull_requests': [association]}
        for number, path in enumerate(answer['ordinary_applicable_workflows'])]

    def api(endpoint):
        if endpoint.endswith('/pulls/7'):
            return copy.deepcopy(pr)
        if '/git/commits/' in endpoint:
            return {'sha': pr['merge_commit_sha'], 'parents': [{'sha': scope['base_sha']}, {'sha': scope['head_sha']}]}
        if '/actions/runs?' in endpoint:
            return {'total_count': len(runs), 'workflow_runs': copy.deepcopy(runs)}
        if '/check-runs?' in endpoint:
            commit = endpoint.split('/commits/', 1)[1].split('/', 1)[0]
            offset = 100 if commit == scope['head_sha'] else 200
            rows = [{'id': offset + number, 'name': name, 'head_sha': commit, 'status': 'completed',
                'conclusion': 'success', 'pull_requests': [association]} for number, name in enumerate(answer['minimum_required_checks'])]
            return {'total_count': len(rows), 'check_runs': rows}
        if '/attempts/1/jobs?' in endpoint:
            return {'total_count': 0, 'jobs': []}
        if '/attempts/1' in endpoint:
            identity = int(endpoint.split('/actions/runs/', 1)[1].split('/', 1)[0])
            return copy.deepcopy(next(run for run in runs if run['id'] == identity))
        raise AssertionError('unexpected API: ' + endpoint)

    check(m.policy_head_status(answer, api)['can_advance_after_archiving'], 'fresh head inventory agrees with original source policy')
    unknown = copy.deepcopy(answer); unknown['source_policy_eligible'] = False; unknown['unknown'] = ['target source unknown']
    check(not m.policy_head_status(unknown, api)['can_advance_after_archiving'], 'source unknown blocks advancing even with terminal runs')
    missing = runs.pop()
    check(not m.policy_head_status(answer, api)['can_advance_after_archiving'], 'missing applicable source workflow cannot advance')
    runs.append(missing)
    runs[0]['status'] = 'queued'
    check(not m.policy_head_status(answer, api)['can_advance_after_archiving'], 'queued workflow cannot advance')
    runs[0]['status'] = 'completed'
    changed = copy.deepcopy(answer); changed['checkout']['sha'] = '0' * 40
    try:
        m.policy_head_status(changed, api)
    except ValueError:
        check(True, 'foreign merge source rejected')
    else:
        raise AssertionError('foreign merge accepted')
    archives = []
    for run in runs:
        prefix = 'transition/' + str(run['id']) + '/'
        context['save'](prefix + 'run.json', run)
        context['save'](prefix + 'attempt-1/run.json', run)
        context['save'](prefix + 'attempt-1/jobs-1.json', {'total_count': 0, 'jobs': []})
        context['save'](prefix + 'attempt-1/logs.zip', fixture.zipped({'setup.log': b'original offline native log fixture'}))
        directory = context['evidence'] / prefix
        m.g.receipt.manifest(directory, {'archive_root': str(directory), 'archive_complete': True,
            'purpose': 'offline raw workflow evidence', 'gaps': []})
        archives.append({'run_id': run['id'], 'receipt': {'path': prefix + 'receipt.json',
            'sha256': m.g.checksum((directory / 'receipt.json').read_bytes())}})
    pages = {commit: [context['save']('transition/checks-' + commit + '.json',
        api('repos/example/project/commits/' + commit + '/check-runs?filter=all'))]
        for commit in [scope['head_sha'], answer['checkout']['sha']]}
    transition = {'schema': 1, 'type': 'serial-transition-request',
        'scope': {name: scope[name] for name in ['repository', 'pr', 'branch', 'head_sha']},
        'expected_base_sha': scope['base_sha'], 'contract': {'applicable_workflows': answer['ordinary_applicable_workflows'],
        'required_checks': answer['minimum_required_checks']}, 'archives': archives, 'check_pages': pages,
        'trigger_policy': {'request': context['save']('transition-trigger-request.json', request),
        'source_root': str(context['work'])}}
    result = m.basis_reader.source.variants.transition(m.g.References(context['evidence']), transition, api)
    check(result['source_trigger_policy']['trigger_policy_source_bound'] and result['archive_readback_verified'],
          'source-aware transition independently replays policy and raw archives')
    missing = runs.pop()
    changed = copy.deepcopy(transition)
    changed['contract']['applicable_workflows'] = [run['path'] for run in runs]
    check(m.g.head_status(changed['scope'], changed['contract'], api)['can_advance_after_archiving'],
          'legacy declared terminal snapshot alone cannot detect an omitted source workflow')
    try:
        m.basis_reader.source.variants.transition(m.g.References(context['evidence']), changed, api)
    except ValueError as error:
        check('declared workflow set differs' in str(error), 'source-aware transition detects caller and API omission')
    else:
        raise AssertionError('source-aware transition accepted omitted workflow')
    runs.append(missing)
    original = copy.deepcopy(event)
    event['workflow_source']['workflow_sha'] = context['base']
    context['artifacts']['ci-shard-plan']['ci-event-inputs.json'] = copy.deepcopy(event)
    context['reseal'](); request['source'] = context['save']('trigger-contract-source.json', context['source_request'])
    check(not project()['source_policy_eligible'], 'separate workflow source cannot authorize a policy')
    event.clear(); event.update(original)
    event['pull_request'].update(labels_snapshot_complete=False, label_name_sha256=None,
        label_ascii_lower_sha256=None, label_ascii_case_complete=False)
    context['artifacts']['ci-shard-plan']['ci-event-inputs.json'] = copy.deepcopy(event)
    context['reseal'](); request['source'] = context['save']('trigger-contract-source.json', context['source_request'])
    check(not project()['source_policy_eligible'], 'missing original labels leave conditional policy ineligible')


fixture.run(controls, only_callback=True, workflow_sources={
    'ci.yml': workflow('    branches: [main]\n', '    if: ${{ always() }}\n'),
    'conditional.yml': workflow('    paths: [fixture.cfg]\n'),
    'excluded.yml': workflow('    paths: ["docs/**"]\n'),
    'labeled-only.yml': workflow('    types: [labeled]\n'),
    'label.yml': workflow(guard="    if: github.event_name != 'pull_request' || contains(github.event.pull_request.labels.*.name, 'upgrade-lane')\n"),
})


def target_controls(context):
    request = seed(context)
    answer = m.project(m.g.References(context['evidence']), context['work'], request)
    target = answer['workflows']['.github/workflows/target.yml']
    check(target['target_trigger'] == 'unknown', 'ordinary checkout cannot supply target workflow provenance')
    check(not answer['source_policy_eligible'] and answer['unknown'], 'target unknown makes policy ineligible')
    check(not answer['R_G_complete'] and not answer['performance_accepted'], 'target unknown never certifies complete evidence')


fixture.run(target_controls, only_callback=True, workflow_sources={
    'ci.yml': workflow('    branches: [main]\n'),
    'target.yml': 'on:\n  pull_request_target:\n    types: [opened]\njobs:\n  target:\n    steps: []\n',
})


def quoted_controls(context):
    request = seed(context)
    answer = m.project(m.g.References(context['evidence']), context['work'], request)
    check(not answer['source_policy_eligible'], 'partially parsed event maps remain ineligible')
    for name in ['quoted-ordinary.yml', 'quoted-target.yml']:
        check(answer['workflows']['.github/workflows/' + name]['ordinary_trigger'] == 'unknown',
              'quoted event key never becomes an absent ordinary or target event')


fixture.run(quoted_controls, only_callback=True, workflow_sources={
    'ci.yml': workflow('    branches: [main]\n'),
    'quoted-ordinary.yml': "on:\n  push:\n  'pull_request':\njobs:\n  ordinary:\n    steps: []\n",
    'quoted-target.yml': 'on:\n  pull_request:\n  "pull_request_target":\njobs:\n  target:\n    steps: []\n',
})
print('CI trigger contract controls passed:', count)
