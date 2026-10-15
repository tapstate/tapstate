#!/usr/bin/env python3
"""Read frozen workflow, label and ruleset source facts without certifying R/G.

Raw source blocks and their hashes remain data. Dynamic expressions, backend path
filtering, historical ruleset applicability and complete run/check instances need
independent evidence; source declarations alone cannot establish those facts.
"""
import argparse
import importlib.util
import io
from pathlib import Path
import re
import sys
import tarfile

sys.dont_write_bytecode = True
spec = importlib.util.spec_from_file_location('trigger_source', Path(__file__).with_name('_ci-source-projection.py'))
source = importlib.util.module_from_spec(spec)
spec.loader.exec_module(source)
g, require = source.groups, source.require
archive_inputs = g.module('trigger_archives', '_ci-archive-inputs.py')


def top_block(text, name):
    lines = text.splitlines(keepends=True)
    starts = [i for i, line in enumerate(lines) if re.match(r'^(?:' + re.escape(name)
               + r'|[\"\x27]' + re.escape(name) + r'[\"\x27]):(?:\s|$)', line)]
    require(len(starts) <= 1, 'duplicate workflow top-level block: ' + name)
    if not starts:
        return None
    start, end = starts[0], len(lines)
    for i in range(start + 1, len(lines)):
        if lines[i].strip() and not lines[i].startswith((' ', '\t', '#')):
            end = i
            break
    return ''.join(lines[start:end])


def workflow_fact(raw):
    text = raw.decode('utf-8')
    on, jobs = top_block(text, 'on'), top_block(text, 'jobs')
    guards = []
    if jobs is not None:
        lines = jobs.splitlines(keepends=True)
        job, job_header = None, None
        for i, line in enumerate(lines):
            match = re.match(r'^  ([A-Za-z0-9_-]+):\s*(?:#.*)?$', line.rstrip('\n'))
            if re.match(r'^  [^ \t#]', line):
                job = None
                job_header = line
            if match:
                job = match[1]
            if re.match(r'^    if:', line):
                require(job_header is not None, 'workflow job guard has no job source')
                block = line
                for following in lines[i + 1:]:
                    if following.strip() and not following.lstrip().startswith('#'):
                        indentation = len(following) - len(following.lstrip(' '))
                        if indentation <= 4:
                            break
                    block += following
                guards.append({'job': job, 'job_source_header': job_header,
                               'job_structure': 'literal-key' if job is not None else 'unparsed-source',
                               'source': block, 'sha256': g.checksum(block.encode())})
    # These are declarations only. They intentionally do not evaluate YAML,
    # actions, guards or path patterns, or invent a workflow applicability set.
    event_blocks = {}
    if on is not None:
        lines = on.splitlines(keepends=True)
        for i, line in enumerate(lines[1:], 1):
            match = re.match(r'^  ([A-Za-z0-9_]+):(?:\s|$)', line)
            if not match:
                continue
            require(match[1] not in event_blocks, 'duplicate workflow event declaration')
            end = len(lines)
            for j in range(i + 1, len(lines)):
                if re.match(r'^  [^ \t#]', lines[j]):
                    end = j
                    break
            block = ''.join(lines[i:end])
            event_blocks[match[1]] = {'source': block, 'sha256': g.checksum(block.encode())}
    return {'source_sha256': g.checksum(raw), 'on_source': on,
            'on_sha256': None if on is None else g.checksum(on.encode()),
            'event_blocks': event_blocks, 'job_guard_blocks': guards,
            'declaration_structure': 'indented-block-source' if event_blocks else 'unparsed-source',
            'applicability_verified': False}


def label_fact(pr):
    require(isinstance(pr, dict), 'invalid original PR event record')
    fields = ('labels_snapshot_complete', 'label_name_sha256', 'label_ascii_lower_sha256', 'label_ascii_case_complete')
    if not any(field in pr for field in fields):
        return {'snapshot': 'unknown', 'reason': 'original label capture predates hash-only label fields'}
    require(all(field in pr for field in fields) and type(pr['labels_snapshot_complete']) is bool
            and type(pr['label_ascii_case_complete']) is bool, 'incomplete label capture fields')
    if not pr['labels_snapshot_complete']:
        require(pr['label_name_sha256'] is None and pr['label_ascii_lower_sha256'] is None
                and pr['label_ascii_case_complete'] is False, 'unknown labels contradict a populated capture')
        return {'snapshot': 'unknown', 'reason': 'original event payload has no labels', **{k: pr[k] for k in fields}}
    for field in ('label_name_sha256', 'label_ascii_lower_sha256'):
        value = pr[field]
        require(isinstance(value, list) and value == sorted(set(value))
                and all(isinstance(d, str) and re.fullmatch(r'[0-9a-f]{64}', d) for d in value),
                'invalid original label hash list')
    require(len(pr['label_ascii_lower_sha256']) <= len(pr['label_name_sha256']), 'label hash counts contradict')
    if pr['label_ascii_case_complete']:
        require(bool(pr['label_ascii_lower_sha256']) == bool(pr['label_name_sha256']),
                'complete ASCII label capture has inconsistent emptiness')
    return {'snapshot': 'captured', **{k: pr[k] for k in fields}, 'workflow_guard_applicability_verified': False}


def default_branch_fact(event):
    if 'repository_default_branch' not in event:
        return {'snapshot': 'unknown'}
    value = event['repository_default_branch']
    require(isinstance(value, dict), 'invalid original default branch snapshot')
    if value.get('snapshot') == 'unknown':
        require(set(value) == {'snapshot'}, 'unknown default branch has captured fields')
    else:
        require(set(value) == {'snapshot', 'name'} and value['snapshot'] == 'captured'
                and isinstance(value['name'], str) and 0 < len(value['name']) <= 1024
                and not any(ord(char) < 32 for char in value['name']), 'invalid original default branch name')
    return value


def workflow_sources(raw):
    workflows = {}
    with tarfile.open(fileobj=io.BytesIO(raw), mode='r:') as tree:
        for member in tree:
            if re.fullmatch(r'\.github/workflows/[^/]+\.ya?ml', member.name):
                require(member.isfile(), 'workflow source cannot be a link')
                workflows[member.name] = workflow_fact(tree.extractfile(member).read())
    return workflows


def project(refs, root, request):
    require(isinstance(request, dict) and set(request) == {'schema', 'type', 'source'}
            and type(request['schema']) is int and request['schema'] == 1
            and request['type'] == 'trigger-basis-request', 'invalid trigger basis request')
    source_request, unused = refs.read(request['source'])
    basis = source.project(refs, root, source_request)
    archive, receipt_path = refs.read(source_request['archive'])

    def raw(name):
        require(name in archive['files'], 'mandatory trigger source missing: ' + name)
        return refs.blob({'path': (receipt_path.parent / name).relative_to(refs.root).as_posix(),
                          'sha256': archive['files'][name]['sha256']})[0]

    require(basis['checkout'] is not None, 'trigger basis requires original checkout source')
    workflows = workflow_sources(raw('checkout-source.tar'))
    require(workflows, 'original workflow sources are absent')
    event = g.json_bytes(raw('event-inputs.json'))
    original = archive_inputs.Archive(refs, source_request['archive'], basis['scope'])
    plan_members = original.artifact('ci-shard-plan')
    require('ci-event-inputs.json' in plan_members
            and g.json_bytes(plan_members['ci-event-inputs.json']) == event,
            'original event source differs from indexed plan artifact')
    pr = event.get('pull_request')
    if pr is not None:
        scope = basis['scope']
        require(scope['event'] == 'pull_request' and isinstance(pr, dict)
                and type(pr.get('number')) is int and pr['number'] == scope['pr']
                and pr.get('head_sha') == scope['head_sha'] and pr.get('base_sha') == scope['base_sha']
                and pr.get('head_ref') == scope['branch'] and pr.get('base_repository') == scope['repository']
                and pr.get('head_repository') == scope['repository']
                and pr.get('action') in ('opened', 'synchronize', 'reopened'), 'original PR trigger scope differs')
    labels = label_fact(pr) if pr is not None else {'snapshot': 'unknown', 'reason': 'original PR event record is absent'}
    workflow = g.receipt.event_workflow_source(event, basis['scope']['repository'])
    workflow_readback = {'runner_declaration': workflow, 'source_bytes_verified': False}
    if workflow['snapshot'] == 'captured':
        require(workflow['workflow_path'] == '.github/workflows/ci.yml', 'runner workflow path differs from original CI')
        if workflow['workflow_sha'] == basis['checkout']['sha']:
            require(workflow['workflow_path'] in workflows, 'runner workflow source is absent from bound checkout')
            workflow_readback.update(source_bytes_verified=True,
                                     source_sha256=workflows[workflow['workflow_path']]['source_sha256'],
                                     source_commit=basis['checkout']['sha'])
        else:
            workflow_readback['gap'] = 'declared workflow commit is separate from the archived checkout source'
    rulesets = []
    for name in sorted(archive['files']):
        if re.fullmatch(r'ruleset-[0-9]+\.json', name):
            data = raw(name)
            value = g.json_bytes(data)
            require(isinstance(value, dict) and type(value.get('id')) is int
                    and name == 'ruleset-' + str(value['id']) + '.json', 'ruleset identity differs')
            rulesets.append({'file': name, 'sha256': g.checksum(data), 'value': value})
    result = {'schema': 1, 'type': 'archived-trigger-source-basis', 'scope': basis['scope'], 'checkout': basis['checkout'],
            'workflows': workflows, 'runner_workflow_source': workflow_readback,
            'original_labels': labels, 'ruleset_records': rulesets,
            'source_blocks_readback_verified': True, 'R_G_complete': False,
            'whole_input_projection_verified': False, 'first_complete_PR_sample': False, 'performance_accepted': False,
            'unverified_components': ['workflow_YAML_and_dynamic_guard_evaluation', 'backend_path_filter_evaluation',
                                     'historical_ruleset_applicability_and_completeness', 'all_workflow_run_check_instances',
                                     'pull_request_target_base_source', 'R_G_execution_results'],
            'input_files': refs.finish()}
    if 'base_source' in basis:
        result['base_source'] = {**basis['base_source'], 'workflows': workflow_sources(raw('base-source.tar')),
                                'repository_default_branch': default_branch_fact(event),
                                'target_definition_used_by_backend_verified': False}
        result['input_files'] = refs.finish()
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for field in ('manifest', 'evidence-root', 'root', 'output'):
        parser.add_argument('--' + field, required=True)
    args = parser.parse_args()
    result = project(g.References(args.evidence_root), args.root, g.load(Path(args.manifest)))
    g.write_new(Path(args.output), result)
    print('original workflow/label/ruleset source facts read back; R/G applicability remains unverified')


if __name__ == '__main__':
    try:
        main()
    except (ValueError, OSError, KeyError, TypeError, tarfile.TarError) as error:
        print('trigger basis rejected: ' + str(error), file=sys.stderr)
        sys.exit(1)
