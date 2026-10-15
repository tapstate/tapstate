#!/usr/bin/env python3
"""Derive a bounded ordinary-event trigger policy from original archived source."""
import argparse
import hashlib
import importlib.util
import json
from pathlib import Path
import re
import sys

sys.dont_write_bytecode = True
spec = importlib.util.spec_from_file_location('contract_basis', Path(__file__).with_name('_ci-trigger-basis.py'))
basis_reader = importlib.util.module_from_spec(spec)
spec.loader.exec_module(basis_reader)
g, require = basis_reader.g, basis_reader.require


class Unsupported(ValueError):
    pass


def scalar(text):
    text = text.strip()
    if text.startswith('"'):
        value = json.loads(text)
        if not isinstance(value, str):
            raise Unsupported('non-string filter item')
    elif text.startswith("'"):
        if not re.fullmatch(r"'[^'\\]*'", text):
            raise Unsupported('unsupported quoted filter item')
        value = text[1:-1]
    else:
        if not re.fullmatch(r'[A-Za-z0-9_./!*-]+', text):
            raise Unsupported('unsupported filter item')
        value = text
    if not value or len(value) > 4096:
        raise Unsupported('empty or oversized filter item')
    return value


def event_options(block, event_name='pull_request'):
    lines = block.splitlines()
    if not lines or not re.fullmatch(r'  ' + re.escape(event_name) + r':\s*', lines[0]):
        raise Unsupported('unsupported ordinary event structure')
    options, current = {}, None
    for line in lines[1:]:
        if not line.strip() or line.lstrip().startswith('#'):
            continue
        # Unsupported YAML stays unknown; this is not a general YAML parser.
        if '#' in line:
            raise Unsupported('unsupported inline filter comment')
        match = re.fullmatch(r'    (types|branches|branches-ignore|paths|paths-ignore):\s*(.*)', line)
        if match:
            current, value = match.groups()
            if current in options:
                raise Unsupported('duplicate event filter')
            options[current] = []
            if value:
                if not value.startswith('[') or not value.endswith(']'):
                    raise Unsupported('unsupported filter list')
                items = value[1:-1].split(',')
                options[current] = [scalar(item) for item in items]
                current = None
        else:
            item = re.fullmatch(r'      - (.+)', line)
            if item is None or current is None:
                raise Unsupported('unsupported event filter indentation or key')
            options[current].append(scalar(item[1]))
    if any(not values or len(values) > 2048 for values in options.values()):
        raise Unsupported('empty or oversized event filter')
    for positive in ('branches', 'paths'):
        if positive in options and positive + '-ignore' in options:
            raise Unsupported('conflicting event filters')
    return options


def pattern_match(pattern, path):
    if re.fullmatch(r'[A-Za-z0-9_. /-]+', pattern):
        return path == pattern
    if pattern.endswith('/**') and re.fullmatch(r'[A-Za-z0-9_. /-]+', pattern[:-3]):
        return path.startswith(pattern[:-2])
    raise Unsupported('unsupported filter pattern')


def included(patterns, path):
    result = False
    if not any(not pattern.startswith('!') for pattern in patterns):
        raise Unsupported('filter has no positive pattern')
    for pattern in patterns:
        negative = pattern.startswith('!')
        if pattern_match(pattern[1:] if negative else pattern, path):
            result = not negative
    return result


def complete_event_mapping(fact):
    if fact['declaration_structure'] != 'indented-block-source':
        raise Unsupported('unparsed workflow event source')
    recognized = set()
    for line in fact['on_source'].splitlines()[1:]:
        if not line.strip() or line.lstrip().startswith('#'):
            continue
        indentation = len(line) - len(line.lstrip(' '))
        if indentation < 2 or indentation == 3 or line[indentation:].startswith('\t'):
            raise Unsupported('unsupported event mapping indentation')
        if indentation == 2:
            header = re.match(r'^  ([A-Za-z0-9_]+):(?:\s|$)', line)
            if header is None or header[1] not in fact['event_blocks']:
                raise Unsupported('partially parsed workflow event mapping')
            recognized.add(header[1])
    if recognized != set(fact['event_blocks']):
        raise Unsupported('incomplete workflow event mapping')


def trigger(fact, action, base_ref, changed_paths, event_name='pull_request'):
    complete_event_mapping(fact)
    block = fact['event_blocks'].get(event_name)
    if block is None:
        return False
    options = event_options(block['source'], event_name)
    for name in ('branches', 'branches-ignore', 'paths', 'paths-ignore'):
        for pattern in options.get(name, []):
            if name.endswith('-ignore') and pattern.startswith('!'):
                raise Unsupported('unsupported negative ignore pattern')
            pattern_match(pattern.removeprefix('!'), '')
    if action not in options.get('types', ['opened', 'synchronize', 'reopened']):
        return False
    if 'branches' in options and not included(options['branches'], base_ref):
        return False
    if 'branches-ignore' in options and any(pattern_match(p, base_ref) for p in options['branches-ignore']):
        return False
    if 'paths' in options or 'paths-ignore' in options:
        if changed_paths is None:
            raise Unsupported('changed path set is not eligible for bounded evaluation')
        if 'paths' in options and not any(included(options['paths'], path) for path in changed_paths):
            return False
        if 'paths-ignore' in options and all(any(pattern_match(p, path) for p in options['paths-ignore']) for path in changed_paths):
            return False
    return True


def guard_fact(guard, labels):
    if guard['job_structure'] != 'literal-key':
        raise Unsupported('unparsed job guard owner')
    expression = guard['source'].split('if:', 1)[1].strip()
    if expression.startswith(('>-', '>')):
        expression = expression.split('\n', 1)[1]
    expression = ' '.join(expression.split())
    if expression.startswith('${{') and expression.endswith('}}'):
        expression = expression[3:-2].strip()
    if expression == 'always()':
        return {'predicate': 'native-result-required', 'value': None}
    prefix = "github.event_name != 'pull_request' || "
    if expression.startswith(prefix):
        expression = expression[len(prefix):]
    if expression == 'github.event.pull_request.head.repo.full_name == github.repository':
        return {'predicate': 'same-repository', 'value': True}
    match = re.fullmatch(r"contains\(github.event.pull_request.labels\.\*\.name, '([A-Za-z0-9_-]+)'\)", expression)
    if match:
        if labels.get('snapshot') != 'captured' or labels.get('label_ascii_case_complete') is not True:
            raise Unsupported('original ASCII label membership is unknown')
        identity = hashlib.sha256(match[1].lower().encode('ascii')).hexdigest()
        return {'predicate': 'original-label-membership', 'value': identity in labels['label_ascii_lower_sha256']}
    raise Unsupported('unsupported dynamic job guard')


def changed_paths(root, scope):
    variants = basis_reader.source.variants
    bases = variants.text(root, 'merge-base', '--all', scope['base_sha'], scope['head_sha']).splitlines()
    if len(bases) != 1:
        return None, {'status': 'unknown', 'reason': 'merge base is not unique'}
    raw = variants.git(root, 'diff', '--no-ext-diff', '--no-textconv', '--name-status', '-z', '-M',
                       bases[0], scope['head_sha'], '--').stdout
    tokens = raw.decode('utf-8').split('\0')
    paths, offset = [], 0
    while offset < len(tokens) - 1:
        status = tokens[offset]
        if status not in ('A', 'D', 'M', 'T') or offset + 1 >= len(tokens) - 1:
            return None, {'status': 'unknown', 'reason': 'rename, copy or unsupported diff status'}
        paths.append(tokens[offset + 1])
        offset += 2
    if len(paths) > 3000:
        return None, {'status': 'unknown', 'reason': 'changed paths exceed bounded filter scope'}
    paths = sorted(set(paths))
    return paths, {'status': 'source-derived', 'merge_base_sha': bases[0], 'path_count': len(paths),
                   'paths_sha256': g.checksum(g.canonical(paths)), 'backend_evaluation_observed': False}


def project(refs, root, request):
    require(isinstance(request, dict) and set(request) == {'schema', 'type', 'source'}
            and type(request['schema']) is int and request['schema'] == 1
            and request['type'] == 'trigger-contract-request', 'invalid trigger contract request')
    basis = basis_reader.project(refs, root, {'schema': 1, 'type': 'trigger-basis-request', 'source': request['source']})
    scope = basis['scope']
    require(scope['event'] == 'pull_request', 'trigger contract requires original ordinary PR event')
    source_request, unused = refs.read(request['source'])
    archive = basis_reader.archive_inputs.Archive(refs, source_request['archive'], scope)
    event = archive.json('event-inputs.json')
    pr = event.get('pull_request')
    require(isinstance(pr, dict) and pr.get('action') in ('opened', 'synchronize', 'reopened')
            and isinstance(pr.get('base_ref'), str) and pr['base_ref'], 'original PR filter identity missing')
    paths, diff = changed_paths(root, scope)
    workflows, unknown = {}, []
    if basis['runner_workflow_source']['source_bytes_verified'] is not True:
        unknown.append('CI runner workflow source is separate or unknown')
    for path, fact in sorted(basis['workflows'].items()):
        row = {'source_sha256': fact['source_sha256'], 'ordinary_trigger': 'unknown', 'job_guards': []}
        try:
            applies = trigger(fact, pr['action'], pr['base_ref'], paths)
            row['ordinary_trigger'] = applies
            if applies:
                for guard in fact['job_guard_blocks']:
                    row['job_guards'].append({'job': guard['job'], 'source_sha256': guard['sha256'],
                                              **guard_fact(guard, basis['original_labels'])})
        except (Unsupported, json.JSONDecodeError) as error:
            row['gap'] = str(error)
            unknown.append(path + ': ' + str(error))
        workflows[path] = row
    target_workflows = {}
    base = basis.get('base_source')
    default = {} if base is None else base['repository_default_branch']
    base_eligible = (base is not None and base['source_readback_verified'] is True
        and base['sha'] == scope['base_sha'] and default.get('snapshot') == 'captured'
        and default.get('name') == pr['base_ref'])
    if not base_eligible:
        unknown.append('original default/base branch source is unavailable or differs; target applicability unknown')
        for path, fact in basis['workflows'].items():
            if 'pull_request_target' in fact['event_blocks']:
                workflows[path]['target_trigger'] = 'unknown'
                workflows[path]['target_gap'] = 'checkout bytes cannot substitute for original default/base source'
    else:
        for path, fact in sorted(base['workflows'].items()):
            target = {'source_sha256': fact['source_sha256'], 'source_commit': base['sha'],
                      'target_trigger': 'unknown', 'backend_definition_used_verified': False}
            try:
                applies = trigger(fact, pr['action'], pr['base_ref'], paths, 'pull_request_target')
                target['target_trigger'] = applies
                if applies:
                    unknown.append(path + ': triggered target event membership and execution remain unverified')
            except (Unsupported, json.JSONDecodeError) as error:
                target['gap'] = str(error)
                unknown.append(path + ': target source policy: ' + str(error))
            target_workflows[path] = target
            if path in workflows:
                workflows[path]['target_trigger'] = target['target_trigger']
    require(workflows.get('.github/workflows/ci.yml', {}).get('ordinary_trigger') is not False,
            'original CI event contradicts its source trigger policy')
    return {'schema': 1, 'type': 'source-derived-trigger-contract', 'scope': scope,
            'checkout': basis['checkout'], 'event_action': pr['action'], 'base_ref': pr['base_ref'],
            'changed_paths': diff, 'workflows': workflows,
            'target_workflows': target_workflows,
            'original_default_base_source_eligible': base_eligible,
            'ordinary_applicable_workflows': sorted(path for path, fact in workflows.items() if fact['ordinary_trigger'] is True),
            'minimum_required_checks': sorted(g.REQUIRED_CHECKS),
            'source_policy_eligible': not unknown, 'unknown': unknown,
            'R_G_complete': False, 'backend_applicability_observed': False,
            'first_complete_PR_sample': False, 'performance_accepted': False,
            'input_files': refs.finish()}


def policy_head_status(policy, request):
    scope = {name: policy['scope'][name] for name in ('repository', 'pr', 'branch', 'head_sha')}
    contract = {'applicable_workflows': policy['ordinary_applicable_workflows'],
                'required_checks': policy['minimum_required_checks']}
    snapshot = g.head_status(scope, contract, request)
    require(snapshot['base_sha'] == policy['scope']['base_sha']
            and snapshot['merge_sha'] == policy['checkout']['sha'], 'current head/base/merge differs from original policy')
    reasons = list(snapshot['reasons'])
    if policy['source_policy_eligible'] is not True:
        reasons.append('original source trigger policy contains unknowns')
    return {**snapshot, 'can_advance_after_archiving': not reasons, 'reasons': reasons,
            'trigger_policy_source_bound': True, 'source_policy_eligible': policy['source_policy_eligible'],
            'trigger_policy_sha256': g.checksum(g.canonical(policy)),
            'R_G_complete': False, 'backend_applicability_observed': False,
            'all_workflow_event_membership_verified': False, 'performance_accepted': False}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for field in ('manifest', 'evidence-root', 'root', 'output'):
        parser.add_argument('--' + field, required=True)
    parser.add_argument('--head-status', action='store_true', help='join the derived policy to a fresh read-only head snapshot')
    args = parser.parse_args()
    manifest = Path(args.manifest)
    raw = g.file_bytes(manifest)
    refs = g.References(args.evidence_root)
    result = project(refs, args.root, g.json_bytes(raw))
    if args.head_status:
        if '.github/workflows/ci.yml' in result['ordinary_applicable_workflows']:
            result['head_status'] = policy_head_status(result, g.receipt.api)
        else:
            result['head_status'] = {'can_advance_after_archiving': False,
                'reasons': ['original CI trigger policy is unknown'], 'live_snapshot_observed': False}
    require(g.checksum(g.file_bytes(manifest)) == g.checksum(raw), 'trigger manifest changed during readback')
    require(refs.finish() == result['input_files'], 'trigger inputs changed during readback')
    result['manifest_sha256'] = g.checksum(raw)
    g.write_new(Path(args.output), result)
    print('bounded original trigger policy derived; unknowns remain ineligible')


if __name__ == '__main__':
    try:
        main()
    except (ValueError, OSError, KeyError, TypeError) as error:
        print('trigger contract rejected: ' + str(error), file=sys.stderr)
        sys.exit(1)
