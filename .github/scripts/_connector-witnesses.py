#!/usr/bin/env python3
"""One source selector for both connector execution and global report admission."""
import argparse
from collections import Counter
import fnmatch
import hashlib
import importlib.util
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile
import xml.etree.ElementTree as ET

sys.dont_write_bytecode = True

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location('shards', HERE / '_ci-shards.py')
shards = importlib.util.module_from_spec(spec)
spec.loader.exec_module(shards)
# Named witnesses include cases that configure connector jars without calling the gate.
SELECTOR = 'PublishedExamplesIT,RealMysqlToMongo*IT,Nest*IT,DataBrowser*IT,Watch*IT,AnObjectIdReadsBackTheSameThroughBothFacesIT,SinkValueRoundTripIT,*TargetPreparationIT'
GATE_CALL = re.compile(r'\bRealConnectorGate\s*\.\s*require\s*\(')
LEDGER = Path('e2e/target/witness-ledger.txt')
TIERS = ('IN_PROCESS', 'REAL_PROCESS')
LEDGERS = Path('e2e/target/witness-ledgers')
MARKER = 'tapstate.published-case='
DEFERRALS = Path('.github/connector-witness-deferrals.json')
ATLAS_INPUT = re.compile(r'\bSystem\s*\.\s*getenv\s*\(\s*"TAPSTATE_ATLAS_(?:TEST|STANDARD)_URI"\s*\)')
AUTHORIZED_DEFERRALS = frozenset({
    'io.tapstate.e2e.CloudAtlasObservabilityRecoveryIT',
    'io.tapstate.e2e.RealAtlasSourceHttpIT',
    'io.tapstate.e2e.RealAtlasToAtlasPipelineIT',
    'io.tapstate.e2e.RealMySqlToAtlasPipelineIT',
})
JAVA_NON_CODE = re.compile(r'"""[\s\S]*?"""|"(?:\\.|[^"\\])*"|\'(?:\\.|[^\'\\])*\'|/\*[\s\S]*?\*/|//[^\n]*')


def external_atlas_input(source):
    text = JAVA_NON_CODE.sub(lambda match: match.group() if match.group().startswith('"')
                             and not match.group().startswith('"""') else ' ', source)
    return ATLAS_INPUT.search(text) is not None


def inventory(root):
    patterns = SELECTOR.split(',')
    result = []
    for test in shards.discover(root):
        if test['module'] != 'e2e' or test['kind'] != 'it':
            continue
        name = test['class'].rsplit('.', 1)[-1]
        named = any(fnmatch.fnmatchcase(name, pattern) for pattern in patterns)
        source = root / 'e2e/src/test/java' / (test['class'].replace('.', '/') + '.java')
        if not named and not GATE_CALL.search(source.read_text()):
            continue
        if test['class'].endswith('.PublishedExamplesIT'):
            examples = sorted((root / 'e2e/examples').rglob('*.e2e.yml'))
            shards.require(examples, 'no published example specifications discovered')
            for example in examples:
                result.append(dict(test, example=example.relative_to(root / 'e2e').as_posix()))
        else:
            result.append(test)
    return result


def active_inventory(root):
    tests = inventory(root)
    path = root / DEFERRALS
    raw = path.read_bytes()
    policy = shards.read(path)
    shards.require(set(policy) == {'schema', 'issue', 'reason', 'classes'} and policy['schema'] == 1,
                   'invalid connector deferral policy')
    shards.require(policy['issue'] == 'https://github.com/tapstate/tapstate/issues/702', 'invalid deferral issue')
    shards.require(isinstance(policy['reason'], str) and policy['reason'].strip(), 'missing deferral reason')
    classes = policy['classes']
    shards.require(isinstance(classes, list) and all(isinstance(name, str) for name in classes)
                   and classes == sorted(set(classes)), 'deferrals must be unique sorted class names')
    shards.require(set(classes) <= AUTHORIZED_DEFERRALS, 'deferral exceeds the authorized four-class exception')
    known = {test['class'] for test in tests}
    shards.require(set(classes) <= known, 'deferral names an unknown connector witness')
    external = set()
    for test in tests:
        source = root / 'e2e/src/test/java' / (test['class'].replace('.', '/') + '.java')
        if external_atlas_input(source.read_text()):
            external.add(test['class'])
    shards.require(set(classes) <= external, 'only external Atlas fixture witnesses may be deferred')
    active = [test for test in tests if test['class'] not in classes]
    record = dict(policy, path=DEFERRALS.as_posix(), sha256=hashlib.sha256(raw).hexdigest())
    return active, record, bool(external - set(classes))


def expected_cases(tests):
    return Counter(t['example'] + ' on ' + tier for t in tests if 'example' in t for tier in TIERS)


def engine(command, args, *extra):
    subprocess.run([str(HERE / 'ci-shard.sh'), command, '--root', str(args.root), *map(str, extra)], check=True)


def strict_reports(root, selected):
    counts = Counter()
    identities = Counter()
    for report in (root / 'e2e/target/failsafe-reports').glob('TEST-*.xml'):
        suite = ET.parse(report).getroot()
        name = suite.get('name', '').split('$', 1)[0]
        shards.require(int(suite.get('skipped', '0')) == 0 and suite.find('.//skipped') is None,
                       'real witness skipped: ' + name)
        cases = suite.findall('testcase')
        shards.require(int(suite.get('tests', '0')) == len(cases), 'witness testcase count differs: ' + name)
        counts[name] += len(cases)
        if name.endswith('.PublishedExamplesIT'):
            for case in cases:
                markers = [line[len(MARKER):] for output in case.findall('system-out')
                           for line in (output.text or '').splitlines() if line.startswith(MARKER)]
                shards.require(len(markers) == 1, 'Published testcase needs exactly one execution identity')
                identities[markers[0]] += 1
    for test in selected['tests']:
        shards.require(counts[test['class']] > 0, 'real witness executed no tests: ' + test['class'])
    shards.require(identities == expected_cases(selected['tests']), 'Published testcase identities differ from assignment')
    return identities


def shard_ledger(path, tests):
    shards.require(path.is_file() and not path.is_symlink(), 'missing witness ledger')
    actual = Counter(path.read_text().splitlines())
    shards.require(actual == expected_cases(tests), 'witness ledger differs from assignment')
    return actual


def ledger_check(root, path):
    shards.require(path.is_file() and not path.is_symlink(), 'missing witness ledger')
    expected = [line.strip() for line in (root / 'e2e/witness-manifest.txt').read_text().splitlines()
                if line.strip() and not line.startswith('#')]
    actual = path.read_text().splitlines()
    shards.require(Counter(actual) == Counter(expected), 'witness ledger differs from the manifest')
    # Invoke the unchanged manifest/example gate against the exact ledger being admitted.
    with tempfile.TemporaryDirectory() as tmp:
        stage = Path(tmp)
        (stage / 'e2e/target').mkdir(parents=True)
        shutil.copytree(root / 'e2e/examples', stage / 'e2e/examples')
        shutil.copyfile(root / 'e2e/witness-manifest.txt', stage / 'e2e/witness-manifest.txt')
        shutil.copyfile(path, stage / LEDGER)
        subprocess.run([str(HERE / 'witness-gate.sh')], cwd=stage, check=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='command', required=True)
    for name in ['plan', 'run', 'pack', 'verify']:
        cmd = sub.add_parser(name)
        cmd.add_argument('--root', default='.', type=Path)
        if name == 'plan':
            cmd.add_argument('--durations')
        else:
            cmd.add_argument('--plan', required=True)
        if name in ['plan', 'pack']:
            cmd.add_argument('--output', required=True)
        if name in ['run', 'pack']:
            cmd.add_argument('--shard', required=True)
        if name == 'run':
            cmd.add_argument('--repo-local', required=True)
        if name == 'verify':
            cmd.add_argument('--artifacts', required=True, type=Path)
            cmd.add_argument('--restore', required=True, type=Path)
    args = parser.parse_args()
    args.root = args.root.resolve()
    tests, deferrals, requires_atlas = active_inventory(args.root)
    if args.command == 'plan':
        with tempfile.TemporaryDirectory() as tmp:
            source = Path(tmp) / 'inventory.json'
            shards.write(source, tests)
            extra = ['--durations', args.durations] if args.durations else []
            engine('plan', args, '--inventory', source, '--count', '10', '--output', args.output, *extra)
        plan = shards.read(args.output)
        plan.update(connector_deferrals=deferrals, requires_atlas_fixtures=requires_atlas)
        shards.write(args.output, plan)
        for name in deferrals['classes']:
            print('Deferred external Atlas witness: ' + name + ' (' + deferrals['issue'] + ')')
        return
    plan = shards.load_plan(args, args.root)
    shards.require(plan.get('connector_deferrals') == deferrals
                   and plan.get('requires_atlas_fixtures') is requires_atlas,
                   'connector deferral policy differs from plan')
    shards.require(plan['selection'] == 'inventory' and plan['cohort_hash'] == shards.cohort(tests),
                   'connector witness cohort differs from plan')
    shards.require(len(plan['shards']) == 10, 'connector lane requires ten shards')
    if args.command == 'run':
        (args.root / LEDGER).unlink(missing_ok=True)
        shutil.rmtree(args.root / LEDGERS, ignore_errors=True)
        engine('run', args, '--plan', args.plan, '--shard', args.shard, '--repo-local', args.repo_local)
    elif args.command == 'pack':
        selected = shards.selected(plan, args.shard)
        strict_reports(args.root, selected)
        paths = list((args.root / LEDGERS).glob('*'))
        has_examples = bool(expected_cases(selected['tests']))
        shards.require(not (args.root / LEDGER).exists(), 'unexpected legacy ledger in a connector shard')
        shards.require(len(paths) == int(has_examples), 'unexpected sweep JVM ledger set')
        if has_examples:
            shard_ledger(paths[0], selected['tests'])
        engine('pack', args, '--plan', args.plan, '--shard', args.shard, '--output', args.output)
        if has_examples:
            path = Path(args.output) / 'witness-ledger.txt'
            shutil.copyfile(paths[0], path)
            shards.write(Path(args.output) / 'ledger.json', dict(source_sha=plan['source_sha'], cohort_hash=plan['cohort_hash'],
                         shard_id=args.shard, sha256=hashlib.sha256(path.read_bytes()).hexdigest()))
    else:
        all_cases = Counter()
        all_ledgers = Counter()
        for selected in plan['shards']:
            directory = args.artifacts / selected['id']
            has_examples = bool(expected_cases(selected['tests']))
            allowed = {'files', 'manifest.json'} | ({'witness-ledger.txt', 'ledger.json'} if has_examples else set())
            shards.require(directory.is_dir() and not directory.is_symlink()
                           and {p.name for p in directory.iterdir()} == allowed,
                           'connector shard evidence differs: ' + selected['id'])
            all_cases.update(strict_reports(directory / 'files', selected))
            if has_examples:
                ledger = directory / 'witness-ledger.txt'
                all_ledgers.update(shard_ledger(ledger, selected['tests']))
                shards.require(shards.read(directory / 'ledger.json') == dict(source_sha=plan['source_sha'], cohort_hash=plan['cohort_hash'],
                               shard_id=selected['id'], sha256=hashlib.sha256(ledger.read_bytes()).hexdigest()), 'ledger identity differs')
        expected = expected_cases(tests)
        shards.require(all_cases == expected and all_ledgers == expected, 'global Published case or ledger multiset differs')
        # The existing release gate still consumes its original manifest vocabulary.
        # Keep every occurrence through translation; no set conversion can hide a duplicate.
        legacy = []
        for identity in all_ledgers.elements():
            example, tier = identity.rsplit(' on ', 1)
            legacy.append(Path(example).parent.name + ' on ' + tier)
        with tempfile.TemporaryDirectory() as tmp:
            ledger = Path(tmp) / 'witness-ledger.txt'
            ledger.write_text(''.join(line + '\n' for line in sorted(legacy)))
            ledger_check(args.root, ledger)
            engine('verify', args, '--plan', args.plan, '--artifacts', args.artifacts, '--restore', args.restore)
            (args.restore / LEDGER).parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(ledger, args.restore / LEDGER)


if __name__ == '__main__':
    try:
        main()
    except (ValueError, KeyError, OSError, ET.ParseError, subprocess.CalledProcessError) as error:
        print('connector-witnesses: ' + str(error), file=sys.stderr)
        sys.exit(1)
