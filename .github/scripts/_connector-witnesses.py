#!/usr/bin/env python3
"""One source selector for both connector execution and global report admission."""
import argparse
from collections import Counter
import fnmatch
import hashlib
import importlib.util
from pathlib import Path
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
# This is the only pattern list. Duration hints never determine membership.
SELECTOR = 'PublishedExamplesIT,RealMysqlToMongo*IT,Nest*IT,DataBrowser*IT,Watch*IT,AnObjectIdReadsBackTheSameThroughBothFacesIT,SinkValueRoundTripIT'
LEDGER = Path('e2e/target/witness-ledger.txt')
TIERS = ('IN_PROCESS', 'REAL_PROCESS')
LEDGERS = Path('e2e/target/witness-ledgers')
MARKER = 'tapstate.published-case='


def inventory(root):
    patterns = SELECTOR.split(',')
    result = []
    for test in shards.discover(root):
        if test['module'] != 'e2e' or test['kind'] != 'it' or not any(
                fnmatch.fnmatchcase(test['class'].rsplit('.', 1)[-1], p) for p in patterns):
            continue
        if test['class'].endswith('.PublishedExamplesIT'):
            examples = sorted((root / 'e2e/examples').rglob('*.e2e.yml'))
            shards.require(examples, 'no published example specifications discovered')
            for example in examples:
                result.append(dict(test, example=example.relative_to(root / 'e2e').as_posix()))
        else:
            result.append(test)
    return result


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
    if args.command == 'plan':
        with tempfile.TemporaryDirectory() as tmp:
            source = Path(tmp) / 'inventory.json'
            shards.write(source, inventory(args.root))
            extra = ['--durations', args.durations] if args.durations else []
            engine('plan', args, '--inventory', source, '--count', '10', '--output', args.output, *extra)
        return
    plan = shards.load_plan(args, args.root)
    shards.require(plan['selection'] == 'inventory' and plan['cohort_hash'] == shards.cohort(inventory(args.root)),
                   'connector pattern cohort differs from plan')
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
        expected = expected_cases(inventory(args.root))
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
