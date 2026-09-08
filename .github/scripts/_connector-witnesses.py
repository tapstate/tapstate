#!/usr/bin/env python3
"""One source selector for both connector execution and global report admission."""
import argparse
from collections import Counter
import fnmatch
import hashlib
import importlib.util
import json
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


def inventory(root):
    patterns = SELECTOR.split(',')
    return [test for test in shards.discover(root)
            if test['module'] == 'e2e' and test['kind'] == 'it'
            and any(fnmatch.fnmatchcase(test['class'].rsplit('.', 1)[-1], pattern) for pattern in patterns)]


def engine(command, args, *extra):
    subprocess.run([str(HERE / 'ci-shard.sh'), command, '--root', str(args.root), *map(str, extra)], check=True)


def strict_reports(root, selected):
    counts = Counter()
    for report in (root / 'e2e/target/failsafe-reports').glob('TEST-*.xml'):
        suite = ET.parse(report).getroot()
        name = suite.get('name', '').split('$', 1)[0]
        shards.require(int(suite.get('skipped', '0')) == 0 and suite.find('.//skipped') is None,
                       'real witness skipped: ' + name)
        cases = suite.findall('testcase')
        shards.require(int(suite.get('tests', '0')) == len(cases), 'witness testcase count differs: ' + name)
        counts[name] += len(cases)
    for test in selected['tests']:
        shards.require(counts[test['class']] > 0, 'real witness executed no tests: ' + test['class'])


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
            engine('plan', args, '--inventory', source, '--count', '4', '--output', args.output, *extra)
        return
    plan = shards.load_plan(args, args.root)
    shards.require(plan['selection'] == 'inventory' and plan['cohort_hash'] == shards.cohort(inventory(args.root)),
                   'connector pattern cohort differs from plan')
    shards.require(len(plan['shards']) == 4, 'connector lane requires four shards')
    sweep = [s for s in plan['shards'] if any(t['class'].endswith('.PublishedExamplesIT') for t in s['tests'])]
    shards.require(len(sweep) == 1, 'example sweep must occur in exactly one shard')
    if args.command == 'run':
        (args.root / LEDGER).unlink(missing_ok=True)
        engine('run', args, '--plan', args.plan, '--shard', args.shard, '--repo-local', args.repo_local)
    elif args.command == 'pack':
        selected = shards.selected(plan, args.shard)
        strict_reports(args.root, selected)
        if selected == sweep[0]:
            ledger_check(args.root, args.root / LEDGER)
        engine('pack', args, '--plan', args.plan, '--shard', args.shard, '--output', args.output)
        if selected == sweep[0]:
            path = Path(args.output) / 'witness-ledger.txt'
            shutil.copyfile(args.root / LEDGER, path)
            shards.write(Path(args.output) / 'ledger.json', dict(source_sha=plan['source_sha'], cohort_hash=plan['cohort_hash'],
                         shard_id=args.shard, sha256=hashlib.sha256(path.read_bytes()).hexdigest()))
    else:
        for selected in plan['shards']:
            directory = args.artifacts / selected['id']
            allowed = {'files', 'manifest.json'} | ({'witness-ledger.txt', 'ledger.json'} if selected == sweep[0] else set())
            shards.require(directory.is_dir() and {p.name for p in directory.iterdir()} == allowed,
                           'connector shard evidence differs: ' + selected['id'])
            strict_reports(directory / 'files', selected)
        directory = args.artifacts / sweep[0]['id']
        ledger = directory / 'witness-ledger.txt'
        shards.require(shards.read(directory / 'ledger.json') == dict(source_sha=plan['source_sha'], cohort_hash=plan['cohort_hash'],
                       shard_id=sweep[0]['id'], sha256=hashlib.sha256(ledger.read_bytes()).hexdigest()), 'ledger identity differs')
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
