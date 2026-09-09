#!/usr/bin/env python3
"""Source discovery, longest-first balancing, and fail-closed shard artifact admission."""
import argparse
import copy
from collections import Counter
import fnmatch
import hashlib
import json
from pathlib import Path
import re
import shutil
import subprocess
import sys
import xml.etree.ElementTree as ET


def require(ok, message):
    if not ok:
        raise ValueError(message)


def read(path):
    return json.loads(Path(path).read_text())


def write(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + '\n')


def key(test):
    return class_key(test) + ((test['example'],) if 'example' in test else ())


def class_key(test):
    return test['module'], test['kind'], test['class']


def cohort(tests):
    return hashlib.sha256(json.dumps(sorted(key(t) for t in tests)).encode()).hexdigest()


def safe_path(value):
    path = Path(value)
    require(not path.is_absolute() and '..' not in path.parts, 'unsafe artifact path: ' + value)
    return path


def pom(path):
    tree = ET.parse(path).getroot()
    for node in tree.iter():
        node.tag = node.tag.rsplit('}', 1)[-1]
    return tree


def reactor(root):
    """Default reactor only: profile-only modules are intentionally excluded."""
    modules = []
    def visit(directory, inherited_it=False):
        tree = pom(directory / 'pom.xml')
        bound_it = inherited_it or any(
            p.findtext('artifactId') == 'maven-failsafe-plugin'
            and p.find("executions/execution/goals/goal[.='integration-test']") is not None
            for p in tree.findall('build/plugins/plugin'))
        modules.append((directory.relative_to(root).as_posix(), bound_it))
        for node in tree.findall('modules/module'):
            visit(directory / node.text.strip(), bound_it)
    visit(root)
    return modules


def discover(root):
    tests = []
    for module, bound_it in reactor(root):
        for source in sorted((root / module / 'src/test/java').rglob('*.java')):
            name = source.stem
            unit = any(fnmatch.fnmatchcase(name, p) for p in ['Test*', '*Test', '*Tests', '*TestCase'])
            integration = bound_it and any(fnmatch.fnmatchcase(name, p) for p in ['IT*', '*IT', '*ITCase'])
            if not (unit or integration) or '$' in name:
                continue
            # Comments and literals may contain source fixtures, including declarations.
            text = re.sub(r'/\*[\s\S]*?\*/|//[^\n]*|"""[\s\S]*?"""|"(?:\\.|[^"\\])*"|\'(?:\\.|[^\'\\])*\'',
                          ' ', source.read_text())
            # JUnit does not instantiate abstract bases, even when their filenames match.
            if re.search(r'\babstract\s+class\s+' + re.escape(name) + r'\b', text):
                continue
            # A matching filename can also be a utility holder. JUnit cannot construct
            # a class with only a private constructor; do not demand a report for it.
            if re.search(r'\bprivate\s+' + re.escape(name) + r'\s*\(', text) and not re.search(
                    r'@(Test|ParameterizedTest|RepeatedTest|TestFactory|TestTemplate|Nested|ArchTest)\b', text):
                continue
            package = re.search(r'^\s*package\s+([\w.]+)\s*;', text, re.M)
            classname = (package.group(1) + '.' if package else '') + name
            for kind, selected in [('unit', unit), ('it', integration)]:
                if selected:
                    tests.append(dict(module=module, kind=kind, **{'class': classname}))
    return sorted(tests, key=key)


def execs(tests):
    return sorted({t['module'] + '/target/' + ('jacoco.exec' if t['kind'] == 'unit' else 'jacoco-it.exec')
                   for t in tests if not (t['module'] == 'e2e' and t['kind'] == 'it')})


def balance(tests, count, hints):
    require(count > 0, 'shard count must be positive')
    bins = [[] for _ in range(count)]
    totals = [0.0] * count
    def duration(test):
        identity = test['class'] + ('#' + test['example'] if 'example' in test else '')
        # New source-discovered units join automatically with a one-second fallback weight.
        value = hints.get(test['module'] + '/' + identity, hints.get(identity, 1.0))
        require(isinstance(value, (float, int)) and value >= 0, 'invalid duration hint')
        return float(value)
    for test in sorted(tests, key=lambda t: (-duration(t), key(t))):
        slot = min(range(count), key=lambda n: (totals[n], n))
        bins[slot].append(test)
        totals[slot] += duration(test)
    return [(sorted(tests, key=key), total) for tests, total in zip(bins, totals)]


def sha(root):
    result = subprocess.run(['git', '-C', str(root), 'rev-parse', 'HEAD'], capture_output=True, text=True)
    return result.stdout.strip() if result.returncode == 0 else 'unversioned'


def make_plan(args, root):
    tests = read(args.inventory) if args.inventory else discover(root)
    require(tests and len({key(t) for t in tests}) == len(tests), 'empty or duplicate test inventory')
    hints = read(args.durations) if args.durations else {}
    if args.inventory:
        groups = [(f'shard-{i+1}', ts, total) for i, (ts, total) in enumerate(balance(tests, args.count, hints))]
    else:
        e2e = [t for t in tests if t['module'] == 'e2e' and t['kind'] == 'it']
        engine = [t for t in tests if t['module'] == 'runtime/engine']
        rest = [t for t in tests if t not in e2e and t not in engine]
        groups = [(f'e2e-{i+1}', ts, total) for i, (ts, total) in enumerate(balance(e2e, 2, hints))]
        groups += [(name, *balance(ts, 1, hints)[0]) for name, ts in [('engine', engine), ('rest', rest)]]
    plan = dict(schema=1, source_sha=sha(root), cohort_hash=cohort(tests),
                selection='inventory' if args.inventory else 'default-reactor', expected=tests,
                shards=[dict(id=name, tests=ts, estimated_seconds=round(total, 3), required_exec=execs(ts))
                        for name, ts, total in groups])
    write(args.output, plan)
    print('planned ' + str(len(tests)) + ' source-selected units across ' + str(len(groups)) + ' shards')


def load_plan(args, root):
    plan = read(args.plan)
    require(plan['schema'] == 1, 'unsupported shard schema')
    require(plan['source_sha'] == sha(root), 'plan source SHA differs from checkout')
    tests = plan['expected']
    require(plan['cohort_hash'] == cohort(tests), 'invalid cohort hash')
    require(len({key(t) for t in tests}) == len(tests), 'duplicate expected class')
    if plan['selection'] == 'default-reactor':
        require(cohort(discover(root)) == plan['cohort_hash'], 'source-selected cohort differs from plan')
    else:
        require(set(class_key(t) for t in tests) <= set(class_key(t) for t in discover(root)), 'inventory contains unselected source classes')
        for test in tests:
            if 'example' in test:
                example = safe_path(test['example'])
                require(test['class'].endswith('.PublishedExamplesIT') and test['module'] == 'e2e'
                        and test['kind'] == 'it' and example.parts[0] == 'examples'
                        and example.name.endswith('.e2e.yml') and (root / 'e2e' / example).is_file(),
                        'invalid example scheduling unit')
    ids = [s['id'] for s in plan['shards']]
    require(len(set(ids)) == len(ids), 'duplicate shard ID')
    for shard in plan['shards']:
        safe_path(shard['id'])
        require('/' not in shard['id'], 'invalid shard ID')
        require(shard['required_exec'] == execs(shard['tests']), 'invalid required coverage manifest')
    actual = Counter(key(t) for s in plan['shards'] for t in s['tests'])
    require(actual == Counter(key(t) for t in tests), 'shard union differs from expected class set')
    return plan


def selected(plan, name):
    matches = [s for s in plan['shards'] if s['id'] == name]
    require(len(matches) == 1, 'unknown shard: ' + name)
    return matches[0]


def run_shard(args, root, plan):
    shard = selected(plan, args.shard)
    require(shard['tests'], 'cannot run empty shard')
    modules = sorted({t['module'] for t in shard['tests']})
    # Only stale test results are removed. install has built every module, including
    # test classes and sources consumed by repository-wide source-scanning gates.
    for module, _ in reactor(root):
        target = root / module / 'target'
        for directory in ['surefire-reports', 'failsafe-reports']:
            shutil.rmtree(target / directory, ignore_errors=True)
        for filename in ['jacoco.exec', 'jacoco-it.exec']:
            (target / filename).unlink(missing_ok=True)
    command = ['mvn', '-B', '-Dmaven.repo.local=' + str(Path(args.repo_local).resolve()), '-pl', ','.join(modules), 'verify']
    for kind, flag in [('unit', 'test'), ('it', 'it.test')]:
        names = sorted({t['class'] for t in shard['tests'] if t['kind'] == kind})
        command.append('-D' + flag + '=' + (','.join(names) if names else '__ci_shard_no_tests__'))
    # Selectors span several modules. Exact admission below catches any missing class;
    # Maven must permit the modules in which a selector has no local match.
    command += ['-Dsurefire.failIfNoSpecifiedTests=false', '-Dfailsafe.failIfNoSpecifiedTests=false']
    examples = [t['example'] for t in shard['tests'] if 'example' in t]
    if examples:
        command += ['-Dtapstate.e2e.published-examples=' + ','.join(examples),
                    '-DforkCount=1', '-DreuseForks=true', '-Djunit.jupiter.execution.parallel.enabled=false']
    subprocess.run(command, cwd=root, check=True)


def reports(root, shard):
    actual = set()
    seen = set()
    paths = []
    statistics = dict(reported_classes=0, tests=0, skipped=0, zero_case_classes=0)
    modules = ({module for module, _ in reactor(root)} if (root / 'pom.xml').exists() else
               {p.parent.parent.parent.relative_to(root).as_posix()
                for p in root.rglob('TEST-*.xml') if p.parent.name in ['surefire-reports', 'failsafe-reports']}
               | {t['module'] for t in shard['tests']})
    for module in sorted(modules):
        for kind, directory in [('unit', 'surefire-reports'), ('it', 'failsafe-reports')]:
            for path in sorted((root / module / 'target' / directory).glob('TEST-*.xml')):
                require(not path.is_symlink(), 'symlink report: ' + str(path))
                suite = ET.parse(path).getroot()
                require(suite.tag == 'testsuite', 'invalid JUnit report: ' + str(path))
                name = suite.get('name', '')
                # Restoration uses filenames as keys, so their identity must match the admitted suite.
                # This includes nested suites: the full name, including '$', owns its own report.
                require(path.name == 'TEST-' + name + '.xml', 'report filename differs from suite identity: ' + str(path))
                require(name and (module, kind, name) not in seen, 'duplicate or unnamed test report: ' + name)
                seen.add((module, kind, name))
                require(int(suite.get('failures', '0')) == 0 and int(suite.get('errors', '0')) == 0
                        and suite.find('.//failure') is None and suite.find('.//error') is None,
                        'non-success test report: ' + name)
                # Nested JUnit suites belong to the source-selected outer class. A zero-case
                # outer suite is legitimate; its nested suites and disabled tests still count.
                actual.add((module, kind, name.split('$', 1)[0]))
                for case in suite.findall('testcase'):
                    case_name = case.get('classname', name).split('$', 1)[0]
                    require(case_name == name.split('$', 1)[0], 'testcase outside report class: ' + case_name)
                count = int(suite.get('tests', '0'))
                skipped = int(suite.get('skipped', '0'))
                require(count >= 0 and 0 <= skipped <= count, 'invalid test counters: ' + name)
                statistics['reported_classes'] += 1
                statistics['tests'] += count
                statistics['skipped'] += skipped
                statistics['zero_case_classes'] += int(count == 0)
                paths.append(path)
    expected = {class_key(t) for t in shard['tests']}
    require(actual == expected, 'report class set differs: missing=' + str(sorted(expected - actual)) + ' extra=' + str(sorted(actual - expected)))
    for test in shard['tests']:
        compiled = root / test['module'] / 'target/test-classes' / (test['class'].replace('.', '/') + '.class')
        require(compiled.is_file() and not compiled.is_symlink(), 'missing compiled test class: ' + str(compiled))
    for name in shard['required_exec']:
        path = root / safe_path(name)
        require(path.is_file() and not path.is_symlink() and path.stat().st_size > 0, 'missing coverage data: ' + name)
    return paths, statistics, sorted(name for _, _, name in seen)


def pack(args, root, plan):
    shard = selected(plan, args.shard)
    report_paths, statistics, report_classes = reports(root, shard)
    output = Path(args.output)
    require(not output.exists(), 'artifact output already exists: ' + str(output))
    files = output / 'files'
    paths = report_paths + [root / p for p in shard['required_exec']]
    for module, _ in reactor(root):
        for folder in ['classes', 'test-classes']:
            paths += [p for p in (root / module / 'target' / folder).rglob('*') if p.is_file()]
    for path in paths:
        require(not path.is_symlink(), 'symlink artifact: ' + str(path))
        dest = files / path.relative_to(root)
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(path, dest)
    write(output / 'manifest.json', dict(schema=1, source_sha=plan['source_sha'], cohort_hash=plan['cohort_hash'],
                                        shard_id=shard['id'], tests=shard['tests'], required_exec=shard['required_exec'],
                                        statistics=statistics, report_classes=report_classes,
                                        files={p.relative_to(files).as_posix(): hashlib.sha256(p.read_bytes()).hexdigest()
                                               for p in sorted(files.rglob('*')) if p.is_file()}))
    print('packed ' + shard['id'] + ': ' + str(len(shard['tests'])) + ' source-selected units')


def verify(args, root, plan):
    artifacts = Path(args.artifacts)
    expected_ids = {s['id'] for s in plan['shards']}
    require(artifacts.is_dir(), 'missing artifact root')
    require({p.name for p in artifacts.iterdir()} == expected_ids, 'artifact shard set differs from declared shards')
    admitted = []
    for shard in plan['shards']:
        directory = artifacts / shard['id']
        manifest = read(directory / 'manifest.json')
        declared_files = manifest.pop('files')
        declared_statistics = manifest.pop('statistics')
        declared_classes = manifest.pop('report_classes')
        require(manifest == dict(schema=1, source_sha=plan['source_sha'], cohort_hash=plan['cohort_hash'],
                                 shard_id=shard['id'], tests=shard['tests'], required_exec=shard['required_exec']),
                'shard manifest differs: ' + shard['id'])
        files = directory / 'files'
        actual_files = {}
        for path in files.rglob('*'):
            require(not path.is_symlink(), 'symlink artifact: ' + str(path))
            if path.is_file():
                relative = path.relative_to(files).as_posix()
                require(re.fullmatch(r'.+/target/(?:classes/.+|test-classes/.+|(?:surefire|failsafe)-reports/TEST-[^/]+\.xml|jacoco(?:-it)?\.exec)', relative),
                        'unexpected artifact path: ' + relative)
                actual_files[relative] = hashlib.sha256(path.read_bytes()).hexdigest()
        require(actual_files == declared_files, 'artifact files differ from manifest: ' + shard['id'])
        _, statistics, report_classes = reports(files, shard)
        require(statistics == declared_statistics and report_classes == declared_classes,
                'report evidence differs from manifest: ' + shard['id'])
        print(shard['id'] + ': ' + json.dumps(statistics, sort_keys=True))
        required = set(shard['required_exec'])
        found_exec = {p.relative_to(files).as_posix() for p in files.rglob('*.exec')}
        require(found_exec == required, 'coverage file set differs: ' + shard['id'])
        admitted.append((shard, files))
    # Nothing reaches the restored tree until all shards passed admission.
    destination = Path(args.restore)
    shared_reports = {}
    for shard, files in admitted:
        for path in sorted(files.rglob('*')):
            require(not path.is_symlink(), 'symlink artifact: ' + str(path))
            if not path.is_file():
                continue
            relative = path.relative_to(files)
            if relative.as_posix() in shard['required_exec']:
                relative = Path('target/ci-shards') / shard['id'] / relative
            if path.name.startswith('TEST-') and any('example' in t and path.name == 'TEST-' + t['class'] + '.xml' for t in shard['tests']):
                shared_reports.setdefault(relative, []).append(path)
                relative = Path('target/ci-shards') / shard['id'] / relative
            target = destination / safe_path(relative.as_posix())
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(path, target)
    for relative, paths in shared_reports.items():
        suites = [ET.parse(path).getroot() for path in paths]
        merged = copy.deepcopy(suites[0])
        for node in list(merged):
            if node.tag != 'properties':
                merged.remove(node)
        for suite in suites:
            for case in suite.findall('testcase'):
                merged.append(copy.deepcopy(case))
        for attr in ['tests', 'failures', 'errors', 'skipped']:
            merged.set(attr, str(sum(int(s.get(attr, '0')) for s in suites)))
        merged.set('time', str(sum(float(s.get('time', '0')) for s in suites)))
        target = destination / safe_path(relative.as_posix())
        target.parent.mkdir(parents=True, exist_ok=True)
        ET.ElementTree(merged).write(target, encoding='utf-8', xml_declaration=True)
    print('admitted ' + str(len(plan['expected'])) + ' source-selected units exactly once across ' + str(len(admitted)) + ' shards')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='command', required=True)
    for name in ['plan', 'run', 'pack', 'verify']:
        command = commands.add_parser(name)
        command.add_argument('--root', default='.')
        if name == 'plan':
            command.add_argument('--durations')
            command.add_argument('--inventory')
            command.add_argument('--count', type=int, default=4)
            command.add_argument('--output', required=True)
        else:
            command.add_argument('--plan', required=True)
        if name in ['run', 'pack']:
            command.add_argument('--shard', required=True)
        if name == 'run':
            command.add_argument('--repo-local', required=True)
        if name == 'pack':
            command.add_argument('--output', required=True)
        if name == 'verify':
            command.add_argument('--artifacts', required=True)
            command.add_argument('--restore', required=True)
    args = parser.parse_args()
    root = Path(args.root).resolve()
    if args.command == 'plan':
        make_plan(args, root)
    else:
        plan = load_plan(args, root)
        {'run': run_shard, 'pack': pack, 'verify': verify}[args.command](args, root, plan)


if __name__ == '__main__':
    try:
        main()
    except (ValueError, KeyError, OSError, ET.ParseError, subprocess.CalledProcessError) as error:
        print('ci-shards: ' + str(error), file=sys.stderr)
        sys.exit(1)
