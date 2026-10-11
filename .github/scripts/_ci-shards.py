#!/usr/bin/env python3
"""Source discovery, longest-first balancing, and fail-closed shard artifact admission."""
import argparse
import copy
from collections import Counter
from contextlib import contextmanager
from datetime import datetime, timezone
import fnmatch
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import time
import uuid
import zipfile
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
        # Source discovery implements Maven's default name patterns. Reject custom
        # selectors, including managed/profile/execution configuration, before a
        # plan can certify the same incomplete cohort on both sides of admission.
        for plugin in tree.findall('.//plugin'):
            if plugin.findtext('artifactId') in ('maven-surefire-plugin', 'maven-failsafe-plugin'):
                for config in plugin.findall('.//configuration'):
                    require(not any(config.find(name) is not None for name in
                                    ('includes', 'excludes', 'includesFile', 'excludesFile')),
                            'unsupported test selection in ' + str(directory / 'pom.xml'))
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


def execution_identity():
    run_id, attempt = os.environ.get('GITHUB_RUN_ID'), os.environ.get('GITHUB_RUN_ATTEMPT')
    if run_id is None and attempt is None:
        require(os.environ.get('GITHUB_ACTIONS') != 'true', 'missing Actions execution identity')
        return dict(execution='local', run_id=None, run_attempt=None)
    require(run_id is not None and attempt is not None and run_id.isdecimal() and attempt.isdecimal()
            and int(run_id) > 0 and int(attempt) > 0, 'invalid Actions execution identity')
    return dict(execution='github-actions', run_id=run_id, run_attempt=int(attempt))


@contextmanager
def phase_timing(phase, command, plan=None, shard=None):
    """Bounded observations only: no wait predicate, timeout, or lifecycle is changed."""
    identity = dict(schema=1, command=command, phase=phase, shard_id=shard,
                    source_sha=plan['source_sha'] if plan else None,
                    cohort_hash=plan['cohort_hash'] if plan else None, **execution_identity())
    started = time.monotonic_ns()
    state = dict(identity, event='start', monotonic_ns=started,
                 utc=datetime.now(timezone.utc).isoformat())
    print('CI_PHASE_JSON ' + json.dumps(state, sort_keys=True), flush=True)
    status = 'failure'
    try:
        yield state
        status = 'success'
    finally:
        ended = time.monotonic_ns()
        state.update(event='end', monotonic_ns=ended, elapsed_ns=ended-started,
                     started_monotonic_ns=started, status=status,
                     ended_utc=datetime.now(timezone.utc).isoformat())
        print('CI_PHASE_JSON ' + json.dumps(state, sort_keys=True), flush=True)


def owned_path(root, relative, operation='cleanup'):
    """Refuse links in every path component before any owned evidence is removed."""
    relative = safe_path(relative)
    current = root
    for index, part in enumerate(relative.parts):
        current = current / part
        require(not current.is_symlink(), 'unsafe ' + operation + ' path: ' + relative.as_posix())
        require(index == len(relative.parts)-1 or not current.exists() or current.is_dir(),
                'invalid ' + operation + ' path parent: ' + relative.as_posix())
    require(current.resolve().is_relative_to(root), 'unsafe ' + operation + ' path: ' + relative.as_posix())
    return current


def provenance_path(shard):
    return Path('target/ci-shards') / ('run-' + shard['id'] + '.json')


def reset_test_evidence(root, shard):
    paths = []
    for module, _ in reactor(root):
        target = Path(module) / 'target'
        paths += [(target / name, True) for name in ['surefire-reports', 'failsafe-reports']]
        paths += [(target / name, False) for name in ['jacoco.exec', 'jacoco-it.exec',
                    'site/jacoco/jacoco.xml', 'site/jacoco-it/jacoco.xml']]
    paths += [(Path('e2e/target/witness-ledger.txt'), False),
              (Path('e2e/target/witness-ledgers'), True), (Path('target/jacoco-reactor.exec'), False),
              (provenance_path(shard), False)]
    # Preflight the whole set, including absent paths and dangling links. A refusal
    # must not partially reset another module or follow a target link out of checkout.
    owned = [(owned_path(root, path.as_posix()), directory) for path, directory in paths]
    for path, directory in owned:
        require(not path.exists() or (path.is_dir() if directory else path.is_file()),
                'invalid cleanup path type: ' + str(path))
    for path, directory in owned:
        if directory:
            if path.exists():
                shutil.rmtree(path)
        else:
            path.unlink(missing_ok=True)


def validate_provenance(value, plan, shard):
    identity = execution_identity()
    require(isinstance(value, dict), 'invalid run provenance object')
    require(type(value.get('schema')) is int and value['schema'] == 1 and value.get('source_sha') == plan['source_sha']
            and value.get('cohort_hash') == plan['cohort_hash'] and value.get('shard_id') == shard['id']
            and all(value.get(k) == v for k, v in identity.items()), 'run provenance differs: ' + shard['id'])
    require(value.get('run_attempt') is None or type(value['run_attempt']) is int, 'invalid run attempt type')
    require(value.get('status') == 'success', 'selected verify did not succeed: ' + shard['id'])
    observer = value.get('case_observer')
    require(isinstance(observer, dict) and type(observer.get('enabled')) is bool,
            'invalid case observer provenance')
    require(observer.get('providers') == ([OBSERVER] if observer['enabled'] else []),
            'case observer provider set differs')
    if observer['enabled']:
        require(isinstance(observer.get('build_inputs_sha256'), str)
                and re.fullmatch(r'[0-9a-f]{64}', observer['build_inputs_sha256'])
                and type(observer.get('audit_elapsed_ns')) is int and observer['audit_elapsed_ns'] >= 0,
                'invalid case observer input or cost binding')
        hashes = observer.get('observer_files')
        require(isinstance(hashes, dict) and set(hashes) == set(OBSERVER_FILES)
                and all(isinstance(h, str) and re.fullmatch(r'[0-9a-f]{64}', h) for h in hashes.values()),
                'invalid case observer class/service binding')
        scopes = observer.get('checked_scopes')
        require(isinstance(scopes, list) and all(isinstance(scope, dict) for scope in scopes),
                'invalid case observer checked scopes')
        actual = Counter((scope.get('module'), scope.get('kind')) for scope in scopes)
        expected = Counter({(t['module'], t['kind']): 1 for t in shard['tests']})
        require(actual == expected and all(isinstance(scope.get('dependency_excludes'), list)
                and scope['dependency_excludes'] == sorted(set(scope['dependency_excludes']))
                and all(isinstance(excluded, str) and re.fullmatch(r'[\w.-]+:[\w.-]+', excluded)
                        for excluded in scope['dependency_excludes']) for scope in scopes),
                'case observer checked classpath scopes differ')
    require(re.fullmatch(r'[0-9a-f]{32}', value.get('invocation_id', '')) is not None,
            'invalid run invocation: ' + shard['id'])
    for phase in ['reset', 'verify']:
        observed = value.get(phase, {})
        require(isinstance(observed, dict), 'invalid run phase object: ' + phase)
        require(observed.get('run_attempt') is None or type(observed['run_attempt']) is int, 'invalid phase attempt type')
        require(type(observed.get('schema')) is int and observed['schema'] == 1 and observed.get('command') == 'run'
                and observed.get('status') == 'success' and observed.get('event') == 'end'
                and observed.get('phase') == ('reset-test-evidence' if phase == 'reset' else 'selected-verify')
                and observed.get('source_sha') == plan['source_sha'] and observed.get('cohort_hash') == plan['cohort_hash']
                and observed.get('shard_id') == shard['id']
                and all(observed.get(k) == v for k, v in identity.items()), 'invalid run phase: ' + phase)
        start, end, elapsed = (observed.get(k) for k in ['started_monotonic_ns', 'monotonic_ns', 'elapsed_ns'])
        require(all(type(n) is int and n >= 0 for n in [start, end, elapsed])
                and end >= start and elapsed == end-start, 'invalid monotonic run interval: ' + phase)
        for field in ['utc', 'ended_utc']:
            require(isinstance(observed.get(field), str), 'missing run timestamp')
            timestamp = datetime.fromisoformat(observed[field])
            require(timestamp.tzinfo is not None, 'run timestamp has no timezone')
    require(value['verify']['started_monotonic_ns'] >= value['reset']['monotonic_ns'],
            'verify precedes reset: ' + shard['id'])


def ledger_paths(root, shard):
    result = []
    if 'e2e' not in {t['module'] for t in shard['tests']}:
        return result
    legacy = owned_path(root, 'e2e/target/witness-ledger.txt')
    if legacy.exists():
        require(legacy.is_file(), 'invalid witness ledger')
        result.append(legacy)
    directory = owned_path(root, 'e2e/target/witness-ledgers')
    if directory.exists():
        require(directory.is_dir(), 'invalid witness ledger directory')
        for path in sorted(directory.iterdir()):
            require(path.is_file() and not path.is_symlink() and re.fullmatch(r'[0-9]+\.txt', path.name),
                    'unexpected witness ledger path: ' + str(path))
            result.append(path)
    return sorted(result)


OBSERVER = 'io.tapstate.testsupport.CiCaseEvidence'
EXTENSION_SERVICE = 'META-INF/services/org.junit.jupiter.api.extension.Extension'
OBSERVER_PREFIX = 'test-support/target/test-classes/'
OBSERVER_FILES = [OBSERVER_PREFIX + 'io/tapstate/testsupport/CiCaseEvidence.class',
                  OBSERVER_PREFIX + 'io/tapstate/testsupport/CiCaseEvidence$Canonical.class',
                  OBSERVER_PREFIX + EXTENSION_SERVICE]


def file_digest(path):
    result = hashlib.sha256()
    with path.open('rb') as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b''):
            result.update(chunk)
    return result.hexdigest()


def extension_providers(content):
    require(len(content) <= 65536, 'extension service registry exceeds audit bound')
    providers = set()
    for raw in content.decode('utf-8').splitlines():
        name = raw.split('#', 1)[0].strip()
        if name:
            require(re.fullmatch(r'[A-Za-z_$][\w$]*(?:\.[A-Za-z_$][\w$]*)*', name) is not None,
                    'invalid extension service declaration')
            providers.add(name)
    return providers


def classpath_exclusions(root, module, kind, inputs):
    plugin_name = 'maven-surefire-plugin' if kind == 'unit' else 'maven-failsafe-plugin'
    directories, directory = [root], root
    for part in safe_path(module).parts:
        directory = directory / part
        if directory != root and (directory / 'pom.xml').is_file():
            directories.append(directory)
    result = set()
    for directory in directories:
        path = directory / 'pom.xml'
        require(inputs.get('source_files', {}).get(path.relative_to(root).as_posix()) == file_digest(path),
                'classpath audit POM changed or lacks input binding')
        tree = pom(path)
        supported = tree.findall('build/plugins/plugin') + tree.findall('build/pluginManagement/plugins/plugin')
        for plugin in tree.findall('.//plugin'):
            if plugin.findtext('artifactId') != plugin_name:
                continue
            for config in plugin.findall('.//configuration'):
                unsupported = ['classpathDependencyScopeExclude', 'additionalClasspathElements',
                               'additionalClasspathDependencies', 'testClassesDirectory', 'classesDirectory']
                require(not any(config.find(name) is not None for name in unsupported),
                        'unsupported effective test classpath customization')
                excludes = config.find('classpathDependencyExcludes')
                if excludes is None:
                    continue
                require(plugin in supported and config is plugin.find('configuration')
                        and not excludes.attrib, 'conditional or execution classpath exclusion requires effective model evidence')
                values = {node.text.strip() for node in excludes.findall('classpathDependencyExclude') if node.text}
                require(len(values) == len(list(excludes)) and all(re.fullmatch(r'[\w.-]+:[\w.-]+', value) for value in values),
                        'unsupported classpath dependency exclusion')
                require(not result or result == values, 'ambiguous inherited classpath exclusions')
                result = values
    return result


def classpath_coordinate(root, scope, relative, inputs):
    parts = safe_path(relative).parts
    if scope == 'repository':
        require(len(parts) >= 4, 'cannot prove Maven coordinates for classpath exclusion')
        return '.'.join(parts[:-3]) + ':' + parts[-3]
    require('/target/' in relative, 'cannot prove checkout coordinates for classpath exclusion')
    module = relative.split('/target/', 1)[0]
    path = owned_path(root, module + '/pom.xml', 'classpath')
    require(path.is_file() and inputs.get('source_files', {}).get(path.relative_to(root).as_posix()) == file_digest(path),
            'checkout classpath POM lacks input binding')
    tree = pom(path)
    group = tree.findtext('groupId') or tree.findtext('parent/groupId')
    artifact = tree.findtext('artifactId')
    require(group and artifact, 'cannot prove checkout coordinates for classpath exclusion')
    return group + ':' + artifact


def case_observer(args, root, plan, modules):
    if not args.case_evidence:
        require(args.build_inputs is None, '--build-inputs requires --case-evidence')
        return dict(enabled=False, providers=[])
    require(args.build_inputs is not None, '--case-evidence requires --build-inputs')
    started = time.monotonic_ns()
    observer_files = {name: owned_path(root, name, 'observer') for name in OBSERVER_FILES}
    require(all(path.is_file() for path in observer_files.values()),
            'case observer classes and service must be built before selected verify')
    observer_hashes = {name: file_digest(path) for name, path in observer_files.items()}
    require(extension_providers(observer_files[OBSERVER_PREFIX + EXTENSION_SERVICE].read_bytes()) == {OBSERVER},
            'case observer service registration differs')
    manifest_path = Path(args.build_inputs)
    require(manifest_path.is_file() and not manifest_path.is_symlink(), 'invalid build inputs file')
    inputs = read(manifest_path)
    identity = execution_identity()
    run_id = identity['run_id'] if identity['execution'] == 'github-actions' else 'local'
    attempt = identity['run_attempt'] if identity['execution'] == 'github-actions' else 1
    require(isinstance(inputs, dict) and type(inputs.get('schema')) is int and inputs['schema'] == 1
            and inputs.get('phase') == 'pre-test-after-install' and inputs.get('source_sha') == plan['source_sha']
            and inputs.get('run_id') == run_id and str(inputs.get('run_attempt')) == str(attempt),
            'case observer build inputs source/run/attempt differs')
    for name, actual_hash in observer_hashes.items():
        if name.endswith('.class'):
            require(inputs.get('test_files', {}).get(name) == actual_hash, 'case observer class differs from captured build input')
    classpaths = inputs.get('resolved_test_classpaths')
    require(isinstance(classpaths, dict) and set(modules) <= set(classpaths),
            'case observer requires resolved test classpaths for every selected module')
    repository = Path(args.repo_local).resolve()
    inspected, scopes = {}, []
    shard = selected(plan, args.shard)
    for module in modules:
        providers = {OBSERVER}
        for directory in ['classes', 'test-classes']:
            relative = (Path(module) / 'target' / directory).as_posix()
            output = owned_path(root, relative, 'observer')
            require(directory != 'test-classes' or output.is_dir(), 'missing prepared test classes: ' + module)
            if output.exists():
                require(output.is_dir(), 'invalid prepared classes directory')
                require(not (output / 'module-info.class').exists(), 'module-path observer audit requires effective model evidence')
                service = owned_path(root, relative + '/' + EXTENSION_SERVICE, 'observer')
                if service.exists():
                    require(service.is_file(), 'invalid extension service file')
                    providers.update(extension_providers(service.read_bytes()))
                for observer_name in ['CiCaseEvidence.class', 'CiCaseEvidence$Canonical.class']:
                    candidate = owned_path(root, relative + '/io/tapstate/testsupport/' + observer_name, 'observer')
                    require(not candidate.exists() or file_digest(candidate) == observer_hashes[
                                OBSERVER_PREFIX + 'io/tapstate/testsupport/' + observer_name],
                            'case observer class shadowed in prepared output')
        require(providers == {OBSERVER}, 'unexpected extension provider in prepared module classes: ' + module)
        kinds = sorted({test['kind'] for test in shard['tests'] if test['module'] == module})
        exclusions = {kind: classpath_exclusions(root, module, kind, inputs) for kind in kinds}
        entries = classpaths[module]
        require(isinstance(entries, list), 'invalid resolved test classpath')
        active_providers = {kind: set(providers) for kind in kinds}
        for entry in entries:
            require(isinstance(entry, dict) and isinstance(entry.get('identity'), str)
                    and type(entry.get('size')) is int and entry['size'] >= 0
                    and isinstance(entry.get('sha256'), str) and re.fullmatch(r'[0-9a-f]{64}', entry['sha256']),
                    'invalid resolved classpath entry')
            scope, separator, relative = entry['identity'].partition('/')
            require(separator and scope in ['repository', 'checkout'] and relative and '\\' not in relative,
                    'invalid resolved classpath identity')
            path = owned_path(repository if scope == 'repository' else root, relative, 'classpath')
            require(path.is_file(), 'missing resolved test classpath file')
            coordinate = classpath_coordinate(root, scope, relative, inputs) if any(exclusions.values()) else None
            active = [kind for kind in kinds if coordinate not in exclusions[kind]]
            if path not in inspected:
                inspected[path] = [file_digest(path), path.stat().st_size, None]
            actual_hash, actual_size, jar_providers = inspected[path]
            require(actual_hash == entry['sha256'] and actual_size == entry['size'],
                    'resolved test classpath bytes changed after input capture')
            if active and jar_providers is None:
                jar_providers = set()
                with zipfile.ZipFile(path) as archive:
                    services = [member for member in archive.infolist() if member.filename == EXTENSION_SERVICE]
                    require(len(services) <= 1, 'duplicate extension service in resolved classpath JAR')
                    if services:
                        require(services[0].file_size <= 65536, 'extension service registry exceeds audit bound')
                        jar_providers = extension_providers(archive.read(services[0]))
                    for observer_name in ['CiCaseEvidence.class', 'CiCaseEvidence$Canonical.class']:
                        member = 'io/tapstate/testsupport/' + observer_name
                        if member in archive.namelist():
                            require(archive.getinfo(member).file_size == observer_files[OBSERVER_PREFIX + member].stat().st_size,
                                    'case observer class shadowed in active dependency')
                            require(hashlib.sha256(archive.read(member)).hexdigest() == observer_hashes[OBSERVER_PREFIX + member],
                                    'case observer class shadowed in active dependency')
                inspected[path][2] = jar_providers
            for kind in active:
                active_providers[kind].update(jar_providers)
        for kind in kinds:
            require(active_providers[kind] == {OBSERVER},
                    'unexpected extension provider in actual selected test classpath: ' + module + '/' + kind)
            scopes.append(dict(module=module, kind=kind, dependency_excludes=sorted(exclusions[kind])))
    return dict(enabled=True, providers=[OBSERVER], checked_scopes=scopes,
                build_inputs_sha256=file_digest(manifest_path), observer_files=observer_hashes,
                audit_elapsed_ns=time.monotonic_ns()-started)


def run_shard(args, root, plan):
    shard = selected(plan, args.shard)
    require(shard['tests'], 'cannot run empty shard')
    modules = sorted({t['module'] for t in shard['tests']})
    if args.case_evidence:
        with phase_timing('audit-case-observer', 'run', plan, shard['id']):
            observer = case_observer(args, root, plan, modules)
    else:
        observer = case_observer(args, root, plan, modules)
    # Retain installed outputs and the original verify hooks; only declared test
    # evidence and this shard's freshness receipt are reset in the owned checkout.
    with phase_timing('reset-test-evidence', 'run', plan, shard['id']) as reset:
        reset_test_evidence(root, shard)
    receipt = dict(schema=1, source_sha=plan['source_sha'], cohort_hash=plan['cohort_hash'],
                   shard_id=shard['id'], invocation_id=uuid.uuid4().hex,
                   status='running', reset=reset, case_observer=observer, **execution_identity())
    marker = owned_path(root, provenance_path(shard).as_posix())
    write(marker, receipt)
    command = ['mvn', '-B', '-Dmaven.repo.local=' + str(Path(args.repo_local).resolve()), '-pl', ','.join(modules), 'verify']
    for kind, flag in [('unit', 'test'), ('it', 'it.test')]:
        names = sorted({t['class'] for t in shard['tests'] if t['kind'] == kind})
        command.append('-D' + flag + '=' + (','.join(names) if names else '__ci_shard_no_tests__'))
    if args.case_evidence:
        command += ['-Dmaven.test.additionalClasspath=' + str(root / 'test-support/target/test-classes'),
                    '-Djunit.jupiter.extensions.autodetection.enabled=true', '-Dtapstate.ci.case-evidence=true']
    # Selectors span several modules. Exact admission below catches any missing class;
    # Maven must permit the modules in which a selector has no local match.
    command += ['-Dsurefire.failIfNoSpecifiedTests=false', '-Dfailsafe.failIfNoSpecifiedTests=false']
    examples = [t['example'] for t in shard['tests'] if 'example' in t]
    if examples:
        command += ['-Dtapstate.e2e.published-examples=' + ','.join(examples),
                    '-DforkCount=1', '-DreuseForks=true', '-Djunit.jupiter.execution.parallel.enabled=false']
    try:
        with phase_timing('selected-verify', 'run', plan, shard['id']) as observed:
            subprocess.run(command, cwd=root, check=True)
    finally:
        receipt.update(status=observed['status'], verify=observed)
        write(marker, receipt)


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
    marker = owned_path(root, provenance_path(shard).as_posix())
    require(marker.is_file(), 'missing current run provenance: ' + shard['id'])
    provenance = read(marker)
    validate_provenance(provenance, plan, shard)
    if provenance['case_observer']['enabled']:
        require(all(owned_path(root, name, 'observer').is_file()
                    and file_digest(owned_path(root, name, 'observer')) == digest
                    for name, digest in provenance['case_observer']['observer_files'].items()),
                'case observer classes/service changed after selected verify')
    witness_ledgers = ledger_paths(root, shard)
    report_paths, statistics, report_classes = reports(root, shard)
    output = Path(args.output)
    require(not output.exists(), 'artifact output already exists: ' + str(output))
    files = output / 'files'
    paths = report_paths + [root / p for p in shard['required_exec']] + witness_ledgers + [marker]
    for module in sorted({t['module'] for t in shard['tests']}):
        paths += [p for p in (root / module / 'target/test-classes').rglob('*') if p.is_file()]
    for path in paths:
        require(not path.is_symlink(), 'symlink artifact: ' + str(path))
        owned_path(root, path.relative_to(root).as_posix())
        dest = files / path.relative_to(root)
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(path, dest)
    write(output / 'manifest.json', dict(schema=2, source_sha=plan['source_sha'], cohort_hash=plan['cohort_hash'],
                                        shard_id=shard['id'], tests=shard['tests'], required_exec=shard['required_exec'],
                                        statistics=statistics, report_classes=report_classes,
                                        provenance=provenance, provenance_path=provenance_path(shard).as_posix(),
                                        witness_ledgers=[p.relative_to(root).as_posix() for p in witness_ledgers],
                                        files={p.relative_to(files).as_posix(): hashlib.sha256(p.read_bytes()).hexdigest()
                                               for p in sorted(files.rglob('*')) if p.is_file()}))
    print('packed ' + shard['id'] + ': ' + str(len(shard['tests'])) + ' source-selected units')


def verify(args, root, plan):
    artifacts = Path(args.artifacts)
    require(not artifacts.is_symlink(), 'symlink artifact root')
    artifacts = artifacts.resolve()
    expected_ids = {s['id'] for s in plan['shards']}
    require(artifacts.is_dir(), 'missing artifact root')
    require({p.name for p in artifacts.iterdir()} == expected_ids, 'artifact shard set differs from declared shards')
    admitted = []
    for shard in plan['shards']:
        directory = artifacts / shard['id']
        require(directory.is_dir() and not directory.is_symlink(), 'invalid shard artifact directory')
        require((directory / 'manifest.json').is_file() and not (directory / 'manifest.json').is_symlink(), 'invalid shard manifest file')
        manifest = read(directory / 'manifest.json')
        declared_files = manifest.pop('files')
        declared_statistics = manifest.pop('statistics')
        declared_classes = manifest.pop('report_classes')
        provenance = manifest.pop('provenance')
        marker = manifest.pop('provenance_path')
        ledgers = manifest.pop('witness_ledgers')
        validate_provenance(provenance, plan, shard)
        require(marker == provenance_path(shard).as_posix(), 'run provenance path differs')
        require(isinstance(ledgers, list) and ledgers == sorted(set(ledgers))
                and all(re.fullmatch(r'e2e/target/(?:witness-ledger\.txt|witness-ledgers/[0-9]+\.txt)', p) for p in ledgers)
                and (not ledgers or 'e2e' in {t['module'] for t in shard['tests']}), 'invalid witness ledger manifest')
        require(manifest == dict(schema=2, source_sha=plan['source_sha'], cohort_hash=plan['cohort_hash'],
                                 shard_id=shard['id'], tests=shard['tests'], required_exec=shard['required_exec']),
                'shard manifest differs: ' + shard['id'])
        files = directory / 'files'
        require(files.is_dir() and not files.is_symlink(), 'invalid artifact files directory')
        actual_files = {}
        for path in files.rglob('*'):
            require(not path.is_symlink(), 'symlink artifact: ' + str(path))
            if path.is_file():
                relative = path.relative_to(files).as_posix()
                metadata = relative == marker or relative in ledgers
                require(metadata or re.fullmatch(r'.+/target/(?:test-classes/.+|(?:surefire|failsafe)-reports/TEST-[^/]+\.xml|jacoco(?:-it)?\.exec)', relative),
                        'unexpected artifact path: ' + relative)
                require(metadata or relative.split('/target/', 1)[0] in {t['module'] for t in shard['tests']},
                        'artifact outside shard modules: ' + relative)
                actual_files[relative] = hashlib.sha256(path.read_bytes()).hexdigest()
        require(actual_files == declared_files, 'artifact files differ from manifest: ' + shard['id'])
        require(marker in actual_files and read(files / safe_path(marker)) == provenance,
                'raw run provenance differs: ' + shard['id'])
        actual_ledgers = [p.relative_to(files).as_posix() for p in ledger_paths(files, shard)]
        require(actual_ledgers == ledgers, 'witness ledger file set differs: ' + shard['id'])
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
    require(not destination.is_symlink(), 'unsafe restore path: destination root')
    destination = destination.resolve()
    shared_reports = {}
    restorations = []
    for shard, files in admitted:
        for path in sorted(files.rglob('*')):
            require(not path.is_symlink(), 'symlink artifact: ' + str(path))
            if not path.is_file():
                continue
            relative = path.relative_to(files)
            if relative.as_posix() == provenance_path(shard).as_posix():
                relative = Path('target/ci-shards') / shard['id'] / 'run.json'
            elif re.fullmatch(r'e2e/target/(?:witness-ledger\.txt|witness-ledgers/[0-9]+\.txt)', relative.as_posix()):
                relative = Path('target/ci-shards') / shard['id'] / relative
            if relative.as_posix() in shard['required_exec']:
                relative = Path('target/ci-shards') / shard['id'] / relative
            if path.name.startswith('TEST-') and any('example' in t and path.name == 'TEST-' + t['class'] + '.xml' for t in shard['tests']):
                shared_reports.setdefault(relative, []).append(path)
                merge_target = owned_path(destination, relative.as_posix(), 'restore')
                require(not merge_target.exists() or merge_target.is_file(),
                        'invalid restore path type: ' + str(merge_target))
                relative = Path('target/ci-shards') / shard['id'] / relative
            target = owned_path(destination, relative.as_posix(), 'restore')
            require(not target.exists() or target.is_file(), 'invalid restore path type: ' + str(target))
            restorations.append((path, target))
    # All artifacts and every destination are admitted before the first copy.
    for path, target in restorations:
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
        target = owned_path(destination, relative.as_posix(), 'restore')
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
            command.add_argument('--case-evidence', action='store_true', help='enable the passive built case observer')
            command.add_argument('--build-inputs', help='current pre-test resolved classpaths required by the observer')
        if name == 'pack':
            command.add_argument('--output', required=True)
        if name == 'verify':
            command.add_argument('--artifacts', required=True)
            command.add_argument('--restore', required=True)
    args = parser.parse_args()
    root = Path(args.root).resolve()
    if args.command == 'plan':
        with phase_timing('discover-plan', 'plan'):
            make_plan(args, root)
    else:
        plan = load_plan(args, root)
        operation = {'run': run_shard, 'pack': pack, 'verify': verify}[args.command]
        if args.command == 'run':
            operation(args, root, plan)
        else:
            with phase_timing('pack-evidence' if args.command == 'pack' else 'admit-restore-evidence',
                              args.command, plan, args.shard if args.command == 'pack' else None):
                operation(args, root, plan)


if __name__ == '__main__':
    try:
        main()
    except (ValueError, KeyError, OSError, ET.ParseError, zipfile.BadZipFile, subprocess.CalledProcessError) as error:
        print('ci-shards: ' + str(error), file=sys.stderr)
        sys.exit(1)
