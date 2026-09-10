#!/usr/bin/env bash
# Cache only jars built from the probed upstream checkout and resolved PDK snapshot bytes.
# prepare follows current upstream Maven snapshots; build with -nsu in the same isolated repository.
# seal refuses probe/build drift. Exact-key caches still defer unversioned upstream changes and may
# be evicted independently of upstream changes; they are an optimization, not an upstream event log.
set -euo pipefail
python3 - "$(cd "$(dirname "$0")/../.." && pwd)" "$@" <<'PY'
import argparse
import copy
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import xml.etree.ElementTree as ET

ROOT = Path(sys.argv[1])
MODULES = 'mysql=connectors/mysql-connector,mongodb=connectors/mongodb-connector,postgres=connectors/postgres-connector,oracle=connectors/oracle-connector,sqlserver=connectors/mssql-connector'
KINDS = [module.split('=', 1)[0] for module in MODULES.split(',')]
MODULE_PATHS = ','.join(module.split('=', 1)[1] for module in MODULES.split(','))
STAMP = 'connector-cache-manifest.json'


def digest(path):
    return hashlib.file_digest(path.open('rb'), 'sha256').hexdigest()


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':')).encode()


def xml(path):
    root = ET.parse(path).getroot()
    for element in root.iter():
        element.tag = element.tag.split('}')[-1]
    return root


def fail(message):
    raise ValueError(message)


def fingerprints(repository, reactor=()):
    result = {}
    base = repository / 'io/tapdata'
    for directory in sorted(base.glob('*/*-SNAPSHOT')):
        artifact, version = directory.parent.name, directory.name
        if ['io.tapdata', artifact, version] in reactor:
            continue
        metadata = list(directory.glob('maven-metadata-*.xml'))
        metadata = [path for path in metadata if path.name != 'maven-metadata-local.xml']
        aliases = sorted(path for path in directory.glob(f'{artifact}-{version}*') if path.suffix in ('.jar', '.pom'))
        for alias in aliases:
            suffix = alias.name[len(f'{artifact}-{version}'):]
            extension = alias.suffix[1:]
            classifier = suffix[:-len(alias.suffix)].lstrip('-')
            choices = set()
            for path in metadata:
                document = xml(path)
                if (document.findtext('groupId'), document.findtext('artifactId'), document.findtext('version')) != ('io.tapdata', artifact, version):
                    fail(f'inconsistent snapshot metadata: {path}')
                for entry in document.findall('./versioning/snapshotVersions/snapshotVersion'):
                    if entry.findtext('extension') != extension or (entry.findtext('classifier') or '') != classifier:
                        continue
                    resolved = entry.findtext('value') or ''
                    if not re.fullmatch(re.escape(version[:-9]) + r'-\d{8}\.\d{6}-\d+', resolved):
                        fail(f'unresolved timestamp version in {path}: {resolved}')
                    timestamped = directory / f'{artifact}-{resolved}{suffix}'
                    if timestamped.is_file() and digest(timestamped) == digest(alias):
                        choices.add(resolved)
            if len(choices) != 1:
                fail(f'snapshot alias has no unique timestamp metadata and matching bytes: {alias}')
            relative = str(alias.relative_to(repository))
            result[relative] = {'version': choices.pop(), 'sha256': digest(alias)}
    if not result:
        fail('no resolved PDK snapshot artifacts found')
    return result


def run_maven(arguments):
    result = subprocess.run(arguments, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    sys.stderr.write(result.stdout)
    result.check_returncode()
    if re.search(r'Could not transfer metadata|metadata .*failed to transfer', result.stdout):
        fail('Maven could not refresh snapshot metadata; refusing a stale cache key')


def roots_from_effective(projects):
    if not projects:
        fail('effective POM contains no reactor projects')
    reactor = {(p.findtext('groupId'), p.findtext('artifactId'), p.findtext('version')) for p in projects}
    roots = set()
    for project in projects:
        for dependency in project.findall('./dependencies/dependency') + project.findall('./build/plugins/plugin/dependencies/dependency'):
            group, artifact, version = (dependency.findtext(field) or '' for field in ('groupId', 'artifactId', 'version'))
            if group != 'io.tapdata' or (group, artifact, version) in reactor:
                continue
            if '${' in version or not version:
                fail(f'unresolved external PDK dependency {artifact}: {version}')
            kind = dependency.findtext('type') or 'jar'
            classifier = dependency.findtext('classifier') or ''
            if kind not in ('jar', 'pom', 'test-jar'):
                fail(f'unsupported PDK snapshot artifact type: {kind}')
            if kind == 'test-jar':
                kind, classifier = 'jar', classifier or 'tests'
            roots.add((group, artifact, version, kind, classifier))
    if not roots:
        fail('selected reactor names no external PDK snapshot dependencies')
    return sorted(roots), sorted(reactor)


def key(manifest):
    return 'real-connectors-v2-' + hashlib.sha256(canonical(manifest)).hexdigest()


def load(path):
    data = json.loads(path.read_text())
    if data.get('schema') != 2 or not data.get('snapshots') or not data.get('roots') or not data.get('source_sha') or not data.get('inputs'):
        fail('incomplete connector cache manifest')
    if set(data['source_sha']) != {'oss', 'enterprise'} or any(not re.fullmatch('[0-9a-f]{40}', sha) for sha in data['source_sha'].values()):
        fail('connector cache manifest needs OSS and enterprise revisions')
    return data


def jars_at(directory):
    paths = sorted(directory.glob('*.jar'))
    for connector in KINDS:
        if len([p for p in paths if p.name.startswith(connector + '-connector-v')]) != 1:
            fail(f'expected exactly one shaded {connector} connector jar')
    if len(paths) != len(KINDS):
        fail('unexpected connector jar set')
    if any(not path.stat().st_size for path in paths):
        fail('empty connector jar')
    return {path.name: digest(path) for path in paths}


parser = argparse.ArgumentParser(description=__doc__)
sub = parser.add_subparsers(dest='command', required=True)
prepare = sub.add_parser('prepare')
prepare.add_argument('--checkout', type=Path, action='append', required=True)
prepare.add_argument('--repo-local', type=Path, required=True)
prepare.add_argument('--output', type=Path, required=True)
for name in ('key', 'seal', 'verify'):
    command = sub.add_parser(name)
    command.add_argument('--manifest', type=Path, required=True)
    if name != 'key': command.add_argument('--jars', type=Path, required=True)
    if name == 'seal':
        command.add_argument('--repo-local', type=Path, required=True)
        command.add_argument('--checkout', type=Path, action='append', required=True)
args = parser.parse_args(sys.argv[2:])
try:
    if args.command == 'prepare':
        if len(args.checkout) != 2:
            fail('prepare requires two --checkout arguments: OSS then enterprise')
        checkouts = dict(zip(('oss', 'enterprise'), (path.resolve() for path in args.checkout)))
        repository = args.repo_local.resolve()
        if repository == Path.home() / '.m2/repository':
            fail('prepare requires an isolated Maven repository')
        # Caller configuration must not silently change the inputs that the key claims to bind.
        if os.environ.get('MAVEN_ARGS'):
            fail('prepare requires MAVEN_ARGS unset; use --repo-local and the checked-in connector settings')
        source_sha = {}
        for label, checkout in checkouts.items():
            source_sha[label] = subprocess.check_output(['git', '-C', str(checkout), 'rev-parse', 'HEAD'], text=True).strip()
            if subprocess.check_output(['git', '-C', str(checkout), 'status', '--porcelain', '--untracked-files=no'], text=True).strip():
                fail(f'{label} connector checkout has tracked modifications')
        settings = ROOT / '.github/maven-settings-connectors.xml'
        common = ['mvn', '-B', '-U', f'-Dmaven.repo.local={repository}', '-s', str(settings)]
        with tempfile.TemporaryDirectory() as temporary:
            effective_projects = []
            selected = {label: [] for label in checkouts}
            for module in MODULE_PATHS.split(','):
                owners = [label for label, checkout in checkouts.items() if (checkout / module).is_dir()]
                if len(owners) != 1:
                    fail(f'module must belong to exactly one checkout: {module}')
                selected[owners[0]].append(module)
            # These local artifacts are installed before the enterprise build, not
            # resolved as remote snapshots. Include their full reactor in the graph.
            selected['oss'] += ['connectors-common/sql-core', 'connectors-common/read-partition']
            for label, checkout in checkouts.items():
                if not selected[label]:
                    continue
                effective = Path(temporary) / f'{label}-effective.xml'
                run_maven(common + ['-f', str(checkout / 'pom.xml'), '-pl', ','.join(selected[label]), '-am', 'org.apache.maven.plugins:maven-help-plugin:3.5.1:effective-pom', f'-Doutput={effective}'])
                document = xml(effective)
                effective_projects.extend([document] if document.tag == 'project' else document.findall('project'))
            roots, reactor = roots_from_effective(effective_projects)
            # Separate get executions preserve different versions of the same artifact across
            # modules. A single dependency list would mediate them down to one version. Keeping
            # executions in one Maven session avoids repeated JVM startup and metadata fetches.
            project = ET.Element('project')
            for name, value in [('modelVersion', '4.0.0'), ('groupId', 'io.tapstate'), ('artifactId', 'connector-cache-resolution'), ('version', '1'), ('packaging', 'pom')]:
                ET.SubElement(project, name).text = value
            repositories = ET.SubElement(project, 'repositories')
            seen_repositories = {}
            for repository_element in [element for entry in effective_projects for element in entry.findall('./repositories/repository')]:
                identifier = repository_element.findtext('id')
                normalized = copy.deepcopy(repository_element)
                for element in normalized.iter():
                    element.text = (element.text or '').strip()
                    element.tail = ''
                contents = ET.tostring(normalized)
                if identifier in seen_repositories and seen_repositories[identifier] != contents:
                    fail(f'inconsistent effective repository definitions: {identifier}')
                if identifier not in seen_repositories:
                    repositories.append(copy.deepcopy(repository_element))
                    seen_repositories[identifier] = contents
            plugin = ET.SubElement(ET.SubElement(ET.SubElement(project, 'build'), 'plugins'), 'plugin')
            for name, value in [('groupId', 'org.apache.maven.plugins'), ('artifactId', 'maven-dependency-plugin'), ('version', '3.8.1')]:
                ET.SubElement(plugin, name).text = value
            executions = ET.SubElement(plugin, 'executions')
            for index, (group, artifact, version, kind, classifier) in enumerate(roots):
                execution = ET.SubElement(executions, 'execution')
                ET.SubElement(execution, 'id').text = f'resolve-{index}'
                ET.SubElement(execution, 'phase').text = 'validate'
                ET.SubElement(ET.SubElement(execution, 'goals'), 'goal').text = 'get'
                configuration = ET.SubElement(execution, 'configuration')
                ET.SubElement(configuration, 'artifact').text = ':'.join([group, artifact, version, kind] + ([classifier] if classifier else []))
                ET.SubElement(configuration, 'transitive').text = 'true'
            resolver_pom = Path(temporary) / 'pom.xml'
            ET.ElementTree(project).write(resolver_pom, encoding='utf-8', xml_declaration=True)
            run_maven(common + ['-f', str(resolver_pom), '-N', 'validate'])
        snapshots = fingerprints(repository, [list(entry) for entry in reactor])
        for group, artifact, version, kind, classifier in roots:
            filename = f'{artifact}-{version}' + (f'-{classifier}' if classifier else '') + f'.{kind}'
            if version.endswith('-SNAPSHOT') and f'io/tapdata/{artifact}/{version}/{filename}' not in snapshots:
                fail(f'unresolved required PDK component: {artifact}:{version}:{kind}:{classifier}')
        inputs = {str(path.relative_to(ROOT)): digest(path) for path in [ROOT / 'scripts/build-real-connectors.sh', ROOT / '.github/scripts/connector-cache.sh', settings]}
        manifest = {'schema': 2, 'reactor': reactor, 'source_sha': source_sha, 'modules': MODULES, 'roots': roots, 'snapshots': snapshots, 'inputs': inputs}
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(manifest, indent=2, sort_keys=True) + '\n')
        print(key(manifest))
    elif args.command == 'key':
        print(key(load(args.manifest)))
    elif args.command == 'seal':
        manifest = load(args.manifest)
        if len(args.checkout) != 2:
            fail('seal requires two --checkout arguments: OSS then enterprise')
        for label, source in zip(('oss', 'enterprise'), args.checkout):
            checkout = source.resolve()
            if subprocess.check_output(['git', '-C', str(checkout), 'rev-parse', 'HEAD'], text=True).strip() != manifest['source_sha'][label] or subprocess.check_output(['git', '-C', str(checkout), 'status', '--porcelain', '--untracked-files=no'], text=True).strip():
                fail(f'{label} connector source changed between probe and build')
        if any(digest(ROOT / path) != expected for path, expected in manifest['inputs'].items()):
            fail('connector build inputs changed between probe and build')
        if fingerprints(args.repo_local.resolve(), manifest['reactor']) != manifest['snapshots']:
            fail('PDK snapshot provenance changed between probe and connector build')
        stamp = {'key': key(manifest), 'provenance': manifest, 'jars': jars_at(args.jars)}
        (args.jars / STAMP).write_text(json.dumps(stamp, indent=2, sort_keys=True) + '\n')
    else:
        manifest = load(args.manifest)
        stamp = json.loads((args.jars / STAMP).read_text())
        if stamp.get('key') != key(manifest) or stamp.get('provenance') != manifest or stamp.get('jars') != jars_at(args.jars):
            fail('cached connector jars do not match the requested provenance and file hashes')
        print('connector cache provenance and jar hashes verified')
except (ValueError, OSError, ET.ParseError, subprocess.CalledProcessError) as error:
    print(f'connector cache: {error}', file=sys.stderr)
    sys.exit(1)
PY
