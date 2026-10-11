#!/usr/bin/env python3
"""Archive original Actions evidence and record actual pre-test build inputs."""
import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import stat
import base64
from urllib.request import Request, urlopen
from urllib.parse import urlencode
import subprocess
import sys
import time
import zipfile

sys.dont_write_bytecode = True


def require(ok, message):
    if not ok:
        raise ValueError(message)


def digest(path):
    with path.open('rb') as stream:
        value = hashlib.file_digest(stream, 'sha256')
    return value.hexdigest()


def write(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + '\n')


def command(args, binary=False, cwd=None):
    result = subprocess.run(args, capture_output=True, text=not binary, cwd=cwd)
    require(result.returncode == 0, 'command failed: ' + ' '.join(args[:3]))
    return result.stdout


def api(endpoint):
    return json.loads(command(['gh', 'api', endpoint]))


def pages(endpoint, key, save, stem):
    result, seen, page = [], set(), 1
    while True:
        data = api(endpoint + ('&' if '?' in endpoint else '?') + f'per_page=100&page={page}')
        write(save / f'{stem}-{page}.json', data)
        rows, count = data.get(key), data.get('total_count')
        require(isinstance(rows, list) and isinstance(count, int), 'invalid paginated API: ' + key)
        for row in rows:
            require(row['id'] not in seen, 'duplicate API identity')
            seen.add(row['id'])
            result.append(row)
        if len(result) == count:
            return result
        require(rows and len(result) < count, 'incomplete API pagination: ' + key)
        page += 1


def safe_files(root):
    require(root.is_dir() and not root.is_symlink(), 'unsafe file root: ' + str(root))
    for path in sorted(root.rglob('*')):
        require(not path.is_symlink(), 'symlink evidence: ' + str(path))
        if path.is_file():
            yield path


def crc64(data):
    # JaCoCo's CRC64 class identity, including its early Java 9 compatibility adjustment.
    if len(data) > 7 and data[6:8] == b'\x00\x35':
        data = data[:7] + b'\x34' + data[8:]
    crc = 0
    for byte in data:
        crc = (crc >> 8) ^ CRC_TABLE[(crc ^ byte) & 0xff]
    return f'{crc:016x}'


def crc_table():
    table = []
    for value in range(256):
        for _ in range(8):
            value = (value >> 1) ^ (0xd800000000000000 if value & 1 else 0)
        table.append(value)
    return table


CRC_TABLE = crc_table()


def class_access_flags(data):
    require(data[:4] == b'\xca\xfe\xba\xbe' and len(data) >= 12, 'invalid class file')
    count = int.from_bytes(data[8:10], 'big')
    position, slot = 10, 1
    sizes = {3: 4, 4: 4, 5: 8, 6: 8, 7: 2, 8: 2, 9: 4, 10: 4,
             11: 4, 12: 4, 15: 3, 16: 2, 17: 4, 18: 4, 19: 2, 20: 2}
    while slot < count:
        require(position < len(data), 'truncated class constant pool')
        tag = data[position]
        position += 1
        if tag == 1:
            require(position + 2 <= len(data), 'truncated class string')
            size = 2 + int.from_bytes(data[position:position + 2], 'big')
        else:
            require(tag in sizes, 'unsupported class constant pool tag')
            size = sizes[tag]
        position += size
        require(position <= len(data), 'truncated class constant pool entry')
        slot += 2 if tag in (5, 6) else 1
    require(position + 2 <= len(data), 'missing class access flags')
    return int.from_bytes(data[position:position + 2], 'big')


def git(root, *args):
    return command(['git', '-C', str(root), *args]).strip()


def inputs(args):
    root = Path(args.root).resolve()
    require(Path(git(root, 'rev-parse', '--show-toplevel')).resolve() == root, 'input root must be the actual product checkout')
    require(not git(root, 'status', '--porcelain', '--untracked-files=no'), 'tracked source must be clean')
    untracked = git(root, 'ls-files', '--others', '--exclude-standard').splitlines()
    require(not any(name.endswith(('.java', '.py', '.sh', '.xml', '.yml', '.yaml', '.properties'))
                    or '/META-INF/services/' in name for name in untracked), 'untracked source cannot be attributed to the recorded commit')
    start = time.monotonic_ns()
    from importlib.util import spec_from_file_location, module_from_spec
    spec = spec_from_file_location('ci_shards', Path(__file__).with_name('_ci-shards.py'))
    shards = module_from_spec(spec)
    spec.loader.exec_module(shards)
    if args.resolve_classpath:
        command(['mvn', '-B', '-Dmaven.repo.local=' + str(Path(args.repo_local).resolve()),
                 'dependency:build-classpath', '-DincludeScope=test',
                 '-Dmdep.outputFile=target/ci-dependency-classpath.txt'], cwd=root)
    production, class_ids, test_files, test_ids, outputs, classpaths = {}, {}, {}, {}, {}, {}
    jar_classes, class_flags = {}, {}
    for module, _ in shards.reactor(root):
        target = shards.owned_path(root, (Path(module) / 'target').as_posix())
        classpath = target / 'ci-dependency-classpath.txt'
        if classpath.exists():
            resolved = []
            for name in classpath.read_text().strip().split(os.pathsep):
                if not name:
                    continue
                path = Path(name).resolve()
                require(path.is_file(), 'missing actual classpath file')
                repository = Path(args.repo_local).resolve()
                if path.is_relative_to(repository):
                    identity = 'repository/' + path.relative_to(repository).as_posix()
                elif path.is_relative_to(root):
                    identity = 'checkout/' + path.relative_to(root).as_posix()
                else:
                    raise ValueError('classpath outside the declared checkout/dependency repository')
                if identity not in jar_classes:
                    known = {}
                    if path.suffix == '.jar':
                        with zipfile.ZipFile(path) as archive:
                            for member in archive.infolist():
                                if member.filename.startswith('io/tapstate/') and member.filename.endswith('.class'):
                                    require(not member.is_dir() and '..' not in Path(member.filename).parts,
                                            'unsafe classpath archive member')
                                    name = member.filename.removesuffix('.class')
                                    require(name not in known, 'duplicate classpath class')
                                    bytecode = archive.read(member)
                                    known[name] = {'class_id': crc64(bytecode), 'sha256': hashlib.sha256(bytecode).hexdigest()}
                    jar_classes[identity] = known
                resolved.append({'identity': identity, 'sha256': digest(path), 'size': path.stat().st_size,
                                 'own_classes': jar_classes[identity]})
            classpaths[module] = resolved
        classes = target / 'classes'
        if classes.exists():
            for path in safe_files(classes):
                relative = path.relative_to(root).as_posix()
                production[relative] = digest(path)
                if path.suffix == '.class':
                    bytecode = path.read_bytes()
                    class_ids[relative] = crc64(bytecode)
                    class_flags[relative] = class_access_flags(bytecode)
        test_classes = target / 'test-classes'
        if test_classes.exists():
            for path in safe_files(test_classes):
                if path.suffix == '.class':
                    relative = path.relative_to(root).as_posix()
                    test_files[relative] = digest(path)
                    test_ids[relative] = crc64(path.read_bytes())
        if target.exists():
            for path in sorted(target.iterdir()):
                if path.is_file() and path.name.endswith(('.jar', '.tar.gz', '.zip', '.pom')):
                    require(not path.is_symlink(), 'symlink build output')
                    outputs[path.relative_to(root).as_posix()] = {'sha256': digest(path), 'size': path.stat().st_size,
                                                                'mode': stat.S_IMODE(path.stat().st_mode)}
    require(production and class_ids, 'no production class evidence')
    dependencies = {}
    for path in safe_files(Path(args.repo_local).resolve()):
        relative = path.relative_to(Path(args.repo_local).resolve()).as_posix()
        if not relative.startswith('io/tapstate/') and path.suffix in ('.jar', '.pom'):
            dependencies[relative] = digest(path)
    require(dependencies, 'no resolved dependency byte evidence')
    source_files = {}
    for name in git(root, 'ls-files').splitlines():
        if name.endswith(('.java', '.xml', '.yml', '.yaml', '.json', '.sh', '.py', '.properties')):
            source_files[name] = digest(shards.owned_path(root, name))
    java = subprocess.run(['java', '-version'], capture_output=True, text=True, cwd=root)
    require(java.returncode == 0 and java.stderr.strip(), 'Java version evidence unavailable')
    value = {'schema': 1, 'source_sha': git(root, 'rev-parse', 'HEAD'),
             'source_tree': git(root, 'rev-parse', 'HEAD^{tree}'), 'event': os.getenv('GITHUB_EVENT_NAME', 'local'),
             'run_id': os.getenv('GITHUB_RUN_ID', 'local'), 'run_attempt': os.getenv('GITHUB_RUN_ATTEMPT', '1'),
             'captured_at': datetime.now(timezone.utc).isoformat(), 'phase': args.phase,
             'source_files': source_files, 'production_files': production, 'production_class_ids': class_ids,
             'production_class_access_flags': class_flags, 'jacoco_version': shards.pom(root / 'pom.xml').findtext('properties/plugin.jacoco.version'),
             'test_files': test_files, 'test_class_ids': test_ids,
             'packaged_outputs': outputs, 'dependency_repository_files': dependencies,
             'dependency_scope': 'conservative available JAR/POM byte binding plus independently resolved ordered test classpaths',
             'resolved_test_classpaths': classpaths,
             'toolchain': {'java': java.stderr,
                           'maven': command(['mvn', '--version']), 'os': platform.platform(), 'architecture': platform.machine(),
                           'cpu_count': os.cpu_count()},
             'elapsed_seconds': (time.monotonic_ns() - start) / 1e9}
    if args.resolve_classpath:
        expected_modules = {t['module'] for t in shards.discover(root)}
        require(expected_modules <= set(classpaths), 'selected test module classpaths missing: ' + str(sorted(expected_modules - set(classpaths))))
    write(Path(args.output), value)
    print('CI_PHASE_JSON ' + json.dumps({'schema': 1, 'phase': 'capture-build-inputs', 'input_phase': args.phase, 'event': 'end',
                                         'elapsed_seconds': value['elapsed_seconds'], 'status': 'success'}))


def resources(args):
    value = {'schema': 1, 'run_id': os.getenv('GITHUB_RUN_ID', 'local'),
             'run_attempt': os.getenv('GITHUB_RUN_ATTEMPT', '1'),
             'source_sha': git(Path(args.root).resolve(), 'rev-parse', 'HEAD'),
             'captured_at': datetime.now(timezone.utc).isoformat(), 'docker_images': [], 'quality_gate': None,
             'unknown': []}
    if args.docker:
        # This is inspection only: it never starts a daemon or container.
        try:
            command(['docker', 'info', '--format', '{{.ServerVersion}}'])
            ids = command(['docker', 'image', 'ls', '--quiet', '--no-trunc']).splitlines()
            if ids:
                raw = json.loads(command(['docker', 'image', 'inspect', *sorted(set(ids))]))
                value['docker_images'] = [{'id': i['Id'], 'repo_digests': i.get('RepoDigests', []),
                                            'repo_tags': i.get('RepoTags', []), 'architecture': i.get('Architecture'),
                                            'os': i.get('Os')} for i in raw]
        except (ValueError, OSError):
            value['unknown'].append('Docker image evidence unavailable; daemon was not started')
    if args.sonar:
        host, token = os.getenv('SONAR_HOST_URL', ''), os.getenv('SONAR_TOKEN', '')
        if not host or not token:
            value['unknown'].append('Sonar gate not evaluated: repository secrets unavailable')
        else:
            def sonar(endpoint, query):
                request = Request(host.rstrip('/') + '/api/' + endpoint + '?' + urlencode(query))
                auth = base64.b64encode((token + ':').encode()).decode()
                request.add_header('Authorization', 'Basic ' + auth)
                with urlopen(request, timeout=30) as response:
                    return json.load(response)
            try:
                assigned = sonar('qualitygates/get_by_project', {'project': 'tapstate'})
                gate = assigned.get('qualityGate', {})
                require(gate.get('name'), 'quality gate assignment unavailable')
                conditions = sonar('qualitygates/show', {'name': gate['name']})
                value['quality_gate'] = {'assignment': assigned, 'configuration': conditions}
            except Exception:
                # Error bodies can contain server addresses or credentials. Record only absence.
                value['unknown'].append('Sonar gate configuration API unavailable')
    write(Path(args.output), value)


def zip_members(path):
    with zipfile.ZipFile(path) as archive:
        names = set()
        for item in archive.infolist():
            name = item.filename
            p = Path(name)
            require(name and not p.is_absolute() and '..' not in p.parts and '\\' not in name,
                    'unsafe ZIP member: ' + name)
            require(name not in names, 'duplicate ZIP member: ' + name)
            require(not stat.S_ISLNK(item.external_attr >> 16), 'symlink ZIP member: ' + name)
            names.add(name)
            if not item.is_dir():
                with archive.open(item) as stream:
                    while stream.read(1024 * 1024):
                        pass
        return sorted(names)


def manifest(directory, metadata):
    files = {p.relative_to(directory).as_posix(): {'sha256': digest(p), 'size': p.stat().st_size}
             for p in safe_files(directory) if p.relative_to(directory).as_posix() != 'receipt.json'}
    write(directory / 'receipt.json', {**metadata, 'files': files})


def verify(args):
    directory = Path(args.directory).resolve()
    receipt = json.loads((directory / 'receipt.json').read_text())
    actual = {p.relative_to(directory).as_posix(): {'sha256': digest(p), 'size': p.stat().st_size}
              for p in safe_files(directory) if p.relative_to(directory).as_posix() != 'receipt.json'}
    require(actual == receipt['files'], 'archive file set/size/SHA-256 differs')
    for path in directory.rglob('*.zip'):
        zip_members(path)
    require(receipt.get('archive_complete') is True, 'receipt is explicitly incomplete: ' + str(receipt.get('gaps')))
    print('verified durable receipt: ' + str(directory))


def collect(args):
    require(re.fullmatch(r'[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+', args.repo), 'invalid repository')
    prefix = 'repos/' + args.repo
    run = api(f'{prefix}/actions/runs/{args.run}')
    require(run['id'] == args.run and run['repository']['full_name'] == args.repo, 'wrong run/repository')
    archive_root = Path(args.archive_root).resolve()
    require(not str(archive_root).startswith(('/tmp/', '/private/tmp/')), 'archive must be persistent')
    directory = archive_root / f'run-{args.run}' / f'attempt-{run["run_attempt"]}'
    require(not directory.exists(), 'archive exists; verify or choose a new archive root, never overwrite')
    require(run['status'] == 'completed', 'run is not complete; collect after completion')
    directory.mkdir(parents=True)
    write(directory / 'run.json', run)
    gaps, jobs_all = [], []
    external_start = time.monotonic_ns()
    for attempt in range(1, run['run_attempt'] + 1):
        target = directory / f'attempt-{attempt}'
        target.mkdir()
        attempt_run = api(f'{prefix}/actions/runs/{args.run}/attempts/{attempt}')
        write(target / 'run.json', attempt_run)
        jobs = pages(f'{prefix}/actions/runs/{args.run}/attempts/{attempt}/jobs', 'jobs', target, 'jobs')
        require(all(j.get('run_id') == args.run and j.get('run_attempt') == attempt for j in jobs),
                'jobs do not belong to this run and attempt')
        jobs_all.extend(jobs)
        try:
            logs = command(['gh', 'api', f'{prefix}/actions/runs/{args.run}/attempts/{attempt}/logs'], binary=True)
            (target / 'logs.zip').write_bytes(logs)
            zip_members(target / 'logs.zip')
        except (ValueError, zipfile.BadZipFile):
            gaps.append(f'raw logs missing or invalid for attempt {attempt}')
    artifacts = pages(f'{prefix}/actions/runs/{args.run}/artifacts', 'artifacts', directory, 'artifacts')
    pages(f'{prefix}/commits/{run["head_sha"]}/check-runs', 'check_runs', directory, 'check-runs')
    write(directory / 'commit.json', api(f'{prefix}/commits/{run["head_sha"]}'))
    source = Path(args.source_root).resolve()
    require(git(source, 'rev-parse', run['head_sha']) == run['head_sha'], 'run source unavailable locally')
    command(['git', '-C', str(source), 'archive', '--format=tar', run['head_sha'], '-o', str(directory / 'source.tar')])
    for ruleset in api(f'{prefix}/rulesets'):
        write(directory / f'ruleset-{ruleset["id"]}.json', api(f'{prefix}/rulesets/{ruleset["id"]}'))
    names, member_index = [], {}
    for artifact in artifacts:
        identity = str(artifact['id'])
        require(re.fullmatch(r'[A-Za-z0-9_.-]+', artifact['name']), 'unsafe artifact name')
        names.append(artifact['name'])
        if artifact['expired']:
            gaps.append('expired artifact: ' + artifact['name'])
            continue
        path = directory / 'artifacts' / (identity + '-' + artifact['name'] + '.zip')
        path.parent.mkdir(exist_ok=True)
        try:
            path.write_bytes(command(['gh', 'api', f'{prefix}/actions/artifacts/{identity}/zip'], binary=True))
            member_index[path.name] = zip_members(path)
        except (ValueError, zipfile.BadZipFile):
            gaps.append('artifact unavailable or invalid: ' + artifact['name'])
    write(directory / 'artifact-members.json', member_index)
    shard_ids = []
    for path in (directory / 'artifacts').glob('*-ci-shard-plan.zip'):
        with zipfile.ZipFile(path) as archive:
            if 'shards.json' in archive.namelist():
                plan = json.loads(archive.read('shards.json'))
                require(plan['source_sha'] == run['head_sha'], 'plan belongs to another source')
                shard_ids = [s['id'] for s in plan['shards']]
    if not shard_ids:
        gaps.append('source-bound shard plan unavailable')
    expected = ['ci-shard-plan', 'ci-aggregate'] + ['ci-test-results-' + n for n in shard_ids]
    for name in expected:
        if name not in names:
            gaps.append('missing original CI artifact: ' + name)
    if not all('ci-inputs-' + n in names for n in [*shard_ids, 'build']):
        gaps.append('actual dependency/production/toolchain manifests unavailable')
    if not any('witness-ledger' in member for members in member_index.values() for member in members):
        gaps.append('raw witness ledgers unavailable')
    timed_jobs = [job for job in jobs_all if job.get('completed_at') and job.get('started_at') and job.get('conclusion') != 'skipped']
    parse_time = lambda value: datetime.fromisoformat(value.replace('Z', '+00:00'))
    wall = (max(parse_time(j['completed_at']) for j in timed_jobs) - parse_time(run['created_at'])).total_seconds() if timed_jobs else None
    runner_seconds = sum((parse_time(j['completed_at']) - parse_time(j['started_at'])).total_seconds() for j in timed_jobs)
    unknown = ['resolved runner image digest unless present in logs', 'cache condition not independently certified',
               'PR-required short checks are not triggered by this manual CI event' if run['event'] == 'workflow_dispatch' else 'PR event input requires separate reconciliation']
    metadata = {'schema': 1, 'repository': args.repo, 'run_id': args.run, 'run_attempt': run['run_attempt'],
                'event': run['event'], 'source_sha': run['head_sha'], 'conclusion': run['conclusion'],
                'source_workflow': run['path'], 'archive_root': str(directory),
                'collected_at': datetime.now(timezone.utc).isoformat(), 'archive_complete': not gaps,
                'equivalence_proven': False, 'gaps': gaps, 'unknown': unknown,
                'workflow_wall_seconds': wall, 'runner_seconds_all_attempts': runner_seconds,
                'first_attempt': run['run_attempt'] == 1, 'cache_condition_claim': args.cache_condition, 'cache_condition_verified': False,
                'external_collection_seconds': (time.monotonic_ns() - external_start) / 1e9,
                'cost_scope': 'all jobs and attempts; includes build/transfer/admission/Sonar; external collection separately measured'}
    manifest(directory, metadata)
    print(json.dumps({'archive': str(directory), 'archive_complete': not gaps, 'equivalence_proven': False,
                      'workflow_wall_seconds': wall, 'gaps': gaps}, sort_keys=True))
    return 0 if not gaps else 2


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='command', required=True)
    p = commands.add_parser('inputs')
    p.add_argument('--root', default='.')
    p.add_argument('--repo-local', required=True)
    p.add_argument('--output', required=True)
    p.add_argument('--resolve-classpath', action='store_true')
    p.add_argument('--phase', choices=['pre-test-after-install', 'post-test-before-pack'], default='pre-test-after-install')
    p = commands.add_parser('resources')
    p.add_argument('--root', default='.')
    p.add_argument('--output', required=True)
    p.add_argument('--docker', action='store_true')
    p.add_argument('--sonar', action='store_true')
    p = commands.add_parser('collect')
    p.add_argument('--repo', required=True)
    p.add_argument('--run', type=int, required=True)
    p.add_argument('--archive-root', required=True)
    p.add_argument('--source-root', required=True)
    p.add_argument('--cache-condition', choices=['warm', 'cold', 'unknown'], default='unknown')
    p = commands.add_parser('verify')
    p.add_argument('--directory', required=True)
    args = parser.parse_args()
    if args.command == 'inputs':
        inputs(args)
    elif args.command == 'resources':
        resources(args)
    elif args.command == 'collect':
        return collect(args)
    else:
        verify(args)
    return 0


if __name__ == '__main__':
    try:
        sys.exit(main())
    except (ValueError, OSError, KeyError, json.JSONDecodeError, zipfile.BadZipFile) as error:
        print('ci-receipt: ' + str(error), file=sys.stderr)
        sys.exit(1)
