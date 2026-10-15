#!/usr/bin/env python3
"""Archive original Actions evidence and record actual pre-test build inputs."""
import argparse
from datetime import datetime, timezone
import hashlib
import importlib.util
import json
import os
from pathlib import Path, PurePosixPath
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
import xml.etree.ElementTree as ET

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


def dependency_models(repository, hashes):
    """Record literal coordinate/type facts; never evaluate POM expressions or URLs."""
    models, producers = {}, {}
    literal = lambda value: isinstance(value, str) and bool(re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.+-]*', value))
    for relative, checksum in hashes.items():
        if not relative.endswith('.pom'):
            continue
        path = repository / relative
        try:
            require(path.stat().st_size <= 2 * 1024 * 1024, 'POM exceeds metadata bound')
            raw = path.read_bytes()
            require(hashlib.sha256(raw).hexdigest() == checksum, 'POM bytes changed during metadata capture')
            require(b'<!DOCTYPE' not in raw and b'<!ENTITY' not in raw, 'POM entities are not metadata')
            model = ET.fromstring(raw)
            for node in model.iter():
                node.tag = node.tag.rsplit('}', 1)[-1]
            require(model.tag == 'project', 'not a project model')
            parent = model.find('parent')
            parent_gav = ([parent.findtext(field) for field in ('groupId', 'artifactId', 'version')]
                          if parent is not None else None)
            gav = [model.findtext('groupId') or (parent_gav[0] if parent_gav else None),
                   model.findtext('artifactId'),
                   model.findtext('version') or (parent_gav[2] if parent_gav else None)]
            require(all(literal(value) for value in gav), 'nonliteral model coordinates')
            expected = '/'.join([gav[0].replace('.', '/'), gav[1], gav[2], gav[1] + '-' + gav[2] + '.pom'])
            require(expected == relative, 'model coordinates differ from repository path')
            packaging = model.findtext('packaging') or 'jar'
            require(literal(packaging), 'nonliteral model type')
            parent_fact = None
            if parent_gav:
                require(all(literal(value) for value in parent_gav), 'nonliteral parent coordinates')
                parent_path = '/'.join([parent_gav[0].replace('.', '/'), parent_gav[1], parent_gav[2], parent_gav[1] + '-' + parent_gav[2] + '.pom'])
                require(parent_path in hashes, 'parent bytes are outside the repository snapshot')
                parent_fact = dict(gav=parent_gav, path=parent_path, sha256=hashes[parent_path])
            models[relative] = dict(gav=gav, packaging=packaging, sha256=checksum, parent=parent_fact,
                                    coordinate_type_verified=True, effective_dependency_model_proven=False)
        except (ValueError, OSError, ET.ParseError):
            models[relative] = dict(sha256=checksum, coordinate_type_verified=False,
                                    effective_dependency_model_proven=False)
    for relative, checksum in hashes.items():
        if not relative.endswith('.jar'):
            continue
        pom_path = relative[:-4] + '.pom'
        model = models.get(pom_path)
        if not model or not model.get('coordinate_type_verified'):
            continue
        gav = model['gav']
        fact = dict(gav=gav, sha256=checksum, pom_path=pom_path, pom_sha256=hashes[pom_path],
                    archive_type_verified=False, producer_identity_verified=False,
                    producer_identity_source=None, pom_properties_present=False,
                    runtime_realm_proven=False)
        try:
            require(digest(repository / relative) == checksum, 'JAR bytes changed during metadata capture')
            with zipfile.ZipFile(repository / relative) as archive:
                fact['archive_type_verified'] = True
                fact['own_class_count'] = sum(item.filename.endswith('.class') and (item.filename.startswith('io/tapstate/') or re.match(r'META-INF/versions/[0-9]+/io/tapstate/', item.filename) is not None) for item in archive.infolist())
                for item in archive.infolist():
                    if not item.filename.startswith('META-INF/maven/') or not item.filename.endswith('/pom.properties'):
                        continue
                    require(item.file_size <= 65536, 'embedded coordinate facts exceed bound')
                    fact['pom_properties_present'] = True
                    properties = dict(match.groups() for line in archive.read(item).decode('utf-8').splitlines()
                                      if (match := re.fullmatch(r'(groupId|artifactId|version)\s*=\s*(.*)', line)))
                    if [properties.get(field) for field in ('groupId', 'artifactId', 'version')] == gav:
                        fact.update(producer_identity_verified=True, producer_identity_source='embedded-pom-properties')
                if not fact['producer_identity_verified'] and 'META-INF/MANIFEST.MF' in archive.namelist():
                    info = archive.getinfo('META-INF/MANIFEST.MF')
                    require(info.file_size <= 65536, 'producer manifest exceeds bound')
                    manifest = archive.read(info).decode('utf-8').replace('\r\n', '\n')
                    manifest = re.sub(r'\n ', '', manifest)
                    attributes = dict(line.split(': ', 1) for line in manifest.splitlines() if ': ' in line)
                    if attributes.get('Implementation-Title') == gav[1] and attributes.get('Implementation-Version') == gav[2]:
                        fact.update(producer_identity_verified=True, producer_identity_source='embedded-implementation-manifest')
                    elif attributes.get('Bundle-SymbolicName') == gav[0] + '.' + gav[1] and attributes.get('Bundle-Version') == gav[2]:
                        fact.update(producer_identity_verified=True, producer_identity_source='embedded-osgi-manifest')
        except (ValueError, OSError, UnicodeError, zipfile.BadZipFile):
            fact.update(archive_type_verified=False, producer_identity_verified=False, producer_identity_source=None)
        producers[relative] = fact
    return models, producers


def class_structure(data):
    """Read only JVM Class constants and the class header; never emit other UTF8 constants."""
    require(data[:4] == b'\xca\xfe\xba\xbe' and 12 <= len(data) <= 32 * 1024 * 1024, 'invalid runtime class bytes')
    count = int.from_bytes(data[8:10], 'big')
    position, slot, strings, classes = 10, 1, {}, {}
    sizes = {3: 4, 4: 4, 5: 8, 6: 8, 8: 2, 9: 4, 10: 4, 11: 4, 12: 4, 15: 3, 16: 2, 17: 4, 18: 4, 19: 2, 20: 2}
    while slot < count:
        require(position < len(data), 'truncated runtime constant pool')
        tag = data[position]; position += 1
        if tag == 1:
            require(position + 2 <= len(data), 'truncated runtime class string')
            length = int.from_bytes(data[position:position+2], 'big'); position += 2
            strings[slot] = data[position:position+length]; position += length
        elif tag == 7:
            require(position+2 <= len(data), 'truncated runtime Class constant')
            classes[slot] = int.from_bytes(data[position:position+2], 'big'); position += 2
        else:
            require(tag in sizes, 'unsupported runtime constant pool tag')
            position += sizes[tag]
        require(position <= len(data), 'truncated runtime constant pool value')
        slot += 2 if tag in (5, 6) else 1
    def name(index):
        if not index: return None
        require(index in classes and classes[index] in strings, 'invalid runtime class reference')
        value = strings[classes[index]].decode('utf-8')
        internal = r'(?:[\w$/]+|(?:[\w$/]+/)?(?:package-info|module-info))'
        require(re.fullmatch(r'(?:\[+(?:[ZBCSIJFD]|L' + internal + r';)|' + internal + r')', value), 'unsafe runtime class name')
        return value
    require(position+8 <= len(data), 'truncated runtime class header')
    flags, this, parent, interfaces = (int.from_bytes(data[position+i:position+i+2], 'big') for i in [0,2,4,6])
    require(position+8+interfaces*2 <= len(data), 'truncated runtime interface list')
    return dict(name=name(this), access_flags=flags, super_class=name(parent),
                interfaces=[name(int.from_bytes(data[position+8+i*2:position+10+i*2], 'big')) for i in range(interfaces)],
                class_references=sorted({name(index) for index in classes}))


def runtime_class_metadata(args, value):
    if not args.runtime_class_dir:
        return dict(enabled=False), []
    require(args.phase == 'post-test-before-pack' and os.environ.get('RUNNER_TEMP'), 'runtime metadata requires a post-test worker capture')
    directory = Path(args.runtime_class_dir)
    worker = Path(os.environ['RUNNER_TEMP']).resolve()
    require(directory.is_absolute() and directory.name == 'ci-runtime-bytecode' and '..' not in directory.parts
            and directory.absolute() == Path(os.environ['RUNNER_TEMP']).absolute() / 'ci-runtime-bytecode'
            and Path(args.output).resolve().parent.parent == worker, 'runtime bytecode must stay outside input artifacts in the worker sibling')
    require(directory.is_dir() and not directory.is_symlink() and not directory.resolve().is_relative_to(Path(args.root).resolve()), 'unsafe runtime dump root')
    origin_path = directory / 'capture-origin.json'
    require(origin_path.is_file() and not origin_path.is_symlink(), 'missing runtime dump origin')
    origin = json.loads(origin_path.read_text())
    require(isinstance(origin, dict), 'invalid runtime origin object')
    execution = origin.get('execution')
    require(execution in ['github-actions','local'], 'invalid runtime execution origin')
    require(origin.get('run_attempt') is None or type(origin['run_attempt']) is int, 'invalid runtime attempt type')
    run_id = origin.get('run_id') if execution == 'github-actions' else 'local'
    attempt = origin.get('run_attempt') if execution == 'github-actions' else 1
    require(type(origin.get('schema')) is int and origin['schema'] == 1 and origin.get('source_sha') == value['source_sha']
            and str(run_id) == str(value['run_id']) and str(attempt) == str(value['run_attempt'])
            and origin.get('status') == 'success' and re.fullmatch(r'[0-9a-f]{32}', origin.get('invocation_id', ''))
            and isinstance(origin.get('shard_id'), str) and origin['shard_id'], 'runtime dump source/run/attempt/verify differs')
    require(origin.get('runtime_class_capture') == dict(enabled=True, jacoco_version='0.8.13', raw_scope='worker-private-input-sibling')
            and value['jacoco_version'] == '0.8.13', 'runtime dump must use the existing pinned JaCoCo agent')
    verify = origin.get('verify', {})
    reset = origin.get('reset', {})
    for phase, record in [('reset-test-evidence', reset), ('selected-verify', verify)]:
        require(isinstance(record, dict) and type(record.get('schema')) is int and record['schema']==1
                and record.get('command')=='run' and record.get('phase')==phase and record.get('event')=='end'
                and record.get('status')=='success' and record.get('execution')==execution
                and record.get('cohort_hash')==origin.get('cohort_hash') and record.get('source_sha')==value['source_sha']
                and record.get('shard_id')==origin['shard_id'] and record.get('run_id')==origin.get('run_id')
                and record.get('run_attempt')==origin.get('run_attempt')
                and (record.get('run_attempt') is None or type(record['run_attempt']) is int), 'runtime dump verification provenance differs')
        start, end, elapsed = [record.get(key) for key in ['started_monotonic_ns','monotonic_ns','elapsed_ns']]
        require(all(type(number) is int and number>=0 for number in [start,end,elapsed])
                and end>=start and elapsed==end-start, 'runtime dump interval differs')
        require(isinstance(record.get('utc'), str) and isinstance(record.get('ended_utc'), str), 'runtime dump UTC origin missing')
        started_utc=datetime.fromisoformat(record['utc'].replace('Z','+00:00'))
        ended_utc=datetime.fromisoformat(record['ended_utc'].replace('Z','+00:00'))
        require(started_utc.tzinfo is not None and ended_utc.tzinfo is not None and ended_utc>=started_utc, 'runtime dump UTC interval differs')
    require(verify['started_monotonic_ns']>=reset['monotonic_ns']
            and datetime.fromisoformat(verify['utc'].replace('Z','+00:00'))>=datetime.fromisoformat(reset['ended_utc'].replace('Z','+00:00')), 'runtime verify precedes fresh reset')
    require(isinstance(origin.get('cohort_hash'), str) and re.fullmatch(r'[0-9a-f]{64}',origin['cohort_hash']), 'runtime cohort binding missing')
    before = Path(args.output).resolve().parent / 'build-inputs.json'
    build_hash = origin.get('case_observer', {}).get('build_inputs_sha256')
    require(before.is_file() and not before.is_symlink() and isinstance(build_hash,str)
            and re.fullmatch(r'[0-9a-f]{64}',build_hash) and digest(before)==build_hash, 'runtime pre-test input byte binding differs')
    known = {}
    for ids, hashes, segment, label in [('production_class_ids','production_files','/target/classes/','production'),
                                        ('test_class_ids','test_files','/target/test-classes/','test')]:
        for relative, identity in value[ids].items():
            classname = relative.split(segment,1)[1].removesuffix('.class')
            known.setdefault(classname, []).append(dict(name=classname,class_id=identity,sha256=value[hashes][relative],origin=label))
    entries = {}
    for classpath in value['resolved_test_classpaths'].values():
        for entry in classpath:
            entries[entry['identity']] = entry
            for name, record in entry['own_classes'].items():
                known.setdefault(name, []).append(dict(name=name,class_id=record['class_id'],sha256=record['sha256'],origin='resolved-classpath'))
    candidates = []
    dump_count, dump_bytes, own_count, known_count = 0, 0, 0, 0
    for path in safe_files(directory):
        if path == origin_path: continue
        require(path.suffix == '.class', 'unexpected private runtime dump file')
        dump_count += 1; dump_bytes += path.stat().st_size
        relative = path.relative_to(directory).as_posix()
        if not relative.startswith('io/tapstate/'): continue
        own_count += 1
        data = path.read_bytes(); structure = class_structure(data); identity = crc64(data); checksum = hashlib.sha256(data).hexdigest()
        require(relative == structure['name'] + '.' + identity + '.class', 'runtime dump filename/name/CRC differs')
        if structure['name'] in known:
            require(any(record['class_id']==identity and record['sha256']==checksum for record in known[structure['name']]),
                    'runtime dump differs from compiled class with the same name')
            known_count += 1
            continue
        references = structure['class_references']
        normalized_references = {ref.lstrip('[').removeprefix('L').removesuffix(';') if ref.startswith('[') else ref for ref in references}
        compiled = {json.dumps(record,sort_keys=True):record for ref in normalized_references for record in known.get(ref,[])}
        framework = {'spring_cglib': [], 'mockito': [], 'bytebuddy': []}
        for ref in references:
            normalized = ref.lstrip('[').removeprefix('L').removesuffix(';') if ref.startswith('[') else ref
            for family,prefix in [('spring_cglib','org/springframework/cglib/'),('mockito','org/mockito/'),('bytebuddy','net/bytebuddy/')]:
                if normalized.startswith(prefix): framework[family].append(normalized)
        framework = {family: sorted(set(values)) for family,values in framework.items()}
        candidates.append(dict(**structure,class_id=identity,sha256=checksum,compiled_references=sorted(compiled.values(),key=lambda r:(r['name'],r['class_id'],r['origin'])),framework_references=framework,framework_origins=[]))
    # Pin framework structure references only to actual resolved, hash-bound JARs.
    jar_cache = {}
    for candidate in candidates:
        wanted = {ref for references in candidate['framework_references'].values() for ref in references}
        for identity, entry in entries.items():
            if not identity.endswith('.jar'): continue
            scope,_,relative = identity.partition('/')
            model = value['dependency_coordinate_models'].get(relative[:-4]+'.pom', {}) if scope=='repository' else {}
            if not model.get('coordinate_type_verified') or not model.get('gav') or not model['gav'][0].startswith(('org.springframework','org.mockito','net.bytebuddy')):
                continue
            path = Path(args.repo_local).resolve()/relative if scope=='repository' else Path(args.root).resolve()/relative
            if identity not in jar_cache:
                require(digest(path)==entry['sha256'], 'runtime framework dependency bytes differ from captured input')
                with zipfile.ZipFile(path) as archive: jar_cache[identity] = set(archive.namelist())
            matching = sorted(ref for ref in wanted if ref+'.class' in jar_cache[identity])
            if matching:
                require(model.get('coordinate_type_verified'), 'runtime framework coordinates are unproven')
                candidate['framework_origins'].append(dict(gav=model['gav'],sha256=entry['sha256'],class_references=matching))
        captured = {ref for source in candidate['framework_origins'] for ref in source['class_references']}
        candidate['unresolved_framework_references'] = sorted(wanted-captured)
        candidate['ambiguous_framework_references'] = sorted(ref for ref in wanted
            if len({source['sha256'] for source in candidate['framework_origins'] if ref in source['class_references']})>1)
    ids_by_name = {}
    for candidate in candidates: ids_by_name.setdefault(candidate['name'], []).append(candidate['class_id'])
    for candidate in candidates:
        normalized = {ref.lstrip('[').removeprefix('L').removesuffix(';') if ref.startswith('[') else ref for ref in candidate['class_references']}
        candidate['runtime_references'] = [dict(name=ref,class_ids=sorted(set(ids_by_name[ref]))) for ref in sorted(normalized) if ref in ids_by_name and ref!=candidate['name']]
    agent = 'org/jacoco/org.jacoco.agent/0.8.13/org.jacoco.agent-0.8.13-runtime.jar'
    require(agent in value['dependency_repository_files'] and digest(Path(args.repo_local).resolve()/agent)==value['dependency_repository_files'][agent], 'runtime JaCoCo agent byte binding missing or changed')
    capture = dict(enabled=True,jacoco_version='0.8.13',agent_sha256=value['dependency_repository_files'][agent],
                   source_sha=origin['source_sha'],run_id=str(run_id),run_attempt=int(attempt),shard_id=origin['shard_id'],
                   invocation_id=origin['invocation_id'],status='success',reset=reset,verify=verify,cohort_hash=origin['cohort_hash'],
                   build_inputs_sha256=build_hash,capture_origin_sha256=digest(origin_path),raw_bytes_uploaded=False,
                   dump_class_count=dump_count,dump_byte_count=dump_bytes,own_dump_class_count=own_count,known_dump_class_count=known_count,candidate_class_count=len(candidates))
    return capture, sorted(candidates,key=lambda r:(r['name'],r['class_id']))


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
    production, class_ids, test_files, test_ids, outputs, classpaths, test_resources = {}, {}, {}, {}, {}, {}, {}
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
                else:
                    test_resources[path.relative_to(root).as_posix()] = digest(path)
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
    models, producers = dependency_models(Path(args.repo_local).resolve(), dependencies)
    source_files = {}
    for name in git(root, 'ls-files').splitlines():
        source_files[name] = digest(shards.owned_path(root, name))
    java = subprocess.run(['java', '-version'], capture_output=True, text=True, cwd=root)
    require(java.returncode == 0 and java.stderr.strip(), 'Java version evidence unavailable')
    value = {'schema': 1, 'source_sha': git(root, 'rev-parse', 'HEAD'),
             'source_tree': git(root, 'rev-parse', 'HEAD^{tree}'), 'event': os.getenv('GITHUB_EVENT_NAME', 'local'),
             'run_id': os.getenv('GITHUB_RUN_ID', 'local'), 'run_attempt': os.getenv('GITHUB_RUN_ATTEMPT', '1'),
             'captured_at': datetime.now(timezone.utc).isoformat(), 'phase': args.phase,
             'runner_context': input_runner_snapshot(os.environ, getattr(args, 'shard_id', None)),
             'source_files': source_files, 'production_files': production, 'production_class_ids': class_ids,
             'production_class_access_flags': class_flags, 'jacoco_version': shards.pom(root / 'pom.xml').findtext('properties/plugin.jacoco.version'),
             'test_files': test_files, 'test_class_ids': test_ids, 'test_resource_files': test_resources,
             'packaged_outputs': outputs, 'dependency_repository_files': dependencies,
             'dependency_coordinate_models': models, 'dependency_producer_facts': producers,
             'dependency_scope': 'conservative available JAR/POM byte binding plus independently resolved ordered test classpaths',
             'resolved_test_classpaths': classpaths,
             'toolchain': {'java': java.stderr,
                           'maven': command(['mvn', '--version']), 'os': platform.platform(), 'architecture': platform.machine(),
                           'cpu_count': os.cpu_count()},
             'elapsed_seconds': None}
    observation = getattr(args, 'maven_observation', None)
    if observation:
        path = Path(observation)
        require(path.is_file() and not path.is_symlink() and path.stat().st_size <= 64 * 1024 * 1024,
                'unsafe Maven observation summary')
        def unique_observation_object(pairs):
            result = {}
            for key, field in pairs:
                require(key not in result, 'duplicate Maven observation key')
                result[key] = field
            return result
        value['maven_execution_observation'] = json.loads(path.read_text(), object_pairs_hook=unique_observation_object)
        maven_observation_fact(value, getattr(args, 'shard_id', None))
    value['runtime_class_capture'], value['runtime_generated_classes'] = runtime_class_metadata(args, value)
    if args.resolve_classpath:
        expected_modules = {t['module'] for t in shards.discover(root)}
        require(expected_modules <= set(classpaths), 'selected test module classpaths missing: ' + str(sorted(expected_modules - set(classpaths))))
    value['elapsed_seconds'] = (time.monotonic_ns() - start) / 1e9
    write(Path(args.output), value)
    print('CI_PHASE_JSON ' + json.dumps({'schema': 1, 'phase': 'capture-build-inputs', 'input_phase': args.phase, 'event': 'end',
                                         'elapsed_seconds': value['elapsed_seconds'], 'status': 'success'}))



# Compare declared bytes; generator, processor and effective-realm completeness remain separate.
BUILD_INPUT_FIELDS = (
    'source_tree', 'source_files', 'production_files', 'production_class_ids',
    'production_class_access_flags', 'test_files', 'test_class_ids',
    'packaged_outputs', 'resolved_test_classpaths', 'jacoco_version', 'toolchain',
)


def input_runner_snapshot(environment, shard):
    repository = environment.get('GITHUB_REPOSITORY')
    if repository is not None:
        require(isinstance(repository, str) and re.fullmatch(r'[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+', repository),
                'invalid input runner repository')
    if shard is not None:
        require(isinstance(shard, str) and re.fullmatch(r'[a-z0-9]+(?:-[a-z0-9]+)*', shard), 'invalid input runner shard')
    return {'repository': repository, 'shard_id': shard,
            'workflow_source': workflow_source_snapshot(environment, repository or '')}


def input_runner_context(value, name, repository=None):
    if 'runner_context' not in value:
        return None
    context = value['runner_context']
    require(isinstance(context, dict) and set(context) == {'repository', 'shard_id', 'workflow_source'},
            'invalid input runner context')
    repo, shard = context['repository'], context['shard_id']
    require(repo is None or isinstance(repo, str) and re.fullmatch(r'[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+', repo),
            'invalid input runner repository')
    require(repository is None or repo is None or repo == repository, 'input runner repository differs')
    require(shard is None or shard == name, 'input runner shard differs: ' + name)
    require(isinstance(context['workflow_source'], dict), 'invalid input runner workflow object')
    workflow = workflow_source_fact(context['workflow_source'], repo or '')
    if workflow['snapshot'] == 'captured':
        require(repo is not None and shard == name, 'captured input runner requires repository/shard identity')
        require(workflow['workflow_path'] == '.github/workflows/ci.yml'
                and workflow['job'] == ('build' if name == 'build' else 'test-shards'),
                'input runner workflow path/job differs: ' + name)
    return {**context, 'workflow_source': workflow}


def maven_observation_fact(value, name, repository=None):
    if 'maven_execution_observation' not in value:
        return None
    fact = value['maven_execution_observation']
    require(isinstance(fact, dict) and type(fact.get('schema')) is int and fact['schema'] == 1
            and fact.get('type') == 'maven-execution-observation-summary'
            and fact.get('capture_status') == 'captured', 'invalid Maven execution observation')
    require(name is not None and fact.get('source_sha') == value.get('source_sha')
            and fact.get('source_tree') == value.get('source_tree')
            and fact.get('run_id') == value.get('run_id')
            and str(fact.get('run_attempt')) == str(value.get('run_attempt'))
            and fact.get('event') == value.get('event'), 'Maven observation source/run/attempt/event differs')
    context = input_runner_context(value, name, repository)
    require(context is not None and fact.get('runner_context') == value.get('runner_context'),
            'Maven observation runner scope differs')
    for field in ('observer_source_sha256', 'observer_jar_sha256', 'event_stream_sha256', 'observation_metadata_sha256'):
        require(isinstance(fact.get(field), str) and re.fullmatch(r'[0-9a-f]{64}', fact[field]),
                'invalid Maven observation digest')
    source_files = value.get('source_files')
    require(isinstance(source_files, dict), 'Maven observer source bytes unavailable')
    paths = ['.github/scripts/maven-inputs/' + name for name in
             ('BuildInputSpy.java', 'ConfiguredBuildInputListener.java', 'components.xml')]
    require(all(path in source_files for path in paths), 'Maven observer source files missing')
    identities = {Path(path).name: source_files[path] for path in paths}
    expected = hashlib.sha256(json.dumps(identities, sort_keys=True).encode()).hexdigest()
    require(fact['observer_source_sha256'] == expected, 'Maven observer source identity differs')
    observation_module = importlib.util.spec_from_file_location('maven_observation_reader',
            Path(__file__).with_name('_ci-build-observation.py'))
    reader = importlib.util.module_from_spec(observation_module)
    observation_module.loader.exec_module(reader)
    metadata = fact.get('observation_metadata')
    require(isinstance(metadata, dict), 'Maven observation metadata missing')
    raw = json.dumps(metadata, sort_keys=True, separators=(',', ':')).encode()
    replay = reader.observation_summary(raw)
    require(set(fact) == set(replay) | {'schema', 'type', 'capture_status', 'source_sha', 'source_tree',
            'run_id', 'run_attempt', 'event', 'runner_context', 'observer_source_sha256', 'observer_jar_sha256'},
            'invalid Maven observation summary fields')
    for field in replay:
        if field != 'event_stream_sha256':
            require(field in fact and type(fact[field]) is type(replay[field]) and fact[field] == replay[field],
                    'Maven observation projection differs: ' + field)
    return {'status': 'scoped-execution-facts', 'source_sha': fact['source_sha'],
            'runner_context': context, 'event_count': fact['event_count'],
            'goal_executions': fact['goal_executions'],
            'observation_metadata_sha256': fact['observation_metadata_sha256'],
            'observer_source_sha256': fact['observer_source_sha256'],
            'compiler_inputs_verified': False, 'whole_build_provenance_complete': False,
            'raw_event_replay_verified': False}


def compare_maven_observations(aggregate, workers):
    values = {'build': aggregate, **workers}
    if not any('maven_execution_observation' in value for value in values.values()):
        return None
    require(all('maven_execution_observation' in value for value in values.values()),
            'Maven observation missing from a current peer')
    return {'status': 'scoped-execution-facts',
            'roles': {name: maven_observation_fact(value, name) for name, value in values.items()},
            'compiler_inputs_verified': False, 'whole_build_provenance_complete': False}


def compare_runner_contexts(aggregate, workers):
    values = {'build': aggregate, **workers}
    if not any('runner_context' in value for value in values.values()):
        return None
    require(all('runner_context' in value for value in values.values()), 'input runner context missing from a current peer')
    contexts = {name: input_runner_context(value, name) for name, value in values.items()}
    repositories = {c['repository'] for c in contexts.values() if c['repository'] is not None}
    require(len(repositories) <= 1, 'input runner repositories differ')
    captured = [c['workflow_source'] for c in contexts.values() if c['workflow_source']['snapshot'] == 'captured']
    require(not captured or len(captured) == len(contexts), 'input runner workflow capture missing from a current peer')
    if captured:
        fields = ('workflow_ref', 'workflow_sha', 'workflow_path')
        require(len({tuple(c[k] for k in fields) for c in captured}) == 1, 'input runner workflow declarations differ')
    return {'status': 'consistent-declarations' if captured else 'unknown-workflow-source',
            'contexts': contexts, 'source_bytes_verified': False, 'all_workflow_jobs_recorded': False}


def test_resource_snapshot(value):
    if 'test_resource_files' not in value:
        return None
    files=value['test_resource_files']
    require(isinstance(files,dict), 'invalid test resource hash map')
    for name,checksum in files.items():
        require(isinstance(name,str) and name and '\\' not in name and not any(ord(c)<32 for c in name),
                'invalid test resource path')
        path=PurePosixPath(name)
        require(not path.is_absolute() and '..' not in path.parts and '.git' not in path.parts
                and path.as_posix()==name and re.fullmatch(r'(?:.+/)?target/test-classes/.+',name)
                and not name.endswith('.class'), 'invalid test resource path')
        require(isinstance(checksum,str) and re.fullmatch(r'[0-9a-f]{64}',checksum), 'invalid test resource hash')
    return files


def compare_input_snapshots(plan, aggregate, workers):
    require(isinstance(plan, dict) and isinstance(aggregate, dict) and isinstance(workers, dict),
            'invalid independent input snapshot objects')
    ids = [shard['id'] for shard in plan['shards']]
    require(ids and len(set(ids)) == len(ids) and set(workers) == set(ids),
            'independent build input shard set differs from the plan')
    source = plan['source_sha']
    require(re.fullmatch(r'[0-9a-f]{40}', source or ''), 'invalid planned source identity')
    require(re.fullmatch(r'[0-9a-f]{40}', aggregate.get('source_tree', '')), 'invalid source tree identity')
    for field in BUILD_INPUT_FIELDS:
        require(field in aggregate and aggregate[field], 'aggregate input projection missing: ' + field)
    require(isinstance(aggregate.get('dependency_repository_files'), dict), 'aggregate dependency hash map missing')
    run, attempt = aggregate.get('run_id'), str(aggregate.get('run_attempt'))
    require((run == 'local' or str(run).isdecimal()) and attempt.isdecimal() and int(attempt) > 0,
            'invalid independent build run/attempt')
    for name, value in [('build', aggregate), *sorted(workers.items())]:
        require(isinstance(value, dict), 'invalid input snapshot: ' + name)
        require(type(value.get('schema')) is int and value['schema'] == 1
                and value.get('phase') == 'pre-test-after-install', 'input is not a pre-test snapshot: ' + name)
        require(value.get('source_sha') == source and value.get('run_id') == run
                and str(value.get('run_attempt')) == attempt, 'input source/run/attempt differs: ' + name)
        for field in BUILD_INPUT_FIELDS:
            require(value.get(field) == aggregate[field], 'independent build input differs: ' + name + '/' + field)
        available = value.get('dependency_repository_files')
        require(isinstance(available, dict), 'dependency hash map missing: ' + name)
        # Incoming caches may contain unused extras. Their shared bytes may not change;
        # all actually resolved ordered test classpaths are compared above, including the SDK.
        for path in aggregate['dependency_repository_files'].keys() & available.keys():
            require(aggregate['dependency_repository_files'][path] == available[path],
                    'available dependency bytes differ: ' + name)
    resource_sets=[test_resource_snapshot(value) for value in [aggregate,*workers.values()]]
    has_resources=any(value is not None for value in resource_sets)
    if has_resources:
        require(all(value is not None for value in resource_sets), 'test resource capture missing from a current peer')
        require(all(value==resource_sets[0] for value in resource_sets), 'independent test resource hashes differ')
    runner = compare_runner_contexts(aggregate, workers)
    maven = compare_maven_observations(aggregate, workers)
    result = dict(schema=1, type='independent-build-input-hashes', source_sha=source,
                source_tree=aggregate['source_tree'], run_id=run, run_attempt=attempt,
                cohort_hash=plan['cohort_hash'], shards=ids, compared_fields=list(BUILD_INPUT_FIELDS),
                declared_input_hashes_equal=True, whole_build_provenance_complete=False,
                ability_proven=False, raw_bytes_uploaded=False)
    if has_resources:
        result['compared_fields'].append('test_resource_files')
    if runner is not None:
        result['runner_context'] = runner
    if maven is not None:
        result['maven_execution_observation'] = maven
    return result


def worker_input_paths(plan, worker_root):
    require(worker_root.is_dir() and not worker_root.is_symlink(), 'unsafe worker input root')
    expected = {'ci-inputs-' + shard['id'] for shard in plan['shards']}
    actual = {path.name for path in worker_root.iterdir()}
    # A previous aggregate artifact can remain on a rerun. The current aggregate
    # snapshot is the direct local file, never this downloaded copy.
    if 'ci-inputs-build' not in expected:
        actual.discard('ci-inputs-build')
    require(actual == expected, 'input artifact set differs from the plan')
    result = {}
    for shard in plan['shards']:
        directory = worker_root / ('ci-inputs-' + shard['id'])
        require(directory.is_dir() and not directory.is_symlink(), 'unsafe worker input artifact')
        path = directory / 'build-inputs.json'
        require(path.is_file() and not path.is_symlink(), 'worker input snapshot missing')
        require(path.stat().st_size <= 64 * 1024 * 1024, 'worker input snapshot exceeds the budget')
        result[shard['id']] = path
    return result


def compare_inputs(args):
    root = Path(args.root).resolve()
    require(Path(git(root, 'rev-parse', '--show-toplevel')).resolve() == root,
            'input comparison root must be the actual product checkout')
    require(not git(root, 'status', '--porcelain', '--untracked-files=no'), 'comparison source must be clean')
    from importlib.util import spec_from_file_location, module_from_spec
    spec = spec_from_file_location('input_shards', Path(__file__).with_name('_ci-shards.py'))
    shards = module_from_spec(spec)
    spec.loader.exec_module(shards)
    plan = shards.load_plan(args, root)
    aggregate_path = Path(args.aggregate_inputs)
    worker_root = Path(args.inputs_root)
    require(aggregate_path.is_file() and not aggregate_path.is_symlink(), 'unsafe aggregate input file')
    worker_paths = worker_input_paths(plan, worker_root)
    workers = {name: json.loads(path.read_text()) for name, path in worker_paths.items()}
    require(aggregate_path.stat().st_size <= 64 * 1024 * 1024, 'aggregate input snapshot exceeds the budget')
    aggregate = json.loads(aggregate_path.read_text())
    expected_source = git(root, 'rev-parse', 'HEAD')
    require(aggregate.get('source_sha') == expected_source
            and aggregate.get('source_tree') == git(root, 'rev-parse', 'HEAD^{tree}'),
            'aggregate input snapshot differs from the current checkout')
    identity = shards.execution_identity()
    expected_run = identity['run_id'] if identity['execution'] == 'github-actions' else 'local'
    expected_attempt = identity['run_attempt'] if identity['execution'] == 'github-actions' else 1
    require(aggregate.get('run_id') == expected_run and str(aggregate.get('run_attempt')) == str(expected_attempt),
            'aggregate input snapshot differs from the current run/attempt')
    result = compare_input_snapshots(plan, aggregate, workers)
    result['input_files'] = {'build': digest(aggregate_path), **{
        shard['id']: digest(worker_root / ('ci-inputs-' + shard['id']) / 'build-inputs.json')
        for shard in plan['shards']}}
    write(Path(args.output), result)
    print('independent build input hashes agree; whole-build provenance is not established')


def sha(value, label):
    require(isinstance(value, str) and re.fullmatch(r'[0-9a-f]{40}', value), 'invalid SHA: ' + label)
    return value


def event_labels(pr):
    if 'labels' not in pr:
        return dict(labels_snapshot_complete=False, label_name_sha256=None,
                    label_ascii_lower_sha256=None, label_ascii_case_complete=False)
    labels = pr['labels']
    require(isinstance(labels, list) and len(labels) <= 4096, 'invalid original label snapshot')
    names = []
    for label in labels:
        require(isinstance(label, dict) and isinstance(label.get('name'), str)
                and 0 < len(label['name']) <= 1024, 'invalid original label name')
        names.append(label['name'])
    require(len(set(names)) == len(names), 'duplicate original label name')
    hashed = lambda name: hashlib.sha256(name.encode('utf-8')).hexdigest()
    return dict(labels_snapshot_complete=True, label_name_sha256=sorted(hashed(name) for name in names),
                label_ascii_lower_sha256=sorted({hashed(name.lower()) for name in names if name.isascii()}),
                label_ascii_case_complete=all(name.isascii() for name in names))


def workflow_source_fact(value, repository):
    # Runner declarations stay separate from independently read workflow bytes.
    if value is None:
        return {'snapshot': 'unknown', 'source_bytes_verified': False}
    require(isinstance(value, dict), 'invalid runner workflow source')
    if value.get('snapshot') == 'unknown':
        require(value == {'snapshot': 'unknown', 'source_bytes_verified': False}
                and value['source_bytes_verified'] is False,
                'unknown runner workflow source contradicts populated fields')
        return dict(value)
    fields = {'snapshot', 'workflow_ref', 'workflow_sha', 'workflow_path', 'job', 'source_bytes_verified'}
    require(set(value) == fields and value['snapshot'] == 'captured'
            and value['source_bytes_verified'] is False, 'invalid runner workflow source fields')
    ref, path, job = (value[k] for k in ('workflow_ref', 'workflow_path', 'job'))
    require(all(isinstance(v, str) and 0 < len(v) <= 4096 and not any(ord(c) < 32 or ord(c) == 127 for c in v)
                for v in (ref, path, job)), 'invalid runner workflow source text')
    sha(value['workflow_sha'], 'runner workflow')
    require(re.fullmatch(r'\.github/workflows/[^/\\]+\.ya?ml', path)
            and path.rsplit('/', 1)[1] not in ('.', '..'), 'invalid runner workflow path')
    prefix = repository + '/' + path + '@'
    require(ref.startswith(prefix), 'runner workflow repository/path differs')
    git_ref = ref[len(prefix):]
    require(git_ref.startswith('refs/') and subprocess.run(['git', 'check-ref-format', git_ref],
            capture_output=True).returncode == 0, 'invalid runner workflow ref')
    require(re.fullmatch(r'[A-Za-z_][A-Za-z0-9_-]*', job), 'invalid runner workflow job')
    return dict(value)


def event_workflow_source(event, repository):
    require('workflow_source' not in event or isinstance(event['workflow_source'], dict),
            'explicit null runner workflow source or non-object declaration')
    return workflow_source_fact(event.get('workflow_source'), repository)


def workflow_source_snapshot(environment, repository):
    keys = ('GITHUB_WORKFLOW_REF', 'GITHUB_WORKFLOW_SHA', 'GITHUB_JOB')
    if not any(k in environment for k in keys):
        return workflow_source_fact(None, repository)
    require(all(k in environment and isinstance(environment[k], str) and environment[k] for k in keys),
            'incomplete runner workflow source variables')
    ref = environment['GITHUB_WORKFLOW_REF']
    match = re.fullmatch(re.escape(repository) + r'/(\.github/workflows/[^/\\]+\.ya?ml)@(refs/.+)', ref)
    require(match is not None, 'runner workflow repository/path differs')
    return workflow_source_fact({'snapshot': 'captured', 'workflow_ref': ref,
            'workflow_sha': environment['GITHUB_WORKFLOW_SHA'], 'workflow_path': match[1],
            'job': environment['GITHUB_JOB'], 'source_bytes_verified': False}, repository)


def event_snapshot(root, payload, environment):
    # Retain event identities and hash-only labels, never issue text or user details.
    repository = environment['GITHUB_REPOSITORY']
    event = environment['GITHUB_EVENT_NAME']
    require(event in ('pull_request', 'push', 'workflow_dispatch'), 'unsupported CI event')
    require(isinstance(payload, dict), 'invalid event payload')
    if payload.get('repository'):
        require(payload['repository']['full_name'] == repository, 'event repository differs')
    checkout = sha(git(root, 'rev-parse', 'HEAD'), 'checkout')
    require(checkout == sha(environment['GITHUB_SHA'], 'GITHUB_SHA'), 'checkout differs from event SHA')
    # cat-file exposes the original parent headers even in a depth-one checkout.
    headers = git(root, 'cat-file', '-p', checkout).split('\n\n', 1)[0].splitlines()
    parents = [sha(line[7:], 'parent') for line in headers if line.startswith('parent ')]
    tree = sha(git(root, 'rev-parse', 'HEAD^{tree}'), 'checkout tree')
    run_id, attempt = int(environment['GITHUB_RUN_ID']), int(environment['GITHUB_RUN_ATTEMPT'])
    require(run_id > 0 and attempt > 0, 'invalid run identity')
    value = dict(schema=1, repository=repository, event=event, run_id=run_id, run_attempt=attempt,
                 ref=environment['GITHUB_REF'], checkout_sha=checkout, checkout_tree=tree,
                 checkout_parents=parents, workflow_source=workflow_source_snapshot(environment, repository))
    default_branch = (payload.get('repository') or {}).get('default_branch')
    require(default_branch is None or isinstance(default_branch, str) and 0 < len(default_branch) <= 1024
            and not any(ord(char) < 32 for char in default_branch), 'invalid original default branch')
    value['repository_default_branch'] = ({'snapshot': 'unknown'} if default_branch is None
        else {'snapshot': 'captured', 'name': default_branch})
    if event == 'pull_request':
        if not payload.get('pull_request'):
            return dict(value, event_payload_complete=False,
                        gap='original PR payload unavailable; event identity remains unproven')
        pr = payload['pull_request']
        number = payload['number']
        require(type(number) is int and number > 0 and pr['number'] == number, 'PR number differs')
        require(payload['action'] in ('opened', 'synchronize', 'reopened'), 'unexpected PR action')
        head, base = pr['head'], pr['base']
        require(base['repo']['full_name'] == repository, 'PR base repository differs')
        require(environment['GITHUB_HEAD_REF'] == head['ref'] and environment['GITHUB_BASE_REF'] == base['ref'],
                'PR event refs differ from runner refs')
        require(value['ref'] == f'refs/pull/{number}/merge', 'PR checkout is not the merge ref')
        require(parents == [sha(base['sha'], 'PR base'), sha(head['sha'], 'PR head')],
                'checkout parents differ from original PR base/head')
        value['pull_request'] = dict(number=number, action=payload['action'], head_sha=head['sha'],
                                     base_sha=base['sha'], head_ref=head['ref'], base_ref=base['ref'],
                                     head_repository=head['repo']['full_name'], base_repository=repository,
                                     **event_labels(pr))
    else:
        if event == 'push' and payload.get('deleted') is True:
            return dict(value, event_payload_complete=False, ref_deleted=True,
                        gap='deleted ref has no after commit; checkout identity is retained separately')
        if event == 'push':
            require(payload['ref'] == value['ref'], 'event ref differs from runner ref')
            require(payload['after'] == checkout, 'push after differs from checkout')
    return dict(value, event_payload_complete=True)


def event_inputs(args):
    path = Path(os.environ['GITHUB_EVENT_PATH'])
    value = event_snapshot(Path(args.root).resolve(), json.loads(path.read_text()), os.environ)
    write(Path(args.output), value)


def reconcile_pr_identity(run, event, commit, repository, number, head, branch):
    # This binds identities, not full execution, group equivalence or performance.
    sha(head, 'expected head')
    require(type(number) is int and number > 0 and branch, 'explicit PR scope is required')
    require(event.get('event_payload_complete') is True, 'original PR payload is incomplete')
    require(event.get('schema') == 1 and run['event'] == event['event'] == 'pull_request',
            'not an original pull_request event')
    require(run['repository']['full_name'] == event['repository'] == repository, 'PR repository differs')
    require(type(run['id']) is int and run['id'] == event['run_id'], 'PR run ID differs')
    require(type(run['run_attempt']) is int and type(event['run_attempt']) is int
            and run['run_attempt'] == event['run_attempt'] == 1, 'PR first-attempt identity requires attempt 1')
    require(run['status'] == 'completed', 'PR run is not complete')
    require(run['path'] == '.github/workflows/ci.yml', 'unexpected PR workflow')
    workflow = event_workflow_source(event, repository)
    if workflow['snapshot'] == 'captured':
        require(workflow['workflow_path'] == run['path'], 'runner workflow path differs from run')
    pr = event['pull_request']
    require(pr['number'] == number and type(pr['number']) is int, 'foreign PR number')
    require(pr['action'] in ('opened', 'synchronize', 'reopened'), 'not an original PR update action')
    require(pr['head_repository'] == pr['base_repository'] == repository,
            'experiment PR must belong to the expected repository')
    require(pr['base_ref'] == 'main' and pr['head_ref'] == run['head_branch'] == branch, 'PR branch differs')
    require(sha(pr['head_sha'], 'event head') == sha(run['head_sha'], 'run head') == head, 'PR head differs')
    base = sha(pr['base_sha'], 'event base')
    checkout = sha(event['checkout_sha'], 'checkout')
    sha(event['checkout_tree'], 'checkout tree')
    require(sha(commit['sha'], 'API checkout') == checkout, 'checkout differs from archived Git commit')
    require(sha(commit['tree']['sha'], 'API tree') == event['checkout_tree']
            and [sha(parent['sha'], 'API parent') for parent in commit['parents']] == [base, head],
            'archived Git commit tree/parents differ from original PR input')
    require(event['checkout_parents'] == [base, head] and checkout not in (base, head),
            'PR merge checkout does not bind the original base/head')
    require(event['ref'] == f'refs/pull/{number}/merge', 'PR merge ref differs')
    # Historical Actions payloads can have an empty association after a PR closes.
    # A populated association is corroboration and must never contradict the capture.
    associations = run['pull_requests']
    require(isinstance(associations, list), 'invalid run PR associations')
    for association in associations:
        require(association['number'] == number and association['head']['sha'] == head
                and association['base']['sha'] == base, 'run PR association differs from original event')
    return dict(schema=1, repository=repository, run_id=run['id'], run_attempt=1, pull_request=number,
                head_sha=head, base_sha=base, checkout_sha=checkout, checkout_tree=event['checkout_tree'],
                conclusion=run['conclusion'], workflow_source=workflow, pr_event_identity_consistent=True,
                first_attempt_identity_consistent=True, first_complete_pr_sample_proven=False,
                group_equivalence_proven=False)


def pr_identity(args):
    value = reconcile_pr_identity(json.loads(Path(args.run_json).read_text()),
                                  json.loads(Path(args.event_inputs).read_text()),
                                  json.loads(Path(args.checkout_commit).read_text()), args.repo,
                                  args.pr, args.head, args.branch)
    value['checkout_commit_sha256'] = digest(Path(args.checkout_commit))
    value['run_json_sha256'] = digest(Path(args.run_json))
    value['event_inputs_sha256'] = digest(Path(args.event_inputs))
    write(Path(args.output), value)


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
    checkout_sha = None if run['event'] == 'pull_request' else run['head_sha']
    pr_identity_verified = False
    event_records = []
    for path in (directory / 'artifacts').glob('*-ci-shard-plan.zip'):
        try:
            with zipfile.ZipFile(path) as archive:
                if 'ci-event-inputs.json' in archive.namelist():
                    captured = json.loads(archive.read('ci-event-inputs.json'))
                    require(isinstance(captured, dict), 'event capture must be an object')
                    event_records.append(captured)
        except (ValueError, OSError, zipfile.BadZipFile) as error:
            gaps.append('event capture unreadable: ' + str(error))
    if len(event_records) == 1:
        event = event_records[0]
        write(directory / 'event-inputs.json', event)
        try:
            require(event['repository'] == args.repo and event['event'] == run['event']
                    and event['run_id'] == args.run and event['run_attempt'] == run['run_attempt'],
                    'event capture differs from current run/attempt')
            candidate_sha = sha(event['checkout_sha'], 'captured checkout')
            commit = api(f'{prefix}/git/commits/{candidate_sha}')
            write(directory / 'checkout-commit.json', commit)
            require(commit['sha'] == candidate_sha and commit['tree']['sha'] == event['checkout_tree']
                    and [parent['sha'] for parent in commit['parents']] == event['checkout_parents'],
                    'event checkout differs from independent Git commit')
            checkout_sha = candidate_sha
            command(['git', '-C', str(source), 'archive', '--format=tar', checkout_sha,
                     '-o', str(directory / 'checkout-source.tar')])
            if run['event'] == 'pull_request':
                scope = [getattr(args, name, None) for name in ('pr', 'head', 'branch')]
                require(all(scope), 'explicit --pr/--head/--branch scope is required for PR identity')
                identity = reconcile_pr_identity(run, event, commit, args.repo, *scope)
                write(directory / 'pr-identity.json', identity)
                pr_identity_verified = True
                if event.get('repository_default_branch', {}).get('snapshot') == 'captured':
                    base_sha = sha(event['pull_request']['base_sha'], 'original PR base')
                    base_commit = api(f'{prefix}/git/commits/{base_sha}')
                    require(base_commit['sha'] == base_sha, 'original base API source differs')
                    write(directory / 'base-commit.json', base_commit)
                    command(['git', '-C', str(source), 'archive', '--format=tar', base_sha,
                             '-o', str(directory / 'base-source.tar')])
        except (ValueError, KeyError, TypeError, OSError) as error:
            gaps.append('event identity incomplete: ' + str(error))
    elif run['event'] == 'pull_request':
        gaps.append('original PR event capture missing or ambiguous')
    shard_ids = []
    for path in (directory / 'artifacts').glob('*-ci-shard-plan.zip'):
        try:
            with zipfile.ZipFile(path) as archive:
                if 'shards.json' in archive.namelist():
                    plan = json.loads(archive.read('shards.json'))
                    require(checkout_sha and plan['source_sha'] == checkout_sha,
                            'plan checkout source is unproven or differs')
                    candidate_ids = [s['id'] for s in plan['shards']]
                    require(all(isinstance(name, str) and re.fullmatch(r'[a-z0-9]+(?:-[a-z0-9]+)*', name)
                                for name in candidate_ids), 'invalid shard ID')
                    require(len(set(candidate_ids)) == len(candidate_ids), 'duplicate shard ID')
                    shard_ids = candidate_ids
        except (ValueError, KeyError, TypeError, OSError, zipfile.BadZipFile) as error:
            gaps.append('shard plan identity incomplete: ' + str(error))
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
                'first_attempt': run['run_attempt'] == 1,
                'checkout_sha': checkout_sha, 'pr_event_identity_verified': pr_identity_verified,
                'first_complete_pr_sample_proven': False, 'cache_condition_claim': args.cache_condition, 'cache_condition_verified': False,
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
    p.add_argument('--runtime-class-dir', help='private existing-JaCoCo class dumps in the worker input sibling')
    p.add_argument('--shard-id', help='explicit current CI shard, or build for the aggregate')
    p.add_argument('--maven-observation', help='scoped hash-only Maven execution observation summary')
    p = commands.add_parser('compare-inputs')
    p.add_argument('--root', default='.')
    p.add_argument('--plan', required=True)
    p.add_argument('--inputs-root', required=True)
    p.add_argument('--aggregate-inputs', required=True)
    p.add_argument('--output', required=True)
    p = commands.add_parser('event-inputs')
    p.add_argument('--root', default='.')
    p.add_argument('--output', required=True)
    p = commands.add_parser('pr-identity')
    p.add_argument('--run-json', required=True)
    p.add_argument('--event-inputs', required=True)
    p.add_argument('--checkout-commit', required=True, help='archived Git commits API response for the actual checkout')
    p.add_argument('--repo', required=True)
    p.add_argument('--pr', type=int, required=True)
    p.add_argument('--head', required=True)
    p.add_argument('--branch', required=True)
    p.add_argument('--output', required=True)
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
    p.add_argument('--pr', type=int, help='expected experiment PR number')
    p.add_argument('--head', help='expected original PR head SHA')
    p.add_argument('--branch', help='expected experiment head branch')
    p.add_argument('--cache-condition', choices=['warm', 'cold', 'unknown'], default='unknown')
    p = commands.add_parser('verify')
    p.add_argument('--directory', required=True)
    args = parser.parse_args()
    if args.command == 'inputs':
        inputs(args)
    elif args.command == 'compare-inputs':
        compare_inputs(args)
    elif args.command == 'event-inputs':
        event_inputs(args)
    elif args.command == 'pr-identity':
        pr_identity(args)
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
