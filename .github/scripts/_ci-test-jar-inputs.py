#!/usr/bin/env python3
"""Bind default Maven test-JAR members to captured test bytes without hiding raw outputs."""
import hashlib
import io
from pathlib import PurePosixPath
import posixpath
import re
import stat
import xml.etree.ElementTree as ET
import zipfile


PRODUCERS = ('.github/scripts/_ci-receipt.py', '.github/scripts/_ci-test-jar-inputs.py')
HEX = re.compile(r'[0-9a-f]{64}')


def require(ok, message):
    if not ok:
        raise ValueError(message)


def checksum(raw):
    return hashlib.sha256(raw).hexdigest()


def safe_path(name):
    require(isinstance(name, str) and name and '\\' not in name and not any(ord(c) < 32 for c in name),
            'invalid test-JAR member path')
    path = PurePosixPath(name)
    require(not path.is_absolute() and '..' not in path.parts and '.git' not in path.parts
            and path.as_posix() == name, 'invalid test-JAR member path')
    return name


def xml(raw):
    require(len(raw) <= 1024 * 1024 and b'<!DOCTYPE' not in raw and b'<!ENTITY' not in raw,
            'unsupported test-JAR POM')
    root = ET.fromstring(raw)
    for node in root.iter():
        node.tag = node.tag.rsplit('}', 1)[-1]
    require(root.tag == 'project', 'invalid test-JAR POM root')
    return root


def project_identity(read_source, sources, path):
    """Resolve only hash-bound local parents and their literal properties."""
    chain, seen, current = [], set(), path
    while True:
        safe_path(current)
        require(current not in seen and len(chain) < 16, 'cyclic test-JAR parent model')
        require(current in sources, 'external test-JAR parent needs separate proof')
        seen.add(current)
        raw = read_source(current)
        require(sources[current] == checksum(raw), 'test-JAR POM source bytes differ')
        node = xml(raw)
        require(all(len(node.findall(key)) <= 1 and len(node.findall('parent/' + key)) <= 1
                    for key in ('groupId', 'artifactId', 'version')) and len(node.findall('parent')) <= 1,
                'ambiguous test-JAR source coordinates')
        chain.append((current, node))
        parent = node.find('parent')
        if parent is None:
            break
        relative = parent.find('relativePath')
        relative = '../pom.xml' if relative is None else (relative.text or '')
        require(relative and not relative.startswith('/'), 'external test-JAR parent needs separate proof')
        current = posixpath.normpath(str(PurePosixPath(current).parent / relative))
    properties, gav = {}, None
    def expand(value):
        require(isinstance(value, str) and value, 'missing test-JAR model identity')
        for unused in range(8):
            changed = re.sub(r'\$\{([^}]+)\}', lambda m: properties.get(m[1], m[0]), value)
            if changed == value:
                break
            value = changed
        require(re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.+-]*', value), 'unresolved test-JAR model identity')
        return value
    for unused, node in reversed(chain):
        own_properties = node.findall('properties/*')
        require(len({child.tag for child in own_properties}) == len(own_properties), 'ambiguous test-JAR properties')
        properties.update({child.tag: child.text or '' for child in own_properties})
        if gav is not None:
            require([expand(node.findtext('parent/' + key)) for key in ('groupId', 'artifactId', 'version')] == gav,
                    'test-JAR local parent coordinates differ')
        gav = [expand(node.findtext('groupId') or (gav[0] if gav else None)),
               expand(node.findtext('artifactId')),
               expand(node.findtext('version') or (gav[2] if gav else None))]
    return gav, chain, expand


def models(read_source, sources):
    """Only default-reactor, local-parent, unconfigured test-jar executions are admitted."""
    nodes = {}

    def load(name):
        safe_path(name)
        if name not in nodes:
            raw = read_source(name)
            require(sources.get(name) == checksum(raw), 'test-JAR POM source bytes differ')
            nodes[name] = xml(raw)
        return nodes[name]

    modules = []

    def visit(module):
        require(module not in modules and len(modules) < 128, 'duplicate or excessive test-JAR reactor')
        modules.append(module)
        node = load((module + '/' if module else '') + 'pom.xml')
        for child in node.findall('modules/module'):
            name = (module + '/' if module else '') + child.text.strip()
            safe_path(name)
            visit(name)
    visit('')
    result, unresolved = {}, {}
    for module in modules:
        path = (module + '/' if module else '') + 'pom.xml'
        node = load(path)
        selected = [plugin for plugin in node.findall('build/plugins/plugin')
                    if plugin.findtext('artifactId') == 'maven-jar-plugin'
                    and plugin.find("executions/execution/goals/goal[.='test-jar']") is not None]
        if not selected:
            continue
        try:
            require(module and len(selected) == 1, 'unsupported test-JAR producer selection')
            gav, chain, expand = project_identity(read_source, sources, path)
            require(gav[0] == 'io.tapstate', 'test-JAR producer is not a reactor artifact')
            jar_version = None
            for index, (unused, parent_node) in enumerate(reversed(chain)):
                require(all(parent_node.find('build/' + key) is None
                            for key in ('directory', 'testOutputDirectory', 'finalName')),
                        'custom test-JAR output directories require separate proof')
                require(not any(p.findtext('artifactId') == 'maven-jar-plugin'
                                for p in parent_node.findall('profiles/profile/build/plugins/plugin')
                                + parent_node.findall('profiles/profile/build/pluginManagement/plugins/plugin')),
                        'profile-specific test-JAR plugin requires separate proof')
                plugins = parent_node.findall('build/pluginManagement/plugins/plugin') + parent_node.findall('build/plugins/plugin')
                for plugin in plugins:
                    if plugin.findtext('artifactId') != 'maven-jar-plugin':
                        continue
                    require(plugin.findtext('groupId', 'org.apache.maven.plugins') == 'org.apache.maven.plugins'
                            and plugin.find('configuration') is None, 'configured test-JAR plugin requires separate proof')
                    for execution in plugin.findall('executions/execution'):
                        require(execution.find('configuration') is None, 'configured test-JAR execution requires separate proof')
                    if plugin.findtext('version'):
                        jar_version = expand(plugin.findtext('version'))
            executions = [e for e in selected[0].findall('executions/execution')
                          if e.find("goals/goal[.='test-jar']") is not None]
            require(len(executions) == 1 and executions[0].findtext('phase', 'package') == 'package'
                    and [g.text for g in executions[0].findall('goals/goal')] == ['test-jar']
                    and executions[0].findtext('id') and jar_version, 'unsupported test-JAR execution shape')
            output = module + '/target/' + gav[1] + '-' + gav[2] + '-tests.jar'
            result[output] = {'module': module, 'gav': gav, 'classifier': 'tests', 'type': 'test-jar',
                'plugin_version': jar_version, 'execution': executions[0].findtext('id'),
                'pom_sources': {name: sources[name] for name, unused in chain}}
        except ValueError as error:
            unresolved[path] = str(error)
    return result, unresolved


def output_fingerprint(value):
    require(isinstance(value, dict) and set(value) == {'sha256', 'size', 'mode'}
            and isinstance(value['sha256'], str) and HEX.fullmatch(value['sha256'])
            and type(value['size']) is int and 0 < value['size'] <= 512 * 1024 * 1024
            and type(value['mode']) is int and 0 <= value['mode'] <= 0o777,
            'invalid test-JAR output fingerprint')
    return value


def expected_members(snapshot, model):
    prefix = model['module'] + '/target/test-classes/'
    members = {name.removeprefix(prefix): value for field in ('test_files', 'test_resource_files')
               for name, value in snapshot.get(field, {}).items() if name.startswith(prefix)}
    ids = {name.removeprefix(prefix): value for name, value in snapshot['test_class_ids'].items() if name.startswith(prefix)}
    require(members and set(ids) == {name for name in members if name.endswith('.class')},
            'test-JAR compiled tree is incomplete')
    return members, ids


def metadata_names(model):
    prefix = 'META-INF/maven/' + model['gav'][0] + '/' + model['gav'][1] + '/'
    return {'META-INF/MANIFEST.MF', prefix + 'pom.xml', prefix + 'pom.properties'}


def capture(read_source, read_output, snapshot):
    declared, unresolved = models(read_source, snapshot['source_files'])
    records = {}
    for path, model in declared.items():
        if path not in snapshot['packaged_outputs']:
            continue
        output_fingerprint(snapshot['packaged_outputs'][path])
        raw = read_output(path)
        require(checksum(raw) == snapshot['packaged_outputs'][path]['sha256']
                and len(raw) == snapshot['packaged_outputs'][path]['size'], 'test-JAR output bytes changed during capture')
        expected, ids = expected_members(snapshot, model)
        actual, metadata, seen, total = {}, {}, set(), 0
        with zipfile.ZipFile(io.BytesIO(raw)) as jar:
            require(len(jar.infolist()) <= 20000, 'excessive test-JAR members')
            for entry in jar.infolist():
                name = entry.filename.rstrip('/') if entry.is_dir() else entry.filename
                safe_path(name)
                mode = entry.external_attr >> 16
                require(name not in seen and not entry.flag_bits & 1 and not mode & 0o7000
                        and stat.S_IFMT(mode) in (0, stat.S_IFDIR if entry.is_dir() else stat.S_IFREG),
                        'duplicate or special test-JAR member')
                seen.add(name)
                require(0 <= entry.file_size <= 64 * 1024 * 1024, 'test-JAR member exceeds budget')
                total += entry.file_size
                require(total <= 512 * 1024 * 1024, 'expanded test-JAR exceeds budget')
                if entry.is_dir():
                    require(entry.file_size == 0, 'test-JAR directory contains data'); continue
                data = jar.read(entry)
                require(len(data) == entry.file_size, 'test-JAR member size differs')
                (metadata if name in metadata_names(model) else actual)[name] = checksum(data)
            require(actual == expected and set(metadata) == metadata_names(model), 'test-JAR members differ from captured test tree')
            pom_name = next(n for n in metadata if n.endswith('/pom.xml'))
            pom = xml(jar.read(pom_name))
            require(all(len(pom.findall(key)) <= 1 and len(pom.findall('parent/' + key)) <= 1
                        for key in ('groupId', 'artifactId', 'version')), 'ambiguous test-JAR embedded coordinates')
            require([pom.findtext('groupId') or pom.findtext('parent/groupId'), pom.findtext('artifactId'),
                     pom.findtext('version') or pom.findtext('parent/version')] == model['gav'], 'test-JAR embedded coordinates differ')
            properties = next(n for n in metadata if n.endswith('/pom.properties'))
            group, artifact, version = model['gav']
            require(jar.read(properties) == ('artifactId=' + artifact + '\ngroupId=' + group + '\nversion=' + version + '\n').encode(),
                    'unsupported test-JAR properties')
            manifest = jar.read('META-INF/MANIFEST.MF').decode('ascii')
            require(re.fullmatch('Manifest-Version: 1.0\r\nCreated-By: Maven JAR Plugin ' + re.escape(model['plugin_version'])
                                + r'\r\nBuild-Jdk-Spec: [0-9]+\r\n\r\n', manifest), 'unsupported test-JAR manifest')
        for name in [*actual, *metadata]:
            parts = PurePosixPath(name).parts
            require(not any('/'.join(parts[:n]) in actual or '/'.join(parts[:n]) in metadata
                            for n in range(1, len(parts))), 'test-JAR file is also a parent')
        records[path] = {'model': model, 'output': dict(snapshot['packaged_outputs'][path]), 'members': actual,
                         'class_ids': ids, 'metadata': metadata}
    return {'schema': 1, 'producer_sources': {name: snapshot['source_files'].get(name) for name in PRODUCERS},
            'outputs': records, 'unresolved_models': unresolved}


def validate(packet, snapshot, read_source):
    require(isinstance(packet, dict) and set(packet) == {'schema', 'producer_sources', 'outputs', 'unresolved_models'}
            and type(packet['schema']) is int and packet['schema'] == 1, 'invalid test-JAR capture packet')
    require(packet['producer_sources'] == {name: snapshot['source_files'].get(name) for name in PRODUCERS}
            and all(isinstance(value, str) and HEX.fullmatch(value) for value in packet['producer_sources'].values()),
            'test-JAR capture helper source binding missing')
    require(all(checksum(read_source(name)) == value for name, value in packet['producer_sources'].items()),
            'original test-JAR capture source bytes differ')
    declared, unresolved = models(read_source, snapshot['source_files'])
    expected_paths = set(declared) & snapshot['packaged_outputs'].keys()
    require(isinstance(packet['outputs'], dict) and set(packet['outputs']) == expected_paths
            and packet['unresolved_models'] == unresolved, 'test-JAR producer set differs from original source')
    for path, record in packet['outputs'].items():
        require(isinstance(record, dict) and set(record) == {'model', 'output', 'members', 'class_ids', 'metadata'}
                and record['model'] == declared[path] and record['output'] == snapshot['packaged_outputs'][path],
                'test-JAR model/output binding differs')
        output_fingerprint(record['output'])
        members, ids = expected_members(snapshot, declared[path])
        require(record['members'] == members and record['class_ids'] == ids
                and isinstance(record['metadata'], dict) and set(record['metadata']) == metadata_names(declared[path])
                and all(isinstance(value, str) and HEX.fullmatch(value) for value in record['metadata'].values()),
                'test-JAR member binding differs')
    return packet


def observed_file(snapshot, reference):
    files = snapshot.get('maven_execution_observation', {}).get('observation_metadata', {}).get('files', {})
    value = files.get(reference) if isinstance(reference, str) else None
    require(isinstance(value, dict) and set(value) == {'state', 'size', 'sha256', 'path_identity_sha256'}
            and value['state'] == 'observed-file' and type(value['size']) is int and 0 <= value['size'] <= 4 * 1024**3
            and all(isinstance(value[k], str) and HEX.fullmatch(value[k]) for k in ('sha256', 'path_identity_sha256'))
            and checksum((value['path_identity_sha256'] + ':' + str(value['size']) + ':' + value['sha256']).encode()) == reference,
            'test-JAR observed file reference unavailable or inconsistent')
    return {key: value[key] for key in ('sha256', 'size')}


def observed_direct_realm(snapshot, realm):
    """Replay the plugin realm's direct URLs, not inherited loaders or class consumption."""
    require(isinstance(realm, dict) and set(realm) == {'state', 'id', 'ordered_urls', 'parent', 'imports',
                'import_package_prefixes', 'opaque_parent_class'}
            and realm['state'] == 'observed-realm-urls' and isinstance(realm['id'], str) and HEX.fullmatch(realm['id'])
            and realm['import_package_prefixes'] == 'not-observed'
            and (realm['opaque_parent_class'] is None or isinstance(realm['opaque_parent_class'], str)),
            'test-JAR direct plugin realm unobserved or unsupported')
    urls = realm['ordered_urls']
    require(isinstance(urls, list) and 0 < len(urls) <= 2048, 'test-JAR direct plugin realm URLs missing')
    normalized = []
    for url in urls:
        require(isinstance(url, dict) and set(url) == {'file'}, 'test-JAR nonfile direct realm URL needs separate proof')
        normalized.append({'file': observed_file(snapshot, url['file'])})
    return {'id': realm['id'], 'ordered_urls': normalized, 'inherited_realm_inputs': 'not-replayed',
            'class_consumption_proven': False, 'import_package_prefixes': 'not-observed'}


def execution_inputs(snapshot, model):
    fact = snapshot.get('maven_execution_observation')
    if fact is None:
        return None
    require(fact.get('capture_status') == 'captured' and fact.get('source_sha') == snapshot.get('source_sha')
            and fact.get('source_tree') == snapshot.get('source_tree')
            and str(fact.get('run_id')) == str(snapshot.get('run_id'))
            and str(fact.get('run_attempt')) == str(snapshot.get('run_attempt')), 'test-JAR goal origin differs')
    project = ':'.join(model['gav']); plugin = 'org.apache.maven.plugins:maven-jar-plugin:' + model['plugin_version']
    rows = [row for row in fact.get('observation_metadata', {}).get('event_records', [])
            if (row.get('project'), row.get('plugin'), row.get('goal')) == (project, plugin, 'test-jar')]
    require(all(row.get('execution') == model['execution'] for row in rows), 'additional test-JAR execution needs separate proof')
    require([row['event'] for row in rows] == ['MojoStarted', 'MojoConfigured', 'MojoSucceeded']
            and [row['nanos'] for row in rows] == sorted({row['nanos'] for row in rows}),
            'test-JAR original goal did not complete exactly once')
    require(rows[1].get('configured_implementation') == 'org.apache.maven.plugins.jar.TestJarMojo'
            and rows[1].get('active_profile_identities') == [], 'test-JAR goal implementation/profile unsupported')
    require(rows[0].get('merged_xml_config_sha256') == rows[2].get('merged_xml_config_sha256')
            and isinstance(rows[0].get('merged_xml_config_sha256'), str)
            and HEX.fullmatch(rows[0]['merged_xml_config_sha256']), 'test-JAR execution configuration differs')
    artifact = rows[2].get('plugin_artifact', {})
    require(all(artifact.get(key) == value for key, value in {'group': 'org.apache.maven.plugins',
            'artifact': 'maven-jar-plugin', 'version': model['plugin_version'], 'type': 'maven-plugin',
            'classifier': None, 'resolved': True}.items()), 'test-JAR plugin identity differs')
    path = 'org/apache/maven/plugins/maven-jar-plugin/' + model['plugin_version'] + '/maven-jar-plugin-' + model['plugin_version'] + '.jar'
    artifact_file = observed_file(snapshot, artifact.get('file'))
    require(artifact_file['sha256'] == snapshot['dependency_repository_files'].get(path), 'test-JAR plugin bytes missing')
    inputs = rows[2].get('plugin_artifacts')
    require(isinstance(inputs, list) and 0 < len(inputs) <= 2048, 'test-JAR plugin descriptor identities unavailable')
    normalized = []
    for item in inputs:
        require(isinstance(item, dict) and item.get('type') in ('jar', 'maven-plugin')
                and all(isinstance(item.get(key), str) and re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.+-]*', item[key])
                        for key in ('group', 'artifact', 'version')) and item.get('resolved') is True,
                'test-JAR plugin input identity unavailable')
        classifier = item.get('classifier')
        require(classifier is None or isinstance(classifier, str) and re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.+-]*', classifier),
                'invalid test-JAR plugin input classifier')
        name = '/'.join([item['group'].replace('.', '/'), item['artifact'], item['version'],
                        item['artifact'] + '-' + item['version'] + ('-' + classifier if classifier else '') + '.jar'])
        observed = observed_file(snapshot, item.get('file'))
        require(snapshot['dependency_repository_files'].get(name) == observed['sha256'],
                'test-JAR plugin input bytes unavailable or differ')
        normalized.append({**item, 'file': observed})
    realm = observed_direct_realm(snapshot, rows[2].get('realm'))
    require({'file': artifact_file} in realm['ordered_urls'], 'test-JAR plugin artifact absent from direct realm')
    return {'plugin': plugin, 'implementation': rows[1]['configured_implementation'],
            'artifact': {**artifact, 'file': artifact_file}, 'descriptor_artifacts': normalized,
            'observed_direct_realm': realm}



def observed_execution(snapshot, model):
    return execution_inputs(snapshot, model) is not None


def classpath_role(entry, records):
    matches = []
    for path, record in records.items():
        group, artifact, version = record['model']['gav']
        repository = 'repository/' + group.replace('.', '/') + '/' + artifact + '/' + version + '/' + artifact + '-' + version + '-tests.jar'
        if entry['identity'] in ('checkout/' + path, repository):
            require(entry['sha256'] == record['output']['sha256'] and entry['size'] == record['output']['size'],
                    'resolved test-JAR bytes differ from producer')
            classes = {name.removesuffix('.class'): {'sha256': record['members'][name], 'class_id': value}
                       for name, value in record['class_ids'].items() if name.startswith('io/tapstate/')}
            require(entry.get('own_classes') == classes, 'resolved test-JAR class bindings differ')
            matches.append(path)
    require(len(matches) <= 1, 'ambiguous test-JAR classpath producer')
    return matches[0] if matches else None


def observed_test_scope(snapshot, consumer, record, read_source):
    """A matching filename cannot move a production-scope dependency into test inputs."""
    fact = snapshot.get('maven_execution_observation')
    if fact is None:
        return False
    path = ('pom.xml' if consumer in ('', '.') else safe_path(consumer) + '/pom.xml')
    gav, unused, unused = project_identity(read_source, snapshot['source_files'], path)
    artifacts = [artifact for row in fact.get('observation_metadata', {}).get('event_records', [])
                 if row.get('project') == ':'.join(gav)
                 for artifact in row.get('project_artifacts_at_event', [])
                 if [artifact.get(k) for k in ('group', 'artifact', 'version')] == record['model']['gav']
                 and (artifact.get('classifier') == 'tests' or observed_file(snapshot, artifact.get('file'))['sha256'] == record['output']['sha256'])]
    require(artifacts and all(artifact.get('classifier') == 'tests' and artifact.get('type') == 'test-jar'
                             and artifact.get('scope') == 'test' and artifact.get('resolved') is True
                             and observed_file(snapshot, artifact.get('file')) == {k: record['output'][k] for k in ('sha256', 'size')} for artifact in artifacts),
            'test-JAR consumer scope or byte binding is unobserved or differs')
    return True
