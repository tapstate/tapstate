#!/usr/bin/env python3
"""Bind default repository JAR/POM identities to original resolved Maven observations."""
import hashlib
import importlib.util
from pathlib import Path, PurePosixPath
import re
import posixpath
import xml.etree.ElementTree as ET
import sys

sys.dont_write_bytecode = True
spec = importlib.util.spec_from_file_location('artifact_source_models', Path(__file__).with_name('_ci-test-jar-inputs.py'))
source_models = importlib.util.module_from_spec(spec)
spec.loader.exec_module(source_models)
require = source_models.require
PRODUCERS = ('.github/scripts/_ci-receipt.py', '.github/scripts/_ci-maven-artifacts.py',
             '.github/scripts/_ci-test-jar-inputs.py')
LITERAL = re.compile(r'[A-Za-z0-9][A-Za-z0-9_.+-]*')
HEX = re.compile(r'[0-9a-f]{64}')
SCOPES = {'compile', 'provided', 'runtime', 'system', 'test'}


def checksum(raw):
    return hashlib.sha256(raw).hexdigest()


def declared_reactor(snapshot, read_source):
    """Conservatively include literal modules from both default and profile declarations."""
    sources, seen, declared, poms, errors = snapshot['source_files'], set(), {}, {}, {}
    def visit(module):
        if module in seen:
            return
        require(len(seen) < 128, 'excessive source reactor models')
        seen.add(module)
        name = (module + '/' if module else '') + 'pom.xml'
        try:
            raw = read_source(name)
            require(sources.get(name) == checksum(raw), 'source reactor POM bytes differ')
            node = source_models.xml(raw)
            poms[name] = sources[name]
            gav, chain, unused = source_models.project_identity(read_source, sources, name)
            poms.update({path: sources[path] for path, unused in chain})
            declared.setdefault(':'.join(gav), []).append(module)
            for child in node.findall('modules/module') + node.findall('profiles/profile/modules/module'):
                text = (child.text or '').strip()
                require(text and re.fullmatch(r'[A-Za-z0-9_.+/-]+', text) and not text.startswith('/'),
                        'nonliteral source reactor module')
                relative = posixpath.normpath((module + '/' if module else '') + text)
                source_models.safe_path(relative)
                visit(relative)
        except (ValueError, OSError, KeyError, ET.ParseError) as error:
            errors[name] = str(error)
    visit('')
    observed = sorted({row.get('project') for row in snapshot.get('maven_execution_observation', {}).get(
        'observation_metadata', {}).get('event_records', []) if isinstance(row.get('project'), str)
        and len(row['project'].split(':')) == 3 and all(LITERAL.fullmatch(v) for v in row['project'].split(':'))})
    return {'declared': {key: sorted(value) for key, value in declared.items()}, 'observed': observed,
            'pom_sources': poms, 'unresolved': errors}


def repository_coordinates(path):
    source_models.safe_path(path)
    parts = PurePosixPath(path).parts
    if len(parts) < 4:
        return None
    gav = ['.'.join(parts[:-3]), parts[-3], parts[-2]]
    return gav if all(LITERAL.fullmatch(value) for value in gav) else None


def is_reactor(reactor, gav):
    key = ':'.join(gav)
    return key in reactor['declared'] or key in reactor['observed']


def additional_repository_paths(snapshot, read_source, read_repository):
    """Capture only actually resolved non-reactor coordinates omitted by the legacy namespace filter."""
    reactor = declared_reactor(snapshot, read_source)
    if reactor['unresolved']:
        return []
    result = set()
    for entries in snapshot['resolved_test_classpaths'].values():
        for entry in entries:
            identity = entry['identity']
            if not identity.startswith('repository/io/tapstate/'):
                continue
            path = identity.removeprefix('repository/')
            gav = repository_coordinates(path)
            if gav is None or is_reactor(reactor, gav):
                continue
            base = gav[1] + '-' + gav[2]
            if PurePosixPath(path).name not in (base + '.jar', base + '.pom'):
                continue
            pom = path[:-4] + '.pom'
            result.update((path, pom))
            try:
                node = source_models.xml(read_repository(pom))
                parent = node.find('parent')
                parent_gav = [parent.findtext(k) for k in ('groupId', 'artifactId', 'version')] if parent is not None else []
                if len(parent_gav) == 3 and all(isinstance(v, str) and LITERAL.fullmatch(v) for v in parent_gav):
                    result.add('/'.join([parent_gav[0].replace('.', '/'), parent_gav[1], parent_gav[2],
                                         parent_gav[1] + '-' + parent_gav[2] + '.pom']))
            except (ValueError, OSError, KeyError, ET.ParseError):
                pass
    return sorted(result)


def capture(snapshot, read_source):
    sources = snapshot['source_files']
    helpers = {name: sources.get(name) for name in PRODUCERS}
    require(all(isinstance(h, str) and HEX.fullmatch(h) and checksum(read_source(name)) == h
                for name, h in helpers.items()), 'resolved Maven artifact capture source binding missing')
    observation = snapshot.get('maven_execution_observation')
    require(isinstance(observation, dict) and observation.get('capture_status') == 'captured'
            and all(observation.get(k) == snapshot.get(k) for k in ('source_sha', 'source_tree', 'run_id', 'event'))
            and str(observation.get('run_attempt')) == str(snapshot.get('run_attempt')),
            'resolved Maven artifact observation origin differs')
    metadata = observation.get('observation_metadata')
    require(isinstance(metadata, dict) and isinstance(metadata.get('event_records'), list)
            and len(metadata['event_records']) <= 20000, 'resolved Maven artifact event stream unavailable')
    index, visited = {}, 0
    for row in metadata['event_records']:
        if row.get('event') not in ('MojoStarted', 'MojoSucceeded'):
            continue
        artifacts = row.get('project_artifacts_at_event')
        require(isinstance(artifacts, list) and len(artifacts) <= 2048, 'invalid original project artifact list')
        for artifact in artifacts:
            visited += 1
            require(visited <= 100000 and isinstance(artifact, dict), 'excessive original project artifacts')
            if artifact.get('resolved') is not True or artifact.get('classifier') is not None:
                continue
            gav = tuple(artifact.get(k) for k in ('group', 'artifact', 'version'))
            if not all(isinstance(v, str) and LITERAL.fullmatch(v) for v in gav):
                continue
            index.setdefault((row.get('project'), gav), []).append(artifact)
    reactor = declared_reactor(snapshot, read_source)
    paths = snapshot.get('resolved_test_classpaths')
    require(isinstance(paths, dict) and len(paths) <= 128, 'resolved Maven artifact classpaths unavailable')
    consumers, unresolved, files, total = {}, {}, {}, 0
    for consumer, entries in paths.items():
        require(isinstance(entries, list) and len(entries) <= 2048, 'invalid original consumer classpath')
        selected = {}
        for entry in entries:
            total += 1
            require(total <= 20000 and isinstance(entry, dict), 'excessive original consumer classpaths')
            identity = entry.get('identity')
            require(isinstance(identity, str), 'original classpath identity missing')
            if not identity.startswith('repository/'):
                continue
            path = identity.removeprefix('repository/')
            source_models.safe_path(path)
            gav = repository_coordinates(path)
            if gav is not None and is_reactor(reactor, gav):
                continue
            require(isinstance(entry.get('sha256'), str) and HEX.fullmatch(entry['sha256'])
                    and type(entry.get('size')) is int and 0 <= entry['size'] <= 4 * 1024**3,
                    'resolved Maven artifact classpath byte binding differs')
            require(identity not in selected or selected[identity] == entry, 'conflicting original classpath identity')
            selected[identity] = entry
        if not selected:
            continue
        errors = unresolved.setdefault(consumer, {})
        try:
            pom = 'pom.xml' if consumer in ('', '.') else source_models.safe_path(consumer) + '/pom.xml'
            gav, chain, unused = source_models.project_identity(read_source, sources, pom)
        except (ValueError, KeyError, OSError):
            errors.update({identity: 'original consumer model unresolved' for identity in selected})
            continue
        result = {'gav': gav, 'pom_sources': {name: sources[name] for name, unused in chain}, 'artifacts': {}}
        consumers[consumer] = result
        for identity, entry in selected.items():
            path = identity.removeprefix('repository/')
            candidate = repository_coordinates(path)
            suffix = PurePosixPath(path).suffix
            kind = suffix.removeprefix('.')
            if reactor['unresolved']:
                errors[identity] = 'source reactor membership unresolved'; continue
            if candidate is None or kind not in ('jar', 'pom') or PurePosixPath(path).name != candidate[1] + '-' + candidate[2] + suffix:
                errors[identity] = 'unsupported classifier or repository artifact coordinates'; continue
            available = snapshot['dependency_repository_files'].get(path)
            if available is None:
                errors[identity] = 'original repository byte binding unavailable'; continue
            require(available == entry['sha256'], 'resolved Maven artifact classpath byte binding differs')
            rows = index.get((':'.join(gav), tuple(candidate)), [])
            if not rows:
                errors[identity] = 'no original resolved artifact for this consumer'; continue
            scopes, observed, failure = set(), [], None
            for artifact in rows:
                if artifact.get('type') != kind or artifact.get('scope') not in SCOPES:
                    failure = 'unsupported or conflicting resolved artifact type/scope'; break
                reference = artifact.get('file')
                try:
                    if reference not in files:
                        files[reference] = source_models.observed_file(snapshot, reference)
                    observed.append(files[reference])
                except (ValueError, TypeError):
                    failure = 'original resolved artifact file is unobserved'; break
                scopes.add(artifact['scope'])
            expected = {key: entry[key] for key in ('sha256', 'size')}
            if failure or not observed or any(value != expected for value in observed):
                errors[identity] = failure or 'conflicting original resolved artifact bytes'; continue
            result['artifacts'][identity] = {'gav': candidate, 'type': kind, 'classifier': None,
                                            **expected, 'scopes': sorted(scopes)}
        if not errors:
            unresolved.pop(consumer)
    return {'schema': 1, 'capture_helpers': helpers, 'reactor': reactor, 'consumers': consumers, 'unresolved': unresolved,
            'archive_producer_identity_proven': False, 'actual_class_consumption_proven': False}


def validate(packet, snapshot, read_source):
    require(isinstance(packet, dict) and type(packet.get('schema')) is int and packet['schema'] == 1
            and set(packet) == {'schema', 'capture_helpers', 'reactor', 'consumers', 'unresolved',
                               'archive_producer_identity_proven', 'actual_class_consumption_proven'}
            and packet['archive_producer_identity_proven'] is False
            and packet['actual_class_consumption_proven'] is False,
            'invalid resolved Maven artifact capture')
    require(packet == capture(snapshot, read_source), 'resolved Maven artifact capture differs from original inputs')
    return packet


def consumer_identity(packet, consumer, identity):
    """Only use facts already independently replayed by the original archive reader."""
    if packet is None:
        return None
    require(isinstance(packet, dict) and type(packet.get('schema')) is int and packet['schema'] == 1
            and packet.get('archive_producer_identity_proven') is False
            and packet.get('actual_class_consumption_proven') is False,
            'unverified resolved Maven artifact identity packet')
    value = packet.get('consumers', {}).get(consumer)
    if not isinstance(value, dict) or identity in packet.get('unresolved', {}).get(consumer, {}):
        return None
    artifact = value.get('artifacts', {}).get(identity)
    if not isinstance(artifact, dict):
        return None
    return {'capture_helpers': packet['capture_helpers'], 'reactor': packet['reactor'], 'consumer_gav': value['gav'],
            'consumer_pom_sources': value['pom_sources'], 'artifact': artifact}
