#!/usr/bin/env python3
"""Run the original Maven command with bounded, hash-only execution observations."""
import argparse
from collections import Counter
import hashlib
import importlib.util
import json
import os
import re
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile

sys.dont_write_bytecode = True


def require(value, message):
    if not value:
        raise ValueError(message)


def digest(path):
    result = hashlib.sha256()
    with path.open('rb') as stream:
        while chunk := stream.read(65536):
            result.update(chunk)
    return result.hexdigest()


def load_receipt():
    spec = importlib.util.spec_from_file_location('build_observation_receipt', Path(__file__).with_name('_ci-receipt.py'))
    reader = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(reader)
    return reader


def object_fields(value, fields, label):
    require(isinstance(value, dict) and set(value) == set(fields.split()), 'invalid ' + label + ' fields')


def text_field(value, label):
    require(isinstance(value, str) and 0 < len(value) <= 4096 and value.strip(), 'invalid ' + label)


def sha_field(value, label):
    require(isinstance(value, str) and re.fullmatch(r'[0-9a-f]{64}', value), 'invalid ' + label)


def bounded_list(value, label):
    require(isinstance(value, list) and len(value) <= 2048, 'invalid ' + label)
    return value


def file_reference(value, files, reason=None):
    if isinstance(value, dict) and set(value) == {'file'}:
        require(isinstance(value['file'], str) and value['file'] in files, 'unknown observed file reference')
    else:
        object_fields(value, 'state reason', 'unobserved runtime file')
        require(reason and value['state'] == 'not-observed' and value['reason'] == reason,
                'unsupported runtime file state')


def artifact_fact(value, files):
    if isinstance(value, dict) and value.get('state') == 'not-observed':
        object_fields(value, 'state reason', 'unobserved artifact')
        require(value['reason'] == 'no-artifact', 'unsupported artifact state')
        return
    fields = 'group artifact version type classifier scope resolved'
    if isinstance(value, dict) and value.get('state') == 'not-observed-no-file':
        object_fields(value, fields + ' state', 'artifact')
    else:
        object_fields(value, fields + ' file', 'artifact')
        file_reference({'file': value['file']}, files)
    for name in ('group', 'artifact', 'version', 'type'):
        text_field(value[name], 'artifact ' + name)
    for name in ('classifier', 'scope'):
        if value[name] is not None:
            require(isinstance(value[name], str) and len(value[name]) <= 4096, 'invalid artifact ' + name)
    require(type(value['resolved']) is bool, 'invalid artifact resolution')


def realm_fact(value, files):
    pending, identities = [(value, 0)], set()
    visits = 0
    while pending:
        realm, depth = pending.pop()
        visits += 1
        require(depth <= 33 and visits <= 20000 and isinstance(realm, dict), 'invalid realm structure')
        state = realm.get('state')
        if state == 'not-observed':
            object_fields(realm, 'state reason', 'unobserved realm')
            require(realm['reason'] == 'no-realm', 'unsupported realm state')
        elif state == 'reference':
            object_fields(realm, 'state id', 'realm reference')
            sha_field(realm['id'], 'realm identity')
            require(realm['id'] in identities, 'unknown realm reference')
        else:
            object_fields(realm, 'state id ordered_urls parent imports import_package_prefixes opaque_parent_class', 'realm')
            require(state == 'observed-realm-urls', 'partial or unsupported realm')
            sha_field(realm['id'], 'realm identity')
            identities.add(realm['id'])
            require(len(identities) <= 129 and realm['import_package_prefixes'] == 'not-observed',
                    'unsupported realm completeness')
            if realm['opaque_parent_class'] is not None:
                text_field(realm['opaque_parent_class'], 'opaque parent class')
            for url in bounded_list(realm['ordered_urls'], 'realm URLs'):
                if isinstance(url, dict) and url.get('state') == 'nonfile-url':
                    object_fields(url, 'state identity_sha256', 'nonfile realm URL')
                    sha_field(url['identity_sha256'], 'realm URL identity')
                else:
                    file_reference(url, files)
            # The producer visits imports before the parent, in declared order.
            pending.append((realm['parent'], depth + 1))
            pending.extend((child, depth + 1) for child in reversed(bounded_list(realm['imports'], 'realm imports')))


def event_fact(record, files):
    common = 'event nanos project execution goal plugin fully_injected_parameter_values'
    if record['event'] == 'MojoConfigured':
        object_fields(record, common + ' configured_implementation effective_model_sha256 active_profile_identities '
                      'registered_compile_source_roots registered_test_source_roots requested_concurrency '
                      'runtime_identity compiler_consumed_files selected_or_forked_compiler', 'configured event')
        text_field(record['configured_implementation'], 'configured implementation')
        sha_field(record['effective_model_sha256'], 'effective model digest')
        for profile in bounded_list(record['active_profile_identities'], 'active profiles'):
            object_fields(profile, 'id_sha256', 'profile')
            sha_field(profile['id_sha256'], 'profile identity')
        for name in ('registered_compile_source_roots', 'registered_test_source_roots'):
            for root in bounded_list(record[name], 'source roots'):
                object_fields(root, 'path_identity_sha256 file_consumption_proven', 'source root')
                sha_field(root['path_identity_sha256'], 'source root identity')
                require(root['file_consumption_proven'] is False, 'unsupported source consumption claim')
        require(type(record['requested_concurrency']) is int and record['requested_concurrency'] > 0,
                'invalid requested concurrency')
        for name in ('compiler_consumed_files', 'selected_or_forked_compiler'):
            require(record[name] == 'not-observed', 'unsupported compiler completeness')
        runtime = record['runtime_identity']
        object_fields(runtime, 'maven_jvm_version java_vendor java_home_identity_sha256 maven_core '
                      'major_jvm_file_identities capture_phase whole_JDK_installation_proven '
                      'selected_or_forked_compiler_proven', 'runtime')
        for name in ('maven_jvm_version', 'java_vendor'):
            text_field(runtime[name], name)
        sha_field(runtime['java_home_identity_sha256'], 'Java home identity')
        require(runtime['capture_phase'] == 'first-configured-mojo'
                and runtime['whole_JDK_installation_proven'] is False
                and runtime['selected_or_forked_compiler_proven'] is False, 'unsupported runtime completeness')
        file_reference(runtime['maven_core'], files, 'nonfile-maven-core')
        major = runtime['major_jvm_file_identities']
        require(isinstance(major, dict) and set(major) == {'release', 'bin/java', 'lib/modules', 'lib/ct.sym'},
                'invalid major JVM files')
        for fact in major.values():
            file_reference(fact, files, 'runtime-file-absent')
    else:
        extra = ' plugin_artifact plugin_artifacts realm' if record['event'] == 'MojoSucceeded' else ''
        object_fields(record, common + ' merged_xml_config_sha256 project_artifacts_at_event' + extra, 'Mojo event')
        sha_field(record['merged_xml_config_sha256'], 'merged XML digest')
        for artifact in bounded_list(record['project_artifacts_at_event'], 'project artifacts'):
            artifact_fact(artifact, files)
        if extra:
            artifact_fact(record['plugin_artifact'], files)
            for artifact in bounded_list(record['plugin_artifacts'], 'plugin artifacts'):
                artifact_fact(artifact, files)
            realm_fact(record['realm'], files)
    require(type(record['nanos']) is int and record['nanos'] >= 0, 'invalid event timing')
    require(record['fully_injected_parameter_values'] == 'not-observed', 'unsupported parameter completeness')


def observation_summary(raw):
    require(len(raw) <= 64 * 1024 * 1024, 'build observation exceeds bound')
    def unique_object(pairs):
        value = {}
        for name, field in pairs:
            require(name not in value, 'duplicate observation key')
            value[name] = field
        return value
    value = json.loads(raw, object_pairs_hook=unique_object)
    object_fields(value, 'schema type events event_records files failures dropped_events hashed_bytes java_version '
                  'java_vendor os_arch annotation_processor_inputs external_generator_inputs '
                  'whole_build_provenance_complete elapsed_nanos', 'observation')
    require(type(value.get('schema')) is int and value['schema'] == 3
            and value.get('type') == 'local-configured-build-input-observation', 'unsupported build observation')
    records, files = value.get('event_records'), value.get('files')
    require(isinstance(records, list) and 0 < len(records) <= 20000
            and isinstance(files, dict) and 0 < len(files) <= 10000, 'empty build observation')
    require(value['events'] is None and type(value['hashed_bytes']) is int
            and 0 <= value['hashed_bytes'] <= 4 * 1024**3
            and type(value['elapsed_nanos']) is int and value['elapsed_nanos'] > 0, 'invalid observation metrics')
    for name in ('java_version', 'java_vendor', 'os_arch'):
        text_field(value[name], name)
    for name in ('annotation_processor_inputs', 'external_generator_inputs'):
        require(value[name] == 'not-observed', 'unsupported generator completeness')
    require(type(value.get('failures')) is int and value['failures'] == 0
            and type(value.get('dropped_events')) is int and value['dropped_events'] == 0,
            'incomplete build observation')
    require(value.get('whole_build_provenance_complete') is False, 'unsupported whole provenance claim')
    counts = {kind: Counter() for kind in ('MojoStarted', 'MojoConfigured', 'MojoSucceeded')}
    active, configured = Counter(), Counter()
    previous_nanos = 0
    for record in records:
        require(isinstance(record, dict) and record.get('event') in counts
                and record.get('state') != 'partial', 'failed or partial Mojo observation')
        key = tuple(record.get(name) for name in ('project', 'plugin', 'execution', 'goal'))
        require(all(isinstance(item, str) and item for item in key), 'invalid Mojo identity')
        for item in key:
            text_field(item, 'Mojo identity')
        event_fact(record, files)
        require(previous_nanos <= record['nanos'] <= value['elapsed_nanos'], 'invalid event timing order')
        previous_nanos = record['nanos']
        kind = record['event']
        if kind == 'MojoStarted':
            active[key] += 1
        elif kind == 'MojoConfigured':
            require(active[key] > configured[key], 'configured event precedes Mojo start')
            configured[key] += 1
        else:
            require(configured[key] > 0 and active[key] > 0, 'success event precedes configured Mojo')
            active[key] -= 1
            configured[key] -= 1
        counts[kind][key] += 1
    require(counts['MojoStarted'] == counts['MojoConfigured'] == counts['MojoSucceeded'],
            'configured Mojo event set differs')
    unobserved = 0
    for identity, fact in files.items():
        require(isinstance(identity, str) and re.fullmatch(r'[0-9a-f]{64}', identity)
                and isinstance(fact, dict), 'invalid observed file identity')
        if fact.get('state') == 'not-observed':
            require(set(fact) == {'state', 'reason'} and fact['reason'] == 'nonregular-or-oversize',
                    'unsupported unobserved file state')
            unobserved += 1
            continue
        object_fields(fact, 'state size sha256 path_identity_sha256', 'observed file')
        require(isinstance(identity, str) and re.fullmatch(r'[0-9a-f]{64}', identity)
                and isinstance(fact, dict) and fact.get('state') == 'observed-file'
                and type(fact.get('size')) is int and 0 <= fact['size'] <= 4 * 1024**3
                and isinstance(fact.get('sha256'), str) and re.fullmatch(r'[0-9a-f]{64}', fact['sha256'])
                and isinstance(fact.get('path_identity_sha256'), str)
                and re.fullmatch(r'[0-9a-f]{64}', fact['path_identity_sha256']), 'unobserved build file')
    pending = [value]
    nodes = 0
    while pending:
        current = pending.pop()
        nodes += 1
        require(nodes <= 1000000, 'observation node count exceeds bound')
        if isinstance(current, dict):
            if 'file' in current:
                require(isinstance(current['file'], str) and current['file'] in files, 'unknown observed file reference')
            pending.extend(current.values())
        elif isinstance(current, list):
            pending.extend(current)
    return {'event_count': len(records), 'goal_executions': sum(counts['MojoStarted'].values()),
            'event_stream_sha256': hashlib.sha256(raw).hexdigest(),
            'observation_metadata_sha256': hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':')).encode()).hexdigest(),
            'raw_event_bytes_retained': False,
            'project_count': len({key[0] for key in counts['MojoStarted']}),
            'observed_file_count': len(files) - unobserved, 'unobserved_file_entries': unobserved,
            'all_available_file_identities_observed': unobserved == 0,
            'hashed_bytes': value.get('hashed_bytes'),
            'observation_metadata': value, 'compiler_inputs_verified': False,
            'whole_build_provenance_complete': False, 'ability_proven': False,
            'raw_production_or_dependency_bytes_uploaded': False}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', required=True)
    parser.add_argument('--work-dir', required=True)
    parser.add_argument('--output', required=True)
    parser.add_argument('--shard-id', required=True)
    parser.add_argument('command', nargs=argparse.REMAINDER)
    args = parser.parse_args()
    command = args.command[1:] if args.command[:1] == ['--'] else args.command
    require(command and '-DskipTests' in command and command[-1] == 'install',
            'observation requires the original complete skipTests install command')
    require(not any(item.startswith('-Dmaven.test.skip') or item.startswith('-Dmaven.ext.class.path')
                    or item.startswith(('-pl=', '--projects=', '-rf=', '--resume-from='))
                    or item in ('-pl', '--projects', '-N', '--non-recursive', '-rf', '--resume-from') for item in command),
            'unsupported prepare command narrowing or extension override')
    root = Path(args.root).resolve(strict=True)
    work = Path(args.work_dir).resolve(strict=True)
    output = Path(args.output)
    require(work.is_dir() and not work.is_relative_to(root), 'observer work directory must be outside checkout')
    repositories = [item.split('=', 1)[1] for item in command if item.startswith('-Dmaven.repo.local=')]
    repository = Path(repositories[-1]) if repositories else Path.home() / '.m2/repository'
    require(not work.is_relative_to(repository.resolve()), 'observer work directory must be outside Maven repository')
    runner_temp = os.getenv('RUNNER_TEMP')
    if runner_temp:
        require(not work.is_relative_to((Path(runner_temp) / 'ci-inputs').resolve()),
                'observer compiled files must be outside input artifacts')
    require(output.parent.resolve(strict=True) == work and not output.exists() and not output.is_symlink(),
            'observer output must be a new file in its work directory')
    require(not subprocess.check_output(['git', 'status', '--porcelain', '--untracked-files=no'], cwd=root).strip(),
            'prepare source is dirty')
    receipt = load_receipt()
    source_sha = receipt.git(root, 'rev-parse', 'HEAD')
    source_tree = receipt.git(root, 'rev-parse', 'HEAD^{tree}')
    context = receipt.input_runner_snapshot(os.environ, args.shard_id)
    executable = shutil.which(command[0])
    require(executable, 'Maven executable unavailable')
    maven_home = Path(executable).resolve(strict=True).parent.parent
    require((maven_home / 'lib').is_dir() and (maven_home / 'boot').is_dir(), 'Maven runtime layout unavailable')
    sources = Path(__file__).with_name('maven-inputs')
    source_files = sorted(sources.glob('*.java')) + [sources / 'components.xml']
    source_hash = hashlib.sha256(json.dumps({p.name: digest(p) for p in source_files}, sort_keys=True).encode()).hexdigest()
    with tempfile.TemporaryDirectory(prefix='ci-build-observation-', dir=work) as scratch:
        scratch = Path(scratch)
        classes = scratch / 'classes'
        registration = classes / 'META-INF/plexus'
        registration.mkdir(parents=True)
        shutil.copyfile(sources / 'components.xml', registration / 'components.xml')
        cp = str(maven_home / 'lib/*') + os.pathsep + str(maven_home / 'boot/*')
        subprocess.run(['javac', '-cp', cp, '-d', str(classes), *map(str, sources.glob('*.java'))], check=True)
        jar = scratch / 'observer.jar'
        subprocess.run(['jar', '--create', '--file', str(jar), '-C', str(classes), '.'], check=True)
        raw = scratch / 'observation.json'
        env = os.environ.copy()
        env['TAPSTATE_BUILD_INPUT_OBSERVATION'] = str(raw)
        actual = [command[0], '-Dmaven.ext.class.path=' + str(jar), *command[1:]]
        result = subprocess.run(actual, cwd=root, env=env)
        if result.returncode:
            return result.returncode
        require(receipt.git(root, 'rev-parse', 'HEAD') == source_sha
                and not subprocess.check_output(['git', 'status', '--porcelain', '--untracked-files=no'], cwd=root).strip(),
                'prepare source changed')
        require(raw.is_file() and not raw.is_symlink(), 'build observation missing')
        summary = observation_summary(raw.read_bytes())
        value = {'schema': 1, 'type': 'maven-execution-observation-summary', 'capture_status': 'captured',
                 'source_sha': source_sha, 'source_tree': source_tree,
                 'run_id': os.getenv('GITHUB_RUN_ID', 'local'),
                 'run_attempt': os.getenv('GITHUB_RUN_ATTEMPT', '1'),
                 'event': os.getenv('GITHUB_EVENT_NAME', 'local'), 'runner_context': context,
                 'observer_source_sha256': source_hash, 'observer_jar_sha256': digest(jar), **summary}
        payload = json.dumps(value, sort_keys=True, indent=2).encode() + b'\n'
        require(len(payload) <= 64 * 1024 * 1024, 'summary exceeds bound')
        descriptor = os.open(output, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        with os.fdopen(descriptor, 'wb') as stream:
            stream.write(payload)
    return 0


if __name__ == '__main__':
    try:
        raise SystemExit(main())
    except (ValueError, OSError, subprocess.CalledProcessError) as error:
        print('build observation: ' + str(error), file=sys.stderr)
        raise SystemExit(1)
