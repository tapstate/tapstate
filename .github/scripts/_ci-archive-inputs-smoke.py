#!/usr/bin/env python3
"""Offline protocol controls with real Git archives and original artifact ZIPs."""
import copy
import importlib.util
import io
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import zipfile

sys.dont_write_bytecode = True
spec = importlib.util.spec_from_file_location('inputs', Path(__file__).with_name('_ci-archive-inputs.py'))
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)
v, g, s = m.source.variants, m.g, m.shards
count = 0


def check(value, message):
    global count
    assert value, message
    count += 1


def rejected(call, message):
    try:
        call()
    except (ValueError, OSError, KeyError, TypeError, zipfile.BadZipFile) as error:
        check(message in str(error), 'wrong rejection: ' + str(error))
    else:
        raise AssertionError('expected rejection: ' + message)


def encode(value):
    return (json.dumps(value, sort_keys=True) + '\n').encode()


def zipped(files):
    output = io.BytesIO()
    with zipfile.ZipFile(output, 'w') as archive:
        for name, value in files.items():
            archive.writestr(name, value if isinstance(value, bytes) else encode(value))
    return output.getvalue()


def packaged_test_fixture(pre, work, bytecode, resolved_artifacts=False, maven_schema=3):
    """Protocol bytes and native-shaped observations; this does not run Maven or compile Java."""
    roles = m.test_jars
    path = 'fixture/target/fixture-1-tests.jar'
    support_path = 'fixture/target/test-classes/io/tapstate/fixture/Support.class'
    support = b'nonselected producer helper protocol bytes'
    pre['test_files'][support_path] = g.checksum(support)
    pre['test_class_ids'][support_path] = g.receipt.crc64(support)
    members = {'io/tapstate/fixture/SampleTest.class': bytecode, 'io/tapstate/fixture/Support.class': support,
               'META-INF/MANIFEST.MF': b'Manifest-Version: 1.0\r\nCreated-By: Maven JAR Plugin 3.4.2\r\nBuild-Jdk-Spec: 21\r\n\r\n',
               'META-INF/maven/io.tapstate/fixture/pom.xml': b'<project><groupId>io.tapstate</groupId><artifactId>fixture</artifactId><version>1</version></project>',
               'META-INF/maven/io.tapstate/fixture/pom.properties': b'artifactId=fixture\ngroupId=io.tapstate\nversion=1\n'}
    raw = zipped(members)
    pre['packaged_outputs'][path] = dict(sha256=g.checksum(raw), size=len(raw), mode=420)
    own = {name.removesuffix('.class'): dict(sha256=g.checksum(data), class_id=g.receipt.crc64(data))
           for name, data in members.items() if name.endswith('.class')}
    pre['resolved_test_classpaths']['consumer'] = [dict(identity='repository/io/tapstate/fixture/1/fixture-1-tests.jar',
                                                       sha256=g.checksum(raw), size=len(raw), own_classes=own)]
    files = {}
    def observed_digest(sha, size, label):
        identity = g.checksum(label.encode())
        key = g.checksum((identity + ':' + str(size) + ':' + sha).encode())
        files[key] = dict(state='observed-file', size=size, sha256=sha, path_identity_sha256=identity)
        return key
    def reference(data, label):
        return observed_digest(g.checksum(data), len(data), label)
    plugin_bytes = b'bounded plugin protocol fixture'
    plugin_file, output_file = reference(plugin_bytes, 'plugin.jar'), reference(raw, 'fixture-tests.jar')
    plugin_path = 'org/apache/maven/plugins/maven-jar-plugin/3.4.2/maven-jar-plugin-3.4.2.jar'
    pre['dependency_repository_files'][plugin_path] = g.checksum(plugin_bytes)
    artifact = dict(group='org.apache.maven.plugins', artifact='maven-jar-plugin', version='3.4.2', type='maven-plugin',
                    classifier=None, scope=None, resolved=True, file=plugin_file)
    dependency = dict(group='io.tapstate', artifact='fixture', version='1', type='test-jar', classifier='tests',
                      scope='test', resolved=True, file=output_file)
    runtime = dict(maven_jvm_version='21', java_vendor='Fixture', java_home_identity_sha256='7' * 64,
                   maven_core={'file': plugin_file}, major_jvm_file_identities={n: {'file': plugin_file}
                   for n in ('release', 'bin/java', 'lib/modules', 'lib/ct.sym')}, capture_phase='first-configured-mojo',
                   whole_JDK_installation_proven=False, selected_or_forked_compiler_proven=False)
    external_dependencies = []
    if resolved_artifacts:
        for entry in pre['resolved_test_classpaths']['fixture']:
            relative = entry['identity'].removeprefix('repository/')
            parts = Path(relative).parts
            external_dependencies.append(dict(group='.'.join(parts[:-3]), artifact=parts[-3], version=parts[-2],
                type=Path(relative).suffix.removeprefix('.'), classifier=None, scope='test', resolved=True,
                file=observed_digest(entry['sha256'], entry['size'], relative)))
    rows = []
    for project, execution, goal, implementation, dependencies in [
            ('io.tapstate:fixture:1', 'test-jar', 'test-jar', 'TestJarMojo', external_dependencies),
            ('io.tapstate:consumer:1', 'default-jar', 'jar', 'JarMojo', [dependency])]:
        common = dict(project=project, execution=execution, goal=goal,
                      plugin='org.apache.maven.plugins:maven-jar-plugin:3.4.2', fully_injected_parameter_values='not-observed')
        ordinary = dict(merged_xml_config_sha256='5' * 64, project_artifacts_at_event=dependencies)
        rows.extend([{**common, **ordinary, 'event': 'MojoStarted', 'nanos': len(rows) + 1},
            {**common, 'event': 'MojoConfigured', 'nanos': len(rows) + 2,
             'configured_implementation': 'org.apache.maven.plugins.jar.' + implementation,
             'effective_model_sha256': '8' * 64, 'active_profile_identities': [],
             'registered_compile_source_roots': [], 'registered_test_source_roots': [], 'requested_concurrency': 1,
             'runtime_identity': runtime, 'compiler_consumed_files': 'not-observed', 'selected_or_forked_compiler': 'not-observed'},
            {**common, **ordinary, 'event': 'MojoSucceeded', 'nanos': len(rows) + 3, 'plugin_artifact': artifact,
             'plugin_artifacts': [artifact], 'realm': dict(state='observed-realm-urls', id='e' * 64,
                ordered_urls=[{'file': plugin_file}], parent=dict(state='not-observed', reason='no-realm'),
                imports=[], import_package_prefixes='not-observed', opaque_parent_class=None)}])
    metadata = dict(schema=3, type='local-configured-build-input-observation', events=None, event_records=rows,
        files=files, failures=0, dropped_events=0, hashed_bytes=sum(v['size'] for v in files.values()),
        java_version='21', java_vendor='Fixture', os_arch='fixture', annotation_processor_inputs='not-observed',
        external_generator_inputs='not-observed', whole_build_provenance_complete=False, elapsed_nanos=7)
    if maven_schema == 4:
        metadata['schema'] = 4
        for row in rows:
            artifacts = list(row.get('project_artifacts_at_event', [])) + list(row.get('plugin_artifacts', []))
            if 'plugin_artifact' in row:
                artifacts.append(row['plugin_artifact'])
            for item in artifacts:
                if 'base_version' not in item:
                    item['base_version'] = item['version']
                    if item['version'] == '2-SNAPSHOT':
                        item['version'] = '2-20261016.000000-1'
    observer = g.module('test_jar_fixture_observation', '_ci-build-observation.py')
    source_hash = g.checksum(json.dumps({Path(n).name: h for n, h in pre['source_files'].items()
                                       if n.startswith('.github/scripts/maven-inputs/')}, sort_keys=True).encode())
    pre['runner_context'] = g.receipt.input_runner_snapshot({'GITHUB_REPOSITORY': 'example/project',
        'GITHUB_WORKFLOW_REF': 'example/project/.github/workflows/ci.yml@refs/pull/7/merge',
        'GITHUB_WORKFLOW_SHA': 'e' * 40, 'GITHUB_JOB': 'test-shards'}, 'rest')
    pre['maven_execution_observation'] = dict(schema=1, type='maven-execution-observation-summary', capture_status='captured',
        **{k: pre[k] for k in ('source_sha', 'source_tree', 'run_id', 'run_attempt', 'event', 'runner_context')},
        observer_source_sha256=source_hash, observer_jar_sha256='4' * 64,
        **observer.observation_summary(json.dumps(metadata).encode()))
    pre['packaged_test_outputs'] = roles.capture(lambda n: (work / n).read_bytes(), lambda n: raw if n == path else None, pre)
    if resolved_artifacts:
        pre['resolved_maven_artifacts'] = m.maven_artifacts.capture(pre, lambda n: (work / n).read_bytes())
        m.maven_artifacts.validate(pre['resolved_maven_artifacts'], pre, lambda n: (work / n).read_bytes())
    return raw


def run(callback=None, source_case='package io.tapstate.fixture; class SampleTest { @Test void holds() {} }\n', two_workers=False, only_callback=False, maven_source=False, workflow_sources=None, dependency_metadata=False, base_source=False, variant='C', run_id=42, temporary_parent=None, case_variants=None, extra_source_files=None, test_resource_bytes=None, test_jar=False, resolved_artifacts=False, maven_schema=3):
    global count
    count = 0
    with tempfile.TemporaryDirectory(prefix='ci-archive-inputs-', dir=temporary_parent) as temporary:
        root = Path(temporary).resolve()
        main, work, evidence = root / 'main', root / 'owned', root / 'evidence'
        evidence.mkdir()
        subprocess.run(['git', 'init', '-q', str(main)], check=True)
        for key, value in [('user.name', 'Fixture'), ('user.email', 'fixture@example.test'), ('commit.gpgsign', 'false')]:
            v.git(main, 'config', key, value)
        assert type(maven_schema) is int and maven_schema in (3, 4)
        assert not resolved_artifacts or test_jar, 'resolved artifact fixture requires original Maven observations'
        if test_jar:
            assert not two_workers and test_resource_bytes is None, 'bounded test-JAR fixture uses one complete owner class'
            maven_source = True
            extra_source_files = dict(extra_source_files or {})
            for name in (*m.test_jars.PRODUCERS, *(m.maven_artifacts.PRODUCERS if resolved_artifacts else ())):
                extra_source_files[name] = Path(__file__).with_name(Path(name).name).read_bytes()
            extra_source_files['pom.xml'] = ('<project><groupId>io.tapstate</groupId><artifactId>parent</artifactId>'
                '<version>${revision}</version><properties><revision>1</revision></properties><packaging>pom</packaging>'
                '<modules><module>fixture</module><module>consumer</module></modules><build><pluginManagement><plugins>'
                '<plugin><artifactId>maven-jar-plugin</artifactId><version>3.4.2</version></plugin>'
                '</plugins></pluginManagement></build></project>')
            parent = '<parent><groupId>io.tapstate</groupId><artifactId>parent</artifactId><version>${revision}</version></parent>'
            extra_source_files['fixture/pom.xml'] = ('<project>' + parent + '<artifactId>fixture</artifactId><build><plugins>'
                '<plugin><artifactId>maven-jar-plugin</artifactId><executions><execution><id>test-jar</id>'
                '<goals><goal>test-jar</goal></goals></execution></executions></plugin></plugins></build></project>')
            extra_source_files['consumer/pom.xml'] = ('<project>' + parent + '<artifactId>consumer</artifactId><dependencies>'
                '<dependency><groupId>io.tapstate</groupId><artifactId>fixture</artifactId><version>${revision}</version>'
                '<type>test-jar</type><classifier>tests</classifier><scope>test</scope></dependency></dependencies></project>')
        if resolved_artifacts:
            extra_source_files['fixture/pom.xml'] = extra_source_files['fixture/pom.xml'].replace('</project>',
                '<dependencies><dependency><groupId>example</groupId><artifactId>lib</artifactId><version>1</version>'
                '<scope>test</scope></dependency></dependencies></project>')
        (main / 'fixture.cfg').write_text('base\n')
        (main / 'pom.xml').write_text('<project><modelVersion>4.0.0</modelVersion><groupId>io.tapstate</groupId><artifactId>parent</artifactId><version>1</version><packaging>pom</packaging><modules><module>fixture</module></modules></project>')
        (main / 'fixture/src/test/java/io/tapstate/fixture').mkdir(parents=True)
        (main / 'fixture/pom.xml').write_text('<project><modelVersion>4.0.0</modelVersion><groupId>io.tapstate</groupId><artifactId>fixture</artifactId><version>1</version></project>')
        case_path = 'fixture/src/test/java/io/tapstate/fixture/SampleTest.java'
        (main / case_path).write_text(case_variants['A'] if case_variants else source_case)
        if two_workers:
            (main / 'fixture/src/test/java/io/tapstate/fixture/OtherTest.java').write_text('package io.tapstate.fixture; class OtherTest { @Test void holds() {} }\n')
        (main / '.github/workflows').mkdir(parents=True)
        (main / '.github/workflows/ci.yml').write_text('jobs:\n  test:\n    run: |\n      .github/scripts/ci-evidence.sh inventory --root . --lane ci --published-connectors absent --native-cli-presence absent --output source-inventory.json\n')
        if workflow_sources is not None:
            for name, text in workflow_sources.items():
                assert re.fullmatch(r'[A-Za-z0-9_.-]+\.yml', name), name
                (main / '.github/workflows' / name).write_text(text)
        (main / 'product.txt').write_text('preserved product fix\n')
        for name, raw0 in (extra_source_files or {}).items():
            path0=main/name;path0.parent.mkdir(parents=True,exist_ok=True);path0.write_bytes(raw0 if isinstance(raw0,bytes) else raw0.encode())
        observer_source = main / 'test-support/src/test/java/io/tapstate/testsupport/CiCaseEvidence.java'
        observer_source.parent.mkdir(parents=True)
        observer_source.write_text('package io.tapstate.testsupport; class CiCaseEvidence {}\n')
        if maven_source:
            directory = main / '.github/scripts/maven-inputs'
            directory.mkdir(parents=True)
            for name in ('BuildInputSpy.java', 'ConfiguredBuildInputListener.java', 'components.xml'):
                (directory / name).write_bytes(Path(__file__).with_name('maven-inputs').joinpath(name).read_bytes())
        v.git(main, 'add', '.')
        v.git(main, 'commit', '-qm', 'Original source')
        base = v.text(main, 'rev-parse', 'HEAD')
        (main / 'fixture.cfg').write_text('C\n')
        if case_variants:
            (main / case_path).write_text(case_variants['C'])
        v.git(main, 'add', '.')
        v.git(main, 'commit', '-qm', 'Reviewed source')
        anchor, tree = v.text(main, 'rev-parse', 'HEAD'), v.text(main, 'rev-parse', 'HEAD^{tree}')
        v.git(main, 'remote', 'add', 'origin', 'https://github.com/example/project.git')
        v.git(main, 'worktree', 'add', '-qb', 'ws/fixture', str(work), anchor)
        scope = {'repository': 'example/project', 'branch': 'ws/fixture', 'variant': 'C', 'event': 'pull_request',
                 'run_id': 42, 'run_attempt': 1, 'head_sha': anchor, 'base_sha': base, 'pr': 7}

        def save(name, value):
            path = evidence / name
            path.parent.mkdir(parents=True, exist_ok=True)
            raw = value if isinstance(value, bytes) else encode(value)
            path.write_bytes(raw)
            return {'path': name, 'sha256': g.checksum(raw)}

        def patch(label):
            index = root / ('index-' + label)
            v.git(work, 'read-tree', tree, index=index)
            blob = root / ('blob-' + label)
            blob.write_text(label + '\n')
            oid = v.text(work, 'hash-object', '-w', str(blob))
            v.git(work, 'update-index', '--cacheinfo', '100644', oid, 'fixture.cfg', index=index)
            if case_variants:
                blob.write_text(case_variants[label])
                oid = v.text(work, 'hash-object', '-w', str(blob))
                v.git(work, 'update-index', '--cacheinfo', '100644', oid, case_path, index=index)
            target = v.text(work, 'write-tree', index=index)
            return v.diff_patch(work, tree, target)

        recipe = v.prepare(g.References(evidence), work,
                           {'schema': 1, 'type': 'variant-recipe-request', 'anchor_commit': anchor,
                            'scope': {'repository': scope['repository'], 'branch': scope['branch'], 'worktree': str(work)},
                            'owned_paths': ['fixture.cfg'] + ([case_path] if case_variants else []),
                            'patches': {x: save(x + '.patch', patch(x) if x != 'C' else b'') for x in 'ABC'}})
        recipe_ref = save('recipe.json', recipe)
        if variant != 'C':
            tree = recipe['variant_trees'][variant]
            anchor = v.text(work, 'commit-tree', tree, '-p', base, '-m', 'Reviewed variant ' + variant)
            v.git(work, 'read-tree', '--reset', '-u', tree)
        scope.update(variant=variant, run_id=run_id, head_sha=anchor)
        merge = v.text(work, 'commit-tree', tree, '-p', base, '-p', anchor, '-m', 'Original PR checkout')

        def commit(sha):
            return {'sha': sha, 'tree': {'sha': v.text(work, 'rev-parse', sha + '^{tree}')},
                    'parents': [{'sha': p} for p in v.text(work, 'rev-list', '--parents', '-n', '1', sha).split()[1:]]}

        event = {'repository': scope['repository'], 'event': scope['event'], 'run_id': run_id, 'run_attempt': 1,
                 'checkout_sha': merge, 'checkout_tree': tree, 'checkout_parents': [base, anchor]}
        test = {'module': 'fixture', 'kind': 'unit', 'class': 'io.tapstate.fixture.SampleTest'}
        worker = {'id': 'rest', 'tests': [test], 'estimated_seconds': 1, 'required_exec': s.execs([test])}
        plan = {'schema': 1, 'source_sha': merge, 'cohort_hash': s.cohort([test]), 'selection': 'default-reactor',
                'expected': [test], 'shards': [worker]}
        if two_workers:
            other = {'module': 'fixture', 'kind': 'unit', 'class': 'io.tapstate.fixture.OtherTest'}
            other_worker = {'id': 'engine', 'tests': [other], 'estimated_seconds': 1, 'required_exec': s.execs([other])}
            plan['expected'].append(other)
            plan['shards'].append(other_worker)
            plan['cohort_hash'] = s.cohort(plan['expected'])
        compiled = 'fixture/target/test-classes/io/tapstate/fixture/SampleTest.class'
        bytecode = b'protocol fixture, not an actual compiled build'
        if test_jar:
            bytecode += g.checksum((work / case_path).read_bytes()).encode()
        prod = 'fixture/target/classes/io/tapstate/fixture/Product.class'
        sources = {n: g.checksum((work / n).read_bytes()) for n in v.text(work, 'ls-files').splitlines()}
        dep = 'example/lib/1/lib-1.jar'
        pre = {'schema': 1, 'source_sha': merge, 'source_tree': tree, 'event': scope['event'], 'run_id': str(run_id), 'run_attempt': '1',
               'phase': 'pre-test-after-install', 'source_files': sources, 'production_files': {prod: 'a' * 64},
               'production_class_ids': {prod: 'a' * 16}, 'production_class_access_flags': {prod: 1},
               'test_files': {compiled: g.checksum(bytecode)}, 'test_class_ids': {compiled: g.receipt.crc64(bytecode)},
               'packaged_outputs': {'fixture/target/fixture.jar': {'sha256': 'b' * 64, 'size': 10, 'mode': 420}},
               'dependency_repository_files': {dep: 'c' * 64}, 'jacoco_version': '0.8.13',
               'resolved_test_classpaths': {'fixture': [{'identity': 'repository/' + dep, 'sha256': 'c' * 64, 'size': 10,
                                                       'own_classes': {}}]},
               'toolchain': {'java': '21', 'maven': '3.9.9', 'os': 'fixture', 'architecture': 'aarch64', 'cpu_count': 2}}
        if dependency_metadata:
            pre.update(copy.deepcopy(dependency_metadata) if isinstance(dependency_metadata, dict)
                       else dict(dependency_coordinate_models={}, dependency_producer_facts={}))
        observer_class = 'test-support/target/test-classes/io/tapstate/testsupport/CiCaseEvidence.class'
        pre['test_files'][observer_class] = g.checksum(b'observer fixture')
        pre['test_class_ids'][observer_class] = g.receipt.crc64(b'observer fixture')
        if two_workers:
            other_compiled = 'fixture/target/test-classes/io/tapstate/fixture/OtherTest.class'
            pre['test_files'][other_compiled] = g.checksum(b'other protocol class')
            pre['test_class_ids'][other_compiled] = g.receipt.crc64(b'other protocol class')
        if test_resource_bytes is not None:
            pre['test_resource_files']={name:g.checksum(raw) for name,raw in test_resource_bytes.items()}
        if test_jar:
            packaged_raw = packaged_test_fixture(pre, work, bytecode, resolved_artifacts=resolved_artifacts, maven_schema=maven_schema)
        post = copy.deepcopy(pre)
        post['phase'] = 'post-test-before-pack'
        binding = {'execution': 'github-actions', 'run_id': str(run_id), 'run_attempt': 1}
        provenance = {'schema': 1, 'source_sha': merge, 'cohort_hash': plan['cohort_hash'], 'shard_id': 'rest',
                      **binding, 'status': 'success', 'invocation_id': '1' * 32,
                      'case_observer': {'enabled': True, 'providers': [s.OBSERVER], 'build_inputs_sha256': g.checksum(encode(pre)),
                                        'audit_elapsed_ns': 1, 'observer_files': {n: 'd' * 64 for n in s.OBSERVER_FILES},
                                        'checked_scopes': [{'module': 'fixture', 'kind': 'unit', 'dependency_excludes': []}]}}
        for label, phase, start, end in [('reset', 'reset-test-evidence', 1, 2), ('verify', 'selected-verify', 2, 4)]:
            provenance[label] = {'schema': 1, 'command': 'run', 'phase': phase, 'source_sha': merge,
                                 'cohort_hash': plan['cohort_hash'], 'shard_id': 'rest', **binding, 'status': 'success',
                                 'event': 'end', 'started_monotonic_ns': start, 'monotonic_ns': end, 'elapsed_ns': end - start,
                                 'utc': '2026-10-14T10:00:00+00:00', 'ended_utc': '2026-10-14T10:00:01+00:00'}
        xml_path = 'fixture/target/surefire-reports/TEST-io.tapstate.fixture.SampleTest.xml'
        fixture_methods = re.findall(r'@Test\s+void\s+(\w+)\s*\(', (work / case_path).read_text())
        xml = ('<testsuite name="io.tapstate.fixture.SampleTest" tests="' + str(len(fixture_methods))
               + '" skipped="0" errors="0" failures="0">' + ''.join(
                   '<testcase classname="io.tapstate.fixture.SampleTest" name="' + method + '"/>'
                   for method in fixture_methods) + '</testsuite>').encode()
        marker = s.provenance_path(worker).as_posix()
        result_files = {compiled: bytecode, xml_path: xml, worker['required_exec'][0]: b'coverage protocol fixture', marker: encode(provenance), **(test_resource_bytes or {})}
        if test_jar:
            result_files['fixture/target/test-classes/io/tapstate/fixture/Support.class'] = b'nonselected producer helper protocol bytes'
        manifest = {'schema': 2, 'source_sha': merge, 'cohort_hash': plan['cohort_hash'], 'shard_id': 'rest',
                    'tests': [test], 'required_exec': worker['required_exec'], 'provenance': provenance,
                    'provenance_path': marker, 'witness_ledgers': [], 'files': {n: g.checksum(b) for n, b in result_files.items()},
                    'statistics': {'reported_classes': 1, 'tests': len(fixture_methods), 'skipped': 0, 'zero_case_classes': 0},
                    'report_classes': [test['class']]}
        worker_pre = {'rest': pre}
        if two_workers:
            other_pre = copy.deepcopy(pre)
            other_pre.update(captured_at='2026-10-14T10:00:02+00:00', elapsed_seconds=0.002)
            pre.update(captured_at='2026-10-14T10:00:01+00:00', elapsed_seconds=0.001)
            post.update(captured_at=pre['captured_at'], elapsed_seconds=pre['elapsed_seconds'])
            provenance['case_observer']['build_inputs_sha256'] = g.checksum(encode(pre))
            result_files[marker] = encode(provenance)
            manifest['files'][marker] = g.checksum(result_files[marker])
            worker_pre['engine'] = other_pre
        build_pre = copy.deepcopy(pre)
        if test_jar:
            build_pre['runner_context']['workflow_source']['job'] = 'build'
            build_pre['runner_context']['shard_id'] = 'build'
            build_pre['maven_execution_observation']['runner_context'] = copy.deepcopy(build_pre['runner_context'])
        equality = g.receipt.compare_input_snapshots(plan, build_pre, worker_pre)
        equality['input_files'] = {'build': g.checksum(encode(build_pre)), **{n: g.checksum(encode(v)) for n,v in worker_pre.items()}}
        artifacts = {'ci-shard-plan': {'shards.json': plan, 'ci-event-inputs.json': event},
                     'ci-inputs-rest': {'build-inputs.json': pre, 'post-test-inputs.json': post},
                     'ci-inputs-build': {'build-inputs.json': build_pre, 'build-input-equivalence.json': equality},
                     'ci-test-results-rest': {'rest/manifest.json': manifest,
                                             **{'rest/files/' + n: raw for n, raw in result_files.items()}},
                     'ci-aggregate': {xml_path: xml, 'target/ci-shards/rest/run.json': encode(provenance)}}
        if two_workers:
            other_post = copy.deepcopy(other_pre)
            other_post['phase'] = 'post-test-before-pack'
            other_provenance = copy.deepcopy(provenance)
            other_provenance['shard_id'] = 'engine'
            other_provenance['case_observer']['build_inputs_sha256'] = g.checksum(encode(other_pre))
            for phase in ['reset','verify']:
                other_provenance[phase]['shard_id'] = 'engine'
            other_marker = s.provenance_path(other_worker).as_posix()
            other_xml_path = xml_path.replace('SampleTest','OtherTest')
            other_xml = xml.replace(b'SampleTest',b'OtherTest')
            other_files = {other_compiled: b'other protocol class', other_xml_path: other_xml,
                           other_worker['required_exec'][0]: b'other coverage protocol', other_marker: encode(other_provenance), **(test_resource_bytes or {})}
            other_manifest = copy.deepcopy(manifest)
            other_manifest.update(shard_id='engine',tests=[other],provenance=other_provenance,
                                  provenance_path=other_marker,report_classes=[other['class']],
                                  files={n:g.checksum(raw) for n,raw in other_files.items()})
            artifacts['ci-inputs-engine'] = {'build-inputs.json':other_pre,'post-test-inputs.json':other_post}
            artifacts['ci-test-results-engine'] = {'engine/manifest.json':other_manifest,
                                                   **{'engine/files/'+n:raw for n,raw in other_files.items()}}
            artifacts['ci-aggregate'][other_xml_path] = other_xml
            artifacts['ci-aggregate']['target/ci-shards/engine/run.json'] = encode(other_provenance)
        artifacts = {name: copy.deepcopy(members) for name, members in artifacts.items()}
        records = [{'id': n, 'name': name, 'expired': False,
                    'workflow_run': {'id': run_id, 'head_sha': anchor, 'head_branch': scope['branch']}}
                   for n, name in enumerate(artifacts, 1)]
        originals = copy.deepcopy(artifacts)
        record_originals = copy.deepcopy(records)

        archive_extra_files = {}

        def archive():
            values = {'run.json': {'id': run_id, 'run_attempt': 1, 'head_sha': anchor, 'head_branch': scope['branch'],
                                  'event': scope['event'], 'repository': {'full_name': scope['repository']},
                                  'pull_requests': [{'number': 7, 'head': {'sha': anchor}, 'base': {'sha': base}}]},
                      'commit.json': commit(anchor), 'source.tar': v.git(work, 'archive', '--format=tar', anchor).stdout,
                      'checkout-commit.json': commit(merge), 'checkout-source.tar': v.git(work, 'archive', '--format=tar', merge).stdout,
                      'event-inputs.json': event, 'artifacts-1.json': {'total_count': len(records), 'artifacts': records}}
            if base_source:
                values.update({'base-commit.json': commit(base),
                    'base-source.tar': v.git(work, 'archive', '--format=tar', base).stdout})
            values.update(archive_extra_files)
            index = {}
            for record in records:
                name = str(record['id']) + '-' + record['name'] + '.zip'
                raw = zipped(artifacts[record['name']])
                values['artifacts/' + name] = raw
                with zipfile.ZipFile(io.BytesIO(raw)) as z:
                    index[name] = sorted(z.namelist())
            values['artifact-members.json'] = index
            directory = evidence / 'run'
            if directory.exists():
                for path in directory.rglob('*'):
                    if path.is_file() and path.name != 'receipt.json' and path.relative_to(directory).as_posix() not in values:
                        path.unlink()
            for name, value in values.items():
                save('run/' + name, value)
            return seal()

        def seal():
            directory = evidence / 'run'
            files = {p.relative_to(directory).as_posix(): {'size': p.stat().st_size, 'sha256': g.checksum(p.read_bytes())}
                     for p in directory.rglob('*') if p.is_file() and p.name != 'receipt.json'}
            return save('run/receipt.json', {'schema': 1, 'archive_root': str(directory), 'repository': scope['repository'],
                        'run_id': run_id, 'run_attempt': 1, 'event': scope['event'], 'source_sha': anchor,
                        'checkout_sha': merge, 'archive_complete': True, 'gaps': [],
                        'purpose': 'offline archive protocol fixture, not CI success or producer proof', 'files': files})

        source_request = {'schema': 1, 'type': 'source-projection-request', 'recipe': recipe_ref, 'archive': archive(), 'scope': scope}
        request = {'schema': 1, 'type': 'archive-inputs-request', 'source': save('source-request.json', source_request)}

        def project():
            return m.project(g.References(evidence), work, request)

        def reseal():
            source_request['archive'] = archive()
            request['source'] = save('source-request.json', source_request)

        def reset():
            artifacts.clear()
            artifacts.update(copy.deepcopy(originals))
            records[:] = copy.deepcopy(record_originals)
            reseal()

        if not only_callback:
            answer = project()
            check(answer['archived_input_members_verified'], 'original members are read and bound')
            check(answer['observed_production']['production_files'] == pre['production_files'], 'observed P comes from archived worker/aggregate inputs')
            check(answer['resolved_test_classpaths'] == pre['resolved_test_classpaths'], 'ordered D is read from actual snapshot members')
            check(len(answer['results']['rest']['xml_rows']) == 1, 'XML observations come from result ZIP')
            check(not answer['whole_input_projection_verified'] and not answer['whole_build_provenance_complete']
                  and not answer['first_complete_PR_sample'] and not answer['performance_accepted'], 'protocol readback cannot certify missing provenance/performance')
            check('production_P' in answer['unverified_components'] and 'D' in answer['unverified_components']
                  and 'source_derived_case_inventory' in answer['unverified_components'], 'hash-only and selection gaps remain unknown')
            before = v.text(work, 'rev-parse', 'HEAD'), v.git(work, 'write-tree').stdout
            check(not v.git(work, 'status', '--porcelain').stdout, 'readback leaves a clean worktree')

            def mutate(artifact, path, field, value):
                artifacts[artifact][path][field] = value
                reseal()

            # Post-test records cannot substitute a foreign or partial runner declaration.
            for context, reason in [(None, 'invalid input runner context'),
                    ({'repository': 'foreign/project', 'shard_id': 'rest',
                      'workflow_source': {'snapshot': 'unknown', 'source_bytes_verified': False}}, 'repository differs'),
                    ({'repository': None, 'shard_id': 'engine',
                      'workflow_source': {'snapshot': 'unknown', 'source_bytes_verified': False}}, 'shard differs'),
                    ({'repository': None, 'shard_id': 'rest',
                      'workflow_source': {'snapshot': 'unknown', 'source_bytes_verified': False}}, 'post-test runner context differs')]:
                artifacts['ci-inputs-rest']['post-test-inputs.json']['runner_context'] = context
                reseal(); rejected(project, reason); reset()
            mutate('ci-inputs-rest', 'build-inputs.json', 'source_sha', anchor)
            rejected(project, 'checkout/run/attempt differs')
            reset()
            mutate('ci-inputs-rest', 'build-inputs.json', 'run_attempt', '2')
            rejected(project, 'checkout/run/attempt differs')
            reset()
            mutate('ci-inputs-rest', 'build-inputs.json', 'event', 'workflow_dispatch')
            rejected(project, 'phase/event')
            reset()
            artifacts['ci-inputs-rest']['build-inputs.json']['source_files']['product.txt'] = 'f' * 64
            reseal()
            rejected(project, 'source hashes differ from actual checkout')
            reset()
            artifacts['ci-inputs-rest']['build-inputs.json']['production_files'][prod] = 'f' * 64
            artifacts['ci-inputs-rest']['post-test-inputs.json']['production_files'][prod] = 'f' * 64
            reseal()
            rejected(project, 'independent build input differs')
            reset()
            artifacts['ci-inputs-rest']['build-inputs.json']['dependency_repository_files'][dep] = 'f' * 64
            reseal()
            rejected(project, 'resolved dependency byte binding differs')
            reset()
            mutate('ci-inputs-build', 'build-input-equivalence.json', 'whole_build_provenance_complete', True)
            rejected(project, 'input comparator differs from replay')
            reset()
            records[0]['workflow_run']['head_sha'] = 'f' * 40
            reseal()
            rejected(project, 'artifact workflow source differs')
            reset()
            records.append(copy.deepcopy(records[-1]))
            reseal()
            rejected(project, 'duplicate artifact identity or name')
            reset()
            empty_xml = b'<testsuite name="io.tapstate.fixture.SampleTest" tests="0" skipped="0" errors="0" failures="0"/>'
            artifacts['ci-test-results-rest']['rest/files/' + xml_path] = empty_xml
            artifacts['ci-test-results-rest']['rest/manifest.json']['files'][xml_path] = g.checksum(empty_xml)
            artifacts['ci-test-results-rest']['rest/manifest.json']['statistics'].update(tests=0, zero_case_classes=1)
            zero_answer = None
            reseal()
            zero_answer = project()
            check(zero_answer['results']['rest']['xml_rows'] == [] and not zero_answer['first_complete_PR_sample'],
                  'valid original zero-case worker remains an empty, non-acceptance observation')
            reset()
            artifacts['ci-inputs-extra'] = {'build-inputs.json': copy.deepcopy(pre)}
            records.append({'id': 99, 'name': 'ci-inputs-extra', 'expired': False,
                            'workflow_run': {'id': 42, 'head_sha': anchor, 'head_branch': scope['branch']}})
            reseal()
            rejected(project, 'input artifact shard set differs')
            reset()
            artifacts['ci-test-results-extra'] = copy.deepcopy(artifacts['ci-test-results-rest'])
            records.append({'id': 99, 'name': 'ci-test-results-extra', 'expired': False,
                            'workflow_run': {'id': 42, 'head_sha': anchor, 'head_branch': scope['branch']}})
            reseal()
            rejected(project, 'result artifact shard set differs')
            reset()
            artifacts['ci-shard-plan']['shards.json']['shards'][0]['tests'] = []
            reseal()
            rejected(project, 'shard plan union differs')
            reset()
            artifacts['ci-shard-plan']['ci-event-inputs.json']['checkout_sha'] = anchor
            reseal()
            rejected(project, 'raw event artifact differs')
            reset()
            artifacts['ci-test-results-rest']['rest/files/' + xml_path] = xml.replace(b'name="holds"', b'name="different"')
            reseal()
            rejected(project, 'result file set/hash differs')
            reset()
            artifacts['ci-test-results-rest']['rest/manifest.json']['statistics']['tests'] = 2
            reseal()
            rejected(project, 'declared statistics differ')
            reset()
            artifacts['ci-test-results-rest']['rest/files/fixture/target/classes/Foreign.class'] = b'foreign production bytes'
            artifacts['ci-test-results-rest']['rest/manifest.json']['files']['fixture/target/classes/Foreign.class'] = g.checksum(b'foreign production bytes')
            reseal()
            rejected(project, 'unexpected result member')
            reset()
            artifacts['ci-test-results-rest']['rest/files/' + compiled] = b'foreign compiled test'
            artifacts['ci-test-results-rest']['rest/manifest.json']['files'][compiled] = g.checksum(b'foreign compiled test')
            reseal()
            rejected(project, 'test bytecode differs')
            reset()
            artifacts['ci-test-results-rest']['rest/manifest.json']['provenance']['case_observer']['build_inputs_sha256'] = 'f' * 64
            artifacts['ci-test-results-rest']['rest/files/' + marker] = encode(artifacts['ci-test-results-rest']['rest/manifest.json']['provenance'])
            artifacts['ci-test-results-rest']['rest/manifest.json']['files'][marker] = g.checksum(artifacts['ci-test-results-rest']['rest/files/' + marker])
            reseal()
            rejected(project, 'observer input binding differs')
            reset()
            artifacts['ci-test-results-rest']['rest/files/' + xml_path] = xml.replace(b'tests="1"', b'tests="2"')
            artifacts['ci-test-results-rest']['rest/manifest.json']['files'][xml_path] = g.checksum(artifacts['ci-test-results-rest']['rest/files/' + xml_path])
            reseal()
            rejected(project, 'XML counters/outcome differ')
            reset()
            # Resealed XML and manifest still change independently derived observations.
            changed = xml.replace(b'name="holds"', b'name="different"')
            artifacts['ci-test-results-rest']['rest/files/' + xml_path] = changed
            artifacts['ci-test-results-rest']['rest/manifest.json']['files'][xml_path] = g.checksum(changed)
            reseal()
            check(project()['results']['rest']['xml_rows'] != answer['results']['rest']['xml_rows'], 'caller declarations cannot conceal different actual XML identities')
            reset()
            rejected(lambda: m.zip_read(zipped({'../outside': b'a'})), 'escapes checkout')
            out = io.BytesIO()
            with zipfile.ZipFile(out, 'w') as z:
                info = zipfile.ZipInfo('link')
                info.external_attr = 0o120777 << 16
                z.writestr(info, b'outside')
            rejected(lambda: m.zip_read(out.getvalue()), 'special ZIP member')
            output = evidence / 'cli-result.json'
            manifest_path = evidence / 'request.json'
            manifest_path.write_bytes(encode(request))
            command = ['bash', str(Path(__file__).with_name('ci-archive-inputs.sh')), '--manifest', str(manifest_path),
                       '--evidence-root', str(evidence), '--root', str(work), '--output', str(output)]
            child = subprocess.run(command, capture_output=True, text=True)
            check(child.returncode == 0 and json.loads(output.read_bytes())['input_member_hashes'] == answer['input_member_hashes'], 'CLI reads actual archive members: ' + child.stderr)
            prior = output.read_bytes()
            again = subprocess.run(command, capture_output=True, text=True)
            check(again.returncode != 0 and output.read_bytes() == prior, 'output creation is exclusive')
            environment = dict(os.environ)
            try:
                os.environ.update(GIT_DIR=str(main / '.git'), GIT_INDEX_FILE=str(root / 'foreign-index'))
                check(project()['observed_production_digest'] == answer['observed_production_digest'], 'inherited Git state cannot change actual source')
            finally:
                os.environ.clear()
                os.environ.update(environment)
            check(before == (v.text(work, 'rev-parse', 'HEAD'), v.git(work, 'write-tree').stdout), 'projection preserves real ref/index')
            check(not v.git(work, 'status', '--porcelain').stdout, 'projection preserves the worktree')
        if callback is not None:
            callback(locals())
    if only_callback:
        print('CI archive input fixture prepared; callback owns its assertions')
    else:
        print('CI archive input controls passed: ' + str(count))


def packaged_archive_controls(context):
    artifacts, project = context['artifacts'], context['project']
    answer = project()
    fact = answer['observed_packaged_test_outputs']
    check(fact['original_owner_bytes_replayed'] and not fact['unverified'], 'complete original test-JAR owner bytes and native-shaped goal/scope replay')
    check(not fact['whole_build_provenance_complete'] and not answer['whole_build_provenance_complete'],
          'original archive protocol replay never becomes whole-build proof')
    output, request_path = context['evidence'] / 'test-jar-cli.json', context['evidence'] / 'test-jar-request.json'
    request_path.write_bytes(encode(context['request']))
    child = subprocess.run(['bash', str(Path(__file__).with_name('ci-archive-inputs.sh')), '--manifest', str(request_path),
                            '--evidence-root', str(context['evidence']), '--root', str(context['work']), '--output', str(output)],
                           capture_output=True, text=True)
    check(child.returncode == 0 and json.loads(output.read_bytes()) == answer, 'test-JAR original archive CLI: ' + child.stderr)
    support_path = 'fixture/target/test-classes/io/tapstate/fixture/Support.class'
    artifacts['ci-test-results-rest'].pop('rest/files/' + support_path)
    artifacts['ci-test-results-rest']['rest/manifest.json']['files'].pop(support_path)
    context['reseal']()
    rejected(project, 'test-JAR members differ from original owner ZIP bytes')
    context['reset']()
    path = 'fixture/target/fixture-1-tests.jar'
    for field, mutation, reason in [
        ('members', lambda x: x.pop('io/tapstate/fixture/Support.class'), 'test-JAR member binding differs'),
        ('output', lambda x: x.update(sha256='e' * 64), 'test-JAR model/output binding differs'),
        ('model', lambda x: x.update(classifier='foreign'), 'test-JAR model/output binding differs')]:
        packet = artifacts['ci-inputs-rest']['build-inputs.json']['packaged_test_outputs']
        mutation(packet['outputs'][path][field])
        context['reseal'](); rejected(project, reason); context['reset']()
    for key in ('pom_sources',):
        artifacts['ci-inputs-rest']['build-inputs.json']['packaged_test_outputs']['outputs'][path]['model'][key]['fixture/pom.xml'] = 'f' * 64
        context['reseal'](); rejected(project, 'test-JAR model/output binding differs'); context['reset']()
    artifacts['ci-inputs-rest']['build-inputs.json']['packaged_test_outputs']['producer_sources'][m.test_jars.PRODUCERS[0]] = 'f' * 64
    context['reseal'](); rejected(project, 'test-JAR capture helper source binding missing'); context['reset']()


if __name__ == '__main__':
    run()
    run(packaged_archive_controls, test_jar=True, only_callback=True)
    print('CI packaged test archive controls passed:', count)
