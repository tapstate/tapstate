#!/usr/bin/env python3
"""Read actual CI input/result ZIP members without certifying whole provenance.

The source recipe is replayed independently. Inputs are selected from the original
artifact API pages, never from caller-selected component documents. No archived
code is executed, no member is extracted, and no worktree or ref is changed.
"""
import argparse
from collections import Counter
import hashlib
import io
from pathlib import Path, PurePosixPath
import re
import stat
import sys
import tarfile
import xml.etree.ElementTree as ET
import zipfile

sys.dont_write_bytecode = True
import importlib.util
_spec = importlib.util.spec_from_file_location('archive_source', Path(__file__).with_name('_ci-source-projection.py'))
source = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(source)
g, require = source.groups, source.require
shards = g.module('archive_shards', '_ci-shards.py')
test_jars = g.module('archive_test_jars', '_ci-test-jar-inputs.py')
maven_artifacts = g.module('archived_maven_artifacts', '_ci-maven-artifacts.py')
MAX_MEMBER = 64 * 1024 * 1024
MAX_TOTAL = 512 * 1024 * 1024


def hash_map(value, label):
    require(isinstance(value, dict) and value, 'missing hash map: ' + label)
    for name, digest in value.items():
        source.variants.path_name(name)
        require(isinstance(digest, str) and re.fullmatch(r'[0-9a-f]{64}', digest), 'invalid hash: ' + label)
    return value


def zip_read(raw):
    """Read bounded, canonical regular files; retain every member's digest."""
    require(len(raw) <= MAX_TOTAL, 'ZIP exceeds archive budget')
    files, seen, total = {}, set(), 0
    with zipfile.ZipFile(io.BytesIO(raw)) as archive:
        require(len(archive.infolist()) <= 20000, 'too many ZIP members')
        for item in archive.infolist():
            name = item.filename.rstrip('/') if item.is_dir() else item.filename
            source.variants.path_name(name)
            require(name not in seen, 'duplicate ZIP member')
            seen.add(name)
            mode = item.external_attr >> 16
            require(not mode & 0o7000 and stat.S_IFMT(mode) in (0, stat.S_IFDIR if item.is_dir() else stat.S_IFREG),
                    'special ZIP member')
            require(not item.flag_bits & 1, 'encrypted ZIP member')
            require(0 <= item.file_size <= MAX_MEMBER, 'ZIP member exceeds budget')
            total += item.file_size
            require(total <= MAX_TOTAL, 'expanded ZIP exceeds budget')
            if item.is_dir():
                require(item.file_size == 0, 'ZIP directory has data')
                continue
            with archive.open(item) as stream:
                data = stream.read(MAX_MEMBER + 1)
                require(len(data) == item.file_size, 'ZIP member size differs')
            files[name] = data
    require(files, 'empty artifact ZIP')
    for name in files:
        parts = PurePosixPath(name).parts
        require(not any('/'.join(parts[:i]) in files for i in range(1, len(parts))), 'ZIP file is also a parent')
    return files, sorted(item.filename for item in archive.infolist())


class Archive:
    def __init__(self, refs, reference, scope):
        self.refs, self.scope = refs, scope
        self.receipt, self.path = refs.read(reference)
        self.members = {}
        self.index = self.json('artifact-members.json')
        pages = sorted((name for name in self.receipt['files'] if re.fullmatch(r'artifacts-[0-9]+\.json', name)),
                       key=lambda n: int(n.split('-')[1].split('.')[0]))
        require(pages == ['artifacts-' + str(n) + '.json' for n in range(1, len(pages) + 1)],
                'artifact API pages missing or non-contiguous')
        require(pages, 'artifact API pages unavailable')
        total, records = None, []
        for page in pages:
            value = self.json(page)
            require(isinstance(value, dict) and type(value.get('total_count')) is int
                    and value['total_count'] >= 0 and isinstance(value.get('artifacts'), list), 'invalid artifact API page')
            require(total is None or total == value['total_count'], 'artifact API totals differ')
            total = value['total_count']
            records.extend(value['artifacts'])
        require(len(records) == total, 'artifact API page set is incomplete')
        self.artifacts, ids = {}, set()
        for record in records:
            require(isinstance(record, dict) and type(record.get('id')) is int and record['id'] > 0,
                    'invalid artifact identity')
            name = record.get('name')
            require(isinstance(name, str) and re.fullmatch(r'[A-Za-z0-9_.-]+', name), 'invalid artifact name')
            require(record['id'] not in ids and name not in self.artifacts, 'duplicate artifact identity or name')
            ids.add(record['id'])
            run = record.get('workflow_run')
            require(isinstance(run, dict) and type(run.get('id')) is int and run['id'] == scope['run_id']
                    and run.get('head_sha') == scope['head_sha'] and run.get('head_branch') == scope['branch'],
                    'artifact workflow source differs')
            self.artifacts[name] = record
        archived = {name.removeprefix('artifacts/') for name in self.receipt['files'] if name.startswith('artifacts/')}
        require(set(self.index) == archived, 'artifact member index set differs')
        expected = {str(r['id']) + '-' + r['name'] + '.zip' for r in records if not r.get('expired')}
        require(archived == expected, 'artifact API and archived ZIP set differ')

    def raw(self, name):
        require(name in self.receipt['files'], 'mandatory archive input missing: ' + name)
        return self.refs.blob({'path': (self.path.parent / name).relative_to(self.refs.root).as_posix(),
                               'sha256': self.receipt['files'][name]['sha256']})[0]

    def json(self, name):
        return g.json_bytes(self.raw(name))

    def artifact(self, name):
        require(name in self.artifacts, 'mandatory artifact missing: ' + name)
        record = self.artifacts[name]
        require(record.get('expired') is False, 'artifact expired: ' + name)
        basename = str(record['id']) + '-' + name + '.zip'
        files, members = zip_read(self.raw('artifacts/' + basename))
        require(self.index[basename] == members, 'artifact member index differs: ' + name)
        self.members[name] = {n: {'sha256': g.checksum(data), 'size': len(data)} for n, data in sorted(files.items())}
        return files


def checkout_hashes(raw):
    hashes = {}
    with tarfile.open(fileobj=io.BytesIO(raw), mode='r:') as archive:
        for item in archive:
            if item.isfile():
                hashes[item.name] = g.checksum(archive.extractfile(item).read())
            elif item.issym():
                # Bind the Git link blob, never the file at its target.
                hashes[item.name] = g.checksum(item.linkname.encode())
    return hashes


def checkout_poms(raw):
    poms = {}
    with tarfile.open(fileobj=io.BytesIO(raw), mode='r:') as archive:
        for item in archive:
            if item.isfile() and (item.name == 'pom.xml' or item.name.endswith('/pom.xml') or item.name in (*test_jars.PRODUCERS, *maven_artifacts.PRODUCERS)):
                require(item.size <= 1024 * 1024, 'test-JAR source POM exceeds budget')
                poms[item.name] = archive.extractfile(item).read()
    return poms


def plan_read(value, checkout):
    require(type(value.get('schema')) is int and value['schema'] == 1 and value.get('source_sha') == checkout,
            'shard plan checkout differs')
    expected, workers = value['expected'], value['shards']
    require(isinstance(expected, list) and expected and isinstance(workers, list) and workers, 'empty shard plan')
    for test in expected:
        require(isinstance(test, dict) and set(test) in ({'module', 'kind', 'class'}, {'module', 'kind', 'class', 'example'}),
                'invalid planned test')
        source.variants.path_name(test['module'])
        require(test['kind'] in ('unit', 'it') and isinstance(test['class'], str)
                and re.fullmatch(r'[A-Za-z_$][\w$]*(?:\.[A-Za-z_$][\w$]*)*', test['class']), 'invalid planned class')
        if 'example' in test:
            source.variants.path_name(test['example'])
    require(value.get('cohort_hash') == shards.cohort(expected)
            and len(set(shards.key(t) for t in expected)) == len(expected), 'invalid planned cohort')
    ids = [worker['id'] for worker in workers]
    require(all(isinstance(n, str) and re.fullmatch(r'[a-z0-9]+(?:-[a-z0-9]+)*', n) for n in ids)
            and len(ids) == len(set(ids)) and 'build' not in ids, 'invalid planned shard IDs')
    require(Counter(shards.key(t) for worker in workers for t in worker['tests']) == Counter(shards.key(t) for t in expected),
            'shard plan union differs')
    require(all(worker['required_exec'] == shards.execs(worker['tests']) for worker in workers), 'planned execs differ')
    return value


def snapshot(value, scope, checkout, tree, sources, phase, poms=None):
    require(isinstance(value, dict) and type(value.get('schema')) is int and value['schema'] == 1
            and value.get('phase') == phase and value.get('event') == scope['event'], 'invalid input snapshot phase/event')
    require(g.origin(value) == {'source_sha': checkout, 'run_id': scope['run_id'], 'run_attempt': scope['run_attempt']}
            and value.get('source_tree') == tree, 'input snapshot checkout/run/attempt differs')
    require(hash_map(value.get('source_files'), 'source_files') == sources, 'input source hashes differ from actual checkout')
    for field in ('production_files', 'test_files', 'dependency_repository_files'):
        hash_map(value.get(field), field)
    for field, files in [('production_class_ids', 'production_files'), ('test_class_ids', 'test_files')]:
        ids = value.get(field)
        require(isinstance(ids, dict) and set(ids) == {p for p in value[files] if p.endswith('.class')}
                and all(isinstance(v, str) and re.fullmatch(r'[0-9a-f]{16}', v) for v in ids.values()),
                'class ID map differs: ' + field)
    g.receipt.test_resource_snapshot(value)
    if 'packaged_test_outputs' in value:
        require(poms is not None, 'original test-JAR POM bytes unavailable')
        test_jars.validate(value['packaged_test_outputs'], value, lambda name: poms[name])
    paths = value.get('resolved_test_classpaths')
    require(isinstance(paths, dict) and paths, 'resolved classpaths unavailable')
    for module, entries in paths.items():
        source.variants.path_name(module)
        require(isinstance(entries, list), 'invalid resolved classpath')
        for entry in entries:
            require(isinstance(entry, dict) and type(entry.get('size')) is int and entry['size'] >= 0,
                    'invalid resolved classpath entry')
            identity = entry['identity']
            source.variants.path_name(identity)
            category, _, name = identity.partition('/')
            require(category in ('checkout', 'repository') and name, 'invalid classpath identity')
            require(isinstance(entry.get('sha256'), str) and re.fullmatch(r'[0-9a-f]{64}', entry['sha256']),
                    'invalid classpath byte digest')
            if category == 'repository' and not name.startswith('io/tapstate/'):
                require(value['dependency_repository_files'].get(name) == entry['sha256'], 'resolved dependency byte binding differs')
            if category == 'checkout':
                require(value['packaged_outputs'].get(name, {}).get('sha256') == entry['sha256'],
                        'resolved checkout output binding differs')
    return value


def result_read(files, worker, plan, binding, pre_raw, post):
    prefix = worker['id'] + '/'
    require(all(n.startswith(prefix) for n in files), 'result ZIP contains foreign shard')
    manifest = g.json_bytes(files[prefix + 'manifest.json'])
    declared = hash_map(manifest['files'], 'result files')
    actual = {n.removeprefix(prefix + 'files/'): g.checksum(data)
              for n, data in files.items() if n.startswith(prefix + 'files/')}
    require(actual == declared and set(files) == {prefix + 'manifest.json'} | {prefix + 'files/' + n for n in declared},
            'result file set/hash differs')
    require(type(manifest.get('schema')) is int and manifest['schema'] == 2
            and all(manifest.get(k) == v for k, v in {'source_sha': plan['source_sha'], 'cohort_hash': plan['cohort_hash'],
                                                   'shard_id': worker['id'], 'tests': worker['tests'],
                                                   'required_exec': worker['required_exec']}.items()), 'result manifest plan differs')
    provenance = manifest['provenance']
    original_identity = shards.execution_identity
    try:
        shards.execution_identity = lambda: binding
        shards.validate_provenance(provenance, plan, worker)
    finally:
        shards.execution_identity = original_identity
    marker = shards.provenance_path(worker).as_posix()
    require(manifest['provenance_path'] == marker
            and g.json_bytes(files[prefix + 'files/' + marker]) == provenance, 'raw result provenance differs')
    require(provenance['case_observer'].get('enabled') is True
            and provenance['case_observer'].get('build_inputs_sha256') == g.checksum(pre_raw),
            'result observer input binding differs')
    ledgers = manifest['witness_ledgers']
    require(isinstance(ledgers, list) and ledgers == sorted(set(ledgers))
            and all(isinstance(n, str) and re.fullmatch(r'e2e/target/(?:witness-ledger\.txt|witness-ledgers/[0-9]+\.txt)', n)
                    for n in ledgers)
            and (not ledgers or 'e2e' in {t['module'] for t in worker['tests']}), 'invalid witness ledger paths')
    reports, rows, execs, zero = [], [], set(), 0
    modules = {t['module'] for t in worker['tests']}
    for name, digest in declared.items():
        data = files[prefix + 'files/' + name]
        if name == marker or name in manifest['witness_ledgers']:
            continue
        match = re.fullmatch(r'(.+)/target/(?:(test-classes/.+)|((?:surefire|failsafe)-reports)/TEST-([^/]+)\.xml|(jacoco(?:-it)?\.exec))', name)
        require(match and match[1] in modules, 'unexpected result member')
        if match[2]:
            if name.endswith('.class'):
                require(post['test_files'].get(name) == digest
                        and post['test_class_ids'].get(name) == g.receipt.crc64(data),
                        'result test bytecode differs from post-test input')
            elif 'test_resource_files' in post:
                require(post['test_resource_files'].get(name)==digest, 'result test resource differs from post-test input')
        elif match[5]:
            require(data, 'empty result exec')
            execs.add(name)
        else:
            require(b'<!DOCTYPE' not in data and b'<!ENTITY' not in data, 'unsupported XML declarations')
            suite = ET.fromstring(data)
            cases = suite.findall('testcase')
            require(suite.tag == 'testsuite' and suite.get('name') == match[4]
                    and int(suite.get('tests', '-1')) == len(cases)
                    and int(suite.get('skipped', '0')) == sum(c.find('skipped') is not None for c in cases)
                    and int(suite.get('errors', '0')) == int(suite.get('failures', '0')) == 0
                    and suite.find('.//failure') is None and suite.find('.//error') is None, 'result XML counters/outcome differ')
            kind = 'unit' if match[3] == 'surefire-reports' else 'it'
            reports.append((match[1], kind, suite.get('name')))
            zero += int(not cases)
            for case in cases:
                require(case.get('classname', suite.get('name')).split('$', 1)[0] == suite.get('name').split('$', 1)[0],
                        'result XML testcase family differs')
                skipped = case.find('skipped')
                rows.append({'module': match[1], 'kind': kind, 'class': case.get('classname', suite.get('name')),
                             'name': case.get('name'), 'skipped': None if skipped is None else
                             {'type': skipped.get('type', ''), 'message': skipped.get('message', ''), 'text': skipped.text or ''}})
    require(len(reports) == len(set(reports)) and {(m, k, c.split('$', 1)[0]) for m, k, c in reports}
            == {shards.class_key(t) for t in worker['tests']}, 'result report class set differs')
    require(execs == set(worker['required_exec']), 'result coverage member set differs')
    require(manifest['statistics'] == {'reported_classes': len(reports), 'tests': len(rows),
            'skipped': sum(r['skipped'] is not None for r in rows), 'zero_case_classes': zero}
            and manifest['report_classes'] == sorted(c for m, k, c in reports), 'result declared statistics differ')
    require(all(t['module'] + '/target/test-classes/' + t['class'].replace('.', '/') + '.class' in declared
                for t in worker['tests']), 'result compiled selected class missing')
    if 'test_resource_files' in post:
        resource_set={name:checksum for name,checksum in post['test_resource_files'].items()
                      if any(name.startswith(module+'/target/test-classes/') for module in modules)}
        actual_resources={name:checksum for name,checksum in declared.items()
                          if any(name.startswith(module+'/target/test-classes/') for module in modules) and not name.endswith('.class')}
        require(resource_set==actual_resources, 'result test resource set differs from post-test inputs')
    return {'manifest_sha256': g.checksum(files[prefix + 'manifest.json']), 'files': declared,
            'xml_rows': sorted(rows, key=lambda v: g.canonical(v)),
            'statistics': manifest['statistics'], 'test_execution_proven': False,
            'raw_witness_ledgers': {n: declared[n] for n in manifest['witness_ledgers']}}


def test_jar_readback(plan, pre, post, results, poms):
    if not any('packaged_test_outputs' in value for value in pre.values()):
        return None
    require(all('packaged_test_outputs' in value for value in [*pre.values(), *post.values()]),
            'test-JAR capture missing from an original peer')
    packet = pre['build']['packaged_test_outputs']
    missing, outputs = [], {}
    for path, record in packet['outputs'].items():
        model = record['model']
        runtime_inputs = []
        for name, snapshot in [*pre.items(), *[(name + '/post', value) for name, value in post.items()]]:
            require(snapshot['packaged_test_outputs'] == packet, 'original test-JAR capture differs across peers/phases')
            runtime = test_jars.execution_inputs(snapshot, model)
            if runtime is None:
                missing.append(name + ':' + path + ': original goal not observed')
            else:
                runtime_inputs.append(runtime)
        if runtime_inputs:
            require(all(value == runtime_inputs[0] for value in runtime_inputs), 'test-JAR producer runtime bytes differ')
        outputs[path] = {**record, 'producer_runtime_inputs': runtime_inputs[0] if runtime_inputs else None}
        owners = [worker['id'] for worker in plan['shards']
                  if model['module'] in {test['module'] for test in worker['tests']}]
        if not owners:
            missing.append(path + ': no archived test byte owner')
        prefix = model['module'] + '/target/test-classes/'
        for owner in owners:
            members = {name.removeprefix(prefix): checksum for name, checksum in results[owner]['files'].items()
                       if name.startswith(prefix)}
            require(members == record['members'], 'test-JAR members differ from original owner ZIP bytes')
        for name, snapshot in pre.items():
            for consumer, entries in snapshot['resolved_test_classpaths'].items():
                for entry in entries:
                    if test_jars.classpath_role(entry, {path: record}) is not None:
                        if not test_jars.observed_test_scope(snapshot, consumer, record, lambda name: poms[name]):
                            missing.append(name + ':' + path + ': original consumer scope not observed')
    return {'schema': 1, 'outputs': outputs, 'producer_sources': packet['producer_sources'],
            'original_owner_bytes_replayed': not missing, 'unverified': sorted(set(missing)),
            'whole_build_provenance_complete': False}


def project(refs, root, request):
    require(isinstance(request, dict) and set(request) == {'schema', 'type', 'source'}
            and type(request['schema']) is int and request['schema'] == 1
            and request['type'] == 'archive-inputs-request', 'invalid archive input request')
    source_request, unused = refs.read(request['source'])
    basis = source.project(refs, root, source_request)
    scope = basis['scope']
    archive = Archive(refs, source_request['archive'], scope)
    require(basis['checkout'] is not None, 'actual checkout archive required')
    checkout, tree = basis['checkout']['sha'], basis['checkout']['tree']
    sources = checkout_hashes(archive.raw('checkout-source.tar'))
    poms = checkout_poms(archive.raw('checkout-source.tar'))
    plan_files = archive.artifact('ci-shard-plan')
    require(set(plan_files) == {'shards.json', 'ci-event-inputs.json'}, 'shard plan artifact members differ')
    require(g.json_bytes(plan_files['ci-event-inputs.json']) == archive.json('event-inputs.json'), 'raw event artifact differs')
    plan = plan_read(g.json_bytes(plan_files['shards.json']), checkout)
    worker_names = {w['id'] for w in plan['shards']}
    require({n for n in archive.artifacts if n.startswith('ci-inputs-')}
            == {'ci-inputs-' + n for n in worker_names | {'build'}}, 'input artifact shard set differs')
    require({n for n in archive.artifacts if n.startswith('ci-test-results-')}
            == {'ci-test-results-' + n for n in worker_names}, 'result artifact shard set differs')
    pre, post, raw, results = {}, {}, {}, {}
    provider_members, result_payloads = {}, {}
    for name in ['build', *[w['id'] for w in plan['shards']]]:
        inputs = archive.artifact('ci-inputs-' + name)
        if name != 'build':
            provider_members[name] = inputs
        raw[name] = inputs['build-inputs.json']
        pre[name] = snapshot(g.json_bytes(raw[name]), scope, checkout, tree, sources, 'pre-test-after-install', poms)
        g.receipt.input_runner_context(pre[name], name, scope['repository'])
        g.receipt.maven_observation_fact(pre[name], name, scope['repository'])
        if 'resolved_maven_artifacts' in pre[name]:
            maven_artifacts.validate(pre[name]['resolved_maven_artifacts'], pre[name], lambda path: poms[path])
        if name != 'build':
            post[name] = snapshot(g.json_bytes(inputs['post-test-inputs.json']), scope, checkout, tree, sources,
                                  'post-test-before-pack', poms)
            g.receipt.input_runner_context(post[name], name, scope['repository'])
            g.receipt.maven_observation_fact(post[name], name, scope['repository'])
            if 'resolved_maven_artifacts' in post[name]:
                maven_artifacts.validate(post[name]['resolved_maven_artifacts'], post[name], lambda path: poms[path])
            require(pre[name].get('resolved_maven_artifacts') == post[name].get('resolved_maven_artifacts'),
                    'post-test resolved Maven artifact capture differs: ' + name)
            require(pre[name].get('maven_execution_observation') == post[name].get('maven_execution_observation'),
                    'post-test Maven observation differs: ' + name)
            require(g.receipt.test_resource_snapshot(pre[name])==g.receipt.test_resource_snapshot(post[name]),
                    'post-test resource hashes differ from prepared inputs')
            require(pre[name].get('packaged_test_outputs') == post[name].get('packaged_test_outputs'),
                    'post-test test-JAR capture differs: ' + name)
            require(pre[name].get('runner_context') == post[name].get('runner_context'),
                    'post-test runner context differs: ' + name)
            for field in ('production_files', 'production_class_ids', 'resolved_test_classpaths', 'toolchain'):
                require(pre[name][field] == post[name][field], 'post-test production/dependency/toolchain differs: ' + field)
    equality = g.receipt.compare_input_snapshots(plan, pre['build'], {n: v for n, v in pre.items() if n != 'build'})
    input_hashes = {n: g.checksum(data) for n, data in raw.items()}
    build_files = archive.artifact('ci-inputs-build')
    comparison = g.json_bytes(build_files['build-input-equivalence.json'])
    require(comparison == {**equality, 'input_files': input_hashes}, 'archived input comparator differs from replay')
    binding = {'execution': 'github-actions', 'run_id': str(scope['run_id']), 'run_attempt': scope['run_attempt']}
    for worker in plan['shards']:
        name = worker['id']
        result_payloads[name] = archive.artifact('ci-test-results-' + name)
        results[name] = result_read(result_payloads[name], worker, plan, binding, raw[name], post[name])
    test_jar_facts = test_jar_readback(plan, pre, post, results, poms)
    provider = g.module('provider_archive_readback', '_ci-provider-readback.py').project(
        archive, plan, pre, post, provider_members, result_payloads, sources, archive.raw('checkout-source.tar'), zip_read)
    aggregate = archive.artifact('ci-aggregate')
    # The aggregate is retained in the readback chain, but merged report semantics
    # and complete provider identities still require their dedicated comparator.
    observed = pre['build']
    production = {k: observed[k] for k in ('production_files', 'production_class_ids', 'production_class_access_flags')}
    dependency = observed['resolved_test_classpaths']
    missing = set(basis['unverified_components']) | {'effective_build_model', 'generator_processor_realms',
               'actual_dependency_bytes_readback', 'production_byte_readback', 'source_derived_case_inventory', 'resolved_provider_replay',
               'aggregate_report_replay', 'R', 'G_v', 'runtime_spec'}
    if provider is not None:
        missing.add('provider_raw_result_readback')
        if any(not value['prepared_resource_hashes_complete'] for value in provider.values()):
            missing.add('provider_prepared_resource_hashes')
        if any(not value['helper_class_bytes_read_back'] for value in provider.values()):
            missing.add('provider_helper_byte_readback')
        if any(value['provider_class_bytes_read_back'] is False for value in provider.values()):
            missing.add('provider_class_byte_readback')
        if any(not value['workflow_source_is_archived_checkout'] for value in provider.values()):
            missing.add('provider_workflow_source_readback')
    result = {'schema': 1, 'type': 'archived-input-readback', 'scope': scope, 'checkout': basis['checkout'],
            'source_basis': {'preserved_source_digest': basis['preserved_source_digest'],
                             'owned_recipe_digest': basis['owned_recipe_digest']},
            'observed_production': production, 'observed_production_digest': g.checksum(g.canonical(production)),
            'resolved_test_classpaths': dependency, 'resolved_classpath_digest': g.checksum(g.canonical(dependency)),
            'observed_toolchain': observed['toolchain'], 'plan': plan,
            'independent_input_replay': equality, 'input_member_hashes': input_hashes,
            'results': results, 'aggregate_member_count': len(aggregate), 'artifact_members': archive.members,
            'archived_input_members_verified': True, 'whole_build_provenance_complete': False,
            'whole_input_projection_verified': False, 'first_complete_PR_sample': False, 'performance_accepted': False,
            'unverified_components': sorted(missing), 'input_files': refs.finish()}
    if test_jar_facts is not None:
        result['observed_packaged_test_outputs'] = test_jar_facts
    if 'resolved_maven_artifacts' in observed:
        result['observed_resolved_maven_artifacts'] = {name: value['resolved_maven_artifacts'] for name, value in pre.items()}
    if 'test_resource_files' in observed:
        result['observed_test_resource_files']=observed['test_resource_files']
    if provider is not None:
        result['provider_captures'] = provider
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for field in ('manifest', 'evidence-root', 'root', 'output'):
        parser.add_argument('--' + field, required=True)
    args = parser.parse_args()
    answer = project(g.References(args.evidence_root), args.root, g.load(Path(args.manifest)))
    g.write_new(Path(args.output), answer)
    print('archived CI input members read back; whole provenance and PR performance remain unverified')


if __name__ == '__main__':
    try:
        main()
    except (ValueError, OSError, KeyError, TypeError, tarfile.TarError, zipfile.BadZipFile, ET.ParseError) as error:
        print('archive input projection rejected: ' + str(error), file=sys.stderr)
        sys.exit(1)
