#!/usr/bin/env python3
"""Explicitly reproduce captured provider identities from hash-matched local bytes.

This active command is separate from passive archive readers. It stages recorded
inputs privately, runs the fixed bounded provider helper and compares case multisets.
Historical runtime equivalence, original assertions and whole CI remain unproven.
"""
import argparse
from collections import Counter
from datetime import datetime, timezone
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import zipfile

sys.dont_write_bytecode = True
HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location('replay_inputs', HERE / '_ci-archive-inputs.py')
inputs_reader = importlib.util.module_from_spec(spec)
spec.loader.exec_module(inputs_reader)
g, require, ev = inputs_reader.g, inputs_reader.require, inputs_reader.g.evidence
variants = inputs_reader.source.variants
owned = g.module('provider_owned_process', '_ci-pr-work.py')
MAX_STAGE_BYTES = 2 * 1024 * 1024 * 1024
MAX_RESULT_BYTES = 64 * 1024 * 1024
MAX_LOG_BYTES = 8 * 1024 * 1024
HELPER_PREFIX = 'test-support/target/test-classes/'


def source_file(root, name):
    variants.path_name(name)
    path = root
    for part in Path(name).parts:
        path /= part
        require(not path.is_symlink(), 'symlink local replay input')
    require(path.is_file(), 'local replay input is missing: ' + name)
    return path


def stage_file(base, name, checksum, destination, staged, budget, size=None):
    source = source_file(base, name)
    require(isinstance(checksum, str) and len(checksum) == 64, 'invalid replay input digest')
    target = destination / name
    if target in staged:
        require(staged[target] == checksum and (size is None or target.stat().st_size == size), 'inconsistent repeated replay input')
        return
    require(source.stat().st_size <= MAX_STAGE_BYTES - budget[0], 'provider replay stage exceeds byte budget')
    target.parent.mkdir(parents=True, exist_ok=True)
    total, digest = 0, hashlib.sha256()
    with source.open('rb') as incoming, target.open('xb') as outgoing:
        while block := incoming.read(1024 * 1024):
            total += len(block)
            require(total <= MAX_STAGE_BYTES - budget[0], 'provider replay input grew beyond stage budget')
            outgoing.write(block)
            digest.update(block)
    require(digest.hexdigest() == checksum and (size is None or total == size), 'local replay byte hash/size differs: ' + name)
    target.chmod(0o400)
    staged[target] = checksum
    budget[0] += total


def stage_inputs(root, repository, before, capture, stage):
    require('test_resource_files' in before, 'provider replay requires recorded prepared test resources')
    g.receipt.test_resource_snapshot(before)
    checkout, dependencies = stage / 'checkout', stage / 'repository'
    checkout.mkdir(); dependencies.mkdir()
    staged, budget = {}, [0]
    for field in ('source_files', 'production_files', 'test_files', 'test_resource_files'):
        for name, checksum in sorted(before[field].items()):
            stage_file(root, name, checksum, checkout, staged, budget)
    for name, checksum in capture['resource_hashes'].items():
        stage_file(root, name, checksum, checkout, staged, budget)
    for entries in before['resolved_test_classpaths'].values():
        for entry in entries:
            category, name = entry['identity'].split('/', 1)
            require(category in ('checkout', 'repository'), 'foreign replay classpath identity')
            stage_file(root if category == 'checkout' else repository, name, entry['sha256'],
                       checkout if category == 'checkout' else dependencies, staged, budget, entry['size'])
    for field, checksum_field in (('production_class_ids', 'production_files'), ('test_class_ids', 'test_files')):
        for name, identity in before[field].items():
            require((checkout/name).stat().st_size <= 32*1024*1024, 'replay class metadata exceeds byte budget')
            raw = (checkout / name).read_bytes()
            require(g.receipt.crc64(raw) == identity, 'local replay class ID differs: ' + name)
            require(g.checksum(raw) == before[checksum_field][name], 'local replay class bytes changed')
    return checkout, dependencies, staged, budget[0]


def unchanged(staged, checkout, dependencies):
    actual = {path for directory in (checkout, dependencies) for path in g.receipt.safe_files(directory)}
    require(actual == set(staged), 'provider replay changed staged file membership')
    require(all(g.receipt.digest(path) == checksum for path, checksum in staged.items()), 'provider replay changed staged bytes')


def helper_jar(checkout, capture, stage):
    target = stage / 'provider-helper.jar'
    with zipfile.ZipFile(target, 'x') as archive:
        for name, checksum in sorted(capture['helper_class_hashes'].items()):
            require(name.startswith(HELPER_PREFIX) and name.endswith('.class'), 'invalid replay helper class path')
            path = checkout / name
            require(g.receipt.digest(path) == checksum, 'replay helper byte hash differs')
            expected = name.removeprefix(HELPER_PREFIX).removesuffix('.class')
            require(g.receipt.class_structure(path.read_bytes())['name'] == expected, 'replay helper binary identity differs')
            archive.write(path, name.removeprefix(HELPER_PREFIX))
    target.chmod(0o400)
    return target


def bounded_command(command, cwd, directory, timeout=120):
    require(0 < timeout <= 120, 'invalid provider replay timeout')
    stdout, stderr = directory / 'stdout', directory / 'stderr'
    for path in (stdout, stderr):
        descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        os.close(descriptor)
    environment = {'PATH': os.defpath, 'LANG': 'C.UTF-8', 'LC_ALL': 'C.UTF-8', 'TZ': 'UTC',
                   'PYTHONDONTWRITEBYTECODE': '1'}
    wrapped = [sys.executable, str(HERE/'_ci-provider-command.py'), str(stdout), str(stderr),
               str(MAX_LOG_BYTES), str(MAX_RESULT_BYTES), '--', *command]
    process, interrupted, prior, completed = None, [], {}, False
    started = time.monotonic_ns()
    start_utc = datetime.now(timezone.utc).isoformat()
    try:
        for number in (signal.SIGINT, signal.SIGTERM):
            prior[number] = signal.signal(number, lambda number, frame: interrupted.append(number))
        process = owned.OwnedCommand(wrapped, cwd=cwd, env=environment)
        deadline = time.monotonic() + timeout
        while process.poll() is None:
            require(not interrupted, 'provider replay interrupted')
            require(time.monotonic() < deadline, 'bounded provider replay timed out')
            require(all(path.stat().st_size <= MAX_LOG_BYTES for path in (stdout, stderr)), 'provider replay log budget exceeded')
            result = directory / 'result.jsonl'
            require(not result.exists() or result.stat().st_size <= MAX_RESULT_BYTES, 'provider replay result budget exceeded')
            try:
                process.wait(timeout=min(0.05, max(0.001, deadline - time.monotonic())))
            except subprocess.TimeoutExpired:
                pass
        require(process.returncode != 122, 'provider replay log budget exceeded')
        require(not interrupted and process.returncode == 0, 'bounded provider replay process failed or interrupted')
        completed = True
    finally:
        try:
            if process is not None:
                try:
                    if not completed and not getattr(process, 'stopped', False):
                        owned.stop_owned(process, graces=(0.2, 0.2))
                finally:
                    process.close()
        finally:
            for number, handler in prior.items():
                signal.signal(number, handler)
    require(not interrupted, 'provider replay interrupted during cleanup')
    require(all(path.stat().st_size <= MAX_LOG_BYTES for path in (stdout, stderr)), 'provider replay log budget exceeded')
    return {'exit_code': 0, 'elapsed_ns': time.monotonic_ns() - started, 'started_at': start_utc,
            'ended_at': datetime.now(timezone.utc).isoformat(), 'stdout_sha256': g.receipt.digest(stdout),
            'stderr_sha256': g.receipt.digest(stderr), 'command_sha256': g.checksum(g.canonical(command)),
            'termination_scope': 'owned-held-process-group', 'inherited_environment': False}


def result_methods(raw, call, capture, before):
    require(len(raw) <= MAX_RESULT_BYTES, 'provider replay result exceeds metadata budget')
    wanted = {(row['class'], row['method']): (row['provider'], row['provider_kind']) for row in call['requested_methods']}
    original = {(row['class'], row['method']): row for row in capture['resolved_methods'] if row['module'] == call['module']}
    rows, found, counts = [], set(), {}
    for line in raw.decode('utf-8').splitlines():
        row = json.loads(line, object_pairs_hook=ev.provider_record)
        ev.validate_provider_record(row, wanted, before, call['module'])
        key = row['class'], row['test_method']
        require(key not in found and key in original, 'duplicate or foreign replay result')
        found.add(key)
        method = original[key]
        if method['mapping'] == 'dynamic-marker':
            require(row['kind'] == 'dynamic', 'replay dynamic projection differs')
            parameters = [case['display_name_digest'] for case in row['cases']]
        elif method['mapping'] == 'named-carrier-marker':
            require(row['class'] == 'io.tapstate.e2e.JoinConformanceIT' and row['provider'] == 'carriers', 'unreviewed replay carrier projection')
            require(all(case['first_string_digest'] and len(case['argument_descriptors']) == 2
                        and case['argument_descriptors'][0]['stable'] for case in row['cases']), 'replay carrier lacks stable source identity')
            parameters = [case['first_string_digest'] for case in row['cases']]
        else:
            require(method['mapping'] == 'argument-marker' and row['kind'] == 'source'
                    and all(case['stable'] for case in row['cases']), 'replay arguments lack stable identities')
            parameters = [case['argument_digest'] for case in row['cases']]
        require(len(parameters) == len(set(parameters)), 'duplicate provider replay case')
        digests = [ev.case_hash({**{field: method[field] for field in ('lane', 'module', 'kind', 'class', 'method')},
                                'parameter': parameter}) for parameter in parameters]
        require(Counter(digests) == Counter(method['case_digests']), 'replayed provider case multiset differs')
        counts[key] = len(digests)
        rows.append(row)
    require(found == set(wanted) == set(original), 'provider replay method set is incomplete')
    return {'method_count': len(found), 'case_count': sum(counts.values()),
            'result_sha256': g.checksum(raw), 'result_records_sha256': g.checksum(g.canonical(rows))}


def replay(refs, root, repository, request, java='java'):
    require(isinstance(request, dict) and set(request) == {'schema', 'type', 'archive_inputs', 'worker'}
            and type(request['schema']) is int and request['schema'] == 1 and request['type'] == 'provider-replay-request', 'invalid provider replay request')
    reader_names=['_ci-provider-replay.py','_ci-archive-inputs.py','_ci-archive-cases.py','_ci-provider-readback.py','_ci-pr-groups.py',
                  '_ci-receipt.py','_ci-evidence.py','_ci-source-projection.py','_ci-variants.py','_ci-shards.py',
                  '_ci-pr-work.py','_ci-owned-command.py','_ci-provider-command.py']
    reader_hashes={name:g.receipt.digest(HERE/name) for name in reader_names}
    input_request, unused = refs.read(request['archive_inputs'])
    basis = inputs_reader.project(refs, root, input_request)
    worker = request['worker']
    require(isinstance(worker, str) and worker in basis.get('provider_captures', {}), 'provider replay capture unavailable for worker')
    require(basis['provider_captures'][worker]['prepared_resource_hashes_complete'], 'provider replay prepared resource bindings are incomplete')
    source_request, unused = refs.read(input_request['source'])
    archive = inputs_reader.Archive(refs, source_request['archive'], basis['scope'])
    members = archive.artifact('ci-inputs-' + worker)
    before = g.json_bytes(members['build-inputs.json'])
    capture = g.json_bytes(members['provider-probe-receipt.json'])
    require(capture['invocations'], 'no original provider invocations to replay')
    root, repository = Path(root).resolve(strict=True), Path(repository).resolve(strict=True)
    require(repository.is_dir() and not repository.is_relative_to(root) and not root.is_relative_to(repository), 'replay dependency repository must be separate')
    require(variants.text(root, 'rev-parse', 'HEAD^{tree}') == before['source_tree']
            and not variants.text(root, 'status', '--porcelain', '--untracked-files=no'), 'local replay source tree differs or is dirty')
    local_sha = variants.text(root, 'rev-parse', 'HEAD')
    executable = shutil.which(java)
    require(executable is not None, 'provider replay Java executable unavailable')
    executable = Path(executable).resolve(strict=True)
    require(executable.is_file() and os.access(executable, os.X_OK), 'provider replay Java executable is not executable')
    executable_sha = g.receipt.digest(executable)
    refs.finish()
    calls = []
    with tempfile.TemporaryDirectory(prefix='ci-provider-replay-') as temporary:
        stage = Path(temporary).resolve()
        checkout, dependencies, staged, byte_count = stage_inputs(root, repository, before, capture, stage)
        helper = helper_jar(checkout, capture, stage)
        helper_sha = g.receipt.digest(helper)
        for index, call in enumerate(capture['invocations']):
            directory = stage / ('call-' + str(index)); directory.mkdir()
            requests, result = directory / 'requests.tsv', directory / 'result.jsonl'
            requests.write_text(''.join('\t'.join([row['class'], row['method'], row['provider'], row['provider_kind']]) + '\n'
                                        for row in call['requested_methods']))
            require(g.receipt.digest(requests) == call['request_sha256'], 'provider replay request bytes differ')
            module = call['module']
            classpath = [str(helper), str(checkout / module / 'target/test-classes'), str(checkout / module / 'target/classes')]
            for entry in before['resolved_test_classpaths'][module]:
                category, name = entry['identity'].split('/', 1)
                classpath.append(str((checkout if category == 'checkout' else dependencies) / name))
            command = [str(executable), '-Xmx256m', '-XX:ActiveProcessorCount=1', '-Duser.timezone=UTC',
                       '-Djava.io.tmpdir=' + str(directory), '-cp', os.pathsep.join(classpath),
                       'io.tapstate.testsupport.CiProviderInventory', str(requests), str(result)]
            observed = bounded_command(command, checkout / module, directory)
            require(result.is_file() and not result.is_symlink() and result.stat().st_size <= MAX_RESULT_BYTES, 'provider replay result missing or unsafe')
            checked = result_methods(result.read_bytes(), call, capture, before)
            calls.append({'module': module, **observed, **checked,
                          'original_result_records_hash_equal': checked['result_records_sha256'] == call['result_records_sha256']})
            unchanged(staged, checkout, dependencies)
            require(g.receipt.digest(helper) == helper_sha and g.receipt.digest(executable) == executable_sha, 'replay helper or executable changed')
        refs.finish()
    require(all(g.receipt.digest(HERE/name)==checksum for name,checksum in reader_hashes.items()), 'provider replay reader code changed')
    return {'schema': 1, 'type': 'provider-byte-replay', 'original_scope': basis['scope'], 'worker': worker,
            'original_receipt_sha256': basis['provider_captures'][worker]['receipt_sha256'], 'original_binding': capture['binding'],
            'prepared_inputs_sha256': g.checksum(members['build-inputs.json']),
            'local_source_sha': local_sha, 'local_source_tree': before['source_tree'], 'staged_bytes': byte_count,
            'recorded_byte_hashes_equal': True, 'staged_inputs_unchanged': True, 'invocations': calls,
            'method_count': sum(call['method_count'] for call in calls), 'case_count': sum(call['case_count'] for call in calls),
            'case_digest_multiset_reproduced': True, 'java_executable_sha256': executable_sha,
            'historical_runtime_byte_equivalence_proven': False, 'independent_provider_replay_proven': False,
            'original_assertions_proven': False, 'whole_build_provenance_complete': False, 'ability_proven': False,
            'native_CI_proven': False, 'first_complete_PR_sample': False, 'performance_accepted': False,
            'reader_code_hashes': reader_hashes, 'input_files': refs.finish()}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for field in ('manifest', 'evidence-root', 'root', 'repo-local', 'output'):
        parser.add_argument('--' + field, required=True)
    parser.add_argument('--java', default='java')
    args = parser.parse_args()
    output = Path(args.output)
    require(not output.exists() and not output.is_symlink(), 'provider replay output must be a new file')
    require(all(not output.resolve().is_relative_to(Path(path).resolve()) for path in (args.root, args.repo_local)), 'provider replay output must stay outside its byte inputs')
    raw = g.file_bytes(Path(args.manifest))
    result = replay(g.References(args.evidence_root), args.root, args.repo_local, g.json_bytes(raw), args.java)
    require(g.file_bytes(Path(args.manifest)) == raw, 'provider replay request changed during execution')
    g.write_new(output, result)
    print('bounded provider case multiset reproduced; historical runtime, assertions and whole CI remain unproven')


if __name__ == '__main__':
    try:
        main()
    except (ValueError, OSError, KeyError, TypeError, subprocess.TimeoutExpired) as error:
        print('provider replay rejected: ' + str(error), file=sys.stderr)
        sys.exit(1)
