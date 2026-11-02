#!/usr/bin/env python3
"""Verify and identify the exact connector inputs used by a witness run."""
import argparse
import hashlib
import importlib.util
import json
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import zipfile

sys.dont_write_bytecode = True
IDS = frozenset({'mysql', 'mongodb', 'postgres', 'oracle', 'sqlserver', 'mongodb-atlas', 'aws-rds-mysql'})
RELEASE = 'connectors-preview'
ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location('stage', ROOT / 'deploy/cloud/stage-connectors.py')
stage = importlib.util.module_from_spec(spec)
spec.loader.exec_module(stage)


def require(condition, message):
    if not condition:
        raise ValueError(message)


def digest(path):
    require(path.is_file() and not path.is_symlink(), 'input must be a regular file: ' + path.name)
    return stage.sha256_and_size(path)


def identify(directory, mode, lock):
    entries, licenses = stage.read_lock(lock)
    expected = {entry['id']: entry for entry in entries if entry['id'] in IDS}
    require(set(expected) == IDS, 'release lock does not cover the seven witness connectors')
    files = sorted(directory.glob('*.jar'))
    selected = {}
    for path in files:
        candidates = [name for name in IDS if path.name == name + '-connector.jar'
                      or path.name.startswith(name + '-connector-')]
        require(len(candidates) == 1, 'unclassified connector jar: ' + path.name)
        name = candidates[0]
        require(name not in selected, 'duplicate connector input: ' + name)
        sha, size = digest(path)
        with zipfile.ZipFile(path) as jar:
            require(jar.namelist().count('META-INF/MANIFEST.MF') == 1, 'missing or duplicate jar manifest')
            headers = stage.manifest_headers(jar.read('META-INF/MANIFEST.MF'))
        if mode == 'release':
            require(path.name == name + '-connector.jar', 'Release input must use its published asset name')
            stage.verify_jar(path, expected[name])
        selected[name] = dict(id=name, file=path.name, bytes=size, sha256=sha,
                              pdkApiVersion=headers.get('PDK-API-Version'),
                              upstreamRevision=headers.get('Git-Commit-Id'))
    require(set(selected) == IDS, 'connector input set differs from the seven witness connectors')
    if mode == 'release':
        for entry in licenses:
            stage.verify_companion_license(directory / entry['name'], entry)
    return [selected[name] for name in sorted(selected)]


def record(directory, mode, lock, source_sha):
    require(mode in {'upstream', 'release'}, 'unknown connector input mode')
    require(len(source_sha) == 40 and all(c in '0123456789abcdef' for c in source_sha), 'invalid host revision')
    return dict(schema=1, mode=mode, source_sha=source_sha,
                release=RELEASE if mode == 'release' else None,
                lock_sha256=digest(lock)[0] if mode == 'release' else None,
                connectors=identify(directory, mode, lock))


def validate_identity(value, lock, source_sha, mode):
    require(isinstance(value, dict) and set(value) == {'schema', 'mode', 'source_sha', 'release', 'lock_sha256', 'connectors'},
            'invalid connector input identity fields')
    require(type(value['schema']) is int and value['schema'] == 1 and value['mode'] == mode and value['source_sha'] == source_sha,
            'connector input selection or host revision differs')
    require(mode in {'upstream', 'release'}, 'unknown input mode')
    entries, _ = stage.read_lock(lock)
    locked = {entry['id']: entry for entry in entries}
    require(isinstance(value['connectors'], list) and len(value['connectors']) == len(IDS), 'incomplete connector identity')
    seen = set()
    for entry in value['connectors']:
        require(isinstance(entry, dict) and set(entry) == {'id', 'file', 'bytes', 'sha256', 'pdkApiVersion', 'upstreamRevision'},
                'invalid connector identity entry')
        name = entry['id']
        require(name in IDS and name not in seen, 'duplicate or unknown connector identity')
        seen.add(name)
        require(Path(entry['file']).name == entry['file'] and entry['file'].endswith('.jar'), 'invalid connector file name')
        require(type(entry['bytes']) is int and entry['bytes'] > 0, 'invalid connector byte length')
        require(isinstance(entry['sha256'], str) and len(entry['sha256']) == 64
                and all(c in '0123456789abcdef' for c in entry['sha256']), 'invalid connector digest')
        if mode == 'release':
            require(entry['file'] == name + '-connector.jar' and all(entry[key] == locked[name][key]
                    for key in ['bytes', 'sha256', 'pdkApiVersion', 'upstreamRevision']), 'Release identity differs from lock')
    require(value['release'] == (RELEASE if mode == 'release' else None), 'unexpected connector release identity')
    require(value['lock_sha256'] == (digest(lock)[0] if mode == 'release' else None), 'connector lock identity differs')
    return value


def verify(manifest, directory, lock, source_sha, mode=None):
    digest(manifest)
    value = json.loads(manifest.read_text())
    require(isinstance(value, dict) and value.get('mode') in {'upstream', 'release'}, 'invalid connector input manifest')
    require(mode is None or value['mode'] == mode, 'connector input mode differs from selected workflow mode')
    require(value == record(directory, value['mode'], lock, source_sha), 'connector input identity changed')
    return value


def prepare_release(directory, lock, source_sha):
    require(not directory.exists() and not directory.is_symlink(), 'connector output already exists')
    entries, licenses = stage.read_lock(lock)
    with tempfile.TemporaryDirectory(prefix='released-connector-inputs-') as temporary:
        downloaded = Path(temporary)
        command = ['gh', 'release', 'download', RELEASE, '--repo', 'tapstate/tapstate', '--dir', str(downloaded)]
        for entry in entries:
            if entry['id'] in IDS:
                command += ['--pattern', entry['id'] + '-connector.jar']
        for entry in licenses:
            command += ['--pattern', entry['name']]
        subprocess.run(command, check=True)
        value = record(downloaded, 'release', lock, source_sha)
        directory.mkdir(parents=True, exist_ok=False)
        for path in downloaded.iterdir():
            shutil.copyfile(path, directory / path.name)
        (directory / 'connector-inputs.json').write_text(json.dumps(value, sort_keys=True, indent=2) + '\n')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=['prepare-release', 'record', 'verify'])
    parser.add_argument('--directory', required=True, type=Path)
    parser.add_argument('--lock', type=Path, default=ROOT / 'deploy/cloud/connectors.lock.json')
    parser.add_argument('--manifest', type=Path)
    parser.add_argument('--mode', choices=['upstream', 'release'])
    parser.add_argument('--source-sha', required=True)
    args = parser.parse_args()
    manifest = args.manifest or args.directory / 'connector-inputs.json'
    if args.command == 'prepare-release':
        require(args.mode in {None, 'release'}, 'Release preparation cannot select upstream inputs')
        prepare_release(args.directory, args.lock, args.source_sha)
    elif args.command == 'record':
        require(args.mode is not None, 'record requires an explicit input mode')
        require(not manifest.exists(), 'input manifest already exists')
        manifest.write_text(json.dumps(record(args.directory, args.mode, args.lock, args.source_sha), sort_keys=True, indent=2) + '\n')
    else:
        verify(manifest, args.directory, args.lock, args.source_sha, args.mode)
    print('Connector input identities verified')


if __name__ == '__main__':
    try:
        main()
    except (ValueError, OSError, KeyError, zipfile.BadZipFile, subprocess.CalledProcessError, stage.StageError) as failure:
        print('connector-inputs: ' + str(failure), file=sys.stderr)
        sys.exit(1)
