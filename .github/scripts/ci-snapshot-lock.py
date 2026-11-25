#!/usr/bin/env python3
"""Freeze the public Nexus snapshots used by one CI run."""
import hashlib
import json
import shutil
import sys
from pathlib import Path

ALLOWED = ('io/tapdata/', 'io/tapstate/cloud-control-plane-sdk/', 'io/tapstate/tapstate-cloud-parent/')
METADATA = ('_remote.repositories', 'resolver-status.properties')
SUFFIXES = ('.jar', '.pom', '.xml', '.sha1', '.sha256', '.sha512')


def digest(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def safe_relative(value):
    path = Path(value)
    if path.is_absolute() or '..' in path.parts or not value.startswith(ALLOWED):
        raise ValueError('snapshot path is outside the public dependency namespaces')
    if len(path.parts) < 5 or not path.parts[-2].endswith('-SNAPSHOT'):
        raise ValueError('invalid snapshot path')
    return path


def capture(repository, lock):
    if lock.exists():
        raise ValueError('snapshot lock already exists')
    lock.mkdir(parents=True)
    files = {}
    for namespace in ALLOWED:
        directory = repository / namespace
        if not directory.exists():
            continue
        for path in sorted(directory.rglob('*')):
            if path.is_symlink():
                raise ValueError('snapshot inputs cannot contain symlinks')
            if not path.is_file() or not path.parent.name.endswith('-SNAPSHOT'):
                continue
            if path.name not in METADATA and not path.name.endswith(SUFFIXES):
                continue
            rel = safe_relative(path.relative_to(repository).as_posix())
            target = lock / rel
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(path, target)
            files[rel.as_posix()] = digest(target)
    if not files:
        raise ValueError('no public Nexus snapshots were resolved')
    (lock / 'manifest.json').write_text(json.dumps({'schema': 1, 'files': files}, sort_keys=True))
    print(f'captured {len(files)} snapshot files')


def restore(lock, repository):
    manifest = json.loads((lock / 'manifest.json').read_text())
    if manifest.get('schema') != 1 or not isinstance(manifest.get('files'), dict) or not manifest['files']:
        raise ValueError('invalid snapshot lock')
    entries = []
    for name, expected in manifest['files'].items():
        rel = safe_relative(name)
        source = lock / rel
        if source.is_symlink() or not source.is_file() or digest(source) != expected:
            raise ValueError('snapshot lock bytes differ: ' + name)
        entries.append(rel)
    # Validate the complete input before replacing any cached snapshot directory.
    for directory in sorted({rel.parent for rel in entries}):
        target = repository / directory
        if target.is_symlink():
            raise ValueError('snapshot cache directory is a symlink')
        if target.exists():
            shutil.rmtree(target)
        target.mkdir(parents=True)
    for rel in entries:
        shutil.copyfile(lock / rel, repository / rel)
    print(f'restored {len(entries)} snapshot files')


def main():
    if len(sys.argv) != 4 or sys.argv[1] not in ('capture', 'restore'):
        raise ValueError('usage: ci-snapshot-lock.py capture REPOSITORY LOCK | restore LOCK REPOSITORY')
    first, second = (Path(value).resolve() for value in sys.argv[2:])
    if first == second or first in second.parents or second in first.parents:
        raise ValueError('repository and lock must be separate directories')
    (capture if sys.argv[1] == 'capture' else restore)(first, second)


if __name__ == '__main__':
    try:
        main()
    except (OSError, ValueError, KeyError, json.JSONDecodeError) as error:
        sys.exit('ci-snapshot-lock: ' + str(error))
