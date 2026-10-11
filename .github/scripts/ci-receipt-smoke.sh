#!/usr/bin/env bash
# Negative controls for persistent raw evidence, unsafe archives and provenance.
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
python3 - "$here" <<'PY'
import argparse
import importlib.util
import io
import json
from pathlib import Path
import tempfile
import zipfile
import sys
spec = importlib.util.spec_from_file_location('receipt', Path(sys.argv[1]) / '_ci-receipt.py')
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)
checks = 0

def check(value, message):
    global checks
    assert value, message
    checks += 1

def rejected(fn, message):
    try:
        fn()
    except (ValueError, OSError, zipfile.BadZipFile):
        check(True, message)
    else:
        raise AssertionError(message)

with tempfile.TemporaryDirectory() as tmp:
    root = Path(tmp)
    raw = root / 'XML'
    raw.mkdir()
    (raw / 'TEST-A.xml').write_text('<testsuite/>')
    m.manifest(root, {'archive_complete': True, 'gaps': []})
    m.verify(argparse.Namespace(directory=str(root)))
    check(True, 'complete raw evidence reads back')
    (raw / 'TEST-A.xml').write_text('<testsuite tests="0"/>')
    rejected(lambda: m.verify(argparse.Namespace(directory=str(root))), 'same file identity with tampered bytes rejects')
    (raw / 'TEST-A.xml').write_text('<testsuite/>')
    (raw / 'EXTRA.xml').write_text('unexpected')
    rejected(lambda: m.verify(argparse.Namespace(directory=str(root))), 'extra evidence rejects')
    (raw / 'EXTRA.xml').unlink()
    (raw / 'TEST-A.xml').unlink()
    rejected(lambda: m.verify(argparse.Namespace(directory=str(root))), 'missing original evidence rejects')
    (raw / 'TEST-A.xml').write_text('<testsuite/>')
    m.manifest(root, {'archive_complete': False, 'gaps': ['only expired URL']})
    rejected(lambda: m.verify(argparse.Namespace(directory=str(root))), 'hash-valid but incomplete receipt rejects')
    (raw / 'LINK.xml').symlink_to(raw / 'TEST-A.xml')
    rejected(lambda: m.manifest(root, {'archive_complete': True}), 'symlink evidence rejects')
    (raw / 'LINK.xml').unlink()
    for member in ['../escape.xml', '/absolute.xml', 'unsafe\\name.xml']:
        path = root / 'unsafe.zip'
        with zipfile.ZipFile(path, 'w') as archive:
            archive.writestr(member, 'wrong')
        rejected(lambda: m.zip_members(path), 'archive traversal rejects: ' + member)
    path = root / 'unsafe.zip'
    with zipfile.ZipFile(path, 'w') as archive:
        link = zipfile.ZipInfo('link.xml')
        link.create_system = 3
        link.external_attr = 0o120777 << 16
        archive.writestr(link, '../outside')
    rejected(lambda: m.zip_members(path), 'ZIP symlink rejects')
    with zipfile.ZipFile(path, 'w') as archive:
        archive.writestr('data.xml', '<testsuite/>')
    check(m.zip_members(path) == ['data.xml'], 'normal raw ZIP reads back')
    path.write_bytes(b'not zip')
    rejected(lambda: m.zip_members(path), 'corrupt ZIP rejects')
    classfile = b'\xca\xfe\xba\xbe\x00\x00\x00\x41original'
    check(m.crc64(classfile) != m.crc64(classfile + b'changed'), 'changed class bytes have different identity')
    check(m.crc64(b'123456789') == '46a5a9388a5beffe', 'CRC64 known polynomial vector')
    # First-attempt provenance is validated independently of successful conclusion.
    original_api = m.api
    m.api = lambda endpoint: {'id': 10, 'run_attempt': 1, 'status': 'in_progress', 'repository': {'full_name': 'example/project'}}
    rejected(lambda: m.collect(argparse.Namespace(repo='example/project', run=10, archive_root=str(root / 'archive'),
                                                  source_root=str(root), cache_condition='unknown')), 'unfinished run cannot publish a receipt')
    m.api = original_api
print(f'ci-receipt smoke: {checks} assertions passed')
PY
