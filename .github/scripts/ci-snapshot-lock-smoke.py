#!/usr/bin/env python3
import subprocess
import tempfile
from pathlib import Path

script = Path(__file__).with_name('ci-snapshot-lock.py')
with tempfile.TemporaryDirectory() as temporary:
    root = Path(temporary)
    producer = root / 'producer/io/tapdata/demo/1-SNAPSHOT'
    worker = root / 'worker/io/tapdata/demo/1-SNAPSHOT'
    producer.mkdir(parents=True)
    worker.mkdir(parents=True)
    (producer / 'demo-1-SNAPSHOT.jar').write_bytes(b'old snapshot')
    (producer / 'demo-1-SNAPSHOT.pom').write_bytes(b'pom')
    (producer / '_remote.repositories').write_text('demo-1-SNAPSHOT.jar>nexus-snapshots=\n')
    (worker / 'demo-1-SNAPSHOT.jar').write_bytes(b'new snapshot')
    (worker / 'stale.jar').write_bytes(b'stale')
    lock = root / 'lock'
    subprocess.run(['python3', str(script), 'capture', str(root / 'producer'), str(lock)], check=True)
    subprocess.run(['python3', str(script), 'restore', str(lock), str(root / 'worker')], check=True)
    assert (worker / 'demo-1-SNAPSHOT.jar').read_bytes() == b'old snapshot'
    assert not (worker / 'stale.jar').exists()
    (lock / 'io/tapdata/demo/1-SNAPSHOT/demo-1-SNAPSHOT.jar').write_bytes(b'tampered')
    result = subprocess.run(['python3', str(script), 'restore', str(lock), str(root / 'worker')], capture_output=True)
    assert result.returncode != 0
    assert (worker / 'demo-1-SNAPSHOT.jar').read_bytes() == b'old snapshot'
    print('snapshot lock: freezes producer bytes, replaces stale cache and rejects tampering')
