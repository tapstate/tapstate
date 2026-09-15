#!/usr/bin/env bash
# Only analyzer binaries are cached; the scanner still reads current server indexes.
set -euo pipefail
: "${SONAR_HOST_URL:?SONAR_HOST_URL is required}"
: "${SONAR_TOKEN:?SONAR_TOKEN is required}"
probe="$(mktemp -d)"
trap 'rm -rf "$probe"' EXIT
server="${SONAR_HOST_URL%/}"
curl --fail --silent --show-error --max-time 20 --user "$SONAR_TOKEN:" \
  "$server/api/plugins/installed" > "$probe/plugins.json"
curl --fail --silent --show-error --max-time 20 --user "$SONAR_TOKEN:" \
  "$server/batch/index" > "$probe/engine.txt"
python3 - "$probe" <<'PY'
import hashlib
import json
import os
from pathlib import Path
import re
import sys

def require(condition):
    if not condition:
        raise ValueError('invalid cache input')

try:
    root = Path(sys.argv[1])
    plugins = json.loads((root / 'plugins.json').read_text())['plugins']
    require(isinstance(plugins, list) and plugins)
    pairs = [(plugin['key'], plugin['hash']) for plugin in plugins]
    require(all(isinstance(key, str) and key and isinstance(digest, str)
                and re.fullmatch(r'[0-9a-fA-F]{32,64}', digest) for key, digest in pairs))
    require(len({key for key, digest in pairs}) == len(pairs))
    engine = [line.strip().split('|') for line in (root / 'engine.txt').read_text().splitlines() if line.strip()]
    require(engine and all(len(row) == 2 and row[0] and re.fullmatch(r'[0-9a-fA-F]{32,64}', row[1]) for row in engine))
    identity = {'plugins': sorted(pairs), 'engine': sorted(engine)}
    manifest = hashlib.sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest()
    server = hashlib.sha256(os.environ['SONAR_HOST_URL'].rstrip('/').encode()).hexdigest()
except (KeyError, TypeError, ValueError, OSError):
    print('Invalid Sonar cache inputs; normal scanner analysis must still run.', file=sys.stderr)
    sys.exit(1)
print('server=' + server)
print('manifest=' + manifest)
PY
