#!/usr/bin/env python3
"""Bundle a pinned, committed DSL contract: python3 tools/sync-ai-dsl-guidance.py REPO REVISION."""
import hashlib
import json
from pathlib import Path
import subprocess
import sys

repo, revision = sys.argv[1:]
def git(*args):
    return subprocess.check_output(['git', '-C', repo, *args])
commit = git('rev-parse', '--verify', revision + '^{commit}').decode().strip()
files = {
    'pipeline-semantics.txt': 'skills/dsl-authoring/references/pipeline-semantics.md',
    'runtime-support.txt': 'skills/dsl-authoring/references/runtime-support.md',
    'tapstate-v1.schema.json': 'skills/dsl-authoring/references/tapstate-v1.schema.json',
    'LICENSE.txt': 'LICENSE',
}
# Read every input before writing; never bundle uncommitted local edits.
payloads = {name: git('show', commit + ':' + path) for name, path in files.items()}
destination = Path(__file__).resolve().parents[1] / 'control/control-core/src/main/resources/ai/dsl'
destination.mkdir(parents=True, exist_ok=True)
for name, data in payloads.items():
    (destination / name).write_bytes(data)
manifest = {'repository': 'https://github.com/tapstate/tapstate-skills', 'revision': commit,
            'files': {name: {'source': files[name], 'sha256': hashlib.sha256(data).hexdigest()}
                      for name, data in payloads.items()}}
(destination / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
print('Bundled DSL contract at ' + commit)
