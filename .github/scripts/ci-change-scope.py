#!/usr/bin/env python3
"""Fail closed to full CI unless the original PR diff contains only regular prose."""
import json
import os
from pathlib import Path
import re
import subprocess


ROOT_PAGES = {'README.md', 'CONTRIBUTING.md', 'SECURITY.md', 'CODE_OF_CONDUCT.md', 'CHANGELOG.md'}


def git(root, *args):
    return subprocess.check_output(
        ['git', '-C', str(root), *args], stderr=subprocess.PIPE, timeout=30)


def documentation_path(path):
    parts = path.split('/')
    return path in ROOT_PAGES or (
        len(parts) >= 2 and parts[0] == 'docs' and path.endswith('.md')
        and all(part and not part.startswith('.') for part in parts))


def classify(root, event_name, event):
    if event_name != 'pull_request':
        return False, 'non-PR events retain full CI'
    try:
        pr = event['pull_request']
        base, head = pr['base']['sha'], pr['head']['sha']
        if not all(isinstance(sha, str) and re.fullmatch('[0-9a-f]{40}', sha) for sha in (base, head)):
            return False, 'invalid PR commit identities'
        checkout = git(root, 'rev-parse', 'HEAD').decode().strip()
        if checkout != head:
            parents = git(root, 'show', '-s', '--format=%P', checkout).decode().split()
            if parents != [base, head]:
                return False, 'checkout does not match the original PR commits'
        bases = git(root, 'merge-base', '--all', base, head).decode().split()
        if len(bases) != 1:
            return False, 'merge base is not unique'
        raw = git(root, 'diff', '--no-ext-diff', '--no-textconv', '--no-renames',
                  '--raw', '--no-abbrev', '-z', bases[0], head, '--')
        tokens = raw.split(b'\0')
        if tokens[-1] != b'' or (len(tokens) - 1) % 2 or not 0 < (len(tokens) - 1) // 2 <= 3000:
            return False, 'empty or unsupported diff'
        for offset in range(0, len(tokens) - 1, 2):
            fields = tokens[offset].decode('ascii').split()
            path = tokens[offset + 1].decode('utf-8')
            if len(fields) != 5 or fields[4] not in ('A', 'M', 'D'):
                return False, 'unsupported diff status'
            if fields[0] not in (':100644', ':000000') or fields[1] not in ('100644', '000000'):
                return False, 'non-regular or executable file change'
            if not documentation_path(path):
                return False, 'non-documentation path changed'
        return True, 'only regular prose documentation changed'
    except (KeyError, TypeError, ValueError, OSError, subprocess.SubprocessError):
        return False, 'PR comparison unavailable; full CI required'


def main():
    try:
        event = json.loads(Path(os.environ['GITHUB_EVENT_PATH']).read_text())
        docs_only, reason = classify(Path.cwd(), os.environ.get('GITHUB_EVENT_NAME', ''), event)
    except (KeyError, ValueError, OSError):
        docs_only, reason = False, 'event unavailable; full CI required'
    print(f'CI scope: {reason}')
    with open(os.environ['GITHUB_OUTPUT'], 'a') as output:
        output.write(f'docs-only={str(docs_only).lower()}\n')
        if docs_only:
            output.write('matrix={"include":[{"shard":"docs-only","prepare_cloud_inputs":false}]}\n')


if __name__ == '__main__':
    main()
