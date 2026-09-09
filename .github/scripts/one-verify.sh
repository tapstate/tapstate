#!/usr/bin/env bash
# Admit one automatic full-reactor test invocation. Shard legs form one logical suite.
# Reads conventional Actions block jobs/steps; shell comments and quoted diagnostic text
# are not invocations. No YAML or shell execution takes place during this read-only check.
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
exec python3 - "${1:-$here/../workflows}" <<'PY'
from pathlib import Path
import json
import re
import shlex
import sys


def scalar(value):
    value = value.strip()
    if value.startswith('"'):
        return json.loads(value)
    if value.startswith("'"):
        return value[1:-1].replace("''", "'")
    return value.split(' #', 1)[0].strip()


def triggers(text):
    match = re.search(r'^(?:on|[\'\"]on[\'\"]):([^\n]*)(?:\n|$)', text, re.M)
    if not match:
        return set(), ''
    inline = match[1].split('#', 1)[0].strip()
    body = re.split(r'^\S', text[match.end():], maxsplit=1, flags=re.M)[0]
    if inline:
        if inline.startswith('{'):
            raise ValueError('flow event mappings are unsupported; use an event block')
        return set(re.findall(r'[\w_]+', scalar(inline))), body
    return set(re.findall(r'^  ([\w_]+):', body, re.M)), body


def push_overlaps_pr(events, body):
    if not {'push', 'pull_request'} <= events:
        return False
    match = re.search(r'^  push:[^\n]*\n', body, re.M)
    if not match:
        return True
    push = re.split(r'^  \S', body[match.end():], maxsplit=1, flags=re.M)[0]
    branches = re.search(r'^    branches:[ \t]*([^\n]*)\n?', push, re.M)
    if not branches:
        # A tags-only push declaration never receives a branch update.
        return not re.search(r'^    tags:', push, re.M)
    inline = branches[1].strip()
    if inline:
        names = [scalar(item) for item in inline.strip('[]').split(',')]
    else:
        remaining = re.split(r'^    \S', push[branches.end():], maxsplit=1, flags=re.M)[0]
        names = [scalar(item) for item in re.findall(r'^      - (.*)', remaining, re.M)]
    # Automatic full tests belong to PRs; the integration branch is tested on merge.
    # Reject wildcard/feature push subscriptions even when they look path-disjoint.
    return names != ['main']


def runs(text):
    lines = text.splitlines()
    job, inside, index = None, False, 0
    while index < len(lines):
        line = lines[index]
        if line == 'jobs:':
            inside = True
        elif re.match(r'^jobs:[ \t]*[^#\s]', line):
            raise ValueError('flow job mappings are unsupported; use job blocks')
        elif inside and re.match(r'^\S', line):
            inside = False
        match = re.fullmatch(r'  ([\w-]+):\s*(?:#.*)?', line) if inside else None
        if match:
            job = match[1]
        elif inside and re.match(r'^  [\w-]+:[ \t]*[^#\s]', line):
            raise ValueError('flow job definitions are unsupported; use job blocks')
        if inside and re.match(r'^\s+steps:[ \t]*[^#\s]', line):
            raise ValueError('flow step lists are unsupported; use step blocks')
        run = re.match(r'^(\s+)(?:- )?run:\s*(.*)$', line) if job and inside else None
        if not run:
            index += 1
            continue
        indent = len(run[1]) + (2 if line.lstrip().startswith('- ') else 0)
        value = run[2].strip()
        number = index + 1
        end = index + 1
        if re.fullmatch(r'[|>][+-]?(?:\s+#.*)?', value):
            command = []
            while end < len(lines) and (not lines[end].strip() or len(lines[end]) - len(lines[end].lstrip()) > indent):
                command.append(lines[end][indent + 2:])
                end += 1
            value = (' ' if value.startswith('>') else '\n').join(command)
        else:
            if value.startswith(('*', '&')):
                raise ValueError('run aliases are unsupported; write the executable command')
            value = scalar(value)
        # A selected module's lifecycle does not execute the root reactor.
        start = index
        while start > 0 and not re.match(r'^\s+- ', lines[start]):
            start -= 1
        step_end = end
        while step_end < len(lines) and not re.match(r'^\s+- |^  [\w-]+:', lines[step_end]):
            step_end += 1
        step = '\n'.join(lines[start:step_end])
        cwd = re.search(r'^\s+working-directory:\s*(.*)$', step, re.M)
        if not cwd or scalar(cwd[1]) in ('.', '${{ github.workspace }}'):
            yield job, number, value
        index = end


def shell_comments(script):
    result, quote, escaped, comment = [], None, False, False
    for character in script:
        if comment:
            if character == '\n':
                result.append(character)
                comment = False
            continue
        if escaped:
            result.append(character)
            escaped = False
            continue
        if character == '\\' and quote != "'":
            result.append(character)
            escaped = True
        elif quote:
            result.append(character)
            if character == quote:
                quote = None
        elif character in ("'", '"'):
            quote = character
            result.append(character)
        elif character == '#' and (not result or result[-1].isspace() or result[-1] in ';&|()'):
            comment = True
        else:
            result.append(character)
    return ''.join(result)


def commands(script):
    # Strip shell comments and keep quoted diagnostics as one token. Keeping
    # newlines as separators distinguishes commands in a literal YAML run block.
    script = shell_comments(script.replace('\\\n', ''))
    lexer = shlex.shlex(script, posix=True, punctuation_chars=';&|()\n')
    lexer.commenters = ''
    lexer.whitespace = ' \t\r'
    lexer.whitespace_split = True
    current = []
    for token in lexer:
        if token and all(char in ';&|()\n' for char in token):
            if current:
                yield current
                current = []
        else:
            current.append(token)
    if current:
        yield current


def full_suite(arguments):
    # Count the call itself, not a name/comment/string containing its spelling.
    while arguments and (arguments[0] in ('then', 'do', 'if', '!') or re.match(r'^\w+=', arguments[0])):
        arguments = arguments[1:]
    if not arguments:
        return False
    executable = Path(arguments[0]).name
    if executable in ('env', 'command', 'exec', 'nice', 'stdbuf', 'timeout'):
        args = list(arguments[1:])
        flags = {'env': {'-i', '--ignore-environment', '-0', '--null'},
                 'command': {'-p'}, 'exec': {'-c', '-l'}, 'nice': set(),
                 'stdbuf': set(), 'timeout': {'--preserve-status', '--foreground', '-v', '--verbose'}}
        values = {'env': {'-u', '--unset', '-C', '--chdir'}, 'command': set(),
                  'exec': {'-a'}, 'nice': {'-n', '--adjustment'},
                  'stdbuf': {'-i', '-o', '-e', '--input', '--output', '--error'},
                  'timeout': {'-s', '--signal', '-k', '--kill-after'}}
        while args and args[0].startswith('-'):
            option = args.pop(0)
            if option == '--':
                break
            if option in flags[executable]:
                continue
            if option in values[executable] and args:
                args.pop(0)
                continue
            if any(option.startswith(flag + '=') for flag in values[executable] if flag.startswith('--')):
                continue
            if any(option.startswith(flag) and len(option) > len(flag) for flag in values[executable] if len(flag) == 2):
                continue
            raise ValueError('unsupported ' + executable + ' option: ' + option)
        if executable == 'timeout':
            if not args or not re.fullmatch(r'\d+(?:\.\d+)?[smhd]?', args[0]):
                raise ValueError('unsupported timeout duration')
            args.pop(0)
        return full_suite(args)
    if executable in ('xargs', 'eval', 'sudo'):
        raise ValueError('unsupported command wrapper: ' + executable)
    if executable == 'time':
        # Bash time and the external time utility execute their following command.
        # Handle the common options explicitly; unknown options must not conceal it.
        arguments = arguments[1:]
        while arguments and arguments[0].startswith('-'):
            option = arguments.pop(0)
            if option == '--':
                break
            if option in ('-p', '-v', '-a', '-q', '--portability', '--verbose', '--append', '--quiet'):
                continue
            if option in ('-f', '-o', '--format', '--output') and arguments:
                arguments.pop(0)
                continue
            if option.startswith(('--format=', '--output=')):
                continue
            raise ValueError('unsupported time option: ' + option)
        return full_suite(arguments)
    if executable in ('bash', 'sh'):
        args = list(arguments[1:])
        while args and args[0].startswith('-'):
            option = args.pop(0)
            if option == '--':
                break
            if not re.fullmatch(r'-[ceuxl]+', option):
                raise ValueError('unsupported shell option: ' + option)
            if 'c' in option:
                if len(args) != 1 or '$' in args[0] or '`' in args[0]:
                    raise ValueError('unsupported dynamic shell command')
                calls = [suite for command in commands(args[0]) if (suite := full_suite(command))]
                if len(calls) > 1:
                    raise ValueError('shell command repeats full-reactor tests')
                return calls[0] if calls else False
        if not args:
            raise ValueError('unsupported shell input')
        return full_suite(args)
    if executable == 'ci-shard.sh':
        return 'shards' if len(arguments) > 1 and arguments[1] == 'run' else False
    if executable not in ('mvn', 'mvnw'):
        # Unknown launchers must not conceal an executable suite operand. Quoted
        # diagnostics are data; known script entry points remain inspected at the
        # workflow boundary, not recursively through their implementation.
        if executable not in ('echo', 'printf', 'shellcheck') and any(
                re.search(r'(?:^|\s)(?:\S*/)?(?:mvnw?|ci-shard\.sh)(?:\s|$)', arg)
                for arg in arguments[1:]):
            raise ValueError('unsupported command wrapper: ' + executable)
        return False
    args = arguments[1:]
    if not {'test', 'package', 'integration-test', 'verify', 'install', 'deploy'} & set(args):
        return False
    for arg in args:
        if arg in ('-N', '--non-recursive', '-pl', '--projects') or arg.startswith(('-pl', '--projects=', '-Dtest=', '-Dit.test=')):
            return False
        if re.fullmatch(r'-D(?:skipTests|maven.test.skip)(?:=true)?', arg):
            return False
    for index, arg in enumerate(args):
        if arg in ('-f', '--file') and index + 1 < len(args) and args[index + 1] not in ('pom.xml', './pom.xml'):
            return False
    return 'maven'


def matrix_job(text, job):
    body = re.search(r'^  ' + re.escape(job) + r':[^\n]*\n(.*?)(?=^  [\w-]+:|\Z)', text, re.M | re.S)
    # Inline or aliased strategies cannot prove a single raw Maven execution.
    # The recognized shard driver remains valid with a strategy in either form.
    return bool(body and (re.search(r'^\s+matrix:', body[1], re.M)
                         or re.search(r'^    strategy:[ \t]*[^#\s]', body[1], re.M)))


try:
    root = Path(sys.argv[1])
    locations, overlapping = [], []
    for path in sorted([*root.glob('*.yml'), *root.glob('*.yaml')]):
        text = path.read_text()
        events, body = triggers(text)
        if not events & {'push', 'pull_request'}:
            continue
        calls = []
        for job, line, script in runs(text):
            for command in commands(script):
                suite = full_suite(command)
                if suite == 'maven' and matrix_job(text, job):
                    raise ValueError(f'{path.name}:{job}:{line}: full-reactor Maven matrix repeats tests; use the shard driver')
                if suite:
                    calls.append((job, line))
        locations.extend(f'{path.name}:{job}:{line}' for job, line in calls)
        if calls and push_overlaps_pr(events, body):
            overlapping.append(path.name)
    print(f'one-verify: found {len(locations)} automatic full-reactor test invocation(s)')
    for location in locations:
        print(f'  {location}')
    if overlapping:
        print('::error::push/pull_request overlap repeats the full suite: ' + ', '.join(overlapping))
    if len(locations) != 1:
        print('::error::expected exactly one automatic full-reactor test invocation')
    sys.exit(0 if len(locations) == 1 and not overlapping else 1)
except (OSError, ValueError) as error:
    print(f'::error::one-verify cannot inspect workflows: {error}', file=sys.stderr)
    sys.exit(1)
PY
