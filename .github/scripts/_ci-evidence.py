#!/usr/bin/env python3
"""Independent source inventories and fail-closed test/coverage evidence comparison.

Snapshots are observations, not CI admission waivers. Unresolved source forms and
missing provenance remain incomplete; existing shard and witness gates still run.
"""
import argparse
from collections import Counter
import csv
from decimal import Decimal
import hashlib
import os
import json
from pathlib import Path
import re
import subprocess
import sys
import textwrap
import tarfile
import tempfile
import zipfile
import xml.etree.ElementTree as ET

sys.dont_write_bytecode = True

SCHEMA = 1
ENUM_PATHS = {}
TEST_ANNOTATIONS = {'Test', 'ParameterizedTest', 'RepeatedTest', 'TestFactory', 'TestTemplate', 'ArchTest'}


def require(condition, message):
    if not condition:
        raise ValueError(message)


def digest(value):
    return hashlib.sha256(value).hexdigest()


def load(path):
    return json.loads(Path(path).read_text())


def output(path, value):
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(value, indent=2, sort_keys=True) + '\n')


def identity(record):
    return tuple(record[k] for k in ('lane', 'module', 'kind', 'class', 'method', 'parameter'))


def method_key(record):
    return tuple(record[k] for k in ('lane', 'module', 'kind', 'class', 'method'))


def current_sha(root):
    top = subprocess.run(['git', '-C', str(root), 'rev-parse', '--show-toplevel'], text=True, capture_output=True)
    if top.returncode or Path(top.stdout.strip()).resolve() != Path(root).resolve():
        return None
    result = subprocess.run(['git', '-C', str(root), 'rev-parse', 'HEAD'], text=True, capture_output=True)
    return result.stdout.strip() if result.returncode == 0 else None


def masked(text):
    # Preserve offsets while hiding comments and literals from the declaration scan.
    pattern = r'/\*[\s\S]*?\*/|//[^\n]*|"""[\s\S]*?"""|"(?:\\.|[^"\\])*"|\'(?:\\.|[^\'\\])*\''
    return re.sub(pattern, lambda m: ''.join('\n' if c == '\n' else ' ' for c in m.group()), text)


def close_at(text, start, left='(', right=')'):
    require(text[start] == left, 'invalid source delimiter')
    depth = 0
    for position in range(start, len(text)):
        if text[position] == left:
            depth += 1
        elif text[position] == right:
            depth -= 1
            if depth == 0:
                return position
    raise ValueError('unclosed source delimiter')


def uncomment(text):
    pattern = r'/\*[\s\S]*?\*/|//[^\n]*|"""[\s\S]*?"""|"(?:\\.|[^"\\])*"|\'(?:\\.|[^\'\\])*\''
    return re.sub(pattern, lambda m: ' ' if m.group().startswith(('/', '*')) else m.group(), text)


def enum_constants(text, simple):
    clean = masked(text)
    match = re.search(r'\benum\s+' + re.escape(simple) + r'\b[^\{]*\{', clean)
    if not match:
        return None
    start = match.end()
    end = close_at(clean, start - 1, '{', '}')
    body = clean[start:end]
    depth, stop = 0, len(body)
    for position, char in enumerate(body):
        if char in '({[':
            depth += 1
        elif char in ')}]':
            depth -= 1
        elif char == ';' and depth == 0:
            stop = position
            break
    values = []
    for constant in split_values(body[:stop]):
        name = re.match(r'\s*(\w+)', constant)
        require(name is not None, 'unsupported enum constant declaration')
        values.append(name.group(1))
    require(values and len(values) == len(set(values)), 'invalid enum source constants')
    return values


def resolve_enum(root, source, name, sources):
    text = source.read_text()
    simple = name.rsplit('.', 1)[-1]
    local = enum_constants(text, simple)
    if local:
        ENUM_PATHS[(str(source), name)] = source
        return local
    first = name.split('.')[0]
    imported = re.search(r'\bimport\s+([\w.]+\.' + re.escape(first) + r')\s*;', masked(text))
    package = re.search(r'\bpackage\s+([\w.]+)\s*;', masked(text))
    full = (imported.group(1) + name[len(first):]) if imported else ((package.group(1) + '.' if package else '') + name)
    names = [full]
    if imported is None:
        names += [m.group(1) + '.' + name for m in re.finditer(r'\bimport\s+([\w.]+)\.\*\s*;', masked(text))]
    matches = []
    for module, _ in source_modules(root):
        for tree in ('src/main/java', 'src/test/java'):
            for full_name in names:
                parts = full_name.split('.')
                for count in range(len(parts), 0, -1):
                    candidate = root / module / tree / ('/'.join(parts[:count]) + '.java')
                    if candidate.is_file():
                        values = enum_constants(candidate.read_text(), simple)
                        if values:
                            matches.append((candidate, values))
                        break
    require(len(matches) == 1, 'external enum cannot be independently resolved: ' + name)
    candidate, values = matches[0]
    ENUM_PATHS[(str(source), name)] = candidate
    sources[candidate.relative_to(root).as_posix()] = digest(candidate.read_bytes())
    return values


def split_values(value, generic=False):
    """Split a Java literal list without treating commas inside strings as separators."""
    parts, start, depth, quote, escaped = [], 0, 0, None, False
    for position, char in enumerate(value):
        if quote:
            if escaped:
                escaped = False
            elif char == '\\':
                escaped = True
            elif char == quote:
                quote = None
        elif char in '\"\'':
            quote = char
        elif char in '({[' or generic and char == '<':
            depth += 1
        elif char in ')}]' or generic and char == '>':
            depth -= 1
        elif char == ',' and depth == 0:
            parts.append(value[start:position].strip())
            start = position + 1
    parts.append(value[start:].strip())
    return [p for p in parts if p]


def literals(value, text):
    value = uncomment(value).strip()
    if value.startswith('{') and value.endswith('}'):
        value = value[1:-1]
    result = []
    for item in split_values(value):
        if re.fullmatch(r'"(?:\\.|[^"\\])*"', item):
            # Java and JSON string escapes coincide for ordinary annotation strings.
            try:
                result.append(json.loads(item))
            except json.JSONDecodeError:
                raise ValueError('unsupported Java string escape')
        elif re.fullmatch(r"'(?:\\.|[^'\\])'", item):
            result.append(item)
        elif re.fullmatch(r'(?:true|false|-?\d+(?:\.\d+)?[dDfFlL]?)', item):
            result.append(item)
        elif re.fullmatch(r'\w+', item):
            definition = re.search(r'\b' + re.escape(item) + r'\s*=\s*("(?:\\.|[^"\\])*"|true|false|-?\d+)\s*;', text)
            require(definition is not None, 'unresolved annotation constant: ' + item)
            result.extend(literals(definition.group(1), text))
        else:
            raise ValueError('unresolved annotation argument: ' + item)
    return result


def source_modules(root):
    """Walk Maven's default reactor independently of the scheduling implementation."""
    result = []
    def visit(directory, inherited_it):
        tree = ET.parse(directory / 'pom.xml').getroot()
        for node in tree.iter():
            node.tag = node.tag.rsplit('}', 1)[-1]
        for plugin in tree.findall('.//plugin'):
            if plugin.findtext('artifactId') in ('maven-surefire-plugin', 'maven-failsafe-plugin'):
                for config in plugin.findall('.//configuration'):
                    require(not any(config.find(name) is not None for name in ('includes', 'excludes', 'includesFile', 'excludesFile')),
                            'custom Maven selector requires an independent selection contract: ' + str(directory / 'pom.xml'))
        bound_it = inherited_it or any(plugin.findtext('artifactId') == 'maven-failsafe-plugin' and
                plugin.find("executions/execution/goals/goal[.='integration-test']") is not None
                for plugin in tree.findall('build/plugins/plugin'))
        result.append((directory.relative_to(root).as_posix(), bound_it))
        for child in tree.findall('modules/module'):
            visit(directory / child.text.strip(), bound_it)
    visit(root, False)
    return result


def declarations(text, outer, package):
    clean = masked(text)
    classes = []
    for match in re.finditer(r'\b(class|interface|enum|record)\s+(\w+)\b[^;{]*\{', clean):
        start = match.end() - 1
        end = close_at(clean, start, '{', '}')
        parent = next((c for c in reversed(classes) if c['start'] < start < c['end']), None)
        name = parent['name'] + '$' + match.group(2) if parent else package + match.group(2)
        classes.append(dict(start=start, end=end, name=name))
    result, consumed = [], -1
    for found in re.finditer(r'@([\w.]+)\b', clean):
        if found.start() < consumed or found.group(1).rsplit('.', 1)[-1] not in TEST_ANNOTATIONS:
            continue
        # Start at this test annotation and collect following declaration annotations.
        position, annotations = found.start(), {}
        while True:
            annotation = re.match(r'\s*@([\w.]+)\b', clean[position:])
            if not annotation:
                break
            name = annotation.group(1).rsplit('.', 1)[-1]
            position += annotation.end()
            while position < len(clean) and clean[position].isspace():
                position += 1
            argument = ''
            if position < len(clean) and clean[position] == '(':
                end = close_at(clean, position)
                argument = text[position + 1:end]
                position = end + 1
            annotations[name] = argument
        declaration = re.match(r'\s*(?:[\w<>\[\].?,]+\s+)+?(\w+)\s*\(', clean[position:])
        if declaration:
            name = declaration.group(1)
            consumed = position + declaration.end()
            parameter_end = close_at(clean, consumed - 1)
            parameter_types, parameter_injected = [], []
            for parameter in split_values(text[consumed:parameter_end], generic=True):
                parameter_injected.append(bool(re.search(r'@(?:[\w.]+\.)?TempDir\b', parameter)))
                parameter = re.sub(r'@[\w.]+(?:\([^)]*\))?\s*', '', parameter).strip()
                parameter_types.append(re.sub(r'\s+\w+\s*$', '', parameter).replace('final ', '').strip())
        elif 'ArchTest' in annotations:
            field = re.match(r'\s*(?:[\w<>\[\].?,]+\s+)+?(\w+)\s*=', clean[position:])
            if not field:
                result.append(dict(unresolved='unsupported ArchTest declaration', offset=found.start()))
                continue
            name = field.group(1)
            consumed = position + field.end()
            parameter_types, parameter_injected = [], []
        else:
            result.append(dict(unresolved='unsupported test declaration', offset=found.start()))
            continue
        if 'ArchTest' not in annotations and re.search(r'\bprivate\b', clean[position:consumed]):
            continue
        owner = next((c for c in reversed(classes) if c['start'] < found.start() < c['end']), None)
        if owner is None or not (owner['name'] == package + outer or owner['name'].startswith(package + outer + '$')):
            continue
        # Include preceding Disabled annotations in the declaration's contract.
        prefix = text[max(text.rfind('}', 0, found.start()), text.rfind(';', 0, found.start())) + 1:found.start()]
        disabled = re.search(r'@Disabled(?:\s*\((.*?)\))?', prefix, re.S)
        result.append(dict(**{'class': owner['name']}, method=name, annotations=annotations,
                           disabled_source=disabled.group(1) if disabled else None,
                           parameter_types=parameter_types, parameter_injected=parameter_injected, declaration_sha256=digest(text[found.start():consumed].encode())))
    return result


def argument_digest(arguments):
    return digest(json.dumps(['arguments', arguments], separators=(',', ':'), ensure_ascii=False).encode('utf-8'))


def canonical_literal(value, typename):
    if value is None:
        return ['null']
    types = dict(String='java.lang.String', char='java.lang.Character', Character='java.lang.Character',
                 boolean='java.lang.Boolean', Boolean='java.lang.Boolean', byte='java.lang.Byte', Byte='java.lang.Byte',
                 short='java.lang.Short', Short='java.lang.Short', int='java.lang.Integer', Integer='java.lang.Integer',
                 long='java.lang.Long', Long='java.lang.Long', float='java.lang.Float', Float='java.lang.Float',
                 double='java.lang.Double', Double='java.lang.Double')
    require(typename in types, 'unsupported source argument type: ' + typename)
    typename = types[typename]
    if typename == 'java.lang.String':
        return [typename, value]
    if typename == 'java.lang.Boolean':
        require(str(value).lower() in ('true', 'false'), 'unsupported source boolean')
        return [typename, str(value).lower()]
    if typename == 'java.lang.Character':
        return [typename, value.strip("'")]
    spelling = str(Decimal(str(value).rstrip('dDfFlL')).normalize())
    spelling = format(Decimal(spelling), 'f')
    return [typename, spelling]


def enum_qualified_name(root, source, enum_name, sources):
    simple = enum_name.rsplit('.', 1)[-1]
    candidates = [ENUM_PATHS.get((str(source), enum_name), source)]
    for candidate in candidates:
        text = candidate.read_text()
        if enum_constants(text, simple) is None:
            continue
        clean = masked(text)
        package = re.search(r'\bpackage\s+([\w.]+)\s*;', clean)
        prefix = package.group(1) + '.' if package else ''
        classes = []
        for match in re.finditer(r'\b(class|interface|enum|record)\s+(\w+)\b[^;{]*\{', clean):
            start = match.end() - 1
            parent = next((c for c in reversed(classes) if c['start'] < start < c['end']), None)
            name = parent['name'] + '$' + match.group(2) if parent else prefix + match.group(2)
            record = dict(start=start, end=close_at(clean, start, '{', '}'), name=name)
            classes.append(record)
            if match.group(1) == 'enum' and match.group(2) == simple:
                return name
    raise ValueError('enum binary identity cannot be independently resolved: ' + enum_name)


def provider_body(text, name):
    clean = masked(text)
    match = re.search(r'\b' + re.escape(name) + r'\s*\(\s*\)\s*(?:throws[^\{]+)?\{', clean)
    require(match is not None, 'external or overloaded parameter provider requires an independent contract: ' + name)
    start = match.end() - 1
    return text[start + 1:close_at(clean, start, '{', '}')]


def canonical_expression(expression, root, source, sources, body):
    expression = uncomment(expression).strip()
    expression = re.sub(r'^\(Object\)\s*', '', expression)
    if expression == 'null':
        return ['null']
    if expression.startswith('"""') and expression.endswith('"""'):
        value = expression[3:-3]
        require(not re.search(r'\\[^"\\nrtbf]', value), 'unsupported Java text block escape')
        value = textwrap.dedent(value.removeprefix('\n'))
        value = value.replace('\\n', '\n').replace('\\t', '\t').replace('\\r', '\r').replace('\\"', '"').replace('\\\\', '\\')
        return ['java.lang.String', value]
    if re.fullmatch(r'"(?:\\.|[^"\\])*"', expression):
        return ['java.lang.String', json.loads(expression)]
    if expression in ('true', 'false'):
        return ['java.lang.Boolean', expression]
    if re.fullmatch(r'-?\d[\d_]*(?:\.\d+)?[dDfFlL]?', expression):
        typename = ('long' if expression[-1:] in 'lL' else 'float' if expression[-1:] in 'fF' else
                    'double' if '.' in expression or expression[-1:] in 'dD' else 'int')
        return canonical_literal(expression.replace('_', ''), typename)
    array = re.fullmatch(r'new\s+String\s*\[\s*\]\s*\{([\s\S]*)\}', expression)
    if array:
        return ['array', 'java.lang.String', [canonical_expression(v, root, source, sources, body) for v in split_values(array.group(1))]]
    call = re.fullmatch(r'(List\.of|Map\.of|new Document|TableRef\.literal|TableRef\.regex)\s*\(([\s\S]*)\)', expression)
    if call:
        values = [canonical_expression(v, root, source, sources, body) for v in split_values(call.group(2))]
        if call.group(1) == 'List.of':
            return ['list', values]
        if call.group(1) in ('Map.of', 'new Document'):
            require(len(values) % 2 == 0, 'invalid source map argument arity')
            entries = list(zip(values[::2], values[1::2]))
            entries.sort(key=lambda entry: json.dumps(entry, separators=(',', ':'), ensure_ascii=False).encode())
            return ['map', entries]
        require(len(values) == 1, 'invalid TableRef factory argument arity')
        record_name = 'Literal' if call.group(1).endswith('literal') else 'Regex'
        field = 'name' if record_name == 'Literal' else 'pattern'
        definition = root / 'core/core-model/src/main/java/io/tapstate/core/model/TableRef.java'
        require(definition.is_file(), 'TableRef source definition is missing')
        definition_text = masked(definition.read_text())
        require(re.search(r'static\s+' + record_name + r'\s+' + field.replace('pattern', 'regex').replace('name', 'literal') + r'\s*\(String\s+\w+\)\s*\{\s*return\s+new\s+' + record_name + r'\(\w+\);\s*\}', definition_text) is not None,
                'TableRef factory source contract changed')
        sources[definition.relative_to(root).as_posix()] = digest(definition.read_bytes())
        return ['record', 'io.tapstate.core.model.TableRef$' + record_name, [[field, values[0]]]]
    enum = re.fullmatch(r'([\w.]+)\.(\w+)', expression)
    if enum:
        names = resolve_enum(root, source, enum.group(1), sources)
        require(enum.group(2) in names, 'source enum constant is absent')
        return ['enum', enum_qualified_name(root, source, enum.group(1), sources), enum.group(2)]
    if re.fullmatch(r'\w+', expression):
        local = re.search(r'\b(?:String|int|long|double|boolean)\s+' + re.escape(expression) + r'\s*=\s*([^;]+);', body)
        if local:
            return canonical_expression(local.group(1), root, source, sources, body)
    raise ValueError('source parameter expression requires an independent contract: ' + expression[:100])


def static_provider_arguments(root, source, declaration, sources):
    annotation = declaration['annotations'].get('MethodSource')
    require(annotation is not None, 'custom parameter provider requires an independent contract')
    names = literals(annotation or '"' + declaration['method'] + '"', source.read_text())
    require(len(names) == 1, 'multiple parameter providers require an independent contract')
    body = provider_body(source.read_text(), names[0])
    declaration['provider_body_sha256'] = digest(body.encode())
    clean = masked(body)
    native = re.sub(r'\s+', '', body)
    if native == 'returnSystem.getenv().containsKey("TAPSTATE_TEST_NATIVE_CLI")?List.of(CliFlavor.JVM,CliFlavor.NATIVE):List.of(CliFlavor.JVM);':
        presence = declaration.get('native_cli_presence', 'unknown')
        require(presence in ('absent', 'present'), 'native CLI environment presence must be frozen independently')
        resolve_enum(root, source, 'CliFlavor', sources)
        binary = enum_qualified_name(root, source, 'CliFlavor', sources)
        declaration['environment_contract'] = dict(TAPSTATE_TEST_NATIVE_CLI=presence)
        return [[['enum', binary, value]] for value in (['JVM', 'NATIVE'] if presence == 'present' else ['JVM'])]
    compact = re.sub(r'\s+', '', clean)
    cross = re.fullmatch(r'return(?:Arrays\.stream|Stream\.of)\(([\w.]+)\.values\(\)\)\.flatMap\((\w+)->(?:Arrays\.stream|Stream\.of)\(([\w.]+)\.values\(\)\)\.map\((\w+)->Arguments\.of\((\w+),(\w+)\)\)\);', compact)
    if cross:
        first_type, first_name, second_type, second_name, first_arg, second_arg = cross.groups()
        require((first_name, second_name) == (first_arg, second_arg), 'enum matrix argument binding changed')
        first = resolve_enum(root, source, first_type, sources)
        second = resolve_enum(root, source, second_type, sources)
        first_binary = enum_qualified_name(root, source, first_type, sources)
        second_binary = enum_qualified_name(root, source, second_type, sources)
        return [[['enum', first_binary, a], ['enum', second_binary, b]] for a in first for b in second]
    matrix = re.fullmatch(r'returnArrays\.stream\(([\w.]+)\.values\(\)\)\.flatMap\((\w+)->Stream\.of\(([^()]+)\)\.flatMap\((\w+)->(?:java\.util\.stream\.)?IntStream\.range\((\d+),(\d+)\)\.mapToObj\((\w+)->Arguments\.of\((\w+),(\w+),(\w+)\)\)\)\);', compact)
    if matrix:
        first_type, first_name, second_values, second_name, low, high, third_name, first_arg, second_arg, third_arg = matrix.groups()
        require((first_name, second_name, third_name) == (first_arg, second_arg, third_arg), 'matrix argument binding changed')
        require(0 <= int(low) <= int(high) <= 256, 'matrix range requires an independent bounded contract')
        first = resolve_enum(root, source, first_type, sources)
        first_binary = enum_qualified_name(root, source, first_type, sources)
        second = [canonical_expression(v, root, source, sources, body) for v in split_values(second_values)]
        return [[['enum', first_binary, a], b, ['java.lang.Integer', str(c)]] for a in first for b in second for c in range(int(low), int(high))]
    returned = re.search(r'\breturn\s+Stream\.of\s*\(', clean)
    if returned:
        start = returned.end() - 1
        end = close_at(clean, start)
        tail = re.sub(r'\s+', '', clean[end + 1:])
        require(tail == ';' or tail == '.map(hosts->Arguments.of((Object)hosts));', 'computed Stream transformation requires an independent contract')
        result = []
        for expression in split_values(body[start + 1:end]):
            argument_call = re.fullmatch(r'(?:Arguments\.of|org\.junit\.jupiter\.params\.provider\.Arguments\.of)\s*\(([\s\S]*)\)', expression.strip())
            arguments = split_values(argument_call.group(1)) if argument_call else [expression]
            result.append([canonical_expression(value, root, source, sources, body) for value in arguments])
        return result
    cli = re.fullmatch(r'\s*return\s+Cli\.(\w+)\.stream\(\);\s*', body)
    if cli:
        definition = root / 'cli/src/main/java/io/tapstate/cli/Cli.java'
        text = definition.read_text(); stripped = masked(text)
        sources[definition.relative_to(root).as_posix()] = digest(definition.read_bytes())
        def values_for(name):
            start = re.search(r'\b' + re.escape(name) + r'\s*=\s*List\.of\s*\(', stripped)
            require(start is not None, 'CLI list source is not literal: ' + name)
            end = close_at(stripped, start.end() - 1)
            return literals(text[start.end():end], text)
        if cli.group(1) != 'CONNECTED_VERBS':
            values = values_for(cli.group(1))
        else:
            derivation = re.search(r'\bCONNECTED_VERBS\s*=([^;]+);', text)
            require(derivation and re.sub(r'\s+', '', derivation.group(1)) == 'VERB_BY_OPERATION.values().stream().filter(verb->!OFFLINE_VERBS.contains(verb)).distinct().sorted().toList()', 'CLI connected-verb derivation changed')
            start = re.search(r'\bVERB_BY_OPERATION\s*=\s*Map\.ofEntries\s*\(', stripped)
            require(start is not None, 'CLI operation mapping source is missing')
            end = close_at(stripped, start.end() - 1)
            entries = []
            for entry in split_values(uncomment(text[start.end():end])):
                match = re.fullmatch(r'Map\.entry\("(?:\\.|[^"\\])*",\s*("(?:\\.|[^"\\])*"|\w+)\)', entry)
                require(match is not None, 'CLI operation mapping is not literal')
                entry_values = literals(match.group(1), text)
                require(len(entry_values) == 1, 'CLI operation mapping value is not singular')
                entries.append(entry_values[0])
            require(entries, 'CLI operation mapping is not literal')
            values = sorted(set(entries) - set(values_for('OFFLINE_VERBS')))
        return [[['java.lang.String', value]] for value in values]
    if re.fullmatch(r'\s*return\s+Examples\.specifications\(\);\s*', body):
        definition = source.parent / 'Examples.java'
        require(definition.is_file(), 'example source discovery contract is missing')
        sources[definition.relative_to(root).as_posix()] = digest(definition.read_bytes())
        definition_text = definition.read_text()
        require('Path.of("examples")' in definition_text and '".e2e.yml"' in definition_text and
                'Files.walk(ROOT)' in definition_text and '.sorted().toList()' in definition_text,
                'example source discovery contract changed')
        module = source.relative_to(root).as_posix().split('/src/test/java/', 1)[0]
        paths = sorted((root / module / 'examples').rglob('*.e2e.yml'))
        for path in paths:
            sources[path.relative_to(root).as_posix()] = digest(path.read_bytes())
        return [[['path', path.relative_to(root / module).as_posix()]] for path in paths]
    raise ValueError('computed parameter provider requires an independent parameter contract: ' + annotation)


def with_injected(arguments, declaration):
    result, position = [], 0
    for typename, injected in zip(declaration['parameter_types'], declaration['parameter_injected']):
        if injected:
            require(typename == 'Path', 'unsupported injected TempDir type: ' + typename)
            result.append(['injected', 'org.junit.jupiter.api.io.TempDir', 'java.nio.file.Path'])
        else:
            require(position < len(arguments), 'source and argument arity differs')
            result.append(arguments[position])
            position += 1
    require(position == len(arguments), 'source and argument arity differs')
    return result


def inherited_declarations(root, source, sources, stack=None):
    stack = set() if stack is None else stack
    require(source not in stack, 'cyclic test inheritance')
    stack = stack | {source}
    text, clean = source.read_text(), masked(source.read_text())
    package = re.search(r'\bpackage\s+([\w.]+)\s*;', clean)
    prefix = package.group(1) + '.' if package else ''
    own = [(source, item) for item in declarations(text, source.stem, prefix)]
    parent = re.search(r'\bclass\s+' + re.escape(source.stem) + r'\s+extends\s+([\w.]+)', clean)
    if parent is None:
        return own
    parent_name = parent.group(1)
    first = parent_name.split('.')[0]
    imported = re.search(r'\bimport\s+([\w.]+\.' + re.escape(first) + r')\s*;', clean)
    names = [(imported.group(1) + parent_name[len(first):]) if imported else prefix + parent_name]
    if imported is None:
        names += [m.group(1) + '.' + parent_name for m in re.finditer(r'\bimport\s+([\w.]+)\.\*\s*;', clean)]
    found = []
    for module, _ in source_modules(root):
        for tree in ('src/main/java', 'src/test/java'):
            for name in names:
                candidate = root / module / tree / (name.replace('.', '/') + '.java')
                if candidate.is_file():
                    found.append(candidate)
    require(len(found) == 1, 'test parent source requires an independent inheritance contract: ' + parent_name)
    base = found[0]
    sources[base.relative_to(root).as_posix()] = digest(base.read_bytes())
    overrides = {(item['class'].removeprefix(prefix + source.stem), item['method']) for _, item in own if 'method' in item}
    base_package = re.search(r'\bpackage\s+([\w.]+)\s*;', masked(base.read_text()))
    base_name = (base_package.group(1) + '.' if base_package else '') + base.stem
    for declaring_source, item in inherited_declarations(root, base, sources, stack):
        if 'class' not in item:
            own.append((declaring_source, item))
            continue
        suffix = item['class'].removeprefix(base_name)
        if (suffix, item['method']) not in overrides:
            inherited = dict(item)
            inherited['class'] = prefix + source.stem + suffix
            own.append((declaring_source, inherited))
    return own


def before_all_contracts(root, source, sources):
    text, clean = source.read_text(), masked(source.read_text())
    callbacks = []
    package = re.search(r'\bpackage\s+([\w.]+)\s*;', clean)
    classname = (package.group(1) + '.' if package else '') + source.stem
    for annotation in re.finditer(r'@(?:org\.junit\.jupiter\.api\.)?BeforeAll\b', clean):
        header = re.search(r'\b(?:void|[\w.]+)\s+(\w+)\s*\(', clean[annotation.end():annotation.end() + 500])
        if header is None:
            continue
        opening = annotation.end() + header.end() - 1
        parameters_end = close_at(clean, opening)
        body_start = clean.find('{', parameters_end)
        require(body_start >= 0, 'BeforeAll source body is missing')
        body_end = close_at(clean, body_start, '{', '}')
        body, body_clean = text[body_start + 1:body_end], clean[body_start + 1:body_end]
        policy = None
        for gate, predicate, property_name in [('RealConnectorGate', 'nonblank-false', 'tapstate.e2e.connectors-dir'),
                                               ('UpgradeLaneGate', 'boolean-true-false', 'tapstate.e2e.upgrade-lane')]:
            call = re.search(r'\b' + gate + r'\s*\.\s*require\s*\(', body_clean)
            if call is None:
                continue
            require(body_clean[:call.start()].count('{') == body_clean[:call.start()].count('}'), 'conditional class gate requires an explicit contract')
            helper = source.parent / (gate + '.java')
            require(helper.is_file(), 'class source gate is missing')
            helper_text = helper.read_text()
            reason = ('no -Dtapstate.e2e.connectors-dir: skipping a witness that needs real connector jars' if gate == 'RealConnectorGate' else
                      'no -Dtapstate.e2e.upgrade-lane: skipping a witness that upgrades from a published release')
            require(reason in helper_text if gate == 'RealConnectorGate' else 'skipping a witness that upgrades from a published release' in helper_text,
                    'class source gate reason changed')
            sources[helper.relative_to(root).as_posix()] = digest(helper.read_bytes())
            if gate == 'RealConnectorGate':
                naming = source.parent / 'ConnectorJars.java'
                require(naming.is_file() and 'configured != null && !configured.isBlank()' in naming.read_text(), 'connector naming predicate changed')
                sources[naming.relative_to(root).as_posix()] = digest(naming.read_bytes())
            policy = dict(kind=gate, predicate=predicate, input_kind='properties', inputs=[property_name], reason_digest=digest(json.dumps(reason, ensure_ascii=False).encode()))
            break
        if policy is None and source.stem == 'PostgresResumeOffsetTest' and 'configured != null || directory != null' in body:
            policy = dict(kind='postgres-jar-absence', predicate='all-present-false', input_kind='properties',
                          inputs=['tapstate.pdk.it.postgres-jar', 'tapstate.e2e.connectors-dir'],
                          reason_digest=digest(json.dumps('Assumption failed: no real PostgreSQL connector jar supplied').encode()))
        atlas = re.search(r'(?:Assumptions\.)?assumeTrue\(System\.getenv\("(TAPSTATE_ATLAS_(?:TEST|STANDARD)_URI)"\) != null,\s*("[^"\\]+")\)', body)
        if atlas:
            if policy:
                callbacks.append(dict(**{'class': classname}, method=header.group(1), source=source.relative_to(root).as_posix(), source_sha256=digest(source.read_bytes()), body_sha256=digest(body.encode()), policy=policy))
            policy = dict(kind='atlas-uri-absence', predicate='all-present-false', input_kind='environment', inputs=[atlas.group(1)],
                          reason_digest=digest(json.dumps('Assumption failed: ' + json.loads(atlas.group(2))).encode()))
        if policy:
            callbacks.append(dict(**{'class': classname}, method=header.group(1), source=source.relative_to(root).as_posix(),
                                  source_sha256=digest(source.read_bytes()), body_sha256=digest(body.encode()), policy=policy))
    return callbacks


def published_policy(root, source, examples, sources, mode):
    synthetic_source = source.parent / 'E2eConnectorJar.java'
    require(synthetic_source.is_file(), 'published synthetic connector identity source is missing')
    match = re.search(r'\bCONNECTOR_ID\s*=\s*"([^"\\]+)"\s*;', synthetic_source.read_text())
    require(match is not None, 'published synthetic connector identity is not literal')
    synthetic = match.group(1)
    sources[synthetic_source.relative_to(root).as_posix()] = digest(synthetic_source.read_bytes())
    gate = source.parent / 'RealConnectorGate.java'
    message, gate_hash = None, None
    if gate.is_file():
        gate_text = gate.read_text()
        abort = re.search(r'case\s+SKIP\s*->\s*Assumptions\.abort\(\s*("(?:\\.|[^"\\])*")\s*\)', gate_text)
        if abort:
            message, gate_hash = json.loads(abort.group(1)), digest(gate.read_bytes())
            sources[gate.relative_to(root).as_posix()] = gate_hash
    policies = {}
    for example in examples:
        relative = example.relative_to(root / 'e2e').as_posix()
        text = example.read_text()
        setup = re.search(r'^setup:\s*\n((?:[ \t]+[^\n]*\n|\s*\n)*)', text, re.M)
        require(setup is not None, 'published setup source is not independently readable: ' + relative)
        connectors = re.search(r'^  connectors:\s*\[([^]\n]*)\]\s*(?:#[^\n]*)?$', setup.group(1), re.M)
        require(connectors is not None, 'published connector selection requires an independent contract: ' + relative)
        ids = [value.strip().strip('\"\'') for value in connectors.group(1).split(',') if value.strip()]
        require(ids and all(re.fullmatch(r'[a-z0-9_-]+', value) for value in ids), 'unsupported published connector identifier')
        real = any(value != synthetic for value in ids)
        if real and mode == 'absent':
            require(message and gate_hash and 'RealConnectorGate.require(real.toArray(String[]::new))' in source.read_text(),
                    'real-connector skip policy is not bound to the original source gate')
        for tier in ('IN_PROCESS', 'REAL_PROCESS'):
            policies[relative + ' on ' + tier] = dict(requires_real=real, mode=mode, connectors=ids,
                    expected_status='skipped' if real and mode == 'absent' else 'executed',
                    skip_message=message if real and mode == 'absent' else None)
        sources[example.relative_to(root).as_posix()] = digest(example.read_bytes())
    return dict(cases=policies, gate_sha256=gate_hash, published_source_sha256=digest(source.read_bytes()), synthetic_source_sha256=digest(synthetic_source.read_bytes()))


def source_parameters(root, source, declaration, sources):
    annotations, text = declaration['annotations'], source.read_text()
    if source.stem == 'PublishedExamplesIT' and declaration['method'] == 'thePublishedExampleRuns':
        examples = sorted((root / 'e2e/examples').rglob('*.e2e.yml'))
        declaration['published_policy'] = published_policy(root, source, examples, sources, declaration.get('published_connectors', 'unknown'))
        return [p.relative_to(root / 'e2e').as_posix() + ' on ' + tier for p in examples
                for tier in ('IN_PROCESS', 'REAL_PROCESS')], 'published-marker'
    if 'RepeatedTest' in annotations:
        count = re.match(r'(?:value\s*=\s*)?(\d+)', annotations['RepeatedTest'])
        require(count is not None and int(count.group(1)) > 0, 'unresolved RepeatedTest count')
        declaration['argument_digests'] = [argument_digest([])] * int(count.group(1))
        return ['repetition=' + str(n) for n in range(1, int(count.group(1)) + 1)], 'ordinal-to-source'
    if 'TestFactory' in annotations or 'TestTemplate' in annotations:
        raise ValueError('runtime dynamic identities require an independent parameter contract')
    if 'ParameterizedTest' not in annotations:
        declaration['argument_digests'] = [argument_digest([])]
        return [''], 'method'
    primary_sources = [name for name in ('ValueSource', 'CsvSource', 'EnumSource', 'MethodSource', 'ArgumentsSource', 'ArgumentsSources', 'CsvFileSource') if name in annotations]
    require(len(primary_sources) <= 1, 'multiple primary parameter sources require independent composite enumeration')
    require(not any(name in annotations for name in ('ArgumentsSource', 'ArgumentsSources', 'CsvFileSource')), 'custom parameter source requires independent enumeration')
    values = []
    declaration['argument_digests'] = []
    if 'ValueSource' in annotations:
        match = re.fullmatch(r'\s*(\w+)\s*=\s*([\s\S]+)', annotations['ValueSource'])
        require(match is not None, 'unsupported ValueSource form')
        raw_values = literals(match.group(2), text)
        values = [[match.group(1), value] for value in raw_values]
        typename = dict(strings='String', booleans='boolean', ints='int', longs='long', doubles='double', floats='float', shorts='short', bytes='byte', chars='char').get(match.group(1))
        declaration['argument_digests'] = [argument_digest(with_injected([canonical_literal(v, typename)], declaration)) for v in raw_values]
    elif 'CsvSource' in annotations:
        argument = uncomment(annotations['CsvSource']).strip()
        delimiter = re.search(r"\bdelimiter\s*=\s*'(.)'", argument)
        nulls = re.search(r'\bnullValues\s*=\s*(\{[\s\S]*?\}|"(?:\\.|[^"\\])*")', argument)
        null_values = literals(nulls.group(1), text) if nulls else []
        block = re.search(r'\btextBlock\s*=\s*"""([\s\S]*?)"""', argument)
        if block:
            rows = [line.strip() for line in block.group(1).splitlines() if line.strip() and not line.strip().startswith('#')]
        else:
            match = re.search(r'\bvalue\s*=\s*(\{[\s\S]*?\}|"(?:\\.|[^"\\])*")', argument)
            rows = literals(match.group(1) if match else argument, text)
        for row in rows:
            parsed = next(csv.reader([row], delimiter=delimiter.group(1) if delimiter else ',', quotechar="'", skipinitialspace=True))
            values.append([None if value.strip() in null_values or not value.strip() else value.strip() for value in parsed])
        source_types = [typename for typename, injected in zip(declaration['parameter_types'], declaration['parameter_injected']) if not injected]
        require(all(len(row) == len(source_types) for row in values), 'CSV row and source parameter arity differs')
        declaration['argument_digests'] = [argument_digest(with_injected([canonical_literal(value, typename) for value, typename in zip(row, source_types)], declaration)) for row in values]
    elif 'EnumSource' in annotations:
        argument = annotations['EnumSource']
        enum = re.search(r'(?:value\s*=\s*)?([\w.]+)\.class', argument)
        require(enum is not None, 'implicit EnumSource requires an independent parameter contract')
        names = re.search(r'\bnames\s*=\s*(\{[\s\S]*?\}|"(?:\\.|[^"\\])*")', argument)
        mode = re.search(r'\bmode\s*=\s*([\w.]+)', argument)
        require(mode is None or mode.group(1).endswith(('INCLUDE', 'EXCLUDE')), 'regex EnumSource requires an independent parameter contract')
        simple = enum.group(1).rsplit('.', 1)[-1]
        constants = resolve_enum(root, source, enum.group(1), sources)
        selected = literals(names.group(1), text) if names else constants
        values = [[enum.group(1), v] for v in constants if (v not in selected if mode and mode.group(1).endswith('EXCLUDE') else v in selected)]
        binary_name = enum_qualified_name(root, source, enum.group(1), sources)
        declaration['argument_digests'] = [argument_digest(with_injected([['enum', binary_name, value[1]]], declaration)) for value in values]
    elif 'MethodSource' in annotations:
        arguments = static_provider_arguments(root, source, declaration, sources)
        values = arguments
        declaration['argument_digests'] = [argument_digest(with_injected(args, declaration)) for args in arguments]
    elif any(name in annotations for name in ('NullSource', 'EmptySource', 'NullAndEmptySource')):
        pass
    else:
        providers = annotations.get('MethodSource', annotations.get('ArgumentsSource', 'unknown'))
        raise ValueError('computed parameter provider requires an independent parameter contract: ' + providers)
    if 'NullSource' in annotations or 'NullAndEmptySource' in annotations:
        values.append(['null'])
        declaration['argument_digests'].append(argument_digest(with_injected([['null']], declaration)))
    if 'EmptySource' in annotations or 'NullAndEmptySource' in annotations:
        require(declaration['parameter_types'] == ['String'], 'non-string EmptySource requires an independent parameter contract')
        values.append(['empty'])
        declaration['argument_digests'].append(argument_digest(with_injected([['java.lang.String', '']], declaration)))
    require(values, 'empty parameter source')
    encoded = [json.dumps(value, separators=(',', ':'), ensure_ascii=True) for value in values]
    require(len(encoded) == len(set(encoded)) and len(declaration['argument_digests']) == len(set(declaration['argument_digests'])), 'duplicate stable parameter values require explicit occurrence identities')
    return encoded, 'argument-marker'


def archive_source_sha(root, archive_path, receipt_path):
    archive, receipt_file = Path(archive_path).resolve(), Path(receipt_path).resolve()
    receipt = load(receipt_file)
    require(type(receipt.get('schema')) is int and receipt['schema'] == 1 and receipt.get('repository') == 'tapstate/tapstate', 'source receipt repository/schema differs')
    require(isinstance(receipt.get('source_sha'), str) and re.fullmatch(r'[0-9a-f]{40}', receipt['source_sha']), 'source receipt commit is invalid')
    require(archive.is_relative_to(receipt_file.parent), 'source archive is outside its receipt')
    relative = archive.relative_to(receipt_file.parent).as_posix()
    declaration = receipt.get('files', {}).get(relative)
    require(declaration == dict(size=archive.stat().st_size, sha256=digest(archive.read_bytes())), 'source archive size/hash differs from authenticated receipt')
    raw_run = receipt_file.parent / 'run.json'
    require(raw_run.is_file(), 'raw source workflow run metadata is missing')
    run_declaration = receipt.get('files', {}).get('run.json')
    require(run_declaration == dict(size=raw_run.stat().st_size, sha256=digest(raw_run.read_bytes())), 'raw source run metadata hash differs')
    run = load(raw_run)
    require(run.get('head_sha') == receipt['source_sha'] and run.get('id') == receipt.get('run_id') and
            run.get('run_attempt') == receipt.get('run_attempt') and run.get('repository', {}).get('full_name') == receipt['repository'], 'raw source run origin differs from receipt')
    declared = {}
    with tarfile.open(archive) as tar:
        require(tar.pax_headers.get('comment') == receipt['source_sha'], 'git source archive commit differs from raw run')
        for member in tar.getmembers():
            name = Path(member.name.rstrip('/')).as_posix()
            require(name and not Path(name).is_absolute() and '..' not in Path(name).parts and '.git' not in Path(name).parts, 'unsafe source archive member')
            if member.isdir():
                continue
            require(member.isfile() and name not in declared, 'non-regular or duplicate source archive member')
            declared[name] = digest(tar.extractfile(member).read())
    actual = {}
    for path in root.rglob('*'):
        require(not path.is_symlink(), 'symlink in extracted source archive')
        if path.is_file():
            actual[path.relative_to(root).as_posix()] = digest(path.read_bytes())
    require(actual == declared, 'extracted source file set/bytes differs from original git archive')
    return receipt['source_sha']


def inventory(args):
    root = Path(args.root).resolve()
    require(bool(args.source_archive) == bool(args.source_receipt), 'source archive and receipt must be supplied together')
    authenticated_sha = archive_source_sha(root, args.source_archive, args.source_receipt) if args.source_archive else None
    methods, unresolved, sources, class_contracts = [], [], {}, []
    overrides = load(args.parameters) if args.parameters else {}
    used = set()
    for module, bound_it in source_modules(root):
        for source in sorted((root / module / 'src/test/java').rglob('*.java')):
            name = source.stem
            kinds = []
            if re.fullmatch(r'(?:Test.*|.*Test|.*Tests|.*TestCase)', name):
                kinds.append('unit')
            if bound_it and re.fullmatch(r'(?:IT.*|.*IT|.*ITCase)', name):
                kinds.append('it')
            if args.kind:
                kinds = [k for k in kinds if k in args.kind]
            if not kinds:
                continue
            text = source.read_text()
            clean = masked(text)
            if re.search(r'\babstract\s+class\s+' + re.escape(name) + r'\b', clean):
                continue
            package = re.search(r'\bpackage\s+([\w.]+)\s*;', clean)
            prefix = package.group(1) + '.' if package else ''
            relative = source.relative_to(root).as_posix()
            sources[relative] = digest(source.read_bytes())
            class_contracts.extend(dict(lane=args.lane, module=module, kind=kind, **contract) for kind in kinds for contract in before_all_contracts(root, source, sources))
            try:
                declared = inherited_declarations(root, source, sources)
            except ValueError as error:
                unresolved.append(dict(source=relative, reason=str(error)))
                declared = [(source, d) for d in declarations(text, name, prefix)]
            for declaring_source, declaration in declared:
                declaration_relative = declaring_source.relative_to(root).as_posix()
                if 'unresolved' in declaration:
                    unresolved.append(dict(source=relative, **declaration))
                    continue
                for kind in kinds:
                    declaration['published_connectors'] = args.published_connectors
                    declaration['native_cli_presence'] = args.native_cli_presence
                    record = dict(lane=args.lane, module=module, kind=kind, source=declaration_relative, **declaration)
                    override_key = '#'.join(str(record[k]) for k in ('lane', 'module', 'kind', 'class', 'method'))
                    try:
                        if override_key in overrides:
                            spec = overrides[override_key]
                            require(spec['source_sha256'] == sources[declaration_relative], 'parameter contract source hash differs')
                            require(spec.get('reviewed_from') and spec.get('parameters'), 'parameter contract lacks independent source or values')
                            require(spec.get('mapping') in ('ordinal-to-source', 'argument-marker', 'published-marker', 'display', 'marker'), 'invalid parameter contract mapping')
                            parameters, mapping = spec['parameters'], spec['mapping']
                            declaration['argument_digests'] = spec.get('argument_digests')
                            used.add(override_key)
                        else:
                            parameters, mapping = source_parameters(root, declaring_source, declaration, sources)
                        require(all(isinstance(p, str) for p in parameters) and len(parameters) == len(set(parameters)), 'invalid stable parameter contract')
                        record.update(parameters=parameters, mapping=mapping, argument_digests=declaration.get('argument_digests'), provider_body_sha256=declaration.get('provider_body_sha256'), published_policy=declaration.get('published_policy'), environment_contract=declaration.get('environment_contract'))
                    except ValueError as error:
                        record.update(parameters=None, mapping=None)
                        unresolved.append(dict(source=relative, **{'class': record['class']}, method=record['method'], reason=str(error), source_sha256=sources[relative]))
                    methods.append(record)
    require(methods, 'source inventory has no test declarations')
    require(not (set(overrides) - used), 'unused parameter contracts: ' + str(sorted(set(overrides) - used)))
    counts = Counter(method_key(m) for m in methods)
    require(all(n == 1 for n in counts.values()), 'duplicate or overloaded source test declaration requires explicit signature identities')
    cases = [dict((k, m[k]) for k in ('lane', 'module', 'kind', 'class', 'method')) | {'parameter': p}
             for m in methods for p in (m['parameters'] or [])]
    checkout_sha = current_sha(root)
    require(authenticated_sha is None or checkout_sha is None or checkout_sha == authenticated_sha, 'source archive origin differs from checkout')
    product_sources = {}
    if authenticated_sha:
        for path in root.rglob('*'):
            if path.is_file() and ('/src/main/' in path.relative_to(root).as_posix() or path.name == 'pom.xml'):
                product_sources[path.relative_to(root).as_posix()] = digest(path.read_bytes())
    elif checkout_sha:
        tracked = subprocess.run(['git', '-C', str(root), 'ls-files'], capture_output=True, text=True, check=True).stdout.splitlines()
        for relative in tracked:
            if '/src/main/' in relative or relative.endswith('/pom.xml') or relative == 'pom.xml':
                product_sources[relative] = digest((root / relative).read_bytes())
    origin = dict(kind='authenticated-git-archive' if authenticated_sha else 'checkout' if checkout_sha else 'unknown', source_sha=authenticated_sha or checkout_sha)
    if authenticated_sha:
        origin.update(source_archive_sha256=digest(Path(args.source_archive).read_bytes()), source_receipt_sha256=digest(Path(args.source_receipt).read_bytes()))
    value = dict(schema=SCHEMA, type='source-inventory', lane=args.lane, source_sha=authenticated_sha or checkout_sha, source_origin=origin, original_product_sources=product_sources, sources=sources,
                 methods=methods, class_contracts=class_contracts, cases=sorted(cases, key=identity), unresolved=unresolved, complete=not unresolved)
    output(args.output, value)
    print(json.dumps(dict(methods=len(methods), stable_cases=len(cases), unresolved=len(unresolved), complete=value['complete']), sort_keys=True))


def report_module(relative, modules):
    matches = [(module, kind) for module in modules for kind, directory in [('unit', 'surefire-reports'), ('it', 'failsafe-reports')]
               if '/' + module + '/target/' + directory + '/' in '/' + relative]
    require(len(matches) == 1, 'report path has no unique source module: ' + relative)
    return matches[0]


def skip_record(node):
    if node is None:
        return None
    typename, message, text = node.get('type', ''), node.get('message'), node.text or ''
    first_line = next((line for line in text.splitlines() if line), '')
    if message is not None:
        reason = message
    elif typename == 'org.opentest4j.TestAbortedException' and first_line.startswith(typename + ': '):
        # Surefire preserves the known abort reason on its first line; observer
        # frames may differ. Keep the complete original text separately.
        reason = first_line[len(typename + ': '):]
    else:
        reason = text
    return dict(type=typename, reason=reason, raw=dict(attributes=dict(node.attrib), text=text))


def skip_identity(record):
    return None if record is None else dict(type=record['type'], reason=record['reason'])


def runtime_pair(encoded, classname, method, skipped, allow_opaque=False):
    records = [json.loads(value) for value in encoded]
    require(all(isinstance(value, dict) for value in records), 'invalid runtime identity record')
    starts = [value for value in records if value.get('phase') == 'start']
    finishes = [value for value in records if value.get('phase') == 'finish']
    if len(records) != 2 or len(starts) != 1 or len(finishes) != 1:
        return None
    start, finish = starts[0], finishes[0]
    fields = ('schema', 'class', 'method', 'argument_digest', 'stable', 'unique_id',
              'display_name_digest', 'identity_kind', 'argument_descriptors', 'first_string_digest', 'repetition')
    if any(start.get(field) != finish.get(field) for field in fields):
        return None
    if not (type(start.get('schema')) is int and start['schema'] == 1 and
            type(finish.get('schema')) is int and finish['schema'] == 1 and
            type(start.get('stable')) is bool and type(finish.get('stable')) is bool and (start['stable'] or allow_opaque) and
            start.get('class') == classname and start.get('method') == method and
            isinstance(start.get('unique_id'), str) and start['unique_id'] and
            isinstance(start.get('argument_digest'), str) and re.fullmatch(r'[0-9a-f]{64}', start['argument_digest']) and
            finish.get('status') == ('aborted' if skipped else 'executed')):
        return None
    return start


def class_gate_records(directory, binding, inputs):
    require(type(inputs.get('schema')) is int and inputs['schema'] == 1 and inputs.get('phase') == 'pre-test-after-install', 'class gate inputs are not a prepared pre-test snapshot')
    require(isinstance(binding.get('source_sha'), str) and re.fullmatch(r'[0-9a-f]{40}', binding['source_sha']) and str(binding.get('run_id', '')).isdigit() and str(binding.get('attempt', '')).isdigit(), 'invalid class gate run binding')
    directory = Path(directory)
    require(directory.is_dir() and not directory.is_symlink(), 'class gate ledger directory is missing or a symlink')
    require(binding.get('source_sha') == inputs.get('source_sha') and str(binding.get('run_id')) == str(inputs.get('run_id')) and str(binding.get('attempt')) == str(inputs.get('run_attempt')), 'class gate input origin differs')
    records = []
    for path in sorted(directory.glob('*')):
        require(path.is_file() and not path.is_symlink() and re.fullmatch(r'before-all-\d+\.jsonl', path.name), 'unexpected class gate ledger member')
        for line in path.read_text().splitlines():
            value = json.loads(line)
            require(type(value.get('schema')) is int and value['schema'] == 1 and type(value.get('stable')) is bool and value['stable'], 'incomplete class gate observation')
            require(value.get('source_sha') == binding['source_sha'] and str(value.get('run_id')) == str(binding['run_id']) and str(value.get('run_attempt')) == str(binding['attempt']), 'old or foreign class gate origin')
            require(all(isinstance(value.get(field), str) and value[field] for field in ('class', 'method', 'declaring_class', 'unique_id')), 'unnamed class gate observation')
            require(value.get('phase') in ('start', 'finish'), 'invalid class gate phase')
            for category in ('properties', 'environment'):
                flags = value.get('gate_inputs', {}).get(category)
                require(isinstance(flags, dict) and all(isinstance(entry, dict) and set(entry) == {'present', 'nonblank', 'boolean_true'} and all(type(flag) is bool for flag in entry.values()) for entry in flags.values()), 'invalid class gate predicate flags')
            require(all((entry['present'] or not entry['nonblank']) and (entry['nonblank'] or not entry['boolean_true']) for category in ('properties', 'environment') for entry in value['gate_inputs'][category].values()), 'contradictory class gate predicate flags')
            records.append(value)
    groups = {}
    for value in records:
        group = (value['class'], value['declaring_class'], value['method'], value['unique_id'])
        groups.setdefault(group, []).append(value)
    paired = []
    for group, values in groups.items():
        starts = [value for value in values if value['phase'] == 'start']; finishes = [value for value in values if value['phase'] == 'finish']
        require(len(starts) == len(finishes) == 1 and len(values) == 2, 'missing or duplicate class gate phase')
        start, finish = starts[0], finishes[0]
        require(all(start.get(field) == finish.get(field) for field in ('class', 'method', 'declaring_class', 'unique_id', 'source_sha', 'run_id', 'run_attempt', 'gate_inputs')) and finish.get('status') in ('executed', 'aborted', 'failed'), 'class gate phases differ')
        paired.append(finish)
    return paired


def class_not_applicable(inv, snapshot_suites, actual_cases, records, inputs):
    result, unknown = [], []
    for suite in snapshot_suites:
        if suite['count']:
            continue
        family = [case for case in inv['cases'] if case['module'] == suite['module'] and case['kind'] == suite['kind'] and (case['class'] == suite['class'] or case['class'].startswith(suite['class'] + '$'))]
        observed = Counter(identity(case) for case in actual_cases if case.get('parameter') is not None)
        expected = [case for case in family if observed[identity(case)] != 1]
        if not expected:
            continue
        if any(observed[identity(case)] for case in family):
            unknown.append(dict(**{'class': suite['class']}, reason='zero-case family wrapper still has missing or duplicate source identities'))
            continue
        applicable = [contract for contract in inv.get('class_contracts', []) if contract['module'] == suite['module'] and contract['kind'] == suite['kind'] and contract['class'] == suite['class']]
        accepted = None
        for contract in applicable:
            metadata = [record for record in records if record['class'] == contract['class'] and record['method'] == contract['method'] and record['declaring_class'] == contract['class']]
            require(len(metadata) <= 1, 'duplicate class applicability callback')
            if not metadata:
                continue
            record, policy = metadata[0], contract['policy']
            if record['status'] != 'aborted' or record.get('reason_digest') != policy['reason_digest']:
                continue
            source_path = contract['source']; class_path = contract['module'] + '/target/test-classes/' + contract['class'].replace('.', '/') + '.class'
            require(inputs.get('source_files', {}).get(source_path) == contract['source_sha256'] and inputs.get('test_files', {}).get(class_path), 'class applicability source/compiled bytecode is not bound')
            flags = record['gate_inputs'][policy['input_kind']]
            require(all(name in flags for name in policy['inputs']), 'class gate predicates are missing')
            attribute = 'present' if policy['predicate'] == 'all-present-false' else 'nonblank' if policy['predicate'] == 'nonblank-false' else 'boolean_true'
            if all(flags[name][attribute] is False for name in policy['inputs']):
                accepted = dict(contract_sha256=digest(json.dumps(contract, sort_keys=True).encode()), source_sha256=contract['source_sha256'], compiled_class_sha256=inputs['test_files'][class_path], reason_digest=policy['reason_digest'], gate_inputs=record['gate_inputs'], callback=contract['method'])
                break
        if accepted is None:
            unknown.append(dict(**{'class': suite['class']}, reason='zero-case class requires verified original source gate and current callback evidence'))
        else:
            for case in expected:
                result.append(dict(**case, reported=None, status='not_applicable', skip=dict(type='source-before-all-gate', reason=accepted['reason_digest'], raw={}), evidence=accepted, report=suite['path'], report_sha256=None))
    return result, unknown


def reports(args):
    root = Path(args.root).resolve()
    inv = load(args.inventory)
    require(inv['type'] == 'source-inventory' and inv['lane'] == args.lane, 'inventory lane differs')
    modules = {m['module'] for m in inv['methods']}
    methods = {method_key(m): m for m in inv['methods']}
    allowed_suites = set()
    for declaration in inv['methods']:
        classname = declaration['class']
        while True:
            allowed_suites.add((declaration['module'], declaration['kind'], classname))
            if '$' not in classname:
                break
            classname = classname.rsplit('$', 1)[0]
    cases, unknown, files, suites = [], [], {}, []
    for path in sorted(root.rglob('TEST-*.xml')):
        if path.parent.name not in ('surefire-reports', 'failsafe-reports'):
            continue
        require(not path.is_symlink(), 'symlink report')
        relative = path.relative_to(root).as_posix()
        module, kind = report_module(relative, modules)
        suite = ET.parse(path).getroot()
        require(suite.tag == 'testsuite' and path.name == 'TEST-' + suite.get('name', '') + '.xml', 'report filename or root differs')
        require((module, kind, suite.get('name')) in allowed_suites, 'report suite is outside independent source declarations: ' + relative)
        actual = suite.findall('testcase')
        require(int(suite.get('tests', '-1')) == len(actual), 'test count differs from testcase records: ' + relative)
        require(int(suite.get('skipped', '0')) == sum(c.find('skipped') is not None for c in actual), 'skip count differs from testcase records')
        require(int(suite.get('errors', '0')) == 0 and int(suite.get('failures', '0')) == 0 and
                suite.find('.//error') is None and suite.find('.//failure') is None, 'non-success report: ' + relative)
        files[relative] = digest(path.read_bytes())
        suites.append(dict(module=module, kind=kind, **{'class': suite.get('name')}, count=len(actual), path=relative))
        for case in actual:
            classname = case.get('classname', suite.get('name'))
            require(classname.split('$', 1)[0] == suite.get('name', '').split('$', 1)[0], 'testcase classname differs from source-selected report family')
            reported = case.get('name', '')
            method = re.split(r'[\[(]', reported, 1)[0]
            base = dict(lane=args.lane, module=module, kind=kind, **{'class': classname}, method=method)
            declaration = methods.get(method_key(base))
            markers = [line.removeprefix('tapstate.published-case=') for out in case.findall('system-out')
                       for line in (out.text or '').splitlines() if line.startswith('tapstate.published-case=')]
            stable_markers = [line.removeprefix('tapstate.ci-case=') for out in case.findall('system-out')
                              for line in (out.text or '').splitlines() if line.startswith('tapstate.ci-case=')]
            parameter = None
            if declaration is None:
                unknown.append(dict(**base, reported=reported, reason='report has no independently discovered source method'))
            elif declaration['parameters'] is None:
                unknown.append(dict(**base, reported=reported, reason='source parameter identities are unresolved'))
            elif declaration['mapping'] in ('marker', 'argument-marker'):
                marker = runtime_pair(stable_markers, classname, method, case.find('skipped') is not None)
                digests = declaration['parameters'] if declaration['mapping'] == 'marker' else declaration.get('argument_digests') or []
                if marker is not None and marker['argument_digest'] in digests:
                    parameter = (marker['argument_digest'] if declaration['mapping'] == 'marker' else
                                 declaration['parameters'][digests.index(marker['argument_digest'])])
                else:
                    unknown.append(dict(**base, reported=reported, reason='runtime identity pair does not match independent source values and XML outcome'))
            elif declaration['mapping'] == 'dynamic-marker':
                marker = runtime_pair(stable_markers, classname, method, case.find('skipped') is not None)
                if marker and marker.get('identity_kind') == 'dynamic' and marker.get('display_name_digest') in declaration['parameters']:
                    parameter = marker['display_name_digest']
                else:
                    unknown.append(dict(**base, reported=reported, reason='dynamic execution differs from independently enumerated source names'))
            elif declaration['mapping'] == 'named-carrier-marker':
                marker = runtime_pair(stable_markers, classname, method, case.find('skipped') is not None, allow_opaque=True)
                descriptors = marker.get('argument_descriptors', []) if marker else []
                if (marker and declaration.get('provider_contract') and marker.get('first_string_digest') in declaration['parameters'] and
                        len(descriptors) == 2 and [value.get('declared_type') for value in descriptors] == declaration['projection_types'] and
                        descriptors[0].get('stable') is True and descriptors[0].get('digest') == marker['first_string_digest']):
                    parameter = marker['first_string_digest']
                else:
                    unknown.append(dict(**base, reported=reported, reason='named carrier differs from explicit source name and declared parameter contract'))
            elif declaration['mapping'] == 'published-marker':
                if len(markers) == 1:
                    parameter = markers[0]
                else:
                    unknown.append(dict(**base, reported=reported, reason='published case requires exactly one stable execution marker'))
            elif declaration['mapping'] == 'method':
                types = [re.sub(r'<.*>', '', typename).rsplit('.', 1)[-1].replace('...', '[]') for typename in declaration['parameter_types']]
                signatures = {method} if not types else set()
                signatures.add(method + '(' + ', '.join(types) + ')')
                if reported in signatures:
                    parameter = ''
                else:
                    unknown.append(dict(**base, reported=reported, reason='ordinary method has unexpected invocation name'))
            elif declaration['mapping'] == 'display':
                if reported in declaration['parameters']:
                    parameter = reported
                else:
                    unknown.append(dict(**base, reported=reported, reason='display identity differs from independent parameter contract'))
            else:
                ordinal = re.search(r'\[(\d+)\]$', reported)
                if ordinal and 1 <= int(ordinal.group(1)) <= len(declaration['parameters']):
                    parameter = declaration['parameters'][int(ordinal.group(1)) - 1]
                else:
                    unknown.append(dict(**base, reported=reported, reason='invocation cannot be mapped to independent source arguments'))
            if declaration and declaration.get('argument_digests') and declaration['mapping'] == 'ordinal-to-source' and parameter is not None:
                expected_digest = declaration['argument_digests'][declaration['parameters'].index(parameter)]
                marker = runtime_pair(stable_markers, classname, method, case.find('skipped') is not None)
                if marker is None or marker['argument_digest'] != expected_digest or marker.get('identity_kind') != 'repeated' or marker.get('repetition') != declaration['parameters'].index(parameter) + 1:
                    unknown.append(dict(**base, reported=reported, reason='runtime identity pair does not match independent repeated invocation'))
            skipped = case.find('skipped')
            cases.append(dict(**base, parameter=parameter, reported=reported, status='skipped' if skipped is not None else 'executed',
                              skip=skip_record(skipped),
                              report=relative, report_sha256=files[relative]))
    require(files, 'no JUnit reports found')
    ledgers = []
    for path in sorted(root.rglob('witness-ledger.txt')) + sorted(root.rglob('witness-ledgers/*.txt')):
        require(not path.is_symlink(), 'symlink witness ledger')
        ledgers.extend(path.read_text().splitlines())
        files[path.relative_to(root).as_posix()] = digest(path.read_bytes())
    source_not_applicable = []
    class_records = []
    run_binding, inputs_sha256, class_files = None, None, {}
    if args.class_gates:
        require(args.inputs, 'class gate ledgers require prepared run/source/test bytecode inputs')
        inputs = load(args.inputs)
        run_binding = load(args.binding) if args.binding else dict(source_sha=inputs.get('source_sha'), run_id=inputs.get('run_id'), attempt=inputs.get('run_attempt'))
        require((inv.get('provider_probe', {}).get('actual_source_sha') or inv.get('source_sha')) == inputs.get('source_sha'), 'class gate source inventory origin differs from prepared input source')
        observer_source = 'test-support/src/test/java/io/tapstate/testsupport/CiCaseEvidence.java'
        observer_class = 'test-support/target/test-classes/io/tapstate/testsupport/CiCaseEvidence.class'
        require(inputs.get('source_files', {}).get(observer_source) and inputs.get('test_files', {}).get(observer_class), 'class gate observer source/compiled bytes are not prepared')
        inputs_sha256 = digest(Path(args.inputs).read_bytes())
        class_records = class_gate_records(args.class_gates, run_binding, inputs)
        class_files = {path.name: digest(path.read_bytes()) for path in sorted(Path(args.class_gates).glob('*'))}
        source_not_applicable, class_unknown = class_not_applicable(inv, suites, cases, class_records, inputs)
        unknown.extend(class_unknown)
    else:
        _, class_unknown = class_not_applicable(inv, suites, cases, [], {})
        unknown.extend(class_unknown)
    value = dict(schema=SCHEMA, type='test-observation', lane=args.lane, source_inventory_sha256=digest(Path(args.inventory).read_bytes()),
                 cases=cases, suites=suites, class_gate_records=class_records, class_gate_files=class_files, input_binding=run_binding, inputs_sha256=inputs_sha256, source_not_applicable=source_not_applicable, source_not_applicable_count=len(source_not_applicable), actual_xml_case_count=len(cases), ledger=ledgers, files=files, unresolved=unknown, complete=inv['complete'] and not unknown)
    output(args.output, value)
    print(json.dumps(dict(cases=len(cases), source_not_applicable=len(source_not_applicable), unresolved=len(unknown), complete=value['complete']), sort_keys=True))


def counter_difference(expected, actual):
    return dict(missing=[(list(k) if isinstance(k, tuple) else [k]) + [v] for k, v in sorted((expected - actual).items())],
                extra=[(list(k) if isinstance(k, tuple) else [k]) + [v] for k, v in sorted((actual - expected).items())])


def validate_not_applicable(snapshot, inventory):
    contracts = {digest(json.dumps(contract, sort_keys=True).encode()): contract for contract in inventory.get('class_contracts', [])}
    failures = []
    for case in snapshot.get('source_not_applicable', []):
        proof = case.get('evidence', {})
        contract = contracts.get(proof.get('contract_sha256'))
        if not contract or case.get('status') != 'not_applicable' or case.get('class', '').split('$', 1)[0] != contract['class']:
            failures.append('class not-applicable identity/source contract differs')
            continue
        policy = contract['policy']
        if (proof.get('source_sha256') != contract['source_sha256'] or proof.get('reason_digest') != policy['reason_digest'] or
                proof.get('callback') != contract['method'] or not re.fullmatch(r'[0-9a-f]{64}', proof.get('compiled_class_sha256', ''))):
            failures.append('class not-applicable source/bytecode/reason binding differs')
            continue
        attribute = {'all-present-false': 'present', 'nonblank-false': 'nonblank', 'boolean-true-false': 'boolean_true'}.get(policy['predicate'])
        flags = proof.get('gate_inputs', {}).get(policy['input_kind'], {})
        if attribute is None or not all(type(flags.get(name, {}).get(attribute)) is bool and flags[name][attribute] is False for name in policy['inputs']):
            failures.append('class not-applicable source predicate is not proved')
        records = [record for record in snapshot.get('class_gate_records', []) if record.get('class') == contract['class'] and record.get('method') == contract['method'] and record.get('declaring_class') == contract['class']]
        if len(records) != 1 or records[0].get('status') != 'aborted' or records[0].get('reason_digest') != policy['reason_digest'] or records[0].get('gate_inputs') != proof.get('gate_inputs'):
            failures.append('class not-applicable actual callback evidence differs')
        if not snapshot.get('inputs_sha256') or not snapshot.get('input_binding') or not snapshot.get('class_gate_files'):
            failures.append('class not-applicable prepared input/ledger binding is missing')
    return failures


def compare_tests(args):
    inv, before, after = load(args.inventory), load(args.before), load(args.after)
    candidate_path = args.candidate_inventory or args.inventory
    candidate = load(candidate_path)
    additions = load(args.additions) if args.additions else []
    additional = Counter(identity(case) for case in additions)
    expected = Counter(identity(case) for case in inv['cases'])
    final_expected = Counter(identity(case) for case in candidate['cases'])
    original_policies = {method_key(method): method['published_policy'] for method in inv['methods'] if method.get('published_policy')}
    candidate_policies = {method_key(method): method['published_policy'] for method in candidate['methods'] if method.get('published_policy')}
    failures = []
    original_contracts = {(contract['module'], contract['kind'], contract['class'], contract['method'], contract['policy']['kind']): contract for contract in inv.get('class_contracts', [])}
    candidate_contracts = {(contract['module'], contract['kind'], contract['class'], contract['method'], contract['policy']['kind']): contract for contract in candidate.get('class_contracts', [])}
    if any(candidate_contracts.get(key) != value for key, value in original_contracts.items()):
        failures.append('original class applicability source contract changed')
    original_helpers = set()
    for contract in original_contracts.values():
        directory = str(Path(contract['source']).parent)
        if contract['policy']['kind'] == 'RealConnectorGate':
            original_helpers.update(directory + '/' + filename for filename in ('RealConnectorGate.java', 'ConnectorJars.java'))
        elif contract['policy']['kind'] == 'UpgradeLaneGate':
            original_helpers.add(directory + '/UpgradeLaneGate.java')
    if any(not inv.get('sources', {}).get(path) or candidate.get('sources', {}).get(path) != inv['sources'][path] for path in original_helpers):
        failures.append('original class gate helper source hash changed or missing')
    for method, policy in original_policies.items():
        other = candidate_policies.get(method)
        if other is None or any(policy.get(field) != other.get(field) for field in ('gate_sha256', 'synthetic_source_sha256', 'published_source_sha256')):
            failures.append('published gate source contract changed')
        elif any(other['cases'].get(case) != value for case, value in policy['cases'].items()):
            failures.append('published applicability or allowed-skip contract changed')
    if expected & additional or any(count != 1 for count in additional.values()):
        failures.append('new controls overlap original cases or contain duplicates')
    if final_expected != expected + additional:
        failures.append('candidate source case multiset differs from original plus declared new controls')
    for label, snapshot in [('inventory', inv), ('candidate inventory', candidate), ('before', before), ('after', after)]:
        if not snapshot.get('complete'):
            failures.append(label + ' contains unresolved identities')
        if label in ('before', 'after'):
            inventory_path = args.inventory if label == 'before' else candidate_path
            if snapshot.get('source_inventory_sha256') != digest(Path(inventory_path).read_bytes()):
                failures.append(label + ' source inventory binding differs')
    require(all(v == 1 for v in expected.values()), 'source inventory has duplicate case identities')
    comparisons = {}
    for label, snapshot in [('before', before), ('after', after)]:
        failures.extend(label + ' ' + failure for failure in validate_not_applicable(snapshot, inv if label == 'before' else candidate))
        all_states = snapshot['cases'] + snapshot.get('source_not_applicable', [])
        actual = Counter(identity(c) for c in all_states if c['parameter'] is not None)
        target_cases = expected if label == 'before' else final_expected
        target_inventory = inv if label == 'before' else candidate
        comparisons[label] = counter_difference(target_cases, actual)
        if actual != target_cases:
            failures.append(label + ' original case multiset differs')
        class_counts = Counter((s['module'], s['kind'], s['class']) for s in snapshot['suites'])
        if any(count != 1 and not classname.endswith('.PublishedExamplesIT') for (_, _, classname), count in class_counts.items()):
            failures.append(label + ' contains duplicate report suites')
        emitted = Counter(method_key(case) for case in all_states)
        for record in target_inventory['methods']:
            if not emitted[method_key(record)]:
                failures.append(label + ' source method executed zero cases: ' + record['class'] + '#' + record['method'])
        published_methods = [method for method in target_inventory['methods'] if method['class'].endswith('.PublishedExamplesIT')]
        if published_methods:
            policies = {published_identity: policy for method in published_methods for published_identity, policy in (method.get('published_policy') or {}).get('cases', {}).items()}
            published_cases = [case for case in snapshot['cases'] if case['class'].endswith('.PublishedExamplesIT')]
            source_published = Counter(case['parameter'] for case in target_inventory['cases'] if case['class'].endswith('.PublishedExamplesIT'))
            if Counter(policies.keys()) != source_published:
                failures.append(label + ' published applicability contract differs from independent specs and tiers')
            for case in published_cases:
                policy = policies.get(case['parameter'])
                if policy is None or case['status'] != policy['expected_status']:
                    failures.append(label + ' published execution or skip differs from source connector policy')
                elif case['status'] == 'skipped' and (case['skip'] is None or case['skip']['reason'] != policy['skip_message']):
                    failures.append(label + ' published skip reason differs from original source gate')
            expected_ledger = Counter(published_identity for published_identity, policy in policies.items() if policy['expected_status'] == 'executed')
            if Counter(snapshot['ledger']) != expected_ledger:
                failures.append(label + ' published ledger multiset differs from source applicable executed specs and tiers')
    skip_before = {identity(c): (c['status'], skip_identity(c['skip'])) for c in before['cases'] + before.get('source_not_applicable', []) if c['parameter'] is not None}
    skip_after = {identity(c): (c['status'], skip_identity(c['skip'])) for c in after['cases'] + after.get('source_not_applicable', []) if c['parameter'] is not None and identity(c) in expected}
    if skip_before != skip_after:
        failures.append('per-case skip status or reason changed')
    value = dict(schema=SCHEMA, type='test-comparison', complete=not failures, failures=failures, differences=comparisons,
                 original_cases=sum(expected.values()), additional_cases=[case for case in additions], skipped_before=sum(c['status'] == 'skipped' for c in before['cases']),
                 skipped_after=sum(c['status'] == 'skipped' for c in after['cases']))
    output(args.output, value)
    require(not failures, '; '.join(failures[:8]))


class ExecReader:
    def __init__(self, content):
        self.content, self.position = content, 0
    def take(self, size):
        require(size >= 0 and self.position + size <= len(self.content), 'truncated JaCoCo execution data')
        value = self.content[self.position:self.position + size]
        self.position += size
        return value
    def number(self, size):
        return int.from_bytes(self.take(size), 'big')
    def utf(self):
        raw = self.take(self.number(2))
        # Class and generated session names here are ASCII/UTF-8. Modified UTF-8
        # outside that subset requires the original JaCoCo reader, not guessing.
        return raw.decode('utf-8')
    def varint(self):
        result = 0
        for shift in range(0, 35, 7):
            value = self.number(1)
            result |= (value & 0x7f) << shift
            if value & 0x80 == 0:
                return result
        raise ValueError('invalid JaCoCo probe count')


def parse_exec(path):
    reader = ExecReader(path.read_bytes())
    sessions, classes, header = [], [], False
    while reader.position < len(reader.content):
        block = reader.number(1)
        if block == 0x01:
            require(reader.number(2) == 0xc0c0 and reader.number(2) == 0x1007, 'unsupported JaCoCo header/version')
            header = True
        elif block == 0x10:
            require(header, 'JaCoCo session before header')
            sessions.append(dict(id=reader.utf(), start_ms=reader.number(8), end_ms=reader.number(8)))
        elif block == 0x11:
            require(header, 'JaCoCo class before header')
            class_id, name, count = reader.number(8), reader.utf(), reader.varint()
            require(0 <= count <= 10000000, 'invalid JaCoCo class probe count')
            packed = reader.take((count + 7) // 8)
            hits = [i for i in range(count) if packed[i // 8] & (1 << (i % 8))]
            classes.append(dict(name=name, class_id=f'{class_id:016x}', probes=count, covered_probes=hits))
        else:
            raise ValueError('unknown JaCoCo execution block: ' + hex(block))
    require(header and sessions and classes, 'empty JaCoCo execution data')
    return dict(sessions=sessions, classes=classes)


def crc_table():
    table = []
    for value in range(256):
        for _ in range(8):
            value = (value >> 1) ^ (0xd800000000000000 if value & 1 else 0)
        table.append(value)
    return table


CRC_TABLE = crc_table()


def crc64(content):
    # JaCoCo uses the reflected ISO 3309 polynomial. Java 9's legacy version
    # workaround is irrelevant to the current JDK 21 bytecode, and is rejected.
    require(content[:4] == b'\xca\xfe\xba\xbe' and len(content) >= 8, 'invalid production class file')
    require(content[6:8] != b'\x00\x35', 'Java 9 class ID requires the original JaCoCo reader')
    result = 0
    for byte in content:
        result = (result >> 8) ^ CRC_TABLE[(result ^ byte) & 0xff]
    return f'{result:016x}'


def class_access_flags(content):
    require(content[:4] == b'\xca\xfe\xba\xbe' and len(content) >= 12, 'invalid compiled class flags input')
    count, position, slot = int.from_bytes(content[8:10], 'big'), 10, 1
    while slot < count:
        require(position < len(content), 'truncated class constant pool')
        tag = content[position]
        position += 1
        if tag == 1:
            require(position + 2 <= len(content), 'truncated class UTF constant')
            length = int.from_bytes(content[position:position + 2], 'big')
            position += 2 + length
        elif tag in (3, 4, 9, 10, 11, 12, 17, 18):
            position += 4
        elif tag in (5, 6):
            position += 8
            slot += 1
        elif tag in (7, 8, 16, 19, 20):
            position += 2
        elif tag == 15:
            position += 3
        else:
            raise ValueError('unsupported class constant pool tag')
        slot += 1
    require(position + 2 <= len(content), 'truncated compiled class access flags')
    return int.from_bytes(content[position:position + 2], 'big')


def cached_artifact_facts(name, snapshot):
    """Verify coordinate/type/parent facts, without guessing a Maven runtime realm."""
    hashes = snapshot['dependency_repository_files']
    models = snapshot.get('dependency_coordinate_models', {})
    producers = snapshot.get('dependency_producer_facts', {})
    model_path = name if name.endswith('.pom') else name[:-4] + '.pom'
    seen = set()
    while model_path:
        if model_path in seen or len(seen) >= 64:
            return False
        seen.add(model_path)
        model = models.get(model_path, {})
        if model.get('coordinate_type_verified') is not True:
            return False
        require(not Path(model_path).is_absolute() and '..' not in Path(model_path).parts and '\\' not in model_path, 'unsafe cached model path')
        parts = Path(model_path).parts
        expected = ['.'.join(parts[:-3]), parts[-3], parts[-2]] if len(parts) >= 4 else []
        require(model.get('gav') == expected and len(expected) == 3 and parts[-1] == expected[1] + '-' + expected[2] + '.pom', 'cached model coordinates differ from artifact path')
        require(model.get('sha256') == hashes.get(model_path) and re.fullmatch(r'[0-9a-f]{64}', model.get('sha256', '')), 'cached model bytes differ from dependency snapshot')
        require(isinstance(model.get('packaging'), str) and re.fullmatch(r'[A-Za-z0-9_.+-]+', model['packaging']), 'cached model type is invalid')
        parent = model.get('parent')
        if parent is None:
            break
        require(isinstance(parent, dict) and isinstance(parent.get('path'), str), 'invalid cached model parent')
        parent_model = models.get(parent['path'], {})
        require(parent.get('gav') == parent_model.get('gav') and parent.get('sha256') == hashes.get(parent['path']), 'cached model parent bytes or coordinates differ')
        model_path = parent['path']
    if name.endswith('.jar'):
        producer = producers.get(name, {})
        model_path = name[:-4] + '.pom'
        if producer.get('archive_type_verified') is not True or producer.get('producer_identity_verified') is not True:
            return False
        require(models[model_path]['packaging'] == 'jar', 'cached provider JAR has a non-JAR model type')
        require(producer.get('gav') == models[model_path]['gav'] and producer.get('sha256') == hashes[name] and producer.get('pom_path') == model_path and producer.get('pom_sha256') == hashes.get(model_path), 'cached JAR producer model/bytes differ')
        require(producer.get('producer_identity_source') in ('embedded-pom-properties', 'embedded-implementation-manifest') and type(producer.get('pom_properties_present')) is bool, 'unsupported cached JAR producer evidence')
        require(type(producer.get('own_class_count')) is int and producer['own_class_count'] >= 0, 'cached JAR own-class count is missing')
        if producer['own_class_count']:
            return False
    return True


def coverage(args):
    root = Path(args.root).resolve()
    binding = load(args.binding)
    for key in ('run_id', 'attempt', 'source_sha', 'started_ms', 'completed_ms', 'required_exec', 'required_reports', 'required_report_classes'):
        require(key in binding, 'missing coverage run binding: ' + key)
    require(binding['run_id'] and int(binding['attempt']) >= 1 and re.fullmatch(r'[0-9a-f]{40}', binding['source_sha']), 'invalid coverage run binding')
    require(0 < binding['started_ms'] <= binding['completed_ms'], 'invalid coverage run time bounds')
    declared = binding['required_exec']
    require(declared and len(declared) == len(set(declared)), 'empty or duplicate required exec list')
    found = {p.relative_to(root).as_posix() for p in root.rglob('*.exec')}
    require(found == set(declared), 'coverage exec path set differs from run binding')
    required_reports = binding['required_reports']
    require(isinstance(required_reports, list) and required_reports and len(required_reports) == len(set(required_reports)), 'empty or duplicate required coverage report set')
    found_reports = {path.relative_to(root).as_posix() for path in root.rglob('jacoco.xml')}
    require(found_reports == set(required_reports), 'coverage XML path set differs from declared production modules')
    production, test_classes, expected_names, production_modules = {}, {}, {}, {}
    repository_growth = {}
    dependency_unknown = []
    observed_sha = current_sha(Path(args.production_root).resolve() if args.production_root else root)
    require(observed_sha is None or observed_sha == binding['source_sha'], 'coverage source SHA differs from production checkout')
    production_root = Path(args.production_root).resolve() if args.production_root else root
    if args.inputs:
        inputs = load(args.inputs)
        require(type(inputs.get('schema')) is int and inputs['schema'] == 1 and inputs.get('phase') == 'pre-test-after-install', 'invalid coverage build inputs schema/phase')
        require(inputs.get('source_sha') == binding['source_sha'] and str(inputs.get('run_id')) == str(binding['run_id']) and
                str(inputs.get('run_attempt')) == str(binding['attempt']), 'coverage build inputs source/run/attempt differs')
        require(inputs.get('jacoco_version') == '0.8.13', 'unsupported compiled coverage analyzer version')
        require(isinstance(inputs.get('production_class_access_flags'), dict) and set(inputs['production_class_access_flags']) == set(inputs['production_class_ids']), 'production class access flag set differs')
        if args.test_inputs:
            post = load(args.test_inputs)
            require(type(post.get('schema')) is int and post['schema'] == 1 and post.get('phase') == 'post-test-before-pack' and post.get('source_sha') == binding['source_sha'] and str(post.get('run_id')) == str(binding['run_id']) and str(post.get('run_attempt')) == str(binding['attempt']), 'post-test bytecode input origin differs')
            for field in ('production_files', 'production_class_ids', 'production_class_access_flags', 'resolved_test_classpaths', 'jacoco_version'):
                require(post.get(field) == inputs.get(field), 'production/dependency bytes changed during tests: ' + field)
            before_repository, after_repository = inputs.get('dependency_repository_files'), post.get('dependency_repository_files')
            require(isinstance(before_repository, dict) and isinstance(after_repository, dict), 'dependency repository snapshot is missing')
            require(all(after_repository.get(name) == checksum for name, checksum in before_repository.items()), 'existing dependency repository bytes changed or disappeared')
            for name in sorted(after_repository.keys() - before_repository.keys()):
                require(not Path(name).is_absolute() and '..' not in Path(name).parts and re.fullmatch(r'[0-9a-f]{64}', after_repository[name]), 'malformed new dependency repository entry')
                family = ('surefire-provider' if re.fullmatch(r'org/apache/maven/surefire/(?:common-java5|surefire-junit-platform|surefire-providers)/[^/]+/[^/]+\.(?:jar|pom)', name) else
                          'junit-platform-provider' if re.fullmatch(r'org/junit/platform/junit-platform-(?:commons|engine|launcher)/[^/]+/[^/]+\.(?:jar|pom)', name) else
                          'opentest4j-provider' if re.fullmatch(r'org/opentest4j/opentest4j/[^/]+/[^/]+\.(?:jar|pom)', name) else 'unclassified')
                verified = cached_artifact_facts(name, post) if family != 'unclassified' else False
                repository_growth[name] = dict(sha256=after_repository[name], classification=family,
                                               coordinate_type_producer_verified=verified,
                                               effective_dependency_model_proven=False, runtime_realm_proven=False)
                if not verified:
                    dependency_unknown.append('new tool/cache dependency needs byte-bound coordinate/type/producer facts: ' + name)
            test_metadata = post
        else:
            test_metadata = inputs
        for segment, id_field, hash_field, destination in [('/target/classes/', 'production_class_ids', 'production_files', production),
                                                         ('/target/test-classes/', 'test_class_ids', 'test_files', test_classes)]:
            metadata = inputs if destination is production else test_metadata
            require(isinstance(metadata.get(id_field), dict) and isinstance(metadata.get(hash_field), dict), 'missing compiled bytecode input metadata')
            for relative, class_id in metadata[id_field].items():
                require(not Path(relative).is_absolute() and '..' not in Path(relative).parts and segment in relative and relative.endswith('.class'), 'unsafe compiled bytecode metadata path')
                file_hash = metadata[hash_field].get(relative)
                require(isinstance(file_hash, str) and re.fullmatch(r'[0-9a-f]{64}', file_hash) and
                        isinstance(class_id, str) and re.fullmatch(r'[0-9a-f]{16}', class_id), 'invalid compiled bytecode input hash or class ID')
                module, classname = relative.split(segment, 1)
                destination[module + '/' + classname.removesuffix('.class')] = dict(sha256=file_hash, class_id=class_id)
                expected_names.setdefault(classname.removesuffix('.class'), set()).add(class_id)
                if destination is production:
                    flags = inputs['production_class_access_flags'][relative]
                    require(type(flags) is int and 0 <= flags <= 65535, 'invalid production class access flags')
                    if not flags & (0x8000 | 0x1000):
                        production_modules.setdefault(module, set()).add(classname.removesuffix('.class'))
        for classpath in inputs.get('resolved_test_classpaths', {}).values():
            for entry in classpath:
                require(isinstance(entry.get('sha256'), str) and re.fullmatch(r'[0-9a-f]{64}', entry['sha256']) and isinstance(entry.get('own_classes'), dict), 'resolved own dependency bytecode metadata is missing')
                for name, record in entry['own_classes'].items():
                    require(name.startswith('io/tapstate/') and isinstance(record, dict) and re.fullmatch(r'[0-9a-f]{16}', record.get('class_id', '')) and re.fullmatch(r'[0-9a-f]{64}', record.get('sha256', '')), 'invalid resolved dependency bytecode metadata')
                    expected_names.setdefault(name, set()).add(record['class_id'])
    else:
        for path in sorted(production_root.rglob('*.class')):
            relative = path.relative_to(production_root).as_posix()
            segment = '/target/classes/' if '/target/classes/' in '/' + relative else '/target/test-classes/' if '/target/test-classes/' in '/' + relative else None
            if segment is None:
                continue
            require(not path.is_symlink(), 'symlink production class')
            module, classname = relative.split(segment, 1)
            key = module + '/' + classname.removesuffix('.class')
            destination = production if segment == '/target/classes/' else test_classes
            destination[key] = dict(sha256=digest(path.read_bytes()), class_id=crc64(path.read_bytes()))
            expected_names.setdefault(classname.removesuffix('.class'), set()).add(destination[key]['class_id'])
            if destination is production and not class_access_flags(path.read_bytes()) & (0x8000 | 0x1000):
                production_modules.setdefault(module, set()).add(classname.removesuffix('.class'))
    require(production, 'production class bytes are required to verify JaCoCo class IDs')
    require(all(len(values) == 1 for values in expected_names.values()), 'conflicting production class IDs')
    report_classes = binding['required_report_classes']
    require(isinstance(report_classes, dict) and set(report_classes) == set(required_reports), 'declared coverage report class sets differ')
    report_modules = {}
    for report_path, names in report_classes.items():
        candidates = [module for module in production_modules if ('/' + module + '/target/') in ('/' + report_path)]
        require(len(candidates) == 1, 'coverage report has no unique production module')
        module = candidates[0]
        require(set(names) == production_modules[module], 'declared coverage class set differs from compiled production module')
        report_modules[report_path] = module
    require(set(report_modules.values()) == set(production_modules), 'coverage reports omit compiled production modules')
    for report_path, names in report_classes.items():
        require(isinstance(names, list) and names and len(names) == len(set(names)) and all(isinstance(name, str) and name in expected_names for name in names), 'coverage report class inventory is missing or foreign')
    executions = {}
    for relative in declared:
        path = root / relative
        require(not Path(relative).is_absolute() and '..' not in Path(relative).parts and not path.is_symlink(), 'unsafe coverage path')
        parsed = parse_exec(path)
        for session in parsed['sessions']:
            require(binding['started_ms'] <= session['start_ms'] <= session['end_ms'] <= binding['completed_ms'],
                    'stale or out-of-run coverage session: ' + relative)
        for record in parsed['classes']:
            if record['name'].startswith('io/tapstate/'):
                require(record['name'] in expected_names, 'undeclared own bytecode in JaCoCo data: ' + record['name'])
            if record['name'] in expected_names:
                require(record['class_id'] in expected_names[record['name']], 'JaCoCo class ID differs from production: ' + record['name'])
        executions[relative] = dict(sha256=digest(path.read_bytes()), size=path.stat().st_size, **parsed)
    hits, reports_found, unresolved = [], [], list(dependency_unknown)
    session_keys = {(s['id'], s['start_ms'], s['end_ms']) for data in executions.values() for s in data['sessions']}
    for path in sorted(root.rglob('jacoco.xml')):
        require(not path.is_symlink(), 'symlink coverage report')
        tree = ET.parse(path).getroot()
        require(tree.tag == 'report', 'invalid JaCoCo XML')
        report_path = path.relative_to(root).as_posix()
        xml_classes = [node.get('name') for node in tree.findall('package/class')]
        require(len(xml_classes) == len(set(xml_classes)) and set(xml_classes) == set(report_classes[report_path]), 'coverage XML production class set differs: ' + report_path)
        xml_sessions = {(s.get('id'), int(s.get('start', '-1')), int(s.get('dump', '-1'))) for s in tree.findall('sessioninfo')}
        if not xml_sessions:
            unresolved.append('coverage XML lacks session provenance: ' + report_path)
        require(xml_sessions <= session_keys, 'coverage XML references old or foreign sessions: ' + report_path)
        reports_found.append(dict(path=report_path, sha256=digest(path.read_bytes())))
        for package in tree.findall('package'):
            for source in package.findall('sourcefile'):
                for line in source.findall('line'):
                    if int(line.get('ci', '0')) or int(line.get('cb', '0')):
                        hits.append([report_path, package.get('name'), source.get('name'), int(line.get('nr')),
                                     int(line.get('ci', '0')), int(line.get('cb', '0'))])
    value = dict(schema=SCHEMA, type='coverage-observation', binding=binding, production=production, test_classes=test_classes, executions=executions,
                 reports=reports_found, covered_lines_branches=hits, dependency_repository_growth=repository_growth, dependency_growth_accounting='all additions retained; download and verification cost remains inside original steps', unresolved=unresolved, complete=bool(reports_found) and not unresolved)
    output(args.output, value)
    print(json.dumps(dict(exec_files=len(executions), production_classes=len(production), coverage_xml=len(reports_found), complete=value['complete']), sort_keys=True))


def production_probe_sets(snapshot):
    result = {}
    for execution in snapshot['executions'].values():
        for record in execution['classes']:
            matching = [entry for name, entry in snapshot['production'].items() if name.endswith('/' + record['name']) and entry['class_id'] == record['class_id']]
            if matching:
                key = record['name'] + ':' + record['class_id']
                item = result.setdefault(key, dict(probes=record['probes'], covered=set()))
                require(item['probes'] == record['probes'], 'same production class has inconsistent probe layouts')
                item['covered'].update(record['covered_probes'])
    return result


def compare_coverage(args):
    before, after = load(args.before), load(args.after)
    failures = []
    if not before.get('complete') or not after.get('complete'):
        failures.append('coverage XML or provenance is incomplete')
    if before['production'] != after['production']:
        failures.append('production bytes or JaCoCo class IDs changed')
    original = {tuple(v[:4]): v[4:] for v in before['covered_lines_branches']}
    candidate = {tuple(v[:4]): v[4:] for v in after['covered_lines_branches']}
    reduced = [list(k) for k, v in original.items() if k not in candidate or any(b < a for a, b in zip(v, candidate[k]))]
    if reduced:
        failures.append('original covered instructions or branches decreased; repeated controls must explain nondeterminism')
    before_probes, after_probes = production_probe_sets(before), production_probe_sets(after)
    probe_reductions = {}
    for name, entry in before_probes.items():
        candidate_entry = after_probes.get(name)
        missing = entry['covered'] - (candidate_entry['covered'] if candidate_entry else set())
        if candidate_entry and candidate_entry['probes'] != entry['probes']:
            failures.append('production probe layout changed: ' + name)
        if missing:
            probe_reductions[name] = sorted(missing)
    if probe_reductions:
        failures.append('original production probe hits decreased; repeated controls must explain nondeterminism')
    output(args.output, dict(schema=SCHEMA, type='coverage-comparison', complete=not failures, failures=failures, reductions=reduced, production_probe_reductions=probe_reductions))
    require(not failures, '; '.join(failures))


def gate_contract(snapshot):
    require(type(snapshot.get('schema')) is int and snapshot['schema'] == SCHEMA, 'unsupported gate snapshot schema')
    require(isinstance(snapshot.get('gates'), list) and snapshot['gates'], 'empty gate contract')
    for gate in snapshot['gates']:
        require(isinstance(gate, dict) and isinstance(gate.get('id'), str) and gate['id'].strip(), 'invalid gate instance ID')
        require(type(gate.get('required')) is bool and type(gate.get('applicable')) is bool, 'gate required/applicable must be booleans')
        require(gate.get('scope') in ('always', 'conditional'), 'invalid gate applicability scope')
        require(isinstance(gate.get('contract_sha256'), str) and re.fullmatch(r'[0-9a-f]{64}', gate['contract_sha256']), 'gate contract is not frozen')
        if gate['required'] or gate['scope'] == 'always':
            require(gate['applicable'], 'required or always gate cannot be non-applicable')
        if gate['scope'] == 'conditional':
            basis = gate.get('applicability')
            require(isinstance(basis, dict), 'conditional gate needs a structured applicability contract')
            require(type(basis.get('decision')) is bool and basis['decision'] == gate['applicable'], 'gate applicability decision differs')
            require(isinstance(basis.get('reason'), str) and basis['reason'].strip(), 'gate applicability reason is missing')
            for field in ('condition_sha256', 'inputs_sha256'):
                require(isinstance(basis.get(field), str) and re.fullmatch(r'[0-9a-f]{64}', basis[field]), 'gate applicability hash is missing or invalid')
        if gate['applicable']:
            require(gate.get('conclusion') == 'success', 'applicable gate is absent, skipped, canceled or unsuccessful')
        else:
            require(gate.get('conclusion') in ('not_applicable', 'skipped', None), 'non-applicable gate outcome is inconsistent')


def gates(args):
    baseline, candidate = load(args.baseline), load(args.candidate)
    gate_contract(baseline)
    gate_contract(candidate)
    expected = {g['id']: g for g in baseline['gates']}
    actual = {g['id']: g for g in candidate['gates']}
    require(expected, 'empty baseline gate contract')
    require(len(expected) == len(baseline['gates']) and len(actual) == len(candidate['gates']), 'duplicate gate instances')
    failures = []
    if set(expected) != set(actual):
        failures.append('gate instance set changed')
    for name in expected.keys() & actual.keys():
        old, new = expected[name], actual[name]
        if any(old.get(k) != new.get(k) for k in ('required', 'scope', 'contract_sha256')):
            failures.append('gate applicability or contract changed: ' + name)
        if old.get('applicable') != new.get('applicable') or old.get('applicability') != new.get('applicability'):
            conditional = old.get('scope') == new.get('scope') == 'conditional'
            old_basis, new_basis = old.get('applicability'), new.get('applicability')
            supported = conditional and isinstance(old_basis, dict) and isinstance(new_basis, dict)
            supported = supported and old_basis.get('condition_sha256') == new_basis.get('condition_sha256')
            supported = supported and all(basis.get('inputs_sha256') and basis.get('reason') and basis.get('decision') == record.get('applicable')
                                          for basis, record in [(old_basis, old), (new_basis, new)])
            if not supported:
                failures.append('gate applicability or contract changed: ' + name)
        if old.get('applicable') and old.get('conclusion') != 'success':
            failures.append('baseline applicable gate is unsuccessful: ' + name)
        if new.get('applicable') and new.get('conclusion') != 'success':
            failures.append('applicable gate is absent, skipped, canceled or unsuccessful: ' + name)
        if not new.get('applicable') and not new.get('applicability'):
            failures.append('non-applicable gate lacks evidence: ' + name)
        if not re.fullmatch(r'[0-9a-f]{64}', new.get('contract_sha256', '')):
            failures.append('gate contract is not frozen: ' + name)
    output(args.output, dict(schema=SCHEMA, type='gate-comparison', complete=not failures, failures=failures))
    require(not failures, '; '.join(failures))


def artifacts(args):
    root = Path(args.root).resolve()
    manifest, binding = load(args.manifest), load(args.binding)
    for key in ('run_id', 'attempt', 'source_sha'):
        require(key in binding and manifest.get(key) == binding[key], 'artifact origin binding differs: ' + key)
    actual = {}
    for path in sorted(root.rglob('*')):
        require(not path.is_symlink(), 'symlink artifact member')
        if path.is_file():
            relative = path.relative_to(root).as_posix()
            actual[relative] = digest(path.read_bytes())
    declared = manifest.get('files')
    require(isinstance(declared, dict) and declared, 'empty artifact manifest')
    for name in declared:
        require(not Path(name).is_absolute() and '..' not in Path(name).parts, 'unsafe artifact member path')
    require(actual == declared, 'artifact hash or file set differs from manifest')
    output(args.output, dict(schema=SCHEMA, type='artifact-admission', complete=True, binding=binding, files=actual))


def resolve_providers(args):
    root, repository = Path(args.root).resolve(), Path(args.repo_local).resolve()
    inv, inputs, contracts = load(args.inventory), load(args.inputs), load(args.contracts)
    require(inv['type'] == 'source-inventory' and contracts.get('schema') == 1, 'invalid provider inventory/contract schema')
    require(inputs.get('schema') == 1 and inputs.get('phase') == 'pre-test-after-install', 'provider probe requires pre-test prepared inputs')
    if inputs.get('source_sha') != inv.get('source_sha'):
        require(args.project_original and inv.get('source_origin', {}).get('kind') == 'authenticated-git-archive', 'provider probe source differs without authenticated original projection')
        require(inv.get('original_product_sources'), 'original product input hashes are missing')
        for relative, expected in inv['original_product_sources'].items():
            require(inputs.get('source_files', {}).get(relative) == expected and (root / relative).is_file() and digest((root / relative).read_bytes()) == expected, 'original product source changed during HOW projection: ' + relative)
    original_sha = digest(Path(args.inventory).read_bytes())
    problems = []
    for relative, expected in inv['sources'].items():
        path = root / relative
        require(path.is_file() and not path.is_symlink() and digest(path.read_bytes()) == expected, 'provider source/resources differ from original inventory: ' + relative)
        if inputs.get('source_files', {}).get(relative) != expected:
            problems.append('source not bound to prepared inputs: ' + relative)
    require(args.diagnostic or not problems, 'provider sources lack prepared input provenance')
    pending = [method for method in inv['methods'] if method['parameters'] is None]
    grouped, selected = {}, {}
    for method in pending:
        contract = contracts['classes'].get(method['class'])
        if contract is None:
            continue
        path = root / contract['source']
        require(digest(path.read_bytes()) == contract['source_sha256'] and contract['module'] == method['module'], 'reviewed provider source contract changed')
        if 'TestFactory' in method['annotations']:
            name, kind = method['method'], 'dynamic'
        elif 'MethodSource' in method['annotations']:
            providers = literals(method['annotations']['MethodSource'], path.read_text())
            require(len(providers) == 1, 'multiple provider resolution is unsupported')
            name, kind = providers[0], 'source'
        else:
            continue
        approved = contract['providers'].get(name)
        require(approved and approved['kind'] == kind, 'provider is outside the reviewed source whitelist')
        grouped.setdefault(method['module'], []).append((method, name, kind, approved['projection']))
        selected[method_key(method)] = (contract, name, kind, approved['projection'])
    helper_root = root / 'test-support/target/test-classes'
    helper_files = [path for path in sorted((helper_root / 'io/tapstate/testsupport').glob('Ci*.class'))
                    if path.name.startswith(('CiCaseEvidence', 'CiProviderInventory')) and 'Test' not in path.name]
    require(any(path.name == 'CiProviderInventory.class' for path in helper_files), 'provider helper is not compiled')
    for path in helper_files:
        relative = path.relative_to(root).as_posix()
        if inputs.get('test_files', {}).get(relative) != digest(path.read_bytes()):
            problems.append('provider helper not bound to prepared inputs: ' + relative)
    require(args.diagnostic or not problems, 'provider helper lacks prepared input provenance')
    observed, resource_hashes, classpath_bindings = {}, {}, {}
    with tempfile.TemporaryDirectory() as temporary:
        temp = Path(temporary)
        helper = temp / 'provider-helper.jar'
        with zipfile.ZipFile(helper, 'w') as jar:
            for path in helper_files:
                jar.write(path, path.relative_to(helper_root).as_posix())
        for module, rows in grouped.items():
            requests, result = temp / 'requests.tsv', temp / 'result.jsonl'
            requests.write_text(''.join('\t'.join([method['class'], method['method'], name, kind]) + '\n' for method, name, kind, _ in rows))
            classpath = [str(helper), str(root / module / 'target/test-classes'), str(root / module / 'target/classes')]
            entries = inputs.get('resolved_test_classpaths', {}).get(module)
            require(isinstance(entries, list) and entries, 'prepared provider module classpath is missing: ' + module)
            classpath_bindings[module] = entries
            for entry in entries:
                classpath_identity = entry['identity']
                if classpath_identity.startswith('repository/'):
                    path = repository / classpath_identity.removeprefix('repository/')
                elif classpath_identity.startswith('checkout/'):
                    path = root / classpath_identity.removeprefix('checkout/')
                else:
                    raise ValueError('provider classpath identity is outside declared inputs')
                require(path.is_file() and digest(path.read_bytes()) == entry['sha256'], 'provider classpath bytes differ from prepared inputs')
                classpath.append(str(path))
            for location in ('src/test/resources', 'src/main/resources', 'target/test-classes', 'target/classes'):
                directory = root / module / location
                for path in sorted(directory.rglob('*')) if directory.exists() else []:
                    require(not path.is_symlink(), 'symlink provider resource')
                    if path.is_file():
                        resource_hashes[path.relative_to(root).as_posix()] = digest(path.read_bytes())
            run = subprocess.run([args.java, '-cp', os.pathsep.join(classpath), 'io.tapstate.testsupport.CiProviderInventory', str(requests), str(result)],
                                 cwd=root / module, capture_output=True, text=True, timeout=120)
            require(run.returncode == 0 and result.is_file(), 'bounded provider enumeration failed for module ' + module)
            for line in result.read_text().splitlines():
                row = json.loads(line)
                key = (inv['lane'], module, next(method['kind'] for method, _, _, _ in rows if method['class'] == row['class'] and method['method'] == row['test_method']), row['class'], row['test_method'])
                require(key not in observed and key in selected and row.get('schema') == 1, 'duplicate or foreign provider probe record')
                class_path = module + '/target/test-classes/' + row['class'].replace('.', '/') + '.class'
                require(row['provider_class_sha256'] == inputs.get('test_files', {}).get(class_path), 'provider class differs from prepared bytecode')
                observed[key] = row
    require(set(observed) == set(selected), 'provider probe set differs from independent source requests')
    resolved = []
    for method in inv['methods']:
        key = method_key(method)
        if key not in observed:
            continue
        row, (contract, name, kind, projection) = observed[key], selected[key]
        require(row['cases'] and len(row['cases']) <= 4096, 'empty or unbounded source provider')
        if kind == 'dynamic':
            parameters = [case['display_name_digest'] for case in row['cases']]
            method.update(parameters=parameters, mapping='dynamic-marker', argument_digests=None)
        elif projection == 'named-carrier':
            require(method['class'] == 'io.tapstate.e2e.JoinConformanceIT' and name == 'carriers', 'unreviewed opaque projection')
            require(all(case.get('first_string_digest') and len(case['argument_descriptors']) == 2 and case['argument_descriptors'][0]['stable'] is True for case in row['cases']), 'carrier lacks a stable explicit source name')
            parameters = [case['first_string_digest'] for case in row['cases']]
            method.update(parameters=parameters, mapping='named-carrier-marker', argument_digests=None,
                          projection_types=['java.lang.String', 'java.util.function.BiFunction'])
        else:
            require(all(case.get('stable') is True for case in row['cases']), 'source provider arguments cannot be represented stably: ' + row['class'])
            parameters = [case['argument_digest'] for case in row['cases']]
            method.update(parameters=parameters, mapping='argument-marker', argument_digests=parameters)
        require(all(re.fullmatch(r'[0-9a-f]{64}', value) for value in parameters) and len(parameters) == len(set(parameters)), 'duplicate or invalid stable provider cases')
        method['provider_contract'] = dict(source_sha256=contract['source_sha256'], provider=name, provider_class_sha256=row['provider_class_sha256'])
        resolved.append((method['source'], method['method']))
    inv['unresolved'] = [gap for gap in inv['unresolved'] if (gap.get('source'), gap.get('method')) not in resolved]
    if args.diagnostic:
        problems.append('diagnostic mode cannot establish accepted workflow provenance')
    if problems:
        inv['unresolved'].append(dict(reason='diagnostic probe has incomplete prepared-source/helper provenance', problems=problems))
    inv['cases'] = sorted([dict((field, method[field]) for field in ('lane', 'module', 'kind', 'class', 'method')) | dict(parameter=parameter)
                           for method in inv['methods'] for parameter in method['parameters'] or []], key=identity)
    inv['complete'] = not inv['unresolved']
    inv['provider_probe'] = dict(schema=1, original_inventory_sha256=original_sha, inputs_sha256=digest(Path(args.inputs).read_bytes()),
                                 contracts_sha256=digest(Path(args.contracts).read_bytes()), before_test_selection=True,
                                 resolved_records=list(observed.values()), resource_hashes=resource_hashes,
                                 resolved_classpaths=classpath_bindings, diagnostic=bool(problems), actual_source_sha=inputs['source_sha'], original_source_sha=inv['source_sha'], project_original=bool(args.project_original))
    output(args.output, inv)
    print(json.dumps(dict(resolved_methods=len(resolved), unresolved=len(inv['unresolved']), stable_cases=len(inv['cases']), complete=inv['complete']), sort_keys=True))


def case_hash(record):
    return digest(json.dumps(list(identity(record)), separators=(',', ':'), ensure_ascii=False).encode())


def export_hashes(args):
    value = load(args.input)
    require(value.get('schema') == SCHEMA and value.get('type') in ('source-inventory', 'test-observation'), 'unsupported evidence hash export')
    method_index = {method_key(method): method for method in value.get('methods', [])}
    cases = []
    for record in value['cases'] + value.get('source_not_applicable', []):
        require(record.get('parameter') is not None, 'unresolved case cannot be exported as an expected identity')
        row = {field: record[field] for field in ('lane', 'module', 'kind', 'class', 'method')}
        row.update(case_digest=case_hash(record), parameter_digest=digest(record['parameter'].encode()))
        method = method_index.get(method_key(record))
        if method:
            row['mapping'] = method['mapping']
            position = method['parameters'].index(record['parameter'])
            if method.get('argument_digests'):
                row['argument_digest'] = method['argument_digests'][position]
            if method['mapping'] == 'dynamic-marker':
                row['display_name_digest'] = record['parameter']
            if method['mapping'] == 'named-carrier-marker':
                row['named_key_digest'] = record['parameter']
                row['projection_types'] = method['projection_types']
            if method['mapping'] == 'ordinal-to-source':
                row['repetition'] = position + 1
            policy = (method.get('published_policy') or {}).get('cases', {}).get(record['parameter'])
            if policy:
                row['execution_contract'] = dict(expected_status=policy['expected_status'], reason_sha256=digest(policy['skip_message'].encode()) if policy['skip_message'] else None, applicable=policy['expected_status'] == 'executed')
            else:
                row['execution_contract'] = dict(expected_status='requires-entry-contract', applicable=None)
        if value['type'] == 'test-observation':
            row.update(status=record['status'], skip=(None if record['skip'] is None else dict(type=record['skip']['type'], reason_sha256=digest(record['skip']['reason'].encode()))))
            if record['status'] == 'not_applicable':
                row['source_gate_evidence'] = {field: record['evidence'][field] for field in ('contract_sha256', 'source_sha256', 'compiled_class_sha256', 'reason_digest', 'callback')}
                row['source_gate_evidence']['predicate_flags_sha256'] = digest(json.dumps(record['evidence']['gate_inputs'], sort_keys=True, separators=(',', ':')).encode())
        cases.append(row)
    methods = []
    for method in value.get('methods', []):
        row = {field: method[field] for field in ('lane', 'module', 'kind', 'class', 'method', 'source')}
        row['declaration_sha256'] = method['declaration_sha256']
        row['mapping'] = method['mapping']
        row['signature_types'] = [re.sub(r'<.*>', '', typename).rsplit('.', 1)[-1].replace('...', '[]') for typename in method['parameter_types']]
        row['source_sha256'] = value['sources'].get(method['source'])
        row['provider_contract_sha256'] = digest(json.dumps(method.get('provider_contract'), sort_keys=True, separators=(',', ':')).encode())
        methods.append(row)
    public = dict(schema=SCHEMA, type='expected-case-hashes' if value['type'] == 'source-inventory' else 'observed-case-hashes',
                  complete=value.get('complete') is True, source_sha=value.get('source_sha'),
                  source_inventory_sha256=value.get('source_inventory_sha256') or digest(Path(args.input).read_bytes()),
                  source_origin=({field: value['source_origin'][field] for field in ('kind', 'source_sha', 'source_archive_sha256', 'source_receipt_sha256') if field in value['source_origin']} if value.get('source_origin') else None), actual_source_sha=value.get('provider_probe', {}).get('actual_source_sha'),
                  original_source_sha=value.get('provider_probe', {}).get('original_source_sha'),
                  provider_probe_sha256=digest(json.dumps(value.get('provider_probe'), sort_keys=True, separators=(',', ':')).encode()),
                  input_binding=value.get('input_binding'), inputs_sha256=value.get('inputs_sha256'), class_gate_files=value.get('class_gate_files', {}), actual_xml_case_count=value.get('actual_xml_case_count'), source_not_applicable_count=value.get('source_not_applicable_count'), ledger_digests=[digest(entry.encode()) for entry in value.get('ledger', [])], methods=methods, cases=cases, case_count=len(cases), unresolved_count=len(value.get('unresolved', [])), ability_proven=False)
    output(args.output, public)


def compare_hashes(args):
    expected, actual = load(args.expected), load(args.actual)
    require(expected.get('schema') == actual.get('schema') == SCHEMA and expected.get('type') == 'expected-case-hashes' and actual.get('type') == 'observed-case-hashes', 'invalid expected/observed hash evidence schema')
    failures = []
    if expected.get('complete') is not True or actual.get('complete') is not True:
        failures.append('expected or observed hash evidence is incomplete')
    wanted, found = Counter(row['case_digest'] for row in expected['cases']), Counter(row['case_digest'] for row in actual['cases'])
    if wanted != found:
        failures.append('hashed original case multiset differs')
    if actual.get('source_inventory_sha256') != expected.get('source_inventory_sha256'):
        failures.append('observations are not bound to this expected source inventory')
    output(args.output, dict(schema=SCHEMA, type='case-hash-comparison', complete=False, identity_equal=not failures, ability_proven=False, mandatory_comparison='compare', failures=failures, differences=counter_difference(wanted, found)))
    require(not failures, '; '.join(failures))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='command', required=True)
    inv = sub.add_parser('inventory')
    inv.add_argument('--root', default='.')
    inv.add_argument('--source-archive', help='Original git source tar authenticated by a durable raw-run receipt')
    inv.add_argument('--source-receipt', help='Receipt with raw run/source tar hashes and original commit')
    inv.add_argument('--lane', required=True)
    inv.add_argument('--kind', choices=['unit', 'it'], action='append')
    inv.add_argument('--published-connectors', choices=['unknown', 'absent', 'available'], default='unknown', help='Frozen real connector jar availability for this original lane entry')
    inv.add_argument('--native-cli-presence', choices=['unknown', 'absent', 'present'], default='unknown', help='Frozen presence of the native CLI fixture input, without revealing its path')
    inv.add_argument('--parameters', help='Independently reviewed stable argument contracts, bound to source hashes')
    inv.add_argument('--output', required=True)
    rep = sub.add_parser('reports')
    rep.add_argument('--root', default='.')
    rep.add_argument('--lane', required=True)
    rep.add_argument('--inventory', required=True)
    rep.add_argument('--class-gates', help='Explicit task-owned hash-only BeforeAll ledger directory')
    rep.add_argument('--binding', help='Actual run/attempt/source binding for class gate receipts')
    rep.add_argument('--inputs', help='Verified prepared source/test bytecode hashes for class applicability')
    rep.add_argument('--output', required=True)
    comp = sub.add_parser('compare')
    comp.add_argument('--inventory', required=True)
    comp.add_argument('--candidate-inventory', help='Independent source inventory for the candidate checkout')
    comp.add_argument('--additions', help='Explicit identity records for new regression controls')
    comp.add_argument('--before', required=True)
    comp.add_argument('--after', required=True)
    comp.add_argument('--output', required=True)
    cov = sub.add_parser('coverage')
    cov.add_argument('--root', default='.')
    cov.add_argument('--production-root')
    cov.add_argument('--test-inputs', help='Verified post-test compiled fixture metadata; production/dependency bytes must stay identical')
    cov.add_argument('--inputs', help='Verified pre-test production/test class hash metadata from this run')
    cov.add_argument('--binding', required=True)
    cov.add_argument('--output', required=True)
    covcomp = sub.add_parser('compare-coverage')
    covcomp.add_argument('--before', required=True)
    covcomp.add_argument('--after', required=True)
    covcomp.add_argument('--output', required=True)
    gate = sub.add_parser('gates')
    gate.add_argument('--baseline', required=True)
    gate.add_argument('--candidate', required=True)
    gate.add_argument('--output', required=True)
    artifact = sub.add_parser('artifacts')
    artifact.add_argument('--root', required=True)
    artifact.add_argument('--manifest', required=True)
    artifact.add_argument('--binding', required=True)
    artifact.add_argument('--output', required=True)
    resolve = sub.add_parser('resolve')
    resolve.add_argument('--root', default='.')
    resolve.add_argument('--inventory', required=True)
    resolve.add_argument('--inputs', required=True)
    resolve.add_argument('--repo-local', required=True)
    resolve.add_argument('--contracts', default=str(Path(__file__).with_name('ci-provider-contracts.json')))
    resolve.add_argument('--java', default='java')
    resolve.add_argument('--project-original', action='store_true', help='Project authenticated unchanged original WHAT/product inputs onto a different HOW input commit')
    resolve.add_argument('--diagnostic', action='store_true', help='Keep unbound local probe observations explicitly incomplete')
    resolve.add_argument('--output', required=True)
    export = sub.add_parser('export-hashes')
    export.add_argument('--input', required=True)
    export.add_argument('--output', required=True)
    hashed = sub.add_parser('compare-hashes')
    hashed.add_argument('--expected', required=True)
    hashed.add_argument('--actual', required=True)
    hashed.add_argument('--output', required=True)
    args = parser.parse_args()
    {'inventory': inventory, 'reports': reports, 'compare': compare_tests, 'coverage': coverage,
     'compare-coverage': compare_coverage, 'gates': gates, 'artifacts': artifacts, 'resolve': resolve_providers, 'export-hashes': export_hashes, 'compare-hashes': compare_hashes}[args.command](args)


if __name__ == '__main__':
    try:
        main()
    except (ValueError, KeyError, OSError, UnicodeError, ET.ParseError) as error:
        print('ci-evidence: ' + str(error), file=sys.stderr)
        sys.exit(1)
