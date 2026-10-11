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
import json
from pathlib import Path
import re
import subprocess
import sys
import textwrap
import tarfile
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


def split_values(value):
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
        elif char in '({[':
            depth += 1
        elif char in ')}]':
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
            for parameter in split_values(text[consumed:parameter_end]):
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
    values = []
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
    else:
        providers = annotations.get('MethodSource', annotations.get('ArgumentsSource', 'unknown'))
        raise ValueError('computed parameter provider requires an independent parameter contract: ' + providers)
    if 'NullSource' in annotations:
        values.append(['null'])
        declaration['argument_digests'].append(argument_digest([['null']]))
    if 'EmptySource' in annotations:
        require(declaration['parameter_types'] == ['String'], 'non-string EmptySource requires an independent parameter contract')
        values.append(['empty'])
        declaration['argument_digests'].append(argument_digest([['java.lang.String', '']]))
    require(values, 'empty parameter source')
    encoded = [json.dumps(value, separators=(',', ':'), ensure_ascii=True) for value in values]
    require(len(encoded) == len(set(encoded)), 'duplicate stable parameter values require explicit occurrence identities')
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
    methods, unresolved, sources = [], [], {}
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
                        record.update(parameters=parameters, mapping=mapping, argument_digests=declaration.get('argument_digests'), provider_body_sha256=declaration.get('provider_body_sha256'), published_policy=declaration.get('published_policy'))
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
    value = dict(schema=SCHEMA, type='source-inventory', lane=args.lane, source_sha=authenticated_sha or checkout_sha, sources=sources,
                 methods=methods, cases=sorted(cases, key=identity), unresolved=unresolved, complete=not unresolved)
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
    value = dict(schema=SCHEMA, type='test-observation', lane=args.lane, source_inventory_sha256=digest(Path(args.inventory).read_bytes()),
                 cases=cases, suites=suites, ledger=ledgers, files=files, unresolved=unknown, complete=inv['complete'] and not unknown)
    output(args.output, value)
    print(json.dumps(dict(cases=len(cases), unresolved=len(unknown), complete=value['complete']), sort_keys=True))


def counter_difference(expected, actual):
    return dict(missing=[list(k) + [v] for k, v in sorted((expected - actual).items())],
                extra=[list(k) + [v] for k, v in sorted((actual - expected).items())])


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
        actual = Counter(identity(c) for c in snapshot['cases'] if c['parameter'] is not None)
        target_cases = expected if label == 'before' else final_expected
        target_inventory = inv if label == 'before' else candidate
        comparisons[label] = counter_difference(target_cases, actual)
        if actual != target_cases:
            failures.append(label + ' original case multiset differs')
        class_counts = Counter((s['module'], s['kind'], s['class']) for s in snapshot['suites'])
        if any(count != 1 and not classname.endswith('.PublishedExamplesIT') for (_, _, classname), count in class_counts.items()):
            failures.append(label + ' contains duplicate report suites')
        emitted = Counter(method_key(case) for case in snapshot['cases'])
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
    skip_before = {identity(c): skip_identity(c['skip']) for c in before['cases'] if c['parameter'] is not None}
    skip_after = {identity(c): skip_identity(c['skip']) for c in after['cases'] if c['parameter'] is not None and identity(c) in expected}
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
            for field in ('production_files', 'production_class_ids', 'production_class_access_flags', 'dependency_repository_files', 'resolved_test_classpaths', 'jacoco_version'):
                require(post.get(field) == inputs.get(field), 'production/dependency bytes changed during tests: ' + field)
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
    hits, reports_found, unresolved = [], [], []
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
                 reports=reports_found, covered_lines_branches=hits, unresolved=unresolved, complete=bool(reports_found) and not unresolved)
    output(args.output, value)
    print(json.dumps(dict(exec_files=len(executions), production_classes=len(production), coverage_xml=len(reports_found), complete=value['complete']), sort_keys=True))


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
    output(args.output, dict(schema=SCHEMA, type='coverage-comparison', complete=not failures, failures=failures, reductions=reduced))
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
    inv.add_argument('--parameters', help='Independently reviewed stable argument contracts, bound to source hashes')
    inv.add_argument('--output', required=True)
    rep = sub.add_parser('reports')
    rep.add_argument('--root', default='.')
    rep.add_argument('--lane', required=True)
    rep.add_argument('--inventory', required=True)
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
    args = parser.parse_args()
    {'inventory': inventory, 'reports': reports, 'compare': compare_tests, 'coverage': coverage,
     'compare-coverage': compare_coverage, 'gates': gates, 'artifacts': artifacts}[args.command](args)


if __name__ == '__main__':
    try:
        main()
    except (ValueError, KeyError, OSError, UnicodeError, ET.ParseError) as error:
        print('ci-evidence: ' + str(error), file=sys.stderr)
        sys.exit(1)
