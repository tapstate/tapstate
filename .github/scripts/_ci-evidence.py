#!/usr/bin/env python3
"""Independent source inventories and fail-closed test/coverage evidence comparison.

Snapshots are observations, not CI admission waivers. Unresolved source forms and
missing provenance remain incomplete; existing shard and witness gates still run.
"""
import argparse
from collections import Counter
from decimal import Decimal
from datetime import datetime, timezone
import hashlib
import os
import json
from pathlib import Path
import re
import subprocess
import sys
import tarfile
import tempfile
import time
import zipfile
import xml.etree.ElementTree as ET

sys.dont_write_bytecode = True

SCHEMA = 1
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


ANNOTATION_NAME = re.compile(r'@\s*([\w$]+(?:\s*\.\s*[\w$]+)*)(?![\w$])')


def annotation_spans(clean, start=0, end=None):
    end = len(clean) if end is None else end
    consumed = start
    for found in ANNOTATION_NAME.finditer(clean, start, end):
        offset, finish = found.start(), found.end()
        if offset < consumed:
            continue
        while finish < end and clean[finish].isspace():
            finish += 1
        argument = None
        if finish < end and clean[finish] == '(':
            closing = close_at(clean, finish)
            require(closing < end, 'annotation extends beyond its declaration')
            argument = (finish + 1, closing)
            finish = closing + 1
        consumed = finish
        yield offset, finish, re.sub(r'\s+', '', found.group(1)), argument


def without_annotations(text):
    clean = masked(text)
    result = list(clean)
    for start, end, unused_name, unused_argument in annotation_spans(clean):
        result[start:end] = ['\n' if char == '\n' else ' ' for char in clean[start:end]]
    return ''.join(result)


def declaration_body(clean, start):
    parentheses = 0
    for position in range(start, len(clean)):
        char = clean[position]
        if char == '(':
            parentheses += 1
        elif char == ')':
            parentheses -= 1
            require(parentheses >= 0, 'unbalanced type declaration header')
        elif parentheses == 0 and char == '{':
            return position
        elif parentheses == 0 and char == ';':
            return None
    return None


def uncomment(text):
    pattern = r'/\*[\s\S]*?\*/|//[^\n]*|"""[\s\S]*?"""|"(?:\\.|[^"\\])*"|\'(?:\\.|[^\'\\])*\''
    return re.sub(pattern, lambda m: ' ' if m.group().startswith(('/', '*')) else m.group(), text)


def enum_constants(text, declaration):
    clean = without_annotations(text)
    body = clean[declaration['start'] + 1:declaration['end']]
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


def enum_definition(root, source, name, sources, enum_context=None):
    lookup, offset = enum_context or (DisabledLookup(root, sources), 0)
    unused_name, target = lookup.resolve(source, name, offset)
    require(target is not None and target[1]['kind'] == 'enum',
            'enum declaration cannot be independently resolved: ' + name)
    return target


def resolve_enum(root, source, name, sources, enum_context=None):
    path, declaration = enum_definition(root, source, name, sources, enum_context)
    return enum_constants(path.read_text(), declaration)


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
    number = r'-?\d(?:_*\d)*(?:\.\d(?:_*\d)*)?[dDfFlL]?'
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
        elif re.fullmatch(r'(?:true|false|' + number + r')', item):
            result.append(item.replace('_', ''))
        elif re.fullmatch(r'\w+', item):
            definition = re.search(r'\b' + re.escape(item) + r'\s*=\s*("(?:\\.|[^"\\])*"|true|false|' + number + r')\s*;', text)
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


def annotation_prefix(text, clean, position, floor=0):
    # Walk past whole annotation arguments; array braces and strings are not
    # declaration boundaries and cannot supply a Disabled annotation.
    depth, start = 0, floor
    for index in range(position - 1, floor - 1, -1):
        char = clean[index]
        if char == ')':
            depth += 1
        elif char == '(':
            depth -= 1
        elif depth == 0 and char in '{};':
            start = index + 1
            break
    result = []
    for offset, end, name, argument in annotation_spans(clean, start, position):
        result.append(dict(name=name, argument=text[argument[0]:argument[1]] if argument else '', offset=offset))
    return result, start


def disabled_annotation(text, clean, position, floor=0, lookup=None, source=None):
    annotations, start = annotation_prefix(text, clean, position, floor)
    if lookup is not None:
        for annotation in annotations:
            found = lookup.annotation_disabled(source, annotation)
            if found is not None:
                return found
        return None
    return next((annotation['argument'] for annotation in annotations
                 if annotation['name'] in ('Disabled', 'org.junit.jupiter.api.Disabled')), None)


def class_declarations(text, clean, package, lookup=None, source=None):
    classes = []
    for match in re.finditer(r'\b(class|interface|enum|record)\s+(\w+)\b', clean):
        start = declaration_body(clean, match.end())
        if start is None:
            continue
        end = close_at(clean, start, '{', '}')
        parent = next((c for c in reversed(classes) if c['start'] < start < c['end']), None)
        name = parent['name'] + '$' + match.group(2) if parent else package + match.group(2)
        local_disabled = (lookup.class_disabled(source, name) if lookup is not None else
                          disabled_annotation(text, clean, match.start(), parent['start'] + 1 if parent else 0))
        _, prefix_start = annotation_prefix(text, clean, match.start())
        access = re.search(r'\b(public|protected|private)\b', clean[prefix_start:match.start()])
        classes.append(dict(start=start, end=end, name=name, offset=match.start(),
                            kind='annotation' if match.group(1) == 'interface' and
                                clean[:match.start()].rstrip().endswith('@') else match.group(1),
                            access=access.group(1) if access else 'public' if parent and parent['kind'] in ('interface', 'annotation') else 'package',
                            header=clean[match.start():start], disabled_source=local_disabled
                            if local_disabled is not None else parent['disabled_source'] if parent else None))
    return classes


class DisabledLookup:
    """Resolve source-defined runtime annotations using Jupiter's disabled lookup order."""
    DISABLED = 'org.junit.jupiter.api.Disabled'
    INHERITED = 'java.lang.annotation.Inherited'
    RETENTION = 'java.lang.annotation.Retention'

    def __init__(self, root, sources):
        self.root, self.sources, self.contexts = root, sources, {}
        self.directories = [root / module / tree for module, _ in source_modules(root)
                            for tree in ('src/main/java', 'src/test/java')]
        self.named_cache, self.qualified_cache, self.packages = {}, {}, {}

    def context(self, source):
        if source not in self.contexts:
            text = source.read_text()
            clean = masked(text)
            package = re.search(r'\bpackage\s+([\w.]+)\s*;', clean)
            prefix = package.group(1) + '.' if package else ''
            self.contexts[source] = dict(text=text, clean=clean, prefix=prefix,
                                        classes=class_declarations(text, clean, prefix))
            self.sources[source.relative_to(self.root).as_posix()] = digest(source.read_bytes())
        return self.contexts[source]

    def member_type(self, source, node):
        context = self.context(source)
        current = node
        while current is not None:
            parent = next((item for item in reversed(context['classes'])
                           if item['start'] < current['offset'] < item['end']), None)
            prefix = context['clean'][parent['start'] + 1 if parent else 0:current['offset']]
            if prefix.count('{') != prefix.count('}'):
                return False
            current = parent
        return True

    def named(self, name):
        if name in self.named_cache:
            return self.named_cache[name]
        found = []
        parts = name.split('.')
        for directory in self.directories:
            for length in range(len(parts), 0, -1):
                candidate = directory.joinpath(*parts[:length]).with_suffix('.java')
                if candidate.is_file():
                    found.extend((candidate, node) for node in self.context(candidate)['classes']
                                 if node['name'].replace('$', '.') == name and self.member_type(candidate, node))
        require(len(found) <= 1, 'ambiguous annotation or interface source: ' + name)
        self.named_cache[name] = found[0] if found else None
        return self.named_cache[name]

    def package_named(self, prefix, name):
        if prefix not in self.packages:
            found = {}
            for directory in self.directories:
                package = directory.joinpath(*prefix.rstrip('.').split('.'))
                for candidate in package.glob('*.java'):
                    for node in self.context(candidate)['classes']:
                        if not self.member_type(candidate, node):
                            continue
                        found.setdefault(node['name'].replace('$', '.'), []).append((candidate, node))
            self.packages[prefix] = found
        return self.packages[prefix].get(name, [])

    def definition(self, name):
        if name not in self.qualified_cache:
            direct = self.named(name)
            candidates = {(direct[0], direct[1]['name']): direct} if direct else {}
            parts = name.split('.')
            # A canonical name can end in nested types, not only a package and one type.
            # Include nonmatching filenames even after a direct hit to reject source-root ambiguity.
            for length in range(len(parts) - 1, -1, -1):
                prefix = '.'.join(parts[:length]) + ('.' if length else '')
                for path, node in self.package_named(prefix, name):
                    candidates[(path, node['name'])] = (path, node)
            require(len(candidates) <= 1, 'ambiguous annotation or interface source: ' + name)
            found = next(iter(candidates.values()), None)
            self.qualified_cache[name] = found
        return self.qualified_cache[name]

    def resolve(self, source, name, offset=0):
        context = self.context(source)
        lexical = [node for node in reversed(context['classes']) if node['start'] < offset < node['end'] and self.member_type(source, node)]
        for owner in lexical:
            members = self.members(source, owner, name)
            require(len(members) <= 1, 'ambiguous inherited annotation source: ' + name)
            if members:
                path, node = members[0]
                return node['name'].replace('$', '.'), (path, node)
        local = [node for node in context['classes'] if node['name'].replace('$', '.') == context['prefix'] + name and self.member_type(source, node)]
        if local:
            return local[0]['name'].replace('$', '.'), (source, local[0])
        first = name.split('.')[0]
        imported = re.search(r'\bimport\s+(?:static\s+)?([\w.]+\.' + re.escape(first) + r')\s*;', context['clean'])
        if imported:
            qualified = imported.group(1) + name[len(first):]
            return qualified, self.definition(qualified)
        if '.' in name and name[0].islower():
            return name, self.definition(name)
        qualified = context['prefix'] + name
        if qualified in (self.DISABLED, self.INHERITED, self.RETENTION):
            return qualified, None
        found = self.definition(qualified)
        if found:
            return qualified, found
        candidates = []
        for imported in re.finditer(r'\bimport\s+(?:static\s+)?([\w.]+)\.\*\s*;', context['clean']):
            qualified = imported.group(1) + '.' + name
            target = self.definition(qualified)
            if target or qualified in (self.DISABLED, self.INHERITED, self.RETENTION):
                candidates.append((qualified, target))
        require(len(candidates) <= 1, 'ambiguous annotation or interface imports: ' + name)
        if candidates:
            return candidates[0]
        return name, None

    def annotations(self, source, node):
        context = self.context(source)
        return annotation_prefix(context['text'], context['clean'], node['offset'])[0]

    def runtime(self, source, node):
        for annotation in self.annotations(source, node):
            name, _ = self.resolve(source, annotation['name'], annotation['offset'])
            if name == self.RETENTION:
                value = re.sub(r'\s+', '', annotation['argument']).removeprefix('value=')
                require(re.fullmatch(r'(?:java\.lang\.annotation\.)?(?:RetentionPolicy\.)?(?:RUNTIME|CLASS|SOURCE)', value),
                        'annotation retention requires an independent contract: ' + node['name'])
                return value.rsplit('.', 1)[-1] == 'RUNTIME'
        return False

    def annotation_disabled(self, source, annotation, seen=frozenset()):
        name, definition = self.resolve(source, annotation['name'], annotation['offset'])
        if name == self.DISABLED:
            return annotation['argument']
        if definition is None or name in seen:
            return None
        path, node = definition
        if node['kind'] != 'annotation' or not self.runtime(path, node):
            return None
        for meta in self.annotations(path, node):
            disabled = self.annotation_disabled(path, meta, seen | {name})
            if disabled is not None:
                return disabled
        return None

    def parent(self, source, node):
        name = self.parent_name(node)
        return self.resolve(source, name, node['offset'])[1] if name else None

    def parent_name(self, node):
        match = re.search(r'\bextends\s+([\w.]+)', self.type_header(node)) if node['kind'] == 'class' else None
        return match.group(1) if match else None

    def interfaces(self, source, node):
        keyword = 'extends' if node['kind'] == 'interface' else 'implements'
        found = re.search(r'\b' + keyword + r'\s+(.+)', self.type_header(node), re.S)
        return [self.resolve(source, name.strip(), node['offset'])[1]
                for name in split_values(found.group(1), generic=True)] if found else []

    def members(self, source, node, name, seen=frozenset(), requester=None):
        key = (source, node['name'])
        if key in seen:
            return []
        direct = [(source, member) for member in self.context(source)['classes']
                  if member['name'].replace('$', '.') == node['name'].replace('$', '.') + '.' + name and self.member_type(source, member)]
        inherited = {}
        if not direct:
            for parent in [self.parent(source, node), *self.interfaces(source, node)]:
                if parent:
                    for path, member in self.members(*parent, name, seen | {key}, self.context(source)['prefix']):
                        inherited[(path, member['name'])] = (path, member)
        result = direct or list(inherited.values())
        return [(path, member) for path, member in result if requester is None or
                member['access'] != 'private' and (member['access'] != 'package' or self.context(path)['prefix'] == requester)]

    @staticmethod
    def type_header(node):
        header = without_annotations(node['header'])
        parentheses = angles = 0
        for token in re.finditer(r'[()<>]|\bpermits\b', header):
            char = token.group()
            if char == '(':
                parentheses += 1
            elif char == ')':
                parentheses -= 1
            elif char == '<':
                angles += 1
            elif char == '>':
                angles -= 1
            elif parentheses == angles == 0 and not header[:token.start()].rstrip().endswith('.') and not header[token.end():].lstrip().startswith('.'):
                header = header[:token.start()]
                break
        while re.search(r'<[^<>]*>', header):
            header = re.sub(r'<[^<>]*>', '', header)
        require('<' not in header and '>' not in header, 'unsupported source type parameters')
        return re.sub(r'\s*\.\s*', '.', header)

    def class_disabled(self, source, name, seen=frozenset()):
        if name in seen:
            return None
        node = next(node for node in self.context(source)['classes'] if node['name'] == name)
        annotations = self.annotations(source, node)
        for annotation in annotations:
            disabled = self.annotation_disabled(source, annotation)
            if disabled is not None:
                return disabled
        for target in self.interfaces(source, node):
            if target:
                disabled = self.class_disabled(target[0], target[1]['name'], seen | {name})
                if disabled is not None:
                    return disabled
        # Class.getAnnotations exposes inherited composed annotations, even
        # though Disabled itself does not authorize superclass traversal.
        blocked = {self.resolve(source, a['name'], a['offset'])[0] for a in annotations}
        parent, visited = self.parent(source, node), set()
        while parent and parent[1]['name'] not in visited:
            path, base = parent
            visited.add(base['name'])
            for annotation in self.annotations(path, base):
                qualified, definition = self.resolve(path, annotation['name'], annotation['offset'])
                if qualified not in blocked and definition:
                    declaring, annotation_type = definition
                    inherited = any(self.resolve(declaring, meta['name'], meta['offset'])[0] == self.INHERITED
                                    for meta in self.annotations(declaring, annotation_type))
                    if inherited:
                        disabled = self.annotation_disabled(path, annotation)
                        if disabled is not None:
                            return disabled
                blocked.add(qualified)
            parent = self.parent(path, base)
        return None


def declarations(text, outer, package, lookup=None, source=None):
    clean = masked(text)
    classes = class_declarations(text, clean, package, lookup, source)
    result, consumed = [], -1
    for offset, unused_end, qualified, unused_argument in annotation_spans(clean):
        if offset < consumed or qualified.rsplit('.', 1)[-1] not in TEST_ANNOTATIONS:
            continue
        # Start at this test annotation and collect following declaration annotations.
        position, annotations = offset, {}
        while True:
            while position < len(clean) and clean[position].isspace():
                position += 1
            annotation = ANNOTATION_NAME.match(clean, position)
            if not annotation:
                break
            name = re.sub(r'\s+', '', annotation.group(1)).rsplit('.', 1)[-1]
            position = annotation.end()
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
                temporary = any(name.rsplit('.', 1)[-1] == 'TempDir'
                                for unused_start, unused_end, name, unused_arg in annotation_spans(masked(parameter)))
                parameter = without_annotations(parameter).strip()
                typename = re.sub(r'\s+\w+\s*$', '', parameter).replace('final ', '').strip()
                parameter_types.append(typename)
                parameter_injected.append(temporary or typename in ('TestInfo', 'org.junit.jupiter.api.TestInfo',
                    'RepetitionInfo', 'org.junit.jupiter.api.RepetitionInfo'))
        elif 'ArchTest' in annotations:
            field = re.match(r'\s*(?:[\w<>\[\].?,]+\s+)+?(\w+)\s*=', clean[position:])
            if not field:
                result.append(dict(unresolved='unsupported ArchTest declaration', offset=offset))
                continue
            name = field.group(1)
            consumed = position + field.end()
            parameter_types, parameter_injected = [], []
        else:
            result.append(dict(unresolved='unsupported test declaration', offset=offset))
            continue
        if 'ArchTest' not in annotations and re.search(r'\bprivate\b', clean[position:consumed]):
            continue
        owner = next((c for c in reversed(classes) if c['start'] < offset < c['end']), None)
        if owner is None or not (owner['name'] == package + outer or owner['name'].startswith(package + outer + '$')):
            continue
        # Include preceding Disabled annotations in the declaration's contract.
        method_disabled = disabled_annotation(text, clean, position, owner['start'] + 1, lookup, source)
        disabled_source = method_disabled if method_disabled is not None else owner['disabled_source']
        result.append(dict(**{'class': owner['name']}, method=name, annotations=annotations,
                           disabled_source=disabled_source, method_disabled_source=method_disabled,
                           class_disabled_source=owner['disabled_source'],
                           parameter_types=parameter_types, parameter_injected=parameter_injected, source_offset=offset, declaration_sha256=digest(text[offset:consumed].encode())))
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


def canonical_csv_argument(value, typename, root, source, sources, enum_context):
    if value is None:
        return ['null']
    lookup, offset = enum_context
    unused_name, target = lookup.resolve(source, typename, offset)
    if target is not None and target[1]['kind'] == 'enum':
        path, declaration = target
        sources[path.relative_to(root).as_posix()] = digest(path.read_bytes())
        require(value in enum_constants(path.read_text(), declaration), 'CSV enum constant is absent')
        return ['enum', declaration['name'], value]
    return canonical_literal(value, typename)


def enum_qualified_name(root, source, enum_name, sources, enum_context=None):
    unused_path, declaration = enum_definition(root, source, enum_name, sources, enum_context)
    return declaration['name']


def provider_body(root, source, declaration, name, sources, enum_context):
    lookup, unused_offset = enum_context
    target = lookup.definition(declaration['class'].replace('$', '.'))
    require(target is not None, 'parameter provider owner cannot be independently resolved')

    def find(path, owner, seen=frozenset()):
        key = (path, owner['name'])
        require(key not in seen, 'cyclic parameter provider inheritance')
        context = lookup.context(path)
        matches = []
        for match in re.finditer(r'\b' + re.escape(name) + r'\s*\(\s*\)\s*(?:throws[^\{;]+)?\{', context['clean']):
            enclosing = next((node for node in reversed(context['classes'])
                              if node['start'] < match.start() < node['end']), None)
            prefix = context['clean'][owner['start'] + 1:match.start()]
            if enclosing is not None and enclosing['name'] == owner['name'] and prefix.count('{') == prefix.count('}'):
                matches.append(match.end() - 1)
        require(len(matches) <= 1, 'overloaded parameter provider requires an independent contract: ' + name)
        if matches:
            start = matches[0]
            end = close_at(context['clean'], start, '{', '}')
            body = context['text'][start + 1:end]
            require(re.search(r'\b(?:class|interface|enum|record)\s+\w+', masked(body)) is None,
                    'local parameter provider types require an independent binary identity contract')
            return path, body, (lookup, start + 1)
        parent = lookup.parent(path, owner)
        return find(*parent, seen | {key}) if parent else None

    result = find(*target)
    require(result is not None, 'external or overloaded parameter provider requires an independent contract: ' + name)
    return result


def canonical_expression(expression, root, source, sources, body, enum_context=None):
    expression = uncomment(expression).strip()
    expression = re.sub(r'^\(Object\)\s*', '', expression)
    if expression == 'null':
        return ['null']
    if expression.startswith('"""') and expression.endswith('"""'):
        value = text_block_content(expression)
        escapes = {'n': '\n', 't': '\t', 'r': '\r', 'b': '\b', 'f': '\f', '"': '"', '\\': '\\'}
        decoded, position = [], 0
        while position < len(value):
            char = value[position]
            if char == '\\':
                position += 1
                require(position < len(value) and value[position] in escapes, 'unsupported Java text block escape')
                char = escapes[value[position]]
            decoded.append(char)
            position += 1
        return ['java.lang.String', ''.join(decoded)]
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
        return ['array', 'java.lang.String', [canonical_expression(v, root, source, sources, body, enum_context) for v in split_values(array.group(1))]]
    constructor = re.fullmatch(r'new\s+((?:[\w.]+\.)?RenameSpec)\s*\(([\s\S]*)\)', expression)
    if constructor:
        lookup, offset = enum_context or (DisabledLookup(root, sources), 0)
        name, target = lookup.resolve(source, constructor.group(1), offset)
        if name == 'io.tapstate.core.model.RenameSpec' and target is not None:
            definition, declaration = target
            # This constructor only copies the map; it does not compute new component values.
            # Bind both its shape and body before projecting source arguments as record fields.
            contract = ('package io.tapstate.core.model;'
                        'import java.util.Collections;import java.util.LinkedHashMap;import java.util.Map;'
                        'public record RenameSpec(Map<String,String> map,RenameCase caseMode,String prefix,String suffix) {'
                        'public RenameSpec {map = map == null ? null : Collections.unmodifiableMap(new LinkedHashMap<>(map));}}')
            require(declaration['kind'] == 'record' and
                    re.sub(r'\s+', '', without_annotations(definition.read_text())) == re.sub(r'\s+', '', contract),
                    'RenameSpec source constructor contract changed')
            values = [canonical_expression(v, root, source, sources, body, enum_context)
                      for v in split_values(constructor.group(2))]
            require(len(values) == 4, 'invalid RenameSpec source constructor arity')
            return ['record', name, list(zip(('map', 'caseMode', 'prefix', 'suffix'), values))]
    call = re.fullmatch(r'(List\.of|Map\.of|new Document|TableRef\.literal|TableRef\.regex)\s*\(([\s\S]*)\)', expression)
    if call:
        values = [canonical_expression(v, root, source, sources, body, enum_context) for v in split_values(call.group(2))]
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
        names = resolve_enum(root, source, enum.group(1), sources, enum_context)
        require(enum.group(2) in names, 'source enum constant is absent')
        return ['enum', enum_qualified_name(root, source, enum.group(1), sources, enum_context), enum.group(2)]
    if re.fullmatch(r'\w+', expression):
        local = re.search(r'\b(?:String|int|long|double|boolean)\s+' + re.escape(expression) + r'\s*=\s*([^;]+);', body)
        if local:
            return canonical_expression(local.group(1), root, source, sources, body, enum_context)
    raise ValueError('source parameter expression requires an independent contract: ' + expression[:100])


def static_provider_arguments(root, source, declaration, sources, enum_context=None):
    annotation = declaration['annotations'].get('MethodSource')
    require(annotation is not None, 'custom parameter provider requires an independent contract')
    names = literals(annotation or '"' + declaration['method'] + '"', source.read_text())
    require(len(names) == 1, 'multiple parameter providers require an independent contract')
    source, body, enum_context = provider_body(root, source, declaration, names[0], sources, enum_context)
    declaration['provider_body_sha256'] = digest(body.encode())
    clean = masked(body)
    native = re.sub(r'\s+', '', body)
    if native == 'returnSystem.getenv().containsKey("TAPSTATE_TEST_NATIVE_CLI")?List.of(CliFlavor.JVM,CliFlavor.NATIVE):List.of(CliFlavor.JVM);':
        presence = declaration.get('native_cli_presence', 'unknown')
        require(presence in ('absent', 'present'), 'native CLI environment presence must be frozen independently')
        resolve_enum(root, source, 'CliFlavor', sources, enum_context)
        binary = enum_qualified_name(root, source, 'CliFlavor', sources, enum_context)
        declaration['environment_contract'] = dict(TAPSTATE_TEST_NATIVE_CLI=presence)
        return [[['enum', binary, value]] for value in (['JVM', 'NATIVE'] if presence == 'present' else ['JVM'])]
    compact = re.sub(r'\s+', '', clean)
    cross = re.fullmatch(r'return(?:Arrays\.stream|Stream\.of)\(([\w.]+)\.values\(\)\)\.flatMap\((\w+)->(?:Arrays\.stream|Stream\.of)\(([\w.]+)\.values\(\)\)\.map\((\w+)->Arguments\.of\((\w+),(\w+)\)\)\);', compact)
    if cross:
        first_type, first_name, second_type, second_name, first_arg, second_arg = cross.groups()
        require((first_name, second_name) == (first_arg, second_arg), 'enum matrix argument binding changed')
        first = resolve_enum(root, source, first_type, sources, enum_context)
        second = resolve_enum(root, source, second_type, sources, enum_context)
        first_binary = enum_qualified_name(root, source, first_type, sources, enum_context)
        second_binary = enum_qualified_name(root, source, second_type, sources, enum_context)
        return [[['enum', first_binary, a], ['enum', second_binary, b]] for a in first for b in second]
    matrix = re.fullmatch(r'returnArrays\.stream\(([\w.]+)\.values\(\)\)\.flatMap\((\w+)->Stream\.of\(([^()]+)\)\.flatMap\((\w+)->(?:java\.util\.stream\.)?IntStream\.range\((\d+),(\d+)\)\.mapToObj\((\w+)->Arguments\.of\((\w+),(\w+),(\w+)\)\)\)\);', compact)
    if matrix:
        first_type, first_name, second_values, second_name, low, high, third_name, first_arg, second_arg, third_arg = matrix.groups()
        require((first_name, second_name, third_name) == (first_arg, second_arg, third_arg), 'matrix argument binding changed')
        require(0 <= int(low) <= int(high) <= 256, 'matrix range requires an independent bounded contract')
        first = resolve_enum(root, source, first_type, sources, enum_context)
        first_binary = enum_qualified_name(root, source, first_type, sources, enum_context)
        second = [canonical_expression(v, root, source, sources, body, enum_context) for v in split_values(second_values)]
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
            result.append([canonical_expression(value, root, source, sources, body, enum_context) for value in arguments])
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


def with_injected(arguments, declaration, repetition=None):
    result, position = [], 0
    for typename, injected in zip(declaration['parameter_types'], declaration['parameter_injected']):
        if injected:
            if typename in ('TestInfo', 'org.junit.jupiter.api.TestInfo'):
                result.append(['injected', 'org.junit.jupiter.api.TestInfo'])
            elif typename in ('RepetitionInfo', 'org.junit.jupiter.api.RepetitionInfo'):
                require(repetition is not None, 'RepetitionInfo injection requires RepeatedTest')
                result.append(['repetition', *repetition])
            else:
                require(typename in ('Path', 'java.nio.file.Path'), 'unsupported injected TempDir type: ' + typename)
                result.append(['injected', 'org.junit.jupiter.api.io.TempDir', 'java.nio.file.Path'])
        else:
            require(position < len(arguments), 'source and argument arity differs')
            result.append(arguments[position])
            position += 1
    require(position == len(arguments), 'source and argument arity differs')
    return result


def inherited_declarations(root, source, sources, stack=None, lookup=None, target=None):
    stack = set() if stack is None else stack
    text, clean = source.read_text(), masked(source.read_text())
    package = re.search(r'\bpackage\s+([\w.]+)\s*;', clean)
    prefix = package.group(1) + '.' if package else ''
    lookup = lookup or DisabledLookup(root, sources)
    context = lookup.context(source)
    owner = target or next((node for node in context['classes'] if node['name'] == prefix + source.stem), None)
    key = (source, owner['name'] if owner else prefix + source.stem)
    require(key not in stack, 'cyclic test inheritance')
    stack = stack | {key}
    outer = target['name'].removeprefix(prefix) if target else source.stem
    own = [(source, item) for item in declarations(text, outer, prefix, lookup, source)]
    if target:
        own = [(path, item) for path, item in own if 'class' not in item or
               item['class'] == target['name'] or item['class'].startswith(target['name'] + '$')]
    parent_name = lookup.parent_name(owner) if owner else None
    if parent_name is None:
        return own
    parent = lookup.resolve(source, parent_name, owner['offset'])[1]
    require(parent is not None, 'test parent source requires an independent inheritance contract: ' + parent_name)
    base, base_node = parent
    sources[base.relative_to(root).as_posix()] = digest(base.read_bytes())
    if base == source:
        own = [(path, item) for path, item in own if 'class' not in item or
               item['class'] != base_node['name'] and not item['class'].startswith(base_node['name'] + '$')]
    overrides = {(item['class'].removeprefix(owner['name']), item['method']) for _, item in own if 'method' in item}
    base_name = base_node['name']
    owners = {c['name']: c for c in class_declarations(text, clean, prefix, lookup, source)}
    for declaring_source, item in inherited_declarations(root, base, sources, stack, lookup, base_node):
        if 'class' not in item:
            own.append((declaring_source, item))
            continue
        suffix = item['class'].removeprefix(base_name)
        if (suffix, item['method']) not in overrides:
            inherited = dict(item)
            inherited['class'] = owner['name'] + suffix
            # Disabled is not inherited from a superclass; method annotations are.
            container = owners.get(inherited['class'], owners[owner['name']])
            local = inherited.get('method_disabled_source')
            inherited['disabled_source'] = local if local is not None else container['disabled_source']
            inherited['class_disabled_source'] = container['disabled_source']
            own.append((declaring_source, inherited))
    return own


def before_all_contracts(root, source, sources):
    text, clean = source.read_text(), masked(source.read_text())
    callbacks = []
    package = re.search(r'\bpackage\s+([\w.]+)\s*;', clean)
    classname = (package.group(1) + '.' if package else '') + source.stem
    for unused_start, annotation_end, name, unused_argument in annotation_spans(clean):
        if name not in ('BeforeAll', 'org.junit.jupiter.api.BeforeAll'):
            continue
        header = re.search(r'\b(?:void|[\w.]+)\s+(\w+)\s*\(', clean[annotation_end:annotation_end + 500])
        if header is None:
            continue
        opening = annotation_end + header.end() - 1
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
    ledger_source, selection_source = source.parent / 'WitnessLedger.java', source.parent / 'PublishedExampleSelection.java'
    require(ledger_source.is_file() and selection_source.is_file(), 'published ledger serializer sources are missing')
    ledger_text, selection_text = ledger_source.read_text(), selection_source.read_text()
    require(all(part in ledger_text for part in ('System.getProperty(PublishedExampleSelection.PROPERTY) == null',
            'specification.getParent().getFileName().toString()', "specification.toString().replace('\\\\', '/')", 'example + " on " + tier.name()')) and
            'WitnessLedger.record(specification, tier)' in source.read_text() and
            'static final String PROPERTY = "tapstate.e2e.published-examples";' in selection_text,
            'published ledger serializer requires an independently reviewed source contract')
    helper_sources = {path.relative_to(root).as_posix(): digest(path.read_bytes()) for path in (ledger_source, selection_source)}
    sources.update(helper_sources)
    policies = {}
    aliases = {}
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
            aliases[relative + ' on ' + tier] = dict(default=example.parent.name + ' on ' + tier, selected=relative + ' on ' + tier)
            policies[relative + ' on ' + tier] = dict(requires_real=real, mode=mode, connectors=ids,
                    expected_status='skipped' if real and mode == 'absent' else 'executed',
                    skip_message=message if real and mode == 'absent' else None)
        sources[example.relative_to(root).as_posix()] = digest(example.read_bytes())
    require(len({row['default'] for row in aliases.values()}) == len(aliases) and len({row['selected'] for row in aliases.values()}) == len(aliases), 'ambiguous published ledger aliases')
    return dict(cases=policies, ledger_serializer=dict(helper_sources=helper_sources, aliases=aliases), gate_sha256=gate_hash, published_source_sha256=digest(source.read_bytes()), synthetic_source_sha256=digest(synthetic_source.read_bytes()))


def csv_literals(value):
    """Admit literal strings only; Java constant scope requires independent enumeration."""
    value = uncomment(value).strip()
    if value.startswith('{') and value.endswith('}'):
        value = value[1:-1]
    entries = split_values(value)
    require(all(re.fullmatch(r'"(?:\\.|[^"\\])*"', item) for item in entries),
            'CSV constant expressions require independent enumeration')
    return literals(value, '')


def csv_record(row, delimiter):
    """Preserve quoted empty/whitespace values while unquoted empty cells are null."""
    whitespace = ''.join(chr(value) for value in range(33))
    require(row.strip(whitespace) and len(delimiter) == 1 and delimiter not in whitespace and delimiter != "'",
            'unsupported CSV record or delimiter')
    cells, position = [], 0
    while True:
        start = position
        while position < len(row) and row[position] in whitespace:
            position += 1
        if position < len(row) and row[position] == "'":
            position += 1
            value = []
            while position < len(row):
                char = row[position]; position += 1
                if char == "'":
                    if position < len(row) and row[position] == "'":
                        value.append("'"); position += 1
                    else:
                        break
                else:
                    value.append(char)
            else:
                raise ValueError('unterminated CSV quoted field')
            value = ''.join(value)
            while position < len(row) and row[position] in whitespace:
                position += 1
            require(position == len(row) or row[position] == delimiter, 'unsupported CSV text after quoted field')
        else:
            while position < len(row) and row[position] != delimiter:
                position += 1
            raw = row[start:position]
            require('\n' not in raw and '\r' not in raw, 'unquoted CSV newline requires independent enumeration')
            value = raw.strip(whitespace) or None
        cells.append(value)
        if position == len(row):
            return cells
        position += 1


JAVA_TEXT_WHITESPACE = ' \t\f\v\u001c\u001d\u001e\u001f\u1680' + ''.join(
    chr(code) for code in (*range(0x2000, 0x2007), *range(0x2008, 0x200b), 0x2028, 0x2029, 0x205f, 0x3000))


def text_block_content(value):
    """Normalize the supported Java text-block whitespace before escape decoding."""
    block = re.fullmatch(r'"""([\s\S]*?)"""', value)
    require(block is not None, 'unsupported Java text block')
    raw = block.group(1).replace('\r\n', '\n').replace('\r', '\n')
    # JDK 21 Character.isWhitespace differs from Python's Unicode whitespace.
    whitespace = JAVA_TEXT_WHITESPACE
    opening = re.match(r'^[ \t\f]*\n', raw)
    require(opening is not None, 'unsupported Java text block opening')
    lines = raw[opening.end():].split('\n')
    indentation = [len(line) - len(line.lstrip(whitespace)) for line in lines if line.strip(whitespace)]
    if not lines[-1].strip(whitespace):
        indentation.append(len(lines[-1]))
    common = min(indentation, default=0)
    return '\n'.join(line[min(common, len(line) - len(line.lstrip(whitespace))):].rstrip(whitespace)
                     if line.strip(whitespace) else '' for line in lines)


def csv_text_block(value):
    """Apply bounded Java whitespace normalization before default CSV comments."""
    require('\\' not in value and all(ord(c) >= 32 or c in JAVA_TEXT_WHITESPACE + '\r\n' for c in value),
            'CSV text-block escapes or whitespace require independent enumeration')
    return [line for line in text_block_content(value).split('\n') if line.strip(' \t') and not line.startswith('#')]


def signature_types(declaration):
    result = []
    for typename in declaration['parameter_types']:
        typename = re.sub(r'<.*>', '', typename)
        typename = re.sub(r'\s*\.\.\.\s*$', '[]', typename)
        typename = re.sub(r'\s*\[\s*\]', '[]', typename)
        result.append(typename.rsplit('.', 1)[-1])
    return result


def source_parameters(root, source, declaration, sources, lookup=None):
    enum_context = (lookup or DisabledLookup(root, sources), declaration.get('source_offset', 0))
    annotations, text = declaration['annotations'], source.read_text()
    # Disabled templates are reported once without invoking their providers or interceptor.
    if declaration.get('disabled_source') is not None:
        return [''], 'disabled-method'
    if source.stem == 'PublishedExamplesIT' and declaration['method'] == 'thePublishedExampleRuns':
        examples = sorted((root / 'e2e/examples').rglob('*.e2e.yml'))
        declaration['published_policy'] = published_policy(root, source, examples, sources, declaration.get('published_connectors', 'unknown'))
        return [p.relative_to(root / 'e2e').as_posix() + ' on ' + tier for p in examples
                for tier in ('IN_PROCESS', 'REAL_PROCESS')], 'published-marker'
    if 'RepeatedTest' in annotations:
        count = re.match(r'(?:value\s*=\s*)?(\d+)', annotations['RepeatedTest'])
        require(count is not None and int(count.group(1)) > 0, 'unresolved RepeatedTest count')
        total = int(count.group(1))
        declaration['argument_digests'] = [argument_digest(with_injected([], declaration, (n, total)))
                                           for n in range(1, total + 1)]
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
        require('\\u' not in annotations['CsvSource'], 'CSV Unicode preprocessing requires independent enumeration')
        argument = uncomment(annotations['CsvSource']).strip()
        entries = split_values(argument)
        named = [re.fullmatch(r'(\w+)\s*=\s*([\s\S]+)', entry) for entry in entries]
        require(not any(named) or all(named), 'mixed CSV annotation option forms')
        options = {entry.group(1): entry.group(2) for entry in named if entry}
        require(len(options) == sum(entry is not None for entry in named), 'duplicate CSV annotation option')
        require(set(options) <= {'value', 'textBlock', 'delimiter', 'nullValues'},
                'nondefault CSV options require independent enumeration')
        delimiter = re.fullmatch(r"'(.)'", options['delimiter']) if 'delimiter' in options else None
        require('delimiter' not in options or delimiter is not None, 'unsupported CSV delimiter literal')
        null_values = csv_literals(options['nullValues']) if 'nullValues' in options else []
        if 'textBlock' in options:
            require('value' not in options, 'mixed CSV text block and value source')
            rows = csv_text_block(options['textBlock'])
        else:
            rows = csv_literals(options.get('value', argument))
        for row in rows:
            parsed = csv_record(row, delimiter.group(1) if delimiter else ',')
            values.append([None if value in null_values else value for value in parsed])
        source_types = [typename for typename, injected in zip(declaration['parameter_types'], declaration['parameter_injected']) if not injected]
        require(all(len(row) == len(source_types) for row in values), 'CSV row and source parameter arity differs')
        declaration['argument_digests'] = [argument_digest(with_injected([canonical_csv_argument(value, typename, root, source, sources, enum_context) for value, typename in zip(row, source_types)], declaration)) for row in values]
    elif 'EnumSource' in annotations:
        argument = annotations['EnumSource']
        enum = re.search(r'(?:value\s*=\s*)?([\w.]+)\.class', argument)
        require(enum is not None, 'implicit EnumSource requires an independent parameter contract')
        names = re.search(r'\bnames\s*=\s*(\{[\s\S]*?\}|"(?:\\.|[^"\\])*")', argument)
        mode = re.search(r'\bmode\s*=\s*([\w.]+)', argument)
        require(mode is None or mode.group(1).endswith(('INCLUDE', 'EXCLUDE')), 'regex EnumSource requires an independent parameter contract')
        constants = resolve_enum(root, source, enum.group(1), sources, enum_context)
        selected = literals(names.group(1), text) if names else constants
        values = [[enum.group(1), v] for v in constants if (v not in selected if mode and mode.group(1).endswith('EXCLUDE') else v in selected)]
        binary_name = enum_qualified_name(root, source, enum.group(1), sources, enum_context)
        declaration['argument_digests'] = [argument_digest(with_injected([['enum', binary_name, value[1]]], declaration)) for value in values]
    elif 'MethodSource' in annotations:
        arguments = static_provider_arguments(root, source, declaration, sources, enum_context)
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
        supplied_types = [typename for typename, injected in zip(declaration['parameter_types'], declaration['parameter_injected']) if not injected]
        require(supplied_types in (['String'], ['java.lang.String']), 'non-string EmptySource requires an independent parameter contract')
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
    disabled_lookup = DisabledLookup(root, sources)
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
                declared = inherited_declarations(root, source, sources, lookup=disabled_lookup)
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
                            parameters, mapping = source_parameters(root, declaring_source, declaration, sources, disabled_lookup)
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


def ledger_selector(args, root, inv, files):
    require(bool(args.artifact_manifest) != bool(args.shard_plan), 'published ledger requires one actual artifact manifest or shard plan selector')
    require(args.inputs, 'published ledger selector requires prepared run inputs')
    inputs = load(args.inputs)
    actual_source = inv.get('provider_probe', {}).get('actual_source_sha') or inv.get('source_sha')
    require(actual_source == inputs.get('source_sha') and re.fullmatch(r'[0-9a-f]{40}', actual_source or ''), 'published selector/source origin differs')
    require(type(inputs.get('schema')) is int and inputs['schema'] == 1 and inputs.get('phase') == 'pre-test-after-install', 'published selector inputs are not prepared')
    if args.artifact_manifest:
        path = Path(args.artifact_manifest).resolve(); manifest = load(path)
        require(path.parent / 'files' == root and type(manifest.get('schema')) is int and manifest['schema'] == 2, 'published artifact root/schema differs')
        require(all(manifest.get('files', {}).get(name) == sha for name, sha in files.items()), 'published raw report/ledger artifact hashes differ')
        tests, provenance, source, cohort, shard = manifest['tests'], manifest['provenance'], manifest['source_sha'], manifest['cohort_hash'], manifest['shard_id']
        marker = Path(manifest['provenance_path'])
        require(not marker.is_absolute() and '..' not in marker.parts and (root / marker).is_file() and not (root / marker).is_symlink() and load(root / marker) == provenance, 'published raw provenance differs')
        receipt_sha = digest(path.read_bytes())
    else:
        require(args.shard and re.fullmatch(r'[A-Za-z0-9_-]+', args.shard), 'published shard identity is invalid')
        path = Path(args.shard_plan); plan = load(path)
        require(type(plan.get('schema')) is int and plan['schema'] == 1, 'published shard plan schema differs')
        identities = [tuple(test[field] for field in ('module', 'kind', 'class')) + ((test['example'],) if 'example' in test else ()) for test in plan['expected']]
        require(plan['cohort_hash'] == digest(json.dumps(sorted(identities)).encode()), 'published plan cohort differs')
        matches = [entry for entry in plan['shards'] if entry['id'] == args.shard]
        require(len(matches) == 1, 'published shard selector is missing or duplicate')
        tests, source, cohort, shard = matches[0]['tests'], plan['source_sha'], plan['cohort_hash'], args.shard
        marker = root / 'target/ci-shards' / ('run-' + shard + '.json')
        require(marker.is_file() and not marker.is_symlink(), 'published selected verify provenance is missing')
        provenance = load(marker); receipt_sha = digest(path.read_bytes())
    require(source == actual_source and provenance.get('source_sha') == source and provenance.get('cohort_hash') == cohort and provenance.get('shard_id') == shard and provenance.get('status') == 'success', 'published selector provenance differs')
    require(type(provenance.get('schema')) is int and provenance['schema'] == 1 and str(provenance.get('run_id')) == str(inputs.get('run_id')) and str(provenance.get('run_attempt')) == str(inputs.get('run_attempt')), 'published selector run/attempt differs')
    require(provenance.get('case_observer', {}).get('build_inputs_sha256') == digest(Path(args.inputs).read_bytes()), 'published selector prepared input hash differs')
    for name, phase in [('reset', 'reset-test-evidence'), ('verify', 'selected-verify')]:
        record = provenance.get(name, {})
        require(record.get('phase') == phase and record.get('status') == 'success' and record.get('source_sha') == source and record.get('cohort_hash') == cohort and record.get('shard_id') == shard and str(record.get('run_id')) == str(inputs.get('run_id')) and str(record.get('run_attempt')) == str(inputs.get('run_attempt')), 'published selector phase origin differs')
    selected = [test for test in tests if test.get('class', '').endswith('.PublishedExamplesIT')]
    source_classes = {method['class'] for method in inv['methods'] if method.get('published_policy')}
    require(selected and all(test.get('module') == 'e2e' and test.get('kind') == 'it' and test['class'] in source_classes for test in selected), 'published selector contains no source class')
    require(all('example' in test for test in selected) or all('example' not in test for test in selected), 'mixed published class and example selectors')
    mode = 'selected' if all('example' in test for test in selected) else 'default'
    examples = [test['example'] for test in selected] if mode == 'selected' else None
    require(examples is None or len(examples) == len(set(examples)), 'duplicate published example selector')
    policies = [method['published_policy'] for method in inv['methods'] if method.get('published_policy')]
    require(policies and all(isinstance(policy.get('ledger_serializer'), dict) for policy in policies), 'published inventory lacks a bound ledger serializer contract')
    serializers = [policy['ledger_serializer'] for policy in policies]
    aliases = {identity: row for serializer in serializers for identity, row in serializer['aliases'].items()}
    require(examples is None or all(example + ' on IN_PROCESS' in aliases for example in examples), 'published selector has unknown source example')
    helpers = {name: sha for serializer in serializers for name, sha in serializer['helper_sources'].items()}
    require(all(inputs.get('source_files', {}).get(name) == sha for name, sha in helpers.items()), 'published serializer helper input hashes differ')
    return dict(mode=mode, examples=examples, actual_published_selector=selected, source_sha=source, run_id=inputs['run_id'], attempt=inputs['run_attempt'], shard=shard, selector_receipt_sha256=receipt_sha, run_provenance_sha256=digest(json.dumps(provenance, sort_keys=True).encode()), helpers=helpers)


def canonical_ledger(inv, raw, selector):
    policies = [method['published_policy'] for method in inv['methods'] if method.get('published_policy')]
    require(policies and all(isinstance(policy.get('ledger_serializer'), dict) for policy in policies), 'published inventory lacks a bound ledger serializer contract')
    serializers = [policy['ledger_serializer'] for policy in policies]
    aliases = {identity: row for serializer in serializers for identity, row in serializer['aliases'].items()}
    require(selector.get('mode') in ('default', 'selected'), 'published ledger selector mode is missing')
    actual = selector.get('actual_published_selector')
    source_classes = {method['class'] for method in inv['methods'] if method.get('published_policy')}
    require(isinstance(actual, list) and actual and all(isinstance(row, dict) and row.get('module') == 'e2e' and row.get('kind') == 'it' and row.get('class') in source_classes for row in actual), 'published actual selector is missing or invalid')
    derived = 'selected' if all('example' in row for row in actual) else 'default' if all('example' not in row for row in actual) and len(actual) == 1 else None
    require(derived == selector['mode'] and selector.get('examples') == ([row['example'] for row in actual] if derived == 'selected' else None), 'published serializer mode differs from actual selector')
    helpers = {name: sha for serializer in serializers for name, sha in serializer['helper_sources'].items()}
    require(selector.get('helpers') == helpers and selector.get('source_sha') == (inv.get('provider_probe', {}).get('actual_source_sha') or inv.get('source_sha')) and all(isinstance(selector.get(field), str) and re.fullmatch(r'[0-9a-f]{64}', selector[field]) for field in ('selector_receipt_sha256', 'run_provenance_sha256')), 'published serializer source/selector binding differs')
    reverse = {}
    for identity, row in aliases.items():
        label = row[selector['mode']]
        require(label not in reverse, 'ambiguous published ledger alias')
        reverse[label] = identity
    result = []
    for label in raw:
        require(label in reverse, 'unknown or wrong-mode published ledger alias')
        identity = reverse[label]
        require(selector.get('examples') is None or identity.rsplit(' on ', 1)[0] in selector['examples'], 'ledger identity is outside actual published selector')
        result.append(identity)
    return result


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
    xml_case_count = 0
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
        xml_case_count += len(actual)
        require(int(suite.get('tests', '-1')) == len(actual), 'test count differs from testcase records: ' + relative)
        require(int(suite.get('skipped', '0')) == sum(c.find('skipped') is not None for c in actual), 'skip count differs from testcase records')
        require(int(suite.get('errors', '0')) == 0 and int(suite.get('failures', '0')) == 0 and
                suite.find('.//error') is None and suite.find('.//failure') is None, 'non-success report: ' + relative)
        files[relative] = digest(path.read_bytes())
        suites.append(dict(module=module, kind=kind, **{'class': suite.get('name')}, count=len(actual), path=relative))
        for case_index, case in enumerate(actual):
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
            if not reported and case.find('skipped') is not None and not stable_markers and not markers:
                # Surefire emits one skipped nested container when an enclosing
                # class is disabled. Retain its raw count and map only the
                # independently disabled source methods, never executions.
                disabled = [m for m in inv['methods'] if m['module'] == module and m['kind'] == kind
                            and (m['class'] == classname or m['class'].startswith(classname + '$'))]
                if disabled and all(m.get('class_disabled_source') is not None and m['mapping'] == 'disabled-method'
                                    and m['parameters'] == [''] for m in disabled):
                    for method_record in disabled:
                        cases.append(dict((key, method_record[key]) for key in ('lane', 'module', 'kind', 'class', 'method'))
                            | dict(parameter='', reported=reported, status='skipped', skip=skip_record(case.find('skipped')),
                                   report=relative, report_sha256=files[relative],
                                   source_disabled_container=dict(container_class=classname, xml_case_index=case_index,
                                       source_sha256=inv['sources'][method_record['source']],
                                       disabled_source_sha256=digest(method_record['class_disabled_source'].encode()))))
                    continue
            parameter = None
            if declaration is None:
                unknown.append(dict(**base, reported=reported, reason='report has no independently discovered source method'))
            elif declaration['parameters'] is None:
                unknown.append(dict(**base, reported=reported, reason='source parameter identities are unresolved'))
            elif declaration['mapping'] == 'disabled-method':
                types = signature_types(declaration)
                signatures = {method + '(' + ', '.join(types) + ')'} | ({method} if not types else set())
                if (declaration.get('disabled_source') is not None and case.find('skipped') is not None
                        and not stable_markers and not markers and reported in signatures):
                    parameter = ''
                else:
                    unknown.append(dict(**base, reported=reported, reason='disabled source method requires one markerless skipped method report'))
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
                types = signature_types(declaration)
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
    ledger_selection, canonical_ledgers = None, []
    if ledgers or any(case['class'].endswith('.PublishedExamplesIT') for case in cases):
        if args.artifact_manifest or args.shard_plan:
            ledger_selection = ledger_selector(args, root, inv, files)
            canonical_ledgers = canonical_ledger(inv, ledgers, ledger_selection)
        else:
            unknown.append(dict(reason='published ledger has no actual selector/provenance binding'))
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
                 cases=cases, suites=suites, class_gate_records=class_records, class_gate_files=class_files, input_binding=run_binding, inputs_sha256=inputs_sha256, source_not_applicable=source_not_applicable, source_not_applicable_count=len(source_not_applicable), actual_xml_case_count=xml_case_count, ledger=ledgers, canonical_ledger=canonical_ledgers, ledger_selector=ledger_selection, files=files, unresolved=unknown, complete=inv['complete'] and not unknown)
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
        if other is None or any(policy.get(field) != other.get(field) for field in ('gate_sha256', 'synthetic_source_sha256', 'published_source_sha256', 'ledger_serializer')):
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
            try:
                normalized_ledger = canonical_ledger(target_inventory, snapshot['ledger'], snapshot.get('ledger_selector') or {})
                if normalized_ledger != snapshot.get('canonical_ledger'):
                    failures.append(label + ' canonical published ledger differs from preserved raw evidence')
                if Counter(normalized_ledger) != expected_ledger:
                    failures.append(label + ' published ledger multiset differs from source applicable executed specs and tiers')
            except ValueError as error:
                failures.append(label + ' published ledger contract rejected: ' + str(error))
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
    return parse_exec_bytes(path.read_bytes())


def parse_exec_bytes(content):
    reader = ExecReader(content)
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
    # Match capture and JaCoCo's early Java 9 compatibility adjustment exactly.
    require(content[:4] == b'\xca\xfe\xba\xbe' and len(content) >= 8, 'invalid production class file')
    if content[6:8] == b'\x00\x35':
        content = content[:7] + b'\x34' + content[8:]
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
        require(producer.get('producer_identity_source') in ('embedded-pom-properties', 'embedded-implementation-manifest', 'embedded-osgi-manifest') and type(producer.get('pom_properties_present')) is bool, 'unsupported cached JAR producer evidence')
        require(type(producer.get('own_class_count')) is int and producer['own_class_count'] >= 0, 'cached JAR own-class count is missing')
        if producer['own_class_count']:
            return False
    return True


def verified_runtime_classes(inputs, post, binding, inputs_sha256):
    """Admit structural generated-class facts bound to the actual successful shard."""
    records = post.get('runtime_generated_classes', []) if post else []
    capture = post.get('runtime_class_capture', {}) if post else {}
    if not capture.get('enabled'):
        require(not records, 'runtime classes lack an enabled current capture')
        return {}
    require(capture.get('enabled') is True and capture.get('raw_bytes_uploaded') is False and
            capture.get('jacoco_version') == inputs.get('jacoco_version') == '0.8.13', 'unsupported runtime capture')
    for field, expected in [('source_sha', binding['source_sha']), ('run_id', str(binding['run_id'])),
                            ('run_attempt', str(binding['attempt'])), ('build_inputs_sha256', inputs_sha256)]:
        require(str(capture.get(field)) == str(expected), 'runtime capture source/run/attempt/input differs: ' + field)
    require(type(capture.get('run_attempt')) is int and capture['run_attempt'] > 0, 'invalid runtime capture attempt type')
    origin = binding.get('runtime_class_capture', {})
    for field, pattern in [('shard_id', r'[A-Za-z0-9_-]+'), ('invocation_id', r'[0-9a-f]{32}'),
                           ('cohort_hash', r'[0-9a-f]{64}'), ('capture_origin_sha256', r'[0-9a-f]{64}')]:
        require(isinstance(capture.get(field), str) and re.fullmatch(pattern, capture[field]) and
                capture[field] == origin.get(field), 'runtime capture differs from actual shard provenance: ' + field)
    require(capture.get('status') == 'success', 'runtime capture has no successful selected verify')
    from datetime import datetime
    intervals = {}
    for field, phase in [('reset', 'reset-test-evidence'), ('verify', 'selected-verify')]:
        record = capture.get(field, {})
        require(record == origin.get(field), 'runtime phase differs from actual shard provenance: ' + field)
        require(type(record.get('schema')) is int and record['schema'] == 1 and record.get('event') == 'end' and
                record.get('command') == 'run' and record.get('phase') == phase and record.get('status') == 'success' and
                record.get('execution') == ('local' if str(binding['run_id']) == 'local' else 'github-actions'), 'invalid runtime phase contract: ' + field)
        for name in ('source_sha', 'run_id', 'run_attempt', 'shard_id', 'cohort_hash'):
            expected = capture[name]
            actual = record.get(name)
            if actual is None and record['execution'] == 'local' and name in ('run_id', 'run_attempt'):
                actual = 'local' if name == 'run_id' else 1
            require(str(actual) == str(expected), 'runtime phase origin differs: ' + name)
        require((record['execution'] == 'local' and record.get('run_attempt') is None) or
                (type(record.get('run_attempt')) is int and record['run_attempt'] > 0), 'invalid runtime phase attempt type')
        clocks = [record.get(name) for name in ('started_monotonic_ns', 'monotonic_ns', 'elapsed_ns')]
        require(all(type(value) is int and value >= 0 for value in clocks) and clocks[1] - clocks[0] == clocks[2],
                'invalid runtime phase monotonic clocks: ' + field)
        start_time = datetime.fromisoformat(record['utc'].replace('Z', '+00:00'))
        end_time = datetime.fromisoformat(record['ended_utc'].replace('Z', '+00:00'))
        require(start_time.tzinfo is not None and end_time.tzinfo is not None, 'runtime verify timestamps lack timezone')
        start, end = int(start_time.timestamp() * 1000), int(end_time.timestamp() * 1000)
        require(binding['started_ms'] <= start <= end <= binding['completed_ms'], 'runtime phase interval differs from coverage run')
        intervals[field] = (start, end, clocks[0], clocks[1])
    require(intervals['reset'][1] <= intervals['verify'][0] and intervals['reset'][3] <= intervals['verify'][2],
            'runtime reset must finish before selected verify')
    agent = 'org/jacoco/org.jacoco.agent/0.8.13/org.jacoco.agent-0.8.13-runtime.jar'
    require(re.fullmatch(r'[0-9a-f]{64}', capture.get('agent_sha256', '')) and
            capture['agent_sha256'] == inputs.get('dependency_repository_files', {}).get(agent), 'runtime agent bytes differ')
    known, classpath = {}, {}
    for metadata, id_field, hash_field, segment, label in [
            (inputs, 'production_class_ids', 'production_files', '/target/classes/', 'production'),
            (post, 'test_class_ids', 'test_files', '/target/test-classes/', 'test')]:
        for relative, identity in metadata[id_field].items():
            name = relative.split(segment, 1)[1].removesuffix('.class')
            row = dict(name=name, class_id=identity, sha256=metadata[hash_field][relative], origin=label)
            known.setdefault(name, set()).add(json.dumps(row, sort_keys=True))
    for entries in inputs['resolved_test_classpaths'].values():
        for entry in entries:
            classpath.setdefault(entry['identity'], set()).add(entry['sha256'])
            for name, record in entry['own_classes'].items():
                row = dict(name=name, class_id=record['class_id'], sha256=record['sha256'], origin='resolved-classpath')
                known.setdefault(name, set()).add(json.dumps(row, sort_keys=True))
    require(all(len(hashes) == 1 for hashes in classpath.values()), 'ambiguous resolved runtime framework bytes')
    families = {'spring_cglib': ('org/springframework/cglib/', ['org.springframework', 'spring-core']),
                'mockito': ('org/mockito/', ['org.mockito', 'mockito-core']),
                'bytebuddy': ('net/bytebuddy/', ['net.bytebuddy', 'byte-buddy'])}
    admitted, by_name = {}, {}
    require(isinstance(records, list) and all(isinstance(record, dict) for record in records), 'runtime class metadata must preserve each name/class ID')
    captured_names = {record.get('name') for record in records}
    for record in records:
        name, identity = record.get('name'), record.get('class_id')
        require(isinstance(name, str) and name.startswith('io/tapstate/') and re.fullmatch(r'[A-Za-z0-9_$/]+', name) and
                re.fullmatch(r'[0-9a-f]{16}', identity or '') and re.fullmatch(r'[0-9a-f]{64}', record.get('sha256', '')),
                'invalid runtime class name/ID/hash')
        require(name not in known, 'runtime generated metadata cannot exempt a compiled class')
        require((name, identity) not in admitted, 'duplicate runtime class name/ID')
        refs = record.get('class_references')
        require(isinstance(refs, list) and len(refs) == len(set(refs)) and name in refs and all(
                isinstance(ref, str) and re.fullmatch(r'(?:\[+(?:[ZBCSIJFD]|L(?:[A-Za-z0-9_$/]+|(?:[A-Za-z0-9_$/]+/)?(?:package-info|module-info));)|(?:[A-Za-z0-9_$/]+|(?:[A-Za-z0-9_$/]+/)?(?:package-info|module-info)))', ref) for ref in refs),
                'invalid runtime structural references')
        normalized = {ref.lstrip('[').removeprefix('L').removesuffix(';') if ref.startswith('[') else ref for ref in refs}
        own_references = {ref for ref in normalized if ref.startswith('io/tapstate/')}
        require(own_references <= known.keys() | captured_names, 'unclosed own structural reference')
        require(type(record.get('access_flags')) is int and 0 <= record['access_flags'] <= 65535 and
                isinstance(record.get('interfaces'), list) and len(record['interfaces']) == len(set(record['interfaces'])) and
                set(record['interfaces']) <= normalized and record.get('super_class') in normalized, 'invalid runtime class header')
        expected_compiled = {row for ref in normalized for row in known.get(ref, set())}
        require({json.dumps(row, sort_keys=True) for row in record.get('compiled_references', [])} == expected_compiled,
                'runtime compiled references differ from actual bytecode inputs')
        expected_framework = {family: sorted(ref for ref in normalized if ref.startswith(prefix))
                              for family, (prefix, unused) in families.items()}
        require(record.get('framework_references') == expected_framework and record.get('unresolved_framework_references') == [],
                'runtime framework structural references are incomplete')
        origins, seen = record.get('framework_origins'), {}
        require(isinstance(origins, list), 'runtime framework origins are missing')
        for producer in origins:
            gav = producer.get('gav')
            require(isinstance(gav, list) and len(gav) == 3 and all(isinstance(part, str) and re.fullmatch(r'[A-Za-z0-9_.+-]+', part) for part in gav),
                    'invalid runtime framework coordinates')
            relative = gav[0].replace('.', '/') + '/' + gav[1] + '/' + gav[2] + '/' + gav[1] + '-' + gav[2] + '.jar'
            require(classpath.get('repository/' + relative) == {producer.get('sha256')} and
                    post['dependency_repository_files'].get(relative) == producer.get('sha256') and cached_artifact_facts(relative, post),
                    'runtime framework producer/bytes differ from actual classpath')
            references = producer.get('class_references')
            require(isinstance(references, list) and references and len(references) == len(set(references)), 'invalid runtime framework producer references')
            for ref in references:
                matches = [family for family, (prefix, coordinate) in families.items() if ref.startswith(prefix) and gav[:2] == coordinate]
                require(len(matches) == 1 and ref in expected_framework[matches[0]], 'foreign runtime framework producer reference')
                require(ref not in seen, 'ambiguous runtime framework reference producer')
                seen[ref] = (tuple(gav), producer['sha256'])
        require(set(seen) == {ref for refs in expected_framework.values() for ref in refs}, 'runtime framework origin set differs')
        admitted[name, identity] = record
        by_name.setdefault(name, set()).add(identity)
    # Keep all variants. An auxiliary reference must bind the full captured set,
    # then each variant must have a closed structural chain to original classes.
    roles = {}
    for key, record in admitted.items():
        references = {ref.lstrip('[').removeprefix('L').removesuffix(';') if ref.startswith('[') else ref for ref in record['class_references']}
        expected_refs = {name: sorted(by_name[name]) for name in references if name in by_name and name != record['name']}
        actual_refs = record.get('runtime_references')
        require(isinstance(actual_refs, list) and len(actual_refs) == len(expected_refs) and
                {row.get('name'): row.get('class_ids') for row in actual_refs} == expected_refs, 'runtime generated reference set is incomplete or ambiguous')
        compiled = {row['name'] for row in record['compiled_references']}
        base = {name for name in compiled if record['name'].startswith(name + '$$SpringCGLIB$$')}
        family = record['framework_references']
        if base and family['spring_cglib'] and (record['super_class'] in base or record['super_class'] == 'org/springframework/cglib/reflect/FastClass'):
            roles[key] = 'spring-cglib'
        elif compiled and family['mockito'] and 'org/mockito/internal/creation/bytebuddy/access/MockAccess' in record['interfaces'] and any(
                record['name'].startswith(name + '$MockitoMock$') and (record['super_class'] == name or name in record['interfaces']) for name in compiled):
            roles[key] = 'mockito-subclass'
    pending = set(admitted) - set(roles)
    while pending:
        newly = []
        for key in pending:
            record = admitted[key]
            parents = [(ref['name'], identity) for ref in record['runtime_references'] for identity in ref['class_ids']]
            if (parents and all(parent in roles and roles[parent] == 'mockito-subclass' for parent in parents) and
                    record['super_class'] == 'java/lang/Object' and set(record['interfaces']) <= {'java/util/concurrent/Callable', 'java/lang/Runnable', 'java/io/Serializable'} and set(record['interfaces']) & {'java/util/concurrent/Callable', 'java/lang/Runnable'} and
                    any(record['name'].startswith(parent[0] + '$auxiliary$') for parent in parents)):
                newly.append((key, 'mockito-auxiliary'))
            elif (parents and all(parent in roles and roles[parent] == 'spring-cglib' for parent in parents) and
                  record['super_class'] == 'org/springframework/cglib/reflect/FastClass' and
                  record['framework_references']['spring_cglib'] and all(
                      record['name'].startswith(parent[0].split('$$SpringCGLIB$$', 1)[0] + '$$SpringCGLIB$$FastClass$$') for parent in parents)):
                newly.append((key, 'spring-cglib'))
        if not newly:
            break
        for key, role in newly:
            roles[key] = role
            pending.remove(key)
    require(not pending, 'unsupported or unanchored generated own class: ' + ', '.join(name for name, unused in sorted(pending)))
    return {key: dict(**record, verified_structural_role=roles[key],
                      framework_reference_scope='resolved-reactor-test-classpath-union',
                      runtime_realm_proven=False, generator_causality_proven=False) for key, record in admitted.items()}


def coverage_input_context(args, binding, prepared=None, tested=None):
    """Verify one original worker's prepared and post-test class declarations."""
    root = Path(args.root).resolve()
    production, test_classes, expected_names, production_modules = {}, {}, {}, {}
    repository_growth = {}
    dependency_unknown = []
    post, runtime_classes = None, {}
    observed_sha = current_sha(Path(args.production_root).resolve() if args.production_root else root)
    require(observed_sha is None or observed_sha == binding['source_sha'], 'coverage source SHA differs from production checkout')
    production_root = Path(args.production_root).resolve() if args.production_root else root
    if args.inputs:
        if prepared is None:
            raw_inputs = Path(args.inputs).read_bytes()
            inputs, inputs_sha256 = json.loads(raw_inputs), digest(raw_inputs)
        else:
            inputs, inputs_sha256 = prepared
        require(type(inputs.get('schema')) is int and inputs['schema'] == 1 and inputs.get('phase') == 'pre-test-after-install', 'invalid coverage build inputs schema/phase')
        require(inputs.get('source_sha') == binding['source_sha'] and str(inputs.get('run_id')) == str(binding['run_id']) and
                str(inputs.get('run_attempt')) == str(binding['attempt']), 'coverage build inputs source/run/attempt differs')
        require(inputs.get('jacoco_version') == '0.8.13', 'unsupported compiled coverage analyzer version')
        require(isinstance(inputs.get('production_class_access_flags'), dict) and set(inputs['production_class_access_flags']) == set(inputs['production_class_ids']), 'production class access flag set differs')
        if args.test_inputs:
            post = load(args.test_inputs) if tested is None else tested
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
    if args.inputs and post:
        runtime_classes = verified_runtime_classes(inputs, post, binding, inputs_sha256)
    require(production, 'production class bytes are required to verify JaCoCo class IDs')
    require(all(len(values) == 1 for values in expected_names.values()), 'conflicting production class IDs')
    return dict(production=production, test_classes=test_classes, expected_names=expected_names,
                production_modules=production_modules, runtime_classes=runtime_classes,
                runtime_capture=post.get('runtime_class_capture') if post else None,
                repository_growth=repository_growth, dependency_unknown=dependency_unknown,
                prepared_inputs=inputs if args.inputs else None)


def coverage_worker_contexts(args, binding, declared, base):
    """Keep every runtime capture attached to its original worker and exec files."""
    manifest_path = Path(args.worker_inputs).absolute()

    def unique(pairs):
        value = {}
        for key, item in pairs:
            require(key not in value, 'duplicate worker coverage metadata field')
            value[key] = item
        return value

    def document(path):
        require(path.is_file() and not path.is_symlink() and path.stat().st_size <= 64 * 1024 * 1024,
                'unsafe worker coverage input file')
        raw = path.read_bytes()
        return json.loads(raw, object_pairs_hook=unique), digest(raw)

    def reference(value):
        require(isinstance(value, dict) and set(value) == {'path', 'sha256'}
                and isinstance(value['path'], str) and value['path']
                and re.fullmatch(r'[0-9a-f]{64}', value.get('sha256', '')), 'invalid worker coverage reference')
        path = Path(value['path'])
        require('..' not in path.parts, 'unsafe worker coverage reference path')
        path = path if path.is_absolute() else manifest_path.parent / path
        data, checksum = document(path)
        require(checksum == value['sha256'], 'worker coverage reference hash differs')
        return path, data, checksum

    manifest, manifest_sha256 = document(manifest_path)
    require(isinstance(manifest, dict) and set(manifest) == {'schema', 'type', 'workers'}
            and type(manifest['schema']) is int and manifest['schema'] == 1
            and manifest['type'] == 'coverage-worker-inputs'
            and isinstance(manifest['workers'], dict) and 0 < len(manifest['workers']) <= 64,
            'invalid worker coverage manifest')
    required_workers, merged = binding.get('required_workers'), binding.get('merged_exec')
    require(isinstance(required_workers, list) and required_workers
            and all(isinstance(name, str) for name in required_workers)
            and len(required_workers) == len(set(required_workers))
            and set(required_workers) == set(manifest['workers']), 'worker coverage set differs from run binding')
    require(isinstance(merged, list) and merged and all(isinstance(name, str) for name in merged)
            and len(merged) == len(set(merged))
            and all(isinstance(name, str) and name.endswith('.exec') and not Path(name).is_absolute()
                    and '..' not in Path(name).parts and not name.startswith('target/ci-shards/') for name in merged)
            and set(merged) <= set(declared), 'invalid merged coverage exec binding')
    contexts, owners, records, exec_hashes = {}, {}, {}, {}
    same_fields = ('production_files', 'production_class_ids', 'production_class_access_flags',
                   'resolved_test_classpaths', 'jacoco_version', 'test_class_ids', 'test_files')
    for worker, entry in manifest['workers'].items():
        require(isinstance(worker, str) and re.fullmatch(r'[a-z0-9]+(?:-[a-z0-9]+)*', worker)
                and isinstance(entry, dict) and set(entry) == {'binding', 'inputs', 'test_inputs', 'required_exec', 'result_manifest', 'provenance'},
                'invalid worker coverage entry')
        binding_path, peer_binding, unused = reference(entry['binding'])
        inputs_path, inputs, inputs_sha256 = reference(entry['inputs'])
        post_path, post, unused = reference(entry['test_inputs'])
        packet_path, packet, unused = reference(entry['result_manifest'])
        provenance_path, provenance, unused = reference(entry['provenance'])
        require(isinstance(peer_binding, dict)
                and set(peer_binding) == {'run_id', 'attempt', 'source_sha', 'started_ms', 'completed_ms', 'runtime_class_capture'}
                and all(peer_binding.get(key) == binding[key]
                for key in ('run_id', 'attempt', 'source_sha', 'started_ms', 'completed_ms'))
                and isinstance(peer_binding.get('runtime_class_capture'), dict),
                'worker coverage run/source/time binding differs')
        require(isinstance(inputs, dict) and all(inputs.get(key) == base.get(key) for key in same_fields),
                'worker prepared class/dependency inputs differ')
        require(isinstance(packet, dict) and type(packet.get('schema')) is int and packet['schema'] == 2
                and packet.get('source_sha') == binding['source_sha'] and packet.get('shard_id') == worker
                and isinstance(packet.get('files'), dict) and isinstance(provenance, dict)
                and packet.get('provenance') == provenance, 'original worker result manifest differs')
        marker = packet.get('provenance_path')
        require(isinstance(marker, str) and marker and not Path(marker).is_absolute() and '..' not in Path(marker).parts
                and packet['files'].get(marker) == entry['provenance']['sha256'],
                'original worker provenance bytes differ')
        require(provenance.get('source_sha') == binding['source_sha'] and provenance.get('shard_id') == worker
                and str(provenance.get('run_id')) == str(binding['run_id'])
                and str(provenance.get('run_attempt')) == str(binding['attempt']),
                'original worker provenance run/source differs')
        origin = {key: provenance.get(key) for key in ('shard_id', 'invocation_id', 'cohort_hash', 'reset', 'verify')}
        receipt_capture = provenance.get('runtime_class_capture', {})
        require(isinstance(receipt_capture, dict)
                and receipt_capture.get('build_inputs_sha256') == inputs_sha256,
                'worker prepared inputs differ from original runtime capture receipt')
        observer = provenance.get('case_observer', {})
        require(isinstance(observer, dict) and (not observer.get('enabled')
                and 'build_inputs_sha256' not in observer
                or observer.get('build_inputs_sha256') == inputs_sha256),
                'worker prepared inputs differ from original case observer receipt')
        origin['capture_origin_sha256'] = receipt_capture.get('capture_origin_sha256')
        require(peer_binding['runtime_class_capture'] == origin, 'worker binding differs from original result provenance')
        capture = post.get('runtime_class_capture', {}) if isinstance(post, dict) else {}
        require(capture.get('enabled') is True and capture.get('shard_id') == worker,
                'worker runtime capture missing or belongs to another worker')
        peer_args = argparse.Namespace(**vars(args))
        peer_args.inputs, peer_args.test_inputs = str(inputs_path), str(post_path)
        context = coverage_input_context(peer_args, peer_binding, prepared=(inputs, inputs_sha256), tested=post)
        paths = entry['required_exec']
        prefix = 'target/ci-shards/' + worker + '/'
        require(isinstance(paths, list) and all(isinstance(name, str) for name in paths)
                and len(paths) == len(set(paths))
                and all(isinstance(name, str) and name.startswith(prefix) and '..' not in Path(name).parts
                        and name in declared and name not in owners for name in paths),
                'worker exec ownership is missing, duplicate or foreign')
        original_paths = packet.get('required_exec')
        require(isinstance(original_paths, list)
                and all(isinstance(name, str) and name and not Path(name).is_absolute() and '..' not in Path(name).parts
                        for name in original_paths)
                and len(original_paths) == len(set(original_paths))
                and {name.removeprefix(prefix) for name in paths} == set(original_paths),
                'worker exec set differs from original result manifest')
        for name in paths:
            checksum = packet['files'].get(name.removeprefix(prefix))
            require(isinstance(checksum, str) and re.fullmatch(r'[0-9a-f]{64}', checksum),
                    'worker exec digest is missing from original result manifest')
            exec_hashes[name] = checksum
        owners.update({name: worker for name in paths})
        contexts[worker] = context
        records[worker] = dict(binding=peer_binding, references=entry, required_exec=paths,
                               test_classes=context['test_classes'],
                               runtime_generated_classes=list(context['runtime_classes'].values()),
                               runtime_capture=context['runtime_capture'],
                               dependency_repository_growth=context['repository_growth'])
    require(set(owners) | set(merged) == set(declared) and not set(owners) & set(merged),
            'worker exec ownership does not cover the declared raw files')
    return contexts, owners, set(merged), exec_hashes, dict(manifest_sha256=manifest_sha256, workers=records)


def coverage_exec_union(executions):
    sessions, classes = set(), {}
    for data in executions:
        sessions.update((row['id'], row['start_ms'], row['end_ms']) for row in data['sessions'])
        for row in data['classes']:
            key = row['name'], row['class_id']
            if key not in classes:
                classes[key] = (row['probes'], set())
            require(classes[key][0] == row['probes'], 'inconsistent coverage probe layout')
            classes[key][1].update(row['covered_probes'])
    return sessions, classes


def coverage(args):
    root = Path(args.root).resolve()
    binding = load(args.binding)
    worker_mode = 'required_workers' in binding or 'merged_exec' in binding
    require(not worker_mode or getattr(args, 'worker_inputs', None),
            'worker coverage manifest required by the run binding')
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
    context = coverage_input_context(args, binding)
    production, test_classes = context['production'], context['test_classes']
    expected_names, production_modules = context['expected_names'], context['production_modules']
    runtime_classes = context['runtime_classes']
    repository_growth, dependency_unknown = context['repository_growth'], context['dependency_unknown']
    worker_contexts, owners, merged, worker_inputs, exec_hashes = {}, {}, set(), None, {}
    if getattr(args, 'worker_inputs', None):
        require(args.inputs and not args.test_inputs, 'worker coverage requires prepared inputs and no aggregate post-test capture')
        worker_contexts, owners, merged, exec_hashes, worker_inputs = coverage_worker_contexts(
            args, binding, declared, context['prepared_inputs'])
        dependency_unknown.extend(worker + ': ' + problem for worker, value in worker_contexts.items()
                                  for problem in value['dependency_unknown'])
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
        raw = path.read_bytes()
        checksum = digest(raw)
        require(relative not in exec_hashes or checksum == exec_hashes[relative],
                'worker exec digest differs from original result manifest')
        parsed = parse_exec_bytes(raw)
        for session in parsed['sessions']:
            require(binding['started_ms'] <= session['start_ms'] <= session['end_ms'] <= binding['completed_ms'],
                    'stale or out-of-run coverage session: ' + relative)
        selected = worker_contexts[owners[relative]] if relative in owners else context
        if relative not in merged:
            for record in parsed['classes']:
                names, runtime = selected['expected_names'], selected['runtime_classes']
                if record['name'].startswith('io/tapstate/'):
                    require(record['name'] in names or (record['name'], record['class_id']) in runtime,
                            'undeclared own bytecode in JaCoCo data: ' + record['name'])
                if record['name'] in names:
                    require(record['class_id'] in names[record['name']],
                            'JaCoCo class ID differs from production: ' + record['name'])
        executions[relative] = dict(sha256=checksum, size=len(raw), **parsed)
    if worker_inputs:
        original = coverage_exec_union(executions[name] for name in owners)
        for name in merged:
            require(coverage_exec_union([executions[name]]) == original,
                    'merged coverage differs from the verified original worker exec union')
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
                 reports=reports_found, covered_lines_branches=hits, runtime_generated_classes=list(runtime_classes.values()), runtime_capture=context['runtime_capture'], dependency_repository_growth=repository_growth, dependency_growth_accounting='all additions retained; download and verification cost remains inside original steps', unresolved=unresolved, complete=bool(reports_found) and not unresolved)
    if worker_inputs:
        value['worker_inputs'] = worker_inputs
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


def provider_record(pairs):
    value = {}
    for key, field in pairs:
        require(key not in value, 'duplicate provider result JSON field')
        value[key] = field
    return value


def validate_provider_record(row, wanted, inputs, module):
    require(isinstance(row, dict) and set(row) == {'schema', 'class', 'test_method', 'provider', 'kind', 'provider_class_sha256', 'cases'}
            and type(row['schema']) is int and row['schema'] == 1, 'invalid provider result schema')
    require(all(isinstance(row[field], str) for field in ('class', 'test_method', 'provider', 'kind', 'provider_class_sha256')), 'invalid provider result identity scalar')
    request = wanted.get((row['class'], row['test_method']))
    require(request is not None and (row['provider'], row['kind']) == request,
            'provider result differs from the source-approved request')
    class_path = module + '/target/test-classes/' + row['class'].replace('.', '/') + '.class'
    require(row['provider_class_sha256'] == inputs.get('test_files', {}).get(class_path),
            'provider class differs from prepared bytecode')
    require(isinstance(row['cases'], list) and 0 < len(row['cases']) <= 4096, 'empty or unbounded source provider')
    hex_digest = lambda value: isinstance(value, str) and re.fullmatch(r'[0-9a-f]{64}', value) is not None
    for case in row['cases']:
        require(isinstance(case, dict) and type(case.get('stable')) is bool, 'invalid provider case scalar')
        if row['kind'] == 'dynamic':
            require(set(case) == {'stable', 'display_name_digest'} and case['stable'] is True
                    and hex_digest(case['display_name_digest']), 'invalid dynamic provider hashes')
        else:
            require(set(case) == {'stable', 'argument_digest', 'first_string_digest', 'argument_descriptors'}
                    and hex_digest(case['argument_digest']) and (case['first_string_digest'] is None or hex_digest(case['first_string_digest']))
                    and isinstance(case['argument_descriptors'], list), 'invalid argument provider hashes')
            for position, descriptor in enumerate(case['argument_descriptors']):
                require(isinstance(descriptor, dict) and set(descriptor) == {'position', 'stable', 'digest'}
                        and type(descriptor['position']) is int and descriptor['position'] == position
                        and type(descriptor['stable']) is bool and hex_digest(descriptor['digest']), 'invalid argument descriptor hashes')
            require(case['stable'] == all(descriptor['stable'] for descriptor in case['argument_descriptors']),
                    'provider case stability differs from its argument descriptors')
            if case['first_string_digest'] is not None:
                require(case['argument_descriptors'] and case['argument_descriptors'][0]['stable'] is True
                        and case['first_string_digest'] == case['argument_descriptors'][0]['digest'],
                        'provider first-string digest differs from its stable first argument')
    return class_path


def provider_invocation_receipt(root, inv, inputs, args, helper_hashes, invocations):
    contract_path = Path(args.contracts).resolve()
    require(contract_path.is_relative_to(root), 'provider receipt contracts must belong to the prepared checkout')
    relative = contract_path.relative_to(root).as_posix()
    contracts_hash = digest(contract_path.read_bytes())
    require(inputs.get('source_files', {}).get(relative) == contracts_hash,
            'provider receipt contracts differ from prepared inputs')
    probe = inv['provider_probe']
    resolved_keys = {(module, row['class'], row['test_method']) for module, rows in probe['module_records'].items() for row in rows}
    methods = []
    for method in inv['methods']:
        if (method['module'], method['class'], method['method']) not in resolved_keys:
            continue
        cases = [{field: method[field] for field in ('lane', 'module', 'kind', 'class', 'method')} | {'parameter': parameter}
                 for parameter in method['parameters']]
        methods.append({field: method[field] for field in ('lane', 'module', 'kind', 'class', 'method', 'source', 'mapping')} |
                       {'provider_contract': method['provider_contract'], 'case_digests': sorted(case_hash(case) for case in cases)})
    require(len(methods) == sum(call['requested_method_count'] for call in invocations),
            'provider receipt method count differs from successful invocations')
    context = inputs.get('runner_context')
    require(isinstance(context, dict), 'provider receipt requires explicit prepared runner context')
    return {'schema': 1, 'type': 'provider-probe-receipt',
            'binding': {field: inputs.get(field) for field in ('source_sha', 'source_tree', 'run_id', 'run_attempt', 'event')},
            'runner_context': context, 'lane': inv['lane'], 'original_source_sha': inv['source_sha'],
            'original_inventory_sha256': probe['original_inventory_sha256'],
            'producer_script_sha256': probe['producer_script_sha256'],
            'producer_prepared_source_bound': inputs.get('source_files', {}).get('.github/scripts/_ci-evidence.py') == probe['producer_script_sha256'],
            'build_inputs_sha256': probe['inputs_sha256'], 'contracts': {'path': relative, 'sha256': contracts_hash},
            'helper_class_hashes': helper_hashes, 'resolved_classpaths': probe['resolved_classpaths'],
            'resource_hashes': probe['resource_hashes'], 'invocations': invocations, 'resolved_methods': methods,
            'resolved_method_count': len(methods), 'resolved_case_count': sum(len(m['case_digests']) for m in methods),
            'capture_status': 'diagnostic' if probe['diagnostic'] else ('success' if invocations else 'not-required'),
            'inventory_complete': inv['complete'], 'before_test_selection': True,
            'project_original': bool(args.project_original), 'independent_provider_replay_proven': False,
            'ability_proven': False, 'whole_build_provenance_complete': False}


def resolve_providers(args):
    root, repository = Path(args.root).resolve(), Path(args.repo_local).resolve()
    receipt_path = getattr(args, 'probe_receipt', None)
    if receipt_path:
        target = Path(receipt_path)
        require(not target.exists() and not target.is_symlink(), 'provider receipt must be a new file')
        require(target.resolve() != Path(args.output).resolve(), 'provider receipt and inventory outputs must differ')
        resolved_output = Path(args.output)
        require(not resolved_output.exists() and not resolved_output.is_symlink(), 'provider resolved inventory must be a new file')
        require(all(not path.resolve().is_relative_to(location) for path in (target, resolved_output) for location in (root, repository)),
                'provider capture outputs must stay outside the prepared checkout and dependency repository')
    inventory_raw, inputs_raw, contracts_raw = (Path(path).read_bytes() for path in (args.inventory, args.inputs, args.contracts))
    inv, inputs, contracts = (json.loads(raw) for raw in (inventory_raw, inputs_raw, contracts_raw))
    if receipt_path:
        actual_source = current_sha(root)
        require(isinstance(actual_source, str) and re.fullmatch(r'[0-9a-f]{40}', actual_source) is not None
                and inputs.get('source_sha') == actual_source, 'provider prepared inputs differ from the actual checkout')
        tree = subprocess.check_output(['git', '-C', str(root), 'rev-parse', 'HEAD^{tree}'], text=True).strip()
        require(inputs.get('source_tree') == tree, 'provider prepared tree differs from the actual checkout')
        for field, variable in (('run_id', 'GITHUB_RUN_ID'), ('run_attempt', 'GITHUB_RUN_ATTEMPT'), ('event', 'GITHUB_EVENT_NAME'), ('source_sha', 'GITHUB_SHA')):
            if variable in os.environ:
                require(str(inputs.get(field)) == os.environ[variable], 'provider prepared runner identity differs: ' + field)
        contract_path = Path(args.contracts).resolve()
        require(contract_path.is_relative_to(root)
                and inputs.get('source_files', {}).get(contract_path.relative_to(root).as_posix()) == digest(contracts_raw),
                'provider receipt contracts differ from prepared checkout inputs')
    require(inv['type'] == 'source-inventory' and contracts.get('schema') == 1, 'invalid provider inventory/contract schema')
    require(inputs.get('schema') == 1 and inputs.get('phase') == 'pre-test-after-install', 'provider probe requires pre-test prepared inputs')
    if inputs.get('source_sha') != inv.get('source_sha'):
        require(args.project_original and inv.get('source_origin', {}).get('kind') == 'authenticated-git-archive', 'provider probe source differs without authenticated original projection')
        require(inv.get('original_product_sources'), 'original product input hashes are missing')
        for relative, expected in inv['original_product_sources'].items():
            require(inputs.get('source_files', {}).get(relative) == expected and (root / relative).is_file() and digest((root / relative).read_bytes()) == expected, 'original product source changed during HOW projection: ' + relative)
    original_sha = digest(inventory_raw)
    producer_sha = digest(Path(__file__).read_bytes())
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
    helper_hashes = {path.relative_to(root).as_posix(): digest(path.read_bytes()) for path in helper_files}
    observed, resource_hashes, classpath_bindings = {}, {}, {}
    invocations, module_records = [], {}
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
            started_at, started_ns = datetime.now(timezone.utc).isoformat(), time.monotonic_ns()
            command = [args.java, '-cp', os.pathsep.join(classpath), 'io.tapstate.testsupport.CiProviderInventory', str(requests), str(result)]
            run = subprocess.run(command, cwd=root / module, capture_output=True, text=True, timeout=120)
            elapsed_ns, ended_at = time.monotonic_ns() - started_ns, datetime.now(timezone.utc).isoformat()
            require(run.returncode == 0 and result.is_file(), 'bounded provider enumeration failed for module ' + module)
            result_raw = result.read_bytes()
            require(len(result_raw) <= 64 * 1024 * 1024, 'provider result exceeds metadata budget')
            invocations.append({'module': module, 'requested_methods': [
                {'class': method['class'], 'method': method['method'], 'kind': method['kind'], 'provider': name, 'provider_kind': kind}
                for method, name, kind, _ in rows], 'requested_method_count': len(rows),
                'request_sha256': digest(requests.read_bytes()), 'result_sha256': digest(result_raw),
                'helper_jar_sha256': digest(helper.read_bytes()), 'helper_entry_hashes_sha256': digest(json.dumps(helper_hashes, sort_keys=True, separators=(',', ':')).encode()),
                'stdout_sha256': digest(run.stdout.encode()), 'stderr_sha256': digest(run.stderr.encode()),
                'java_command_sha256': digest(args.java.encode()), 'exit_code': run.returncode,
                'started_at': started_at, 'ended_at': ended_at, 'elapsed_ns': elapsed_ns})
            module_records[module] = []
            wanted = {(method['class'], method['method']): (name, kind) for method, name, kind, _ in rows}
            require(len(wanted) == len(rows), 'duplicate source provider request')
            test_kinds = {(method['class'], method['method']): method['kind'] for method, *_ in rows}
            for line in result_raw.decode('utf-8').splitlines():
                row = json.loads(line, object_pairs_hook=provider_record)
                validate_provider_record(row, wanted, inputs, module)
                key = (inv['lane'], module, test_kinds[(row['class'], row['test_method'])], row['class'], row['test_method'])
                require(key not in observed and key in selected, 'duplicate or foreign provider probe record')
                observed[key] = row
                module_records[module].append(row)
            invocations[-1]['result_record_count'] = len(module_records[module])
            invocations[-1]['result_records_sha256'] = digest(json.dumps(module_records[module], sort_keys=True, separators=(',', ':')).encode())
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
    inv['provider_probe'] = dict(schema=1, producer_script_sha256=producer_sha, original_inventory_sha256=original_sha, inputs_sha256=digest(inputs_raw),
                                 contracts_sha256=digest(contracts_raw), before_test_selection=True,
                                 resolved_records=list(observed.values()), module_records=module_records, resource_hashes=resource_hashes,
                                 resolved_classpaths=classpath_bindings, diagnostic=bool(problems), actual_source_sha=inputs['source_sha'], original_source_sha=inv['source_sha'], project_original=bool(args.project_original))
    if receipt_path:
        require(digest(Path(__file__).read_bytes()) == producer_sha, 'provider producer source changed during enumeration')
        require(all(digest(Path(path).read_bytes()) == digest(raw) for path, raw in
                    ((args.inventory, inventory_raw), (args.inputs, inputs_raw), (args.contracts, contracts_raw))),
                'provider frozen input changed during enumeration')
        capture = provider_invocation_receipt(root, inv, inputs, args, helper_hashes, invocations)
        target.parent.mkdir(parents=True, exist_ok=True)
        descriptor = os.open(target, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        with os.fdopen(descriptor, 'w') as stream:
            stream.write(json.dumps(capture, indent=2, sort_keys=True) + '\n')
        inv['provider_probe']['receipt_sha256'] = digest(target.read_bytes())
        print('CI_PROVIDER_PROBE_RECEIPT_SHA256 ' + inv['provider_probe']['receipt_sha256'], flush=True)
    if receipt_path:
        resolved_target = Path(args.output)
        resolved_target.parent.mkdir(parents=True, exist_ok=True)
        descriptor = os.open(resolved_target, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        with os.fdopen(descriptor, 'w') as stream:
            stream.write(json.dumps(inv, indent=2, sort_keys=True) + '\n')
    else:
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
            if record.get('source_disabled_container'):
                row['source_disabled_container'] = record['source_disabled_container']
            if record['status'] == 'not_applicable':
                row['source_gate_evidence'] = {field: record['evidence'][field] for field in ('contract_sha256', 'source_sha256', 'compiled_class_sha256', 'reason_digest', 'callback')}
                row['source_gate_evidence']['predicate_flags_sha256'] = digest(json.dumps(record['evidence']['gate_inputs'], sort_keys=True, separators=(',', ':')).encode())
        cases.append(row)
    methods = []
    for method in value.get('methods', []):
        row = {field: method[field] for field in ('lane', 'module', 'kind', 'class', 'method', 'source')}
        row['declaration_sha256'] = method['declaration_sha256']
        row['mapping'] = method['mapping']
        row['signature_types'] = signature_types(method)
        row['source_sha256'] = value['sources'].get(method['source'])
        row['provider_contract_sha256'] = digest(json.dumps(method.get('provider_contract'), sort_keys=True, separators=(',', ':')).encode())
        methods.append(row)
    public = dict(schema=SCHEMA, type='expected-case-hashes' if value['type'] == 'source-inventory' else 'observed-case-hashes',
                  complete=value.get('complete') is True, source_sha=value.get('source_sha'),
                  source_inventory_sha256=value.get('source_inventory_sha256') or digest(Path(args.input).read_bytes()),
                  source_origin=({field: value['source_origin'][field] for field in ('kind', 'source_sha', 'source_archive_sha256', 'source_receipt_sha256') if field in value['source_origin']} if value.get('source_origin') else None), actual_source_sha=value.get('provider_probe', {}).get('actual_source_sha'),
                  original_source_sha=value.get('provider_probe', {}).get('original_source_sha'),
                  provider_probe_sha256=digest(json.dumps(value.get('provider_probe'), sort_keys=True, separators=(',', ':')).encode()),
                  provider_probe_receipt_sha256=value.get('provider_probe', {}).get('receipt_sha256'),
                  original_inventory_sha256=value.get('provider_probe', {}).get('original_inventory_sha256'),
                  input_binding=value.get('input_binding'), inputs_sha256=value.get('inputs_sha256'), class_gate_files=value.get('class_gate_files', {}), actual_xml_case_count=value.get('actual_xml_case_count'), source_not_applicable_count=value.get('source_not_applicable_count'), ledger_digests=[digest(entry.encode()) for entry in value.get('canonical_ledger', [])], raw_ledger_digests=[digest(entry.encode()) for entry in value.get('ledger', [])], ledger_selector_sha256=digest(json.dumps(value.get('ledger_selector'), sort_keys=True).encode()), methods=methods, cases=cases, case_count=len(cases), unresolved_count=len(value.get('unresolved', [])), ability_proven=False)
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
    rep.add_argument('--artifact-manifest', help='Packed shard manifest binding actual published selector and provenance')
    rep.add_argument('--shard-plan', help='Pre-pack shard plan bound to the successful actual verify receipt')
    rep.add_argument('--shard', help='Shard identity in the supplied selector plan')
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
    cov.add_argument('--worker-inputs', help='Hash-bound per-worker inputs, runtime captures and original exec ownership')
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
    resolve.add_argument('--probe-receipt', help='Write a new hash-only receipt and resolved inventory outside prepared inputs; not independent semantic replay')
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
