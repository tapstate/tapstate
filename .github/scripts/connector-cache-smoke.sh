#!/usr/bin/env bash
# Exercise snapshot freshness, complete provenance, and cached jar admission without network access.
set -euo pipefail
python3 - "$(cd "$(dirname "$0")" && pwd)/connector-cache.sh" <<'PY'
import json, os, pathlib, subprocess, sys, tempfile
helper = pathlib.Path(sys.argv[1])
with tempfile.TemporaryDirectory() as temporary:
    root = pathlib.Path(temporary)
    checkout, repo, jars, bin_dir = [root / name for name in ('checkout', 'repository', 'jars', 'bin')]
    for directory in (checkout, repo, jars, bin_dir): directory.mkdir()
    subprocess.run(['git', 'init', '-q', str(checkout)], check=True)
    (checkout / 'pom.xml').write_text('<project/>')
    subprocess.run(['git', '-C', str(checkout), 'add', '.'], check=True)
    subprocess.run(['git', '-C', str(checkout), '-c', 'user.name=Test', '-c', 'user.email=test@example.com', 'commit', '-qm', 'fixture'], check=True)
    mock = bin_dir / 'mvn'
    mock.write_text('''#!/usr/bin/env python3
import json, os, pathlib, sys, xml.etree.ElementTree as ET
args = sys.argv[1:]
if os.environ.get('METADATA_WARNING'): print('[WARNING] Could not transfer metadata; using local cache')
def value(prefix): return next(x[len(prefix):] for x in args if x.startswith(prefix))
repo = pathlib.Path(value('-Dmaven.repo.local='))
stamp = os.environ.get('SNAPSHOT_BUILD', '20260908.010203-1')
if any('effective-pom' in arg for arg in args):
    projects = ET.Element('projects')
    for module, version, kind in [('mysql', '2.0-SNAPSHOT', 'jar'), ('mongodb', '2.1-SNAPSHOT', 'jar'), ('postgres', '2.1-SNAPSHOT', 'test-jar')]:
        project = ET.SubElement(projects, 'project')
        for field, text in [('groupId', 'io.tapdata'), ('artifactId', module + '-connector'), ('version', '1.0-SNAPSHOT')]: ET.SubElement(project, field).text = text
        dependency = ET.SubElement(ET.SubElement(project, 'dependencies'), 'dependency')
        for field, text in [('groupId', 'io.tapdata'), ('artifactId', 'tapdata-pdk-api'), ('version', version), ('type', kind)]: ET.SubElement(dependency, field).text = text
    ET.ElementTree(projects).write(value('-Doutput='))
else:
    resolver = ET.parse(args[args.index('-f') + 1]).getroot()
    invocations = []
    def resolve(group, artifact, version, extension='jar', classifier=''):
        if artifact == os.environ.get('OMIT_COMPONENT'): return
        directory = repo / group.replace('.', '/') / artifact / version
        directory.mkdir(parents=True, exist_ok=True)
        resolved = version[:-9] + '-' + stamp
        records = ''
        # Metadata describes all published variants; only the requested variant and its POM
        # become local aliases. This catches lost classifiers independently of the helper.
        for ext, label in [('pom', ''), ('jar', ''), ('jar', 'tests')]:
            records += f'<snapshotVersion><extension>{ext}</extension><classifier>{label}</classifier><value>{resolved}</value></snapshotVersion>'
        for ext, label in {('pom', ''), (extension, classifier)}:
            suffix = ('-' + label if label else '') + '.' + ext
            content = f'{artifact}-{resolved}{suffix}'.encode()
            (directory / f'{artifact}-{version}{suffix}').write_bytes(content)
            (directory / f'{artifact}-{resolved}{suffix}').write_bytes(content)
        (directory / 'maven-metadata-fixture.xml').write_text(f'<metadata><groupId>{group}</groupId><artifactId>{artifact}</artifactId><version>{version}</version><versioning><snapshotVersions>{records}</snapshotVersions></versioning></metadata>')
    for execution in resolver.findall('./build/plugins/plugin/executions/execution'):
        assert execution.findtext('goals/goal') == 'get'
        coordinate = execution.findtext('configuration/artifact')
        parts = coordinate.split(':')
        assert len(parts) in (4, 5), coordinate
        group, artifact, version, kind = parts[:4]
        classifier = parts[4] if len(parts) == 5 else ''
        transitive = execution.findtext('configuration/transitive') == 'true'
        invocations.append([group, artifact, version, kind, classifier, transitive])
        resolve(group, artifact, version, kind, classifier)
        if transitive:
            # The API's external dependency is not directly declared by a connector.
            resolve('io.tapdata', 'pdk-error-code', '2.0-SNAPSHOT')
    (repo / 'resolution-invocations.json').write_text(json.dumps(invocations))
''')
    mock.chmod(0o755)
    env = dict(os.environ, PATH=f'{bin_dir}:{os.environ["PATH"]}')
    manifest = root / 'probe.json'
    def call(*args, ok=True, extra=None):
        result = subprocess.run([str(helper), *map(str, args)], env=dict(env, **(extra or {})), capture_output=True, text=True)
        if (result.returncode == 0) != ok: raise AssertionError(f'{args}: {result.stdout}\n{result.stderr}')
        return result.stdout.strip()
    def prepare(output=manifest, **extra):
        call('prepare', '--checkout', checkout, '--repo-local', repo, '--output', output, extra=extra)
        return call('key', '--manifest', output)
    first = prepare()
    expected_roots = [
        ['io.tapdata', 'tapdata-pdk-api', '2.0-SNAPSHOT', 'jar', ''],
        ['io.tapdata', 'tapdata-pdk-api', '2.1-SNAPSHOT', 'jar', ''],
        ['io.tapdata', 'tapdata-pdk-api', '2.1-SNAPSHOT', 'jar', 'tests'],
    ]
    evidence = json.loads(manifest.read_text())
    assert evidence['roots'] == expected_roots, 'effective POM root versions or classifiers lost'
    assert json.loads((repo / 'resolution-invocations.json').read_text()) == [entry + [True] for entry in expected_roots], 'resolver omitted roots, versions, classifiers, or transitive closure'
    expected_aliases = {
        'io/tapdata/tapdata-pdk-api/2.0-SNAPSHOT/tapdata-pdk-api-2.0-SNAPSHOT.jar',
        'io/tapdata/tapdata-pdk-api/2.0-SNAPSHOT/tapdata-pdk-api-2.0-SNAPSHOT.pom',
        'io/tapdata/tapdata-pdk-api/2.1-SNAPSHOT/tapdata-pdk-api-2.1-SNAPSHOT.jar',
        'io/tapdata/tapdata-pdk-api/2.1-SNAPSHOT/tapdata-pdk-api-2.1-SNAPSHOT-tests.jar',
        'io/tapdata/tapdata-pdk-api/2.1-SNAPSHOT/tapdata-pdk-api-2.1-SNAPSHOT.pom',
        'io/tapdata/pdk-error-code/2.0-SNAPSHOT/pdk-error-code-2.0-SNAPSHOT.jar',
        'io/tapdata/pdk-error-code/2.0-SNAPSHOT/pdk-error-code-2.0-SNAPSHOT.pom',
    }
    assert set(evidence['snapshots']) == expected_aliases, 'resolved snapshot closure differs from fixture dependency graph'
    assert first == prepare(), 'unchanged snapshots changed key'
    second = prepare(root / 'new.json', SNAPSHOT_BUILD='20260909.010203-2')
    assert first != second, 'republished PDK did not invalidate key'
    prepare()
    call('prepare', '--checkout', checkout, '--repo-local', repo, '--output', manifest, extra={'METADATA_WARNING':'1'}, ok=False)
    for name in ('mysql', 'mongodb', 'postgres'): (jars / f'{name}-connector-v1.jar').write_bytes(b'connector')
    call('seal', '--checkout', checkout, '--manifest', manifest, '--repo-local', repo, '--jars', jars)
    call('verify', '--manifest', manifest, '--jars', jars)
    # A different probe must never accept an older restored cache.
    call('verify', '--manifest', root / 'new.json', '--jars', jars, ok=False)
    extra_jar = jars / 'unexpected.jar'
    extra_jar.write_bytes(b'extra')
    call('verify', '--manifest', manifest, '--jars', jars, ok=False)
    extra_jar.unlink()
    (checkout / 'pom.xml').write_text('<project>changed</project>')
    call('seal', '--checkout', checkout, '--manifest', manifest, '--repo-local', repo, '--jars', jars, ok=False)
    (checkout / 'pom.xml').write_text('<project/>')
    jar = jars / 'mysql-connector-v1.jar'
    jar.write_bytes(b'changed')
    call('verify', '--manifest', manifest, '--jars', jars, ok=False)
    jar.write_bytes(b'connector')
    metadata = repo / 'io/tapdata/pdk-error-code/2.0-SNAPSHOT/maven-metadata-fixture.xml'
    saved = metadata.read_text()
    metadata.unlink()
    call('seal', '--checkout', checkout, '--manifest', manifest, '--repo-local', repo, '--jars', jars, ok=False)
    metadata.write_text(saved.replace('2.0-20260908.010203-1', '2.0-SNAPSHOT'))
    call('seal', '--checkout', checkout, '--manifest', manifest, '--repo-local', repo, '--jars', jars, ok=False)
    metadata.write_text(saved)
    alias = repo / 'io/tapdata/pdk-error-code/2.0-SNAPSHOT/pdk-error-code-2.0-SNAPSHOT.jar'
    alias.write_bytes(b'locally installed stale artifact')
    call('seal', '--checkout', checkout, '--manifest', manifest, '--repo-local', repo, '--jars', jars, ok=False)
    prepare()
    import shutil
    shutil.rmtree(repo / 'io/tapdata/pdk-error-code')
    call('seal', '--checkout', checkout, '--manifest', manifest, '--repo-local', repo, '--jars', jars, ok=False)
    # A direct PDK component cannot disappear from the effective reactor selection.
    shutil.rmtree(repo / 'io/tapdata/tapdata-pdk-api')
    call('prepare', '--checkout', checkout, '--repo-local', repo, '--output', manifest, extra={'OMIT_COMPONENT':'tapdata-pdk-api'}, ok=False)
    print('connector cache smoke: freshness, jar integrity, missing metadata/component, and build drift passed')
PY
