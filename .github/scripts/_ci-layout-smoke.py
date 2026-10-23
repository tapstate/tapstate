#!/usr/bin/env python3
"""Layout and matrix controls preserve the complete independently seeded source set."""
from collections import Counter
import copy
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile

sys.dont_write_bytecode = True
HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location('layout_shards', HERE / '_ci-shards.py')
s = importlib.util.module_from_spec(spec)
spec.loader.exec_module(s)
count = 0


def check(value, message):
    global count
    assert value, message
    count += 1


def rejected(call, message):
    try:
        call()
    except (ValueError, KeyError, TypeError):
        check(True, message)
    else:
        raise AssertionError('accepted ' + message)


with tempfile.TemporaryDirectory(prefix='ci-layout-controls-') as temporary:
    root = Path(temporary).resolve()

    def put(name, value):
        path = root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(value if isinstance(value, str) else json.dumps(value, sort_keys=True) + '\n')
        return path

    modules = ['e2e', 'runtime/engine', 'app', 'core', 'arch-tests']
    put('pom.xml', '<project><modules>' + ''.join('<module>' + module + '</module>' for module in modules) + '</modules></project>')
    for module in modules:
        put(module + '/pom.xml', '<project><build><plugins><plugin><artifactId>maven-failsafe-plugin</artifactId><executions><execution><goals><goal>integration-test</goal></goals></execution></executions></plugin></plugins></build></project>')
    seed = [('e2e', 'Example' + str(i) + 'IT', 'it') for i in range(1, 7)] + [
        ('e2e', 'SelectorTest', 'unit'), ('runtime/engine', 'EngineTest', 'unit'),
        ('app', 'AppTest', 'unit'), ('app', 'AppFixtureIT', 'it'),
        ('app', 'TestBothIT', 'unit'), ('app', 'TestBothIT', 'it'),
        ('app', 'AppSlowIT', 'it'), ('app', 'AppTinyTest', 'unit'),
        ('core', 'CoreTest', 'unit'), ('arch-tests', 'RulesTest', 'unit')]
    for module, name, unused in seed:
        put(module + '/src/test/java/sample/' + name + '.java', 'package sample; class ' + name + ' {}')
    independent = Counter((module, kind, 'sample.' + name) for module, name, kind in seed)
    hints = {'sample.Example1IT': 100, 'sample.Example2IT': 80, 'sample.AppTest': 1, 'app/sample.AppTest': 40}
    put('durations.json', hints)

    def plan(layout=None):
        config = root / s.LAYOUT_PATH
        if layout is None:
            config.unlink(missing_ok=True)
        else:
            put(s.LAYOUT_PATH, layout)
        command = ['bash', str(HERE / 'ci-shard.sh'), 'plan', '--root', str(root),
                   '--durations', str(root / 'durations.json'), '--output', str(root / 'plan.json')]
        result = subprocess.run(command, capture_output=True, text=True)
        check(result.returncode == 0, 'actual planner succeeds: ' + result.stderr)
        value = json.loads((root / 'plan.json').read_bytes())
        check(Counter(s.class_key(test) for test in value['expected']) == independent, 'source discovery equals the independently seeded set')
        check(Counter(s.class_key(test) for shard in value['shards'] for test in shard['tests']) == independent,
              'every seeded source unit remains assigned exactly once')
        return value

    def matrix(value=None):
        if value is not None:
            put('plan.json', value)
        return subprocess.run(['bash', str(HERE / 'ci-shard.sh'), 'matrix', '--root', str(root),
                               '--plan', str(root / 'plan.json')], capture_output=True, text=True)

    legacy = plan()
    check(legacy['layout_source']['legacy_defaults'] and legacy['duration_hints']['fallback_seconds'] == 1,
          'missing default layout preserves explicit legacy fallback')
    legacy_matrix = matrix()
    check(legacy_matrix.returncode == 0 and [row['shard'] for row in json.loads(legacy_matrix.stdout)['include']]
          == ['e2e-1', 'e2e-2', 'engine', 'rest'], 'legacy layout still emits the original four workers')
    cohorts = set()
    for workers in (2, 4, 6):
        layout = {'schema': 1, 'e2e_shards': workers, 'rest_modules': {}, 'new_test_seconds': 30.0}
        value = plan(layout)
        cohorts.add(value['cohort_hash'])
        result = matrix()
        check(result.returncode == 0, 'actual matrix CLI succeeds: ' + result.stderr)
        actual = json.loads(result.stdout)['include']
        check([row['shard'] for row in actual] == [row['id'] for row in value['shards']], 'matrix consumes the exact plan IDs')
        check(sum(row['prepare_cloud_inputs'] for row in actual) == 1 and actual[-1]['prepare_cloud_inputs'],
              'original residual tests retain Cloud inputs without preparing E2E/engine workers')
        check(all(not shard['required_exec'] for shard in value['shards'][:workers]), 'every E2E IT worker retains XML-only coverage treatment')
        for shard in value['shards']:
            expected = sum(hints.get(test['module'] + '/' + s.hint_key(test), hints.get(s.hint_key(test), 30)) for test in shard['tests'])
            check(shard['estimated_seconds'] == expected, 'loads use independent hint lookup and the declared fallback')
        missing = {module + '/sample.' + name for module, name, kind in seed
                   if module + '/sample.' + name not in hints and 'sample.' + name not in hints}
        check(set(value['duration_hints']['missing']) == missing and value['duration_hints']['estimates_only'],
              'new-source fallback identities and estimate limits remain explicit')
    check(len(cohorts) == 1, '2/4/6 layouts do not change the source cohort')
    layout = {'schema': 1, 'e2e_shards': 4, 'rest_modules': {'app': ['app']}, 'new_test_seconds': 30}
    grouped = plan(layout)
    result = matrix(); check(result.returncode == 0, 'independent app module group emits a matrix')
    active = {row['shard'] for row in json.loads(result.stdout)['include'] if row['prepare_cloud_inputs']}
    check(active == {'app', 'rest'}, 'all groups of original residual tests retain the original Cloud environment')
    check({test['module'] for test in s.selected(grouped, 'app')['tests']} == {'app'}
          and all(test['module'] != 'app' for test in s.selected(grouped, 'rest')['tests']), 'an entire module moves without splitting its classes')
    put(s.LAYOUT_PATH, dict(layout, e2e_shards=6))
    check(matrix().returncode != 0, 'source layout changes after planning reject')
    put(s.LAYOUT_PATH, layout)
    foreign = copy.deepcopy(grouped)
    foreign['layout']['schema'] = True
    check(matrix(foreign).returncode != 0, 'boolean schema cannot impersonate the captured layout')
    foreign = copy.deepcopy(grouped)
    first, engine = foreign['shards'][0], next(shard for shard in foreign['shards'] if shard['id'] == 'engine')
    first['tests'][0], engine['tests'][0] = engine['tests'][0], first['tests'][0]
    first['required_exec'], engine['required_exec'] = s.execs(first['tests']), s.execs(engine['tests'])
    check(matrix(foreign).returncode != 0, 'cohort-preserving foreign module assignments reject')
    foreign = copy.deepcopy(grouped)
    foreign['shards'].append({'id': 'extra', 'tests': [], 'estimated_seconds': 0, 'required_exec': []})
    check(matrix(foreign).returncode != 0, 'unplanned empty worker cannot expand the matrix')
    for bundles in (2,4):
        app_layout={'schema':1,'e2e_shards':4,'rest_modules':{},'new_test_seconds':30,'app_shards':bundles}
        bundled=plan(app_layout); result=matrix()
        check(result.returncode==0,'whole App class matrix succeeds: '+result.stderr)
        check([row['shard'] for row in json.loads(result.stdout)['include']]==
              ['e2e-1','e2e-2','e2e-3','e2e-4','engine']+['app-'+str(i+1) for i in range(bundles)]+['rest'],
              'matrix IDs derive only from source layout')
        owners={}
        for shard in bundled['shards']:
            for test in shard['tests']:
                if test['module']=='app':owners.setdefault(test['class'],set()).add(shard['id'])
            if shard['id'].startswith('app-'):
                check(shard['tests'] and all(t['module']=='app' for t in shard['tests']),'App workers contain complete App source units only')
                check(shard['required_exec']==s.execs(shard['tests']),'each App worker retains its own required coverage files')
            check(shard['estimated_seconds']==sum(hints.get(t['module']+'/'+s.hint_key(t),hints.get(s.hint_key(t),30)) for t in shard['tests']),
                  'class-group estimates preserve the independent source duration weights')
        check(all(len(v)==1 and next(iter(v)).startswith('app-') for v in owners.values()),'unit and IT selections of one top-level class stay together')
        check(sum(row['prepare_cloud_inputs'] for row in json.loads(result.stdout)['include'])==bundles+1,
              'every independent App/rest worker retains original Cloud preparation')
        foreign=copy.deepcopy(bundled)
        owner=next(sh for sh in foreign['shards'] if any(t['class']=='sample.TestBothIT' for t in sh['tests']))
        target=next(sh for sh in foreign['shards'] if sh['id'].startswith('app-') and sh is not owner)
        unit=next(t for t in owner['tests'] if t['class']=='sample.TestBothIT' and t['kind']=='unit')
        owner['tests'].remove(unit);target['tests'].append(unit)
        owner['required_exec'],target['required_exec']=s.execs(owner['tests']),s.execs(target['tests'])
        result=matrix(foreign)
        check(result.returncode!=0 and 'source class is split' in result.stderr,'exact-cohort split across App workers rejects')
        foreign=copy.deepcopy(bundled);foreign['shards'][5]['id']='app-extra'
        check(matrix(foreign).returncode!=0,'unconfigured App worker ID rejects')
        for label,change in [('bad App count',{'app_shards':3}),('boolean App count',{'app_shards':True}),
                             ('explicit singleton App count',{'app_shards':1}),
                             ('App module double assignment',{'rest_modules':{'isolated':['app']}}),
                             ('App namespace collision',{'rest_modules':{'app-1':['core']}})]:
            put(s.LAYOUT_PATH,dict(app_layout,**change))
            rejected(lambda:s.default_groups(s.discover(root),hints,s.layout_config(type('Args',(),{'layout':s.LAYOUT_PATH})(),root)[0]),label)
        rejected(lambda:s.balance_classes([t for t in s.discover(root) if t['module']=='app' and t['class']=='sample.TestBothIT'],2,hints,30),
                 'two unit/IT entries of one source class cannot fill two App shards')
    zero_tests=[{'module':'app','kind':'it','class':'sample.Zero'+str(i)+'IT'} for i in range(4)]
    zero_groups=s.balance_classes(zero_tests,4,{test['class']:0 for test in zero_tests},30)
    check(all(tests for tests,total in zero_groups) and sum(len(tests) for tests,total in zero_groups)==4,
          'legitimate zero-time hints still produce all nonempty App workers')
    put(s.LAYOUT_PATH,layout)
    for label, change in [
        ('unsupported count', {'e2e_shards': 3}), ('boolean count', {'e2e_shards': True}),
        ('zero fallback', {'new_test_seconds': 0}), ('nonfinite fallback', {'new_test_seconds': float('inf')}),
        ('reserved worker', {'rest_modules': {'engine': ['app']}}),
        ('aggregate input namespace', {'rest_modules': {'build': ['app']}}),
        ('reserved E2E namespace', {'rest_modules': {'e2e-extra': ['app']}}),
        ('duplicate module', {'rest_modules': {'app': ['app'], 'also': ['app']}}),
        ('unknown residual module', {'rest_modules': {'unknown': ['missing']}}),
    ]:
        bad = dict(layout, **change); put(s.LAYOUT_PATH, bad)
        args = type('Args', (), {'layout': s.LAYOUT_PATH})()

        def configure():
            parsed, unused = s.layout_config(args, root)
            s.default_groups(s.discover(root), hints, parsed)

        rejected(configure, label)
    put(s.LAYOUT_PATH, '{"schema":1,"e2e_shards":2,"e2e_shards":6,"rest_modules":{},"new_test_seconds":30}')
    rejected(lambda: s.layout_config(type('Args', (), {'layout': s.LAYOUT_PATH})(), root), 'duplicate layout keys')
    for value in (True, float('inf'), float('nan'), -1):
        rejected(lambda: s.balance([{'module': 'core', 'kind': 'unit', 'class': 'sample.CoreTest'}], 1,
                                  {'sample.CoreTest': value}), 'invalid duration scalar')
    workflow = (HERE.parent / 'workflows/ci.yml').read_text()
    title = '      - name: Derive every test from the current reactor\n'
    check(workflow.count(title) == 1, 'the exercised workflow producer step is unique')
    step = workflow.split(title, 1)[1].split('\n      - ', 1)[0]
    block = step.split('        run: |\n', 1)[1]
    command = '\n'.join(line[10:] for line in block.splitlines() if line.strip())
    producer = put('.github/scripts/ci-shard.sh', '#!/usr/bin/env bash\nset -e\n'
        'if [[ "$1" == "matrix" ]]; then printf observed > "$RUNNER_TEMP/matrix-called"; exit 7; fi\n'
        '[[ "$1" == "plan" ]]\n')
    producer.chmod(0o755)
    output = root / 'workflow-output'
    result = subprocess.run(['bash', '--noprofile', '--norc', '-e', '-o', 'pipefail', '-c', command], cwd=root,
        env={**os.environ, 'RUNNER_TEMP': str(root), 'GITHUB_OUTPUT': str(output)}, capture_output=True, text=True)
    check((root / 'matrix-called').is_file(), 'the actual workflow block executed the failing producer')
    check(result.returncode != 0 and not output.exists(), 'workflow must stop before publishing a failed matrix')
    expected = '{"include":[{"shard":"sample","prepare_cloud_inputs":false}]}'
    producer.write_text('#!/usr/bin/env bash\nset -e\n'
        'if [[ "$1" == "matrix" ]]; then printf \'%s\\n\' \'' + expected + '\'; exit 0; fi\n'
        '[[ "$1" == "plan" ]]\n')
    output.write_text('prior=unchanged\n')
    result = subprocess.run(['bash', '--noprofile', '--norc', '-e', '-o', 'pipefail', '-c', command], cwd=root,
        env={**os.environ, 'RUNNER_TEMP': str(root), 'GITHUB_OUTPUT': str(output)}, capture_output=True, text=True)
    check(result.returncode == 0 and output.read_text() == 'prior=unchanged\nmatrix=' + expected + '\n',
          'successful workflow publication preserves existing outputs and the exact matrix JSON')

    cloud = put('e2e/src/test/java/sample/CloudFixtureIT.java', 'package sample; class CloudFixtureIT {}')
    independent[('e2e', 'it', 'sample.CloudFixtureIT')] += 1
    unmarked = plan({'schema': 1, 'e2e_shards': 2, 'rest_modules': {}, 'new_test_seconds': 30})
    cloud.write_text('package sample; @RequiresCloudReleaseInputs class CloudFixtureIT {}')
    check(s.cohort(s.discover(root)) == unmarked['cohort_hash'],
          'adding the input marker changes metadata without changing the source class cohort')
    stale_marker = matrix(unmarked)
    check(stale_marker.returncode != 0 and 'source Cloud input metadata differs' in stale_marker.stderr,
          'matrix rejects changed source input metadata even when class identities remain equal')

    # A source-marked IT can be the only residual class after module grouping.
    selector = root / 'e2e/src/test/java/sample/SelectorTest.java'
    selector_source = selector.read_bytes()
    selector.unlink()
    independent.pop(('e2e', 'unit', 'sample.SelectorTest'))
    try:
        cloud_only = plan({'schema': 1, 'e2e_shards': 2,
                           'rest_modules': {'ordinary': ['app', 'core', 'arch-tests']},
                           'new_test_seconds': 30})
        residual = s.selected(cloud_only, 'rest')['tests']
        check(len(residual) == 1 and residual[0]['class'] == 'sample.CloudFixtureIT'
              and residual[0]['requires_cloud_release_inputs'] is True,
              'the source-marked Cloud class remains selected exactly once in rest')
        cloud_matrix = matrix()
        check(cloud_matrix.returncode == 0, 'Cloud-only residual matrix succeeds: ' + cloud_matrix.stderr)
        check(next(row for row in json.loads(cloud_matrix.stdout)['include'] if row['shard'] == 'rest')
              ['prepare_cloud_inputs'], 'Cloud-only residual worker stages its source-declared locked inputs')
    finally:
        cloud.unlink()
        selector.write_bytes(selector_source)
        independent.pop(('e2e', 'it', 'sample.CloudFixtureIT'))
        independent[('e2e', 'unit', 'sample.SelectorTest')] += 1

print('CI source layout controls passed:', count)
