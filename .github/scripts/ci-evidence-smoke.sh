#!/usr/bin/env bash
# Independent method/spec inventories must discriminate successful-looking evidence loss.
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
export EVIDENCE_TOOL="$here/ci-evidence.sh" EVIDENCE_MODULE="$here/_ci-evidence.py"
python3 "$here/_ci-evidence-regression-smoke.py"
python3 - <<'PY'
import copy, hashlib, importlib.util, json, os, pathlib, shutil, struct, subprocess, sys, tempfile, tarfile, zipfile
import xml.etree.ElementTree as ET
sys.dont_write_bytecode = True
spec = importlib.util.spec_from_file_location('evidence', os.environ['EVIDENCE_MODULE'])
evidence = importlib.util.module_from_spec(spec); spec.loader.exec_module(evidence)
checks = 0
with tempfile.TemporaryDirectory() as temporary:
    root = pathlib.Path(temporary)
    source = root/'source'; reports = root/'fixture-artifact/files'
    def put(base, path, value):
        target = base/path; target.parent.mkdir(parents=True, exist_ok=True)
        if isinstance(value, bytes): target.write_bytes(value)
        else: target.write_text(value)
        return target
    fixture_ledger_modes={}
    def fixture_selector(report_root, inventory_file):
        inv=json.loads(pathlib.Path(inventory_file).read_text());policies=[m['published_policy'] for m in inv['methods'] if m.get('published_policy')]
        examples=sorted({identity.rsplit(' on ',1)[0] for policy in policies for identity in policy['cases']})
        tests=[dict(module='e2e',kind='it',**{'class':'sample.PublishedExamplesIT'},example=example) for example in examples]
        if fixture_ledger_modes.get(str(report_root))=='default':tests=[dict(module='e2e',kind='it',**{'class':'sample.PublishedExamplesIT'})]
        inputs=put(report_root.parent,'build-inputs.json',json.dumps(dict(schema=1,phase='pre-test-after-install',source_sha=inv['source_sha'],run_id='123',run_attempt='1',source_files=inv['sources'])))
        phase=dict(status='success',source_sha=inv['source_sha'],cohort_hash='f'*64,shard_id='fixture',run_id='123',run_attempt=1)
        provenance=dict(schema=1,status='success',source_sha=inv['source_sha'],cohort_hash='f'*64,shard_id='fixture',run_id='123',run_attempt=1,case_observer=dict(build_inputs_sha256=hashlib.sha256(inputs.read_bytes()).hexdigest()),reset=phase|dict(phase='reset-test-evidence'),verify=phase|dict(phase='selected-verify'))
        marker='target/ci-shards/run-fixture.json';put(report_root,marker,json.dumps(provenance))
        manifest=put(report_root.parent,'manifest.json',json.dumps(dict(schema=2,source_sha=inv['source_sha'],cohort_hash='f'*64,shard_id='fixture',tests=tests,provenance=provenance,provenance_path=marker,files={path.relative_to(report_root).as_posix():hashlib.sha256(path.read_bytes()).hexdigest() for path in report_root.rglob('*') if path.is_file()})))
        return ['--artifact-manifest',str(manifest),'--inputs',str(inputs)]
    def run(command, expected=True, reason=None):
        global checks
        command=list(command)
        if command[0]=='reports' and '--artifact-manifest' not in command and '--shard-plan' not in command:
            fixture_inv=json.loads(pathlib.Path(command[command.index('--inventory')+1]).read_text())
            if any(method.get('published_policy') for method in fixture_inv['methods']):command+=fixture_selector(pathlib.Path(command[command.index('--root')+1]),command[command.index('--inventory')+1])
        result = subprocess.run([os.environ['EVIDENCE_TOOL'], *command], capture_output=True, text=True)
        assert (result.returncode == 0) == expected, result.stdout + result.stderr
        if reason is not None: assert reason in result.stderr, result.stderr
        checks += 1
        if expected and command[0]=='inventory':
            fixture_output=pathlib.Path(command[command.index('--output')+1]);fixture=json.loads(fixture_output.read_text())
            if fixture.get('source_sha') is None:fixture['source_sha']='a'*40;fixture_output.write_text(json.dumps(fixture))
    def save(name, value): return put(root, name, json.dumps(value))
    put(source,'pom.xml','<project><modules><module>core</module><module>e2e</module></modules></project>')
    for module in ('core','e2e'):
        put(source,module+'/pom.xml','<project><build><plugins><plugin><artifactId>maven-failsafe-plugin</artifactId><executions><execution><goals><goal>integration-test</goal></goals></execution></executions></plugin></plugins></build></project>')
    put(source,'core/src/test/java/sample/CoreTest.java','''package sample;
class CoreTest extends Base {
  @Test void one() { String fixture = "@Test void nonexistent() {}"; }
  // @Test void commentIsNotACase() {}
  @Test void two() {}
  @Test void injected(@TempDir Path directory) {}
  @ParameterizedTest @NullAndEmptySource @ValueSource(strings = {"first", "second"}) void values(String value) {}
  @Nested class Inner { @Test void nested() {} }
}''')
    put(source,'core/src/test/java/sample/Base.java','package sample; abstract class Base { @Test void inherited() {} }')
    put(source,'e2e/src/test/java/sample/PublishedExamplesIT.java','''package sample;
class PublishedExamplesIT {
  void ledger(Path specification, Tiers tier) { WitnessLedger.record(specification, tier); }
  @ParameterizedTest @MethodSource("everyPublishedExampleOnEveryTier")
  void thePublishedExampleRuns(Path specification, Tiers tier) {}
}''')
    original_helper_root=pathlib.Path(os.environ['EVIDENCE_MODULE']).parents[2]/'e2e/src/test/java/io/tapstate/e2e'
    for filename in ('WitnessLedger.java','PublishedExampleSelection.java'):
        put(source,'e2e/src/test/java/sample/'+filename,(original_helper_root/filename).read_text().replace('package io.tapstate.e2e;','package sample;'))
    put(source,'e2e/src/test/java/sample/E2eConnectorJar.java','package sample; class E2eConnectorJar { static final String CONNECTOR_ID = "e2e_file"; }')
    for example in ('alpha','beta'): put(source,'e2e/examples/'+example+'/case.e2e.yml','setup:\n  connectors: [e2e_file]\n')
    inventory = root/'inventory.json'
    run(['inventory','--root',str(source),'--lane','ci','--output',str(inventory)])
    inv = json.loads(inventory.read_text()); assert inv['complete'] and len(inv['cases']) == 13
    assert {m['method'] for m in inv['methods']} == {'one','two','values','nested','thePublishedExampleRuns','inherited','injected'}
    def suite(classname, cases, module, kind='unit'):
        element = ET.Element('testsuite', name=classname, tests=str(len(cases)), failures='0', errors='0', skipped=str(sum(c.find('skipped') is not None for c in cases)))
        element.extend(cases)
        return put(reports,module+'/target/'+('surefire' if kind=='unit' else 'failsafe')+'-reports/TEST-'+classname+'.xml',ET.tostring(element,encoding='unicode'))
    def case(classname, name, marker=None, skipped=None):
        element = ET.Element('testcase',classname=classname,name=name)
        if marker: ET.SubElement(element,'system-out').text='tapstate.published-case='+marker+'\n'
        if skipped: ET.SubElement(element,'skipped',type='fixture',message=skipped)
        return element
    value_method = next(m for m in inv['methods'] if m['method']=='values')
    value_cases = [case('sample.CoreTest','values(String)['+str(n+1)+']') for n in range(len(value_method['parameters']))]
    for n,element in enumerate(value_cases):
        marker=dict(schema=1,phase='start',unique_id='value-'+str(n),stable=True,**{'class':'sample.CoreTest'},method='values',argument_digest=value_method['argument_digests'][n])
        ET.SubElement(element,'system-out').text='tapstate.ci-case='+json.dumps(marker)+'\n'+'tapstate.ci-case='+json.dumps(marker|dict(phase='finish',status='executed'))+'\n'
    core = suite('sample.CoreTest',[case('sample.CoreTest','one',skipped='original expected skip'),case('sample.CoreTest','two'),*value_cases,case('sample.CoreTest','inherited'),case('sample.CoreTest','injected(Path)')],'core')
    nested = suite('sample.CoreTest$Inner',[case('sample.CoreTest$Inner','nested')],'core')
    expected = [c['parameter'] for c in inv['cases'] if c['class']=='sample.PublishedExamplesIT']
    published = suite('sample.PublishedExamplesIT',[case('sample.PublishedExamplesIT','thePublishedExampleRuns(Path, Tiers)['+str(n+1)+']',identity) for n,identity in enumerate(expected)],'e2e','it')
    ledger = put(reports,'e2e/target/witness-ledgers/123.txt','\n'.join(expected)+'\n')
    before = root/'before.json'; after = root/'after.json'; comparison = root/'comparison.json'
    def observe(destination): run(['reports','--root',str(reports),'--lane','ci','--inventory',str(inventory),'--output',str(destination)])
    def compare(success=True, reason=None):
        observe(after)
        run(['compare','--inventory',str(inventory),'--before',str(before),'--after',str(after),'--output',str(comparison)],success,reason)
    observe(before); compare()
    assert len(value_method['parameters'])==4 and len(set(value_method['argument_digests']))==4
    null_digest=evidence.argument_digest([['null']]);empty_digest=evidence.argument_digest([['java.lang.String','']])
    assert null_digest in value_method['argument_digests'] and empty_digest in value_method['argument_digests']
    def csv_controls():
        global checks
        # Quoted empty strings are values; unquoted empty cells remain null. These
        # expected arguments are independent of the source parser and marker producer.
        csv_source=root/'csv-source';csv_reports=root/'csv-reports'
        put(csv_source,'pom.xml','<project><modules><module>core</module></modules></project>')
        put(csv_source,'core/pom.xml',(source/'core/pom.xml').read_text())
        csv_file=put(csv_source,'core/src/test/java/sample/CsvTest.java',"""package sample;
    class CsvTest {
      @ParameterizedTest @CsvSource(value = {
        "'', metadata, metadata", "'', '', admin", ", metadata, metadata",
        "'  padded  ', value, other", "'a,b','it''s',x", "'NULL', normal, tail"
      }, nullValues = "NULL")
      void csv(String first, String second, String third) {}
    }""")
        csv_inventory=root/'csv-inventory.json'
        run(['inventory','--root',str(csv_source),'--lane','ci','--output',str(csv_inventory)])
        csv_inv=json.loads(csv_inventory.read_text());csv_method=csv_inv['methods'][0]
        golden=[['','metadata','metadata'],['','','admin'],[None,'metadata','metadata'],
                ['  padded  ','value','other'],['a,b',"it's",'x'],[None,'normal','tail']]
        golden_digests=[evidence.argument_digest([evidence.canonical_literal(value,'String') for value in row]) for row in golden]
        assert csv_inv['complete'] and csv_method['argument_digests']==golden_digests
        assert len(set(golden_digests))==6
        csv_cases=[]
        for number,arg in enumerate(golden_digests,1):
            element=ET.Element('testcase',classname='sample.CsvTest',name=f'csv(String, String, String)[{number}]')
            marker=dict(schema=1,phase='start',unique_id='csv-'+str(number),stable=True,**{'class':'sample.CsvTest'},method='csv',argument_digest=arg)
            ET.SubElement(element,'system-out').text='tapstate.ci-case='+json.dumps(marker)+'\n'+'tapstate.ci-case='+json.dumps(marker|dict(phase='finish',status='executed'))+'\n'
            csv_cases.append(element)
        csv_suite=ET.Element('testsuite',name='sample.CsvTest',tests='6',failures='0',errors='0',skipped='0');csv_suite.extend(csv_cases)
        csv_xml=put(csv_reports,'core/target/surefire-reports/TEST-sample.CsvTest.xml',ET.tostring(csv_suite,encoding='unicode'))
        csv_observation=root/'csv-observation.json'
        csv_command=['reports','--root',str(csv_reports),'--lane','ci','--inventory',str(csv_inventory),'--output',str(csv_observation)]
        run(csv_command);assert json.loads(csv_observation.read_text())['complete']
        run(['export-hashes','--input',str(csv_observation),'--output',str(root/'csv-hashes.json')])
        pristine_csv=csv_xml.read_text();csv_before=save('csv-before.json',json.loads(csv_observation.read_text()))
        csv_xml.write_text(pristine_csv.replace(golden_digests[0],golden_digests[2]))
        run(csv_command)
        run(['compare','--inventory',str(csv_inventory),'--before',str(csv_before),'--after',str(csv_observation),
             '--output',str(root/'csv-comparison.json')],False,'case multiset differs')
        csv_xml.write_text(pristine_csv.replace(golden_digests[0],'0'*64))
        run(csv_command);assert not json.loads(csv_observation.read_text())['complete']
        run(['export-hashes','--input',str(csv_observation),'--output',str(root/'bad-csv-hashes.json')],False,'unresolved case')
        for record in ["'unterminated", "'value'garbage,other", '']:
            try:evidence.csv_record(record,',')
            except ValueError:checks+=1
            else:raise AssertionError('accepted malformed/blank CSV record')
        assert evidence.csv_record("''|  bare  |' a|b '",'|')==['','bare',' a|b ']
        try:evidence.csv_record('one two',' ')
        except ValueError:checks+=1
        else:raise AssertionError('accepted unsupported whitespace delimiter')
        original_csv_source=csv_file.read_text()
        csv_file.write_text('package sample; class CsvTest { @ParameterizedTest @CsvSource("'
            +chr(92)+'u005ctext,x") void csv(String first, String second) {} }')
        target=root/'unicode-literal-csv.json'
        run(['inventory','--root',str(csv_source),'--lane','ci','--output',str(target)])
        assert not json.loads(target.read_text())['complete']
        csv_file.write_text(original_csv_source)
        csv_file.write_text(original_csv_source.replace('class CsvTest {','class CsvTest { static final String NIL = "NULL";')
                            .replace('nullValues = "NULL"','nullValues = NIL'))
        constant_csv=root/'constant-null-csv.json'
        run(['inventory','--root',str(csv_source),'--lane','ci','--output',str(constant_csv)])
        assert not json.loads(constant_csv.read_text())['complete']
        csv_file.write_text(original_csv_source.replace('class CsvTest {',
            'class CsvTest { static final String NIL = "NULL"; static class Inner { static final String NIL = "OTHER"; }')
            .replace('nullValues = "NULL"','nullValues = NIL'))
        run(['inventory','--root',str(csv_source),'--lane','ci','--output',str(root/'ambiguous-null-csv.json')])
        assert not json.loads((root/'ambiguous-null-csv.json').read_text())['complete']
        for impostor in ['// NIL = "OTHER";\n',
                         'static final String NOTE = "NIL = \\"OTHER\\";";\n',
                         'static final String NOTE = """\nNIL = "OTHER";\n""";\n']:
            csv_file.write_text(original_csv_source.replace('class CsvTest {',
                'class CsvTest {\n'+impostor+'static final String NIL = "NULL";')
                .replace('nullValues = "NULL"','nullValues = NIL'))
            target=root/'impostor-null-csv.json'
            run(['inventory','--root',str(csv_source),'--lane','ci','--output',str(target)])
            value=json.loads(target.read_text())
            assert not value['complete']
        put(csv_source,'core/src/test/java/sample/Constants.java','package sample; class Constants { static final String NIL = "NULL"; }')
        csv_file.write_text('package sample; import static sample.Constants.NIL; class CsvTest { '
            '@ParameterizedTest @CsvSource(value={"NULL,x"}, nullValues=NIL) void csv(String first, String second) {} '
            'void unrelated(){ String NIL="OTHER"; } }')
        target=root/'unrelated-local-csv.json'
        run(['inventory','--root',str(csv_source),'--lane','ci','--output',str(target)])
        assert not json.loads(target.read_text())['complete']
        csv_file.write_text('package sample; class CsvTest { static final String ROW="left,right"; '
            '@ParameterizedTest @CsvSource(ROW) void csv(String first, String second) {} }')
        target=root/'constant-row-csv.json'
        run(['inventory','--root',str(csv_source),'--lane','ci','--output',str(target)])
        assert not json.loads(target.read_text())['complete']
        escaped_note='"""\n'+chr(92)+'"""\nNIL = "OTHER";\n"""'
        csv_file.write_text('package sample; import static sample.Constants.NIL; class CsvTest { '
            '@ParameterizedTest @CsvSource(value={"NULL,x"}, nullValues=NIL) void csv(String first, String second) {} '
            'static final String NOTE = '+escaped_note+'; }')
        target=root/'escaped-definition-csv.json'
        run(['inventory','--root',str(csv_source),'--lane','ci','--output',str(target)])
        value=json.loads(target.read_text())
        assert not value['complete'] and any('CSV constant expressions' in gap.get('reason','') for gap in value['unresolved'])
        csv_file.write_text(original_csv_source.replace('class CsvTest {',
            'class CsvTest {\n// unicode escape '+chr(92)+'u0020\nstatic final String NIL="NULL";')
            .replace('nullValues = "NULL"','nullValues = NIL'))
        target=root/'unicode-preprocessing-csv.json'
        run(['inventory','--root',str(csv_source),'--lane','ci','--output',str(target)])
        assert not json.loads(target.read_text())['complete']
        csv_file.write_text(original_csv_source.replace('nullValues = "NULL"','nullValues = Other.NIL'))
        run(['inventory','--root',str(csv_source),'--lane','ci','--output',str(root/'unresolved-null-csv.json')])
        assert not json.loads((root/'unresolved-null-csv.json').read_text())['complete']
        csv_file.write_text(original_csv_source.replace('nullValues = "NULL"','emptyValue = "replacement"'))
        run(['inventory','--root',str(csv_source),'--lane','ci','--output',str(root/'unsupported-csv.json')])
        assert not json.loads((root/'unsupported-csv.json').read_text())['complete']
        for number,row in enumerate(["NIL, nullValues='NIL'", "left, delimiter='|'"]):
            csv_file.write_text('package sample; class CsvTest { @ParameterizedTest @CsvSource('+json.dumps(row)+') void csv(String first, String second) {} }')
            target=root/f'literal-csv-option-{number}.json'
            run(['inventory','--root',str(csv_source),'--lane','ci','--output',str(target)])
            value=json.loads(target.read_text())
            expected=row.split(', ',1)
            assert value['complete'] and value['methods'][0]['argument_digests']==[
                evidence.argument_digest([['java.lang.String',item] for item in expected])]
        block_source=root/'csv-block-source'
        put(block_source,'pom.xml','<project><modules><module>core</module></modules></project>')
        put(block_source,'core/pom.xml',(source/'core/pom.xml').read_text())
        for number,text in enumerate(['"""\na,b\n #data,c\n"""','"""\n    a,b\n    #comment,ignored\n     #data,c\n    """']):
            put(block_source,'core/src/test/java/sample/BlockTest.java',
                'package sample; class BlockTest { @ParameterizedTest @CsvSource(textBlock = '+text+') void values(String first, String second) {} }')
            target=root/f'csv-block-{number}.json'
            run(['inventory','--root',str(block_source),'--lane','ci','--output',str(target)])
            block_inv=json.loads(target.read_text())
            assert block_inv['complete'] and block_inv['methods'][0]['argument_digests']==[
                evidence.argument_digest([['java.lang.String','a'],['java.lang.String','b']]),
                evidence.argument_digest([['java.lang.String','#data'],['java.lang.String','c']])]
        escaped_block='"""\na,b\\s\n"""'
        put(block_source,'core/src/test/java/sample/BlockTest.java',
            'package sample; class BlockTest { @ParameterizedTest @CsvSource(textBlock = '+escaped_block+') void values(String first, String second) {} }')
        run(['inventory','--root',str(block_source),'--lane','ci','--output',str(root/'escaped-csv-block.json')])
        assert not json.loads((root/'escaped-csv-block.json').read_text())['complete']
    csv_controls()
    omitted=json.loads(inventory.read_text())
    omitted_method=next(method for method in omitted['methods'] if method['method']=='values')
    omitted_method['parameters']=omitted_method['parameters'][:2];omitted_method['argument_digests']=omitted_method['argument_digests'][:2]
    omitted['cases']=[record for record in omitted['cases'] if record['method']!='values' or record['parameter'] in omitted_method['parameters']]
    omitted_inventory=save('omitted-null-empty.json',omitted)
    omitted_observation=root/'omitted-observation.json'
    run(['reports','--root',str(reports),'--lane','ci','--inventory',str(omitted_inventory),'--output',str(omitted_observation)])
    assert not json.loads(omitted_observation.read_text())['complete']
    run(['compare','--inventory',str(omitted_inventory),'--before',str(omitted_observation),'--after',str(omitted_observation),'--output',str(comparison)],False,'unresolved identities')
    baseline_reports = root/'pristine'; shutil.copytree(reports,baseline_reports)
    def reset(): shutil.rmtree(reports); shutil.copytree(baseline_reports,reports)
    def edit(path, action):
        element = ET.parse(path).getroot(); action(element)
        element.set('tests',str(len(element.findall('testcase')))); element.set('skipped',str(sum(c.find('skipped') is not None for c in element.findall('testcase'))))
        path.write_text(ET.tostring(element,encoding='unicode'))
    # An equally-sized report replacing one original identity must not pass.
    edit(core,lambda s:s.findall('testcase')[1].set('name','invented')); compare(False,'unresolved identities'); reset()
    edit(core,lambda s:(s.findall('testcase')[3].set('name','values(String)[1]'),s.findall('testcase')[3].find('system-out').__setattr__('text',s.findall('testcase')[2].find('system-out').text))); compare(False,'case multiset differs'); reset()
    edit(core,lambda s:s.findall('testcase')[2].find('system-out').__setattr__('text',s.findall('testcase')[2].find('system-out').text.replace(value_method['argument_digests'][0],'0'*64)))
    compare(False,'unresolved identities'); reset()
    # A finish receipt cannot borrow a valid start while changing its identity/outcome.
    for field,value in [('schema',True),('stable',False),('method','invented'),('argument_digest','0'*64),('status','aborted')]:
        def corrupt_finish(element):
            stream=element.findall('testcase')[2].find('system-out'); lines=stream.text.splitlines(); finish=json.loads(lines[1].split('=',1)[1]); finish[field]=value; lines[1]='tapstate.ci-case='+json.dumps(finish); stream.text='\n'.join(lines)+'\n'
        edit(core,corrupt_finish); compare(False,'unresolved identities'); reset()
    put(reports,'core/target/surefire-reports/TEST-sample.ForeignTest.xml','<testsuite name="sample.ForeignTest" tests="0" failures="0" errors="0" skipped="0"/>')
    run(['reports','--root',str(reports),'--lane','ci','--inventory',str(inventory),'--output',str(after)],False,'outside independent source'); reset()
    # The report and ledger agree on the same omitted tier; the independent source refuses it.
    edit(published,lambda s:s.remove(s.findall('testcase')[-1])); ledger.write_text('\n'.join(expected[:-1])+'\n'); compare(False,'case multiset differs'); reset()
    ledger.write_text(ledger.read_text()+expected[0]+'\n'); compare(False,'ledger multiset differs'); reset()
    edit(core,lambda s:[s.remove(c) for c in s.findall('testcase')]); compare(False,'zero cases'); reset()
    edit(core,lambda s:s.findall('testcase')[0].find('skipped').set('message','rewritten skip')); compare(False,'skip status or reason changed'); reset()
    put(reports,'copy/core/target/surefire-reports/TEST-sample.CoreTest.xml',core.read_text()); compare(False,'duplicate report suites'); reset()
    edit(published,lambda s:s.findall('testcase')[0].find('system-out').__setattr__('text','tapstate.published-case='+expected[-1]+'\n')); compare(False,'case multiset differs'); reset()
    # Several shards may own disjoint spec/tier cases of the same published class.
    whole = ET.parse(published).getroot(); children = list(whole.findall('testcase'))
    suite('sample.PublishedExamplesIT',children[:2],'e2e','it')
    split = ET.Element('testsuite',name='sample.PublishedExamplesIT',tests='2',failures='0',errors='0',skipped='0'); split.extend(children[2:])
    put(reports,'split/e2e/target/failsafe-reports/TEST-sample.PublishedExamplesIT.xml',ET.tostring(split,encoding='unicode'))
    compare(); reset()
    # A zero outer suite is only a wrapper when every independent family identity
    # is actually present once in the nested reports. Missing an inner or outer
    # case cannot borrow the wrapper exemption.
    outer_tree=ET.parse(core).getroot(); nested_tree=ET.parse(nested).getroot()
    outer_cases=list(outer_tree.findall('testcase'))
    nested_tree.extend(outer_cases);nested_tree.set('tests',str(len(nested_tree.findall('testcase'))));nested_tree.set('skipped',str(sum(c.find('skipped') is not None for c in nested_tree.findall('testcase'))))
    nested.write_text(ET.tostring(nested_tree,encoding='unicode'))
    for element in outer_cases:outer_tree.remove(element)
    outer_tree.set('tests','0');outer_tree.set('skipped','0');core.write_text(ET.tostring(outer_tree,encoding='unicode'))
    compare()
    edit(nested,lambda element:element.remove(element.findall('testcase')[-1]));compare(False,'unresolved identities');reset()
    # A real class-level abort needs original source predicates, compiled bytes,
    # and fresh paired callback receipts. It contributes N/A, never XML execution.
    gate_source=root/'gate-source';gate_reports=root/'gate-reports';gate_dir=root/'class-gates'
    put(gate_source,'pom.xml','<project><modules><module>core</module></modules></project>')
    put(gate_source,'core/pom.xml',(source/'core/pom.xml').read_text())
    gate_reason='no -Dtapstate.e2e.connectors-dir: skipping a witness that needs real connector jars'
    put(gate_source,'core/src/test/java/sample/GateTest.java','package sample; class GateTest { @BeforeAll static void prepare() { RealConnectorGate.require("mysql"); } @Test void first() {} @Test void second() {} }')
    put(gate_source,'core/src/test/java/sample/RealConnectorGate.java','package sample; class RealConnectorGate { static void require(String id) { Assumptions.abort("'+gate_reason+'"); } }')
    put(gate_source,'core/src/test/java/sample/ConnectorJars.java','package sample; class ConnectorJars { boolean named() { return configured != null && !configured.isBlank(); } }')
    gate_inventory=root/'gate-inventory.json';run(['inventory','--root',str(gate_source),'--lane','ci','--output',str(gate_inventory)])
    gate_inv=json.loads(gate_inventory.read_text());gate_inv['source_sha']='a'*40;gate_inventory.write_text(json.dumps(gate_inv))
    gate_input=save('gate-inputs.json',dict(schema=1,phase='pre-test-after-install',source_sha='a'*40,run_id=123,run_attempt=1,source_files=gate_inv['sources']|{'test-support/src/test/java/io/tapstate/testsupport/CiCaseEvidence.java':'c'*64},test_files={'core/target/test-classes/sample/GateTest.class':'b'*64,'test-support/target/test-classes/io/tapstate/testsupport/CiCaseEvidence.class':'d'*64}))
    put(gate_reports,'core/target/surefire-reports/TEST-sample.GateTest.xml','<testsuite name="sample.GateTest" tests="0" failures="0" errors="0" skipped="0"/>')
    absent=dict(present=False,nonblank=False,boolean_true=False)
    gate_record=dict(schema=1,stable=True,**{'class':'sample.GateTest'},declaring_class='sample.GateTest',method='prepare',unique_id='class-gate',source_sha='a'*40,run_id='123',run_attempt='1',gate_inputs=dict(properties={'tapstate.e2e.connectors-dir':absent},environment={}))
    gate_ledger=put(gate_dir,'before-all-123.jsonl',json.dumps(gate_record|dict(phase='start'))+'\n'+json.dumps(gate_record|dict(phase='finish',status='aborted',reason_digest=evidence.digest(json.dumps(gate_reason).encode())))+'\n')
    gate_observation=root/'gate-observation.json'
    gate_command=['reports','--root',str(gate_reports),'--lane','ci','--inventory',str(gate_inventory),'--class-gates',str(gate_dir),'--inputs',str(gate_input),'--output',str(gate_observation)]
    run(gate_command);gate_baseline=json.loads(gate_observation.read_text())
    assert gate_baseline['complete'] and gate_baseline['actual_xml_case_count']==0 and len(gate_baseline['cases'])==0 and gate_baseline['source_not_applicable_count']==2 and len(gate_baseline['class_gate_files'])==1
    gate_before=save('gate-before.json',gate_baseline)
    gate_compare=['compare','--inventory',str(gate_inventory),'--before',str(gate_before),'--after',str(gate_observation),'--output',str(root/'gate-comparison.json')]
    run(gate_compare)
    # Identical ordinary-PR N/A cannot authorize changing the original helper's
    # available/nightly path. Freeze its full bytes independently of the abort.
    for filename in ('RealConnectorGate.java','ConnectorJars.java'):
        revised=copy.deepcopy(gate_inv);helper='core/src/test/java/sample/'+filename
        revised['sources'][helper]=hashlib.sha256((gate_source/helper).read_bytes()+b'\n// changed available path\n').hexdigest()
        changed_inventory=save('changed-gate-helper-inventory.json',revised)
        unchanged_na=copy.deepcopy(gate_baseline);unchanged_na['source_inventory_sha256']=hashlib.sha256(changed_inventory.read_bytes()).hexdigest()
        changed_observation=save('changed-gate-helper-observation.json',unchanged_na)
        run(['compare','--inventory',str(gate_inventory),'--candidate-inventory',str(changed_inventory),'--before',str(gate_before),'--after',str(changed_observation),'--output',str(root/'changed-gate-helper-comparison.json')],False,'original class gate helper source hash changed')
    original_ledger=gate_ledger.read_text()
    for change,reason in [(('source_sha','e'*40),'old or foreign'),(('run_attempt','2'),'old or foreign')]:
        key,value=change;rows=[json.loads(line) for line in original_ledger.splitlines()]
        for row in rows:row[key]=value
        gate_ledger.write_text('\n'.join(json.dumps(row) for row in rows)+'\n');run(gate_command,False,reason)
    gate_ledger.write_text(original_ledger+original_ledger);run(gate_command,False,'duplicate class gate phase')
    gate_ledger.write_text(original_ledger.splitlines()[0]+'\n');run(gate_command,False,'missing or duplicate')
    rows=[json.loads(line) for line in original_ledger.splitlines()];rows[1]['reason_digest']='0'*64
    gate_ledger.write_text('\n'.join(json.dumps(row) for row in rows)+'\n');run(gate_command);assert not json.loads(gate_observation.read_text())['complete'];run(gate_compare,False,'unresolved identities')
    rows=[json.loads(line) for line in original_ledger.splitlines()]
    for row in rows:row['gate_inputs']['properties']['tapstate.e2e.connectors-dir']=dict(present=True,nonblank=True,boolean_true=False)
    gate_ledger.write_text('\n'.join(json.dumps(row) for row in rows)+'\n');run(gate_command);assert not json.loads(gate_observation.read_text())['complete'];run(gate_compare,False,'unresolved identities')
    gate_ledger.write_text(original_ledger);run(gate_command)
    forged_gate=json.loads(gate_observation.read_text());forged_gate['source_not_applicable'][0]['evidence']['compiled_class_sha256']='invalid';gate_observation.write_text(json.dumps(forged_gate));run(gate_compare,False,'bytecode/reason binding')
    gate_observation.write_text(json.dumps(gate_baseline));public_gate=root/'gate-hashes.json';run(['export-hashes','--input',str(gate_observation),'--output',str(public_gate)])
    exported=json.loads(public_gate.read_text());assert exported['actual_xml_case_count']==0 and exported['source_not_applicable_count']==2 and len(exported['cases'])==2 and gate_reason not in public_gate.read_text() and str(gate_dir) not in public_gate.read_text()
    # New regression controls are explicit additions, never replacements for originals.
    core_source=source/'core/src/test/java/sample/CoreTest.java'; old_source=core_source.read_text()
    core_source.write_text(old_source.replace('@Test void two() {}','@Test void two() {} @Test void newControl() {}'))
    candidate_inventory=root/'candidate-inventory.json'
    run(['inventory','--root',str(source),'--lane','ci','--output',str(candidate_inventory)])
    candidate=json.loads(candidate_inventory.read_text())
    additional=[record for record in candidate['cases'] if record['method']=='newControl']
    additions=save('additions.json',additional)
    edit(core,lambda element:element.append(case('sample.CoreTest','newControl')))
    run(['reports','--root',str(reports),'--lane','ci','--inventory',str(candidate_inventory),'--output',str(after)])
    candidate_compare=['compare','--inventory',str(inventory),'--candidate-inventory',str(candidate_inventory),'--before',str(before),'--after',str(after),'--output',str(comparison)]
    run(candidate_compare+['--additions',str(additions)])
    assert len(json.loads(comparison.read_text())['additional_cases'])==1
    run(candidate_compare,False,'declared new controls')
    forged=save('forged-additions.json',[inv['cases'][0]])
    run(candidate_compare+['--additions',str(forged)],False,'overlap original')
    core_source.write_text(old_source); reset()
    # Public hash receipts reveal no literal arguments, skip plaintext or private paths.
    private_inventory=json.loads(inventory.read_text())
    private_inventory['source_origin']=dict(kind='authenticated-git-archive',source_sha='a'*40,source_archive_sha256='b'*64,private_path='/private/credential-sentinel')
    secret_case=next(record for record in private_inventory['cases'] if record['method']=='values')
    secret_method=next(method for method in private_inventory['methods'] if method['method']=='values')
    position=secret_method['parameters'].index(secret_case['parameter']);secret_method['parameters'][position]='credential-token-sentinel';secret_case['parameter']='credential-token-sentinel'
    private_file=save('private-inventory.json',private_inventory)
    public_inventory=root/'public-hashes.json'
    run(['export-hashes','--input',str(private_file),'--output',str(public_inventory)])
    public_text=public_inventory.read_text();assert 'credential-token-sentinel' not in public_text and 'credential-sentinel' not in public_text and 'annotations' not in public_text
    # Identity hashes alone never certify execution or lawful skips.
    all_skipped=json.loads(before.read_text())
    for record in all_skipped['cases']:
        record['status']='skipped';record['skip']=dict(type='fixture',reason='unexpected skip',raw={})
    all_skipped_file=save('all-skipped.json',all_skipped)
    run(['compare','--inventory',str(inventory),'--before',str(before),'--after',str(all_skipped_file),'--output',str(comparison)],False,'execution or skip differs')
    expected_hashes=root/'expected-hashes.json';actual_hashes=root/'actual-hashes.json'
    run(['export-hashes','--input',str(inventory),'--output',str(expected_hashes)])
    run(['export-hashes','--input',str(all_skipped_file),'--output',str(actual_hashes)])
    hash_result=root/'identity-only.json'
    run(['compare-hashes','--expected',str(expected_hashes),'--actual',str(actual_hashes),'--output',str(hash_result)])
    hash_receipt=json.loads(hash_result.read_text());assert hash_receipt['identity_equal'] and not hash_receipt['complete'] and not hash_receipt['ability_proven'] and hash_receipt['mandatory_comparison']=='compare'
    # Both original serializers are bound to actual selector/provenance. Raw
    # labels remain intact while their independent case identities agree.
    full_labels=list(expected);serializer=next(m['published_policy']['ledger_serializer'] for m in inv['methods'] if m.get('published_policy'))
    fixture_ledger_modes[str(reports)]='default';ledger.write_text('\n'.join(serializer['aliases'][label]['default'] for label in full_labels)+'\n');compare()
    default_observation=json.loads(after.read_text());assert default_observation['ledger']!=default_observation['canonical_ledger'] and default_observation['canonical_ledger']==full_labels
    default_ledger=ledger.read_text();ledger.write_text(default_ledger+full_labels[0]+'\n')
    run(['reports','--root',str(reports),'--lane','ci','--inventory',str(inventory),'--output',str(after)],False,'wrong-mode published ledger alias');ledger.write_text(default_ledger)
    labels=default_ledger.splitlines();labels[-1]=labels[0];ledger.write_text('\n'.join(labels)+'\n');compare(False,'ledger multiset differs');ledger.write_text(default_ledger)
    ledger.write_text('\n'.join(default_ledger.splitlines()[:-1])+'\n');compare(False,'ledger multiset differs');ledger.write_text(default_ledger)
    ledger.write_text(default_ledger+'unknown-slug on IN_PROCESS\n');run(['reports','--root',str(reports),'--lane','ci','--inventory',str(inventory),'--output',str(after)],False,'unknown or wrong-mode');ledger.write_text(default_ledger)
    # A selector receipt cannot claim the other serialization mode.
    forged_mode=copy.deepcopy(default_observation);forged_mode['ledger_selector']['mode']='selected';save('wrong-ledger-mode.json',forged_mode)
    run(['compare','--inventory',str(inventory),'--before',str(before),'--after',str(root/'wrong-ledger-mode.json'),'--output',str(comparison)],False,'serializer mode differs from actual selector')
    # Even identical labels/outcomes cannot waive a changed helper implementation.
    for filename in ('WitnessLedger.java','PublishedExampleSelection.java'):
        changed=copy.deepcopy(inv);helper='e2e/src/test/java/sample/'+filename;new_hash='e'*64
        changed['sources'][helper]=new_hash
        next(m['published_policy']['ledger_serializer'] for m in changed['methods'] if m.get('published_policy'))['helper_sources'][helper]=new_hash
        changed_inv=save('changed-serializer-inventory.json',changed);changed_obs=copy.deepcopy(default_observation);changed_obs['source_inventory_sha256']=hashlib.sha256(changed_inv.read_bytes()).hexdigest();changed_obs['ledger_selector']['helpers'][helper]=new_hash
        save('changed-serializer-observation.json',changed_obs)
        run(['compare','--inventory',str(inventory),'--candidate-inventory',str(changed_inv),'--before',str(before),'--after',str(root/'changed-serializer-observation.json'),'--output',str(comparison)],False,'published gate source contract changed')
    ambiguous=put(source,'e2e/examples/other/alpha/case.e2e.yml','setup:\n  connectors: [e2e_file]\n')
    run(['inventory','--root',str(source),'--lane','ci','--output',str(root/'ambiguous-aliases.json')]);assert any('ambiguous published ledger aliases' in row['reason'] for row in json.loads((root/'ambiguous-aliases.json').read_text())['unresolved']);ambiguous.unlink()
    fixture_ledger_modes.pop(str(reports));reset()
    # The pre-pack CI route reads the same successful raw verify receipt, rather
    # than accepting a caller-supplied mode or inferring it from ledger text.
    fixture_args=fixture_selector(reports,inventory);fixture_manifest=json.loads(pathlib.Path(fixture_args[1]).read_text());planned=fixture_manifest['tests']
    cohort=hashlib.sha256(json.dumps(sorted((row['module'],row['kind'],row['class'],row['example']) for row in planned)).encode()).hexdigest()
    plan=save('published-shard-plan.json',dict(schema=1,source_sha='a'*40,cohort_hash=cohort,expected=planned,shards=[dict(id='fixture',tests=planned)]))
    receipt=fixture_manifest['provenance'];receipt['cohort_hash']=cohort
    for phase in ['reset','verify']:receipt[phase]['cohort_hash']=cohort
    marker=reports/fixture_manifest['provenance_path'];marker.write_text(json.dumps(receipt))
    prepack=['reports','--root',str(reports),'--lane','ci','--inventory',str(inventory),'--shard-plan',str(plan),'--shard','fixture','--inputs',fixture_args[3],'--output',str(after)]
    run(prepack);assert json.loads(after.read_text())['complete'] and json.loads(after.read_text())['ledger_selector']['mode']=='selected'
    receipt['run_attempt']=2;marker.write_text(json.dumps(receipt));run(prepack,False,'run/attempt differs');reset()
    # Unknown source constructs are observations, never complete equivalence.
    unknown = put(source,'core/src/test/java/sample/FutureTest.java','package sample; class FutureTest { @TestFactory Stream tests() {} }')
    run(['inventory','--root',str(source),'--lane','ci','--output',str(root/'unknown.json')]); assert not json.loads((root/'unknown.json').read_text())['complete']; unknown.unlink()
    # Match the existing ordinary-PR capability mix: every spec still has both XML
    # identities, while only the 18 synthetic specs may vouch in the success ledger.
    policy_source=root/'policy-source'; policy_reports=root/'policy-artifact/files'
    put(policy_source,'pom.xml','<project><modules><module>e2e</module></modules></project>')
    put(policy_source,'e2e/pom.xml',(source/'e2e/pom.xml').read_text())
    put(policy_source,'e2e/src/test/java/sample/E2eConnectorJar.java',(source/'e2e/src/test/java/sample/E2eConnectorJar.java').read_text())
    for filename in ('WitnessLedger.java','PublishedExampleSelection.java'):
        put(policy_source,'e2e/src/test/java/sample/'+filename,(source/'e2e/src/test/java/sample'/filename).read_text())
    put(policy_source,'e2e/src/test/java/sample/RealConnectorGate.java','package sample; class RealConnectorGate { void require() { switch (decision) { case SKIP -> Assumptions.abort("original real connector gate"); } } }')
    put(policy_source,'e2e/src/test/java/sample/PublishedExamplesIT.java',(source/'e2e/src/test/java/sample/PublishedExamplesIT.java').read_text().replace('class PublishedExamplesIT {','class PublishedExamplesIT { void gate() { RealConnectorGate.require(real.toArray(String[]::new)); }'))
    for n in range(53):
        put(policy_source,'e2e/examples/example-'+str(n).zfill(2)+'/case.e2e.yml','setup:\n  connectors: ['+('e2e_file' if n<18 else 'mysql')+']\n')
    policy_inventory=root/'policy-inventory.json'
    run(['inventory','--root',str(policy_source),'--lane','ci','--published-connectors','absent','--output',str(policy_inventory)])
    policy_inv=json.loads(policy_inventory.read_text()); assert policy_inv['complete'] and len(policy_inv['cases'])==106
    policies=next(m['published_policy']['cases'] for m in policy_inv['methods'] if m['class']=='sample.PublishedExamplesIT')
    policy_suite=ET.Element('testsuite',name='sample.PublishedExamplesIT',tests='106',skipped='70',failures='0',errors='0')
    successful=[]
    for n,record in enumerate(policy_inv['cases']):
        label=record['parameter']; state=policies[label]['expected_status']
        policy_suite.append(case('sample.PublishedExamplesIT','thePublishedExampleRuns(Path, Tiers)['+str(n+1)+']',label,'original real connector gate' if state=='skipped' else None))
        if state=='executed':successful.append(label)
    assert len(successful)==36
    # Real Surefire aborts carry no message attribute and retain stack frames.
    for element in policy_suite.findall('testcase'):
        skipped=element.find('skipped')
        if skipped is not None:
            skipped.attrib.clear(); skipped.set('type','org.opentest4j.TestAbortedException')
            skipped.text='org.opentest4j.TestAbortedException: original real connector gate\n\tat sample.Original.frame(Original.java:12)\n'
    policy_report=put(policy_reports,'e2e/target/failsafe-reports/TEST-sample.PublishedExamplesIT.xml',ET.tostring(policy_suite,encoding='unicode'))
    policy_ledger=put(policy_reports,'e2e/target/witness-ledgers/456.txt','\n'.join(successful)+'\n')
    policy_before=root/'policy-before.json'; policy_after=root/'policy-after.json'
    def policy_observe(destination):run(['reports','--root',str(policy_reports),'--lane','ci','--inventory',str(policy_inventory),'--output',str(destination)])
    def policy_compare(ok=True,reason=None):
        policy_observe(policy_after)
        run(['compare','--inventory',str(policy_inventory),'--before',str(policy_before),'--after',str(policy_after),'--output',str(root/'policy-comparison.json')],ok,reason)
    policy_observe(policy_before); policy_compare()
    saved_report=policy_report.read_text(); saved_ledger=policy_ledger.read_text()
    def reset_policy():policy_report.write_text(saved_report);policy_ledger.write_text(saved_ledger)
    def unexpected_skip(element):ET.SubElement(element.findall('testcase')[0],'skipped',message='original real connector gate')
    edit(policy_report,unexpected_skip); policy_compare(False,'execution or skip differs'); reset_policy()
    policy_ledger.write_text('\n'.join(successful[:-1])+'\n'); policy_compare(False,'ledger multiset differs'); reset_policy()
    skipped_label=next(label for label,state in policies.items() if state['expected_status']=='skipped')
    policy_ledger.write_text(saved_ledger+skipped_label+'\n'); policy_compare(False,'ledger multiset differs'); reset_policy()
    edit(policy_report,lambda element:next(c for c in element.findall('testcase') if c.find('skipped') is not None).find('skipped').__setattr__('text','org.opentest4j.TestAbortedException: rewritten gate\n\tat sample.Observer.frame(Observer.java:99)'))
    policy_compare(False,'skip reason differs'); reset_policy()
    edit(policy_report,lambda element:[c.find('skipped').__setattr__('text',c.find('skipped').text.replace('sample.Original.frame(Original.java:12)','sample.Observer.frame(Observer.java:99)')) for c in element.findall('testcase') if c.find('skipped') is not None])
    policy_compare(); reset_policy()
    # Coverage provenance is checked inside the binary execution format, not file existence.
    coverage_root = root/'coverage'; classfile = put(coverage_root,'core/target/classes/sample/Production.class',b'\xca\xfe\xba\xbe\x00\x00\x00\x41\x00\x01\x00\x21'+b'isolated-smoke-bytecode')
    def reference_crc(content):
        value = 0
        for byte in content:
            value ^= byte
            for unused in range(8): value=(value>>1)^(0xd800000000000000 if value&1 else 0)
        return format(value,'016x')
    class_id = evidence.crc64(classfile.read_bytes()); assert class_id == reference_crc(classfile.read_bytes())
    def utf(value): encoded=value.encode(); return struct.pack('>H',len(encoded))+encoded
    def exec_data(start=1000,end=2000,identifier=class_id):
        return b'\x01\xc0\xc0\x10\x07\x10'+utf('current-session')+struct.pack('>QQ',start,end)+b'\x11'+bytes.fromhex(identifier)+utf('sample/Production')+b'\x02\x03'
    execfile = put(coverage_root,'core/target/jacoco.exec',exec_data())
    xml = put(coverage_root,'core/target/site/jacoco/jacoco.xml','<report name="current"><sessioninfo id="current-session" start="1000" dump="2000"/><package name="sample"><class name="sample/Production"/><sourcefile name="Production.java"><line nr="1" ci="2" cb="1"/></sourcefile></package></report>')
    binding = save('binding.json',dict(run_id=123,attempt=1,source_sha='a'*40,started_ms=1000,completed_ms=2000,required_exec=['core/target/jacoco.exec'],required_reports=['core/target/site/jacoco/jacoco.xml'],required_report_classes={'core/target/site/jacoco/jacoco.xml':['sample/Production']}))
    def cov(expected=True,reason=None,destination='coverage.json'):
        run(['coverage','--root',str(coverage_root),'--binding',str(binding),'--output',str(root/destination)],expected,reason)
    cov(); shutil.copyfile(root/'coverage.json',root/'coverage-before.json')
    saved_xml=xml.read_text(); xml.unlink(); cov(False,'XML path set differs'); xml.write_text(saved_xml)
    xml.write_text(saved_xml.replace('<class name="sample/Production"/>','')); cov(False,'production class set differs'); xml.write_text(saved_xml)
    unknown_own=exec_data().replace(utf('sample/Production'),utf('io/tapstate/foreign/Injected'))
    execfile.write_bytes(unknown_own); cov(False,'undeclared own bytecode'); execfile.write_bytes(exec_data())
    source_file='core/target/classes/sample/Production.class'
    input_snapshot=dict(schema=1,source_sha='a'*40,run_id=123,run_attempt=1,phase='pre-test-after-install',production_files={source_file:hashlib.sha256(classfile.read_bytes()).hexdigest()},production_class_ids={source_file:class_id},production_class_access_flags={source_file:33},jacoco_version='0.8.13',test_files={},test_class_ids={},resolved_test_classpaths={'core':[]},dependency_repository_files={})
    input_file=save('coverage-inputs.json',input_snapshot)
    run(['coverage','--root',str(coverage_root),'--binding',str(binding),'--inputs',str(input_file),'--output',str(root/'coverage-from-inputs.json')])
    post_file=save('coverage-post.json',input_snapshot|dict(phase='post-test-before-pack'))
    run(['coverage','--root',str(coverage_root),'--binding',str(binding),'--inputs',str(input_file),'--test-inputs',str(post_file),'--output',str(root/'coverage-from-inputs.json')])
    save('coverage-post.json',input_snapshot|dict(phase='post-test-before-pack',production_files={source_file:'0'*64}))
    run(['coverage','--root',str(coverage_root),'--binding',str(binding),'--inputs',str(input_file),'--test-inputs',str(post_file),'--output',str(root/'coverage-from-inputs.json')],False,'changed during tests')
    execfile.write_bytes(exec_data(start=900)); cov(False,'stale or out-of-run'); execfile.write_bytes(exec_data())
    execfile.write_bytes(exec_data(identifier='0'*16)); cov(False,'class ID differs'); execfile.write_bytes(exec_data())
    execfile.write_bytes(exec_data()+exec_data(start=900)); cov(False,'stale or out-of-run'); execfile.write_bytes(exec_data())
    put(coverage_root,'core/target/old.exec',exec_data()); cov(False,'exec path set differs'); (coverage_root/'core/target/old.exec').unlink()
    execfile.unlink(); cov(False,'exec path set differs'); execfile.write_bytes(exec_data())
    execfile.write_bytes(exec_data()[:-1]); cov(False,'truncated'); execfile.write_bytes(exec_data())
    xml.write_text(xml.read_text().replace('start="1000"','start="900"')); cov(False,'old or foreign sessions')
    xml.write_text(xml.read_text().replace('start="900"','start="1000"'))
    xml.write_text(xml.read_text().replace('cb="1"','cb="0"')); cov()
    run(['compare-coverage','--before',str(root/'coverage-before.json'),'--after',str(root/'coverage.json'),'--output',str(root/'coverage-diff.json')],False,'branches decreased')
    xml.write_text(xml.read_text().replace('cb="0"','cb="1"')); cov()
    run(['compare-coverage','--before',str(root/'coverage-before.json'),'--after',str(root/'coverage.json'),'--output',str(root/'coverage-diff.json')])
    # Identical line/branch counts cannot hide swapping one covered production probe.
    baseline_probe=copy.deepcopy(json.loads((root/'coverage-before.json').read_text()))
    before_record=baseline_probe['executions']['core/target/jacoco.exec']['classes'][0]
    before_record['probes']=3;before_record['covered_probes']=[0,1]
    probe_before=save('probe-before.json',baseline_probe)
    swapped=copy.deepcopy(baseline_probe);swapped['executions']['core/target/jacoco.exec']['classes'][0]['covered_probes']=[0,2]
    probe_after=save('probe-after.json',swapped)
    probe_compare=['compare-coverage','--before',str(probe_before),'--after',str(probe_after),'--output',str(root/'probe-difference.json')]
    run(probe_compare,False,'probe hits decreased')
    superset=copy.deepcopy(baseline_probe);superset['executions']['core/target/jacoco.exec']['classes'][0]['covered_probes']=[0,1,2]
    save('probe-after.json',superset);run(probe_compare)
    # New unused/plugin cache entries remain visible, never an unproven complete D.
    post_growth=input_snapshot|dict(phase='post-test-before-pack',dependency_repository_files={'org/apache/maven/surefire/surefire-junit-platform/3.5.2/surefire-junit-platform-3.5.2.jar':'f'*64})
    save('coverage-post.json',post_growth)
    run(['coverage','--root',str(coverage_root),'--binding',str(binding),'--inputs',str(input_file),'--test-inputs',str(post_file),'--output',str(root/'growth-coverage.json')])
    growth=json.loads((root/'growth-coverage.json').read_text());assert not growth['complete'] and list(growth['dependency_repository_growth'].values())[0]['classification']=='surefire-provider'
    # Byte-bound producer facts remove only the cache-growth uncertainty.
    receipt_spec=importlib.util.spec_from_file_location('growth_receipt',pathlib.Path(os.environ['EVIDENCE_MODULE']).with_name('_ci-receipt.py'))
    receipt_module=importlib.util.module_from_spec(receipt_spec);receipt_spec.loader.exec_module(receipt_module)
    repository=root/'model-repository'
    provider='org/apache/maven/surefire/surefire-junit-platform/3.5.2/surefire-junit-platform-3.5.2'
    parent='org/apache/maven/surefire/surefire-providers/3.5.2/surefire-providers-3.5.2.pom'
    put(repository,parent,'<project><groupId>org.apache.maven.surefire</groupId><artifactId>surefire-providers</artifactId><version>3.5.2</version><packaging>pom</packaging></project>')
    put(repository,provider+'.pom','<project><parent><groupId>org.apache.maven.surefire</groupId><artifactId>surefire-providers</artifactId><version>3.5.2</version></parent><artifactId>surefire-junit-platform</artifactId></project>')
    provider_jar=repository/(provider+'.jar')
    with zipfile.ZipFile(provider_jar,'w') as archive:
        archive.writestr('META-INF/MANIFEST.MF','Implementation-Title: surefire-junit-platform\nImplementation-Version: 3.5.2\n')
    hashes={name:receipt_module.digest(repository/name) for name in (parent,provider+'.pom',provider+'.jar')}
    models,producers=receipt_module.dependency_models(repository,hashes)
    bound_growth=post_growth|dict(dependency_repository_files=hashes,dependency_coordinate_models=models,dependency_producer_facts=producers)
    save('coverage-post.json',bound_growth)
    growth_command=['coverage','--root',str(coverage_root),'--binding',str(binding),'--inputs',str(input_file),'--test-inputs',str(post_file),'--output',str(root/'growth-coverage.json')]
    run(growth_command);verified_growth=json.loads((root/'growth-coverage.json').read_text())
    assert verified_growth['complete'] and all(fact['coordinate_type_producer_verified'] and not fact['runtime_realm_proven'] and not fact['effective_dependency_model_proven'] for fact in verified_growth['dependency_repository_growth'].values())
    broken=copy.deepcopy(bound_growth);broken['dependency_coordinate_models'][provider+'.pom']['parent']['sha256']='0'*64
    save('coverage-post.json',broken);run(growth_command,False,'parent bytes or coordinates differ')
    broken=copy.deepcopy(bound_growth);broken['dependency_producer_facts'][provider+'.jar']['sha256']='0'*64
    save('coverage-post.json',broken);run(growth_command,False,'producer model/bytes differ')
    broken=copy.deepcopy(bound_growth);broken['dependency_producer_facts'][provider+'.jar']['own_class_count']=1
    save('coverage-post.json',broken);run(growth_command);assert not json.loads((root/'growth-coverage.json').read_text())['complete']
    broken=copy.deepcopy(bound_growth);broken['dependency_coordinate_models'][provider+'.pom']['packaging']='pom'
    save('coverage-post.json',broken);run(growth_command,False,'non-JAR model type')
    # Generated classes require exact current dump IDs and structural/framework
    # provenance; suffixes alone and a forged known-class ID never grant admission.
    runtime_pre=copy.deepcopy(input_snapshot)
    base_name='io/tapstate/fixture/TestBase';base_path='core/target/test-classes/'+base_name+'.class'
    runtime_pre['test_class_ids']={base_path:'1'*16};runtime_pre['test_files']={base_path:'2'*64}
    runtime_repo=root/'runtime-framework-repository';runtime_models={};runtime_producers={};runtime_hashes={};cp=[]
    for gav,ref in [(['org.springframework','spring-core','7.0.8'],'org/springframework/cglib/proxy/Factory'),(['org.mockito','mockito-core','5.23.0'],'org/mockito/internal/creation/bytebuddy/access/MockAccess')]:
        relative=gav[0].replace('.','/')+'/'+gav[1]+'/'+gav[2]+'/'+gav[1]+'-'+gav[2]
        put(runtime_repo,relative+'.pom','<project><groupId>'+gav[0]+'</groupId><artifactId>'+gav[1]+'</artifactId><version>'+gav[2]+'</version></project>')
        jar=runtime_repo/(relative+'.jar')
        with zipfile.ZipFile(jar,'w') as archive:
            archive.writestr(ref+'.class',b'fixture')
            if gav[1]=='spring-core':archive.writestr('org/springframework/cglib/reflect/FastClass.class',b'fixture')
            archive.writestr('META-INF/MANIFEST.MF','Implementation-Title: '+gav[1]+'\nImplementation-Version: '+gav[2]+'\n')
        for suffix in ('.pom','.jar'):runtime_hashes[relative+suffix]=receipt_module.digest(runtime_repo/(relative+suffix))
        cp.append(dict(identity='repository/'+relative+'.jar',sha256=runtime_hashes[relative+'.jar'],own_classes={}))
    runtime_models,runtime_producers=receipt_module.dependency_models(runtime_repo,runtime_hashes)
    agent_path='org/jacoco/org.jacoco.agent/0.8.13/org.jacoco.agent-0.8.13-runtime.jar';runtime_hashes[agent_path]='f'*64
    runtime_pre.update(resolved_test_classpaths={'core':cp},dependency_repository_files=runtime_hashes)
    runtime_input_file=save('runtime-inputs.json',runtime_pre);runtime_input_hash=hashlib.sha256(runtime_input_file.read_bytes()).hexdigest()
    capture=dict(enabled=True,raw_bytes_uploaded=False,jacoco_version='0.8.13',agent_sha256='f'*64,source_sha='a'*40,run_id='123',run_attempt=1,build_inputs_sha256=runtime_input_hash,shard_id='rest',invocation_id='c'*32,cohort_hash='d'*64,capture_origin_sha256='e'*64,status='success')
    capture['verify']=dict(schema=1,execution='github-actions',command='run',event='end',phase='selected-verify',status='success',source_sha='a'*40,run_id='123',run_attempt=1,shard_id='rest',cohort_hash='d'*64,utc='1970-01-01T00:00:01+00:00',ended_utc='1970-01-01T00:00:02+00:00',started_monotonic_ns=100,monotonic_ns=1100,elapsed_ns=1000)
    capture['reset']=capture['verify']|dict(phase='reset-test-evidence',ended_utc='1970-01-01T00:00:01+00:00',started_monotonic_ns=0,monotonic_ns=100,elapsed_ns=100)
    runtime_binding=json.loads(binding.read_text())|dict(runtime_class_capture={field:copy.deepcopy(capture[field]) for field in ['shard_id','invocation_id','cohort_hash','capture_origin_sha256','reset','verify']})
    base=dict(name=base_name,class_id='1'*16,sha256='2'*64,origin='test')
    def generated(name,identifier,parent,interfaces,refs,family):
        structure=dict(name=name,class_id=identifier,sha256='9'*64,access_flags=33,super_class=parent,interfaces=interfaces,class_references=sorted(set([name,parent,*interfaces,*refs])))
        compiled=[base] if base_name in structure['class_references'] else []
        families={key:[] for key in ('spring_cglib','mockito','bytebuddy')};families[family]=[ref for ref in structure['class_references'] if ref.startswith('org/springframework/cglib/' if family=='spring_cglib' else 'org/mockito/')]
        origins=[dict(gav=['org.springframework','spring-core','7.0.8'] if family=='spring_cglib' else ['org.mockito','mockito-core','5.23.0'],sha256=cp[0 if family=='spring_cglib' else 1]['sha256'],class_references=families[family])] if families[family] else []
        return structure|dict(compiled_references=compiled,framework_references=families,framework_origins=origins,unresolved_framework_references=[],runtime_references=[])
    spring=generated(base_name+'$$SpringCGLIB$$0','3'*16,base_name,['org/springframework/cglib/proxy/Factory'],[],'spring_cglib')
    mock=generated(base_name+'$MockitoMock$fixture','4'*16,base_name,['org/mockito/internal/creation/bytebuddy/access/MockAccess'],[],'mockito')
    auxiliary=generated(mock['name']+'$auxiliary$fixture','5'*16,'java/lang/Object',['java/util/concurrent/Callable'],[mock['name']],'bytebuddy')
    auxiliary['runtime_references']=[dict(name=mock['name'],class_ids=[mock['class_id']])]
    runtime_post=runtime_pre|dict(phase='post-test-before-pack',runtime_class_capture=capture,runtime_generated_classes=[spring,mock,auxiliary],dependency_coordinate_models=runtime_models,dependency_producer_facts=runtime_producers)
    def runtime_check(snapshot,success=True,reason=None,bound=None):
        global checks
        try:result=evidence.verified_runtime_classes(runtime_pre,snapshot,bound or runtime_binding,runtime_input_hash)
        except ValueError as error:
            assert not success and (reason is None or reason in str(error)),str(error)
        else:assert success;assert len(result)==len(snapshot['runtime_generated_classes'])
        checks+=1
    runtime_check(runtime_post)
    local=copy.deepcopy(runtime_post);local['runtime_class_capture']['run_id']='local'
    for phase in ('reset','verify'):local['runtime_class_capture'][phase].update(execution='local',run_id=None,run_attempt=None)
    local_binding=copy.deepcopy(runtime_binding);local_binding['run_id']='local'
    for phase in ('reset','verify'):local_binding['runtime_class_capture'][phase]=copy.deepcopy(local['runtime_class_capture'][phase])
    local_pre=runtime_pre|dict(run_id='local')
    # This focused call preserves raw local null identities; it never fabricates Actions IDs.
    accepted=evidence.verified_runtime_classes(local_pre,local,local_binding,runtime_input_hash);assert len(accepted)==3;checks+=1
    fast=generated(base_name+'$$SpringCGLIB$$FastClass$$1','7'*16,'org/springframework/cglib/reflect/FastClass',[],[spring['name']],'spring_cglib')
    fast['runtime_references']=[dict(name=spring['name'],class_ids=[spring['class_id']])]
    chained=copy.deepcopy(runtime_post);chained['runtime_generated_classes'].append(fast);runtime_check(chained)
    serializable=copy.deepcopy(runtime_post);serializable['runtime_generated_classes'][2]['interfaces'].append('java/io/Serializable');serializable['runtime_generated_classes'][2]['class_references'].append('java/io/Serializable');runtime_check(serializable)
    # Distinct CRCs of the same generated name must both survive admission.
    variants=copy.deepcopy(runtime_post);variant=copy.deepcopy(spring);variant['class_id']='6'*16;variants['runtime_generated_classes'].append(variant);runtime_check(variants)
    for field,value,reason in [('run_id','old','source/run/attempt/input differs'),('build_inputs_sha256','0'*64,'source/run/attempt/input differs'),('capture_origin_sha256','0'*64,'actual shard provenance'),('agent_sha256','0'*64,'agent bytes differ'),('raw_bytes_uploaded',True,'unsupported runtime capture')]:
        bad=copy.deepcopy(runtime_post);bad['runtime_class_capture'][field]=value;runtime_check(bad,False,reason)
    for field,value,reason in [('compiled_references',[],'compiled references differ'),('framework_origins',[],'origin set differs'),('unresolved_framework_references',['missing'],'incomplete'),('super_class','java/lang/Object','invalid runtime class header')]:
        bad=copy.deepcopy(runtime_post);bad['runtime_generated_classes'][0][field]=value;runtime_check(bad,False,reason)
    bad=copy.deepcopy(runtime_post);bad['runtime_generated_classes'][0]['framework_origins'][0]['sha256']='0'*64;runtime_check(bad,False,'producer/bytes differ')
    bad=copy.deepcopy(runtime_post);bad['runtime_generated_classes'][0]['name']=base_name;runtime_check(bad,False,'cannot exempt a compiled class')
    bad=copy.deepcopy(runtime_post);bad['runtime_generated_classes'].append(copy.deepcopy(spring));runtime_check(bad,False,'duplicate runtime class')
    bad=copy.deepcopy(runtime_post);bad['runtime_generated_classes'][2]['runtime_references'][0]['class_ids']=['0'*16];runtime_check(bad,False,'reference set is incomplete')
    bad=copy.deepcopy(runtime_post);bad['runtime_generated_classes'][0]['framework_origins']*=2;runtime_check(bad,False,'ambiguous runtime framework reference producer')
    bad=copy.deepcopy(runtime_post);bad['runtime_generated_classes'][0]['super_class']='java/lang/Object';bad['runtime_generated_classes'][0]['class_references'].append('java/lang/Object');runtime_check(bad,False,'unsupported or unanchored')
    bad=copy.deepcopy(runtime_post);bad['runtime_generated_classes'][0]['class_references'].append('io/tapstate/foreign/Unknown');runtime_check(bad,False,'unclosed own structural reference')
    bad=copy.deepcopy(runtime_post);bad['runtime_generated_classes'][0]['class_references'].append('[Lio/tapstate/foreign/Unknown;');runtime_check(bad,False,'unclosed own structural reference')
    bad=copy.deepcopy(runtime_binding);bad['runtime_class_capture']['invocation_id']='0'*32;runtime_check(runtime_post,False,'actual shard provenance',bad)
    bad=copy.deepcopy(runtime_post);bad['runtime_class_capture']['verify']['utc']='1970-01-01T00:00:01';bound=copy.deepcopy(runtime_binding);bound['runtime_class_capture']['verify']=bad['runtime_class_capture']['verify'];runtime_check(bad,False,'timestamps lack timezone',bound)
    bad=copy.deepcopy(runtime_post);bad['runtime_class_capture']['run_attempt']='1';runtime_check(bad,False,'capture attempt type')
    bad=copy.deepcopy(runtime_post);bad['runtime_class_capture']['verify']['run_attempt']='1';bound=copy.deepcopy(runtime_binding);bound['runtime_class_capture']['verify']=copy.deepcopy(bad['runtime_class_capture']['verify']);runtime_check(bad,False,'phase attempt type',bound)
    # Even identically malformed post and binding phase objects are rejected.
    for field,value,reason in [('schema',999,'invalid runtime phase contract'),('execution','made-up','invalid runtime phase contract'),('command','fake','invalid runtime phase contract'),('event','start','invalid runtime phase contract'),('elapsed_ns',-1,'monotonic clocks'),('started_monotonic_ns',True,'monotonic clocks')]:
        bad=copy.deepcopy(runtime_post);bad['runtime_class_capture']['verify'][field]=value;bound=copy.deepcopy(runtime_binding);bound['runtime_class_capture']['verify']=copy.deepcopy(bad['runtime_class_capture']['verify']);runtime_check(bad,False,reason,bound)
    bad=copy.deepcopy(runtime_post);bad['runtime_class_capture']['verify']['elapsed_ns']=999;bound=copy.deepcopy(runtime_binding);bound['runtime_class_capture']['verify']=copy.deepcopy(bad['runtime_class_capture']['verify']);runtime_check(bad,False,'monotonic clocks',bound)
    bad=copy.deepcopy(runtime_post);bad['runtime_class_capture']['reset']['ended_utc']='1970-01-01T00:00:02+00:00';bound=copy.deepcopy(runtime_binding);bound['runtime_class_capture']['reset']=copy.deepcopy(bad['runtime_class_capture']['reset']);runtime_check(bad,False,'reset must finish',bound)
    # Exercise the public coverage command: exact dump identity admits one class,
    # while an identical name with an unobserved CRC is rejected.
    runtime_post_file=save('runtime-post.json',runtime_post);runtime_binding_file=save('runtime-binding.json',runtime_binding)
    own_exec=b'\x11'+bytes.fromhex(spring['class_id'])+utf(spring['name'])+b'\x02\x03'
    execfile.write_bytes(exec_data()+own_exec)
    runtime_cov=['coverage','--root',str(coverage_root),'--binding',str(runtime_binding_file),'--inputs',str(runtime_input_file),'--test-inputs',str(runtime_post_file),'--output',str(root/'runtime-coverage.json')]
    run(runtime_cov);assert json.loads((root/'runtime-coverage.json').read_text())['complete']
    execfile.write_bytes(exec_data()+own_exec.replace(bytes.fromhex(spring['class_id']),b'\x00'*8));run(runtime_cov,False,'undeclared own bytecode');execfile.write_bytes(exec_data())
    # A merged run keeps each worker's dynamic bytecode attached to its own exec.
    worker_root=root/'worker-coverage'
    worker_report='core/target/site/jacoco/jacoco.xml'
    put(worker_root,worker_report,saved_xml)
    worker_execs={'app-1':'target/ci-shards/app-1/core/target/jacoco.exec',
                  'rest':'target/ci-shards/rest/core/target/jacoco.exec'}
    merged_exec='target/jacoco-reactor.exec'
    mock_exec=b'\x11'+bytes.fromhex(mock['class_id'])+utf(mock['name'])+b'\x02\x03'
    put(worker_root,worker_execs['app-1'],exec_data()+own_exec)
    put(worker_root,worker_execs['rest'],exec_data()+mock_exec)
    put(worker_root,merged_exec,exec_data()+own_exec+mock_exec)
    worker_binding=json.loads(binding.read_text())|dict(required_exec=[*worker_execs.values(),merged_exec],
                required_workers=['app-1','rest','e2e-1'],merged_exec=[merged_exec])
    worker_binding_file=save('worker-binding.json',worker_binding)
    def file_ref(path):return dict(path=str(path),sha256=hashlib.sha256(path.read_bytes()).hexdigest())
    workers={}
    worker_posts={}
    for worker,generated_class in [('app-1',spring),('rest',mock),('e2e-1',None)]:
        post=copy.deepcopy(runtime_post);post['runtime_generated_classes']=[copy.deepcopy(generated_class)] if generated_class else []
        post['runtime_class_capture']['shard_id']=worker
        for phase in ('reset','verify'):post['runtime_class_capture'][phase]['shard_id']=worker
        peer_binding={k:worker_binding[k] for k in ['run_id','attempt','source_sha','started_ms','completed_ms']}
        peer_binding['runtime_class_capture']={k:copy.deepcopy(post['runtime_class_capture'][k]) for k in ['shard_id','invocation_id','cohort_hash','capture_origin_sha256','reset','verify']}
        post_path=save(worker+'-post.json',post);binding_path=save(worker+'-binding.json',peer_binding)
        provenance={k:copy.deepcopy(post['runtime_class_capture'][k]) for k in ['source_sha','run_id','run_attempt','shard_id','invocation_id','cohort_hash','reset','verify']}
        provenance['runtime_class_capture']={key:post['runtime_class_capture'][key]
                for key in ['capture_origin_sha256','build_inputs_sha256']}
        provenance['case_observer']=dict(enabled=True,build_inputs_sha256=runtime_input_hash)
        provenance_path=save(worker+'-provenance.json',provenance)
        member='core/target/jacoco.exec';marker='target/ci-shards/run-'+worker+'.json'
        packet_files={marker:hashlib.sha256(provenance_path.read_bytes()).hexdigest()}
        if worker in worker_execs:
            packet_files[member]=hashlib.sha256((worker_root/worker_execs[worker]).read_bytes()).hexdigest()
        packet=dict(schema=2,source_sha='a'*40,shard_id=worker,provenance=provenance,provenance_path=marker,
                    required_exec=[member] if worker in worker_execs else [],files=packet_files)
        packet_path=save(worker+'-result-manifest.json',packet)
        workers[worker]=dict(inputs=file_ref(runtime_input_file),test_inputs=file_ref(post_path),
                             binding=file_ref(binding_path),required_exec=[worker_execs[worker]] if worker in worker_execs else [],
                             result_manifest=file_ref(packet_path),provenance=file_ref(provenance_path))
        worker_posts[worker]=post
    worker_manifest=dict(schema=1,type='coverage-worker-inputs',workers=workers)
    worker_manifest_file=save('worker-inputs.json',worker_manifest)
    worker_command=['coverage','--root',str(worker_root),'--binding',str(worker_binding_file),
        '--inputs',str(runtime_input_file),'--worker-inputs',str(worker_manifest_file),'--output',str(root/'worker-coverage.json')]
    # The old single-capture scope fails for the second worker for the original reason.
    single=worker_binding|dict(runtime_class_capture=json.loads(pathlib.Path(workers['app-1']['binding']['path']).read_text())['runtime_class_capture'])
    single.pop('required_workers');single.pop('merged_exec')
    single_file=save('worker-single-binding.json',single)
    run(['coverage','--root',str(worker_root),'--binding',str(single_file),'--inputs',str(runtime_input_file),
         '--test-inputs',workers['app-1']['test_inputs']['path'],'--output',str(root/'worker-single.json')],False,'undeclared own bytecode')
    run(worker_command)
    worker_value=json.loads((root/'worker-coverage.json').read_text())
    assert worker_value['complete'] and set(worker_value['worker_inputs']['workers'])=={'app-1','rest','e2e-1'}
    assert worker_value['worker_inputs']['manifest_sha256']==file_ref(worker_manifest_file)['sha256']
    assert worker_value['runtime_capture'] is None and not worker_value['runtime_generated_classes']
    # A self-consistent rewritten input pair still differs from the original receipt.
    rewritten=copy.deepcopy(runtime_pre);rewritten['source_files']={'changed.java':'0'*64}
    rewritten_path=save('rewritten-inputs.json',rewritten)
    rewritten_post=copy.deepcopy(worker_posts['app-1'])
    rewritten_post['runtime_class_capture']['build_inputs_sha256']=file_ref(rewritten_path)['sha256']
    rewritten_post_path=save('rewritten-post.json',rewritten_post)
    rewritten_manifest=copy.deepcopy(worker_manifest)
    rewritten_manifest['workers']['app-1'].update(inputs=file_ref(rewritten_path),test_inputs=file_ref(rewritten_post_path))
    save('worker-inputs.json',rewritten_manifest)
    run(worker_command,False,'prepared inputs differ from original runtime capture receipt')
    save('worker-inputs.json',worker_manifest)
    # Simulated replacement after verification must never feed reopened bytes into parsing.
    read_bytes=pathlib.Path.read_bytes;input_context=evidence.coverage_input_context;load=evidence.load
    protected=set();reopened=[];exec_reads={(worker_root/name).resolve():0 for name in [*worker_execs.values(),merged_exec]}
    def replace_on_reopen(path):
        path=path.resolve()
        if path in protected:
            reopened.append(str(path));return b'changed after verification'
        if path in exec_reads:
            exec_reads[path]+=1
            if exec_reads[path]>1:return b'changed after verification'
        return read_bytes(path)
    def replaced_document(path):
        if pathlib.Path(path).resolve() in protected:
            reopened.append(str(path));raise ValueError('document changed after verification')
        return load(path)
    def consume_verified(args,binding,prepared=None,tested=None):
        if prepared is not None:protected.update([pathlib.Path(args.inputs).resolve(),pathlib.Path(args.test_inputs).resolve()])
        try:return input_context(args,binding,prepared=prepared,tested=tested)
        finally:protected.clear()
    argv=sys.argv
    try:
        pathlib.Path.read_bytes=replace_on_reopen;evidence.coverage_input_context=consume_verified;evidence.load=replaced_document
        sys.argv=[os.environ['EVIDENCE_MODULE'],*worker_command];evidence.main()
    finally:
        pathlib.Path.read_bytes=read_bytes;evidence.coverage_input_context=input_context;evidence.load=load;sys.argv=argv
    assert not reopened and set(exec_reads.values())=={1}, (reopened,exec_reads)
    checks+=1
    # A declared worker binding cannot fall back to legacy aggregate validation.
    for path in [*worker_execs.values(),merged_exec]:put(worker_root,path,exec_data())
    put(worker_root,merged_exec,exec_data()[:-1]+b'\x01')
    missing_manifest=[value for value in worker_command]
    position=missing_manifest.index('--worker-inputs');del missing_manifest[position:position+2]
    run(missing_manifest,False,'worker coverage manifest required')
    put(worker_root,worker_execs['app-1'],exec_data()+own_exec)
    put(worker_root,worker_execs['rest'],exec_data()+mock_exec)
    put(worker_root,merged_exec,exec_data()+own_exec+mock_exec)
    # Another worker's observed class cannot exempt foreign bytes in this worker.
    put(worker_root,worker_execs['app-1'],exec_data()+own_exec+mock_exec)
    run(worker_command,False,'worker exec digest differs')
    original_packet=json.loads(pathlib.Path(workers['app-1']['result_manifest']['path']).read_text())
    changed_packet=copy.deepcopy(original_packet)
    changed_packet['files']['core/target/jacoco.exec']=hashlib.sha256((worker_root/worker_execs['app-1']).read_bytes()).hexdigest()
    changed_packet_path=save('app-1-result-manifest.json',changed_packet)
    changed_manifest=copy.deepcopy(worker_manifest);changed_manifest['workers']['app-1']['result_manifest']=file_ref(changed_packet_path)
    save('worker-inputs.json',changed_manifest)
    run(worker_command,False,'undeclared own bytecode')
    save('app-1-result-manifest.json',original_packet);save('worker-inputs.json',worker_manifest)
    put(worker_root,worker_execs['app-1'],exec_data()+own_exec)
    put(worker_root,merged_exec,exec_data()+own_exec)
    run(worker_command,False,'merged coverage differs')
    put(worker_root,merged_exec,exec_data()+own_exec+mock_exec)
    pristine=copy.deepcopy(worker_manifest)
    worker_manifest['workers'].pop('rest');save('worker-inputs.json',worker_manifest)
    run(worker_command,False,'worker coverage set differs')
    worker_manifest=copy.deepcopy(pristine)
    worker_manifest['workers'].pop('e2e-1');save('worker-inputs.json',worker_manifest)
    run(worker_command,False,'worker coverage set differs')
    worker_manifest=copy.deepcopy(pristine)
    worker_manifest['workers']['app-1']['required_exec']=[worker_execs['rest']];save('worker-inputs.json',worker_manifest)
    run(worker_command,False,'worker exec ownership')
    worker_manifest=copy.deepcopy(pristine)
    worker_manifest['workers']['rest']['test_inputs']['sha256']='0'*64;save('worker-inputs.json',worker_manifest)
    run(worker_command,False,'reference hash differs')
    worker_manifest=copy.deepcopy(pristine)
    old_post=copy.deepcopy(worker_posts['rest']);old_post['runtime_class_capture']['run_id']='old'
    save('rest-post.json',old_post)
    worker_manifest['workers']['rest']['test_inputs']=file_ref(pathlib.Path(pristine['workers']['rest']['test_inputs']['path']))
    save('worker-inputs.json',worker_manifest);run(worker_command,False,'source/run/attempt/input differs')
    save('rest-post.json',worker_posts['rest']);save('worker-inputs.json',pristine)
    run(worker_command)
    gate_record = dict(id='ci:build:dispatch',required=True,applicable=True,applicability='workflow_dispatch',contract_sha256='f'*64,scope='always',conclusion='success')
    gates_before=save('gates-before.json',dict(schema=1,gates=[gate_record])); gates_after=save('gates-after.json',dict(schema=1,gates=[gate_record]))
    def gate(success=True,reason=None):run(['gates','--baseline',str(gates_before),'--candidate',str(gates_after),'--output',str(root/'gates-diff.json')],success,reason)
    gate()
    for mutation,reason in [(dict(conclusion='skipped'),'unsuccessful'),(dict(required=False),'contract changed'),(dict(contract_sha256='e'*64),'contract changed'),(dict(applicable=False),'cannot be non-applicable')]:
        save('gates-after.json',dict(schema=1,gates=[gate_record|mutation])); gate(False,reason)
    save('gates-after.json',dict(schema=1,gates=[])); gate(False,'empty gate contract')
    # Identical malformed contracts on both sides still must be rejected.
    for mutation in [dict(applicable=False,conclusion='skipped',applicability={'reason':'unchecked'}),dict(required='true'),dict(scope='made-up'),dict(scope='conditional',applicability={'reason':'unchecked','decision':True})]:
        save('gates-before.json',dict(schema=1,gates=[gate_record|mutation]));save('gates-after.json',dict(schema=1,gates=[gate_record|mutation]));gate(False)
    save('gates-before.json',dict(schema=1,gates=[gate_record]));save('gates-after.json',dict(schema=1,gates=[gate_record]))
    # A source archive nested under another Git checkout cannot inherit its HEAD.
    parent_repo=root/'ancestor';parent_repo.mkdir()
    subprocess.run(['git','init','-q',str(parent_repo)],check=True)
    put(parent_repo,'README','ancestor')
    subprocess.run(['git','-C',str(parent_repo),'add','README'],check=True)
    subprocess.run(['git','-C',str(parent_repo),'-c','user.name=Fixture','-c','user.email=fixture@example.com','commit','-qm','fixture'],check=True)
    extracted=parent_repo/'readback';shutil.copytree(source,extracted)
    assert evidence.current_sha(extracted) is None
    archive_dir=root/'source-archive';archive_dir.mkdir();source_tar=archive_dir/'source.tar'
    with tarfile.open(source_tar,'w',format=tarfile.PAX_FORMAT,pax_headers={'comment':'a'*40}) as tar:tar.add(source,arcname='.')
    raw_run=put(archive_dir,'run.json',json.dumps(dict(id=123,run_attempt=1,head_sha='a'*40,repository={'full_name':'tapstate/tapstate'})))
    source_receipt=put(archive_dir,'receipt.json',json.dumps(dict(schema=1,repository='tapstate/tapstate',run_id=123,run_attempt=1,source_sha='a'*40,files={path.name:dict(size=path.stat().st_size,sha256=hashlib.sha256(path.read_bytes()).hexdigest()) for path in [source_tar,raw_run]})))
    source_command=['inventory','--root',str(extracted),'--lane','ci','--source-archive',str(source_tar),'--source-receipt',str(source_receipt),'--output',str(root/'archived-inventory.json')]
    run(source_command);assert json.loads((root/'archived-inventory.json').read_text())['source_sha']=='a'*40
    put(extracted,'core/pom.xml','modified');run(source_command,False,'file set/bytes differs');shutil.copyfile(source/'core/pom.xml',extracted/'core/pom.xml')
    original_tar=source_tar.read_bytes();source_tar.write_bytes(original_tar+b'tamper');run(source_command,False,'size/hash differs');source_tar.write_bytes(original_tar)
    artifact_root=root/'artifact'; member=put(artifact_root,'raw/test.xml','original bytes')
    origin=dict(run_id=123,attempt=1,source_sha='a'*40)
    artifact_manifest=save('artifact.json',origin|dict(files={'raw/test.xml':hashlib.sha256(member.read_bytes()).hexdigest()}))
    def artifact(success=True,reason=None):run(['artifacts','--root',str(artifact_root),'--manifest',str(artifact_manifest),'--binding',str(binding),'--output',str(root/'artifact-admission.json')],success,reason)
    artifact(); member.write_text('tampered bytes'); artifact(False,'hash or file set differs'); member.write_text('original bytes')
    save('artifact.json',(origin|dict(attempt=2))|dict(files={'raw/test.xml':hashlib.sha256(member.read_bytes()).hexdigest()})); artifact(False,'origin binding differs')
print('ci-evidence smoke: '+str(checks)+' positive and adverse evidence checks passed')
PY
