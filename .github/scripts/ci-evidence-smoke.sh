#!/usr/bin/env bash
# Independent method/spec inventories must discriminate successful-looking evidence loss.
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
export EVIDENCE_TOOL="$here/ci-evidence.sh" EVIDENCE_MODULE="$here/_ci-evidence.py"
python3 - <<'PY'
import copy, hashlib, importlib.util, json, os, pathlib, shutil, struct, subprocess, sys, tempfile, tarfile
import xml.etree.ElementTree as ET
sys.dont_write_bytecode = True
spec = importlib.util.spec_from_file_location('evidence', os.environ['EVIDENCE_MODULE'])
evidence = importlib.util.module_from_spec(spec); spec.loader.exec_module(evidence)
checks = 0
with tempfile.TemporaryDirectory() as temporary:
    root = pathlib.Path(temporary)
    source = root/'source'; reports = root/'reports'
    def put(base, path, value):
        target = base/path; target.parent.mkdir(parents=True, exist_ok=True)
        if isinstance(value, bytes): target.write_bytes(value)
        else: target.write_text(value)
        return target
    def run(command, expected=True, reason=None):
        global checks
        result = subprocess.run([os.environ['EVIDENCE_TOOL'], *command], capture_output=True, text=True)
        assert (result.returncode == 0) == expected, result.stdout + result.stderr
        if reason is not None: assert reason in result.stderr, result.stderr
        checks += 1
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
  @ParameterizedTest @ValueSource(strings = {"first", "second"}) void values(String value) {}
  @Nested class Inner { @Test void nested() {} }
}''')
    put(source,'core/src/test/java/sample/Base.java','package sample; abstract class Base { @Test void inherited() {} }')
    put(source,'e2e/src/test/java/sample/PublishedExamplesIT.java','''package sample;
class PublishedExamplesIT {
  @ParameterizedTest @MethodSource("everyPublishedExampleOnEveryTier")
  void thePublishedExampleRuns(Path specification, Tiers tier) {}
}''')
    put(source,'e2e/src/test/java/sample/E2eConnectorJar.java','package sample; class E2eConnectorJar { static final String CONNECTOR_ID = "e2e_file"; }')
    for example in ('alpha','beta'): put(source,'e2e/examples/'+example+'/case.e2e.yml','setup:\n  connectors: [e2e_file]\n')
    inventory = root/'inventory.json'
    run(['inventory','--root',str(source),'--lane','ci','--output',str(inventory)])
    inv = json.loads(inventory.read_text()); assert inv['complete'] and len(inv['cases']) == 11
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
    value_cases = [case('sample.CoreTest','values(String)['+str(n+1)+']') for n in range(2)]
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
    # Unknown source constructs are observations, never complete equivalence.
    unknown = put(source,'core/src/test/java/sample/FutureTest.java','package sample; class FutureTest { @TestFactory Stream tests() {} }')
    run(['inventory','--root',str(source),'--lane','ci','--output',str(root/'unknown.json')]); assert not json.loads((root/'unknown.json').read_text())['complete']; unknown.unlink()
    # Match the existing ordinary-PR capability mix: every spec still has both XML
    # identities, while only the 18 synthetic specs may vouch in the success ledger.
    policy_source=root/'policy-source'; policy_reports=root/'policy-reports'
    put(policy_source,'pom.xml','<project><modules><module>e2e</module></modules></project>')
    put(policy_source,'e2e/pom.xml',(source/'e2e/pom.xml').read_text())
    put(policy_source,'e2e/src/test/java/sample/E2eConnectorJar.java',(source/'e2e/src/test/java/sample/E2eConnectorJar.java').read_text())
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
