#!/usr/bin/env bash
# Explicitly run the single diagnostic isolated under issue #911.
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$root"
docker info >/dev/null
test_class=io.tapstate.e2e.AHundredTablesShareTheSinksWritersIT
test_method=aHundredTablesAreWrittenByTheSinksFourWritersOneTableABatch
report="e2e/target/failsafe-reports/TEST-${test_class}.xml"
# An earlier report must never certify a new invocation that executed no test.
rm -f -- "$report"
mvn -B -pl e2e -am -Dtest=__hundred_table_diagnostic_no_unit_tests__ \
  "-Dit.test=${test_class}#${test_method}" \
  -Dsurefire.failIfNoSpecifiedTests=false -Dfailsafe.failIfNoSpecifiedTests=false \
  -Dmaven.test.skip=false -DskipTests=false -DskipITs=false -Dapi.version=1.44 \
  -Djunit.jupiter.conditions.deactivate=org.junit.jupiter.engine.extension.DisabledCondition verify
python3 - "$report" "$test_class" "$test_method" <<'PY'
import sys
import xml.etree.ElementTree as ET
report, classname, method = sys.argv[1:]
suite = ET.parse(report).getroot()
counts = {name: int(suite.get(name, '-1')) for name in ('tests', 'failures', 'errors', 'skipped')}
expected = dict(tests=1, failures=0, errors=0, skipped=0)
cases = list(suite.iter('testcase'))
if counts != expected or len(cases) != 1:
    raise SystemExit('The independent diagnostic requires one executed, successful, zero-skip test: ' + str(counts))
case = cases[0]
if (case.get('classname') != classname or case.get('name', '').split('(', 1)[0] != method
        or any(case.find(tag) is not None for tag in ('skipped', 'failure', 'error'))):
    raise SystemExit('The diagnostic report does not describe the selected executed test')
print('Independent hundred-table diagnostic: 1 executed, 0 failures, 0 errors, 0 skipped.')
PY
