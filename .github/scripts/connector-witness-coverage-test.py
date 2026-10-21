#!/usr/bin/env python3
"""Check that every gated witness is planned or explicitly deferred for Atlas."""

import json
from pathlib import Path
import re
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[2]
GATE_CALL = re.compile(r'\bRealConnectorGate\s*\.\s*require\s*\(')
EXTERNAL_ATLAS = {
    'CloudAtlasObservabilityRecoveryIT', 'RealAtlasSourceHttpIT',
    'RealAtlasToAtlasPipelineIT', 'RealMySqlToAtlasPipelineIT',
}


class ConnectorWitnessCoverageTest(unittest.TestCase):
    def assert_coverage(self, gated, planned, deferred):
        self.assertLessEqual(deferred, EXTERNAL_ATLAS, 'Only the four external Atlas witnesses may be deferred')
        self.assertLessEqual(deferred, gated, 'Deferred witnesses must still exist in the source inventory')
        self.assertFalse(planned & deferred, 'Deferred witnesses must not be assigned to an active shard')
        missing = sorted(gated - planned - deferred)
        if missing:
            self.fail(f'{len(missing)} gated witnesses have no real-connector shard:\n'
                      + '\n'.join(missing))

    def test_every_gated_witness_is_in_the_connector_plan(self):
        with tempfile.TemporaryDirectory() as directory:
            plan_path = Path(directory) / 'plan.json'
            subprocess.run(
                [str(ROOT / '.github/scripts/connector-witnesses.sh'), 'plan',
                 '--root', str(ROOT), '--output', str(plan_path)],
                check=True, capture_output=True, text=True,
            )
            plan = json.loads(plan_path.read_text())
            planned = {test['class'].rsplit('.', 1)[-1] for test in plan['expected']}
            deferred = {name.rsplit('.', 1)[-1] for name in plan['connector_deferrals']['classes']}

        gated = {source.stem for source in (ROOT / 'e2e/src/test/java').rglob('*IT.java')
                 if GATE_CALL.search(source.read_text())}
        self.assertTrue(gated, 'No real-connector witnesses were discovered')
        self.assert_coverage(gated, planned, deferred)

    def test_a_new_gated_witness_cannot_disappear_or_claim_the_atlas_exception(self):
        gated = EXTERNAL_ATLAS | {'NewRecoveryIT'}
        with self.assertRaises(AssertionError):
            self.assert_coverage(gated, set(), EXTERNAL_ATLAS)
        with self.assertRaises(AssertionError):
            self.assert_coverage(gated, set(), gated)

    def test_only_explicit_deferrals_are_exempt_and_restoration_is_supported(self):
        gated = EXTERNAL_ATLAS | {'NewRecoveryIT'}
        self.assert_coverage(gated, {'NewRecoveryIT'}, EXTERNAL_ATLAS)
        self.assert_coverage(gated, gated, set())
        with self.assertRaises(AssertionError):
            self.assert_coverage(gated, {'NewRecoveryIT'}, set())
        with self.assertRaises(AssertionError):
            self.assert_coverage(gated, gated, EXTERNAL_ATLAS)
        with self.assertRaises(AssertionError):
            self.assert_coverage({'NewRecoveryIT'}, {'NewRecoveryIT'}, EXTERNAL_ATLAS)


if __name__ == '__main__':
    unittest.main()
