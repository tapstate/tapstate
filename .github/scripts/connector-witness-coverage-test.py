#!/usr/bin/env python3
"""Check that the connector lane plans every witness requiring real jars."""

import json
from pathlib import Path
import re
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[2]
GATE_CALL = re.compile(r'\bRealConnectorGate\s*\.\s*require\s*\(')


class ConnectorWitnessCoverageTest(unittest.TestCase):
    def test_every_gated_witness_is_in_the_connector_plan(self):
        with tempfile.TemporaryDirectory() as directory:
            plan_path = Path(directory) / 'plan.json'
            subprocess.run(
                [str(ROOT / '.github/scripts/connector-witnesses.sh'), 'plan',
                 '--root', str(ROOT), '--output', str(plan_path)],
                check=True, capture_output=True, text=True,
            )
            planned = {test['class'].rsplit('.', 1)[-1]
                       for test in json.loads(plan_path.read_text())['expected']}

        gated = {source.stem for source in (ROOT / 'e2e/src/test/java').rglob('*IT.java')
                 if GATE_CALL.search(source.read_text())}
        self.assertTrue(gated, 'No real-connector witnesses were discovered')
        missing = sorted(gated - planned)
        if missing:
            self.fail(f'{len(missing)} gated witnesses have no real-connector shard:\n'
                      + '\n'.join(missing))


if __name__ == '__main__':
    unittest.main()
