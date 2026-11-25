"""Positive and adversarial checks for independent historical contracts."""
import copy
import importlib.util
from pathlib import Path
import unittest
import subprocess

spec = importlib.util.spec_from_file_location("error_contract", Path(__file__).with_name("error-contract.py"))
contract = importlib.util.module_from_spec(spec)
spec.loader.exec_module(contract)


class HistoricalContracts(unittest.TestCase):
    def setUp(self):
        self.previous = {"TS0210001": dict(owner="Owner#REFUSED", severity="ERROR",
                                         parameters=["reason"], lifecycle="active")}

    def test_additions_and_deprecation_keep_the_published_contract(self):
        current = copy.deepcopy(self.previous)
        current["TS0210001"]["lifecycle"] = "deprecated"
        current["TS0219999"] = dict(owner="Owner#NEW", severity="WARNING", parameters=[], lifecycle="active")
        contract.compare(self.previous, current)

    def test_synchronized_current_golden_cannot_authorize_a_rename(self):
        current = {"TS0219999": self.previous["TS0210001"]}
        with self.assertRaisesRegex(ValueError, "removed published identity"):
            contract.compare(self.previous, current)

    def test_parameter_severity_and_ownership_changes_are_independently_refused(self):
        for key, value in [("parameters", ["other"]), ("severity", "WARNING"), ("owner", "Other#REFUSED")]:
            with self.subTest(key=key):
                current = copy.deepcopy(self.previous)
                current["TS0210001"][key] = value
                with self.assertRaisesRegex(ValueError, "changed " + key):
                    contract.compare(self.previous, current)

    def test_deprecated_identities_cannot_be_recycled(self):
        previous = copy.deepcopy(self.previous)
        previous["TS0210001"]["lifecycle"] = "deprecated"
        with self.assertRaisesRegex(ValueError, "reactivated"):
            contract.compare(previous, self.previous)

    def test_an_unavailable_history_commit_is_not_an_empty_baseline(self):
        with self.assertRaises(subprocess.CalledProcessError):
            contract.check("0" * 40, self.previous)

    def test_typed_source_selection_accepts_legal_layout_and_qualified_interfaces(self):
        for declaration in ["enum Error implements TapstateErrorCode {", "enum Error implements\n TapstateErrorCode {",
                            "enum Error implements io.tapstate.core.common.TapstateErrorCode {",
                            "enum Error implements java.io.Serializable, TapstateErrorCode {"]:
            self.assertTrue(contract.is_typed_enum(declaration))
        self.assertFalse(contract.is_typed_enum("class Adapter { TapstateErrorCode code; }"))

    def test_an_ambiguous_branch_name_is_not_a_fixed_history_commit(self):
        with self.assertRaisesRegex(ValueError, "complete independently selected"):
            contract.check("main", self.previous)


if __name__ == "__main__":
    unittest.main()
