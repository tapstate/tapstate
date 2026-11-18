import json
from pathlib import Path
import subprocess
import tempfile
import unittest


JAVA_SOURCES = Path(__file__).resolve().parents[1] / "main/java/io/tapstate/benchmarks"
SCANNER = JAVA_SOURCES / "PerfCaseScanner.java"
ANNOTATION = JAVA_SOURCES / "PerfCase.java"


def declaration(case_id, *, profile="standard", annotation="PerfCase", extra=""):
    return f'''@{annotation}(
        id = "{case_id}", version = 1,
        description = "Build \\"orders\\" and details",
        group = "nest", operators = "nest",
        phase = "snapshot", mode = "throughput",
        profile = "{profile}", profileVersion = 1,
        source = "mongodb", target = "mongodb",
        environments = "single-node-op", workload = "finite-batch"{extra}
    )'''


class PerfCaseScannerTest(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)

    def write(self, name, content):
        path = self.root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")
        return path

    def scan(self):
        return subprocess.run(
            ["java", str(SCANNER), str(self.root)], text=True, capture_output=True, check=False
        )

    def assert_rejected(self, message):
        result = self.scan()
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertEqual(result.stdout, "")
        self.assertIn(message, result.stderr)

    def test_four_profiles_share_one_declaration_site_and_spec(self):
        profiles = ["small", "standard", "wide", "large"]
        cases = "\n".join(
            declaration(f"sample.snapshot.{profile}", profile=profile, extra=', spec = "settings.yml"')
            for profile in profiles
        )
        source = self.write(
            "sample/Sample.java",
            f"package sample;\nimport io.tapstate.benchmarks.PerfCase;\n{cases}\nclass Sample {{}}",
        )
        self.write("sample/settings.yml", "fixture: true\n")
        result = self.scan()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.scan().stdout, result.stdout)
        catalog = json.loads(result.stdout)
        self.assertEqual(catalog["schema_version"], 1)
        rows = catalog["cases"]
        self.assertEqual([row["id"] for row in rows], [f"sample.snapshot.{p}" for p in sorted(profiles)])
        self.assertEqual([row["profile"] for row in rows], sorted(profiles))
        self.assertEqual({row["spec"] for row in rows}, {"sample/settings.yml"})
        self.assertEqual({row["definition"] for row in rows}, {"sample/Sample.java"})
        self.assertEqual({row["description"] for row in rows}, {'Build "orders" and details'})
        self.assertTrue(all(row["operators"] == ["nest"] for row in rows))
        self.assertTrue(all(row["execution"] == "single-run" for row in rows))
        self.assertTrue(all(row["case_version"] == row["profile_version"] == 1 for row in rows))
        self.assertFalse(list(self.root.rglob("*.class")))
        compiled = subprocess.run(
            ["javac", "-proc:none", "-d", str(self.root / "classes"), str(ANNOTATION), str(source)],
            text=True, capture_output=True, check=False,
        )
        self.assertEqual(compiled.returncode, 0, compiled.stderr)

    def test_method_discovery_uses_only_source_syntax(self):
        qualified = declaration("sample.method.nest", annotation="io.tapstate.benchmarks.PerfCase")
        other = declaration("sample.ignored.comment", annotation="io.tapstate.benchmarks.PerfCase")
        self.write(
            "Sample.java",
            f'''import missing.Dependency;
            /* {other} */
            class Sample {{
                String example = "@PerfCase(id = \\"sample.ignored.string\\")";
                {qualified}
                void benchmark(Dependency missing) {{}}
            }}''',
        )
        result = self.scan()
        self.assertEqual(result.returncode, 0, result.stderr)
        rows = json.loads(result.stdout)["cases"]
        self.assertEqual([row["id"] for row in rows], ["sample.method.nest"])
        self.assertIsNone(rows[0]["spec"])

    def test_explicit_container_and_mixed_operator_labels(self):
        entry = declaration("sample.mixed.nest").replace(
            'group = "nest", operators = "nest"', 'group = "mixed", operators = {"map", "nest"}'
        )
        self.write(
            "Sample.java",
            f"import io.tapstate.benchmarks.PerfCase;\n@PerfCase.Cases({{ {entry} }}) class Sample {{}}",
        )
        result = self.scan()
        self.assertEqual(result.returncode, 0, result.stderr)
        row, = json.loads(result.stdout)["cases"]
        self.assertEqual(row["group"], "mixed")
        self.assertEqual(row["operators"], ["map", "nest"])

    def test_reports_duplicate_ids_from_two_sources(self):
        for name in ("First", "Second"):
            self.write(
                f"{name}.java",
                f'import io.tapstate.benchmarks.PerfCase;\n{declaration("sample.duplicate")} class {name} {{}}',
            )
        self.assert_rejected("Second.java:2: Duplicate case ID sample.duplicate (first declared at First.java:2)")

    def test_reports_missing_required_metadata(self):
        case = declaration("sample.missing").replace('phase = "snapshot", ', "")
        self.write("Sample.java", f"import io.tapstate.benchmarks.PerfCase;\n{case} class Sample {{}}")
        self.assert_rejected("Sample.java:2: phase must be a non-blank string literal")

    def test_reports_missing_referenced_spec(self):
        case = declaration("sample.missing-spec", extra=', spec = "missing.yml"')
        self.write("Sample.java", f"import io.tapstate.benchmarks.PerfCase;\n{case} class Sample {{}}")
        self.assert_rejected("Sample.java:2: Invalid spec path missing.yml")

    def test_rejects_spec_symlink_that_escapes_source_root(self):
        case = declaration("sample.external-spec", extra=', spec = "external.yml"')
        self.write("Sample.java", f"import io.tapstate.benchmarks.PerfCase;\n{case} class Sample {{}}")
        with tempfile.TemporaryDirectory() as outside:
            external = Path(outside) / "settings.yml"
            external.write_text("fixture: true\n", encoding="utf-8")
            (self.root / "external.yml").symlink_to(external)
            self.assert_rejected("Spec must be a readable file within the case source root")


if __name__ == "__main__":
    unittest.main()
