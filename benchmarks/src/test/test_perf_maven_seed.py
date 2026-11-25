from argparse import Namespace
from io import StringIO
import json
from pathlib import Path
import sys
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import actions_maven
import actions_report
import catalog
import runner


ATTEMPT = {"run_id": 123, "attempt": 1}
SHA = "a" * 40
PREFIX = "perf-maven-v1-Linux-X64-" + "a" * 64 + "-"
CASE = "copy.snapshot.basic.standard"


def write_manifest(root):
    inputs = root / "inputs"
    inputs.mkdir()
    manifest = {
        "github_attempt": ATTEMPT,
        "identity": {"code_sha": SHA},
        "environment": "ubuntu-24.04-x64-op",
        "unallocated": [],
        "selected_ids": [CASE],
        "selected_cases": [{"id": CASE, "version": 1, "repeats": 3}],
        "batches": [{"index": 0, "entries": [{"case": {"id": CASE, "case_version": 1, "tags": []}, "repeats": 3}]}],
    }
    manifest["manifest_sha256"] = catalog.digest(manifest)
    runner.write_json(inputs / "manifest.json", manifest)
    runner.write_json(inputs / "run.json", {"github_attempt": ATTEMPT})
    return inputs, manifest


def maven_home(root):
    executable = root / "bin" / "mvn"
    executable.parent.mkdir(parents=True)
    executable.write_text("#!/bin/sh\n")
    executable.chmod(0o755)


class MavenSeedTest(unittest.TestCase):
    def test_store_digest_rejects_changed_missing_or_symbolic_dependency(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            maven_home(root / "maven-home")
            repository = root / "repository"
            repository.mkdir()
            dependency = repository / "plugin.jar"
            dependency.write_bytes(b"known-plugin")
            key = PREFIX + actions_maven.store_digest(root)
            self.assertEqual(key[-64:], actions_maven.verify_store(root, key))
            dependency.write_bytes(b"changed-plugin")
            with self.assertRaisesRegex(ValueError, "contents do not match"):
                actions_maven.verify_store(root, key)
            dependency.unlink()
            with self.assertRaisesRegex(ValueError, "repository is empty"):
                actions_maven.verify_store(root, key)
            dependency.symlink_to(root / "maven-home" / "bin" / "mvn")
            with self.assertRaisesRegex(ValueError, "Non-regular entry"):
                actions_maven.verify_store(root, key)

    def test_only_central_rate_limit_gets_bounded_retry(self):
        for first_line, expected in ((
                "Could not transfer artifact from/to central (https://repo.maven.apache.org): HTTP Status: 429", 2),
                ("Compilation failed", 1)):
            with self.subTest(first_line=first_line), TemporaryDirectory() as directory:
                evidence = Path(directory)
                logs = iter([first_line, "[INFO] Downloading from central: artifact"])
                receipt = {"refresh_attempts": 0, "central_download_requests": 0, "central_429_attempts": 0}

                def command(stop_at, argv, *, log, env):
                    value = next(logs)
                    log.write(value)
                    self.assertIn("-U", argv)
                    return 0 if "Downloading" in value else 1

                with patch.object(actions_maven, "within_budget", side_effect=command) as executed, \
                        patch.object(actions_maven.time, "time", return_value=0), \
                        patch.object(actions_maven.time, "sleep") as sleep:
                    if expected == 1:
                        with self.assertRaisesRegex(ValueError, "resolution failed"):
                            actions_maven.refresh("mvn", evidence, evidence, 90, receipt, {})
                        sleep.assert_not_called()
                    else:
                        actions_maven.refresh("mvn", evidence, evidence, 90, receipt, {})
                        sleep.assert_called_once_with(10)
                        self.assertEqual(1, receipt["central_download_requests"])
                        self.assertEqual(1, receipt["central_429_attempts"])
                    self.assertEqual(expected, executed.call_count)

    def test_failed_offline_witness_does_not_admit_a_cache(self):
        for offline_exit in (0, 1):
            with self.subTest(offline_exit=offline_exit), TemporaryDirectory() as directory:
                root = Path(directory)
                inputs, manifest = write_manifest(root)
                home = root / "wrapper"
                maven_home(home)
                evidence = root / "evidence"
                store = root / "store"
                output = root / "job-output"
                args = Namespace(evidence=evidence, inputs=inputs, store=store, legacy=root / "legacy",
                                 prefix=PREFIX, warm_key="", legacy_hit=False, stop_at=1000)
                env = {"GITHUB_SHA": SHA, "GITHUB_RUN_ID": "123", "GITHUB_RUN_ATTEMPT": "1",
                       "MAVEN_HOME": str(home), "GITHUB_OUTPUT": str(output)}
                seen = []

                def command(stop_at, argv, *, log=None, env=None):
                    seen.append(argv)
                    self.assertEqual("maven-home", Path(env["MAVEN_HOME"]).name)
                    repository = next(word.partition("=")[2] for word in argv if word.startswith("-Dmaven.repo.local="))
                    if "-U" in argv:
                        (Path(repository) / "plugin.jar").write_bytes(b"plugin")
                        (Path(repository) / "resolver-status.properties").write_text("transport attempt")
                        return 0
                    return offline_exit

                with patch.dict(actions_maven.os.environ, env), \
                        patch.object(actions_maven, "within_budget", side_effect=command), \
                        patch("sys.stdout", new_callable=StringIO):
                    if offline_exit:
                        with self.assertRaisesRegex(ValueError, "failed offline"):
                            actions_maven.prepare(args)
                    else:
                        actions_maven.prepare(args)
                self.assertEqual(2, len(seen))
                self.assertIn("-U", seen[0])
                self.assertIn("-o", seen[1])
                self.assertIn("-Dtest=BenchmarkRunIT", seen[1])
                receipt = json.loads((evidence / "receipt.json").read_bytes())
                self.assertEqual(manifest["manifest_sha256"], receipt["manifest_sha256"])
                self.assertEqual("failed" if offline_exit else "prepared", receipt["status"])
                if offline_exit:
                    self.assertFalse(output.exists())
                    self.assertFalse(store.exists())
                else:
                    self.assertEqual(receipt["sha256"], actions_maven.verify_store(store, receipt["cache_key"]))
                    self.assertIn(receipt["cache_key"], output.read_text())
                    self.assertFalse((store / "repository" / "resolver-status.properties").exists())

    def test_seed_failure_reports_every_repeat_and_its_bound_cause(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            inputs, manifest = write_manifest(root)
            artifact = root / "performance-maven-seed-1"
            artifact.mkdir()
            receipt = {"github_attempt": ATTEMPT, "manifest_sha256": manifest["manifest_sha256"],
                       "code_sha": SHA, "status": "failed", "reason": "Maven Central returned HTTP 429"}
            runner.write_json(artifact / "receipt.json", receipt)
            jobs = {"prepare": "success", "maven-seed": "failure", "measure": "skipped"}
            result = actions_report.collect(inputs, root, ATTEMPT, jobs)
            self.assertEqual({"not-run": 3}, result["repeat_counts"])
            self.assertEqual({"not-run": 1}, result["case_counts"])
            self.assertEqual({"maven-seed"}, {row["last_stage"] for row in result["results"]})
            self.assertEqual({"Maven preparation failed (Maven Central returned HTTP 429)"},
                             {row["reason"] for row in result["results"]})
            self.assertIn("Maven Central returned HTTP 429", actions_report.summary(result, "example/example"))
            receipt["github_attempt"] = {"run_id": 123, "attempt": 2}
            runner.write_json(artifact / "receipt.json", receipt)
            wrong = actions_report.collect(inputs, root, ATTEMPT, jobs)
            self.assertEqual({"not-run": 3}, wrong["repeat_counts"])
            self.assertNotIn("HTTP 429", wrong["results"][0]["reason"])

    def test_seed_rate_limit_near_deadline_is_retained_without_retry(self):
        with TemporaryDirectory() as directory:
            folder = Path(directory)
            receipt = {"refresh_attempts": 0, "central_download_requests": 0, "central_429_attempts": 0}

            def rate_limited(stop_at, argv, *, log, env):
                log.write("[ERROR] from/to central (https://repo.maven.apache.org): HTTP Status: 429")
                return 1

            with patch.object(actions_maven, "within_budget", side_effect=rate_limited) as command, \
                    patch.object(actions_maven.time, "time", return_value=100), \
                    patch.object(actions_maven.time, "sleep") as sleep:
                with self.assertRaisesRegex(ValueError, "HTTP 429"):
                    actions_maven.refresh("mvn", folder, folder, 105, receipt, {})
                command.assert_called_once()
                sleep.assert_not_called()
                self.assertEqual(1, receipt["central_429_attempts"])

    def test_missing_batch_cache_is_reported_as_not_run(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            inputs, manifest = write_manifest(root)
            artifact = root / "performance-maven-seed-1"
            artifact.mkdir()
            receipt = {"github_attempt": ATTEMPT, "manifest_sha256": manifest["manifest_sha256"],
                       "code_sha": SHA, "status": "prepared", "cache_key": PREFIX + "b" * 64, "sha256": "b" * 64}
            runner.write_json(artifact / "receipt.json", receipt)
            results = {"prepare": "success", "maven-seed": "success", "measure": "failure"}
            jobs = [{"name": "measure (0)", "conclusion": "failure",
                     "failed_steps": ["Restore the selected Maven cache"]}]
            report = actions_report.collect(inputs, root, ATTEMPT, results, jobs=jobs)
            self.assertEqual({"not-run": 3}, report["repeat_counts"])
            self.assertEqual({"maven-cache"}, {row["last_stage"] for row in report["results"]})
            self.assertIn("Restore the selected Maven cache", report["results"][0]["reason"])
