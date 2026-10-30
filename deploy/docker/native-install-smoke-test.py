#!/usr/bin/env python3
"""Lost Docker replies cannot strand or remove someone else's installation helper."""

import importlib.util
from pathlib import Path
import subprocess
import unittest
from unittest.mock import patch

SPEC = importlib.util.spec_from_file_location("native_install_smoke", Path(__file__).with_name("native-install-smoke.py"))
SMOKE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(SMOKE)


class Docker:
    def __init__(self, timeout_at=None, foreign_owner=False):
        self.timeout_at = timeout_at
        self.foreign_owner = foreign_owner
        self.identity = "a" * 64
        self.owner = None
        self.removed = []

    def run(self, args, **kwargs):
        action = args[1]
        if action == "create":
            if "--label" in args:
                self.owner = args[args.index("--label") + 1].split("=", 1)[1]
            if self.timeout_at == "create":
                raise subprocess.TimeoutExpired(args, 180)
            output = self.identity
        elif action == "start":
            if self.timeout_at == "start":
                raise subprocess.TimeoutExpired(args, 180)
            output = ""
        elif action == "inspect":
            if self.timeout_at == "inspect":
                raise subprocess.TimeoutExpired(args, 30)
            output = ("foreign" if self.foreign_owner else self.owner) + " " + self.identity
        elif action == "rm":
            self.removed.append(args[-1])
            output = ""
        else:
            raise AssertionError("unexpected Docker operation")
        return subprocess.CompletedProcess(args, 0, output.encode(), b"")


class Ownership(unittest.TestCase):
    def test_a_lost_create_reply_still_removes_its_own_container(self):
        docker = Docker(timeout_at="create")
        with patch.object(SMOKE.subprocess, "run", docker.run):
            with self.assertRaises(subprocess.TimeoutExpired):
                SMOKE.owned_container("fixture-image")
        self.assertEqual(docker.removed, [docker.identity])

    def test_a_timed_out_start_cleans_the_exact_owned_identity(self):
        docker = Docker(timeout_at="start")
        with patch.object(SMOKE.subprocess, "run", docker.run):
            with self.assertRaises(subprocess.TimeoutExpired):
                SMOKE.owned_container("fixture-image")
        self.assertEqual(docker.removed, [docker.identity])

    def test_a_foreign_owner_is_never_deleted(self):
        docker = Docker(foreign_owner=True)
        with patch.object(SMOKE.subprocess, "run", docker.run):
            with self.assertRaisesRegex(RuntimeError, "unowned"):
                SMOKE.owned_container("fixture-image")
        self.assertEqual(docker.removed, [])

    def test_unknown_cleanup_names_the_owned_container_without_claiming_removal(self):
        docker = Docker(timeout_at="inspect")
        with patch.object(SMOKE.subprocess, "run", docker.run):
            with self.assertRaisesRegex(RuntimeError, "could not verify.*inspect tapstate-native-install-"):
                SMOKE.owned_container("fixture-image")
        self.assertEqual(docker.removed, [])


class HttpDiagnostics(unittest.TestCase):
    def test_only_phase_status_and_a_catalog_code_leave_a_failed_response(self):
        response = b'{"code":"control.unauthenticated","message":"private-token","params":{"password":"private-password"}}\n401'
        with patch.object(SMOKE, "command", return_value=response):
            with self.assertRaisesRegex(RuntimeError, "test-CACHE failed.*HTTP 401, code control.unauthenticated") as failure:
                SMOKE.request("fixture", "/api/storage/test", label="test-CACHE")
        self.assertNotIn("private", str(failure.exception))

    def test_unknown_codes_and_non_json_responses_never_leave_the_body(self):
        for response in (b'{"code":"private-token"}\n500', b'private-token\n502'):
            with self.subTest(response=response), patch.object(SMOKE, "command", return_value=response):
                with self.assertRaisesRegex(RuntimeError, "code unavailable") as failure:
                    SMOKE.request("fixture", "/api/storage/setup/commit")
                self.assertNotIn("private", str(failure.exception))

    def test_success_keeps_the_original_json_and_does_not_retry_the_request(self):
        with patch.object(SMOKE, "command", return_value=b'{"phase":"COMPLETE"}\n200') as called:
            self.assertEqual(SMOKE.request("fixture", "/installation/first-admin"), {"phase": "COMPLETE"})
        called.assert_called_once()

    def test_transport_failure_keeps_the_phase_and_hides_arguments_and_output(self):
        failed = subprocess.CompletedProcess([], 7, b"private-token", b"private-password")
        with patch.object(SMOKE.subprocess, "run", return_value=failed):
            with self.assertRaisesRegex(RuntimeError, "test-MASTER failed.*exit 7") as failure:
                SMOKE.request("fixture", "/api/storage/test", label="test-MASTER")
        self.assertNotIn("private", str(failure.exception))

    def test_request_exceptions_cannot_expose_the_capability_or_captured_output(self):
        errors = (subprocess.TimeoutExpired(["curl", "private-token"], 70, output=b"private-password"),
                OSError("private-token"))
        for error in errors:
            with self.subTest(error=type(error).__name__), patch.object(SMOKE, "command", side_effect=error):
                with self.assertRaisesRegex(RuntimeError, "test-CACHE") as failure:
                    SMOKE.request("fixture", "/api/storage/test", label="test-CACHE")
                self.assertNotIn("private", str(failure.exception))
                self.assertTrue(failure.exception.__suppress_context__)


if __name__ == "__main__":
    unittest.main()
