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


if __name__ == "__main__":
    unittest.main()
