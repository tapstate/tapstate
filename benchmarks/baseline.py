"""Read a benchmark baseline without suppressing measurement and coverage reports."""

import hashlib
import json
import math
from pathlib import PurePosixPath
import re
from urllib.error import HTTPError
from urllib.parse import quote
from urllib.request import urlopen


REPOSITORY = "tapstate/tapstate-performance"
RELEASE_RECORD = "baselines/releases.json"


def get_bytes(url):
    with urlopen(url, timeout=20) as response:
        body = response.read(25_000_001)
        if len(body) > 25_000_000:
            raise ValueError("published-baseline-size-limit")
        return body


def pin(directory, environment, requested=None, *, fetch=get_bytes):
    """Resolve one released identity and save the checked content alongside the receipt."""
    api = f"https://api.github.com/repos/{REPOSITORY}"
    raw = f"https://raw.githubusercontent.com/{REPOSITORY}"
    details = {}
    try:
        record_commit = sha(json.loads(fetch(f"{api}/commits/main"))["sha"], 40)
        details["release_record_commit"] = record_commit
        try:
            published = json.loads(fetch(f"{raw}/{record_commit}/{RELEASE_RECORD}"))
        except HTTPError as error:
            if error.code == 404:
                return {**details, "status": "unavailable", "reason": "baseline-not-published"}
            raise
        if published.get("schema_version") != 1 or not isinstance(published.get("versions"), dict):
            raise ValueError("invalid-baseline-release-record")
        version = requested or published["active"].get(environment)
        if version is None:
            return {**details, "status": "unavailable", "reason": "baseline-not-published-for-environment"}
        details["version"] = version
        selected = published["versions"].get(version)
        if not isinstance(selected, dict) or selected.get("environment") != environment:
            raise ValueError("baseline-version-unavailable-for-environment")
        path = PurePosixPath(selected["path"])
        if path.is_absolute() or ".." in path.parts or path.parts[:1] != ("baselines",):
            raise ValueError("invalid-published-baseline-path")
        details.update(id=selected["id"], environment=environment,
                       code_sha=sha(selected["code_sha"], 40), baseline_commit=sha(selected["commit"], 40),
                       sha256=sha(selected["sha256"], 64))
        payload = directory / "baseline.json"
        payload.write_bytes(fetch(f"{raw}/{details['baseline_commit']}/{quote(str(path), safe='/')}"))
        _, receipt = load(payload, identity=details)
        return receipt
    except OSError:
        reason = "published-baseline-unreadable"
    except (json.JSONDecodeError, UnicodeError):
        reason = "published-baseline-invalid-json"
    except (KeyError, TypeError, AttributeError):
        reason = "invalid-baseline-release-record"
    except ValueError as error:
        reason = str(error)
    return {**details, "status": "unavailable", "reason": reason}


def sha(value, length):
    if not isinstance(value, str) or not re.fullmatch(rf"[0-9a-f]{{{length}}}", value):
        raise ValueError("invalid-published-baseline-digest")
    return value


def load(path, *, identity=None):
    if path is None:
        return None, {"status": "unavailable", "reason": "baseline-not-supplied"}
    details = dict(identity or {})
    try:
        contents = path.read_bytes()
        checksum = hashlib.sha256(contents).hexdigest()
        if "sha256" in details and checksum != details["sha256"]:
            raise ValueError("baseline-digest-mismatch")
        baseline = json.loads(contents)
        if (not isinstance(baseline, dict) or not isinstance(baseline.get("id"), str)
                or not baseline["id"] or baseline.get("kind") not in ("fixture", "accepted")
                or not isinstance(baseline.get("environment"), str) or not baseline["environment"]
                or not isinstance(baseline.get("cases"), dict)):
            raise ValueError("invalid-baseline-schema")
        if any(not valid_case(row) for row in baseline["cases"].values()):
            raise ValueError("invalid-baseline-case")
        if "comparison_rules" in baseline and not valid_rules(baseline["comparison_rules"]):
            raise ValueError("invalid-baseline-comparison-rules")
        if "memory_limits" in baseline and not valid_memory_limits(baseline["memory_limits"]):
            raise ValueError("invalid-baseline-memory-limits")
        if "id" in details and (baseline["kind"] != "accepted" or baseline["id"] != details["id"]
                                or baseline.get("version") != details.get("version")
                                or baseline["environment"] != details.get("environment")
                                or baseline.get("code_sha") != details.get("code_sha")):
            raise ValueError("baseline-identity-mismatch")
        return baseline, {**details, "status": "available", "id": baseline["id"],
                          "kind": baseline["kind"], "sha256": checksum}
    except OSError:
        reason = "baseline-unreadable"
    except (json.JSONDecodeError, UnicodeError):
        reason = "baseline-invalid-json"
    except ValueError as error:
        reason = str(error)
    return None, {**details, "status": "unavailable", "reason": reason}


def valid_case(row):
    if not isinstance(row, dict):
        return False
    arms = row.get("paired_arms", {"single": row})
    if not isinstance(arms, dict):
        return False
    for arm in arms.values():
        if not isinstance(arm, dict) or any(
            field in arm and not isinstance(arm[field], dict)
            for field in ("phase_nanos", "live_latency", "resources")
        ):
            return False
        resources = arm.get("resources", {})
        cycles = arm.get("lifecycle_cycles", [])
        if not isinstance(cycles, list) or any(
            not isinstance(cycle, dict) or type(cycle.get("cycle")) is not int
            or type(cycle.get("idle_seconds")) is not int for cycle in cycles
        ):
            return False
        if resources.get("status") == "observed":
            windows = resources.get("windows")
            if not isinstance(windows, dict) or any(not isinstance(window, dict) for window in windows.values()):
                return False
    return True


def valid_rules(policy):
    if (not isinstance(policy, dict) or type(policy.get("version")) is not int
            or policy["version"] < 1 or not isinstance(policy.get("cases"), dict)):
        return False
    for metrics in policy["cases"].values():
        if not isinstance(metrics, dict):
            return False
        for rule in metrics.values():
            if (not isinstance(rule, dict) or rule.get("worse") not in ("higher", "lower")
                    or type(rule.get("minimum_repeats")) is not int or rule["minimum_repeats"] < 2):
                return False
            for key in ("observe_ratio", "suspect_ratio", "minimum_absolute", "baseline_noise"):
                value = rule.get(key)
                if type(value) not in (int, float) or not math.isfinite(value) or value < 0:
                    return False
            if rule["observe_ratio"] >= rule["suspect_ratio"]:
                return False
    return True


def valid_memory_limits(policies):
    if not isinstance(policies, dict):
        return False
    for metrics in policies.values():
        if not isinstance(metrics, dict):
            return False
        for name, policy in metrics.items():
            if (not name.endswith(".rss_peak_kib") or not isinstance(policy, dict)
                    or not isinstance(policy.get("source"), str) or not policy["source"]
                    or type(policy.get("limit_kib")) is not int or policy["limit_kib"] < 1):
                return False
    return True
