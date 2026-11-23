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
PUBLISHED_FIELDS = ("release_record_commit", "baseline_commit", "id", "version", "environment", "code_sha", "sha256")
CASE_CONDITIONS = ("case_version", "definition_sha256", "mode", "scale", "effective_load",
                   "connector_sha256", "environment")


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
        if (not isinstance(published, dict) or type(published.get("schema_version")) is not int
                or published["schema_version"] != 1 or not isinstance(published.get("versions"), dict)
                or not isinstance(published.get("active"), dict)):
            raise ValueError("invalid-baseline-release-record")
        version = requested or published["active"].get(environment)
        if version is None:
            return {**details, "status": "unavailable", "reason": "baseline-not-published-for-environment"}
        if not isinstance(version, str) or not version:
            raise ValueError("invalid-baseline-release-record")
        details["version"] = version
        selected = published["versions"].get(version)
        if not isinstance(selected, dict) or selected.get("environment") != environment:
            raise ValueError("baseline-version-unavailable-for-environment")
        path = PurePosixPath(selected["path"])
        if path.is_absolute() or ".." in path.parts or path.parts[:1] != ("baselines",):
            raise ValueError("invalid-published-baseline-path")
        if not isinstance(selected.get("id"), str) or not selected["id"]:
            raise ValueError("invalid-baseline-release-record")
        details.update(id=selected["id"], environment=environment,
                       code_sha=sha(selected["code_sha"], 40), baseline_commit=sha(selected["commit"], 40),
                       sha256=sha(selected["sha256"], 64))
        payload = directory / "baseline.json"
        payload.write_bytes(fetch(f"{raw}/{details['baseline_commit']}/{quote(str(path), safe='/')}"))
        _, receipt = load(payload, identity=details)
        return receipt
    except OSError:
        reason = "baseline-unreadable" if "baseline_commit" in details else "published-baseline-unreadable"
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


def load_pinned(path, receipt, environment):
    if (not all(isinstance(receipt.get(key), str) and receipt[key] for key in PUBLISHED_FIELDS)
            or receipt["environment"] != environment
            or any(not re.fullmatch(r"[0-9a-f]{40}", receipt[key])
                   for key in ("release_record_commit", "baseline_commit", "code_sha"))
            or not re.fullmatch(r"[0-9a-f]{64}", receipt["sha256"])):
        return None, {**receipt, "status": "unavailable", "reason": "invalid-baseline-receipt"}
    return load(path, identity=receipt)


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
        if not valid_cases(baseline, published="id" in details):
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


def valid_cases(baseline, *, published):
    environment = baseline["environment"] if published else None
    return all(valid_case(row, environment=environment)
               and (not published or (row.get("case_id", key) == key
                                     and row.get("code_sha", baseline.get("code_sha")) == baseline.get("code_sha")))
               for key, row in baseline["cases"].items())


def valid_case(row, *, environment=None):
    if not isinstance(row, dict):
        return False
    if environment and (row.get("status") not in ("completed", "failed", "measurement-incomplete",
                                                  "invalid", "timed-out", "not-run", "out-of-memory", "skipped")
                      or (row["status"] == "completed" and not valid_completed_case(row, environment))):
        return False
    arms = row.get("paired_arms", {"single": row})
    if not isinstance(arms, dict) or not arms:
        return False
    for arm in arms.values():
        if not isinstance(arm, dict) or any(
            field in arm and not isinstance(arm[field], dict)
            for field in ("phase_nanos", "live_latency", "resources")
        ):
            return False
        resources = arm.get("resources", {})
        cycles = arm.get("lifecycle_cycles", [])
        if environment and row["status"] == "completed":
            if arm.get("status") != "completed" or arm.get("environment_profile") != environment:
                return False
            identity = ("case_id", "code_sha", "run_id", "repeat", *CASE_CONDITIONS)
            if any(field in arm and arm[field] != row.get(field) for field in identity):
                return False
        latency = arm.get("live_latency", {})
        phases = arm.get("phase_nanos", {})
        if not nonnegative_metrics(phases, ("completion_observed",)):
            return False
        if not nonnegative_metrics(latency, ("p50Nanos", "p95Nanos", "p99Nanos")):
            return False
        if latency.get("status") == "eligible" and (
            not nonblank(latency.get("source")) or not finite_number(latency.get("p95Nanos"))
        ):
            return False
        if not nonnegative_metrics(resources, ("steady_rss_growth_kib_per_second",), allow_negative=True):
            return False
        if not isinstance(cycles, list) or any(
            not isinstance(cycle, dict) or type(cycle.get("cycle")) is not int
            or type(cycle.get("idle_seconds")) is not int for cycle in cycles
        ):
            return False
        if resources.get("status") == "observed":
            windows = resources.get("windows")
            if not isinstance(windows, dict) or not all(valid_window(window) for window in windows.values()):
                return False
    return True


def nonblank(value):
    return isinstance(value, str) and bool(value.strip())


def nonnegative_metrics(observations, fields, *, allow_negative=False):
    return all(value is None or finite_number(value) and (allow_negative or value >= 0)
               for value in (observations.get(field) for field in fields))


def valid_window(window):
    if (not isinstance(window, dict) or type(window.get("covered")) is not bool
            or not nonnegative_metrics(window, ("rss_peak_kib", "rss_median_kib", "samples", "coverage"))):
        return False
    return not window["covered"] or (nonblank(window.get("source")) and finite_number(window.get("rss_peak_kib")))


def valid_completed_case(row, environment):
    strings = ("definition_sha256", "mode", "scale", "connector_sha256")
    return (row.get("environment_profile") == environment
            and type(row.get("case_version")) is int and row["case_version"] > 0
            and all(nonblank(row.get(field)) for field in strings)
            and all(isinstance(row.get(field), dict) and row[field] for field in ("effective_load", "environment")))


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
                if not finite_number(value) or value < 0:
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
                    or not nonblank(policy.get("source"))
                    or type(policy.get("limit_kib")) is not int or policy["limit_kib"] < 1):
                return False
    return True


def finite_number(value):
    # Values must be representable in the report's floating-point calculations.
    return (type(value) is int and value.bit_length() <= 1023
            or type(value) is float and math.isfinite(value))
