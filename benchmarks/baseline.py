"""Read a benchmark baseline without suppressing measurement and coverage reports."""

from datetime import date
import hashlib
import json
import math
from pathlib import PurePosixPath
import re
from statistics import median
from urllib.error import HTTPError
from urllib.parse import quote
from urllib.request import urlopen


REPOSITORY = "tapstate/tapstate-performance"
RELEASE_RECORD = "baselines/releases.json"
PUBLISHED_FIELDS = ("release_record_commit", "baseline_commit", "id", "version", "environment", "code_sha", "sha256")
CASE_CONDITIONS = ("case_version", "definition_sha256", "mode", "scale", "effective_load",
                   "connector_sha256", "environment")
SOURCE_SCHEMA = 2
SOURCE_METRIC_BINDING = ("conditions", "scope", "source", "unit", "window_ref")
INCOMPLETE_STATUSES = ("failed", "measurement-incomplete", "invalid", "timed-out",
                       "not-run", "out-of-memory", "skipped")


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
        version = baseline.get("schema_version", 1)
        if type(version) is not int or version not in (1, SOURCE_SCHEMA):
            raise ValueError("unsupported-baseline-schema")
        if not valid_cases(baseline, published="id" in details):
            raise ValueError("invalid-baseline-case")
        if "comparison_rules" in baseline and not valid_rules(baseline["comparison_rules"]):
            raise ValueError("invalid-baseline-comparison-rules")
        if "memory_limits" in baseline:
            valid_limits = (valid_source_memory_limits(baseline) if version == SOURCE_SCHEMA
                            else valid_memory_limits(baseline["memory_limits"]))
            if not valid_limits:
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
    if baseline.get("schema_version") == SOURCE_SCHEMA:
        environment = baseline["environment"]
        return all(valid_source_case(key, row, environment) for key, row in baseline["cases"].items())
    environment = baseline["environment"] if published else None
    return all(valid_case(row, environment=environment)
               and (not published or (row.get("case_id", key) == key
                                     and row.get("code_sha", baseline.get("code_sha")) == baseline.get("code_sha")))
               for key, row in baseline["cases"].items())


def valid_source_case(case_id, row, environment):
    if (not nonblank(case_id) or not isinstance(row, dict) or row.get("case_id") != case_id
            or not positive_int(row.get("case_version"))):
        return False
    if row.get("status") == "completed":
        current = row.get("current")
        first = row.get("first")
        return (valid_reference(current, environment) and current["conditions"]["case_version"] == row["case_version"]
                and (first == "current" or valid_reference(first, environment)))
    return (row.get("status") in INCOMPLETE_STATUSES and nonblank(row.get("reason"))
            and valid_measurement(row.get("measurement"))
            and isinstance(row.get("evidence"), list) and bool(row["evidence"])
            and all(valid_evidence_path(path) for path in row["evidence"])
            and not {"current", "first", "arms", "metrics"}.intersection(row))


def valid_reference(reference, environment):
    if not isinstance(reference, dict) or not valid_measurement(reference.get("measurement")):
        return False
    conditions = reference.get("conditions")
    if (not isinstance(conditions, dict) or not valid_completed_case(conditions, environment)
            or not digest_matches(conditions.get("definition_sha256"), 64)
            or not digest_matches(conditions.get("connector_sha256"), 64)):
        return False
    runner = conditions.get("runner")
    if (not isinstance(runner, dict) or not nonblank(runner.get("cpu"))
            or not nonblank(runner.get("runner_image")) or not isinstance(runner.get("mongo_images"), list)
            or any(not nonblank(image) for image in runner["mongo_images"])):
        return False
    arms = reference.get("arms")
    return (isinstance(arms, dict) and bool(arms)
            and all(nonblank(name) and valid_metric_groups(arm) for name, arm in arms.items()))


def valid_metric_groups(arm):
    if not isinstance(arm, dict) or not isinstance(arm.get("metrics"), dict) or not arm["metrics"]:
        return False
    for metric, groups in arm["metrics"].items():
        if not nonblank(metric) or not isinstance(groups, list) or not groups:
            return False
        if not all(valid_metric_group(metric, group) for group in groups):
            return False
        measured = [group for group in groups if group["availability"] == "measured"]
        signatures = [{k: group[k] for k in SOURCE_METRIC_BINDING} for group in measured]
        if any(group == prior for index, group in enumerate(signatures) for prior in signatures[:index]):
            return False
    return True


def valid_metric_group(metric, group):
    if not isinstance(group, dict):
        return False
    if group.get("availability") in ("unavailable", "not_applicable"):
        return nonblank(group.get("reason")) and not {"samples", "summary", "value"}.intersection(group)
    if (group.get("availability") != "measured"
            or not all(nonblank(group.get(field)) for field in ("unit", "scope", "source", "window_ref"))
            or not isinstance(group.get("conditions"), dict) or not isinstance(group.get("samples"), list)
            or not group["samples"]):
        return False
    expected_unit = metric_unit(metric)
    if expected_unit and group["unit"] != expected_unit:
        return False
    repeats = set()
    values = []
    for sample in group["samples"]:
        if (not isinstance(sample, dict) or not positive_int(sample.get("repeat")) or sample["repeat"] in repeats
                or not valid_evidence_path(sample.get("evidence")) or not finite_number(sample.get("value"))
                or (expected_unit and not metric.endswith("_bytes_per_hour") and sample["value"] < 0)):
            return False
        repeats.add(sample["repeat"])
        values.append(sample["value"])
    minimum, maximum = min(values), max(values)
    summary = {"count": len(values), "median": median(values), "min": minimum,
               "max": maximum, "range": maximum - minimum}
    return all(finite_number(value) for value in summary.values()) and group.get("summary") == summary


def metric_unit(metric):
    units = (("_bytes_per_hour", "B/h"), ("_bytes", "B"), ("_ms", "ms"))
    return next((unit for ending, unit in units if metric.endswith(ending)), None)


def valid_source_memory_limits(baseline):
    limits = baseline["memory_limits"]
    if not isinstance(limits, dict):
        return False
    for case_id, arms in limits.items():
        case = baseline["cases"].get(case_id)
        if not case or case["status"] != "completed" or not isinstance(arms, dict):
            return False
        reference = case["current"]["arms"]
        for arm, metrics in arms.items():
            if arm not in reference or not isinstance(metrics, dict):
                return False
            for metric, budgets in metrics.items():
                if metric_unit(metric) not in ("B", "B/h") or not isinstance(budgets, list) or not budgets:
                    return False
                source_groups = reference[arm]["metrics"].get(metric, ())
                bindings = []
                for budget in budgets:
                    if (not isinstance(budget, dict) or not positive_int(budget.get("budget_version"))
                            or not finite_number(budget.get("limit")) or budget["limit"] <= 0
                            or not all(key in budget for key in SOURCE_METRIC_BINDING)):
                        return False
                    binding = {key: budget[key] for key in SOURCE_METRIC_BINDING}
                    if binding in bindings or not any(
                            group["availability"] == "measured" and all(group[key] == value for key, value in binding.items())
                            for group in source_groups):
                        return False
                    bindings.append(binding)
    return True


def valid_measurement(measurement):
    if (not isinstance(measurement, dict) or not positive_int(measurement.get("run_id"))
            or not positive_int(measurement.get("attempt")) or not digest_matches(measurement.get("code_sha"), 40)
            or not digest_matches(measurement.get("manifest_sha256"), 64)
            or not nonblank(measurement.get("measured_on"))):
        return False
    try:
        stamp = measurement["measured_on"]
        return date.fromisoformat(stamp).isoformat() == stamp
    except ValueError:
        return False


def valid_evidence_path(value):
    if not nonblank(value) or any(character in value for character in ("\\", "?", "#", ":")):
        return False
    parts = value.split("/")
    return (len(parts) > 4 and parts[0] == "baselines" and parts[3] == "evidence"
            and all(part not in ("", ".", "..") for part in parts))


def digest_matches(value, length):
    return isinstance(value, str) and re.fullmatch(rf"[0-9a-f]{{{length}}}", value) is not None


def positive_int(value):
    return type(value) is int and value > 0


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
            if "zero_reference" in rule:
                absolute = rule["zero_reference"]
                if (not isinstance(absolute, dict)
                        or not all(finite_number(absolute.get(key)) for key in ("observe_absolute", "suspect_absolute"))
                        or not 0 < absolute["observe_absolute"] < absolute["suspect_absolute"]):
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
