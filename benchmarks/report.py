"""Reconcile batch results and compare compatible benchmark observations."""

import math
from statistics import median

import catalog
import verdict

CASE_CONDITIONS = ("case_version", "definition_sha256", "mode", "scale", "effective_load",
                   "connector_sha256", "environment")
ARM_CONDITIONS = ("effective_input_rows", "mongo_topology", "target_delay_millis", "database_conditions")


def reconcile(manifest, summaries, baseline=None, *, baseline_status=None):
    if manifest["manifest_sha256"] != catalog.digest(
        {key: value for key, value in manifest.items() if key != "manifest_sha256"}
    ):
        raise ValueError("Manifest digest does not match its contents")
    entries = [entry for batch in manifest["batches"] for entry in batch["entries"]]
    ids = [entry["case"]["id"] for entry in entries]
    if manifest["unallocated"] or len(ids) != len(set(ids)) or sorted(ids) != sorted(manifest["selected_ids"]):
        raise ValueError("Reconciliation requires a complete allocation")
    expected = {
        (entry["case"]["id"], repeat): entry
        for entry in entries for repeat in range(1, entry["repeats"] + 1)
    }
    rows = {}
    errors = []
    for summary in summaries:
        batch = next((b for b in manifest["batches"] if b["index"] == summary.get("batch")), None)
        if (summary.get("identity") != manifest["identity"] or
                summary.get("manifest_sha256") != manifest["manifest_sha256"] or batch is None or
                summary.get("github_attempt") != manifest.get("github_attempt")):
            errors.append({"run_id": summary.get("run_id"), "reason": "summary binding mismatch"})
            continue
        allowed = {entry["case"]["id"] for entry in batch["entries"]}
        if summary.get("process_exit") != 0:
            errors.append({"run_id": summary["run_id"], "reason": "runner exited unsuccessfully",
                           "process_exit": summary.get("process_exit")})
        for row in summary["results"]:
            key = row.get("case_id"), row.get("repeat")
            if key not in expected or key[0] not in allowed:
                errors.append({"case_id": key[0], "repeat": key[1], "reason": "unexpected or out-of-batch repeat"})
                continue
            if key in rows:
                errors.append({"case_id": key[0], "repeat": key[1], "reason": "unexpected or duplicate repeat"})
                rows[key] = {"case_id": key[0], "repeat": key[1], "status": "invalid",
                             "reason": "duplicate repeat"}
                continue
            entry = expected[key]
            case = entry["case"]
            effective = {name: value for name, value in entry.items() if name not in {"case", "repeats"}}
            if (row.get("case_version") != case["case_version"] or row.get("run_id") != summary["run_id"] or
                    (row.get("status") == "completed" and
                     (row.get("definition_sha256") != case["definition_sha256"] or
                      row.get("effective_load") != effective or
                      row.get("code_sha") != manifest["identity"]["code_sha"] or
                      not matches_environment(row, manifest["environment"])))):
                rows[key] = {"case_id": key[0], "repeat": key[1], "status": "invalid",
                             "reason": "repeat binding mismatch"}
            else:
                rows[key] = row
    results = []
    for key, entry in expected.items():
        fallback = {"case_id": key[0], "case_version": entry["case"]["case_version"],
                    "repeat": key[1], "status": "missing"}
        comparison = compare(rows.get(key), baseline, manifest["environment"])
        if comparison["status"] == "baseline-not-supplied" and baseline_status:
            reason = baseline_status.get("reason")
            if reason and reason != "baseline-not-supplied":
                comparison = {"status": "baseline-unavailable", "reason": reason}
        results.append({**fallback, **rows.get(key, {}), "comparisons": comparison})
    result = {"manifest_sha256": manifest["manifest_sha256"], "identity": manifest["identity"],
            "environment": manifest["environment"], "baseline_id": baseline["id"] if baseline else None,
            "baseline_kind": baseline["kind"] if baseline else None, "errors": errors, "results": results,
            "statistics": summarize_repeats(results), "case_verdicts": verdict.case_verdicts(results, baseline)}
    if baseline_status is not None:
        result["baseline"] = baseline_status
    if "github_attempt" in manifest:
        result["github_attempt"] = manifest["github_attempt"]
    return result


def matches_environment(row, profile):
    return (row.get("environment_profile") == profile
            and all(arm.get("environment_profile") == profile for arm in row.get("paired_arms", {}).values()))


def summarize_repeats(results):
    observed = {}
    for row in results:
        if row["status"] != "completed" or any(key not in row for key in CASE_CONDITIONS):
            continue
        for name, arm in row.get("paired_arms", {"single": row}).items():
            if arm.get("status") != "completed" or arm.get("mongo_topology") and not arm.get("database_conditions"):
                continue
            conditions = {key: row[key] for key in (*CASE_CONDITIONS, "run_id")}
            conditions["op_boot_sha256"] = arm.get("op_boot_sha256")
            conditions.update({key: arm.get(key) for key in ARM_CONDITIONS})
            for metric, value, source in eligible_metrics(arm):
                if valid(value):
                    binding = {**conditions, "metric_source": source}
                    key = row["case_id"], name, metric, catalog.digest(binding)
                    cohort = observed.setdefault(key, (binding, []))
                    cohort[1].append(value)
    statistics = {}
    for (case, arm, metric, _), (binding, values) in observed.items():
        minimum, maximum = min(values), max(values)
        row = {"conditions": binding, "count": len(values), "median": median(values),
               "min": minimum, "max": maximum, "range": maximum - minimum}
        statistics.setdefault(case, {}).setdefault(arm, {}).setdefault(metric, []).append(row)
    return statistics


def eligible_metrics(arm):
    if arm.get("measurement_status") == "eligible":
        yield "completion_nanos", arm.get("phase_nanos", {}).get("completion_observed"), "completion observation"
    latency = arm.get("live_latency", {})
    if latency.get("status") == "eligible" and identified_source(latency.get("source")):
        yield "endpoint_p95_nanos", latency.get("p95Nanos"), latency["source"]
    resources = arm.get("resources", {})
    if resources.get("status") == "observed":
        for phase, usage in resources["windows"].items():
            if usage.get("covered") and identified_source(usage.get("source")):
                yield f"{phase}.rss_peak_kib", usage.get("rss_peak_kib"), usage["source"]


def compare(row, baseline, environment):
    if baseline is None:
        return {"status": "baseline-not-supplied"}
    if baseline["kind"] not in {"fixture", "accepted"} or not baseline["id"]:
        raise ValueError("A baseline needs an explicit ID and kind")
    if not row or row["status"] != "completed":
        return {"status": "result-unavailable"}
    if baseline["environment"] != environment:
        return {"status": "environment-mismatch"}
    reference = baseline["cases"].get(row["case_id"])
    if reference is not None and reference.get("status") != "completed":
        return {"status": "baseline-result-unavailable"}
    if not reference or any(key not in row or key not in reference or row[key] != reference[key]
                            for key in CASE_CONDITIONS):
        return {"status": "incompatible"}
    arms = row.get("paired_arms", {"single": row})
    reference_arms = reference.get("paired_arms", {"single": reference})
    if arms.keys() != reference_arms.keys():
        return {"status": "incompatible"}
    comparisons = {}
    for name, arm in arms.items():
        ref = reference_arms[name]
        arm_result = {}
        if arm.get("status") != "completed" or ref.get("status") != "completed":
            continue
        if arm.get("mongo_topology") and not arm.get("database_conditions"):
            continue
        if any(arm.get(key) != ref.get(key) for key in ARM_CONDITIONS):
            continue
        reference_metrics = {(metric, source): value for metric, value, source in eligible_metrics(ref)}
        for metric, observed, source in eligible_metrics(arm):
            ratio(arm_result, metric, observed, reference_metrics.get((metric, source)))
        if arm_result:
            comparisons[name] = arm_result
    status = "fixture-comparison" if baseline["kind"] == "fixture" else "compared"
    return {"status": status if comparisons else "no-eligible-observation", "arms": comparisons}


def ratio(output, metric, observed, reference):
    if valid(observed) and valid(reference):
        output[metric] = {"observed": observed, "baseline": reference, "change_ratio": observed / reference - 1}


def valid(value):
    return type(value) in (float, int) and math.isfinite(value) and value > 0


def identified_source(value):
    return isinstance(value, str) and bool(value.strip())


def markdown(report):
    baseline_status = report.get("baseline", {})
    baseline_label = report["baseline_id"] or baseline_status.get("reason", "none supplied")
    lines = ["# Benchmark batch report", "", f"Environment: {report['environment']}",
             f"Baseline: {baseline_label}", "",
             "| Case | Repeat | Run | Measurement | Resources | Comparison |",
             "|---|---:|---|---|---|---|"]
    for row in report["results"]:
        arms = row.get("paired_arms", {"single": row})
        measurements = ", ".join(f"{arm}={value.get('measurement_status', 'unavailable')}"
                                 for arm, value in arms.items())
        resources = ", ".join(f"{arm}={value.get('resources', {}).get('status', 'unavailable')}"
                              for arm, value in arms.items())
        lines.append(f"| {row['case_id']} | {row['repeat']} | {row['status']} | "
                     f"{measurements} | {resources} | {row['comparisons']['status']} |")
    if report["errors"]:
        lines += ["", "Batch reconciliation errors:", ""]
        lines += [f"- {error['reason']}" for error in report["errors"]]
    return "\n".join(lines) + "\n"
