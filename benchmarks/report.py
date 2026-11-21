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
        row = {**fallback, **rows.get(key, {})}
        failures = memory_failures(row, baseline, manifest["environment"])
        if failures:
            row["memory_failures"] = failures
            if row["status"] == "completed":
                row.update(status="failed", last_stage="memory", reason=failures[0]["reason"])
        comparison = compare(row if key in rows else None, baseline, manifest["environment"])
        if comparison["status"] == "baseline-not-supplied" and baseline_status:
            reason = baseline_status.get("reason")
            if reason and reason != "baseline-not-supplied":
                comparison = {"status": "baseline-unavailable", "reason": reason}
        results.append({**row, "comparisons": comparison})
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


def memory_failures(row, baseline, environment):
    failures = []
    limits = {}
    if baseline and baseline["environment"] == environment and row["status"] == "completed":
        reference = baseline["cases"].get(row["case_id"], {})
        if reference and all(key in reference and reference[key] == row.get(key) for key in CASE_CONDITIONS):
            limits = baseline.get("memory_limits", {}).get(row["case_id"], {})
    for name, arm in row.get("paired_arms", {"single": row}).items():
        if arm.get("exit_attribution") == "observed-java-out-of-memory":
            failures.append({"arm": name, "scope": "OP process", "reason": "observed-java-out-of-memory"})
        if limits:
            for metric, observed, source in eligible_metrics(arm):
                limit = limits.get(metric)
                if limit and source == limit["source"] and valid(observed) and observed > limit["limit_kib"]:
                    failures.append({"arm": name, "reason": "memory-budget-exceeded", "metric": metric,
                                     "observed": observed, **limit})
    return failures


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
        windows = resources["windows"]
        for phase, usage in windows.items():
            if usage.get("covered") and identified_source(usage.get("source")):
                yield f"{phase}.rss_peak_kib", usage.get("rss_peak_kib"), usage["source"]
        steady = [windows.get(f"steady-{index}", {}) for index in (1, 2, 3)]
        steady_source = covered_rss_source(steady)
        if steady_source and nondecreasing_rss(steady):
            yield "steady.rss_growth_kib_per_second", resources.get("steady_rss_growth_kib_per_second"), steady_source
        cycles = arm.get("lifecycle_cycles", [])
        idle = [windows.get(f"lifecycle-cycle-{cycle['cycle']}/idle", {}) for cycle in cycles]
        idle_source = covered_rss_source(idle)
        if (len(cycles) >= 3 and idle_source and len({cycle["idle_seconds"] for cycle in cycles}) == 1
                and all(cycle.get("state") == "STOPPED" and cycle.get("target_readback") == "verified"
                        for cycle in cycles)
                and nondecreasing_rss(idle)):
            yield "lifecycle.idle_rss_retained_kib", idle[-1]["rss_median_kib"] - idle[0]["rss_median_kib"], idle_source


def covered_rss_source(windows):
    if not windows or not all(window.get("covered") and valid(window.get("rss_median_kib"))
                              for window in windows):
        return None
    source = windows[0].get("source")
    return source if identified_source(source) and all(window.get("source") == source for window in windows) else None


def nondecreasing_rss(windows):
    return all(first["rss_median_kib"] <= second["rss_median_kib"] for first, second in zip(windows, windows[1:]))


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
