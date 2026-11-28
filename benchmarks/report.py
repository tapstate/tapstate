"""Reconcile batch results and compare compatible benchmark observations."""

from statistics import median

import baseline as baseline_schema
import catalog
import verdict

CASE_CONDITIONS = baseline_schema.CASE_CONDITIONS
ARM_CONDITIONS = ("effective_input_rows", "mongo_topology", "target_delay_millis", "database_conditions",
                  "source_window_protocol")
OOM_ATTRIBUTIONS = {"observed-java-out-of-memory", "observed-op-oom-kill"}


def reconcile(manifest, summaries, baseline=None, *, baseline_status=None, batch_conditions=None):
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
    case_batches = {entry["case"]["id"]: batch["index"]
                    for batch in manifest["batches"] for entry in batch["entries"]}
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
                      not matches_execution(row, manifest["environment"], case)))):
                rows[key] = {"case_id": key[0], "repeat": key[1], "status": "invalid",
                             "reason": "repeat binding mismatch"}
            else:
                rows[key] = row
    results = []
    for key, entry in expected.items():
        fallback = {"case_id": key[0], "case_version": entry["case"]["case_version"],
                    "repeat": key[1], "status": "missing"}
        row = {**fallback, **rows.get(key, {})}
        observed = row if key in rows else None
        if observed and batch_conditions and baseline and baseline.get("schema_version") == baseline_schema.SOURCE_SCHEMA:
            observed = {**row, "runner": batch_conditions.get(case_batches[key[0]], {})}
        outcome = observed or row
        failures = memory_failures(outcome, baseline, manifest["environment"])
        checks = source_memory_checks(outcome, baseline, manifest["environment"])
        if checks:
            row["memory_budget_checks"] = checks
            failures.extend({**check, "reason": "memory-budget-exceeded"}
                            for check in checks if check["status"] == "exceeded")
        if failures:
            row["memory_failures"] = failures
            if row["status"] not in verdict.FAILED_STATUSES:
                if row.get("reason"):
                    row["reason_before_memory_failure"] = row["reason"]
                row.update(status="failed", last_stage="memory", reason=failures[0]["reason"])
        if observed is not None and observed["status"] != row["status"]:
            observed["status"] = row["status"]
        comparison = compare(observed, baseline, manifest["environment"])
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


def matches_execution(row, profile, case):
    if row.get("environment_profile") != profile:
        return False
    arms = row.get("paired_arms")
    if arms is None:
        return "paired-control" not in case.get("tags", [])
    if not isinstance(arms, dict) or not arms:
        return False
    if "paired-control" in case.get("tags", []) and len(arms) != 2:
        return False
    arm_fields = ("case_id", "environment_profile", "code_sha", "run_id", "repeat", *CASE_CONDITIONS)
    return all(isinstance(arm, dict) and arm.get("status") == "completed"
               and all(arm.get(field) == row.get(field) for field in arm_fields) for arm in arms.values())


def memory_failures(row, baseline, environment):
    failures = []
    reference = compatible_case(row, baseline, environment)
    limits = baseline.get("memory_limits", {}).get(row["case_id"], {}) if reference else {}
    reference_arms = arms_for(reference) if reference else {}
    for name, arm in arms_for(row).items():
        attribution = arm.get("exit_attribution")
        if attribution in OOM_ATTRIBUTIONS:
            failures.append({"arm": name, "scope": "OP process", "reason": attribution})
        ref = reference_arms.get(name)
        if limits and ref and matching_arm(arm, ref) and resource_conditions(arm) == resource_conditions(ref):
            for metric, observed, source in eligible_metrics(arm):
                limit = limits.get(metric)
                if limit and source == limit["source"] and valid(observed) and observed > limit["limit_kib"]:
                    failures.append({"arm": name, "reason": "memory-budget-exceeded", "metric": metric,
                                     "observed": observed, **limit})
    return failures


def source_memory_checks(row, baseline, environment):
    if not baseline or baseline.get("schema_version") != baseline_schema.SOURCE_SCHEMA:
        return []
    limits = baseline.get("memory_limits", {}).get(row["case_id"])
    if not limits:
        return []
    reference = baseline["cases"][row["case_id"]]["current"]
    if row["status"] != "completed":
        missing = "result-unavailable"
    elif baseline["environment"] != environment or not matches_reference(row, reference):
        missing = "incompatible-conditions"
    else:
        missing = "evidence-insufficient"
    arms = arms_for(row) if missing == "evidence-insufficient" else {}
    checks = []
    for name, metrics in limits.items():
        observations = list(source_metrics(arms[name])) if name in arms else []
        for metric, budgets in metrics.items():
            for budget in budgets:
                observed = next((value for key, value, binding in observations
                                 if key == metric and metric_binding_matches(budget, binding)), None)
                check = {**budget, "arm": name, "metric": metric, "status": missing}
                if observed is not None:
                    check.update(observed=observed,
                                 status="exceeded" if observed > budget["limit"] else "within-budget")
                checks.append(check)
    return checks


def summarize_repeats(results):
    observed = {}
    for row in results:
        if row["status"] != "completed" or any(key not in row for key in CASE_CONDITIONS):
            continue
        for name, arm in arms_for(row).items():
            if arm.get("status") != "completed" or arm.get("mongo_topology") and not arm.get("database_conditions"):
                continue
            conditions = {key: row[key] for key in (*CASE_CONDITIONS, "run_id")}
            conditions["op_boot_sha256"] = arm.get("op_boot_sha256")
            conditions.update({key: arm[key] for key in ARM_CONDITIONS if key in arm})
            for metric, value, source in eligible_metrics(arm):
                if valid_metric_value(metric, value):
                    binding = {**conditions, **metric_conditions(arm, metric), "metric_source": source}
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


COMPLETION_PROBES = {
    "backlog-throughput": ("backlog_completion", "backlog_probe", "source tails and target writer acknowledgements"),
    "snapshot": ("snapshot_completion", "snapshot_probe", "snapshot target writer acknowledgements"),
}


def eligible_metrics(arm):
    if arm.get("paired_source_comparable") is False:
        return
    probe = COMPLETION_PROBES.get(arm.get("mode"))
    if probe and probe[0] in arm:
        completion_key, identity_key, source = probe
        completion = arm[completion_key]
        duration = completion.get("completion_ms")
        if (completion.get("status") == "eligible" and type(duration) is int and duration > 0
                and identified_source(arm.get(identity_key, {}).get("protocol"))):
            yield "completion_nanos", duration * 1_000_000, source
    elif arm.get("measurement_status") == "eligible":
        yield "completion_nanos", arm.get("phase_nanos", {}).get("completion_observed"), "completion observation"
    latency = arm.get("live_latency", {})
    if latency.get("status") == "eligible" and identified_source(latency.get("source")):
        yield "endpoint_p95_nanos", latency.get("p95Nanos"), latency["source"]
    resources = arm.get("resources", {})
    if resources.get("status") == "observed":
        windows = resources["windows"]
        for phase, usage in windows.items():
            if usage.get("covered") is True and identified_source(usage.get("source")):
                yield f"{phase}.rss_peak_kib", usage.get("rss_peak_kib"), usage["source"]
        steady = [windows.get(f"steady-{index}", {}) for index in (1, 2, 3)]
        steady_source = covered_rss_source(steady)
        steady_starts = [window.get("started_epoch_ms") for window in steady]
        if (steady_source and nondecreasing_rss(steady) and strictly_ordered_integers(steady_starts)):
            seconds = (steady_starts[-1] - steady_starts[0]) / 1000
            yield "steady.rss_growth_kib_per_second", (
                steady[-1]["rss_median_kib"] - steady[0]["rss_median_kib"]
            ) / seconds, steady_source
        cycles = arm.get("lifecycle_cycles", [])
        idle = [windows.get(f"lifecycle-cycle-{cycle['cycle']}/idle", {}) for cycle in cycles]
        idle_source = covered_rss_source(idle)
        if (len(cycles) >= 3 and idle_source and strictly_ordered_integers([cycle["cycle"] for cycle in cycles])
                and len({cycle["idle_seconds"] for cycle in cycles}) == 1
                and all(cycle.get("state") == "STOPPED" and cycle.get("target_readback") == "verified"
                        for cycle in cycles)
                and nondecreasing_rss(idle)):
            yield "lifecycle.idle_rss_retained_kib", idle[-1]["rss_median_kib"] - idle[0]["rss_median_kib"], idle_source


def covered_rss_source(windows):
    if not windows or not all(window.get("covered") is True and valid(window.get("rss_median_kib"))
                              for window in windows):
        return None
    source = windows[0].get("source")
    return source if identified_source(source) and all(window.get("source") == source for window in windows) else None


def nondecreasing_rss(windows):
    return all(first["rss_median_kib"] <= second["rss_median_kib"] for first, second in zip(windows, windows[1:]))


def arms_for(row):
    return row.get("paired_arms", {"single": row})


def compatible_case(row, baseline, environment):
    if not row or row["status"] != "completed" or not baseline or baseline["environment"] != environment:
        return None
    ref = baseline["cases"].get(row["case_id"])
    if (not ref or ref.get("status") != "completed" or arms_for(row).keys() != arms_for(ref).keys()
            or any(key not in row or key not in ref or row[key] != ref[key] for key in CASE_CONDITIONS)
            or row.get("environment_profile") != ref.get("environment_profile")):
        return None
    return ref


def matching_arm(arm, ref):
    return (arm.get("status") == ref.get("status") == "completed"
            and (not arm.get("mongo_topology") or bool(arm.get("database_conditions")))
            and all(arm.get(key) == ref.get(key) for key in (*ARM_CONDITIONS, "environment", "environment_profile")))


def resource_conditions(arm):
    resources = arm.get("resources", {})
    return {key: resources.get(key) for key in ("scope", "op_heap_limit_mib", "sampling_interval_ms")}


def metric_conditions(arm, metric):
    probe = COMPLETION_PROBES.get(arm.get("mode"))
    if metric == "completion_nanos" and probe and probe[0] in arm:
        completion_key, identity_key, _ = probe
        return {"protocol": arm.get(identity_key, {}).get("protocol"),
                "clock_resolution_ms": arm[completion_key].get("clock_resolution_ms")}
    if metric == "endpoint_p95_nanos":
        return {"protocol": arm.get("latency_probe", {}).get("protocol"),
                "source_clock_resolution_ms": arm.get("latency_source_clock_resolution_ms")}
    if ".rss_" not in metric and metric != "lifecycle.idle_rss_retained_kib":
        return {}
    conditions = resource_conditions(arm)
    if metric == "lifecycle.idle_rss_retained_kib":
        conditions["idle_seconds"] = [cycle["idle_seconds"] for cycle in arm["lifecycle_cycles"]]
    return conditions


def strictly_ordered_integers(values):
    return all(type(value) is int and value >= 0 for value in values) and all(
        first < second for first, second in zip(values, values[1:]))


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
    if reference is None or reference.get("status") != "completed":
        return {"status": "baseline-result-unavailable"}
    if baseline.get("schema_version") == baseline_schema.SOURCE_SCHEMA:
        return compare_sources(row, reference, baseline)
    if not compatible_case(row, baseline, environment):
        return {"status": "incompatible"}
    arms = arms_for(row)
    refs = arms_for(reference)
    if any(not matching_arm(arm, refs[name]) for name, arm in arms.items()):
        return {"status": "incompatible"}
    comparisons = {}
    for name, arm in arms.items():
        ref = refs[name]
        reference_metrics = {(metric, source): value for metric, value, source in eligible_metrics(ref)}
        arm_result = {}
        for metric, observed, source in eligible_metrics(arm):
            conditions = metric_conditions(arm, metric)
            if conditions == metric_conditions(ref, metric):
                binding = {
                    "case": {key: row[key] for key in CASE_CONDITIONS},
                    "arm": {key: arm.get(key) for key in ARM_CONDITIONS},
                    **conditions,
                }
                ratio(arm_result, metric, observed, reference_metrics.get((metric, source)), source, binding)
        if arm_result:
            comparisons[name] = arm_result
    if not comparisons:
        status = "no-eligible-observation"
    else:
        status = "fixture-comparison" if baseline["kind"] == "fixture" else "compared"
    return {"status": status, "arms": comparisons}


def compare_sources(row, case, baseline):
    """Evaluate the current and initial references independently."""
    policy = baseline.get("comparison_rules", {})
    current = compare_reference(row, case["current"], baseline["kind"], policy)
    first = case["first"]
    if first != "current" and first != case["current"]:
        current["first"] = compare_reference(row, first, baseline["kind"], policy)
    return current


def compare_reference(row, reference, kind, policy):
    result = {"reference": reference["measurement"], "rules_version": policy.get("version")}
    rules = policy.get("cases", {}).get(row["case_id"], {})
    conditions = reference["conditions"]
    if not matches_reference(row, reference):
        return {**result, "status": "incompatible"}
    comparisons = {}
    unavailable = {}
    for name, arm in arms_for(row).items():
        if arm.get("status") != "completed":
            continue
        groups_by_metric = reference["arms"][name]["metrics"]
        arm_results = {}
        for metric, value, binding in source_metrics(arm):
            match = [group for group in groups_by_metric.get(metric, ()) if group["availability"] == "measured"
                     and metric_binding_matches(group, binding)]
            if len(match) != 1:
                continue
            median = match[0]["summary"]["median"]
            if median == 0 and "zero_reference" not in rules.get(metric, {}):
                unavailable.setdefault(name, {})[metric] = "zero-reference-no-absolute-rule"
            else:
                ratio(arm_results, metric, value, median, binding["source"],
                      {"case": conditions, **binding}, validator=finite_observation)
        if arm_results:
            comparisons[name] = arm_results
    if unavailable:
        result["metric_reasons"] = unavailable
    if comparisons:
        status = "fixture-comparison" if kind == "fixture" else "compared"
    elif unavailable:
        status = "incompatible"
        result["reason"] = "zero-reference-no-absolute-rule"
    else:
        status = "no-eligible-observation"
    return {**result, "status": status, "arms": comparisons}


def matches_reference(row, reference):
    conditions = reference["conditions"]
    return (arms_for(row).keys() == reference["arms"].keys()
            and all(row.get(key) == conditions[key] for key in (*CASE_CONDITIONS, "runner", "environment_profile")))


def metric_binding_matches(stored, observed):
    return all(stored[key] == observed[key] for key in baseline_schema.SOURCE_METRIC_BINDING)


def source_metrics(arm):
    """Convert eligible observations into the units carried by a versioned reference."""
    facts = {key: arm[key] for key in ARM_CONDITIONS if arm.get(key) is not None}
    resources = arm.get("resources", {})
    for metric, value, source in eligible_metrics(arm):
        if not valid_metric_value(metric, value):
            continue
        conditions = {**facts, **metric_conditions(arm, metric)}
        if metric == "completion_nanos":
            name, value, unit, scope, window = "completion_ms", value / 1_000_000, "ms", "pipeline", "completion"
        elif metric == "endpoint_p95_nanos":
            name, value, unit, scope, window = "endpoint_p95_ms", value / 1_000_000, "ms", "target endpoint", "live"
        else:
            window, _, measure = metric.rpartition(".")
            scope = conditions.pop("scope", None) or resources.get("scope")
            if measure.endswith("_kib_per_second"):
                name, value, unit = metric.removesuffix("_kib_per_second") + "_bytes_per_hour", value * 1024 * 3600, "B/h"
            else:
                name, value, unit = metric.removesuffix("_kib") + "_bytes", value * 1024, "B"
        if scope and baseline_schema.finite_number(value):
            yield name, value, {"conditions": conditions, "scope": scope, "window_ref": window,
                                "source": source, "unit": unit}


def finite_observation(metric, value):
    return baseline_schema.finite_number(value) and value >= 0


def ratio(output, metric, observed, reference, source, conditions, *, validator=None):
    validator = validator or valid_metric_value
    if validator(metric, observed) and validator(metric, reference):
        change = observed / reference - 1 if reference else None
        if change is None or baseline_schema.finite_number(change):
            output[metric] = {"observed": observed, "baseline": reference, "change_ratio": change,
                              "source": source, "conditions": conditions}


def valid_metric_value(metric, value):
    if metric in {"steady.rss_growth_kib_per_second", "lifecycle.idle_rss_retained_kib", "endpoint_p95_nanos"}:
        return baseline_schema.finite_number(value) and value >= 0
    return valid(value)


def valid(value):
    return baseline_schema.finite_number(value) and value > 0


def identified_source(value):
    return baseline_schema.nonblank(value)


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
