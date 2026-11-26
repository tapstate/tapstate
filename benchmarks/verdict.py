"""Apply versioned comparison rules to the same samples used in the report."""

from collections import defaultdict
from statistics import median


FAILED_STATUSES = {"failed", "measurement-incomplete", "invalid", "out-of-memory"}
INCOMPARABLE = {"environment-mismatch", "incompatible", "zero-reference-no-absolute-rule"}


def case_verdicts(results, baseline):
    by_case = defaultdict(list)
    for row in results:
        by_case[row["case_id"]].append(row)
    policy = baseline.get("comparison_rules") if baseline else None
    return {case: summarize(rows, policy) for case, rows in by_case.items()}


def summarize(rows, policy):
    statuses = {row["status"] for row in rows}
    if statuses == {"completed"}:
        execution = "completed"
    elif statuses & FAILED_STATUSES:
        execution = "failed"
    elif "timed-out" in statuses:
        execution = "timed-out"
    elif statuses == {"skipped"}:
        execution = "skipped"
    else:
        execution = "not-run"
    reasons = sorted({row["comparisons"].get("reason", row["comparisons"]["status"]) for row in rows})
    result = {"execution": execution, "comparison": "unavailable", "comparison_reasons": reasons, "metrics": []}
    if not policy:
        if set(reasons) & {"compared", "fixture-comparison"}:
            result["comparison_reasons"] = ["comparison-rules-unavailable"]
        return result
    result["rules_version"] = policy["version"]
    if execution != "completed":
        return result
    rules = policy["cases"].get(rows[0]["case_id"], {})
    observations = defaultdict(list)
    for row in rows:
        for arm, comparisons in row["comparisons"].get("arms", {}).items():
            for metric, sample in comparisons.items():
                if metric in rules:
                    observations[arm, metric].append(sample)
    verdicts = []
    for (arm, metric), samples in sorted(observations.items()):
        rule = rules[metric]
        if len(samples) != len(rows) or len(rows) < rule["minimum_repeats"]:
            continue
        binding = {key: samples[0][key] for key in ("baseline", "source", "conditions")}
        if any(any(sample[key] != value for key, value in binding.items()) for sample in samples):
            continue
        verdicts.append({"arm": arm, "metric": metric, "source": binding["source"],
                         "conditions": binding["conditions"], **classify(samples, rule)})
    if verdicts:
        levels = {"normal": 0, "observe": 1, "suspected-regression": 2}
        result["comparison"] = max((v["status"] for v in verdicts), key=levels.__getitem__)
        result["metrics"] = verdicts
        result["comparison_reasons"] = [reason for reason in reasons if reason not in {"compared", "fixture-comparison"}]
    elif any(reason in INCOMPARABLE for reason in reasons):
        result["comparison_reasons"] = reasons
    elif set(reasons) & {"compared", "fixture-comparison"}:
        result["comparison_reasons"] = ["insufficient-rule-or-repeat-evidence"]
    return result


def classify(samples, rule):
    observed = [sample["observed"] for sample in samples]
    reference = samples[0]["baseline"]
    direction = 1 if rule["worse"] == "higher" else -1
    current = median(observed)
    variation = max(observed) - min(observed)
    change = (current - reference) * direction
    minimum = max(rule["minimum_absolute"], rule["baseline_noise"])
    absolute = rule.get("zero_reference")

    def meets_threshold(delta, level):
        if reference:
            return delta / reference >= rule[f"{level}_ratio"]
        return delta >= absolute[f"{level}_absolute"] if absolute else delta > 0

    supporting = sum(
        meets_threshold((value - reference) * direction, "suspect")
        and (value - reference) * direction > minimum
        for value in observed
    )
    status = "normal"
    if meets_threshold(change, "observe"):
        status = "observe"
    if (meets_threshold(change, "suspect") and change > minimum
            and supporting >= rule["minimum_repeats"]):
        status = "suspected-regression"
    return {
        "status": status, "observed_median": current, "baseline": reference,
        "change_ratio": current / reference - 1 if reference else None, "absolute_change": current - reference,
        "variation": variation, "samples": observed,
        "supporting_repeats": supporting, "thresholds": rule,
    }


def format_change(metric):
    ratio = metric["change_ratio"]
    return f"{ratio:+.1%}" if ratio is not None else f"{metric['absolute_change']:+g} (absolute)"
