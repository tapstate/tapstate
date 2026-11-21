"""Apply versioned comparison rules to the same samples used in the report."""

from collections import defaultdict
from statistics import median


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
    elif statuses & {"failed", "measurement-incomplete", "invalid", "out-of-memory"}:
        execution = "failed"
    elif "timed-out" in statuses:
        execution = "timed-out"
    else:
        execution = "not-run"
    result = {"execution": execution, "comparison": "unavailable", "metrics": []}
    if not policy:
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
        verdicts.append({"arm": arm, "metric": metric, **classify(samples, rule)})
    if verdicts:
        levels = {"normal": 0, "observe": 1, "suspected-regression": 2}
        result["comparison"] = max((v["status"] for v in verdicts), key=levels.__getitem__)
        result["metrics"] = verdicts
    return result


def classify(samples, rule):
    observed = [sample["observed"] for sample in samples]
    reference = samples[0]["baseline"]
    direction = 1 if rule["worse"] == "higher" else -1
    current = median(observed)
    variation = max(observed) - min(observed)
    change = (current - reference) * direction
    minimum = max(rule["minimum_absolute"], rule["baseline_noise"])
    supporting = sum(
        (value - reference) * direction / reference >= rule["suspect_ratio"]
        and (value - reference) * direction > minimum
        for value in observed
    )
    rate = change / reference
    status = "normal"
    if rate >= rule["observe_ratio"]:
        status = "observe"
    if rate >= rule["suspect_ratio"] and change > minimum and supporting >= rule["minimum_repeats"]:
        status = "suspected-regression"
    return {
        "status": status, "observed_median": current, "baseline": reference,
        "change_ratio": current / reference - 1, "variation": variation, "samples": observed,
        "supporting_repeats": supporting, "thresholds": rule,
    }
