"""Derive performance notifications from a reconciled report and delivery receipts."""

from datetime import datetime, timedelta, timezone
from html import escape
import re

import actions_report
import baseline
import catalog
import verdict


WORKFLOW_ANOMALIES = {
    "missing-nightly": "Scheduled Nightly missing",
    "missing-handoff": "Performance handoff missing",
    "performance-expired": "Performance run exceeded its deadline",
    "performance-budget-cancelled": "Performance run cancelled at its budget boundary",
    "performance-start-unavailable": "Performance start time missing",
    "report-missing": "Report for this attempt missing",
}
CARD_LABEL = "\u540c\u6b65\u6027\u80fd"
BUSINESS_ZONE = timezone(timedelta(hours=8))


def workflow_problem(health):
    status = health["status"]
    if status in WORKFLOW_ANOMALIES:
        return status
    if status != "reported":
        return None
    report = health["report"]
    if report["errors"]:
        return report["errors"][0]["reason"]
    if report["case_counts"].get("timed-out"):
        return "Cases timed out"
    if any(report["case_counts"].get(key) for key in ("skipped", "not-run")):
        return "Incomplete case coverage"
    return None


def signals(health):
    active, healthy = {}, set()
    report = health.get("report")
    problem = workflow_problem(health)
    if problem:
        active["workflow"] = {"kind": "workflow", "reason": problem}
    elif health["status"] == "reported":
        healthy.add("workflow")
    if not report:
        return active, healthy

    conditions = {}
    for case_id, case in report["case_verdicts"].items():
        if case["execution"] not in {"completed", "failed"}:
            continue
        context = report["case_context"][case_id]
        conditions[case_id] = catalog.digest({
            "suites": sorted(context["suites"]), "version": context["version"], "profile": context["profile"],
            "load": context.get("effective_load"), "environment": report["environment"],
        })
        if case["execution"] == "completed":
            healthy.add(f"execution:{case_id}:{conditions[case_id]}")
        for metric in case["metrics"]:
            if metric["status"] == "normal":
                healthy.add(comparison_key(case_id, conditions[case_id], case, metric))

    # Use the same ordering and individual failure evidence as Actions Summary.
    for (stage, reason), observations in actions_report.execution_failure_groups(report):
        for row, _, _ in observations:
            case_id = row["case_id"]
            active.setdefault(f"execution:{case_id}:{conditions[case_id]}", {
                "kind": "execution", "case_id": case_id, "context": report["case_context"][case_id],
                "stage": stage, "reason": reason,
            })
    for _, metrics in actions_report.regression_groups(report):
        for case_id, metric in metrics:
            active[comparison_key(case_id, conditions[case_id], report["case_verdicts"][case_id], metric)] = {
                "kind": "comparison", "case_id": case_id, "context": report["case_context"][case_id],
                "metric": metric,
            }
    return active, healthy


def comparison_key(case_id, condition, case, metric):
    binding = {key: metric[key] for key in ("source", "conditions", "baseline", "thresholds")}
    cohort = catalog.digest({**binding, "rules_version": case["rules_version"]})
    return f"comparison:{case_id}:{condition}:{metric['arm']}:{metric['metric']}:{cohort}"


def changes(previous, active, healthy):
    """Return changes and the state to persist only after confirmed delivery."""
    updates = []
    next_state = dict(previous)
    for key, signal in active.items():
        older = previous.get(key)
        if older is None or escalated(older, signal):
            updates.append(signal)
            next_state[key] = signal
    for key in sorted((previous.keys() & healthy) - active.keys()):
        updates.append({**previous[key], "recovered": True})
        del next_state[key]
    return updates, next_state


def escalated(previous, current):
    if current["kind"] == "workflow":
        return previous["reason"] != current["reason"]
    if current["kind"] != "comparison":
        return False
    before, after = previous["metric"], current["metric"]
    rule = after["thresholds"]
    direction = 1 if rule["worse"] == "higher" else -1
    minimum = max(rule["minimum_absolute"], rule["baseline_noise"])

    def visibly_worse(observed):
        change = (observed - before["observed_median"]) * direction
        return change > minimum and (not before["observed_median"]
                                     or change / before["observed_median"] >= rule["suspect_ratio"])

    return (visibly_worse(after["observed_median"])
            and sum(visibly_worse(sample) for sample in after["samples"]) >= rule["minimum_repeats"])


def elapsed_label(health):
    performance = health.get("performance") or {}
    if not performance.get("run_started_at"):
        return "Elapsed unavailable"
    start = datetime.fromisoformat(performance["run_started_at"])
    end = datetime.fromisoformat(performance["updated_at"] if performance["status"] == "completed"
                                 else health["observed_at"])
    hours, minutes = divmod(max(0, int((end - start).total_seconds()) // 60), 60)
    return f"Elapsed {hours}h {minutes:02d}m" if hours else f"Elapsed {minutes}m"


def card(health, updates, details_url):
    report = health.get("report") or {}
    timing = report.get("workflow", {}).get("timing", {})
    business_source = (health.get("nightly") or {}).get("created_at", health["due_at"])
    date = timing.get("business_date") or datetime.fromisoformat(business_source).astimezone(BUSINESS_ZONE).date().isoformat()
    case_counts = report.get("case_counts", {})
    comparison_counts = report.get("comparison_counts", {})
    coverage = report.get("comparison_coverage", {})
    lines = [f"**{date} (UTC+8)** | Scheduled synchronization suite | {elapsed_label(health)}"]
    if report:
        outcomes = [f"Planned {len(report['selected_ids'])}", f"Completed {case_counts.get('completed', 0)}",
                    f"Failed {case_counts.get('failed', 0)}"]
        for key, label in (("timed-out", "Timed out"), ("skipped", "Skipped"), ("not-run", "Not run")):
            if case_counts.get(key):
                outcomes.append(f"{label} {case_counts[key]}")
        lines.append(" | ".join(outcomes))
        comparable = sum(count for status, count in comparison_counts.items() if status != "unavailable")
        if comparable:
            comparison = [f"Comparable {comparable}",
                          f"Suspected regressions {comparison_counts.get('suspected-regression', 0)}"]
            for key, label in (("incomparable", "Incomparable"), ("unavailable", "Unavailable")):
                if coverage.get(key):
                    comparison.append(f"{label} {coverage[key]}")
            lines.append(" | ".join(comparison))
        else:
            lines.append("Comparison unavailable")

    problem = workflow_problem(health)
    if problem:
        lines.append(f"**Workflow** {short(WORKFLOW_ANOMALIES.get(problem, problem))}")

    for kind in ("execution", "comparison"):
        item = next((item for item in updates if item["kind"] == kind and not item.get("recovered")), None)
        if not item:
            continue
        context = item["context"]
        suites = f" ({', '.join(context['suites'])})" if context["suites"] else ""
        name = f"{short(item['case_id'])}@{context['version']}{suites} · {short(context['scenario'], 72)}"
        if kind == "execution":
            lines.append(f"**Execution failure** {name} | {short(item['stage'])}: {short(item['reason'])}")
        else:
            metric = item["metric"]
            unit = metric_unit(metric["metric"])
            lines.append(f"**Suspected regression** {name} | {short(metric['metric'])}: "
                         f"{metric['baseline']:g} → {metric['observed_median']:g}{unit} ({verdict.format_change(metric)})")
    recovered = [item for item in updates if item.get("recovered")]
    if recovered:
        summary = ["Workflow"] if any(item["kind"] == "workflow" for item in recovered) else []
        for kind, label in (("execution", "execution"), ("comparison", "performance")):
            cases = {item["case_id"] for item in recovered if item["kind"] == kind}
            if cases:
                summary.append(f"{label} cases {len(cases)}")
        lines.append("**Recovered** " + " | ".join(summary))
    return {
        "msg_type": "interactive",
        "card": {
            "header": {"title": {"tag": "plain_text", "content": CARD_LABEL}},
            "elements": [
                {"tag": "markdown", "content": "\n".join(lines)},
                {"tag": "action", "actions": [{"tag": "button", "type": "primary", "url": details_url,
                 "text": {"tag": "plain_text", "content": "View Actions details"}}]},
            ],
        },
    }


def metric_unit(metric):
    unit = baseline.metric_unit(metric)
    if unit:
        return f" {unit}"
    for suffix, unit in (("_nanos", "ns"), ("_kib_per_second", "KiB/s"), ("_kib", "KiB")):
        if metric.endswith(suffix):
            return f" {unit}"
    return ""


def short(value, limit=120):
    normalized = " ".join(str(value or "unavailable").split())
    if len(normalized) > limit:
        normalized = normalized[:limit - 1] + "…"
    return re.sub(r'([\\`*_~!\[\]()])', r'\\\1', escape(normalized))
