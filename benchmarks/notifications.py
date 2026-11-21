"""Derive performance notifications from a reconciled report and delivery receipts."""

from datetime import datetime
from html import escape

import catalog


WORKFLOW_ANOMALIES = {"missing-nightly", "missing-handoff", "performance-expired", "report-missing"}
CARD_LABEL = "\u540c\u6b65\u6027\u80fd"


def signals(health):
    active, healthy = {}, set()
    status = health["status"]
    if status in WORKFLOW_ANOMALIES:
        active["workflow"] = {"kind": "workflow", "reason": status}
    elif status == "reported":
        if health["report"]["errors"]:
            active["workflow"] = {"kind": "workflow", "reason": health["report"]["errors"][0]["reason"]}
        else:
            healthy.add("workflow")
    report = health.get("report")
    if report:
        for case_id, case in report["case_verdicts"].items():
            context = report["case_context"][case_id]
            condition = catalog.digest({
                "version": context["version"], "profile": context["profile"],
                "load": context.get("effective_load"), "environment": report["environment"],
            })
            key = f"execution:{case_id}:{condition}"
            if case["execution"] == "completed":
                healthy.add(key)
            elif case["execution"] == "failed":
                evidence = next(row for row in report["results"] if row["case_id"] == case_id
                                and row["status"] not in {"completed", "missing", "not-run"})
                active[key] = {"kind": "execution", "case_id": case_id, "context": context,
                               "stage": evidence.get("last_stage", "unknown"),
                               "reason": evidence.get("reason", evidence["status"])}
            for metric in case["metrics"]:
                metric_key = f"comparison:{case_id}:{condition}:{metric['arm']}:{metric['metric']}"
                if metric["status"] == "normal":
                    healthy.add(metric_key)
                elif metric["status"] == "suspected-regression":
                    active[metric_key] = {"kind": "comparison", "case_id": case_id, "context": context,
                                          "metric": metric}
    return active, healthy


def changes(previous, active, healthy):
    """Return changes and the state to persist only after confirmed delivery."""
    updates = []
    next_state = dict(previous)
    for key, signal in active.items():
        older = previous.get(key)
        if older is None or escalated(older, signal):
            updates.append(signal)
            next_state[key] = signal
    for key in previous.keys() & healthy:
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
    change = (after["observed_median"] - before["observed_median"]) * direction
    return (before["baseline"] == after["baseline"] and before["thresholds"] == rule
            and change > max(rule["minimum_absolute"], rule["baseline_noise"])
            and change / before["observed_median"] >= rule["suspect_ratio"])


def card(health, updates, mention_id, details_url):
    report = health.get("report") or {}
    date = datetime.fromisoformat(health["due_at"]).date().isoformat()
    case_counts = report.get("case_counts", {})
    comparison_counts = report.get("comparison_counts", {})
    lines = [f"**{date}** | Scheduled synchronization suite"]
    if any(not update.get("recovered") for update in updates):
        lines[0] += f" | <at id={escape(mention_id, quote=True)}></at>"
    if report:
        lines.append(f"Planned {len(report['selected_ids'])} | Completed {case_counts.get('completed', 0)}"
                     f" | Failed {case_counts.get('failed', 0)} | Timed out {case_counts.get('timed-out', 0)}"
                     f" | Not run {case_counts.get('not-run', 0)}")
        comparable = sum(count for status, count in comparison_counts.items() if status != "unavailable")
        if comparable:
            lines.append(f"Comparable {comparable} | Suspected regressions {comparison_counts.get('suspected-regression', 0)}"
                         f" | Unavailable {comparison_counts.get('unavailable', 0)}")
        else:
            lines.append("Comparison unavailable")
    examples = {}
    for signal in updates:
        if not signal.get("recovered"):
            examples.setdefault(signal["kind"], signal)
    for kind in ("workflow", "execution", "comparison"):
        item = examples.get(kind)
        if not item:
            continue
        if kind == "workflow":
            lines.append(f"**Workflow** {short(item['reason'])}")
            continue
        context = item["context"]
        suites = f" ({', '.join(context['suites'])})" if context["suites"] else ""
        name = f"{item['case_id']}@{context['version']}{suites} · {short(context['scenario'])}"
        if kind == "execution":
            lines.append(f"**Execution failure** {name} | {short(item['stage'])}: {short(item['reason'])}")
        else:
            metric = item["metric"]
            lines.append(f"**Suspected regression** {name} | {metric['metric']}: "
                         f"{metric['baseline']:g} → {metric['observed_median']:g} ({metric['change_ratio']:+.1%})")
    recovered = sum(bool(item.get("recovered")) for item in updates)
    if recovered:
        lines.append(f"**Recovered** {recovered}")
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


def short(value):
    return escape(" ".join(str(value or "unavailable").split())[:120])
