"""Identify missing, queued and expired scheduled performance runs."""

from datetime import timedelta

from actions_run import BUDGET_SECONDS, timestamp


SCHEDULE_HOUR_UTC = 10
# Recent scheduled starts arrived up to 14h36m after the cron slot.
SCHEDULE_GRACE = timedelta(hours=16)
HANDOFF_GRACE = timedelta(minutes=15)


def scheduled_due(now):
    due = now.replace(hour=SCHEDULE_HOUR_UTC, minute=0, second=0, microsecond=0)
    if now < due:
        due -= timedelta(days=1)
    return due


def assess(now, nightly_attempts, performance_runs, reports):
    return select_observation(observations(now, nightly_attempts, performance_runs, reports), {})


def observation_marker(health):
    run = health["performance"]
    return {"due_at": health["due_at"], "status": health["status"],
            "run_id": run["id"] if run else None, "report_available": health["report"] is not None}


def select_observation(observed, previous_marker):
    current, previous = observed
    due = timestamp(current["due_at"])
    if previous_marker.get("due_at", "") > previous["due_at"]:
        return current
    run = previous["performance"]
    if not run:
        nightly = previous["nightly"]
        if (nightly and nightly["status"] == "completed"
                and timestamp(nightly["updated_at"]) >= due
                and previous_marker != observation_marker(previous)):
            return previous
        return current
    if run["status"] == "completed" and timestamp(run["updated_at"]) < due:
        return current
    if previous["status"] == "performance-running":
        return previous
    if previous["status"] == "performance-queued":
        return previous if current["status"] == "awaiting-nightly" else current
    # Settle an overlapping older attempt once before advancing to a newer slot.
    if previous_marker != observation_marker(previous):
        return previous
    return current


def observations(now, nightly_attempts, performance_runs, reports):
    due = scheduled_due(now)
    return [assess_slot(now, slot, nightly_attempts, performance_runs, reports)
            for slot in (due, due - timedelta(days=1))]


def assess_slot(now, due, nightly_attempts, performance_runs, reports):
    next_slot = due + timedelta(days=1)
    expected = [
        run for run in nightly_attempts
        if run["event"] == "schedule" and run["run_attempt"] == 1
        and due <= timestamp(run["created_at"]) < next_slot and timestamp(run["created_at"]) <= now
    ]
    if not expected:
        return outcome(due, "awaiting-nightly" if now < due + SCHEDULE_GRACE else "missing-nightly")
    nightly = min(expected, key=lambda run: timestamp(run["created_at"]))
    if nightly["status"] != "completed":
        return outcome(due, "nightly-running", nightly=nightly)
    title = f"Pipeline Performance | Nightly {nightly['id']}/1"
    matching = [
        run for run in performance_runs
        if run["event"] == "workflow_run" and run["run_attempt"] == 1 and run["display_title"] == title
    ]
    if not matching:
        available_at = timestamp(nightly["updated_at"]) + HANDOFF_GRACE
        return outcome(due, "awaiting-handoff" if now < available_at else "missing-handoff", nightly=nightly)
    performance = min(matching, key=lambda run: timestamp(run["created_at"]))
    identity = {"nightly": nightly, "performance": performance}
    if performance["status"] in {"queued", "requested", "waiting", "pending"}:
        return outcome(due, "performance-queued", **identity)
    start = performance.get("run_started_at")
    if not start:
        return outcome(due, "performance-start-unavailable", **identity)
    deadline = workflow_deadline(performance)
    if performance["status"] != "completed":
        return outcome(due, "performance-running" if now < deadline else "performance-expired", **identity)
    report = reports.get(performance["id"])
    if not matching_report(report, performance, nightly):
        return outcome(due, "report-missing", **identity)
    completed = timestamp(performance["updated_at"])
    status = "performance-expired" if completed > deadline else "reported"
    return outcome(due, status, report=report, **identity)


def matching_report(report, performance, nightly):
    if not report or report.get("github_attempt") != {"run_id": performance["id"], "attempt": 1}:
        return False
    if (any(not isinstance(report.get(key), dict) for key in (
            "identity", "case_counts", "comparison_counts", "case_context", "case_verdicts"))
            or any(not isinstance(report.get(key), list) for key in ("errors", "results", "selected_ids"))):
        return False
    # A failed preparation can publish a report before it has any workflow inputs.
    workflow = report.get("workflow")
    if not report["identity"] and report["errors"] and not report["selected_ids"]:
        if workflow == {}:
            return True
    elif report["identity"].get("code_sha") != performance["head_sha"]:
        return False
    if not isinstance(workflow, dict):
        return False
    upstream = workflow.get("timing", {}).get("nightly") or {}
    return (workflow.get("github_attempt") == report["github_attempt"] and workflow.get("scope") == "all"
            and upstream.get("run_id") == nightly["id"] and upstream.get("attempt") == 1)


def workflow_deadline(performance):
    return timestamp(performance["run_started_at"]) + timedelta(seconds=BUDGET_SECONDS)


def outcome(due, status, *, nightly=None, performance=None, report=None):
    return {"due_at": due.isoformat(), "status": status, "nightly": nightly,
            "performance": performance, "report": report}
