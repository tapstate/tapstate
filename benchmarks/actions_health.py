"""Identify missing, queued and expired scheduled performance runs."""

from datetime import timedelta

from actions_run import BUDGET_SECONDS, timestamp


SCHEDULE_HOUR_UTC = 10
SCHEDULE_GRACE = timedelta(minutes=45)
HANDOFF_GRACE = timedelta(minutes=15)


def assess(now, nightly_attempts, performance_runs, reports):
    due = now.replace(hour=SCHEDULE_HOUR_UTC, minute=0, second=0, microsecond=0)
    if now < due:
        due -= timedelta(days=1)
    expected = [
        run for run in nightly_attempts
        if run["event"] == "schedule" and run["run_attempt"] == 1
        and due <= timestamp(run["created_at"]) < due + timedelta(days=1)
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
    deadline = timestamp(start) + timedelta(seconds=BUDGET_SECONDS)
    if performance["status"] != "completed":
        return outcome(due, "performance-running" if now < deadline else "performance-expired", **identity)
    report = reports.get(performance["id"])
    if not report or report.get("github_attempt") != {"run_id": performance["id"], "attempt": 1}:
        return outcome(due, "report-missing", **identity)
    completed = timestamp(performance["updated_at"])
    status = "performance-expired" if completed > deadline else "reported"
    return outcome(due, status, report=report, **identity)


def outcome(due, status, *, nightly=None, performance=None, report=None):
    return {"due_at": due.isoformat(), "status": status, "nightly": nightly,
            "performance": performance, "report": report}
