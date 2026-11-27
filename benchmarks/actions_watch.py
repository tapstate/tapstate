"""Read scheduled performance evidence from GitHub and prepare a notification receipt."""

import argparse
from datetime import datetime, timedelta, timezone
from hashlib import sha256
from io import BytesIO
import json
import os
from pathlib import Path
import subprocess
import sys
from urllib.error import HTTPError
from urllib.request import Request, urlopen
from zipfile import BadZipFile, ZipFile

import actions_health
import catalog
from actions_run import CONTROL_RESERVE_SECONDS
import notifications
import runner


def api(repository, path):
    output = subprocess.check_output(["gh", "api", f"repos/{repository}/actions/{path}"], text=True)
    return json.loads(output)


HEALTH_PATH = ".github/workflows/pipeline-performance-health.yml"
ARTIFACT_LIMIT = 16 * 1024 * 1024
REPORT_NAME = "performance-report-1"
OBSERVATION_ARTIFACT = "performance-health-observation"


def first_attempt(repository, run):
    if run["run_attempt"] == 1:
        return run
    return api(repository, f"runs/{run['id']}/attempts/1")


def attempt_artifact(item, source):
    run = item.get("workflow_run") or {}
    return (not item["expired"] and run.get("id") == source["id"]
            and run.get("head_sha") == source["head_sha"] and source["run_attempt"] == 1
            and source["status"] == "completed"
            and actions_health.timestamp(source["run_started_at"]) <= actions_health.timestamp(item["created_at"])
            <= actions_health.timestamp(source["updated_at"]))


def has_http_status(error, status):
    return (error.stderr or b"").rstrip().endswith(f"(HTTP {status})".encode())


def artifact(repository, source, name, filename):
    available = api(repository, f"runs/{source['id']}/artifacts?name={name}&per_page=20")["artifacts"]
    candidates = [item for item in available if item["name"] == name and attempt_artifact(item, source)]
    if len(candidates) != 1 or candidates[0]["size_in_bytes"] > ARTIFACT_LIMIT:
        return {}
    item = candidates[0]
    path = f"repos/{repository}/actions/artifacts/{item['id']}/zip"
    archive = subprocess.check_output(["gh", "api", path], stderr=subprocess.PIPE, timeout=60)
    if len(archive) > ARTIFACT_LIMIT or item.get("digest") != f"sha256:{sha256(archive).hexdigest()}":
        return {}
    try:
        with ZipFile(BytesIO(archive)) as zipped:
            files = [entry for entry in zipped.infolist() if entry.filename == filename]
            if len(files) != 1 or files[0].file_size > ARTIFACT_LIMIT:
                return {}
            value = json.loads(zipped.read(files[0]))
            return value if isinstance(value, dict) else {}
    except (BadZipFile, ValueError, UnicodeError):
        return {}


def report_removed_after_rerun(repository, run_id):
    artifacts = api(repository, f"runs/{run_id}/artifacts?name={REPORT_NAME}&per_page=20")["artifacts"]
    if any(item["name"] == REPORT_NAME for item in artifacts):
        return False
    jobs = api(repository, f"runs/{run_id}/attempts/1/jobs?per_page=100")["jobs"]
    return any(step["name"] == "Upload the report" and step["conclusion"] == "success"
               for job in jobs if job["name"] == "report" for step in job["steps"])


def read_scheduled_report(repository, source, rerun):
    run_id = source["id"]
    try:
        report = artifact(repository, source, REPORT_NAME, "report.json")
    except subprocess.CalledProcessError as error:
        gone = has_http_status(error, 410)
        if not gone and not has_http_status(error, 404):
            raise
        # The archive can disappear after both the run and artifact inventories were read.
        rerun = rerun or api(repository, f"runs/{run_id}")["run_attempt"] > 1
        if rerun and report_removed_after_rerun(repository, run_id):
            return {}, True
        if gone:
            return {}, False
        raise
    if report:
        return report, False
    rerun = rerun or api(repository, f"runs/{run_id}")["run_attempt"] > 1
    return report, rerun and report_removed_after_rerun(repository, run_id)


def trusted_artifacts(repository, branch, name):
    artifacts = api(repository, f"artifacts?name={name}&per_page=50")["artifacts"]
    candidates = sorted(artifacts, key=artifact_order, reverse=True)
    for candidate in candidates:
        reference = candidate.get("workflow_run")
        if not reference or candidate["expired"] or reference["head_branch"] != branch:
            continue
        source = first_attempt(repository, api(repository, f"runs/{reference['id']}"))
        if source["event"] == "schedule" and source["path"] == HEALTH_PATH and attempt_artifact(candidate, source):
            yield candidate, source


def latest_artifact(repository, branch, name):
    return next(trusted_artifacts(repository, branch, name), None)


def artifact_order(item):
    return item["created_at"], item["id"]


def latest_state(repository, branch, now):
    committed = latest_artifact(repository, branch, "performance-notification-state")
    state = {"updated_at": (now - timedelta(days=1)).isoformat(), "active": {}}
    if committed:
        _, source = committed
        state = artifact(repository, source, "performance-notification-state", "state.json")
        if not state:
            raise ValueError(f"Notification state from run {source['id']} could not be read")
    intent = latest_artifact(repository, branch, "performance-notification-intent")
    if not intent or (committed and artifact_order(intent[0]) < artifact_order(committed[0])):
        return state
    _, source = intent
    proposed = artifact(repository, source, "performance-notification-intent", "next-state.json")
    receipt = artifact(repository, source, "performance-notification-receipt", "delivery.json")
    if not proposed or not isinstance(proposed.get("active"), dict):
        raise ValueError(f"Notification intent from run {source['id']} could not be read")
    status = receipt.get("status") if receipt.get("intent_sha256") == catalog.digest(proposed) else None
    if status == "accepted":
        return state_from_intent(proposed)
    # The previous accepted receipt may have survived without its state artifact.
    state = proposed.get("previous", state)
    pending = dict(state.get("pending", proposed.get("pending", {})))
    if status != "rejected":
        before, after = state["active"], proposed["active"]
        for key in before.keys() | after.keys():
            if before.get(key) != after.get(key):
                pending[key] = after.get(key)
    return {**state, "pending": pending}


def state_from_intent(proposed):
    return {key: value for key, value in proposed.items() if key != "previous"}


def previous_budget_cancellations(repository, branch, observations, now):
    tracked = set()
    for observation in observations:
        run = observation["performance"]
        if not run or not run.get("run_started_at"):
            continue
        deadline = actions_health.workflow_deadline(run)
        if run.get("conclusion") == "cancelled" or (
                run["status"] != "completed" and now >= deadline - timedelta(seconds=CONTROL_RESERVE_SECONDS)):
            tracked.add(str(run["id"]))
    if not tracked:
        return {}
    receipts = {}
    for _, source in trusted_artifacts(repository, branch, OBSERVATION_ARTIFACT):
        saved = artifact(repository, source, OBSERVATION_ARTIFACT, "health.json")
        if not saved:
            raise ValueError(f"Health observation from run {source['id']} could not be read")
        for run_id, receipt in saved.get("budget_cancellations", {}).items():
            if run_id in tracked and receipt.get("run_attempt") == 1:
                receipts.setdefault(run_id, receipt)
        if not saved.get("cancellation_history_error") or tracked <= receipts.keys():
            break
    return receipts


def reconcile_pending(previous, active, healthy):
    confirmed = dict(previous["active"])
    pending = {}
    for key, attempted in previous.get("pending", {}).items():
        if attempted is None or attempted == "recovery":
            if key in healthy or key in active:
                # The previous proposal observed recovery. End that incident without resending its card.
                confirmed.pop(key, None)
            else:
                pending[key] = attempted
        elif key not in healthy or key in active:
            pending[key] = attempted
    return confirmed, pending


def stop_expiring_run(repository, performance, now):
    deadline = actions_health.workflow_deadline(performance)
    if now < deadline - timedelta(seconds=CONTROL_RESERVE_SECONDS):
        return
    run_id = performance["id"]
    if now < deadline:
        jobs = api(repository, f"runs/{run_id}/attempts/1/jobs?per_page=100")["jobs"]
        unfinished = any(job["status"] != "completed" and
                         (job["name"] in {"prepare", "Prepare Maven dependencies"} or job["name"].startswith("measure ("))
                         for job in jobs)
        if not unfinished:
            return
    live = api(repository, f"runs/{run_id}")
    if live["run_attempt"] != 1 or live["status"] == "completed":
        return
    try:
        subprocess.run(["gh", "api", "--method", "POST", f"repos/{repository}/actions/runs/{run_id}/cancel"],
                       check=True, stderr=subprocess.PIPE, timeout=30)
    except subprocess.CalledProcessError as error:
        if not has_http_status(error, 409):
            raise
        latest = api(repository, f"runs/{run_id}")
        if latest["run_attempt"] == 1 and latest["status"] != "completed":
            raise ValueError("GitHub returned HTTP 409 and the first attempt is still active") from None
        return None
    return {"run_attempt": 1, "requested_at": now.isoformat(), "deadline_at": deadline.isoformat()}


def prepare(output, repository, branch, *, now=None):
    now = now or datetime.now(timezone.utc)
    recent = (now - timedelta(days=2)).date().isoformat()
    nightlies = api(repository, f"workflows/nightly-test.yml/runs?branch={branch}&event=schedule"
                                f"&created=%3E%3D{recent}&per_page=25")["workflow_runs"]
    runs = api(repository, f"workflows/pipeline-performance.yml/runs?branch={branch}&event=workflow_run"
                            f"&created=%3E%3D{recent}&per_page=100")["workflow_runs"]
    nightlies = [first_attempt(repository, run) for run in nightlies]
    # The attempt's original status and timestamps survive later reruns of the workflow.
    rerun_ids = {run["id"] for run in runs if run["run_attempt"] > 1}
    candidates = [first_attempt(repository, run) for run in runs]
    observed = actions_health.observations(now, nightlies, candidates, {})
    cancellations = {}
    output.mkdir(parents=True)
    check = {"budget_cancellations": cancellations, "cancellation_errors": [], "report_errors": []}
    for observation in observed:
        run = observation["performance"]
        if observation["status"] in {"performance-running", "performance-expired"} and run["status"] != "completed":
            try:
                receipt = stop_expiring_run(repository, run, now)
                if receipt:
                    cancellations.setdefault(str(run["id"]), receipt)
            except (OSError, subprocess.SubprocessError, ValueError) as error:
                reason = str(error)
                check["cancellation_errors"].append({"run_id": run["id"], "reason": reason})
                print(f"::warning::Cancellation for performance run {run['id']}: {reason}", file=sys.stderr)
    try:
        # History supplements current evidence and never precedes deadline enforcement.
        cancellations.update(previous_budget_cancellations(repository, branch, observed, now))
    except (OSError, subprocess.SubprocessError, ValueError) as error:
        check["cancellation_history_error"] = str(error)
    reports, removed_reports = {}, set()
    for observation in observed:
        performance = observation["performance"]
        if observation["status"] not in {"report-missing", "performance-expired"} or performance["status"] != "completed":
            continue
        run_id = performance["id"]
        try:
            reports[run_id], removed = read_scheduled_report(repository, performance, run_id in rerun_ids)
        except (OSError, subprocess.SubprocessError) as error:
            check["report_errors"].append({"run_id": run_id, "reason": str(error)})
            if observation["status"] != "performance-expired":
                observation["status"] = "report-unreadable"
                check["observations"] = [observe_run(item, now) for item in observed]
                runner.write_json(output / "health.json", check)
                raise
            continue
        if removed:
            removed_reports.add(run_id)

    observed = actions_health.observations(now, nightlies, candidates, reports)
    for observation in observed:
        if observation["status"] != "report-missing":
            continue
        run = observation["performance"]
        if run["id"] in removed_reports:
            observation["status"] = "report-removed-after-rerun"
        elif run.get("conclusion") == "cancelled" and str(run["id"]) in cancellations:
            observation["status"] = "performance-budget-cancelled"
    check["observations"] = [observe_run(item, now) for item in observed]
    runner.write_json(output / "health.json", check)

    previous = latest_state(repository, branch, now)
    legacy = previous.get("observation", {})
    prior_slots = previous.get("settled_slots", {legacy["due_at"]: legacy} if legacy.get("due_at") else {})
    slots = {item["due_at"]: prior_slots[item["due_at"]] for item in observed if item["due_at"] in prior_slots}
    health = actions_health.select_observation(observed, slots)
    performance = health["performance"]
    health["observed_at"] = now.isoformat()
    slot = health["due_at"]
    marker = actions_health.observation_marker(health)
    if slots.get(slot) == marker:
        active, healthy = {}, set()
    else:
        active, healthy = notifications.signals(health)
        # A late report can add alerts, but it cannot prove recovery from a newer observation.
        if any(other > slot for other in slots):
            healthy = set()
    confirmed, pending = reconcile_pending(previous, active, healthy)
    # An ambiguous card suppresses repeat delivery of its signal, but a material escalation can proceed.
    eligible = {key: signal for key, signal in active.items()
                if key not in pending or (isinstance(pending[key], dict) and notifications.escalated(pending[key], signal))}
    notices, next_state = notifications.changes(confirmed, eligible, healthy - pending.keys())
    runner.write_json(output / "health.json", {**check, **observe_run(health, now), "pending_delivery": pending})
    settled = (health["status"] in {"reported", "report-removed-after-rerun"}
               or health["status"] in notifications.WORKFLOW_ANOMALIES)
    if settled:
        slots[slot] = marker
    state = {"updated_at": now.isoformat(), "active": next_state, "settled_slots": slots, "pending": pending}
    if notices:
        target = performance or health["nightly"]
        details = (f"https://github.com/{repository}/actions/runs/{target['id']}/attempts/1" if target
                   else f"https://github.com/{repository}/actions/workflows/pipeline-performance.yml")
        runner.write_json(output / "card.json", notifications.card(health, notices, details))
        base = {**previous, "active": confirmed, "pending": pending}
        accepted_pending = {key: attempted for key, attempted in pending.items()
                            if next_state.get(key) == confirmed.get(key)}
        runner.write_json(output / "next-state.json", {**state, "pending": accepted_pending, "previous": base})
    elif (slots != prior_slots or pending != previous.get("pending", {})
          or now - actions_health.timestamp(previous["updated_at"]) >= timedelta(days=1)):
        runner.write_json(output / "state.json", state)


def observe_run(health, now):
    performance = health["performance"]
    expired = False
    if performance and performance.get("run_started_at") and health["status"] != "performance-queued":
        end = actions_health.timestamp(performance["updated_at"]) if performance["status"] == "completed" else now
        expired = end > actions_health.workflow_deadline(performance)
    return {
        "status": health["status"], "due_at": health["due_at"],
        "nightly_run_id": health["nightly"]["id"] if health["nightly"] else None,
        "performance_run_id": performance["id"] if performance else None,
        "performance_deadline_exceeded": expired,
        "queue_seconds": (now - actions_health.timestamp(performance["created_at"])).total_seconds()
        if health["status"] == "performance-queued" else None,
    }


def send(output, webhook):
    card = output / "card.json"
    if not card.exists():
        return
    proposed = catalog.read_json(output / "next-state.json")
    intent_sha = catalog.digest(proposed)

    def save_receipt(status):
        runner.write_json(output / "delivery.json", {"status": status, "intent_sha256": intent_sha})

    if not webhook.startswith("https://"):
        save_receipt("rejected")
        raise ValueError("Configure the scheduled Feishu webhook")
    request = Request(webhook, card.read_bytes(), {"Content-Type": "application/json"}, method="POST")
    try:
        with urlopen(request, timeout=15) as response:
            receipt = json.loads(response.read(4096))
    except HTTPError as error:
        error.close()
        if error.code == 429:
            save_receipt("rejected")
            raise ValueError("Feishu rate limit rejected the notification") from None
        save_receipt("unknown")
        raise ValueError("Feishu receipt unavailable") from None
    except (OSError, ValueError):
        save_receipt("unknown")
        raise ValueError("Feishu receipt unavailable") from None
    code = receipt.get("code", receipt.get("StatusCode")) if isinstance(receipt, dict) else None
    if type(code) is not int:
        save_receipt("unknown")
        raise ValueError("Feishu receipt did not confirm a delivery result")
    if code != 0:
        save_receipt("rejected")
        raise ValueError("Feishu did not accept the notification")
    save_receipt("accepted")
    committed = output / "accepted-state.json"
    runner.write_json(committed, state_from_intent(proposed))
    committed.replace(output / "state.json")
    (output / "next-state.json").unlink()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("prepare", "send"))
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    try:
        if args.command == "prepare":
            prepare(args.output, os.environ["GITHUB_REPOSITORY"], os.environ["GITHUB_REF_NAME"])
        else:
            send(args.output, os.environ.get("PERFORMANCE_FEISHU_WEBHOOK", ""))
    except (OSError, ValueError, KeyError, subprocess.SubprocessError) as error:
        print(f"pipeline-performance-health: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
