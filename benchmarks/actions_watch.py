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


def artifact(repository, source, name, filename):
    available = api(repository, f"runs/{source['id']}/artifacts?name={name}&per_page=20")["artifacts"]
    candidates = [item for item in available if item["name"] == name and attempt_artifact(item, source)]
    if len(candidates) != 1 or candidates[0]["size_in_bytes"] > ARTIFACT_LIMIT:
        return {}
    item = candidates[0]
    path = f"repos/{repository}/actions/artifacts/{item['id']}/zip"
    archive = subprocess.check_output(["gh", "api", path], timeout=60)
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


def latest_artifact(repository, branch, name):
    artifacts = api(repository, f"artifacts?name={name}&per_page=50")["artifacts"]
    candidates = sorted(artifacts, key=artifact_order, reverse=True)
    for candidate in candidates:
        reference = candidate.get("workflow_run")
        if not reference or candidate["expired"] or reference["head_branch"] != branch:
            continue
        source = first_attempt(repository, api(repository, f"runs/{reference['id']}"))
        if source["event"] == "schedule" and source["path"] == HEALTH_PATH and attempt_artifact(candidate, source):
            return candidate, source
    return None


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
    if proposed and receipt.get("intent_sha256") == catalog.digest(proposed):
        if receipt["status"] == "rejected":
            return state
        if receipt["status"] == "accepted":
            return proposed
    raise ValueError(f"Delivery from health run {source['id']} is uncertain")


def stop_expiring_run(repository, performance, now):
    deadline = actions_health.workflow_deadline(performance)
    if now < deadline - timedelta(seconds=CONTROL_RESERVE_SECONDS):
        return
    run_id = performance["id"]
    if now < deadline:
        jobs = api(repository, f"runs/{run_id}/attempts/1/jobs?per_page=100")["jobs"]
        unfinished = any(job["status"] != "completed" and
                         (job["name"] == "prepare" or job["name"].startswith("measure ("))
                         for job in jobs)
        if not unfinished:
            return
    live = api(repository, f"runs/{run_id}")
    if live["run_attempt"] != 1 or live["status"] == "completed":
        return
    subprocess.run(["gh", "api", "--method", "POST", f"repos/{repository}/actions/runs/{run_id}/cancel"],
                   check=True, timeout=30)


def prepare(output, repository, branch, *, now=None):
    now = now or datetime.now(timezone.utc)
    recent = (now - timedelta(days=2)).date().isoformat()
    nightlies = api(repository, f"workflows/nightly-test.yml/runs?branch={branch}&event=schedule"
                                f"&created=%3E%3D{recent}&per_page=25")["workflow_runs"]
    runs = api(repository, f"workflows/pipeline-performance.yml/runs?branch={branch}&event=workflow_run"
                            f"&created=%3E%3D{recent}&per_page=100")["workflow_runs"]
    nightlies = [first_attempt(repository, run) for run in nightlies]
    # The attempt's original status and timestamps survive later reruns of the workflow.
    candidates = [first_attempt(repository, run) for run in runs]
    reports = {}
    observed = actions_health.observations(now, nightlies, candidates, reports)
    for observation in observed:
        performance = observation["performance"]
        if observation["status"] == "report-missing":
            reports[performance["id"]] = artifact(repository, performance, "performance-report-1", "report.json")
    observed = actions_health.observations(now, nightlies, candidates, reports)
    output.mkdir(parents=True)
    runner.write_json(output / "health.json", {"observations": [observe_run(item, now) for item in observed]})
    for observation in observed:
        run = observation["performance"]
        if observation["status"] in {"performance-running", "performance-expired"} and run["status"] != "completed":
            stop_expiring_run(repository, run, now)

    previous = latest_state(repository, branch, now)
    previous_marker = previous.get("observation", {})
    health = actions_health.select_observation(observed, previous_marker)
    performance = health["performance"]
    health["observed_at"] = now.isoformat()
    runner.write_json(output / "health.json", {
        **observe_run(health, now), "observations": [observe_run(item, now) for item in observed],
    })
    active, healthy = notifications.signals(health)
    notices, next_state = notifications.changes(previous["active"], active, healthy)
    settled = health["status"] == "reported" or health["status"] in notifications.WORKFLOW_ANOMALIES
    marker = actions_health.observation_marker(health) if settled else previous_marker
    state = {"updated_at": now.isoformat(), "active": next_state, "observation": marker}
    if notices:
        target = performance or health["nightly"]
        details = (f"https://github.com/{repository}/actions/runs/{target['id']}/attempts/1" if target
                   else f"https://github.com/{repository}/actions/workflows/pipeline-performance.yml")
        runner.write_json(output / "card.json", notifications.card(health, notices, details))
        runner.write_json(output / "next-state.json", state)
    elif marker != previous_marker or now - actions_health.timestamp(previous["updated_at"]) >= timedelta(days=1):
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
    (output / "next-state.json").replace(output / "state.json")


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
