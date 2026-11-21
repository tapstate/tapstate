"""Read scheduled performance evidence from GitHub and prepare a notification receipt."""

import argparse
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
from urllib.request import Request, urlopen

import actions_health
import actions_report
import notifications
import runner


def api(repository, path):
    output = subprocess.check_output(["gh", "api", f"repos/{repository}/actions/{path}"], text=True)
    return json.loads(output)


def artifact(repository, run_id, name, filename):
    available = api(repository, f"runs/{run_id}/artifacts?name={name}&per_page=20")["artifacts"]
    candidates = [item for item in available if item["name"] == name and not item["expired"]]
    if len(candidates) != 1:
        return {}
    with tempfile.TemporaryDirectory() as folder:
        subprocess.run(["gh", "run", "download", str(run_id), "--repo", repository,
                        "--name", name, "--dir", folder], check=True, timeout=60)
        return actions_report.object_or_empty(Path(folder) / filename)


def latest_artifact(repository, branch, name):
    artifacts = api(repository, f"artifacts?name={name}&per_page=50")["artifacts"]
    candidates = sorted(artifacts, key=artifact_order, reverse=True)
    for candidate in candidates:
        reference = candidate.get("workflow_run")
        if not reference or candidate["expired"] or reference["head_branch"] != branch:
            continue
        source = api(repository, f"runs/{reference['id']}")
        if source["event"] == "schedule" and source["path"] == ".github/workflows/pipeline-performance-health.yml":
            return candidate
    return None


def artifact_order(item):
    return item["created_at"], item["id"]


def latest_state(repository, branch, now):
    committed = latest_artifact(repository, branch, "performance-notification-state")
    state = {"updated_at": (now - timedelta(days=1)).isoformat(), "active": {}}
    if committed:
        run_id = committed["workflow_run"]["id"]
        state = artifact(repository, run_id, "performance-notification-state", "state.json")
        if not state:
            raise ValueError(f"Notification state from run {run_id} could not be read")
    intent = latest_artifact(repository, branch, "performance-notification-intent")
    if not intent or (committed and artifact_order(intent) < artifact_order(committed)):
        return state
    run_id = intent["workflow_run"]["id"]
    receipt = artifact(repository, run_id, "performance-notification-receipt", "delivery.json")
    delivery = receipt.get("status")
    if delivery == "rejected":
        return state
    if delivery == "accepted":
        proposed = artifact(repository, run_id, "performance-notification-intent", "next-state.json")
        if proposed:
            return proposed
    raise ValueError(f"Delivery from health run {run_id} is uncertain")


def prepare(output, repository, branch, mention_id):
    now = datetime.now(timezone.utc)
    recent = (now - timedelta(days=2)).date().isoformat()
    nightlies = api(repository, f"workflows/nightly-test.yml/runs?branch={branch}&event=schedule"
                                f"&created=%3E%3D{recent}&per_page=25")["workflow_runs"]
    runs = api(repository, f"workflows/pipeline-performance.yml/runs?branch={branch}&event=workflow_run"
                            f"&created=%3E%3D{recent}&per_page=100")["workflow_runs"]
    nightlies = [api(repository, f"runs/{run['id']}/attempts/1") if run["run_attempt"] != 1 else run for run in nightlies]
    # The attempt's original status and timestamps survive later reruns of the workflow.
    candidates = [api(repository, f"runs/{run['id']}/attempts/1") if run["run_attempt"] != 1 else run for run in runs]
    health = actions_health.assess(now, nightlies, candidates, {})
    performance = health["performance"]
    if health["status"] == "report-missing":
        received = artifact(repository, performance["id"], "performance-report-1", "report.json")
        health = actions_health.assess(now, nightlies, candidates, {performance["id"]: received})
    output.mkdir(parents=True)
    runner.write_json(output / "health.json", {
        "status": health["status"], "due_at": health["due_at"],
        "nightly_run_id": health["nightly"]["id"] if health["nightly"] else None,
        "performance_run_id": performance["id"] if performance else None,
        "queue_seconds": (now - actions_health.timestamp(performance["created_at"])).total_seconds()
        if health["status"] == "performance-queued" else None,
    })
    if health["status"] == "performance-expired" and performance["status"] != "completed":
        live = api(repository, f"runs/{performance['id']}")
        if live["run_attempt"] == 1 and live["status"] != "completed":
            subprocess.run(["gh", "api", "--method", "POST",
                            f"repos/{repository}/actions/runs/{performance['id']}/cancel"], check=True, timeout=30)
    previous = latest_state(repository, branch, now)
    active, healthy = notifications.signals(health)
    notices, next_state = notifications.changes(previous["active"], active, healthy)
    if notices:
        if (any(not notice.get("recovered") for notice in notices)
                and (not re.fullmatch(r"[A-Za-z0-9_-]+", mention_id) or mention_id.lower() == "all")):
            raise ValueError("Configure the verified Feishu recipient ID for performance alerts")
        target = performance or health["nightly"]
        details = (f"https://github.com/{repository}/actions/runs/{target['id']}/attempts/1" if target
                   else f"https://github.com/{repository}/actions/workflows/pipeline-performance.yml")
        runner.write_json(output / "card.json", notifications.card(health, notices, mention_id, details))
        runner.write_json(output / "next-state.json", {"updated_at": now.isoformat(), "active": next_state})
    elif now - actions_health.timestamp(previous["updated_at"]) >= timedelta(days=1):
        runner.write_json(output / "state.json", {"updated_at": now.isoformat(), "active": previous["active"]})


def send(output, webhook):
    card = output / "card.json"
    if not card.exists():
        return
    if not webhook.startswith("https://"):
        runner.write_json(output / "delivery.json", {"status": "rejected"})
        raise ValueError("Configure the scheduled Feishu webhook")
    request = Request(webhook, card.read_bytes(), {"Content-Type": "application/json"}, method="POST")
    try:
        with urlopen(request, timeout=15) as response:
            receipt = json.loads(response.read(4096))
    except (OSError, ValueError):
        runner.write_json(output / "delivery.json", {"status": "unknown"})
        raise ValueError("Feishu receipt unavailable") from None
    code = receipt.get("code", receipt.get("StatusCode")) if isinstance(receipt, dict) else None
    if type(code) is not int:
        runner.write_json(output / "delivery.json", {"status": "unknown"})
        raise ValueError("Feishu receipt did not confirm a delivery result")
    if code != 0:
        runner.write_json(output / "delivery.json", {"status": "rejected"})
        raise ValueError("Feishu did not accept the notification")
    runner.write_json(output / "delivery.json", {"status": "accepted"})
    (output / "next-state.json").replace(output / "state.json")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("prepare", "send"))
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    try:
        if args.command == "prepare":
            prepare(args.output, os.environ["GITHUB_REPOSITORY"], os.environ["GITHUB_REF_NAME"],
                    os.environ.get("PERFORMANCE_FEISHU_MENTION_ID", ""))
        else:
            send(args.output, os.environ.get("PERFORMANCE_FEISHU_WEBHOOK", ""))
    except (OSError, ValueError, KeyError, subprocess.SubprocessError) as error:
        print(f"pipeline-performance-health: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
