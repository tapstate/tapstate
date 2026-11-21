"""Prepare the fixed manifest and clock budget for an Actions benchmark run."""

import argparse
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time

import catalog
import runner


RUNNER_PROFILE = "ubuntu-24.04-x64-op"
BUDGET_SECONDS = 7 * 60 * 60
REPORT_RESERVE_SECONDS = 600
TEARDOWN_RESERVE_SECONDS = 120
BUILD_RESERVE_SECONDS = 600
DEFAULT_BATCH_SECONDS = 5 * 60 * 60
DEFAULT_SLOTS = 8


def selection(mode, group="", filters="", case_ids=""):
    parameters = {"group": group.strip(), "filter": filters.strip(), "ids": case_ids.strip()}
    supplied = {key for key, value in parameters.items() if value}
    if mode == "all" and not supplied:
        return []
    if mode not in parameters or supplied != {mode}:
        raise ValueError("Use only the matching input for group, filter or IDs; all takes no input")
    value = parameters[mode]
    if mode == "group":
        return ["--where", f"group={value}"]
    flag = "--where" if mode == "filter" else "--id"
    return [argument for item in re.split(r"[\s,]+", value) for argument in (flag, item)]


def eligible_event(event_name, event, attempt):
    if attempt != 1:
        return False
    if event_name == "workflow_dispatch":
        return True
    if event_name != "workflow_run":
        return False
    upstream = event.get("workflow_run", {})
    repository = event.get("repository", {})
    return (
        event.get("action") == "completed"
        and upstream.get("name") == "Nightly Test"
        and upstream.get("event") == "schedule"
        and upstream.get("run_attempt") == 1
        and upstream.get("head_branch") == repository.get("default_branch")
        and (upstream.get("head_repository") or {}).get("id") == repository.get("id")
        and repository.get("id") is not None
    )


def timestamp(value):
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def run_timing(run, upstream):
    started = timestamp(run["run_started_at"])
    created = timestamp(run["created_at"])
    if started < created:
        raise ValueError("Workflow start precedes its creation")
    business_time = timestamp(upstream["created_at"]) if upstream else created
    return {
        "created_at": created.isoformat(),
        "started_at": started.isoformat(),
        "queue_seconds": (started - created).total_seconds(),
        "deadline_epoch_seconds": int(started.timestamp()) + BUDGET_SECONDS,
        "business_date": business_time.astimezone(timezone(timedelta(hours=8))).date().isoformat(),
        "nightly": {"run_id": upstream["id"], "attempt": upstream["run_attempt"],
                    "completed_at": upstream["updated_at"], "url": upstream["html_url"]} if upstream else None,
    }


def remaining_seconds(timing, *, reserve=0):
    return int(timing["deadline_epoch_seconds"] - time.time() - reserve)


def prepare(args, event_name, event, attempt, run):
    if not eligible_event(event_name, event, attempt["attempt"]):
        raise ValueError("This event or attempt cannot start performance work")
    if run["id"] != attempt["run_id"] or run["run_attempt"] != attempt["attempt"]:
        raise ValueError("Workflow metadata belongs to another run attempt")
    upstream = event["workflow_run"] if event_name == "workflow_run" else None
    timing = run_timing(run, upstream)
    output = args.output
    output.mkdir(parents=True)
    inputs = (event.get("inputs") or {}) if upstream is None else {}
    scope = inputs.get("mode", "all")
    runner.write_json(output / "run.json", {"github_attempt": attempt, "scope": scope, "timing": timing})
    flags = selection(scope, inputs.get("group", ""), inputs.get("filters", ""), inputs.get("case_ids", ""))
    budget = remaining_seconds(timing, reserve=REPORT_RESERVE_SECONDS + TEARDOWN_RESERVE_SECONDS)
    if budget <= BUILD_RESERVE_SECONDS:
        raise ValueError("Insufficient workflow time remains to prepare a measurement batch")
    manifest_path = output / "manifest.json"
    result = subprocess.run(
        [sys.executable, "-B", str(Path(runner.__file__)), "allocate", *flags,
         "--formal-workloads", str(args.workloads), "--estimates", str(args.estimates),
         "--environment", RUNNER_PROFILE, "--slots", str(DEFAULT_SLOTS), "--deadline-seconds", str(budget),
         "--batch-seconds", str(DEFAULT_BATCH_SECONDS), "--reserve-seconds", str(BUILD_RESERVE_SECONDS),
         "--bind-github-attempt", "--output", str(manifest_path)],
        check=False,
    )
    if not manifest_path.exists():
        return result.returncode
    manifest = catalog.read_json(manifest_path)
    print("Selected cases:", ", ".join(manifest["selected_ids"]))
    if result.returncode:
        return result.returncode
    if manifest.get("scale") != "formal" or any(
        entry.get("scale") != "formal" for batch in manifest["batches"] for entry in batch["entries"]
    ):
        raise ValueError("Actions requires a formal workload manifest")
    with Path(os.environ["GITHUB_OUTPUT"]).open("a", encoding="utf-8") as outputs:
        print("batches=" + json.dumps([batch["index"] for batch in manifest["batches"]]), file=outputs)
        print("measurement_stop=" + str(timing["deadline_epoch_seconds"]
                                       - REPORT_RESERVE_SECONDS - TEARDOWN_RESERVE_SECONDS), file=outputs)
    return 0


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workloads", type=Path, required=True)
    parser.add_argument("--estimates", type=Path, required=True)
    parser.add_argument("--run", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    try:
        return prepare(args, os.environ["GITHUB_EVENT_NAME"], catalog.read_json(Path(os.environ["GITHUB_EVENT_PATH"])),
                       runner.github_attempt(), catalog.read_json(args.run))
    except (OSError, ValueError, KeyError) as error:
        print(f"pipeline-performance: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
