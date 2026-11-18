"""Select, allocate and replay versioned synchronization benchmark cases."""

import argparse
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import threading
import uuid

import catalog as definitions


ROOT = definitions.BENCHMARKS.parent
RUNNABLE = {
    "copy.snapshot.dummy-baseline.standard",
    "copy.snapshot.basic.standard",
    "copy.live.basic.standard",
    "copy.backlog.fixed-iud.standard",
}
DIMENSIONS = {
    "group", "operators", "phase", "mode", "profile", "source", "target",
    "environments", "purposes", "workload", "data_features", "tags", "execution",
}


def select(cases, explicit, where, suite, *, allow_empty=False):
    by_id = {case["id"]: case for case in cases}
    requested = set()
    for reference in explicit:
        case_id, separator, version = reference.partition("@")
        if case_id not in by_id:
            raise ValueError(f"Unknown case ID {case_id}")
        if case_id in requested:
            raise ValueError(f"Duplicate explicit case ID {case_id}")
        if separator and version != str(by_id[case_id]["case_version"]):
            raise ValueError(f"Version mismatch for {reference}")
        requested.add(case_id)

    filters = {}
    for condition in where:
        name, separator, value = condition.partition("=")
        if not separator or name not in DIMENSIONS:
            raise ValueError(f"Unknown case filter {condition}")
        if not any(value in values(case[name]) for case in cases):
            raise ValueError(f"Unknown value for {name}: {value}")
        filters.setdefault(name, set()).add(value)
    result = [
        case for case in cases
        if (not requested or case["id"] in requested)
        and (suite != "basic" or case["id"] in RUNNABLE)
        and (suite != "memory" or "memory-regression" in case["tags"])
        and all(wanted.intersection(values(case[name])) for name, wanted in filters.items())
    ]
    if not result and not allow_empty:
        raise ValueError("Selection matched zero cases")
    return result


def values(value):
    return value if isinstance(value, list) else [value]


def identity(catalog):
    return {key: catalog[key] for key in ("code_sha", "catalog_sha256", "schema_version", "source_dirty")}


def make_entries(cases, args):
    return [
        {
            "case": case,
            "repeats": args.repeats,
            "scale": "qualification",
            "rows": args.rows,
            "backlog_events": args.backlog_events,
            "rate": args.rate,
            "warmup_seconds": args.warmup_seconds,
            "measurement_seconds": args.measurement_seconds,
            "timeout_seconds": args.timeout_seconds,
            "mix": args.mix,
        }
        for case in cases
    ]


def allocate(entries, estimates, slots, limit, deadline, reserve):
    if slots < 1 or limit < 1 or deadline < 1 or not 0 <= reserve < deadline:
        raise ValueError("Specify positive capacity, with a reserve shorter than the total deadline")
    maximum = min(limit, deadline - reserve)
    batches = [{"index": index, "estimated_seconds": 0, "entries": []} for index in range(slots)]
    pending = []
    known = []
    for entry in entries:
        case_id = entry["case"]["id"]
        seconds = estimates.get(case_id)
        if seconds is None:
            pending.append({"id": case_id, "reason": "missing whole-case estimate"})
        elif type(seconds) is not int or seconds < 1:
            raise ValueError(f"Estimate for {case_id} must be a positive whole-case duration")
        else:
            known.append((seconds, case_id, entry))
    for seconds, case_id, entry in sorted(known, key=lambda item: (-item[0], item[1])):
        batch = min(batches, key=lambda row: (row["estimated_seconds"], row["index"]))
        if batch["estimated_seconds"] + seconds > maximum:
            pending.append({"id": case_id, "reason": "exceeds available batch capacity"})
        else:
            batch["estimated_seconds"] += seconds
            batch["entries"].append(entry)
    longest = max(batch["estimated_seconds"] for batch in batches)
    return {
        "batches": [batch for batch in batches if batch["entries"]],
        "unallocated": pending,
        "longest_seconds": longest,
        "remaining_seconds": deadline - reserve - longest,
        "reserve_seconds": reserve,
    }


def checked_entries(entries):
    if not entries:
        raise ValueError("Execution requires at least one selected case")
    for entry in entries:
        case = entry["case"]
        if case["id"] not in RUNNABLE:
            raise ValueError(f"Execution is pending for {case['id']}")
        if entry["mix"] is not None:
            source = case["workload_spec"].get("incremental", {})
            if not source.get("allow_manual_mix_override", False):
                raise ValueError(f"Case does not admit a manual I/U/D mix: {case['id']}")
    return entries


def replay(catalog, manifest, index):
    if manifest.get("manifest_sha256") != definitions.digest(
            {key: value for key, value in manifest.items() if key != "manifest_sha256"}):
        raise ValueError("Manifest digest does not match its contents")
    if manifest["identity"] != identity(catalog):
        raise ValueError("Manifest is bound to a different checkout or catalog")
    if manifest["unallocated"]:
        raise ValueError("Refusing an incomplete allocation")
    ids = [entry["case"]["id"] for batch in manifest["batches"] for entry in batch["entries"]]
    if len(ids) != len(set(ids)) or sorted(ids) != sorted(manifest["selected_ids"]):
        raise ValueError("Manifest must assign every selected case exactly once")
    batches = [batch for batch in manifest["batches"] if batch["index"] == index]
    if len(batches) != 1:
        raise ValueError(f"Expected one batch with index {index}")
    current = {case["id"]: case for case in catalog["cases"]}
    entries = batches[0]["entries"]
    if any(current.get(entry["case"]["id"]) != entry["case"] for entry in entries):
        raise ValueError("Batch includes a mismatched case definition")
    return checked_entries(entries)


def run_entries(entries, binding, args):
    checked_entries(entries)
    if not args.boot_jar.is_file() or not args.connectors.is_dir():
        raise ValueError("Supply the candidate OP boot jar and pinned connector directory")
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    run_id = uuid.uuid4().hex
    request = {
        "run_id": run_id,
        "identity": binding,
        "entries": entries,
        "connector_dir": str(args.connectors.resolve()),
        "output": str(output),
    }
    request_file = output / "request.json"
    write_json(request_file, request)
    command = [
        "mvn", "-B", "-pl", "e2e", "-am", "-Dtest=BenchmarkRunIT", "-Dsurefire.failIfNoSpecifiedTests=false",
        f"-Dtapstate.e2e.boot-jar={args.boot_jar.resolve()}",
        f"-Dtapstate.benchmarks.request={request_file}",
        f"-Dtapstate.e2e.resource-owner={run_id}",
        "-Dapi.version=1.44", "test",
    ]
    if args.offline:
        command.insert(1, "-o")
    upper_bound = 600 + sum(entry["repeats"] * (entry["timeout_seconds"] + 45) for entry in entries)
    with (output / "maven.log").open("w", encoding="utf-8") as log:
        process = subprocess.Popen(command, cwd=ROOT, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
        done = threading.Event()
        sampler = threading.Thread(target=sample_database, args=(done, run_id, output / "mongo-resource.jsonl"),
                                   daemon=True)
        sampler.start()
        try:
            code = process.wait(timeout=upper_bound)
        except (subprocess.TimeoutExpired, KeyboardInterrupt):
            stop_process(process)
            code = 124
        finally:
            done.set()
            sampler.join(timeout=8)
    results = []
    for entry in entries:
        case = entry["case"]
        for repeat in range(1, entry["repeats"] + 1):
            relative = Path(case["id"]) / str(repeat) / "result.json"
            file = output / relative
            if file.is_file():
                try:
                    results.append(definitions.read_json(file))
                    continue
                except (OSError, ValueError):
                    status, reason = "measurement-incomplete", "Could not read the raw result file"
            else:
                status, reason = "not-run", f"Runner exited with {code} before the raw repeat was recorded"
            results.append({
                "run_id": run_id, "case_id": case["id"], "case_version": case["case_version"],
                "repeat": repeat, "status": status, "reason": reason,
            })
    summary = {"run_id": run_id, "identity": binding, "process_exit": code, "results": results}
    write_json(output / "summary.json", summary)
    print(json.dumps({"run_id": run_id, "results": str(output / "summary.json"),
                      "statuses": [result["status"] for result in results]}))
    return 1 if code or any(result["status"] != "completed" for result in results) else 0


def sample_database(done, owner, destination):
    from datetime import datetime, timezone

    while not done.wait(2):
        try:
            listing = subprocess.run(
                ["docker", "ps", "-q", "--filter", f"label=io.tapstate.e2e.resource-owner={owner}"],
                text=True, capture_output=True, check=True, timeout=3,
            )
            containers = listing.stdout.split()
            if containers:
                stats = subprocess.run(
                    ["docker", "stats", "--no-stream", "--format", "{{json .}}", *containers],
                    text=True, capture_output=True, check=True, timeout=4,
                )
                with destination.open("a", encoding="utf-8") as output:
                    for line in stats.stdout.splitlines():
                        output.write(json.dumps({"observed_at": datetime.now(timezone.utc).isoformat(),
                                                 "scope": "owned shared MongoDB container",
                                                 "docker_stats": json.loads(line)}) + "\n")
        except (OSError, ValueError, subprocess.SubprocessError):
            # Contention or shutdown can cause a missing resource sample.
            continue


def stop_process(process):
    if process.poll() is None:
        os.killpg(process.pid, signal.SIGTERM)
        try:
            process.wait(timeout=15)
        except subprocess.TimeoutExpired:
            os.killpg(process.pid, signal.SIGKILL)
            process.wait()


def write_json(path, result):
    path.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def positive(value):
    parsed = int(value)
    if parsed < 1:
        raise argparse.ArgumentTypeError("Expected a positive integer")
    return parsed


def mix(value):
    try:
        counts = [int(part) for part in value.split("/")]
        if len(counts) == 3 and min(counts) >= 0 and sum(counts) == 100:
            return counts
    except ValueError:
        pass
    raise argparse.ArgumentTypeError("Expected three nonnegative I/U/D percentages summing to 100")


def add_selection(parser):
    parser.add_argument("--id", action="append", default=[], help="Exact case ID, optionally with @version.")
    parser.add_argument("--where", action="append", default=[], help="Repeat field=value filters.")
    parser.add_argument("--suite", choices=("all", "basic", "memory"), default="all")


def add_load(parser):
    parser.add_argument("--repeats", type=positive, default=3)
    parser.add_argument("--rows", type=positive, default=32)
    parser.add_argument("--backlog-events", type=positive, default=20)
    parser.add_argument("--rate", type=positive, default=10)
    parser.add_argument("--warmup-seconds", type=positive, default=1)
    parser.add_argument("--measurement-seconds", type=positive, default=2)
    parser.add_argument("--timeout-seconds", type=positive, default=45)
    parser.add_argument("--mix", type=mix, help="Override the allowed CDC mix, e.g. 100/0/0.")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    listing = sub.add_parser("list")
    add_selection(listing)
    listing.add_argument("--offset", type=int, default=0)
    listing.add_argument("--limit", type=positive, default=100)
    listing.add_argument("--format", choices=("json", "table"), default="table")
    describe = sub.add_parser("describe")
    describe.add_argument("id")
    describe.add_argument("--format", choices=("json", "markdown"), default="markdown")
    allocator = sub.add_parser("allocate")
    add_selection(allocator)
    add_load(allocator)
    allocator.add_argument("--slots", type=positive, required=True)
    allocator.add_argument("--batch-seconds", type=positive, required=True)
    allocator.add_argument("--deadline-seconds", type=positive, required=True)
    allocator.add_argument("--reserve-seconds", type=int, default=0)
    allocator.add_argument("--environment", required=True)
    allocator.add_argument("--estimates", type=Path, required=True)
    allocator.add_argument("--output", type=Path, required=True)
    execute = sub.add_parser("run")
    add_selection(execute)
    add_load(execute)
    execute.add_argument("--manifest", type=Path)
    execute.add_argument("--batch", type=int, default=0)
    execute.add_argument("--connectors", type=Path, required=True)
    execute.add_argument("--boot-jar", type=Path, default=ROOT / "app/target/app-0.8.0-boot.jar")
    execute.add_argument("--output", type=Path, required=True)
    execute.add_argument("--offline", action="store_true")
    args = parser.parse_args()
    try:
        catalog = definitions.build_catalog()
        binding = identity(catalog)
        if args.command == "describe":
            selected = select(catalog["cases"], [args.id], [], "all")[0]
            print(json.dumps({"identity": binding, "case": selected}, indent=2)
                  if args.format == "json" else definitions.render_markdown(catalog, selected))
            return 0
        if args.command == "run" and args.manifest:
            if args.id or args.where or args.suite != "all":
                raise ValueError("Select in the manifest or on the command line")
            entries = replay(catalog, definitions.read_json(args.manifest), args.batch)
            return run_entries(entries, binding, args)
        cases = select(catalog["cases"], args.id, args.where, args.suite, allow_empty=args.command == "list")
        if args.command == "list":
            if args.offset < 0:
                raise ValueError("Offset must be nonnegative")
            page = cases[args.offset:args.offset + args.limit]
            result = {"identity": binding, "total": len(cases), "offset": args.offset, "cases": page}
            print(json.dumps(result, indent=2) if args.format == "json" else format_page(catalog, page, result))
            return 0
        entries = make_entries(cases, args)
        if args.command == "run":
            return run_entries(entries, binding, args)
        estimates = definitions.read_json(args.estimates)
        if estimates["environment"] != args.environment:
            raise ValueError("Timing estimates were recorded for a different environment")
        allocation = allocate(entries, estimates["durations_seconds"], args.slots,
                              args.batch_seconds, args.deadline_seconds, args.reserve_seconds)
        manifest = {
            "identity": binding, "environment": args.environment,
            "selected_ids": [case["id"] for case in cases], **allocation,
        }
        manifest["manifest_sha256"] = definitions.digest(manifest)
        write_json(args.output, manifest)
        print(json.dumps({key: value for key, value in allocation.items() if key != "batches"}))
        return 1 if allocation["unallocated"] else 0
    except (OSError, ValueError, KeyError, subprocess.CalledProcessError) as failure:
        print(f"benchmark-runner: {failure}", file=sys.stderr)
        return 1


def format_page(catalog, page, result):
    start = result["offset"] + 1 if page else 0
    end = result["offset"] + len(page) if page else 0
    return (f"Showing {start}-{end} of {result['total']} matching cases.\n\n"
            + definitions.render_markdown({**catalog, "cases": page}))


if __name__ == "__main__":
    sys.exit(main())
