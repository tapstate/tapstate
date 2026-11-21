"""Select, allocate and replay versioned synchronization benchmark cases."""

import argparse
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import threading
import time
import uuid

import baseline
import catalog as definitions
import report
import workloads


ROOT = definitions.BENCHMARKS.parent
BASE_CASES = {
    "copy.snapshot.dummy-baseline.standard",
    "copy.snapshot.basic.standard",
    "copy.live.basic.standard",
    "copy.backlog.fixed-iud.standard",
}
OPERATOR_SPECS = {
    "map-basic", "filter-basic", "js-basic", "unwind-basic", "union-basic", "join-basic", "nest-basic",
    "aggregate-basic",
    "mixed-double-map", "join-hot-update", "nest-child-update",
}
DIMENSIONS = {
    "group", "operators", "phase", "mode", "profile", "source", "target",
    "environments", "purposes", "workload", "data_features", "tags", "execution",
}
LOAD_DEFAULTS = {
    "repeats": 3,
    "rows": 32,
    "backlog_events": 20,
    "rate": 10,
    "warmup_seconds": 1,
    "measurement_seconds": 2,
    "timeout_seconds": 45,
    "mix": None,
}
MAX_FIXTURE_INTEGER = 2**31 - 1


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
        and (suite != "basic" or case["id"] in BASE_CASES)
        and (suite != "operators" or operator_scope(case))
        and (suite != "memory" or "memory-regression" in case["tags"])
        and all(wanted.intersection(values(case[name])) for name, wanted in filters.items())
    ]
    if not result and not allow_empty:
        raise ValueError("Selection matched zero cases")
    return result


def values(value):
    return value if isinstance(value, list) else [value]


def specification(case):
    return Path(case["spec"] or "").stem


def operator_scope(case):
    if case["id"] in BASE_CASES or case["id"] == "join.live.hot-update.wide":
        return False
    return specification(case) in OPERATOR_SPECS or (
        case["group"] == "copy" and case["mode"] == "snapshot" and specification(case) == "copy-basic"
    ) or case["id"] == "copy.live.handoff.standard"


def implemented(case):
    return case["id"] in BASE_CASES or operator_scope(case) or "memory-regression" in case["tags"]


def case_listing(case):
    status = "qualification-ready" if implemented(case) else "definition-only"
    return {**case, "execution_status": status}


def identity(catalog):
    return {key: catalog[key] for key in ("code_sha", "catalog_sha256", "schema_version", "source_dirty")}


def make_entries(cases, args):
    load = {name: getattr(args, name, default) for name, default in LOAD_DEFAULTS.items()}
    return [{"case": case, "scale": "qualification", **load} for case in cases]


def resolve_entries(cases, args):
    if not args.formal_workloads:
        return make_entries(cases, args), []
    if any(hasattr(args, name) for name in LOAD_DEFAULTS):
        raise ValueError("Formal workloads do not allow command-line load overrides")
    return workloads.resolve(cases, definitions.read_json(args.formal_workloads), LOAD_DEFAULTS.keys())


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
        if not implemented(case):
            raise ValueError(f"{case['id']}: execution is pending")
        for name in LOAD_DEFAULTS:
            if name != "mix" and (type(entry[name]) is not int or not 1 <= entry[name] <= MAX_FIXTURE_INTEGER):
                raise ValueError(f"--{name.replace('_', '-')} must fit a positive signed 32-bit integer")
        if entry["mix"] is not None:
            source = case["workload_spec"].get("incremental", {})
            if not source.get("allow_manual_mix_override", False):
                raise ValueError(f"Case does not admit a manual I/U/D mix: {case['id']}")
    return entries


def replay(catalog, manifest, index):
    require_clean_manifest_checkout(catalog)
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


def require_clean_manifest_checkout(catalog):
    if catalog["source_dirty"]:
        raise ValueError("Manifest allocation and replay require a clean checkout")


def run_entries(entries, binding, args, manifest=None):
    checked_entries(entries)
    deadline = getattr(args, "stop_at_epoch_seconds", None)
    if deadline is not None and deadline <= time.time():
        raise ValueError("The batch measurement deadline has already expired")
    if not args.boot_jar.is_file() or not args.connectors.is_dir():
        raise ValueError("Supply the candidate OP boot jar and pinned connector directory")
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    run_id = uuid.uuid4().hex
    request = {
        "run_id": run_id,
        "identity": binding,
        "environment": manifest["environment"] if manifest else None,
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
    upper_bound = 600 + sum(
        entry["repeats"] * (2 if "paired-control" in entry["case"]["tags"] else 1)
        * (entry["timeout_seconds"] * 8 + entry["warmup_seconds"] + entry["measurement_seconds"] + 60)
        for entry in entries
    )
    if manifest:
        upper_bound = min(upper_bound, manifest.get("execution_limit_seconds", upper_bound))
    started = time.monotonic()
    started_wall_ns = time.time_ns()
    with (output / "maven.log").open("w", encoding="utf-8") as log:
        process = subprocess.Popen(command, cwd=ROOT, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
        done = threading.Event()
        memory_intervals = {
            profile["mongo_sample_interval_seconds"] for entry in entries
            if (profile := entry["case"]["workload_spec"].get("memory_profile"))
        }
        sampler = threading.Thread(target=sample_database,
                                   args=(done, run_id, output / "mongo-resource.jsonl", min(memory_intervals or {2})),
                                   daemon=True)
        sampler.start()
        code = 1
        try:
            code = wait_for_fixture(process, output, upper_bound, manifest, started, started_wall_ns,
                                    stop_at_epoch_seconds=deadline)
        except (subprocess.TimeoutExpired, KeyboardInterrupt):
            code = 124
        finally:
            try:
                if not stop_process(process):
                    code = code or 1
                    print("Benchmark process group still exists after forced teardown", file=log)
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
                if (output / relative.parent).is_dir() and code == 124:
                    status, reason = "timed-out", "Batch deadline expired during this repeat"
            results.append({
                "run_id": run_id, "case_id": case["id"], "case_version": case["case_version"],
                "repeat": repeat, "status": status, "reason": reason,
            })
    summary = {"run_id": run_id, "identity": binding, "process_exit": code, "results": results,
               "elapsed_seconds": time.monotonic() - started,
               "mongo_resource_file": "mongo-resource.jsonl" if (output / "mongo-resource.jsonl").is_file() else None}
    if manifest:
        summary.update(batch=args.batch, manifest_sha256=manifest["manifest_sha256"])
        if "github_attempt" in manifest and os.environ.get("GITHUB_ACTIONS") == "true":
            summary["github_attempt"] = github_attempt()
    write_json(output / "summary.json", summary)
    print(json.dumps({"run_id": run_id, "results": str(output / "summary.json"),
                      "statuses": [result["status"] for result in results]}))
    return 1 if code or any(result["status"] != "completed" for result in results) else 0


def wait_for_fixture(process, output, fixture_limit, manifest, started, started_wall_ns,
                     *, stop_at_epoch_seconds=None):
    reserve = manifest.get("reserve_seconds", 0) if manifest else 0
    if type(reserve) is not int or reserve < 0:
        raise ValueError("Manifest reserve must be a nonnegative integer")
    if not reserve and stop_at_epoch_seconds is None:
        return process.wait(timeout=fixture_limit)
    wall_limit = (float("inf") if stop_at_epoch_seconds is None else
                  stop_at_epoch_seconds - started_wall_ns / 1_000_000_000)
    if not reserve:
        remaining = started + min(wall_limit, fixture_limit) - time.monotonic()
        if remaining <= 0:
            raise subprocess.TimeoutExpired(process.args, fixture_limit)
        return process.wait(timeout=remaining)
    build_deadline = started + min(wall_limit, reserve)
    total_deadline = started + min(wall_limit, reserve + fixture_limit)
    marker = output / "fixture-started"
    while True:
        observed_at = time.monotonic()
        try:
            marker_wall_ns = marker.stat().st_mtime_ns
        except FileNotFoundError:
            remaining = build_deadline - observed_at
            if remaining <= 0:
                raise subprocess.TimeoutExpired(process.args, reserve)
            try:
                return process.wait(timeout=min(0.1, remaining))
            except subprocess.TimeoutExpired:
                continue
        # Java creates this file in the private run directory at fixture entry.
        # Its filesystem timestamp and the launch timestamp share the host clock.
        marker_offset_ns = marker_wall_ns - started_wall_ns
        if not 0 <= marker_offset_ns <= reserve * 1_000_000_000:
            raise subprocess.TimeoutExpired(process.args, reserve)
        fixture_start = min(observed_at, started + marker_offset_ns / 1_000_000_000)
        break
    remaining = min(fixture_start + fixture_limit, total_deadline) - time.monotonic()
    if remaining <= 0:
        raise subprocess.TimeoutExpired(process.args, fixture_limit + reserve)
    return process.wait(timeout=remaining)


def sample_database(done, owner, destination, interval):
    from datetime import datetime, timezone

    while not done.wait(interval):
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
        except (OSError, ValueError, subprocess.SubprocessError) as failure:
            with destination.open("a", encoding="utf-8") as output:
                output.write(json.dumps({"observed_at": datetime.now(timezone.utc).isoformat(),
                                         "scope": "owned shared MongoDB container",
                                         "status": "sample-missing",
                                         "reason": type(failure).__name__}) + "\n")


def stop_process(process):
    for sig, grace in ((signal.SIGTERM, 15), (signal.SIGKILL, 2)):
        process.poll()
        if not signal_group(process.pid, sig):
            return True
        deadline = time.monotonic() + grace
        while time.monotonic() < deadline:
            process.poll()
            if not signal_group(process.pid, 0):
                return True
            time.sleep(0.05)
    return False


def signal_group(group, sig):
    try:
        os.killpg(group, sig)
    except ProcessLookupError:
        return False
    return True


def write_json(path, result):
    path.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def github_attempt():
    run_id = int(os.environ["GITHUB_RUN_ID"])
    attempt = int(os.environ["GITHUB_RUN_ATTEMPT"])
    if run_id < 1 or attempt < 1:
        raise ValueError("Expected a positive GitHub run ID and attempt")
    return {"run_id": run_id, "attempt": attempt}


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
    parser.add_argument("--suite", choices=("all", "basic", "operators", "memory"), default="all")


def add_load(parser):
    for name in LOAD_DEFAULTS:
        if name == "mix":
            continue
        parser.add_argument("--" + name.replace("_", "-"), type=positive, default=argparse.SUPPRESS)
    parser.add_argument("--mix", type=mix, default=argparse.SUPPRESS,
                        help="Override the allowed CDC mix, e.g. 100/0/0.")
    parser.add_argument("--formal-workloads", type=Path,
                        help="Resolve each selected case from a versioned formal workload registry.")


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
    allocator.add_argument("--bind-github-attempt", action="store_true")
    execute = sub.add_parser("run")
    add_selection(execute)
    add_load(execute)
    execute.add_argument("--manifest", type=Path)
    execute.add_argument("--batch", type=int, default=0)
    execute.add_argument("--connectors", type=Path, required=True)
    execute.add_argument("--boot-jar", type=Path, default=ROOT / "app/target/app-0.8.0-boot.jar")
    execute.add_argument("--output", type=Path, required=True)
    execute.add_argument("--offline", action="store_true")
    execute.add_argument("--stop-at-epoch-seconds", type=positive,
                         help="Stop the benchmark process at this absolute time, leaving time for evidence collection.")
    reporter = sub.add_parser("report")
    reporter.add_argument("--manifest", type=Path, required=True)
    reporter.add_argument("--summary", type=Path, action="append", default=[])
    reporter.add_argument("--baseline", type=Path)
    reporter.add_argument("--output", type=Path, required=True)
    reporter.add_argument("--markdown", type=Path)
    args = parser.parse_args()
    try:
        if args.command == "report":
            reference, reference_status = baseline.load(args.baseline)
            result = report.reconcile(definitions.read_json(args.manifest),
                                      [definitions.read_json(path) for path in args.summary],
                                      reference, baseline_status=reference_status)
            write_json(args.output, result)
            if args.markdown:
                args.markdown.write_text(report.markdown(result), encoding="utf-8")
            return int(bool(result["errors"]) or any(row["status"] != "completed" for row in result["results"]))
        catalog = definitions.build_catalog()
        binding = identity(catalog)
        if args.command == "describe":
            selected = select(catalog["cases"], [args.id], [], "all")[0]
            print(json.dumps({"identity": binding, "case": case_listing(selected)}, indent=2)
                  if args.format == "json" else definitions.render_markdown(catalog, selected))
            return 0
        if args.command == "run" and args.manifest:
            if args.id or args.where or args.suite != "all" or args.formal_workloads:
                raise ValueError("Select the cases and workloads in the manifest or on the command line")
            manifest = definitions.read_json(args.manifest)
            entries = replay(catalog, manifest, args.batch)
            if ("github_attempt" in manifest and os.environ.get("GITHUB_ACTIONS") == "true"
                    and manifest["github_attempt"] != github_attempt()):
                raise ValueError("Manifest belongs to another GitHub run attempt")
            for name in LOAD_DEFAULTS:
                if hasattr(args, name) and any(entry[name] != getattr(args, name) for entry in entries):
                    raise ValueError(f"--{name.replace('_', '-')} conflicts with the manifest batch")
            return run_entries(entries, binding, args, manifest)
        cases = select(catalog["cases"], args.id, args.where, args.suite, allow_empty=args.command == "list")
        if args.command == "list":
            if args.offset < 0:
                raise ValueError("Offset must be nonnegative")
            page = cases[args.offset:args.offset + args.limit]
            result = {"identity": binding, "total": len(cases), "offset": args.offset,
                      "cases": [case_listing(case) for case in page]}
            print(json.dumps(result, indent=2) if args.format == "json" else format_page(catalog, page, result))
            return 0
        entries, missing_workloads = resolve_entries(cases, args)
        if args.command == "run":
            if missing_workloads:
                raise ValueError("; ".join(item["reason"] for item in missing_workloads))
            return run_entries(entries, binding, args)
        require_clean_manifest_checkout(catalog)
        if entries:
            checked_entries(entries)
        estimates = definitions.read_json(args.estimates)
        if estimates["environment"] != args.environment:
            raise ValueError("Timing estimates were recorded for a different environment")
        durations = (workloads.formal_estimates(entries, estimates) if args.formal_workloads
                     else estimates["durations_seconds"])
        allocation = allocate(entries, durations, args.slots,
                              args.batch_seconds, args.deadline_seconds, args.reserve_seconds)
        allocation["unallocated"].extend(missing_workloads)
        manifest = {
            "identity": binding, "environment": args.environment,
            "execution_limit_seconds": min(args.batch_seconds, args.deadline_seconds - args.reserve_seconds),
            "selected_ids": [case["id"] for case in cases], **allocation,
        }
        if args.formal_workloads:
            manifest["scale"] = "formal"
            manifest["whole_case_estimates_seconds"] = durations
            manifest["selected_cases"] = [
                {"id": case["id"], "version": case["case_version"], "scenario": case["description"],
                 "mode": case["mode"], "phase": case["phase"], "profile": case["profile"],
                 "repeats": case["workload_spec"]["repetition"]["count"],
                 "paired_control": "paired-control" in case["tags"]}
                for case in cases
            ]
        if args.bind_github_attempt:
            manifest["github_attempt"] = github_attempt()
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
