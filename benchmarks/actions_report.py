"""Publish a same-attempt Actions report even when some batch evidence is absent."""

import argparse
from collections import Counter
from datetime import datetime, timezone
from html import escape
import json
import os
import re
from pathlib import Path
import sys

import baseline
import catalog
import report
import runner
import verdict


CACHE_SETUP_STEPS = {"Restore the selected Maven cache", "Verify the selected Maven cache"}


def missing_batch_reason(folder, job):
    if (folder / "expired.txt").is_file():
        return "batch expired before measurement"
    failed_steps = job.get("failed_steps", [])
    if failed_steps and failed_steps[0] not in {"Build and measure the fixed batch", "Upload batch evidence"}:
        return f"batch setup failed ({failed_steps[0]})"
    if "Upload batch evidence" in failed_steps:
        return "batch evidence upload failed"
    if job.get("conclusion") == "failure":
        return "batch failed before writing a summary"
    return "batch summary missing or unreadable"


def maven_seed_details(artifacts, attempt, manifest, job_results, jobs_by_name, artifact_index):
    """Read same-attempt evidence, independently of whether the cache was saved."""
    if not manifest or manifest["unallocated"] or "maven-seed" not in job_results:
        return None, None
    name = f"performance-maven-seed-{attempt['attempt']}"
    result = {"name": name, **artifact_index.get(name, {}), "outcome": job_results["maven-seed"]}
    receipt = object_or_empty(artifacts / name / "receipt.json")
    if (receipt.get("github_attempt") == attempt and receipt.get("manifest_sha256") == manifest["manifest_sha256"]
            and receipt.get("code_sha") == manifest["identity"]["code_sha"]):
        result["receipt"] = receipt
    else:
        receipt = {}
    if result["outcome"] != "success":
        steps = jobs_by_name.get("Prepare Maven dependencies", {}).get("failed_steps", [])
        reason = receipt.get("reason") or (steps[0] if steps else result["outcome"])
        return result, f"Maven preparation failed ({reason})"
    return result, None


def collect(inputs, artifacts, attempt, job_results, *, jobs=(), artifact_index=None):
    errors = []
    run = object_or_empty(inputs / "run.json")
    if run.get("github_attempt") != attempt:
        errors.append({"reason": "missing or mismatched run preparation evidence"})
        run = {}
    manifest_path = inputs / "manifest.json"
    manifest = object_or_empty(manifest_path) if run else {}
    if not manifest or manifest.get("github_attempt") != attempt:
        errors.append({"reason": "no manifest for this attempt"})
        manifest = None
    if manifest and manifest.get("unallocated"):
        errors.extend(manifest["unallocated"])
    measurements = []
    accepted_artifacts = {}
    batch_evidence = {}
    blocked_cache_batches = set()
    jobs_by_name = {job["name"]: job for job in jobs}
    seed, seed_failure = maven_seed_details(artifacts, attempt, manifest, job_results, jobs_by_name,
                                            artifact_index or {})
    if seed_failure:
        errors.append({"reason": seed_failure})
    cache = (seed or {}).get("receipt", {})
    selected_cache = None
    if seed and not seed_failure:
        if cache.get("cache_key") and cache.get("sha256"):
            selected_cache = {"key": cache["cache_key"], "sha256": cache["sha256"]}
        else:
            errors.append({"reason": "Maven dependency cache receipt missing or mismatched"})
    if manifest and not manifest["unallocated"] and not seed_failure:
        for batch in manifest["batches"]:
            index = batch["index"]
            artifact_name = f"performance-batch-{attempt['attempt']}-{index}"
            folder = artifacts / artifact_name
            batch_evidence[str(index)] = batch_details(folder, artifact_name, artifact_index or {})
            payload = object_or_empty(folder / "run" / "summary.json")
            if not payload:
                job = jobs_by_name.get(f"measure ({index})", {})
                failed_steps = job.get("failed_steps", [])
                if failed_steps and failed_steps[0] in CACHE_SETUP_STEPS:
                    blocked_cache_batches.add(index)
                reason = missing_batch_reason(folder, job)
                errors.append({"batch": index, "reason": reason})
                continue
            if selected_cache is not None and batch_evidence[str(index)].get("maven_cache") != selected_cache:
                errors.append({"batch": index, "reason": "batch Maven cache binding mismatch"})
                continue
            if (payload.get("batch") != index or payload.get("github_attempt") != attempt
                    or payload.get("manifest_sha256") != manifest["manifest_sha256"]
                    or payload.get("identity") != manifest["identity"]
                    or not isinstance(payload.get("results"), list)
                    or any(not isinstance(row, dict) for row in payload["results"])):
                errors.append({"batch": index, "reason": "batch summary binding mismatch"})
                continue
            measurements.append(payload)
            accepted_artifacts[str(index)] = artifact_name
    reference, baseline_status = read_baseline(inputs, run, manifest)
    if manifest and not manifest["unallocated"] and not seed_failure:
        batch_conditions = {
            int(index): {key: evidence[key] for key in ("cpu", "runner_image", "mongo_images") if key in evidence}
            for index, evidence in batch_evidence.items()
        }
        result = report.reconcile(manifest, measurements, reference, baseline_status=baseline_status,
                                  batch_conditions=batch_conditions)
        result["errors"].extend(errors)
    else:
        reasons = {item["id"]: item["reason"] for item in errors if "id" in item}
        unmeasured = [
            {"case_id": case["id"], "case_version": case["version"], "repeat": repeat, "status": "not-run",
             "last_stage": "maven-seed" if seed_failure else "prepare",
             "reason": seed_failure or reasons.get(case["id"], "allocation incomplete"),
             "comparisons": {"status": "result-unavailable"}}
            for case in (manifest or {}).get("selected_cases", [])
            for repeat in range(1, case["repeats"] + 1)
        ]
        result = {"identity": manifest["identity"] if manifest else {},
                  "manifest_sha256": manifest.get("manifest_sha256") if manifest else None,
                  "environment": manifest["environment"] if manifest else None,
                  "baseline": baseline_status, "baseline_id": baseline_status.get("id"),
                  "errors": errors, "results": unmeasured, "statistics": {},
                  "case_verdicts": verdict.case_verdicts(unmeasured, None)}
    result["github_attempt"] = attempt
    result["workflow"] = run
    if paused := run.get("paused_cases"):
        result["paused_cases"] = paused
    result["job_results"] = job_results
    result["artifact_names"] = accepted_artifacts
    result["batch_evidence"] = batch_evidence
    if seed:
        result["maven_seed"] = seed
    result["report_artifact_name"] = f"performance-report-{attempt['attempt']}"
    result["generated_at"] = datetime.now(timezone.utc).isoformat()
    result["selected_ids"] = manifest["selected_ids"] if manifest else []
    result["case_context"] = case_context(manifest) if manifest else {}
    result["case_verdicts"] = result.get("case_verdicts", {})
    missing_reasons = {error["batch"]: error["reason"] for error in errors if "batch" in error}
    for row in result["results"]:
        batch = result["case_context"].get(row["case_id"], {}).get("batch")
        if row["status"] == "missing" and batch in blocked_cache_batches:
            row.update(status="not-run", last_stage="maven-cache", reason=missing_reasons[batch],
                       comparisons={"status": "result-unavailable"})
        elif row["status"] == "missing":
            row.update(last_stage="batch" if batch in missing_reasons else "reconcile",
                       reason=missing_reasons.get(batch, "Expected repeat absent from batch summary"))
        artifact = batch_evidence.get(str(batch))
        if artifact and str(batch) in accepted_artifacts and row.get("run_id"):
            raw_path = f"run/{row['case_id']}/{row['repeat']}/result.json"
            row["evidence"] = {"artifact": artifact["name"], "artifact_id": artifact.get("id"),
                               "result_path": raw_path if (artifacts / artifact["name"] / raw_path).is_file()
                               else "run/summary.json"}
    if blocked_cache_batches:
        result["case_verdicts"] = verdict.case_verdicts(result["results"], reference)
    result["repeat_counts"] = dict(Counter(row["status"] for row in result["results"]))
    cases = result["case_verdicts"].values()
    result["case_counts"] = dict(Counter(row["execution"] for row in cases))
    result["comparison_counts"] = dict(Counter(row["comparison"] for row in cases))
    result["comparison_coverage"] = dict(Counter(comparison_coverage(case) for case in cases))
    return result


def read_baseline(inputs, run, manifest):
    receipt = object_or_empty(inputs / "baseline-receipt.json") if run else {}
    pinned_digest = run.get("baseline_receipt_sha256")
    if pinned_digest and (not receipt or catalog.digest(receipt) != pinned_digest):
        return None, {"status": "unavailable", "reason": "baseline-receipt-binding-mismatch"}
    if receipt.get("status") == "available":
        environment = manifest["environment"] if manifest else receipt.get("environment")
        return baseline.load_pinned(inputs / "baseline.json", receipt, environment)
    if receipt.get("status") == "unavailable" and isinstance(receipt.get("reason"), str):
        return None, receipt
    if run:
        reason = "invalid-baseline-receipt" if receipt else "baseline-receipt-missing"
        return None, {"status": "unavailable", "reason": reason}
    return baseline.load(None)


def comparison_coverage(case):
    if case["comparison"] != "unavailable":
        return "comparable"
    if verdict.INCOMPARABLE.intersection(case["comparison_reasons"]):
        return "incomparable"
    return "unavailable"


def batch_details(folder, name, index):
    details = {"name": name, **index.get(name, {})}
    cache = object_or_empty(folder / "maven-cache.json")
    if cache:
        details["maven_cache"] = cache
    try:
        environment = (folder / "environment.txt").read_text(encoding="utf-8")
    except (OSError, ValueError):
        environment = ""
    patterns = {
        "runner_image": r"^Image (.+)$",
        "cpu": r"^Model name:\s*(.+)$",
        "cpu_count": r"^CPU\(s\):\s+(\d+)",
        "memory_mib": r"^Mem:\s+(\d+)",
        "available_disk": r"^Filesystem[^\n]*\n\S+\s+\S+\s+\S+\s+(\S+)",
        "java": r"^(openjdk version .+)$",
    }
    for key, pattern in patterns.items():
        match = re.search(pattern, environment, re.MULTILINE)
        details[key] = match.group(1).strip() if match else None
    try:
        details["mongo_images"] = catalog.read_json(folder / "mongo-image.json")
    except (OSError, ValueError):
        details["mongo_images"] = None
    return details


def artifact_details(path, run_id):
    artifacts = object_or_empty(path).get("artifacts", []) if path else []
    return {
        item["name"]: {"id": item["id"], "digest": item.get("digest")}
        for item in artifacts
        if item.get("workflow_run", {}).get("id") == run_id and type(item.get("id")) is int
        and isinstance(item.get("name"), str) and item.get("expired") is False
    }


def case_context(manifest):
    context = {case["id"]: case for case in manifest.get("selected_cases", [])}
    for batch in manifest["batches"]:
        for entry in batch["entries"]:
            case = entry["case"]
            case_id = case["id"]
            scope = [suite for suite in ("basic", "operators", "memory")
                     if runner.select([case], [], [], suite, allow_empty=True)]
            context[case_id] = {
                "id": case_id, "version": case["case_version"], "scenario": case.get("description"),
                "mode": case.get("mode"), "profile": case.get("profile"), "phase": case.get("phase"),
                "fields": case.get("workload_spec", {}).get("profile", {}).get("fields"),
                "definition_sha256": case.get("definition_sha256"),
                "suites": scope, "batch": batch["index"], "repeats": entry["repeats"],
                "effective_load": {key: value for key, value in entry.items() if key not in ("case", "repeats")},
            }
    return context


def object_or_empty(path):
    try:
        document = catalog.read_json(path)
        return document if isinstance(document, dict) else {}
    except (OSError, ValueError):
        return {}


def summary(result, repository):
    attempt = result["github_attempt"]
    run_id = attempt["run_id"]
    run_url = f"https://github.com/{repository}/actions/runs/{run_id}/attempts/{attempt['attempt']}"
    receipt = result["baseline"]
    timeline = result["workflow"].get("timing", {})
    nightly = timeline.get("nightly")
    lines = [
        "# Pipeline Performance",
        "",
        f"- Candidate `{result['identity'].get('code_sha', 'unavailable')}`",
        f"- Run [`{run_id}/{attempt['attempt']}`]({run_url})",
        f"- Scope `{result['workflow'].get('scope', 'unavailable')}`",
        f"- Environment `{result['environment'] or 'unavailable'}`",
        f"- Baseline {cell(receipt.get('id'))}",
        f"- Manifest digest {cell(result.get('manifest_sha256'))}",
        f"- Cases selected {len(result['selected_ids'])}",
    ]
    for pause in result.get("paused_cases", []):
        lines.append(f"- Scheduled pause `{pause['id']}@{pause['version']}` · "
                     f"[Issue]({pause['issue']}) · {cell(pause['reason'])}")
    for key, label in (("version", "Baseline version"), ("sha256", "Baseline SHA-256"),
                       ("baseline_commit", "Baseline commit"), ("release_record_commit", "Release record commit"),
                       ("code_sha", "Baseline candidate"), ("environment", "Baseline environment")):
        if receipt.get(key) is not None:
            lines.append(f"- {label} {cell(receipt[key])}")
    if receipt["status"] != "available":
        lines.append(f"- Comparison unavailable ({cell(receipt.get('reason'))})")
    if nightly:
        lines.append(f"- Upstream [Nightly Test #{nightly['run_id']}]({nightly['url']}) completed at {nightly['completed_at']}")
    if timeline:
        started = datetime.fromisoformat(timeline["started_at"])
        elapsed = (datetime.fromisoformat(result["generated_at"]) - started).total_seconds()
        lines.append(f"- Business date {timeline['business_date']}, initial queue {timeline['queue_seconds']:.0f}s, "
                     f"elapsed {elapsed:.0f}s")
    for job in result.get("jobs", []):
        if job["elapsed_seconds"] is not None:
            lines.append(f"- {escape(job['name'])} elapsed {job['elapsed_seconds']:.0f}s ({job['conclusion']})")
    if seed := result.get("maven_seed"):
        lines.append(f"- Maven preparation {cell(seed['outcome'])} · {artifact_link(seed, repository, run_id)}")
        if seed_receipt := seed.get("receipt"):
            lines.append(f"- Maven seed source {cell(seed_receipt.get('source'))}, Central downloads "
                         f"{cell(seed_receipt.get('central_download_requests'))}, attempts with HTTP 429 "
                         f"{cell(seed_receipt.get('central_429_attempts'))}")
            if seed_receipt.get("cache_key"):
                lines.append(f"- Maven cache key {cell(seed_receipt['cache_key'])}")
    if result["repeat_counts"]:
        lines.append("- Repeat outcomes " + ", ".join(
            f"{status} {count}" for status, count in sorted(result["repeat_counts"].items())))
    for key, label in (("case_counts", "Case outcomes"), ("comparison_counts", "Comparison outcomes")):
        if result[key]:
            lines.append(f"- {label} " + ", ".join(
                f"{status} {count}" for status, count in sorted(result[key].items())))
    lines.append("- Comparison coverage " + ", ".join(f"{name} {count}"
                 for name, count in sorted(result["comparison_coverage"].items())))
    lines.append("- [Case execution failures](#case-execution-failures) · "
                 "[Suspected performance regressions](#suspected-performance-regressions) · "
                 f"[Report and raw artifacts]({run_url}#artifacts)")
    if result["batch_evidence"]:
        lines.extend(["", "<details><summary>Batch environments and raw evidence</summary>", "",
                      "| Batch | Evidence | Image | CPU | vCPUs | RAM MiB | Disk available | Java | MongoDB image |",
                      "|---|---|---|---|---:|---:|---|---|---|"])
        for batch, detail in result["batch_evidence"].items():
            lines.append(table(batch, artifact_link(detail, repository, run_id), detail["runner_image"],
                               detail["cpu"], detail["cpu_count"], detail["memory_mib"], detail["available_disk"], detail["java"],
                               detail["mongo_images"]))
        lines.extend(["", "</details>"])
    lines.extend(["", "## Workflow anomalies", ""])
    lines.extend(f"- {escape(item.get('reason', 'Unknown outcome'))}"
                 + (f" ({item['id']})" if "id" in item else
                    f" (batch {item['batch']})" if "batch" in item else "") for item in result["errors"])
    lines.extend(["", "<details><summary>Coverage by case</summary>", "",
                  "| Case | Profile | Repeats | Execution | Comparison | Evidence qualification | Covered measurements |",
                  "|---|---|---:|---|---|---|---|"])
    for case_id in result["selected_ids"]:
        context = result["case_context"].get(case_id, {})
        case = result["case_verdicts"].get(case_id, {})
        measurements = {
            f"{arm} {metric}": sum(group["count"] for group in cohorts)
            for arm, metrics in result["statistics"].get(case_id, {}).items()
            for metric, cohorts in metrics.items()
        }
        lines.append(table(f"{case_id}@{context.get('version', '-')}", context.get("profile"), context.get("repeats"),
                           case.get("execution"), case.get("comparison"), case.get("comparison_reasons") or "-",
                           measurements or None))
    lines.extend(["", "</details>"])
    lines.extend(execution_failures(result, repository, run_id))
    lines.extend(other_execution_outcomes(result, repository, run_id))
    lines.extend(regressions(result, repository, run_id))
    affected = {case_id for case_id, case in result["case_verdicts"].items()
                if case["execution"] == "failed" or case["comparison"] == "suspected-regression"}
    if affected:
        lines.extend(["", "<details><summary>Measurement conditions for affected cases</summary>"])
        for row in result["results"]:
            if row["case_id"] in affected:
                lines.extend(repeat_details(result, row))
        lines.extend(["", "</details>"])
    lines.extend(["", f"Download the full Markdown and JSON from [Artifacts]({run_url}#artifacts).",
                  "", "```sh",
                  f"gh run download {run_id} --repo {repository} --name {result['report_artifact_name']}",
                  "```", ""])
    return "\n".join(lines)


def cell(value):
    if value is None:
        return "unavailable"
    if isinstance(value, (dict, list)):
        value = json.dumps(value, sort_keys=True, ensure_ascii=True)
    return escape(" ".join(str(value).split())).replace("|", "&#124;").replace("`", "&#96;")


def table(*values):
    return "| " + " | ".join(map(cell, values)) + " |"


def artifact_link(artifact, repository, run_id):
    if artifact.get("id"):
        return f"[{artifact['name']}](https://github.com/{repository}/actions/runs/{run_id}/artifacts/{artifact['id']})"
    return artifact["name"]


def case_label(context):
    return f"{context['id']}@{context['version']}"


def evidence_location(result, row, repository, run_id):
    if row.get("last_stage") == "maven-seed" and result.get("maven_seed"):
        return artifact_link(result["maven_seed"], repository, run_id)
    context = result["case_context"][row["case_id"]]
    artifact = result["batch_evidence"].get(str(context.get("batch")))
    if not artifact:
        return None
    return artifact_link(artifact, repository, run_id)


def failure_summary(reason):
    lines = str(reason).strip().splitlines()
    first = lines[0] if lines else "unavailable"
    return first[:240] + "..." if len(first) > 240 else first


def execution_failure_groups(result):
    groups = {}
    for row in result["results"]:
        if row["status"] not in verdict.FAILED_STATUSES:
            continue
        failures = [(item["arm"], {"last_stage": "memory", "reason": item["reason"], "status": "failed"})
                    for item in row.get("memory_failures", [])]
        if not failures:
            failures = [(name, arm) for name, arm in report.arms_for(row).items()
                        if arm["status"] in verdict.FAILED_STATUSES | {"timed-out"}]
        for name, arm in failures or [("repeat", row)]:
            groups.setdefault((arm.get("last_stage") or "unavailable",
                               failure_summary(arm.get("reason") or arm["status"])), []).append((row, name, arm["status"]))
    return sorted(groups.items())


def execution_failures(result, repository, run_id):
    lines = ["", "## Case execution failures", ""]
    for (stage, reason), observations in execution_failure_groups(result):
        count = len({row["case_id"] for row, _, _ in observations})
        lines.extend(["", f"<details><summary>{cell(stage)} · {cell(reason)} — {count} cases</summary>", "",
                      "| Case | Repeat | Arm | Status | Stage | Reason | Evidence |",
                      "|---|---:|---|---|---|---|---|"])
        for row, name, status in observations:
            context = result["case_context"][row["case_id"]]
            lines.append(table(case_label(context), row["repeat"], name, status, stage, reason,
                               evidence_location(result, row, repository, run_id)))
        lines.extend(["", "</details>"])
    return lines


def other_execution_outcomes(result, repository, run_id):
    rows = [row for row in result["results"] if row["status"] not in verdict.FAILED_STATUSES | {"completed"}]
    if not rows:
        return []
    lines = ["", "<details><summary>Timeouts, skipped work and missing repeats</summary>", "",
             "| Case | Repeat | Status | Stage | Reason | Evidence |",
             "|---|---:|---|---|---|---|"]
    for row in rows:
        lines.append(table(case_label(result["case_context"][row["case_id"]]), row["repeat"], row["status"],
                           row.get("last_stage"), failure_summary(row.get("reason") or "unavailable"), evidence_location(result, row, repository, run_id)))
    lines.extend(["", "</details>"])
    return lines


def regression_groups(result):
    groups = {}
    for case_id in result["selected_ids"]:
        for item in result["case_verdicts"].get(case_id, {}).get("metrics", []):
            if item["status"] == "suspected-regression":
                groups.setdefault(item["metric"], []).append((case_id, item))
    return sorted(groups.items())


def regressions(result, repository, run_id):
    lines = ["", "## Suspected performance regressions", ""]
    groups = regression_groups(result)
    if groups:
        lines.append("A case may appear in more than one metric group.")
    for metric, items in groups:
        lines.extend(["", f"<details><summary>{cell(metric)} — {len({case_id for case_id, _ in items})} cases</summary>", "",
                      "| Case | Arm | Baseline | Median | Change | Source | Samples | Supporting repeats | Rules version | Evidence |",
                      "|---|---|---:|---:|---:|---|---|---:|---:|---|"])
        for case_id, item in items:
            lines.append(table(case_label(result["case_context"][case_id]), item["arm"], item["baseline"],
                               item["observed_median"], verdict.format_change(item), item["source"], item["samples"],
                               item["supporting_repeats"], result["case_verdicts"][case_id]["rules_version"],
                               evidence_location(result, {"case_id": case_id}, repository, run_id)))
        lines.extend(["", "</details>"])
    return lines


def repeat_details(result, row):
    context = result["case_context"][row["case_id"]]
    lines = ["", f"**{case_label(context)}, repeat {row['repeat']}** "
              f"— {cell(context.get('scenario'))} (suites {cell(context.get('suites') or None)})", "",
              f"Planned load {cell(context.get('effective_load'))}. Profile fields {cell(context.get('fields'))}.", "",
              "| Arm | Input | Effective rows | Measured rows | Measured fields |",
              "|---|---|---:|---:|---:|"]
    for name, arm in report.arms_for(row).items():
        sizes = arm.get("effective_input_rows", {})
        measured = arm.get("source_data", {}).get("tables", {})
        for table_name in sorted(sizes.keys() | measured.keys()) or [None]:
            actual = measured.get(table_name, {})
            lines.append(table(name, table_name, sizes.get(table_name), actual.get("rows"), actual.get("fields")))
    lines.extend(["", "| Arm | Confirmed output units | Target readback rows | Window source events | Endpoint latency |",
                  "|---|---:|---:|---|---|"])
    for name, arm in report.arms_for(row).items():
        live = arm.get("live_latency", {})
        source = {stream: reading["acknowledged"] for stream, reading in arm.get("source_window", {}).items()
                  if isinstance(reading, dict) and "acknowledged" in reading}
        source = source or arm.get("window_source_events", arm.get("source_captured_changes"))
        latency = live.get("status") or arm.get("measurement_status")
        if live.get("status") == "eligible":
            latency = {key: live.get(key) for key in ("p50Nanos", "p95Nanos", "p99Nanos", "valid", "coverage", "source")}
        lines.append(table(name, arm.get("confirmed_output_units"), arm.get("target_readback_rows"), source, latency))
    lines.extend(["", "| Arm | OP heap limit MiB | OP process peak RSS KiB | Covered OP windows (RSS peak KiB) | Shared MongoDB resident MiB |",
                  "|---|---:|---:|---|---:|"])
    for name, arm in report.arms_for(row).items():
        resources = arm.get("resources", {})
        windows = {window: usage.get("rss_peak_kib") for window, usage in resources.get("windows", {}).items()
                   if usage.get("covered") is True}
        lines.append(table(name, resources.get("op_heap_limit_mib"), arm.get("op_rss_peak_kib"), windows or None,
                           arm.get("mongo_shared_instance_resident_mib")))
    if result["case_verdicts"][row["case_id"]]["comparison"] == "suspected-regression":
        lines.extend(["", f"Repeat comparison {cell(row['comparisons']['status'])}."])
    if row.get("memory_failures"):
        lines.extend(["", f"Memory failure evidence {cell(row['memory_failures'])}."])
    if row.get("evidence"):
        lines.extend(["", f"Repeat evidence path {cell(row['evidence']['result_path'])}."])
    return lines



def jobs_for_report(path):
    if not path:
        return []
    document = object_or_empty(path)
    jobs = []
    for job in document.get("jobs", []):
        started, completed = job.get("started_at"), job.get("completed_at")
        elapsed = None
        if started and completed:
            elapsed = (datetime.fromisoformat(completed) - datetime.fromisoformat(started)).total_seconds()
        jobs.append({"name": job["name"], "started_at": started, "completed_at": completed,
                     "elapsed_seconds": elapsed, "conclusion": job.get("conclusion"),
                     "failed_steps": [step["name"] for step in job.get("steps", [])
                                      if step.get("conclusion") == "failure"]})
    return jobs


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--inputs", type=Path, required=True)
    parser.add_argument("--artifacts", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--jobs", type=Path)
    parser.add_argument("--artifact-index", type=Path)
    args = parser.parse_args()
    args.output.mkdir(parents=True)
    attempt = runner.github_attempt()
    jobs = json.loads(os.environ.get("PERFORMANCE_JOB_RESULTS", "{}"))
    job_details = jobs_for_report(args.jobs)
    index = artifact_details(args.artifact_index, attempt["run_id"])
    result = collect(args.inputs, args.artifacts, attempt, jobs, jobs=job_details, artifact_index=index)
    result["jobs"] = job_details
    runner.write_json(args.output / "report.json", result)
    markdown = summary(result, os.environ["GITHUB_REPOSITORY"])
    (args.output / "report.md").write_text(markdown, encoding="utf-8")
    with Path(os.environ["GITHUB_STEP_SUMMARY"]).open("a", encoding="utf-8") as step_summary:
        step_summary.write(markdown)
    return int(bool(result["errors"]) or any(row["status"] != "completed" for row in result["results"])
               or any(status != "success" for status in jobs.values()))


if __name__ == "__main__":
    sys.exit(main())
