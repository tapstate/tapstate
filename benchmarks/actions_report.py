"""Publish a same-attempt Actions report even when some batch evidence is absent."""

import argparse
from collections import Counter
from datetime import datetime, timezone
from html import escape
import json
import os
from pathlib import Path
import sys

import baseline
import catalog
import report
import runner
import verdict


def collect(inputs, artifacts, attempt, job_results):
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
    if manifest and not manifest["unallocated"]:
        for batch in manifest["batches"]:
            index = batch["index"]
            artifact_name = f"performance-batch-{attempt['attempt']}-{index}"
            folder = artifacts / artifact_name
            payload = object_or_empty(folder / "run" / "summary.json")
            if not payload:
                reason = ("batch expired before measurement" if (folder / "expired.txt").is_file()
                          else "batch summary missing or unreadable")
                errors.append({"batch": index, "reason": reason})
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
    baseline_path = inputs / "baseline.json"
    baseline_receipt = inputs / "baseline-receipt.json"
    if run and baseline_receipt.is_file():
        receipt = object_or_empty(baseline_receipt)
        if receipt.get("status") == "available":
            reference, baseline_status = baseline.load(baseline_path, identity=receipt)
        else:
            reference, baseline_status = None, receipt or {"status": "unavailable", "reason": "invalid-baseline-receipt"}
    else:
        reference, baseline_status = baseline.load(None)
    if manifest and not manifest["unallocated"]:
        result = report.reconcile(manifest, measurements, reference, baseline_status=baseline_status)
        result["errors"].extend(errors)
    else:
        reasons = {item["id"]: item["reason"] for item in errors if "id" in item}
        unmeasured = [
            {"case_id": case["id"], "case_version": case["version"], "repeat": repeat, "status": "not-run",
             "last_stage": "prepare", "reason": reasons.get(case["id"], "allocation incomplete"),
             "comparisons": {"status": "result-unavailable"}}
            for case in (manifest or {}).get("selected_cases", [])
            for repeat in range(1, case["repeats"] + 1)
        ]
        result = {"identity": manifest["identity"] if manifest else {},
                  "environment": manifest["environment"] if manifest else None,
                  "baseline": baseline_status, "baseline_id": baseline_status.get("id"),
                  "errors": errors, "results": unmeasured, "statistics": {},
                  "case_verdicts": verdict.case_verdicts(unmeasured, None)}
    result["github_attempt"] = attempt
    result["workflow"] = run
    result["job_results"] = job_results
    result["artifact_names"] = accepted_artifacts
    result["generated_at"] = datetime.now(timezone.utc).isoformat()
    result["selected_ids"] = manifest["selected_ids"] if manifest else []
    result["case_context"] = case_context(manifest) if manifest else {}
    result["case_verdicts"] = result.get("case_verdicts", {})
    result["repeat_counts"] = dict(Counter(row["status"] for row in result["results"]))
    cases = result["case_verdicts"].values()
    result["case_counts"] = dict(Counter(row["execution"] for row in cases))
    result["comparison_counts"] = dict(Counter(row["comparison"] for row in cases))
    return result


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
        f"- Baseline {escape(receipt.get('id', 'unavailable'))}",
        f"- Cases selected {len(result['selected_ids'])}",
        "- [Case execution failures](#case-execution-failures) and "
        "[suspected performance regressions](#suspected-performance-regressions)",
    ]
    if receipt["status"] != "available":
        lines.append(f"- Comparison unavailable ({escape(receipt.get('reason', 'unspecified'))})")
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
    if result["repeat_counts"]:
        lines.append("- Repeat outcomes " + ", ".join(
            f"{status} {count}" for status, count in sorted(result["repeat_counts"].items())))
    for key, label in (("case_counts", "Case outcomes"), ("comparison_counts", "Comparison outcomes")):
        if result[key]:
            lines.append(f"- {label} " + ", ".join(
                f"{status} {count}" for status, count in sorted(result[key].items())))
    lines.extend(["", "## Workflow anomalies", ""])
    lines.extend(f"- {escape(item.get('reason', 'Unknown outcome'))}"
                 + (f" ({item['id']})" if "id" in item else
                    f" (batch {item['batch']})" if "batch" in item else "") for item in result["errors"])
    lines.extend(["", "<details><summary>Coverage by case</summary>", "",
                  "| Case | Profile | Repeats | Execution | Comparison |",
                  "|---|---|---:|---|---|"])
    for case_id in result["selected_ids"]:
        context = result["case_context"].get(case_id, {})
        case = result["case_verdicts"].get(case_id, {})
        lines.append(f"| {case_id}@{context.get('version', '-')} | {context.get('profile') or '-'} | "
                     f"{context.get('repeats', '-')} | {case.get('execution', 'unavailable')} | "
                     f"{case.get('comparison', 'unavailable')} |")
    lines.extend(["", "</details>"])
    lines.extend(["", "## Case execution failures", "", "<details><summary>Repeat evidence</summary>", "",
                  "| Case | Repeat | Status | Stage | Reason |",
                  "|---|---:|---|---|---|"])
    for row in result["results"]:
        if row["status"] in {"completed", "not-run", "missing"}:
            continue
        fields = [f"{row['case_id']}@{row['case_version']}", row["repeat"], row["status"],
                  row.get("last_stage", "-"), row.get("reason", "-")]
        cells = [escape(str(value)).replace("|", "&#124;").replace("\n", " ") for value in fields]
        lines.append("| " + " | ".join(cells) + " |")
    lines.extend(["", "</details>", "", "## Suspected performance regressions", "",
                  "<details><summary>Metric evidence</summary>", "",
                  "| Case | Arm | Metric | Baseline | Median | Change |",
                  "|---|---|---|---:|---:|---:|"])
    for case_id, case in result["case_verdicts"].items():
        version = result["case_context"][case_id]["version"]
        for metric in case["metrics"]:
            if metric["status"] == "suspected-regression":
                lines.append(f"| {case_id}@{version} | {metric['arm']} | {metric['metric']} | "
                             f"{metric['baseline']:g} | {metric['observed_median']:g} | {metric['change_ratio']:+.1%} |")
    lines.extend(["", "</details>", "", f"Download the full Markdown and JSON from [Artifacts]({run_url}#artifacts).",
                  "", "```sh",
                  f"gh run download {run_id} --repo {repository} --name performance-report-{attempt['attempt']}",
                  "```", ""])
    return "\n".join(lines)


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
                     "elapsed_seconds": elapsed, "conclusion": job.get("conclusion")})
    return jobs


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--inputs", type=Path, required=True)
    parser.add_argument("--artifacts", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--jobs", type=Path)
    args = parser.parse_args()
    args.output.mkdir(parents=True)
    attempt = runner.github_attempt()
    jobs = json.loads(os.environ.get("PERFORMANCE_JOB_RESULTS", "{}"))
    result = collect(args.inputs, args.artifacts, attempt, jobs)
    result["jobs"] = jobs_for_report(args.jobs)
    runner.write_json(args.output / "report.json", result)
    markdown = summary(result, os.environ["GITHUB_REPOSITORY"])
    (args.output / "report.md").write_text(markdown, encoding="utf-8")
    with Path(os.environ["GITHUB_STEP_SUMMARY"]).open("a", encoding="utf-8") as step_summary:
        step_summary.write(markdown)
    return int(bool(result["errors"]) or any(row["status"] != "completed" for row in result["results"])
               or any(status != "success" for status in jobs.values()))


if __name__ == "__main__":
    sys.exit(main())
