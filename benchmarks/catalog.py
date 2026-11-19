"""Build the versioned performance case catalog from the declared sources."""

import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import sys


BENCHMARKS = Path(__file__).resolve().parent
CASE_ROOT = BENCHMARKS / "cases"
SCANNER = BENCHMARKS / "src/main/java/io/tapstate/benchmarks/PerfCaseScanner.java"


def read_json(path):
    return json.loads(path.read_text(encoding="utf-8"))


def digest(value):
    contents = json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(contents.encode("ascii")).hexdigest()


def resolve_case(case, contracts, spec):
    """Resolve the shared inputs needed to reproduce this case's definition."""
    try:
        profile = contracts["profiles"][case["profile"]]
        mode = case["mode"]
        measurement = contracts["measurement"][mode]
        environments = {name: contracts["environments"][name] for name in case["environments"]}
    except KeyError as error:
        raise ValueError(f"{case['id']}: unknown or missing catalog value {error}") from error
    if profile["version"] != case["profile_version"]:
        raise ValueError(f"{case['id']}: profile version does not match {case['profile']}")
    graph_operators = list(dict.fromkeys(step["operator"] for step in spec["graph"]))
    if set(graph_operators) != set(case["operators"]):
        raise ValueError(f"{case['id']}: declared operators must match the scenario graph: {graph_operators}")
    if spec["version"] < 1 or not spec["inputs"] or not spec["expected"]["readback"]:
        raise ValueError(f"{case['id']}: expected a versioned spec with inputs and readback checks")

    definition = {
        "contract_version": contracts["version"],
        "profile": profile,
        "environments": environments,
        "repetition": contracts["repetition"],
        "measurements": {mode: measurement},
        "scenario": spec,
        "memory": contracts["memory"],
        "eligibility": contracts["eligibility"],
    }
    if "memory-regression" in case["tags"]:
        definition["memory_profile"] = contracts["memory_profile"]
    if case["source"] == "mongodb":
        definition["generation"] = contracts["generation"]
        if case["phase"] != "snapshot":
            incremental = contracts["incremental"].copy()
            operations = spec["parameters"].get("operations")
            if operations:
                incremental = {
                    "operations": operations,
                    "mongo_source": incremental["mongo_source"],
                    "update_mutable_fields_percent": incremental["update_mutable_fields_percent"],
                }
            else:
                incremental["allow_manual_mix_override"] &= spec["parameters"].get("allow_manual_mix_override", False)
            definition["incremental"] = incremental
    if case["phase"] == "snapshot-to-cdc":
        definition["measurements"].update(
            {name: contracts["measurement"][name] for name in ("snapshot", "live-latency")}
        )
    metadata = {key: value for key, value in case.items() if key not in ("definition", "line")}
    return {**case, "execution_status": "definition-only",
            "definition_sha256": digest({"metadata": metadata, "definition": definition}), "workload_spec": definition}


def build_catalog():
    result = subprocess.run(
        ["java", str(SCANNER), str(CASE_ROOT)], text=True, capture_output=True, check=False
    )
    if result.returncode:
        raise ValueError(result.stderr.strip() or f"Source scanner exited with {result.returncode}")
    inventory = json.loads(result.stdout)
    contracts = read_json(CASE_ROOT / "contracts.json")
    specs = {}
    cases = []
    for case in inventory["cases"]:
        reference = case["spec"]
        if not reference:
            raise ValueError(f"{case['id']}: missing workload specification")
        if reference not in specs:
            specs[reference] = read_json(CASE_ROOT / reference)
        cases.append(resolve_case(case, contracts, specs[reference]))
    unused = {path.relative_to(CASE_ROOT).as_posix() for path in (CASE_ROOT / "specs").glob("*.json")} - specs.keys()
    if unused:
        raise ValueError(f"Unreferenced specifications: {', '.join(sorted(unused))}")
    sha = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=BENCHMARKS, text=True).strip()
    dirty = subprocess.check_output(
        ["git", "status", "--porcelain", "--", "."], cwd=BENCHMARKS, text=True
    ).strip()
    return {"schema_version": inventory["schema_version"], "code_sha": sha,
            "source_dirty": bool(dirty), "catalog_sha256": digest([case["definition_sha256"] for case in cases]),
            "cases": cases}


def render_markdown(catalog, selected=None):
    lines = ["# Synchronization performance cases", "", f"Code revision `{catalog['code_sha']}`.",
             f"Catalog digest `{catalog['catalog_sha256']}`.",
             "These definitions describe calibration candidates. Execution and qualified measurements are recorded separately.", ""]
    if catalog["source_dirty"]:
        lines.extend(["The benchmark sources have local changes.", ""])
    if selected is None:
        lines.extend([f"{len(catalog['cases'])} registered cases.", ""])
    lines.extend([
        "| Case ID | Group | Operators | Phase | Measurement | Profile | Tags |",
        "| --- | --- | --- | --- | --- | --- | --- |",
    ])
    for case in ([selected] if selected else catalog["cases"]):
        lines.append(
            f"| `{case['id']}` | {case['group']} | {', '.join(case['operators']) or 'None'} | "
            f"{case['phase']} | {case['mode']} | {case['profile']} | {', '.join(case['tags']) or 'None'} |"
        )
    if selected is None:
        return "\n".join(lines)
    spec = selected["workload_spec"]
    lines += ["", f"## {selected['id']}", "", selected["description"], "",
              f"Case version {selected['case_version']}. Profile version {selected['profile_version']}.",
              f"Definition `{selected['definition']}:{selected['line']}`. Specification `{selected['spec']}`.",
              f"Source `{selected['source']}`. Target `{selected['target']}`.",
              f"Definition digest `{selected['definition_sha256']}`.", "", "### Correctness", ""]
    lines.extend(f"- {check}" for check in spec["scenario"]["expected"]["readback"])
    for name, protocol in spec["measurements"].items():
        lines.extend(["", f"### {name}", "", f"- Start. {protocol['start']}", f"- Finish. {protocol['finish']}"])
    lines.extend(["", "### Resolved specification", "", "```json", json.dumps(spec, indent=2, sort_keys=True), "```"])
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--format", choices=("json", "markdown"), default="json")
    parser.add_argument("--case", help="Render the definition for one exact case ID.")
    args = parser.parse_args()
    try:
        catalog = build_catalog()
        selected = None
        if args.case:
            selected = next((case for case in catalog["cases"] if case["id"] == args.case), None)
            if selected is None:
                raise ValueError(f"Unknown case ID {args.case}")
        if args.format == "markdown":
            print(render_markdown(catalog, selected))
        else:
            if selected:
                catalog["cases"] = [selected]
            print(json.dumps(catalog, indent=2, sort_keys=True))
    except (OSError, ValueError, KeyError, subprocess.CalledProcessError) as error:
        print(f"perf-catalog: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
