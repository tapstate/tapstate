"""Resolve versioned synchronization workloads and their whole-case estimates."""

import catalog


def case_reference(case):
    return f"{case['id']}@{case['case_version']}"


def resolve(cases, workloads, load_fields):
    if (not isinstance(workloads, dict) or type(workloads.get("schema_version")) is not int
            or workloads["schema_version"] != 1
            or not isinstance(workloads.get("cases"), dict)):
        raise ValueError("Expected a version 1 formal workload registry")
    entries = []
    missing = []
    for case in cases:
        reference = case_reference(case)
        recipe = workloads["cases"].get(reference)
        if recipe is None:
            missing.append({"id": case["id"], "reason": f"missing formal workload for {reference}"})
            continue
        if not isinstance(recipe, dict) or recipe.get("definition_sha256") != case["definition_sha256"]:
            missing.append({"id": case["id"], "reason": f"stale formal workload for {reference}"})
            continue
        version = recipe.get("version")
        load = recipe.get("load")
        if type(version) is not int or version < 1 or not isinstance(load, dict) or load.keys() != load_fields:
            raise ValueError(f"{reference}: formal workload needs a positive version and all effective load fields")
        if load.get("repeats") != case["workload_spec"]["repetition"]["count"]:
            raise ValueError(f"{reference}: formal workload must use the catalog repetition count")
        if load.get("mix") is not None:
            raise ValueError(f"{reference}: formal workload must use the declared operation mix")
        workload = {
            "case": reference, "definition_sha256": case["definition_sha256"],
            "version": version, "load": load,
        }
        entries.append({"case": case, "scale": "formal", "workload_version": version,
                        "workload_sha256": catalog.digest(workload), **load})
    return entries, missing


def formal_estimates(entries, estimates):
    if (not isinstance(estimates, dict) or type(estimates.get("schema_version")) is not int
            or estimates["schema_version"] != 1
            or not isinstance(estimates.get("cases"), dict)):
        raise ValueError("Expected version 1 estimates for formal workloads")
    durations = {}
    for entry in entries:
        case = entry["case"]
        observation = estimates["cases"].get(case_reference(case))
        if not isinstance(observation, dict) or observation.get("workload_sha256") != entry["workload_sha256"]:
            continue
        evidence = observation.get("evidence")
        if not isinstance(evidence, str) or not evidence.strip():
            raise ValueError(f"{case_reference(case)}: whole-case estimate requires a calibration reference")
        seconds = observation.get("seconds")
        if type(seconds) is not int or seconds < 1:
            raise ValueError(f"{case_reference(case)}: estimate must be a positive whole-case duration")
        durations[case["id"]] = seconds
    return durations
