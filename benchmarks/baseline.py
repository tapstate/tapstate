"""Read a benchmark baseline without suppressing measurement and coverage reports."""

import hashlib
import json


def load(path, *, identity=None):
    if path is None:
        return None, {"status": "unavailable", "reason": "baseline-not-supplied"}
    details = dict(identity or {})
    try:
        contents = path.read_bytes()
        checksum = hashlib.sha256(contents).hexdigest()
        if "sha256" in details and checksum != details["sha256"]:
            raise ValueError("baseline-digest-mismatch")
        baseline = json.loads(contents)
        if (not isinstance(baseline, dict) or not isinstance(baseline.get("id"), str)
                or not baseline["id"] or baseline.get("kind") not in ("fixture", "accepted")
                or not isinstance(baseline.get("environment"), str) or not baseline["environment"]
                or not isinstance(baseline.get("cases"), dict)):
            raise ValueError("invalid-baseline-schema")
        if any(not isinstance(row, dict) for row in baseline["cases"].values()):
            raise ValueError("invalid-baseline-case")
        if "id" in details and (baseline["kind"] != "accepted" or baseline["id"] != details["id"]
                                or baseline.get("version") != details.get("version")
                                or baseline["environment"] != details.get("environment")):
            raise ValueError("baseline-identity-mismatch")
        return baseline, {**details, "status": "available", "id": baseline["id"],
                          "kind": baseline["kind"], "sha256": checksum}
    except OSError:
        reason = "baseline-unreadable"
    except (json.JSONDecodeError, UnicodeError):
        reason = "baseline-invalid-json"
    except ValueError as error:
        reason = str(error)
    return None, {**details, "status": "unavailable", "reason": reason}
