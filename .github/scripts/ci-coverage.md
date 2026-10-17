# Coverage from original worker inputs

The `coverage` command in `ci-evidence.sh` verifies the declared original exec
and XML sets, production class identities, runtime-generated classes and session
time bounds. Keep this document with `_ci-evidence.py` and its coverage smoke
controls in `ci-evidence-smoke.sh`.

A single-worker observation uses `--inputs`, `--test-inputs` and `--binding` as
before. A merged observation uses `--inputs` for its prepared production model
and `--worker-inputs` for a hash-bound manifest of the original workers. Do not
combine `--worker-inputs` with an invented aggregate `--test-inputs` capture.

The run binding adds `required_workers` and `merged_exec` to its existing
run/source/time and exact exec/XML requirements. Each worker entry provides
`binding`, `inputs`, `test_inputs`, original `result_manifest` and `provenance`
references, plus its `required_exec` paths:

```json
{
  "schema": 1,
  "type": "coverage-worker-inputs",
  "workers": {
    "app-1": {
      "binding": {"path": "app-1-binding.json", "sha256": "<sha256>"},
      "inputs": {"path": "app-1-pre.json", "sha256": "<sha256>"},
      "test_inputs": {"path": "app-1-post.json", "sha256": "<sha256>"},
      "result_manifest": {"path": "app-1-result-manifest.json", "sha256": "<sha256>"},
      "provenance": {"path": "app-1-provenance.json", "sha256": "<sha256>"},
      "required_exec": ["target/ci-shards/app-1/app/target/jacoco.exec"]
    }
  }
}
```

References may be absolute or relative to the manifest; traversal, symlink
files and incorrect digests reject. A worker binding retains that worker's
actual reset/verify and runtime-capture identity, with the same run, attempt,
source and run interval as the merged observation. It is not a synthetic
aggregate invocation. All prepared production, dependency and test bytes must
agree before execution.

A binding declaring workers or merged execs requires the worker manifest;
omitting the option rejects rather than falling back to single-worker scope.
The original result manifest must bind the exact required exec set and each raw
exec digest. Its embedded provenance and the separately referenced provenance
bytes must match the same original capture binding.

A worker whose original result manifest explicitly requires no exec remains a
required worker with its own validated input/capture; an empty exec list does
not permit omitting that worker.

Every raw exec belongs to exactly one named worker. A generated class observed
by one worker cannot exempt undeclared bytes in another worker. Only after all
raw execs pass their own input/capture checks may a merged exec pass; its
sessions, class identities, probe layouts and hits must equal their union.
Missing workers, foreign ownership, stale captures, old or altered bytes, and
missing or extra merged probes reject. Per-worker captures remain separate in
the output. Tool completion does not establish performance or whole-build
provenance.
