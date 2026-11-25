# Native instance snapshot evidence

`ci-instance-evidence.sh` follows a verified PR-instance candidate archive and
retains native Actions attempt logs and run-level artifact ZIPs. It independently
replays the original source/recipe/checkout/event anchor and all candidate JSON
records first. Original CI and the other captured ordinary or PR-target candidates
retain their native outcomes. Foreign target records remain in the candidate
archive without fetching unrelated details.

The request has exactly `schema: 1`, `type: instance-evidence-request`,
`instances_request` (relative path/SHA-256 reference to the original PR-instance
request), and `candidate_archive` (reference to its `receipt.json`).

```sh
bash .github/scripts/ci-instance-evidence.sh collect \
  --manifest /path/to/evidence/native-request.json \
  --evidence-root /path/to/evidence \
  --root /path/to/owned-experiment-worktree \
  --output /path/to/persistent-private-archive/native

bash .github/scripts/ci-instance-evidence.sh project \
  --manifest /path/to/evidence/native-request.json \
  --archive /path/to/evidence/native-receipt-reference.json \
  --evidence-root /path/to/evidence \
  --root /path/to/owned-experiment-worktree \
  --output /path/to/evidence/native-readback.json
```

Collection output uses the same canonical path and source/temporary exclusion as
the candidate collector. JSON and binary creation is exclusive through directory
descriptors without following symlinks; raw ZIP bytes are never converted to text.
Only GET requests run. The streaming download limit is 64 MiB per ZIP; if exceeded,
only that owned GET subprocess is retired. No workflow, PR, Docker daemon, foreign
process or fixture is started, changed, cancelled or removed.

Each candidate must already be terminal, and fresh before/after run records must
match its frozen run/workflow/event/head/branch/latest-attempt/suite and outcome.
Every attempt uses its own native logs endpoint. Valid empty ZIPs retain their
digest and empty member set, add an explicit gap, and never prove test execution. The [artifact API](https://docs.github.com/en/rest/actions/artifacts#list-workflow-run-artifacts) is queried at run scope,
with a complete, stable before/after ID set and exact run/head/branch bindings.
Duplicate names retain different IDs. They never imply an artifact belongs to a
particular attempt. Downloads use constructed numeric-ID endpoints, without
following caller-selected download URLs or using artifact names as paths.

ZIP replay verifies bounded canonical regular members, file sizes and SHA-256
without extraction or execution. An embedded `ci-event-inputs.json` must bind its
run/event/declared attempt; the anchor CI's original event must match exactly.
An embedded event remains a declaration until its producer and original event
membership are independently established.

Missing downloads, expired artifacts, invalid ZIP bytes and valid empty ZIPs
remain diagnostic;
`raw_snapshot_complete` is false, with exact gaps. A complete raw snapshot means
the listed candidates' observed native bytes are retained, not all historical
workflow evidence or applicability. Run-level artifacts, API history limits,
snapshot atomicity and original per-workflow actions remain separate facts.
Unsafe ZIP members, foreign source/run metadata, changed snapshots, substituted
expired bytes, duplicate IDs and unbound extra files reject. Independent replay
verifies the file set, every hash, source/candidate anchor and reconstructed
observation; a resealed replacement result cannot supply missing original data.

The receipt labels collection time before sealing explicitly incomplete. The
stdout summary separately measures collection plus sealing and excludes its last
write; the caller must retain and meter the complete external command cost. Neither
number alone proves complete performance cost.

Both collection and replay exit 2 for a retained incomplete diagnostic snapshot,
0 for complete native snapshot data, and 1 for invalid input or command failure.
`R_G_complete`, first complete PR sample and performance acceptance remain false
in every case. Complete execution inputs, original case/coverage/gate evidence and
trigger applicability still require their existing independent consumers.

The offline smoke uses the shared actual Git/ZIP source fixture and controlled API
boundaries. It verifies separate attempt bytes, same-name artifact IDs, unknown
attempts, missing/expired/corrupt records, foreign runs and embedded events, and
actual CLI hash/readback/exclusive-output behavior. It starts no JVM, Docker or
workflow and proves no performance result.
