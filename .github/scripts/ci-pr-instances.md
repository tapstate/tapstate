# Original PR instance candidates

`ci-pr-instances.sh` collects and independently replays JSON API records for a
frozen ordinary PR event. Its anchor is derived again from the reviewed source
recipe, checkout archive and API-indexed original `ci-event-inputs.json` artifact.
A populated original PR payload and attempt 1 are required. Current PR state does
not fill an original action, base, head, label or checkout gap.

The collection request contains exactly `schema: 1`, `type: pr-instance-request`,
`source` (the path/SHA-256 source-projection request reference), and
`target_created: {start, end}`. Both timestamps are UTC, the interval is at most
one day and includes the original CI creation. The target interval is an explicit
observation boundary, never proof that every historical target event was observed.

```sh
bash .github/scripts/ci-pr-instances.sh collect \
  --manifest /path/to/evidence/instances-request.json \
  --evidence-root /path/to/evidence \
  --root /path/to/owned-experiment-worktree \
  --output /path/to/persistent-private-archive/instances

bash .github/scripts/ci-pr-instances.sh project \
  --manifest /path/to/evidence/instances-request.json \
  --archive /path/to/evidence/instances-receipt-reference.json \
  --evidence-root /path/to/evidence \
  --root /path/to/owned-experiment-worktree \
  --output /path/to/evidence/instance-readback.json
```

The archive reference is a relative path/SHA-256 pair for the collection's
`receipt.json`, under the evidence root. Output creation is exclusive. Collection
must use a canonical absolute path outside product source and temporary roots. It performs JSON GETs
only, without starting, rerunning, cancelling or changing a workflow or PR.

Ordinary PR run candidates are queried by the frozen head. PR-target candidates
are queried separately by event and the bounded creation interval, without an
ordinary-head filter. Every returned candidate record is retained; foreign,
mixed or different-head/base target associations retain the query record without
fetching unrelated run details. Empty associations remain unknown. Matching PR
refs alone never bind a run to the original activity type or label/path conditions.

For each captured candidate, all attempt run records and job pages are kept.
Historical attempt check suites are independent: an older attempt cannot borrow
the latest attempt's suite. The latest attempt must match the frozen run's suite. Checks are queried with `filter=all` on the frozen
head, original checkout and base, plus captured target run heads. Names are never
a unique key; same-name failures, queued instances and reruns survive projection.
Job check URLs join exact check IDs, suites and run/attempt pairs. Missing or
external joins remain visible rather than becoming a fabricated passing gate.

Before/after query identities must agree. Missing pages, duplicate IDs, changed
pagination totals, a search at the 1,000-result bound, a missing original CI anchor,
foreign query sources and mismatched run/attempt/job/check bindings reject.
Independent replay verifies every archived file hash and the exact used file set,
rederives the original anchor and reconstructs observations from raw pages. A
resealed replacement observation cannot fabricate applicability or performance.

A complete candidate JSON archive is only that observation. It leaves
`R_G_complete`, first complete PR sample and performance acceptance false.
Per-workflow original event membership, target event/base workflow source,
trigger/guard/path/historical ruleset applicability, API history limits and
non-atomic snapshots remain unknown. Raw logs, artifact ZIPs and complete
execution provenance are not supplied by this collector; the existing run receipt
collector remains necessary. Candidate IDs and native outcomes do not replace
those inputs or prove an expected workflow was triggered.

The offline smoke uses actual Git/source/ZIP anchors and controlled JSON API
responses. It exercises same-name failures, attempt-specific suites, target and
foreign namespaces, pagination/caps, missing anchors, independent archive replay
and corruption. It starts no workflow, JVM or Docker and measures no performance.

The separate [native instance snapshot](ci-instance-evidence.md) retains bounded
original attempt logs and run-level artifact ZIP bytes for these candidates. It
replays this source and candidate chain; original event membership, artifact
attempts and all complete-PR/performance proof flags remain unverified.
