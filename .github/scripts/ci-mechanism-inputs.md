# Original manual mechanism inputs

`ci-mechanism-inputs.sh` reads two original `workflow_dispatch` archives for
mechanism comparisons. It shares the existing source, case, dependency and
full-component readers and the hard delta comparator. It does not dispatch,
execute archived code, invoke a JVM, change a source checkout or update a PR.
Its source contract and offline controls are maintained together in the CI
catalog job.

The request has exactly `schema: 1`, `type: archived-mechanism-inputs-request`,
`variants`, `review_policy`, `order` and `cache_condition`. The variant map has
exactly A and B; each entry has `archive_cases`, a relative path/SHA-256 reference
to an archived case request, and `source_root`, its absolute owned worktree path.
Use the case request with independently audited provider replays when computed
providers are present. Every reference is beneath one evidence root.

`order` is either `["A", "B"]` or `["B", "A"]`. Both original runs must be complete,
successful attempt-1 manual runs of `.github/workflows/ci.yml`, with distinct
heads/run IDs, the same repository/branch, and matching preserved-source and
owned-recipe digests. The recorded earlier run must end before the later one
is created. PR events and additional variant names reject; the existing PR
three-variant partition and group interfaces retain their original contracts.

```sh
bash .github/scripts/ci-mechanism-inputs.sh \
  --manifest /path/to/evidence/mechanism-inputs-request.json \
  --evidence-root /path/to/evidence \
  --output /path/to/evidence/mechanism-inputs.json
```

The reader derives original cases and methods from A and additions from B.
Missing original cases/methods and duplicates reject. Unresolved source retains
its diagnostics and null original/addition fields. It never substitutes a second
B archive for a C run or accepts caller-authored addition rows.

Input readiness requires the original four workers (`e2e-1`, `e2e-2`, `engine`,
`rest`), a source-bound two-E2E-worker layout with original residual modules,
and its independently replayed canonical matrix. Missing current layout facts
remain unready. Every original worker and aggregate snapshot must retain its
resolved Maven artifact packet; the shared comparator keeps unresolved or
different actual dependency identities visible. Available cache extras remain
side observations. Prepared test-resource maps must be present and equal;
differences reject and missing peer captures stay unknown. Replayed test-JAR roles retain the existing owner-byte,
producer and classpath constraints.

The policy reference uses `schema: 1`, `type: declared-mechanism-delta-policy`
and `entries`. Each entry has `component`, `field`, exact A/B `hashes`, a nonempty
`reason` and hash-bound `evidence` references. Only I `methods`, `class_contracts`,
`sources`, `entry_contract`, `test_packaged_outputs`, and R `workflows` permit
declared deltas. Prepared resource maps, P, D, U and runtime specification
differences cannot be waived. Stale, unsupported, duplicate and unused entries
reject. Full unequal fingerprints remain in the output.

`comparison_ready` describes these bounded input checks. It does not establish
unchanged assertions, whole-input or build provenance, workflow applicability,
cache conditions, local execution, a PR sample, latest-tip validity or performance
acceptance. `cache_condition` accepts a warm/cold claim and always retains
`cache_condition_verified: false`; matching restore logs or a caller's label
cannot prove that every consumed input was cached. Behavior, coverage, gates,
costs, cache evidence and repeated paired runs still need their own verification.

The offline smoke uses original Git objects, four source-selected workers,
native-shaped Maven observations, test-JAR owner bytes and XML over two real
archive chains. These synthetic protocol bytes establish no actual compilation,
native CI execution, warm/cold state or performance result.
