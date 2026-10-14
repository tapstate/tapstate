# Archived CI input readback

`ci-archive-inputs.sh` reads the original artifact API pages and ZIP members from
a durable receipt. It first replays `ci-source-projection.sh` against the reviewed
Git recipe, then binds the actual checkout, plan, worker and aggregate snapshots,
input comparison, and raw test result members. It never extracts members, executes
archived code, changes a Git ref/index/worktree, or sends an API request.

This is an extension of source readback. The existing `ci-pr-groups.sh` declared
components remain declarations; this result does not automatically replace them.
Matching captured production/dependency hashes are observations about the original
CI snapshots. Raw production and dependency bytecode is intentionally absent from
these artifacts. Effective models, compiler/plugin/processor realms and generated
inputs are still unproven. All whole-input, whole-build, first complete PR sample
and performance acceptance flags remain false.

## Request and command

The request has exactly `schema: 1`, `type: archive-inputs-request` and `source`,
a relative path/SHA-256 reference to the original source projection request.
That request supplies the original receipt, recipe and event scope. Keep all
inputs under a real evidence directory without symlink path components.

```sh
bash .github/scripts/ci-archive-inputs.sh \
  --manifest /path/to/evidence/archive-input-request.json \
  --evidence-root /path/to/evidence \
  --root /path/to/owned-experiment-worktree \
  --output /path/to/evidence/input-readback.json
```

Output creation is exclusive. Inputs are reread before returning their digest
map. The original receipt must pass its file/size/hash verifier. Artifact API
pages must be complete and contiguous, IDs/names unique, and every artifact must
identify the scoped run/head/branch. API artifacts are run-scoped; embedded
snapshots and result provenance supply the attempt binding, not the ZIP name.
The API ID selects `artifacts/<id>-<name>.zip`; callers cannot replace its input
member with an unrelated component file.

## Original members

The reader requires the current artifact layout:

- `ci-shard-plan`: `shards.json` and `ci-event-inputs.json`, matching the separately
  archived event record and actual checkout. The planned union, cohort, shard IDs
  and required coverage paths are reconciled. Independent source discovery of the
  whole case inventory remains unverified.
- `ci-inputs-<shard>` and `ci-inputs-build`: `build-inputs.json` from each original
  install, including the exact tracked checkout content hashes and scoped event,
  run and attempt. Workers also need `post-test-inputs.json`; production,
  dependencies and toolchain may not change during verify.
- `ci-inputs-build`: `build-input-equivalence.json`, replayed with the actual
  members and their raw digests through the original input comparator.
- `ci-test-results-<shard>`: `<shard>/manifest.json` and its exact `files/` set,
  successful reset/verify provenance, input observer binding, XML counters/class
  families, required execs and compiled selected test classes. Raw test class
  hashes and class IDs match the post-test snapshot. Production bytecode and
  foreign shard/module members reject.
- `ci-aggregate`: all members are retained in the readback chain. Full aggregate
  merge semantics, stable parameter/provider and ledger identity replay still
  need their dedicated comparison; presence alone cannot certify them.

ZIP names are canonical, unique and bounded. Only regular files/directories are
accepted; links, special modes, encryption, traversal and file-as-parent entries
reject. The evidence reference reader bounds each raw input to 64 MiB. ZIPs allow
at most 20,000 members and 512 MiB expanded data, with 64 MiB per member.

Older archives missing an original event capture or comparator reject with a
missing-input error. Preserve those archives and their gaps; do not rewrite them
or infer a checkout from the declared plan. Diagnostic dispatch/later attempts
retain the source projection's explicit event/first-attempt gaps.

The offline smoke uses real Git objects, synthetic API pages and actual ZIPs to
discriminate source/run/attempt/event drift, changed dependency/production hashes,
omitted plan tests, foreign bytecode, corrupt raw XML, false comparator claims,
and caller declarations that conceal changed XML identities. Its protocol fixture
contains no real compiled build and establishes no CI or performance result.
