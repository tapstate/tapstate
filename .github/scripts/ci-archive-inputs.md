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

`ci-archive-cases.sh` independently parses the verified checkout as data and
replays source case subsets against original worker members. Its computed provider
and remaining whole-input gaps stay explicit; see `ci-archive-cases.md`.

Independent build comparison retains each raw toolchain observation. Hosted
Linux kernel releases are compared as environment observations: only when the
Python platform and a unique Maven OS line identify the same release is that release
excluded from tool identity equality. Java and Maven versions/builds/vendors,
Maven runtime metadata, OS family, libc, architecture, CPU count and every
captured byte hash remain compared. Unknown or inconsistent formats keep their
original exact comparison. This does not waive toolchain changes during verify
or certify compiler consumption.

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

Current worker and aggregate snapshots also retain `runner_context`: the original
repository, distinct workflow ref/commit/job declaration, and explicit `shard_id`.
Both worker phases must keep the same context. Captured contexts require the
original CI path and producer job (`test-shards` or `build`), distinct matrix IDs,
and a common workflow ref/commit. Independent archive readback binds the repository
and shard against its scoped API/plan records. These are runner declarations,
not proof of workflow bytes, all workflow jobs, or effective compiler inputs.

Legacy snapshots without this field retain their original comparator output so
sealed records remain replayable. New peers cannot omit one context or replace
one captured workflow with unknown. Local snapshots missing runner variables stay
explicitly unknown; do not infer workflow commits from checkout SHA or shard IDs
from artifact names. Existing install/verify steps and evidence artifacts retain
their layout, permissions and triggers.

When `packaged_test_outputs` is present, every original peer and phase must
retain it. Readback binds its producer model to the original POM and capture helper
sources, checks the complete producer test tree against each owning result ZIP,
and replays the original goal, descriptor artifacts, ordered direct plugin realm
URLs and consumer test scope. The optional field never adds raw build artifacts
to CI transfers. JAR/member hashes come from source-bound capture; original owner
class/resource bytes are independently read from result ZIPs. Inherited loaders,
actual class consumption and whole-build provenance stay unproven. See
`ci-group-delta.md` for the bounded test-input partition and strict fallback.

Original inputs can also contain `resolved_maven_artifacts`. Readback re-derives
this field from the source-bound Maven observation and original local-parent
consumer POMs, checks the fixed recorder/helper source hashes, and binds default
repository JAR/aggregate-POM coordinates, resolved file SHA-256/size and scopes to every actual
classpath consumer. All observed resolved rows must agree. JAR archive type, literal POM model/parent hashes and JAR/POM byte requirements
remain separate conditions. Source-declared and observed reactor coordinates
prevent treating a missing owned output as an external dependency; first-party
repository dependencies outside that reactor retain their own byte/model facts.
Embedded producer identities and actual class consumption are not upgraded.
Current peers and worker phases must retain the same capture. Legacy absence
keeps the original output/component shape; adding this support does not rewrite
old archives or infer a new capture from an old summary.

ZIP names are canonical, unique and bounded. Only regular files/directories are
accepted; links, special modes, encryption, traversal and file-as-parent entries
reject. The evidence reference reader bounds each raw input to 64 MiB. ZIPs allow
at most 20,000 members and 512 MiB expanded data, with 64 MiB per member.

Older archives missing an original event capture or comparator reject with a
missing-input error. Preserve those archives and their gaps; do not rewrite them
or infer a checkout from the declared plan. Diagnostic dispatch/later attempts
retain the source projection's explicit event/first-attempt gaps. Provider capture
bindings must match the original API event, snapshot, source and tree exactly for
both `pull_request` and `workflow_dispatch`; a manual readback never becomes a
first complete PR sample. Other events and cross-event captures reject.

The offline smoke uses real Git objects, synthetic API pages and actual ZIPs to
discriminate source/run/attempt/event drift, changed dependency/production hashes,
omitted plan tests, foreign bytecode, corrupt raw XML, false comparator claims,
and caller declarations that conceal changed XML identities. Its protocol fixture
contains no real compiled build and establishes no CI or performance result.
