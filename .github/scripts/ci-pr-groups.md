# PR group input reconciliation

`ci-pr-groups.sh` joins persistent original PR receipts, replays the existing test,
coverage and gate comparators, and inspects a current draft PR without changing it.
Use the output as a bounded input check. It never certifies a complete PR sample,
whole-build provenance, cache state, latest-tip success or performance acceptance.

All JSON references have exactly `path` and `sha256`. Paths are relative to an
explicit evidence root; traversal, symlinks, duplicate JSON keys, changed bytes and
unknown required input are rejected. Output files are created exclusively without
following symlink parents or targets, preserving any concurrent writer. Keep these private
source observations and raw comparison results in the persistent evidence archive.

## Group inputs

```sh
bash .github/scripts/ci-pr-groups.sh group-inputs \
  --manifest /path/to/archive/group-inputs.json \
  --evidence-root /path/to/archive \
  --output /path/to/archive/group-inputs-result.json
```

The manifest has schema `1`, type `pr-group-inputs`, a `scope` containing
`repository`, positive `pr` and `ws/` `branch`, a `cache_condition` claim of `warm`
or `cold`, an `order` with A/B/C exactly once, `variants`, and `comparisons`.

Each variant declares its original `head_sha`, an `archive` reference to its
immutable `receipt.json`, and `components` with exactly P, I, D, R, U and spec.
The archive is read back and hash-verified using the original receipt checker.
Original archived `run.json`, `event-inputs.json` and `checkout-commit.json` are
reconciled using the existing identity checker. CI attempt 1 must have completed
successfully. Reused heads/runs and overlapping CI event runs are rejected. This
CI ordering alone does not establish that every other applicable workflow ended.

The original CI event recorder also retains hash-only PR label facts. Missing
event labels have `labels_snapshot_complete: false` and null hash lists; an observed
empty list is a complete empty snapshot. `label_name_sha256` preserves UTF-8 name
identities. `label_ascii_lower_sha256` supplies membership hashes for supported
ASCII case-insensitive expressions; `label_ascii_case_complete` is false when any
name is non-ASCII. No names, descriptions or event text enter the metadata.
GitHub's [expression functions](https://docs.github.com/en/actions/reference/workflows-and-actions/expressions)
compare `contains` values without case sensitivity. Unknown non-ASCII matching
semantics remain outside this limited projection. Preserve these original facts
when deriving historical label applicability; current labels cannot replace a
missing event snapshot. Identity/capture completeness alone does not establish R,
all applicable workflow results or a first complete performance sample.

Each component file has schema `1`, type `declared-group-component`, a matching
`name`, a `binding` of `{run_id, run_attempt: 1, source_sha}` for the actual merge
checkout, and a nonempty structured `payload`. Payloads are canonicalized and
compared across variants. P describes normalized product inputs, I the original
lane/case multiset, D actual dependency inputs, R routine gate contracts, U the
shared candidate recipe, and spec the execution specification. They are declared
projections: independently producing and reviewing these values remains required.
Only the explicitly reviewed owned changes may be normalized away; never exclude
all tests or restore a historical product tree.

Every declared lane needs tests, coverage and gates comparisons for A-to-B and
A-to-C. Each comparison entry has `before: "A"`, `after: "B"` or `"C"`, `lane`
and a `request` reference. Comparison request files have schema `1`, `kind` and
`inputs`. Coverage/gates use `before` and `after`; tests additionally require
`inventory`, `candidate_inventory` and `additions` references. Empty additions
are an explicit JSON list. The inputs are the original comparator snapshots,
not a precomputed green summary.

Test observations and gate snapshots include an `input_binding`; coverage uses
its existing `binding`. Origin matching accepts the existing `attempt` spelling,
numeric strings and extra provenance fields while checking exact run/attempt/source.
Gate origin metadata must be independently attached when producing its snapshot;
the gate comparator itself has no original-event origin field. Test inventory
source and lane must match the corresponding actual checkout and declared lane.

The existing comparators are executed on these exact files. The group output
records their results, all input hashes and the manifest hash. A consistent group
retains `first_complete_pr_sample_proven: false`, `group_equivalence_proven: false`
and `performance_accepted: false`. Missing native producer facts, original
assertion proof, actual applicable-workflow results or independent cache evidence
cannot be replaced by matching declared component payloads.

An individual comparison request can also be replayed with `compare-evidence`
and the same manifest/evidence-root/output flags. That command binds the bytes
and comparator result; it makes no original-event or group-origin claim.

## Read-only current-head status

```sh
bash .github/scripts/ci-pr-groups.sh head-status \
  --manifest /path/to/archive/head-contract.json \
  --output /path/to/archive/head-snapshot.json
```

The manifest has `scope: {repository, pr, branch, head_sha}` and `contract` with
`applicable_workflows` and `required_checks`. Workflows are exact
`.github/workflows/*.yml` paths including CI. The check contract includes build,
no-cjk, no-agent-footprint, sonarqube, e2e-admission, no-binary-bytes, dco,
docs-classification and docs-impact, plus any other expected checks.

The command makes only GET requests. It verifies the current open draft PR,
repository/branch/head/base, the actual merge parents, paginated head workflow runs,
and all check instances on both head and merge using `filter=all`. Populated PR
associations must agree with the scoped PR/head/base; absent associations are
recorded as unknown. Missing or unexpected workflows,
missing required checks and any unfinished run/check prevent advancing. All check
instances remain visible; equal context names do not hide another running instance.
The PR scope is read again before returning to detect concurrent changes.

`can_advance_after_archiving` describes terminal state only. Archive and read back
every observed run/attempt/check and applicable-lane evidence before making the next
ordinary variant commit. A terminal failure or retry can be archived for diagnosis;
it is never promoted to a successful first sample. This command does not archive,
push, dispatch, cancel, change a PR or merge. Invoke it again for a fresh snapshot;
it starts no background poller.

## Declared input impact

`impact` takes the same manifest/evidence-root/output flags. Its manifest supplies
`group_state: "pending"` or `"completed"`, and before/after component reference
maps with exactly P/I/D/R/U/spec. Changed payloads prevent declaring a pending group
comparable and require reviewing/retesting affected completed-group metrics.
Unchanged declarations retain their versioned record; they never establish latest
tip success. Confirm that the projections include new cases, dependency bytes,
production changes, workflow/resource/caching changes and candidate changes before
using this as an impact review input.

This helper performs no source recovery. Use [ci-variants](ci-variants.md) for
ordinary-commit construction, serial archive readback and owned-source recovery.
Real case/control and final native verification remain separate steps.
