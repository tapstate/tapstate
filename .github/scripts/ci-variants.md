# Ordinary variant commits and source recovery

`ci-variants.sh` constructs reviewed A/B/C source recipes and creates ordinary,
one-parent commits in an independent `ws/` worktree. It never pushes, dispatches,
cancels, merges, resets, amends or switches a checkout. Git commit hooks and signing
remain enabled. Source recovery is a source check, not behavior or performance proof.

## Prepare the source recipe

Start from the current clean, reviewed C head. Preserve other product changes,
original cases and added controls. Author only the owned changes for A and B.
Generate each patch using this exact internal diff format:

```sh
git diff --no-ext-diff --no-textconv --binary --no-renames --full-index \
  C_TREE A_TREE -- > A.patch
```

Use the same format for B; C.patch is empty. A tree may be prepared using a separate
temporary Git index; do not switch a shared checkout or replace historical files.
All patches are bounded text changes. The tool replays them into private temporary
indexes and rejects changed paths outside the explicit owned set, symlinks, binary
patches, context offsets or a source anchor older than the current head. Exact
canonical diff equality binds the reviewed hunks to the current C blob identities.

The recipe request JSON has schema `1`, type `variant-recipe-request`,
`scope: {repository, branch, worktree}`, `anchor_commit`, `owned_paths` and
`patches: {A, B, C}`. Each patch reference has exactly a relative `path` and
`sha256`, under the explicit evidence root. The `worktree` is its real absolute
path; `branch` is the existing experiment branch. Prepared C equals the anchor,
and all three variant trees must differ.

```sh
bash .github/scripts/ci-variants.sh prepare \
  --manifest /path/to/evidence/recipe-request.json \
  --evidence-root /path/to/evidence --root /path/to/experiment-worktree \
  --output /path/to/evidence/prepared-recipe.json
```

Preparation writes Git objects and a new private recipe file, never the working
tree, actual index or branch. It records complete Git source/config trees, source
digests and preserved-path digests. Producing and reviewing the owned patches,
original assertion contracts, actual inputs and case manifests remains required.
Do not use a broad path list to undo someone else's changes.

## Archive the preceding published head

The serial transition request has schema `1`, type `serial-transition-request`,
`scope: {repository, pr, branch, head_sha}`, `expected_base_sha`, the same
applicable-workflow/required-check `contract` used by `ci-pr-groups.sh`, `archives`
and `check_pages`.

`archives` lists `{run_id, receipt}` for every actual workflow on that head.
Receipt references point at immutable `receipt.json` indexes. The original archive
verifier is reused. Every run archive must include latest `run.json`, and for each
attempt `attempt-N/run.json`, consecutive `attempt-N/jobs-1.json` etc., and original
`attempt-N/logs.zip`. All attempts and job timelines are reconciled with their
original Actions APIs, including failures and retries. The ZIPs are read back.

With a source-derived trigger policy, the archive set also includes every
applicable branch push instance. Its event and check suite remain distinct from
the PR counterpart, and no PR association is invented for an unassociated push.
Raw head check pages must preserve the source-bound push check-suite IDs.

CI collector archives carry additional CI-specific artifacts and provenance.
Short workflow archives can use the same hash/size index with a narrowly scoped
`raw-workflow-evidence` purpose. This operational readback never replaces full CI
artifact, case, coverage, gate or producer admission. A missing required raw file
cannot be repaired by setting the completeness flag.

`check_pages` maps the actual head and merge SHA to arrays of references to the
original paginated check-runs JSON, captured using `filter=all`. All instances,
including repeated context names, must match the fresh head snapshot. The current
draft/head/base is inspected again after readback. Unexpected base/source changes
stop mutation for impact review; old versioned evidence is retained.

```sh
bash .github/scripts/ci-variants.sh verify-transition \
  --manifest /path/to/evidence/transition.json --evidence-root /path/to/evidence \
  --output /path/to/evidence/transition-readback.json
```

This makes GET requests and reads evidence. It does not infer that terminal failures
or retries are successful first samples, or that a completed run had equivalent
cases/coverage. Unknown native inputs remain unknown. Reinvoke after unfinished
workflows complete; there is no background poller or cancellation.

## Switch and restore with ordinary commits

An operation manifest contains a `recipe` reference, a `transition` reference and
`from_head`. The local head must equal the preceding published PR head, with exact
reviewed source, a clean index/worktree and an origin matching the scoped repository.
The worktree must be independent and linked. The transition is rechecked live;
precomputed green summaries cannot permit mutation. No-op source changes reject
instead of manufacturing an empty PR event.

```sh
bash .github/scripts/ci-variants.sh switch --variant A \
  --manifest /path/to/evidence/switch.json --evidence-root /path/to/evidence \
  --root /path/to/experiment-worktree --journal /path/to/evidence/operation-A
```

The journal directory must be new and outside every product worktree and Git
metadata directory. It retains planned/staged/result records and the exact owned
forward patch. The command applies the bounded patch with the index, executes a
normal `git commit`, then verifies parent, full tree/digest and clean source.
Publish through the normal branch workflow only after reviewing the result.
Wait and archive that published head before selecting another variant.

Use `switch --variant C` to append the final C restoration. To recover a verified
local variant commit that was never published, use `restore-local` with the same
operation fields plus references `previous_plan` and `previous_result`. The current
published head must still be the recorded prior head; the local commit must be its
recorded direct child. Recovery appends C without discarding either commit.

If a normal commit hook fails before creating a commit, `rollback` takes a manifest
with `recipe` and `planned` references, plus root/evidence-root/output flags. It
reverses only the recorded uncommitted owned patch. Exact head/index/worktree checks
are mandatory; a user edit, different staged content or already advanced head stops
recovery and preserves the scene. No reset or historical whole-file checkout occurs.

Finally use `verify-source --variant C` with a `recipe` manifest reference and
root/evidence-root/output flags. It checks the complete reviewed C source/config
digest. Behavior, original cases/controls, latest-tip checks, native provenance and
performance still need their own real verification. Keep all operation records,
including failed and unpublished attempts, out of first-sample statistics.
