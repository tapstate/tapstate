# Archived source projection

`ci-source-projection.sh` derives a source basis from the original hash-indexed
archive and a reviewed A/B/C source recipe. It reads every tar member without
extracting or executing it, checks its Git path/mode/blob identity, and reconciles
head and checkout trees with retained Git objects and the recipe.

This step supplies source readback for broader input reconciliation. Its
`preserved_source_digest` covers the complete tracked tree outside the recipe's
explicit owned paths. `owned_recipe_digest` covers the owned path/mode/blob maps
for all three variants. It is a recipe basis, not proof that compiled production
bytes or a narrower mechanism implementation are equivalent. Product fixes that
must survive every variant belong in the preserved source, never in rollback
patches. Review the actual owned hunks before preparing a recipe.

The current `ci-pr-groups.sh group-inputs` command does not automatically consume
this result. Its declared component documents remain declarations. Inventories,
original assertions and additions, worker/aggregate build inputs, resolved
providers, workflow applicability, actual specifications, and live PR identity
still need independent reconciliation. This tool explicitly leaves whole input,
first complete PR sample, and performance acceptance false.

## Inputs

Prepare a `source-variant-recipe` through `ci-variants.sh prepare`. Retain its
original canonical patch references and the linked experiment worktree; recipe
validation replays the patches into temporary Git indexes. All three trees must
be distinct and C must restore the anchor. No real index, worktree or ref changes.

The projection request JSON has schema `1`, type `source-projection-request`,
`recipe` and `archive` file references, and `scope` with exactly:

- `repository`, `branch`, `variant` (`A`, `B` or `C`), `head_sha`;
- integer `run_id` and `run_attempt`, and `event` (`pull_request` or
  `workflow_dispatch`);
- for a PR, integer `pr` and `base_sha`.

References contain a relative `path` and SHA-256 under the evidence root. The
archive reference points at its original `receipt.json`. Readback uses the
existing verifier: its file set, sizes and hashes must match, and its archive
must be explicitly complete for its stated purpose. Source readback does not
supply missing CI artifact or workflow evidence.

Every archive needs `run.json`, `commit.json` and the collector's `source.tar`.
A PR additionally needs `event-inputs.json`, `checkout-commit.json` and
`checkout-source.tar`. Each tar must be the collector's uncompressed Git commit
archive, including its commit PAX header. The complete file map and every directory prefix must match the
actual Git tree. Sparse/missing/extra members, duplicate/unsafe names, hard links,
special modes and unbound commit/tree metadata reject. Symbolic links are read
as literal Git blobs; their targets are never opened.

For PRs, actual retained checkout parents and both recorded parent lists must be
exactly `[base_sha, head_sha]`; checkout source must preserve the reviewed variant
tree. A populated receipt checkout SHA must match; an absent receipt binding
remains explicitly unverified. A changed merge input rejects for impact review. Populated run associations
must match PR/head/base; an absent association remains explicitly unverified.
The source object proves its recorded content, while the existing collector and
fresh head-status/transition checks prove remote run and PR identity.

## Run

```sh
bash .github/scripts/ci-source-projection.sh \
  --manifest /path/to/evidence/source-request.json \
  --evidence-root /path/to/evidence \
  --root /path/to/owned-experiment-worktree \
  --output /path/to/evidence/source-readback.json
```

Output creation is exclusive and rejects symlink directories. Keep the result
and its input hash map with the immutable run archive. A dispatch or later
attempt can be read for diagnosis, but is marked unsuitable for the original PR
first-sample claim. For dispatch, every populated checkout binding must identify the head. Optional
event/checkout source records are reconciled when present; absent records stay
unverified. Missing producer components remain unverified; matching
source hashes cannot fill them in.

The offline smoke drives actual Git objects and source archives. Its controls
hold declarations constant while corrupting source bytes/modes, PR parents,
commit/tree/run identity or the archive index, and include collapsed A/B/C
recipes. It never sends an API request or starts a workflow, JVM or Docker.
