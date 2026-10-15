# Original trigger source basis

`ci-trigger-basis.sh` reads original workflow, label and ruleset source facts from
the verified checkout archive. It replays the reviewed source recipe and verifies
the original receipt/API-indexed artifact chain. The separate event JSON must
match `ci-shard-plan`'s original `ci-event-inputs.json` member. Populated nested PR
identities must match the requested repository/branch/head/base/PR/action scope.

The original `shard-plan` event producer also captures GitHub's
`GITHUB_WORKFLOW_REF`, `GITHUB_WORKFLOW_SHA` and `GITHUB_JOB` as a separate
`workflow_source` declaration. The workflow commit is never substituted for the
checkout, PR head or base. Missing historical declarations remain unknown;
partial, malformed, foreign-repository or contradictory declarations reject.
This is the producer job's context, not a record for every worker or matrix leg.
GitHub documents these values in its
[default variables reference](https://docs.github.com/en/actions/reference/workflows-and-actions/variables).

The archive reader joins the declared workflow path and commit to independently
verified checkout archive bytes only when that exact commit equals the archived
checkout. It records the source SHA-256 separately. A different workflow commit
remains an explicit source gap, without guessing that checkout bytes served as the
workflow definition. Runner declarations themselves always keep
`source_bytes_verified: false`; source readback does not establish guard or backend
applicability, complete per-job provenance or R/G outcomes. Target workflows retain
their existing no-checkout boundary and are not routed through this Git-dependent
ordinary-event producer.

The original event also retains a bounded `repository_default_branch` snapshot;
missing legacy data stays unknown. When captured, the run collector separately
saves `base-commit.json` and `base-source.tar` for the original PR base. Source
projection checks that API commit/tree/parents and complete archived Git files
match the exact original base objects. The trigger reader keeps these workflow
bytes separate from checkout workflow bytes. Matching original default/base
branch names supplies a source policy expectation, not proof of the actual
backend target definition or original target run membership.

This is raw source readback. It does not evaluate YAML, actions, dynamic expressions
or backend path filters. Each workflow retains its byte digest, raw top-level
`on` block, indented event declaration blocks and job-level `if` source blocks.
Inline or unsupported declarations remain unparsed. Source text is data and is
never executed or extracted into a checkout.

Original label facts distinguish missing captures from observed empty snapshots,
retain hash-only names and their supported ASCII membership projection, and reject
contradictory or foreign metadata. Labels from current PR state cannot fill a
historical gap. Original ruleset files retain their IDs, hashes and content; this
does not establish that the rule list was complete or effective at event time.

## Request and command

The request has exactly `schema: 1`, `type: trigger-basis-request` and `source`, a
relative path/SHA-256 reference to the original source projection request. That
request supplies the original receipt, recipe and event scope.

```sh
bash .github/scripts/ci-trigger-basis.sh \
  --manifest /path/to/evidence/trigger-request.json \
  --evidence-root /path/to/evidence \
  --root /path/to/owned-experiment-worktree \
  --output /path/to/evidence/trigger-source-readback.json
```

Creation is exclusive and inputs are reread before returning their digest map.
Workflow links, duplicate top-level/event blocks, incompatible job guard structure,
foreign PR identities, inconsistent label hashes and mismatched ruleset IDs reject.
Missing original event members reject instead of deriving labels from a replacement
JSON document. An archive with no workflow sources cannot supply this basis.

Output keeps `R_G_complete`, whole input, first complete PR sample and performance
acceptance false. Dynamic guard and path-filter evaluation, historical ruleset
applicability/completeness, the separate base source for `pull_request_target`,
and every actual workflow/run/attempt/check instance and outcome remain explicit
unverified components. The existing current-head status command is a terminal-state
snapshot; neither it nor these declarations supplies missing historical evidence.

The offline smoke uses the shared real-Git/ZIP fixture and discriminates source
hashes, PR scope, missing/empty labels, contradictory metadata, duplicate blocks,
inline YAML, multiline guards and raw ruleset identities. It starts no workflow,
JVM or Docker and establishes no CI or performance result.
