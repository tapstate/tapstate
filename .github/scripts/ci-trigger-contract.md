# Bounded original trigger policy

`ci-trigger-contract.sh` independently replays the original source recipe, CI
receipt, indexed event artifact and checkout archive. It derives an ordinary
`pull_request` workflow set from those bytes and the original base/head Git diff.
It does not execute workflow source, push a commit or change a workflow or PR.

The request has exactly `schema: 1`, `type: trigger-contract-request` and `source`,
a relative path/SHA-256 reference to the existing source-projection request.

```sh
bash .github/scripts/ci-trigger-contract.sh \
  --manifest /path/to/archive/trigger-contract-request.json \
  --evidence-root /path/to/archive --root /path/to/owned-worktree \
  --output /path/to/archive/trigger-contract.json
```

Add `--head-status` to join the derived set to a fresh read-only head snapshot.
The current head/base/merge must match the original event. Missing or queued
workflows/checks prevent advancing. Unknown policy facts prevent advancing even
when the observed instances are terminal. The original input chain and request
are reread before exclusive output creation.

The parser supports indented event blocks, explicit activity lists, exact branch
and file patterns, terminal `/**` prefix patterns and ordered positive/negative
filters. Branch and path filters both have to match. A unique three-dot merge
base and at most 3,000 changed paths are required for path evaluation; renames,
copies, unsupported patterns, aliases or structures remain unknown. No current
PR file list substitutes for the original event's Git objects.

Supported job predicates include same-repository checks and original captured
ASCII label membership. `always()` remains a native result requirement. Missing
or unsupported label/guard facts stay unknown. A false job predicate is retained
separately: it does not remove a triggered workflow from the expected run set.

The checkout cannot establish the original definition or membership of a
`pull_request_target` workflow. The original event separately captures the
repository's default branch name. When that name equals the original PR base
ref, the collector retains the independently API-bound base commit and Git
source archive. This supplies a bounded original default/base source policy:
for example, `synchronize` does not match a definition limited to `opened` or
`closed`. Missing or different default/base source remains unknown and ineligible.
An applicable target still needs its own event membership and execution evidence
and prevents advancing. Base readback never claims that the backend's actual
target definition or filtering was observed. Historical ruleset applicability,
backend execution and complete event membership remain separate evidence. The
required-name list is the existing minimum check contract, not every historical
ruleset.

A source-aware `ci-variants` transition adds `trigger_policy` with exactly
`request` (the hash-bound request reference) and `source_root` (the independent
worktree path). The transition derives the policy again, checks the declared
workflow set and current instances, and rechecks them after raw archive readback.
Legacy transitions retain their bounded operational behavior; their declarations
do not establish original trigger applicability or a complete performance sample.

The output always keeps complete R/G, backend observation, first complete PR
sample and performance acceptance false. A supported source policy is a bounded
expectation for reconciliation, not proof that GitHub evaluated every input or
that every original lane passed.

Trigger behavior is grounded in GitHub's [workflow syntax](https://docs.github.com/en/actions/reference/workflows-and-actions/workflow-syntax)
and [event reference](https://docs.github.com/en/actions/reference/workflows-and-actions/events-that-trigger-workflows).
The offline smoke uses independent Git/ZIP source fixtures and controlled API
responses; it starts no real workflow, JVM or Docker.
