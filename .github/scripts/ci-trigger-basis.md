# Original trigger source basis

`ci-trigger-basis.sh` reads original workflow, label and ruleset source facts from
the verified checkout archive. It replays the reviewed source recipe and verifies
the original receipt/API-indexed artifact chain. The separate event JSON must
match `ci-shard-plan`'s original `ci-event-inputs.json` member. Populated nested PR
identities must match the requested repository/branch/head/base/PR/action scope.

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
