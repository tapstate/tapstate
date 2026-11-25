# Archived full variant components

`ci-group-components.sh` derives typed P/I/D/R/U/spec fingerprints by replaying
the existing source, case, input and trigger archive chains. It reads original
indexed ZIP members and Git source objects. It never runs archived code, a test,
a JVM or a workflow, and never changes a checkout or PR.

The request has exactly `schema: 1`, `type: archived-group-components-request`
and `archive_cases`, a relative path/SHA-256 reference to the existing archived
case request. That request supplies the input/source/recipe/event chain.

```sh
bash .github/scripts/ci-group-components.sh \
  --manifest /path/to/archive/components-request.json \
  --evidence-root /path/to/archive --root /path/to/owned-worktree \
  --output /path/to/archive/full-components.json
```

P retains preserved source, declared production hashes/class IDs/access flags
and packaged outputs. I retains the full independently parsed case inventory,
including unresolved methods and source/entry contracts. D retains actual
ordered classpath bindings and each peer's available dependency/model/producer
observations. Available cache extras may differ; the existing comparator checks
the resolved bindings. Current dependency model/producer fields are mandatory,
so missing legacy fields cannot be replaced with an empty captured value.
R retains workflow/guard/label/ruleset facts with applicability unverified.
U retains the reviewed owned recipe digest. Spec retains the observed toolchain
and coverage version with complete runtime specification unverified.

Add `--component P` (or I/D/R/U/spec) to emit an `archived-group-component`
envelope. Its request must be under the evidence root. The envelope binds the
run/attempt/actual checkout, payload, request and independent source root.
The group and impact readers rederive the component before hashing it; deleting
case, dependency, output or toolchain fields, altering scalar types or substituting
a foreign run cannot pass by copying a precomputed digest. Mixed declared and
archive-replayed component sets reject. All-legacy declarations retain their
existing bounded behavior.

These are full variant fingerprints, including original and newly added cases.
They do not normalize owned production changes or partition original cases from
added controls. Different full fingerprints require explicit review and the
separate original/addition comparators; they are never silently excluded to make
a group equal. Matching full fingerprints do not certify complete provenance,
original assertions, target applicability, cache conditions or a performance
sample. Normalized original-input, whole-build, group, first-sample and performance
proof flags remain false.

The offline smoke uses the shared original Git/ZIP case fixture and rejects
omitted facts and foreign bindings. Its bytecode/dependency declarations are
synthetic protocol data, not actual compiled build or CI evidence.

When original inputs contain `resolved_maven_artifacts`, D also retains each
peer's independently replayed capture. It keeps the original producer facts,
including unknown embedded identities, alongside the distinct recorded Maven
resolution facts. Legacy inputs do not gain this field during readback.
