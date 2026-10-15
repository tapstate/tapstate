# Archived group differences

`ci-group-delta.sh` reports A/B/C differences from the original archived
component and case chains. Full P/I/D/R/U/spec fingerprints remain intact;
the tool never manufactures equal hashes by dropping owned files or new tests.
It neither executes archived code nor changes a checkout, PR or workflow.

The request has exactly `schema: 1`, `type: archived-group-delta-request`,
`partition_request` and `review_policy`. Both inputs are relative path/SHA-256
references under the evidence root. The partition request supplies the three
archived case chains and their independent source roots.

```sh
bash .github/scripts/ci-group-delta.sh \
  --manifest /path/to/archive/delta-request.json \
  --evidence-root /path/to/archive \
  --output /path/to/archive/group-differences.json
```

The policy has exactly `schema: 1`, `type: declared-group-delta-policy` and
`entries`. Each entry has `component`, `field`, `hashes` (exact A/B/C SHA-256
values), a nonempty `reason` and a nonempty `evidence` reference list. Only
I methods/class_contracts/sources/entry_contract and R workflows can be declared
expected differences. The values are hashes of canonical JSON from the original
replay. There are no ignore paths, wildcard rules or hard-constraint waivers.
Duplicate, unused, stale or unsupported policy entries reject. Evidence bytes
are bound and retained; a declaration does not prove its semantic explanation.

P preserved source, production bytes/class IDs/access flags and all packaged
outputs must match exactly, including test-JARs. U, observed toolchain/coverage
specification, original label facts and ruleset records also remain exact.
The adapter deliberately does not resolve legitimate packaged-output changes
without further evidence. D ordered resolved classpaths must be equal before
comparison readiness; different actual dependencies remain `needs_review` and
cannot be waived by this policy. Peer cache/model/producer observation deltas
remain explicit side facts only after the actually resolved entries have been
checked. External repository JARs require hash-bound captured model coordinates
and verified archive/producer identity from every peer; missing or false facts
remain `unknown`, even when classpath bytes match across all variants. Reactor
JARs require a unique matching recorded packaged output by filename/hash/size.
Changed facts for a used dependency remain `needs_review`. These are bounded
captured identities; they do not prove complete build consumption or runtime
realm membership.

The only automatic case normalization is the independently replayed original
identity set and B/C source additions. Unknown providers keep it unknown.
All source method, class, entry and file changes remain separately hashed and
require exact declarations before bounded comparison readiness. This does not
prove unchanged original assertion bodies, gate thresholds, permissions,
fork/secret semantics or workflow applicability.

The existing `pr-group-inputs` request can add `normalization` containing exactly
the same `partition_request` and `review_policy` references. This mode requires
archive-replayed components. It binds the report to the group's original
head/base/run/checkout and every full component hash, then binds each test
comparison's inventories and additions to the independently replayed source.
The existing test, coverage and gate comparators still run unchanged. Unknown or
unreviewed fields block group reconciliation. Legacy mode retains its equality
rule and existing behavior.

`comparison_ready` means these bounded structural inputs can reach the existing
comparators. It is not an acceptance result. Original assertions, declaration
semantics, normalized original inputs, whole-build provenance, group
equivalence, first complete PR sample and performance proof flags remain false.
Native observations and the remaining acceptance evidence are still required.

The offline smoke uses real temporary Git/ZIP source chains with an added test,
checks hard-input and policy failures, binds original/additional inventories,
and exercises the actual CLI. Its bytecode/dependency declarations remain
synthetic protocol data, not native build or CI success evidence.
