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
I methods/class_contracts/sources/entry_contract/test_packaged_outputs and R workflows can be declared
expected differences. The values are hashes of canonical JSON from the original
replay. There are no ignore paths, wildcard rules or hard-constraint waivers.
Duplicate, unused, stale or unsupported policy entries reject. Evidence bytes
are bound and retained; a declaration does not prove its semantic explanation.

P preserved source and production bytes/class IDs/access flags remain exact.
Ordinary JAR, Boot, distribution and other production packaged outputs remain
exact too. U, observed toolchain/coverage specification, original label facts and
ruleset records remain exact. Missing or unknown output roles use the legacy
strict comparison, including changed test-JARs.

For source-bound default `maven-jar-plugin:test-jar` producers only, current input
capture adds hash-only `packaged_test_outputs`. The capture checks actual JAR
members against the complete captured test-class/resource tree and separates the
three standard Maven metadata members. Archived readback replays the original
local-parent POM coordinates and helper source hashes, the original owner ZIP's
complete test tree, goal implementation, descriptor artifacts, direct plugin realm
URLs and resolved consumer test scope. Maven file version references are checked
and dereferenced to content hashes and sizes. Direct plugin realm URL order is
retained. Inherited loaders, import package prefixes and actual class consumption
remain unproven; their original observations stay in the input archive. The
archive contains the original capture's JAR/member hashes, not raw test-JAR bytes,
so this is bounded source/capture/owner replay, not independent JAR extraction or
whole-build attestation.

Only outputs with this replay can be partitioned into test inputs. Their raw full
P output and D classpath fingerprints are retained with an explicit
`replayed_test_input_partition` row. The derived production output set, producer
POM/model, metadata, output mode, descriptor and direct realm byte facts remain
hard equal. Changed test members and their classpath aliases become the explicit
I `test_packaged_outputs` delta, requiring exact hashes, reason and evidence.
No filename, caller glob or P/D policy can grant a role. Custom/profile-specific
producers and missing observations stay unresolved or strict. New test identities,
original assertions and coverage continue through their separate comparators.

D ordered resolved classpaths must otherwise be equal before comparison readiness;
different actual dependencies remain `needs_review` and cannot be waived by this
policy. Peer cache/model/producer observation deltas
remain explicit side facts only after the actually resolved entries have been
checked. External repository JARs require hash-bound captured model coordinates,
verified archive type and matching recorded JAR/POM bytes from every peer. Their
identity can come from verified embedded producer metadata or an original
`resolved_maven_artifacts` capture independently replayed by the archive reader.
The latter requires matching source-bound consumer coordinates and every observed
resolved Maven artifact row for each actual classpath consumer: default JAR type (or aggregate POM type),
null classifier, canonical coordinates, file version reference, content hash,
size and recorded scopes. When any peer has this capture, all peers must retain
these facts for every used non-reactor repository dependency, including JARs
with verified embedded identity. A matching row cannot hide conflicting bytes.
Captured external dependency scope differences remain input differences. This supports JARs without recognized
embedded coordinates without changing their original
`producer_identity_verified: false` fact. It proves recorded Maven resolution,
not archive producer identity or actual class consumption.

Actual aggregate POM classpath entries use a separate bounded path: the literal
model must have POM packaging, its recorded bytes must match the classpath, and
original resolved artifact records must declare POM type. They never acquire a
JAR producer or ZIP claim. A first-party namespace alone does not establish a
reactor producer. Current captures bind source-declared module coordinates
(including literal profile module declarations) and observed Maven projects;
a non-reactor repository dependency requires its own original resolved records.
The recorder adds only the actually used non-reactor JAR/POM and literal parent
POM hashes omitted by the legacy namespace filter. It neither builds that
dependency nor follows repository URLs, evaluates dependency-model expressions or resolves an
external effective model. Literal inherited coordinates retain their existing
parent-byte requirement.

The optional capture binds the recorder and both helper source files. Current
peers/phases cannot omit it. Legacy captures without it retain their original
component shape and strict unknowns; no caller flag, D policy or filename alone
can supply the missing proof. Unsupported consumer/reactor models, classifier forms,
coordinates or observations stay unknown. All repository dependency JAR and POM byte changes
still block comparison readiness. Reactor
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
