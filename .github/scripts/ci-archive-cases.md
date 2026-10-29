# Archived source and case readback

`ci-archive-cases.sh` extends `ci-archive-inputs.sh` by deriving the default reactor
class cohort and static source inventory independently, then replaying each
worker's original XML, class gate and witness ledger members. Producer expected
and observed hash documents are selected from the original input ZIPs, not from
caller component files. Their declarations and known static case/skip identities
must agree with parsed checkout source; observations must agree with raw reports.
Rows for methods absent from source reject; genuinely unresolved source methods
remain unknown. Each worker retains its own inventory/probe digest and input
binding, because raw capture timestamps differ. Shared method/case/source semantics
are compared independently of those worker-specific digests.
The complete known case multiset must appear exactly once across original workers.

This reader runs current trusted parsing/report code. It copies already bound
regular archive files as mode-0600 data into a fresh private temporary directory
and removes that directory on return. Source links reject. It never executes an
archived script, provider, compiled test or JVM, and never changes a real Git
checkout/index/ref. Its output binds the versions of all trusted reader helpers.

Computed providers that static parsing cannot resolve remain explicit unknown
records. The producer's matching declarations cannot fill that gap. This step
does not certify effective build provenance, original assertions, workflow
applicability, whole input equality, a first complete PR sample or performance.
Those acceptance flags stay false even when every simple source case is parsed.

The optional `--include-test-observations` flag retains each worker's full replayed
test observation as exact JSON bytes and a digest, including status, skip text,
class callbacks, prepared input binding and actual published-ledger selector.
Each worker also retains its selected tests and original input and result-manifest
digests. The export retains the exact reconstructed
source inventory JSON and its byte digest. All normal archive and semantic checks
still run before export; unresolved identities and acceptance flags stay unchanged.
The default output is unchanged. Consumers must preserve each worker's provenance
when comparing their union; this export does not invent a single aggregate verify.

## Request and command

The request has exactly `schema: 1`, `type: archive-cases-request` and
`archive_inputs`, a relative path/SHA-256 reference to the original archive input
request. That request retains the original source request, receipt and recipe.

```sh
bash .github/scripts/ci-archive-cases.sh \
  --manifest /path/to/evidence/archive-case-request.json \
  --evidence-root /path/to/evidence \
  --root /path/to/owned-experiment-worktree \
  --output /path/to/evidence/case-readback.json
```

All source, API/member, snapshot and provenance checks of the input reader run
first. Worker input ZIPs must also contain `expected-case-hashes.json` and
`observed-case-hashes.json`. The expected documents agree across workers and bind
the source SHA. Method names, source/declaration hashes and signature types are
independently parsed. Known static parameter and entry skip contracts must match.
The observed document binds run/attempt/checkout, its prepared input member, and
the producer inventory digest; raw XML/ledger/class gate replay supplies an
independent comparison. Zero-case observations remain readable in the input
reader; they cannot satisfy a source method that declares an applicable case.

Connector/native entry flags are read from the archived workflow's literal
inventory command when it has one unambiguous occurrence. Other forms remain
unknown. If a real-connector published policy depends on unknown availability,
case-policy derivation explicitly rejects as unavailable; it cannot invent an
executed policy. Synthetic identities and unrelated computed-provider diagnostics
retain their independent boundaries. This is static command readback, not proof of dynamic job applicability.
Published spec/tier and ledger serialization continue to use their original
source parser and report comparator; unresolved forms do not become complete.

The smoke shares the original real-Git/ZIP fixture, preserving its 35 input
controls. Case controls omit methods, swap a same-count identity, alter source
hashes, bind observations to another attempt, rewrite status/counters, conceal
raw XML changes and omit an applicable source method. A computed-provider control
keeps matching producer records while proving the remaining gap stays unknown.
These synthetic protocol fixtures establish no actual compiled build or CI run.

The two-worker control uses identical source/build semantics with distinct capture
times, input digests and producer inventory/probe digests. It must preserve both
original bindings while accepting the source case union. Foreign case rows and
joint source applicability/status/ledger contradictions reject separately.
