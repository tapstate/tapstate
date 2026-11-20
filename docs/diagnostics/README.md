---
status: engineering-draft
publication: handoff
target: https://tapstate.dev/docs/reference/error-diagnostics
---

# Product error diagnostics

The initial cross-version recovery exercise used Server, CLI and MCP revision
`56f37fab7cd56acae3f8a19679bfbb7e9c784418` and Web revision
`522652029bb1c372468df2b8b331380b62630d35`. The revision pair identifies that exercise. For ordinary OP and Cloud
installations, isolate incompatible writers and verify that the recovery
build can read both diagnostic formats before switching the connection report
writer. A historical report keeps its recorded code, language and provenance.

The first migrated request
refusal is `TS0210001`. Authentication rejection, missing authentication,
insufficient capability and an unreachable control service use `TS0210002`
through `TS0210005`, respectively. AI assistant and provider refusals use
`TS0210006` through `TS0210013`. Control capability refusals and the post-apply
model-refresh warning use `TS0210014` through `TS0210023`. Storage request
refusals use `TS0210024` through `TS0210031`. Source mutation errors use `TS0220001`
through `TS0220007`. Pipeline, artifact and Pipeline draft mutations use modules
`023`, `024` and `025`. Local MCP adapter refusals use `TS0260001` through
`TS0260004`; structured remote refusals retain the Server's code, status and
parameters. The document-key advisory uses `TS0270001` and remains a warning
on a successful apply, with the source column and destination attribution intact.
Lifecycle refusals use `TS0280001` through `TS0280008`; start-check report text
uses `TS0290001` through `TS0290012`. A report's behavior determines whether to
pass, warn, ask or block; its text definition's warning severity does not authorize
a start. Render declared text parameters separately from the complete finding
metadata, and preserve decisions, definition changes and unknown outcomes.
Validation uses module `030`, startup configuration uses `031`, role selection
uses `032`, and CLI command/session diagnostics use `033`. Validation report
readers accept both stored historical identities and current identities, preserving
the report's original transaction, ownership and bounded payload. Verify a compatible
recovery artifact before the first new-code write; an old reader that rejects the
new identity is not a safe rollback target. Never delete reports to make it readable.

CLI `TS0330030` (start checks were not run) and `TS0330060` (version mismatch)
remain warnings. They do not turn a successful operation into a refusal. A timed-out
batch upload retains its attempted server even when a later upload disconnects the
session. The nested error keeps its code/message shape; remote refusals retain their
original code and text. Authentication-file permission degradation still depends on
the typed error and a proven persistence outcome, not a code prefix.

Alert diagnostics use module `034`, and capture diagnostics use `035`. Capture
observer failures remain warnings isolated from source processing. Loss of capture
ownership, incomplete shutdown and missing recovery history retain their original
fences and cleanup handles. A new identity never permits skipping a missing interval.
Latest Pipeline observations keep their existing code-string and parameter fields;
old and new identities read through the same codec without rewriting stored rows.
A later recovery clears the current failure under the original observation epoch.

Execution preparation uses module `036`. Source-schema admission retains HTTP 409,
while join shape drift, member staging and view storage refusals retain their
original typed recovery decisions. Declarative verification accepts canonical TS
identities and historical codes during migration; both the parser and published
schema reject zero modules/serials, malformed lengths and padded TS codes.

Cluster membership, connector lookup, data browsing, monitoring, position write-back,
store migration and row-map key validation use modules `037` through `043`.
Only the typed missing-collection refusal makes a target probe empty; source lookup
and invariant failures still propagate. A status response with HTTP 404 and
`TS0400001` means no observation is available yet, not that execution failed or
stopped. Polling consumers also accept the historical `monitor.no-observation`
reply during migration and must reject unrelated 404 and authorization failures.
Store migrations preserve transaction and version checks. Verification that launches
an actual older binary continues to expect that binary's historical refusal code.

Engine, transform and Nest sizing diagnostics use modules `044`, `045` and `046`.
Frontier pinning, displaced join dimension rows, duplicate expanded-row keys and
both Nest capacity advisories remain warnings. Lease loss and renewal uncertainty
still stop the unauthorized run; a new code cannot grant execution or replay a
write. Runtime exception types, original causes, closing suppression and bare
programmer-error branches retain their existing contracts.

Storage connection and startup diagnostics use module `047`. Initialization and
unsupported configuration schema refusals retain HTTP 409; invalid URI, unsupported
connection settings and invalid TLS trust retain HTTP 400. Keyring repair must keep
the original encrypted configuration and installation identity. Retention mismatch
still requires an explicit, coordinated database change; startup never rewrites TTL
indexes, and diagnostic migration does not authorize destructive recovery.

Storage operations use module `048`. Quota refusal remains HTTP 503; configuration
import refusal and required Pipeline state rebuilds remain HTTP 409. Keep the
original import operation when its outcome is unknown instead of replaying it.
Typed ownership fences, unreadable-document isolation and quota handling retain
their existing decisions. Damaged document diagnostics carry both the document
identity and a safe field category, without embedding the damaged payload.

Nest assembly diagnostics use module `049`. Missing-parent dead-letter handoff and
cold-state access remain warnings while the Pipeline continues. Shape mismatch,
state database or memory budget changes and structural-key tracking retain their
original refusal and recovery boundaries. Restarting a Pipeline does not change
a process-fixed memory budget; changing a state database still requires stopping
all writers and preserving or explicitly rebuilding the original state.

Connector bridge diagnostics use module `006`. The existing connection-test
identity `TS0060001` and its three-language text are retained under the same
shared definition. First-party typed connector refusals still produce failed
default-source reports after resource exit; interruption, cleanup uncertainty
and programmer defects keep their existing paths. Connector registration and
data browsing retain their context-specific HTTP status, and busy CLI polling
accepts current and historical replies. Upstream connector-authored identities
remain source-bound; a product module number never grants an upstream identity.

DSL errors use `TS0010001` through `TS0010031`, and
execution advisories use `TS0010032` and `TS0010033`. All registered first-party typed errors delegate to product definitions.
Historical records continue to render with their stored identities. Upstream
connector diagnostics retain their own identities and recorded provenance.
Coordinate Server, CLI, MCP and Web at deployment.

## Request refusal contract

`RequestErrorDefinitions.MALFORMED_REQUEST` is the single shared definition of
the malformed-request refusal. It owns module `CONTROL` (`021`), serial `0001`,
severity `ERROR`, and named parameter `reason`. The service's existing
`ControlError.MALFORMED_REQUEST` enum delegates to it. The independent CLI uses
the same definition for local request validation without importing service code.

The HTTP response remains 400. CLI diagnostic exit status and MCP `isError`
remain unchanged. A new identity does not mean that a request was accepted or
executed. Preserve typed refusal payloads, uncertain outcomes, cancellation and
cleanup behavior when migrating other families.

Authentication refusals retain their status and information boundaries: invalid
credentials do not reveal whether a username exists, missing authentication is
distinct from insufficient capability, and an unreachable service does not
prove an operation failed. CLI and browser consumers recognize the new codes
and still read the corresponding historical server replies during migration.
This read compatibility does not create aliases in the authoritative catalog.
Browser session refresh must retain account binding and must not replay an
operation as a different user.

AI provider errors retain the existing HTTP and streaming boundaries. Provider
credential refusal is distinct from workbench authentication: it must not end a
valid workbench session. Keep failed-check key cleanup, proposal rejection without
applying partial output, bounded provider behavior and explicit user recovery.
The numeric identity does not authorize an additional retry. Existing English
messages and recovery advice remain; entries without advice now supply localized
recovery guidance through the shared catalog.

Control capability and bootstrap refusals preserve their information boundaries
and existing statuses. An audit-write refusal means the operation did not run.
By contrast, `TS0210016` is a warning after artifacts were committed: retain the
nested cause parameters, partial-refresh explanation, successful CLI exit and
MCP success result. Never reclassify it as rejection of the committed artifacts.
Diagnostic-only scripts preserve a bounded canonical code and still omit
arbitrary server messages, credentials and parameters.

Storage refusals keep their layer and declared cause identities. A failed test
does not grant a save or initialize the runtime. Retain occupied-cache checks,
safe empty-database suggestions and the original operation when availability or
quota is uncertain. Display progress remains bounded, process-owned evidence;
it is not a durable preparation or authentication grant. Do not replay setup to
recover a quota failure. Client cause categories remain an explicit allow-list,
and arbitrary server text or connection URIs must not enter the error display.

DSL definitions live in `DslErrorDefinitions`, with the existing `DslError` and
`ExecutionAdvisoryError` enums retaining their typed APIs. Corpus rule symbols
are separate from diagnostic identities: `unknown-field` remains the rule name
while its diagnostic is `TS0010002`. Keep `DslException` source, path, line and
column through copies and wrappers. Warnings remain successful validation/apply
results; they do not become refusals merely because they carry a diagnostic code.

`SourceErrorDefinitions` owns module `SOURCE` (`022`). The existing `SourceError`
enum retains not-found, duplicate identity, path/body mismatch, missing ETag,
version conflict, references blocking deletion and active-capture SRS restrictions.
Their HTTP statuses and required parameters are unchanged. Consumers must keep
unsaved input and the existing ETag refresh choices after a conflict, distinguish
a missing Source from a missing test result, and stop batch deletion at a refused
Source while retaining the identities already removed. Historical server replies
remain readable during this migration; local browser preflight errors retain
their existing protocol shape.

`PipelineErrorDefinitions`, `ArtifactErrorDefinitions` and
`PipelineDraftErrorDefinitions` retain the existing typed enums and their
conditional-write semantics. A refused deletion means nothing was removed;
`TS0240006` means removal stands and bookkeeping remains incomplete. Keep its
residue and reason, and never suggest retrying that removal. A running Pipeline
must stop before its remaining state can be cleared. Start conflicts trigger
the existing interactive re-read; an unanswered start is never replayed.
Missing Pipeline and missing draft remain different outcomes, and draft
revision, immutable mode and artifact publication fences remain intact.

```java
throw new TapstateException(ControlError.MALFORMED_REQUEST,
        Map.of("reason", "history requires both --from and --to"), null);
```

Keep programmer errors and broken invariants as programmer errors. Do not turn
them into request refusals. Do not concatenate a code string at a throw site,
assign a new meaning to a published identity, or use map iteration to create
positional parameters.

## Registration and translations

`ProductErrorModule` explicitly allocates product module numbers; `006` remains
reserved for the existing connector diagnostic. A module allocation is not an
upstream-code classifier. Register definitions through the owning contract's
module-local `DEFINITIONS.define(...)` collection. Each declaration registers itself;
there is no second per-code list to update. The owner class initializes all constants
before `registered()` seals an immutable snapshot. New modules still need explicit
module and catalog wiring. `RequestDiagnosticCatalog` projects that snapshot into
`ProductDiagnosticCatalog`, which implements `DiagnosticDefinitionLookup`.
There is no runtime classpath scan or enum reconstruction.

The request language resources are
`messages/src/main/resources/messages/request-diagnostic-{en,zh-CN,zh-TW}.json`.
DSL resources use the corresponding `dsl-diagnostic-` prefix and the explicit
`DslDiagnosticCatalog` registration, without a parser dependency in the renderer.
Source resources use `source-diagnostic-` and `SourceDiagnosticCatalog`.
`MutationDiagnosticCatalog` explicitly loads the `pipeline-diagnostic-`,
`artifact-diagnostic-` and `pipeline-draft-diagnostic-` resource families.
Each identity has nonempty `message` and `solution` fields. Their combined named
placeholders must match the definition exactly. This public repository stores
non-ASCII translations as JSON Unicode escapes. The native CLI includes both
YAML and JSON message resources.

Use a backslash to escape a literal brace or backslash in a template, with the
additional JSON escaping required by the resource format. For example,
`"Use $\\{var:NAME:default\\}."` renders `Use ${var:NAME:default}.` without
declaring an extra parameter. Unescaped braces must form a valid named parameter;
unknown escapes and unmatched delimiters fail validation. Inserted argument text
is never parsed again as a template.

```java
var definition = RequestErrorDefinitions.MALFORMED_REQUEST;
var text = MessageCatalog.bundled().render(definition.code(),
        Map.of("reason", "history requires both --from and --to"), Locale.ENGLISH);
```

The renderer is not a sanitizer, authorization check or capture mechanism.
Supply only parameters approved for that output boundary. It does not inspect
arbitrary causes or invent a diagnosis for an unknown identity. Strict lookup
returns empty for unknown codes, unexpected/missing named parameters or
positional inputs; the message renderer retains the unknown code as text.
Legacy request text remains readable under its original identity, but current
request producers no longer emit that identity. The connector compatibility
facade retains its existing source-bound legacy adaptation separately.

## Maintaining and checking the contract

For a new definition, reserve its module/serial, declare its parameter contract,
add it through the owning definition collection, add all three language entries and migrate the actual producers and
consumers in the same batch. Review HTTP/CLI/MCP outcomes independently from the
code's spelling. Extend the current golden only after comparing the independently
retained historical baseline. Never regenerate the historical baseline to make a
changed contract pass.

Find this definition and its consumers:

```sh
rg -n 'MALFORMED_REQUEST|TS0210001' core control cli messages
```

Run directory validation and the definition, history and dependency gates:

```sh
mvn -pl messages -am \
  -Dtest=JsonReaderTest,ProductDiagnosticCatalogTest,MessageCatalogTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
mvn -pl arch-tests -am \
  -Dtest=ErrorCodeGatesTest,RequestDiagnosticDefinitionGatesTest,DslDiagnosticDefinitionGatesTest,SourceDiagnosticDefinitionGatesTest,MutationDiagnosticDefinitionGatesTest,ConnectionDiagnosticDefinitionGatesTest,RingDependencyRulesTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

Positive cases include all three locales, exact named parameters, preserved
unknown-code fallback, literal grammar examples and legacy text reads. The
`product-code-migrations.tsv` test fixture records reviewed replacements against
the frozen historical golden; it is not a runtime alias table. Negative cases include duplicate
registration or JSON members, an unallocated module, serial zero, a missing
locale entry, an extra/missing placeholder, positional `{0}` and malformed
`{{reason}}` delimiters. These checks validate structure and frozen identity;
they cannot prove the business meaning or safety of arbitrary argument values.

Rebuild the actual native CLI after changing resources and exercise both a
migrated refusal and an unmigrated diagnostic. JVM tests alone do not prove that
the native binary includes dynamically addressed language resources.


### Query, export and independent history

Use JDK 21 (`JAVA_HOME`) and Python 3. The maintenance tool compiles the zero-dependency
core definitions into a disposable directory and inspects those compiled classes.
This is build-time inspection, not runtime scanning or source generation. The JSON
export comes from the same authoritative constants used by typed producers:

```sh
python3 tools/error-contract/error-contract.py query --code TS0210001
python3 tools/error-contract/error-contract.py export --output /tmp/tapstate-error-contract.json
base=$(git rev-parse origin/main)
python3 tools/error-contract/error-contract.py check --base "$base"
# Published v0.7.0 and v0.8.0, pinned independently from the target branch.
python3 tools/error-contract/error-contract.py check --base d2cc4b96b244722f3f0c1832c05b14772ae2cff7
python3 tools/error-contract/error-contract.py check --base 90e2be3fc296d7ad309a33102f12251423d51151
python3 -m unittest discover -s tools/error-contract -p 'test_*.py'
```

Fetch the target branch before selecting the base and record its complete SHA.
CI selects the pull request's target SHA or the push event's previous SHA; its
history check also compares the separately pinned published v0.7.0 and v0.8.0 sources and is part of the prerequisite shard-plan job, so a refusal prevents
the aggregate build gate from succeeding. An unavailable commit is a failure,
not an empty contract. The pre-cutover code fixture is also compared with its immutable Git tree;
changing a working-tree golden cannot rewrite that history. Historical wording
changes are reported for review instead of blocking editorial improvements. After cutover, base definitions are compiled independently
and compared for identity, owner, severity, parameter names and deprecation.
New identities may be added; existing identities cannot silently change or vanish.

For the initial major cutover, the tool independently compiles the old typed enums
from the selected Git tree and compares their owner/constant, severity and named
parameters with the current typed producers through the migration correspondence.
Changing that correspondence cannot silently swap the typed owners. An empty or
unavailable typed baseline fails. Behavior tests still establish actual usage.
This proves the historical structural input, not the semantic correctness of a replacement. Reviewers must still assess wording,
business meaning, branch coverage and the approved compatibility window. Editing
this checker or its workflow is itself a reviewed mechanism change, not a way to
approve a contract break. These checks do not claim to resist a malicious change
to all verification code.

### Deprecating a product identity

Mark the authoritative public definition field `@Deprecated` and document its
replacement and the supported caller transition in its Javadoc. Mark any typed
enum constant deprecated as well when callers should stop producing it. Keep the
constant, module/serial, parameter contract and all three language resources:
deprecation is not permission to recycle the identity or rewrite an old event.
The export reports `deprecated`; the independent history check rejects later
reactivation, removal, ownership, severity or parameter changes. A semantic or
breaking change gets a new identity and an explicitly reviewed release boundary.
Do not edit the historical fixtures to retire an identity.

For a maintenance exercise, work in a disposable checkout: add a temporary constant
through `DEFINITIONS.define`, its typed enum and three resource entries; query,
validate and export it, then mark it deprecated and export again. Verify missing
translations, duplicate serials, omitted catalog wiring and changed historical
parameters fail. Never publish the exercise definition or its golden changes.

### Evidence in everyday changes

Each feature or repair identifies affected producers, storage and consumers, the
business branches exercised and their outcomes. A shared diagnostic change needs
consumer regressions; a typed-code assertion alone does not prove cancellation,
cleanup, HTTP/CLI/MCP semantics or recovery. Reproducible defects include a failing
baseline and a passing repair. Compilation failures and skipped checks are not
behavior evidence. Static gates cover deterministic violations; reviewers own
meaning, safety of parameter values, cause attribution and completeness of the
scenario evidence.
