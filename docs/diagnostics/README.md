---
status: engineering-draft
publication: handoff
target: https://tapstate.dev/docs/reference/error-diagnostics
---

# Product error diagnostics

This page describes an unshipped migration candidate. The first migrated request
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
DSL errors use `TS0010001` through `TS0010031`, and
execution advisories use `TS0010032` and `TS0010033`. Other error families retain their existing contract until
their producer, consumers and behavior tests migrate together. The complete
product contract switches with a coordinated major release; this candidate does
not establish a supported mixed-version deployment matrix.

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
`registered()` list. `RequestDiagnosticCatalog` projects that list into
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
register it, add all three language entries and migrate the actual producers and
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
