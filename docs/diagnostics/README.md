---
status: engineering-draft
publication: handoff
target: https://tapstate.dev/docs/reference/error-diagnostics
---

# Product error diagnostics

This page describes an unshipped migration candidate. The first migrated request
refusal is `TS0210001`. Other error families retain their existing contract until
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
Each identity has nonempty `message` and `solution` fields. Their combined named
placeholders must match the definition exactly. This public repository stores
non-ASCII translations as JSON Unicode escapes. The native CLI includes both
YAML and JSON message resources.

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
  -Dtest=ErrorCodeGatesTest,RequestDiagnosticDefinitionGatesTest,ConnectionDiagnosticDefinitionGatesTest,RingDependencyRulesTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

Positive cases include all three locales, exact named parameters, preserved
unknown-code fallback and legacy text reads. Negative cases include duplicate
registration or JSON members, an unallocated module, serial zero, a missing
locale entry, an extra/missing placeholder, positional `{0}` and malformed
`{{reason}}` delimiters. These checks validate structure and frozen identity;
they cannot prove the business meaning or safety of arbitrary argument values.

Rebuild the actual native CLI after changing resources and exercise both a
migrated refusal and an unmigrated diagnostic. JVM tests alone do not prove that
the native binary includes dynamically addressed language resources.
