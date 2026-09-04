# DSL corpus — acceptance baseline for the core-dsl pipeline (plan poc1 B1)

This corpus materializes every scenario of [ADR-0016](../../../../../../docs/adr/0016-dsl-grammar.md)
§14 (14.1–14.11, X19 included) as loadable `.tap.yml` workspaces. It is the acceptance
baseline for B2–B5: parse, validate, CEL checking, and canonical round-trip are all
asserted against these files. Grammar branches the corpus does not exercise are not
implemented defensively (plan risk R3, corpus-first).

Authoring rules:

- **Only defined syntax.** Every file uses §1–§13 grammar exactly as decided; no invented
  fields, no speculative sugar. When the grammar changes, the ADR changes first.
- One file per resource, named `<top-level id>.tap.yml`, single YAML document.
- Comments are English and cite the deciding ADR section / X-decision.
- `CorpusSmokeTest` guards this structural contract (well-formedness, layout, rule
  vocabulary). It does **not** check DSL semantics — that is the validate engine's job.

## valid/ — one workspace directory per ADR-0016 §14 scenario

Each directory is an independently loadable workspace batch: every referenced id resolves
inside the directory (offline closure = the batch, ADR-0021 §3). Ids may repeat across
directories — uniqueness is per batch.

`s01`–`s11` are the original §14 scenarios; `s12`–`s13` are read_mode-amendment additions
(§11.8) exercising grammar that amendment introduced — `read_mode: snapshot_only` bounding a
cdc read, and `srs.enabled: false`.

| Directory | ADR | Scenario | Notable grammar surface |
|---|---|---|---|
| `s01-mirror-rename-ddl` | §14.1 | Oracle → MySQL whole-source mirror | `serve.from: /.*/`, `sync[].rename` (map+case+prefix), `ddl: apply`, `auto_create_table` |
| `s02-modeling-nest-rest` | §14.2 | Oracle → SRS → nest → view → REST | `srs` tuning, `map`, `nest` full tree (2-level embed), view tiers, `query: rest` |
| `s03-shared-srs-es` | §14.3 | Second consumer of a shared source | shared SRS narrative, `filter` + CEL, literal table = frozen link |
| `s04-kafka-stream-ingest` | §14.4 | Kafka → cleanse → MySQL | `mode: stream`, `settings.start_from` (pipeline-level), `js` escape hatch, `write_mode: append` |
| `s05-cdc-to-kafka-push` | §14.5 | MySQL CDC → Kafka | `push` element (no `type`), `topic`, `settings.read_mode: cdc_only` |
| `s06-api-poll-mongo` | §14.6 | GitHub API polling → MongoDB | `mode: api`, `poll_interval`/`cursor`, `tables[].pk`, whole-source regex |
| `s07-csv-batch-import` | §14.7 | CSV batch import + cleanse | `mode: file`, literal-only `tables`, map computed value (`"=CEL"`) |
| `s08-dual-source-join` | §14.8 | Cross-database dual-source join | `source:` id list, `join` (duckdb SQL + aggregation), view-only output |
| `s09-filter-fanout-pipelines` | §14.9 | Conditional fan-out, v1 form | N pipelines × `filter` over one shared source (router is out of v1, X14) |
| `s10-ddl-evolution-chain` | §14.10 | DDL / schema evolution five-hop chain | `include_ddl`, `srs.schema_evolution: track`, map passthrough, `view.schema`, sink `ddl: apply` |
| `s11-reuse-assembly` | §14.11 | Definition bodies + pure-reference assembly | `kind: transform/view/serve` definitions, string = `use:` sugar, natural-order wiring (X19) |
| `s12-cdc-snapshot-only-rerun` | §14.1 | cdc source read one-shot on a schedule | `settings.read_mode: snapshot_only` bounds a cdc read, so `settings.schedule` is legal (read amendment) |
| `s13-srs-disabled-passthrough` | §4 | cdc with the Shared Record Store off | `srs.enabled: false` — D14 lightweight passthrough path (read amendment) |

## invalid/ — minimal self-contained violation batches

Each case is the smallest batch that exhibits exactly one violation, plus an
`expected.yml` sidecar (not matched by the `*.tap.yml` loader glob):

```yaml
rule: <vocabulary key>   # machine-checked against the list below
adr:  "<deciding ADR section>"
path: <field path of the violation>
note: "<one-line human explanation>"
```

Rule vocabulary (B3 maps these to real error codes; extend the list and the smoke test
together):

| `rule` | Meaning | Source |
|---|---|---|
| `unknown-field` | field outside the v1 schema, strict rejection | §11.5 |
| `forbidden-field` | field known to the schema but banned in this position | X18/X19 |
| `missing-reference` | id / table / step reference with no target in the batch | §1/§4/§8 |
| `ambiguous-reference` | bare table name colliding across declared sources | §4 |
| `mode-mismatch` | option / block illegal for the source mode or boundedness | §4/X7/X10 |
| `illegal-value` | enum or format constraint violation | §2/§8 |
| `illegal-expression` | CEL expression field fails to compile or type-check | §12 |
| `composition` | structural composition rule broken | X17 |
| `duplicate-id` | id collision: workspace top-level uniqueness, pipeline-internal uniqueness, or step-id shadowing of a source id / table name | §2/F8, §5 |
| `unsupported-mode` | source mode outside the connector's declared capability matrix | §4 / C3 |
| `config-type-mismatch` | connector config value whose type differs from the connector's declared field type | C3 |
| `invalid-config-value` | connector config value outside the connector's declared enum choices | C3 |

Cases (sNN ties the case to the valid/ scenario it mutates; gNN = general grammar rule):

| Case | rule | Violation |
|---|---|---|
| `s01-unknown-field-mode-typo` | unknown-field | `mod:` typo of `mode` (§11.5 canonical example) |
| `s02-missing-source-ref` | missing-reference | pipeline references a source absent from the batch |
| `s03-unknown-step-ref` | missing-reference | `serve.from` names a nonexistent step |
| `s04-srs-on-stream` | mode-mismatch | `srs:` block on `mode: stream` |
| `s05-push-with-type-field` | unknown-field | push element carries the removed `type` field |
| `s06-start-from-on-api` | unknown-field | source-level `options.start_from`, removed by the read amendment (moved to pipeline settings) |
| `s07-pipeline-no-output` | composition | pipeline with neither view nor serve |
| `s08-ambiguous-table-ref` | ambiguous-reference | bare table name present in two sources |
| `s09-schedule-on-unbounded` | mode-mismatch | `settings.schedule` with a cdc source |
| `s10-illegal-ddl-enum` | illegal-value | `ddl: skip` outside {apply, ignore, fail} |
| `s11-definition-body-with-from` | forbidden-field | `from:` on a `kind: transform` definition body |
| `g01-id-contains-dot` | illegal-value | id containing the reserved `.` separator |
| `g02-duplicate-top-level-id` | duplicate-id | same top-level id in two files of one batch |
| `g03-duplicate-pipeline-internal-id` | duplicate-id | two transforms steps in one pipeline share an id (internal namespace, 2026-06-15) |
| `g04-step-id-shadows-table` | duplicate-id | step id equals a literal table name in the source (no shadowing, ADR-0016 §5) |
| `g05-step-id-shadows-source` | duplicate-id | step id equals a referenced source id (no shadowing, ADR-0016 §5) |
| `g06-cel-unknown-envelope-field` | illegal-expression | filter references `afterr`, a typo of the `after` envelope field (CEL type-check) |
| `g07-cel-map-syntax-error` | illegal-expression | map computed value `"=after.region +"` is not well-formed CEL (CEL parse) |
| `g08-cel-push-object-format` | illegal-expression | push object-form computed field `"=after.region +"` is not well-formed CEL (distinct `format.<field>` path) |
| `g09-unsupported-mode-for-connector` | unsupported-mode | `connector: kafka` with `mode: cdc` (kafka declares only [stream]) |
| `g10-config-type-mismatch` | config-type-mismatch | `mysql.masterSlaveAddress` (array field) given a scalar string |
| `g11-config-enum-violation` | invalid-config-value | `mysql.deploymentMode` set to `cluster`, outside the declared enum |
| `g12-snapshot-only-on-stream` | mode-mismatch | `settings.read_mode: snapshot_only` over a pure stream source (no one-shot read) |
| `g13-start-from-without-tail` | mode-mismatch | `settings.start_from` on a bounded read (api source, no incremental tail) |
| `g14-start-from-on-snapshot-only-cdc` | mode-mismatch | `settings.start_from` on a cdc source read `snapshot_only` (the read_mode removed the tail, so the reported mode is the read_mode) |

Connector-dimension variants (capability matrix, config field checks) are validated against the
bundled catalog by plan task C3. The catalog's mode signal is trusted only where it is reliable —
a database connector's derived modes, or any connector's explicitly declared modes — so a
non-database connector whose modes are purely derived (its real stream/api/file mode being
undeclared) is deferred to the server rather than wrongly rejected offline.
