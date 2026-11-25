# Current Preview Runtime Support

This matrix is pinned to Tapstate commit
`a03aa7265be765b9a48dd2464e2e987f24ada60e`. The generated schema describes the
complete `tapstate/v1` grammar; this file describes the narrower runtime wired at
that commit. Update both this matrix and the repository upstream lock when the
baseline changes.

Offline validation and runtime execution are independent claims. All three
bundled examples pass offline validation, but none is runnable until MCP or
another trusted live source supplies the connector-specific `source.config`
members.

## Online authoring handoff

When MCP exposes `source_draft`, use it to validate a structured Source config
against the live connector contract and render canonical YAML. It does not write
an artifact or create an audit record. The authoring sequence is local file
creation, offline validation, `connection_test` and
`connection_discover_schema` for every read Source, complete-closure
`artifact_validate`, complete-closure `artifact_apply`, then optional Pipeline
start. Sink-only connection suppliers are not discovered. Fix discovery or
validation diagnostics before applying. Do not apply a standalone Source as a
substitute for workspace authoring.

MCP availability is established by the tools exposed to the current agent
session. A local `tapstate validate` result, shell command, or file inspection
does not establish Server reachability. If a required MCP tool is exposed, call
it directly and preserve its transport or structured error; only a direct
transport failure justifies reporting that the Server is unreachable.

### Changing and removing applied resources

`artifact_get` reads one applied resource of any kind by id and returns its
canonical YAML with the `contentHash` of exactly those bytes. `artifact_delete`
removes one applied resource of any kind by id and requires both `id` and
`expectedContentHash`. It is write-scoped, so it appears only in a session
permitted to write. Each draft of an `artifact_apply` closure accepts an
optional `expectedContentHash`, and one stale precondition refuses the whole
closure before any resource in it is written.

`artifact_list` is not exposed on this face at this commit. An id must already
be known, or come from a face that does list.

Removal is permanent and leaves no tombstone. Two grounds refuse it, both judged
before anything is written, so a refusal leaves storage unchanged:
`artifact.in-use` when another resource still references the id, and
`artifact.pipeline-not-stopped` when the id is a pipeline whose actual or
desired state is running or about to be. A precondition that does not match the
stored version is `artifact.version-conflict`; an id that is not stored at all
is `artifact.not-found`.

Removing a pipeline reclaims dependent state in the same operation. What is and
is not reclaimed is a boundary rather than an implementation detail, because
everything that survives is shared with something else:

| On removing a pipeline | Current preview runtime |
|---|---|
| Its desired, state, and observation documents | Reclaimed in the same operation, so the converge side stops reconciling the id. |
| Its own consumer entry on a shared SRS mining chain | Detached. A departed consumer's cursor is otherwise folded into two independent minimums that pin the chain's durable frontier and its cdc write headroom for every other pipeline on that chain, raising no error. |
| The mining chain record itself | Never removed. Other pipelines may still be consuming it. |
| Nest state | Never removed. |
| Target tables and target database contents | Never removed. Removing a pipeline removes the declaration, not the data it already wrote. |

A reclaim that fails does not put the artifact back: the removal has been judged
and applied by then. Every reclaim step is attempted regardless, and the failures
are reported together as `artifact.reclaim-incomplete`, carrying `id`, `reason`
and the `residue` it did not clear. This is the one failure here that is not a
refusal: the resource is already gone, so repeating the removal can only answer
`artifact.not-found`. Report it; do not retry it.

Its `reason` decides the next step, and the two differ:

| `reason` | What happened | Next step |
|---|---|---|
| `step-failed` | The removal stands and the steps that succeeded stand; the named `residue` was not cleared. | Clear the named residue by hand. |
| `pipeline-live` | The pipeline was started again after the refusal was judged and before the reclaim ran, so nothing was reclaimed and all four records are named as residue. | Stop the pipeline first. Clearing by hand while its job still runs discards the fencing epoch, which is what keeps that job from colliding with a later pipeline applied under the same id. |

## Capability matrix

| Surface | Grammar and offline validation | Current preview runtime |
|---|---|---|
| Read Source | All source modes, literal/object/regex table selectors, and multiple tables are authorable. | The runtime path expects one read Source selecting exactly one non-regex table. Literal and object-form names reach capture; omitted, multiple, and regex tables fail during assembly. |
| Target Source | A source may omit mode and tables when used only as a connection supplier. | Supported by inline sync; connector and config are used to open the sink. |
| Source references | Table, step, source-qualified table, and regex references validate. | The linear builder resolves simple literal table or step names. Qualified and regex `from` references are not carried. |
| Inline filter | CEL boolean expressions validate. | Supported. Row events are filtered; DDL bypasses. |
| Inline map | Rename, drop, literal, and `=CEL` rules validate. | Supported over `after`; deletes and DDL bypass. Unlisted fields pass through. |
| Inline JavaScript | Script bodies validate structurally. | Supported by the stateless transform adapter. |
| Inline union | Multi-input list wiring validates. | Supported as a single-lane passthrough fan-in. |
| Inline nest | Alias wiring and recursive array, object, and pathless flat children validate. | Supported. Flat accepts one-to-one and many-to-one; a live one-to-many match stops with `nest.flat-cardinality-violation`. Known model collisions are refused before start and actual-row collisions stop with `nest.flat-field-conflict`. |
| Inline join | Alias wiring, engine, and SQL validate. | Rejected by the linear DAG builder as a stateful transform. |
| Transform `use` | Reusable transform definitions and use sites validate. | Unresolved use steps are rejected; no definition-resolution phase is wired. |
| Inline `serve.sync` | Sync elements, modes, DDL, rename, and options validate. | Supported as sink vertices. Multiple sync elements fan out from the same upstream. |
| Serve `use` | Reusable serve definitions and use sites validate. | Rejected; no definition-resolution phase is wired. |
| `serve.query` | REST, GraphQL, and MCP query declarations validate. | Not executed by the DAG builder. |
| `serve.push` | Push source, topic, format, and options validate. | Not executed by the DAG builder. |
| Inline or reusable view | View wiring, tiered storage, and schema policy validate. | Not materialized or queried by the current data plane. Reusable view resolution is therefore also unavailable. |

The `straight-cdc` and `filter-and-map` examples use the grammar-valid reusable
Source shape with Pipeline-level table selection. The pinned preview runtime may
still reject an open Source table universe; report that runtime limitation rather
than adding a Source allowlist that the user did not request. The
`nested-document` example combines an array child with a many-to-one flat child
and is executable by the pinned source runtime once connector config,
discovery, target support, and online validation have succeeded.

The independently pinned released CLI is v0.5.0 and predates flat embeds. It
continues to validate the compatibility examples, while CI builds the exact
Tapstate source commit above to validate all examples. An installed v0.5.0 CLI
will reject `as: flat`; use a build containing the pinned commit or a later
release. Do not reinterpret that version mismatch as an error in the workspace.

## Settings and options

The current assembly uses only part of the accepted surface:

| Field group | Runtime status |
|---|---|
| `settings.read_mode` | Used to choose snapshot, tail, or both. |
| `settings.start_from` | Used for a shared incremental tail. Runtime accepts exactly `earliest`, `latest`, or an ISO-8601 instant. Other strings pass offline shape checks but fail when the run is assembled. |
| Omitted `settings.start_from` | The current assembly uses `earliest`, while the generated schema documents `latest` as the grammar default. Set it explicitly when cursor position matters. |
| `settings.error_policy`, `batch_size`, `parallelism`, `schedule` | Accepted and mode-checked where applicable, but ignored by the current execution path. |
| `source.srs.key`, `retention`, `enabled` | Used by capture and replay-store assembly. |
| `source.srs.schema_evolution`, `queryable` | Accepted but ignored by the current execution path. |
| `source.options`, table `filter`, `pk`, and table `options` | Accepted but ignored by the current capture translation. Only the selected table name is carried. |
| Transform `options` and `experimental` | Accepted but ignored by the current stateless transform binding. |
| `serve.sync.source`, `write_mode`, `ddl` | Used by sink assembly. |
| `serve.sync.rename` and `options` | Accepted but ignored by the current sink binding. |
| View storage/schema and serve query/push options | Accepted but no runtime surface consumes them. |

Do not convert an ignored field into a run claim. Preserve it as authored intent
and state that the pinned preview does not enforce it.

## CEL runtime gaps

Offline CEL compiles against `op`, `ts`, `src`, `before`, `after`, and `schema`.
Nested row fields are dynamic, so some overload mistakes surface only at
evaluation.

- Keep preview-safe examples string-only. Numeric values from row maps can fail
  CEL numeric overload dispatch even when the expression passed offline checks.
- `now()` is declared by the compiler and therefore validates, but the current
  runtime registers no implementation. Evaluating it fails with a coded
  expression error.
- A missing dynamic field or incompatible runtime type can still fail during
  evaluation. Offline validation cannot infer a connector's row schema.

These limitations do not change map syntax: every computed map rule must still
start with `=`. Without that marker, the authored string is a literal.
