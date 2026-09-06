---
status: engineering-draft
publication: handoff
target: https://tapstate.dev/docs/first-run
---

# First run

This page is the contract for the guided first run: what a new user types, what each
step asks, what gets written to disk, and what the CLI says afterwards. Everything the
implementation does on this path is checked against it — the goldens for each recipe,
the help text, and the machine-readable catalog all derive from here, so a change to
behaviour is a change to this page first.

The whole path, on a clean machine:

```
curl -sSL https://install.tapstate.dev | sh    # installs the CLI, nothing else
tapstate new                                   # which server? which outcome? -> writes a workspace
tapstate up                                    # brings that workspace to RUNNING
```

No resource YAML has to be read or written to get there. The workspace that `new` writes
is an ordinary one — readable, editable, and exactly what the lower-level commands
(`validate`, `ls`, `desc`, `apply`, `start`) operate on. The guided commands hide the
*sequence*, never the model.

## Entry modes

Four ways in, with different side effects. Help text and documentation always say which
one a command enters; they are never presented as interchangeable.

| Entry | What it does | What it must not do |
|---|---|---|
| `curl -sSL https://install.tapstate.dev \| sh` (also `/cli`) | Install the CLI binary for this platform | Start a server or a demo as a side effect |
| `https://install.tapstate.dev/demo` | Bootstrap a disposable full demo: server, sample databases, connectors, CLI, a demo workspace, and one running pipeline | Be described as "installing Tapstate" |
| Server-backed guided use (`tapstate new` → `tapstate up`) | The path this page describes | Introduce a second configuration model or a second lifecycle |
| Manual operation (explicit YAML + `apply` / `discover` / `start`) | Full control; the reference path | Be the public first-run path |

`tapstate` never installs, upgrades, clusters or shuts down a server. The one thing it will
start is a local development stack, as a convenience behind a default answer (below); the
server's own lifecycle is managed outside this CLI.

## `tapstate new`

Bare `new` at a terminal runs the guided flow. `new --kind <kind>` keeps its existing
meaning — scaffold one resource — and is unchanged. The bare form used to enter the
single-resource wizard; from this release it enters the recipe picker. That is a deliberate
change to a released command and is called out in the release note.

### Step 1 — which server

The first question is only asked when the current directory is not already bound to a
server. Two answers:

| Answer | Meaning |
|---|---|
| **(a)** `http://127.0.0.1:8080` — the default, taken on an empty reply | Use the server on this machine. If nothing is listening there, start a local development stack in Docker on that port, wait for it to be healthy, then register and bind it. |
| **(b)** a URL you type | Use a server you already run. You are asked to sign in. |

Either answer is saved as a registered server and bound to the workspace directory, so the
question is not asked again in that directory and `up` knows where to go.

The local development stack, when the default has to start one:

- is a **pinned** version of the server and its managed store, fetched and verified from a
  single configurable artifact source (the release location by default);
- comes with **every official connector pre-registered**, so a recipe against your own
  MySQL / PostgreSQL / MongoDB can be applied without hunting for a jar;
- contains **no sample data** — that is what the demo is for;
- is started **idempotently**: a second `new` finds the running one and does not start a
  second;
- ends by printing where the stack lives on disk and the one command that stops it. There
  are no `stop` / `status` / `upgrade` verbs for it in this CLI, and there will not be.

**Non-interactive runs never start containers silently.** With `--yes` and nothing listening
on the default port, `new` stops with a named error unless `--start-local` was passed
explicitly. Scripts either pass that flag or pass `--server <url>`.

### Step 2 — which outcome

The wizard states what it is building — *a workspace: a directory of `.tap.yml` files you
can read and edit* — and then asks what that workspace is for. The catalog is short and
curated, worded by outcome, not by mechanism:

| id | Shown as | What it writes | Uses |
|---|---|---|---|
| `sample` | Try it with sample data | the three demo files (MySQL orders + PostgreSQL shipments → one order document) | built-in demo stack |
| `mirrored-table` | Mirror one table, as it changes | one source, one pipeline, one view of the same shape | `cdc` |
| `reshaped-table` | Mirror a table, renamed / filtered / trimmed | as above, plus `map` and/or `filter` steps | `cdc`, `map`, `filter` |
| `nested-json` | Assemble several tables into one object | one or more sources, one pipeline with a `nest` step, one view | `nest` |
| `consolidated-table` | Consolidate the same table from several databases | one source per database, one pipeline with a `union` step, one view | `union` |
| `blank` | Nothing generated - I will write it myself | an empty workspace directory | — |

Rules the catalog follows, so that the next recipe added behaves like these:

- **An id names what you end up with**, `<past-participle>-<noun>`, never how it is done.
  Internal type names (`nest`, `union`, `join`) are not ids.
- **Every listed recipe runs today.** A capability that is not shipped yet does not appear
  greyed out; it is added when it ships. `blank` is the deliberate exception — it produces
  nothing to run, and sits last because it means "none of the above", not "start here".
- **Every recipe starts from an empty directory.** Recipes that add to an existing workspace
  are a different class; if one is ever added it declares that, and the picker does not offer
  it in an empty directory.
- **One id space.** Later recipes — including ones aimed at a specific role — are added to
  this list, never to a second catalog.
- **Every recipe's output lands in the view store** (`views.<id>`). The first run does not
  ask who will read the result; publishing it (`serve`) is a later step, not part of this
  path.
- `tapstate demo` is an alias for `new sample --yes` and writes byte-identical files.

`new --list` prints the catalog. `new --list -o json` prints it for scripts and AI entry
points:

```json
{
  "recipes": [
    { "id": "sample",            "title": "Try it with sample data",                          "runnable": true,  "uses": [] },
    { "id": "mirrored-table",    "title": "Mirror one table, as it changes",                  "runnable": true,  "uses": ["cdc"] },
    { "id": "reshaped-table",    "title": "Mirror a table, renamed / filtered / trimmed",     "runnable": true,  "uses": ["cdc", "map", "filter"] },
    { "id": "nested-json",       "title": "Assemble several tables into one object",          "runnable": true,  "uses": ["nest"] },
    { "id": "consolidated-table","title": "Consolidate the same table from several databases","runnable": true,  "uses": ["union"] },
    { "id": "blank",             "title": "Nothing generated - I will write it myself",       "runnable": false, "uses": [] }
  ]
}
```

Keys are stable; new keys may be added, existing ones are not renamed. The text output is
generated from the same list and is held to a golden, so wording cannot drift between the two.

### Step 3 — the recipe's questions

Each recipe asks only what it needs. Connection fields come from the connector's catalog
entry: the required ones and the secret ones are asked, optional ones take their defaults
and are not asked. Ids are suggested and taken on an empty reply.

| Recipe | Asks |
|---|---|
| `sample` | nothing |
| `mirrored-table` | connector (official list, default `mysql`) · connection fields · one table (no default; a blank answer is refused, not re-asked) · view id (defaults to the table name) |
| `reshaped-table` | as `mirrored-table`, then: columns to keep or rename, columns to drop, an optional row filter. No script step — a transform that needs code is written by hand after `blank` or by editing the file |
| `nested-json` | the root table (connector · connection · table · key), then one or more child tables (connector · connection · table · the columns that join it to the root · one-to-one or one-to-many) |
| `consolidated-table` | the table name once, then two or more databases (connector · connection) that hold it |
| `blank` | nothing beyond the workspace directory |

**What the generated files are called, and what is assumed** — fixed here so scripts and goldens
can rely on it:

- ids are derived, never asked: source `<table>_src`, pipeline `<table>_sync`; the view id is the one
  answer the user gives (default: the table name);
- a view's `from` is the **table name**, because that is what the pipeline's reference closure
  resolves (a step id or a table name, never a bare source id);
- the view's `primary_key` is written as `id` — the real key is not known until discovery runs, and
  the summary line for that file says so; edit it if the table is keyed otherwise;
- secret environment names are `<SOURCE_ID>_<FIELD>` upper-cased, e.g. `ORDERS_SRC_PASSWORD`;
- connection fields are written as the canonical writer renders the connector's catalog types (a
  `string`-typed port comes out as `port: "3306"`); the demo files are hand-written and differ in
  such spacing, which is fine — they are copied, never generated;
- a choice list's default is its first entry (for the recipe question, `sample`).

**Answer semantics, the same in every question:**

- An empty reply takes the default shown in brackets. Where there is no default the prompt
  says `(blank to skip)` and an empty reply skips. A choice list marks its default; an
  empty reply takes that one, never the last item.
- Ctrl-C aborts and writes nothing.
- Secrets are prompted masked. They are **not** written into the `.tap.yml`: the file holds
  a reference (`password: ${ORDERS_DB_PASSWORD}`), the value goes into a `.env` file next
  to it, and a generated `.gitignore` excludes `.env`. `up` and the lower-level commands read
  `.env` from the workspace root into the environment they interpolate from.

### What `new` writes

A workspace directory laid out by kind — `source/<id>.tap.yml`, `pipeline/<id>.tap.yml`,
and so on — plus `.env` and `.gitignore` when a secret was entered. Existing files are never
overwritten; `--force` is the only way to replace one, and it says which files it replaced.

### Non-interactive form

Every question has a flag; `--yes` means "never prompt". With the same flags the output is
byte-identical run to run — that is what the goldens assert and what the docs and tests rely
on. A required answer that is missing stops with a named error naming the flag; `--yes`
never guesses.

### What `new` says afterwards

The last thing printed, in this order:

1. **Where the workspace is, and what is in it** — every file, one line each, with what that
   file is for (the same wording `desc` uses for it).
2. **State** — "not running yet".
3. **What you can do next**, as commands: open a file and edit it; `tapstate validate`;
   `tapstate ls` / `tapstate desc <id>`; `tapstate up`.
4. **One line saying an AI assistant can take it from here** — pointing at the published
   documentation, never at a specific integration.

`blank` prints the same shape with an empty file list and says so in words. `-o json` and
`-o yaml` return the created files and their roles as structured fields and carry none of
the prose.

## `tapstate up`

Brings the bound workspace to a running state. It runs the ordinary sequence — apply the
sources, discover schema where a source needs it, apply the rest of the workspace, start the
pipeline — through the same services the individual commands use. There is no second
lifecycle.

- **Idempotent, by convergence.** Running it again on a workspace that is already up
  changes nothing and says so. If something blocks convergence (a resource changed on the
  server in a way the workspace does not describe), it stops with a named error saying
  what, rather than guessing.
- **Preflight before mutation**: server reachable and version-compatible, workspace
  readable, the connector each source needs registered on that server, each source
  reachable. Anything missing is reported with the stage, a stable error code, and the next
  action — before anything is applied.
- **A failure names its stage.** "Discovery failed on `orders_db`: <code> — <next action>",
  never the internal command that happened to be running.
- Flags: `--server <url>` overrides the bound server for this run; `--yes` for scripts.

What `up` says afterwards follows the same shape as `new`: the workspace, the pipeline and
source names with their state, the commands that do the same thing one step at a time, and
the AI line.

## Vocabulary

| Word | Means | Has an id? |
|---|---|---|
| **workload** | what the product is for — the kinds of work on the README | no |
| **recipe** | one guided path to a workload; what `new <id>` takes | yes |
| **template** | a recipe's internal parts — which files it generates | not user-facing |

One workload can have several recipes; one recipe can serve several workloads.

## Not in this path

- Publishing a view (`serve`: REST, MCP, writing back to your own database) — a later step.
- Joining tables into one (a `joined-table` recipe) — added to the catalog when `join` ships.
- Transforms that need code (`js`) — start from `blank` or edit the generated file.
- Managing the local development stack beyond starting it — the printed `docker compose`
  line is the whole interface.
- Air-gapped installation — the artifact source is configurable, so a bundle-based source is
  an addition, not a redesign; it is not part of this release.
