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
tapstate new                                   # which outcome? -> writes a workspace, offline
tapstate up                                    # which server? -> brings that workspace to RUNNING
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

**`new` never reaches a server.** It asks what the workspace is for, writes files, and stops;
which server those files are brought up against is `up`'s question, asked the first time you run
it. Nothing here probes, starts, signs in or binds.

Bare `new` at a terminal runs the guided flow. `new --kind <kind>` keeps its existing
meaning — scaffold one resource — and is unchanged. The bare form used to enter the
single-resource wizard; from this release it enters the recipe picker. That is a deliberate
change to a released command and is called out in the release note.

### Step 1 — which outcome

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
| `blank` | Skeleton files only - I will write it myself | one source and one pipeline, as skeletons with placeholder values | — |

Rules the catalog follows, so that the next recipe added behaves like these:

- **An id names what you end up with**, `<past-participle>-<noun>`, never how it is done.
  Internal type names (`nest`, `union`, `join`) are not ids.
- **Every listed recipe runs today.** A capability that is not shipped yet does not appear
  greyed out; it is added when it ships. `blank` is the deliberate exception — its skeletons
  carry placeholders that connect to nothing, so it is the one entry that is not `runnable`,
  and it sits last because it means "none of the above", not "start here".
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
    { "id": "blank",             "title": "Skeleton files only - I will write it myself",       "runnable": false, "uses": [] }
  ]
}
```

Keys are stable; new keys may be added, existing ones are not renamed. The text output is
generated from the same list and is held to a golden, so wording cannot drift between the two.

### Step 2 — the recipe's questions

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
| `blank` | nothing — the skeletons are written as they stand, for you to edit |

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
- a choice list's default is its first entry (for the recipe question, `sample`);
- `reshaped-table`: "columns to keep" is written as identity renames (`region: $region`) — a `map`
  step lets unlisted fields through, so keeping fixes the order but does not trim; "drop" is an
  explicit drop (`internal_note: false`); a rename wins over a keep of the same column. The step ids
  are `reshape` (map) and `keep` (filter); with nothing to reshape the recipe writes exactly what
  `mirrored-table` writes;
- `nested-json`: one `nest` step `assemble`; a child that shares the root's database is a second
  table on the **same** source, a child elsewhere gets its own `<child>_src`; each embed's
  `arrayKey` is assumed `[id]` (the summary line says so); the view id defaults to `<root>_state`.
  The flag form (`--child …`, `--child-connector`, `--child-set`) names at most one other database —
  mixed placement is interactive only;
- `consolidated-table`: sources are `<table>_1_src`, `<table>_2_src`, …; the `union` step
  `consolidate` addresses them as `<source_id>.<table>`, because a bare table name held by two
  sources is ambiguous to the reference closure; the view id defaults to `<table>_all`. The
  interactive form asks for two databases before offering "another?"; the flag form is `--db
  <connector>[,key=value…]`, repeated.

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
overwritten; `--force` is the only way to replace one, and the summary marks the files it
replaced.

### Non-interactive form

Every question has a flag; `--yes` means "never prompt". With the same flags the output is
byte-identical run to run — that is what the goldens assert and what the docs and tests rely
on. A required answer that is missing stops with a named error naming the flag; `--yes`
never guesses.

### What `new` says afterwards

The last thing printed, in this order:

1. **Where the workspace is, and what is in it** — every file, one line each, with what that
   file is for: `<kind> <id>: ` followed by the same one-line summary `ls` prints for it (so
   the two never disagree), and ` — assumed <what>; edit if the table is keyed otherwise` on a file
   the recipe had to assume something for. Under `--force` a line ends `(replaced)` or, for a
   dotfile that was extended, `(updated)` — a run that overwrote something says which files.
2. **State** — "not running yet".
3. **What you can do next**, as commands: open a file and edit it; `tapstate validate`;
   `tapstate ls` / `tapstate desc <id>`; `tapstate up`.
4. **One line saying an AI assistant can take it from here** — pointing at the published
   documentation, never at a specific integration.

A recipe the catalog marks `runnable: false` — today only `blank` — prints the same shape and
adds one line after its file list saying its files are skeletons to fill in. `-o json` and
`-o yaml` return the created files and their roles as structured fields and carry none of
the prose.

### What a skeleton must satisfy

**A skeleton validates as written.** `tapstate validate` on a freshly written `blank` workspace
passes: the placeholders are values of the right shape (`your_database`, `your_table`), not gaps,
and the pipeline's `view.from` names the table its source declares, so the reference closure
resolves. The password is the one field written as a `${...}` reference rather than a placeholder
value, because a template that ships a literal in that position teaches the wrong thing; `up`
resolves it from a `.env` beside the file, which the author writes. This is pinned rather than left to taste — the summary's own first next step is
`tapstate validate`, and the one recipe whose whole purpose is a clean starting point must not
open with a list of errors.

**A skeleton does not connect.** The placeholder host resolves to nothing, so `tapstate up` on an
unedited skeleton fails at the connection test with the ordinary actionable error. That is the
difference `runnable: false` records: valid to parse, not ready to run.

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
- **The stages are exactly** `preflight`, `apply sources`, `discover`, `apply workspace`, `start`,
  in that order; a run stops at the first one that fails.
- **`.env` is read first.** Before anything is submitted, a `${NAME}` reference in a workspace file
  is resolved from `<workspace>/.env` (the file `new` wrote the secrets to), and only then from the
  process environment. Nothing else reads that file.
- **A failure names its stage.** "`up: discover failed on orders_src: <code> — <message>`" followed
  by the catalog's next action, never the internal command that happened to be running.
- Flags: `--server <url>` has one meaning per state — on an unbound workspace it names the server
  to sign in to and bind to, and on a bound one it overrides the target for this run and leaves the
  binding alone (there is nothing to override before a binding exists); `-u <name>` and
  `--start-local` belong to the server question below; `--yes` never prompts.
- **This verb owns every contact with a server.** Probing one, starting the local development
  stack, signing in and binding the workspace all happen here and nowhere else: `new` and the
  scaffolding verbs write files and learn nothing. A workspace can therefore be authored with
  no server in existence, which is what makes offline authoring a real path rather than a claim.

### The server question

Asked once per workspace, on the first `up` in a directory that is not bound to a server yet,
and not at all when `--server` names one for the run. Two answers:

| Answer | Meaning |
|---|---|
| **(a)** `http://127.0.0.1:8080` — the default, taken on an empty reply | Use the server on this machine. If nothing is listening there, start a local development stack in Docker on that port, wait for it to be healthy, then register and bind it. |
| **(b)** a URL you type | Use a server you already run. You are asked to sign in. |

Either answer is saved as a registered server and bound to the workspace directory, so the
question is not asked again in that directory and `up` knows where to go.

The local development stack, when the default has to start one:

- is a **pinned** version of the server and its managed store, fetched and verified from a
  single configurable artifact source (the release location by default);
- comes with the **published connectors pre-registered** — today the MySQL, PostgreSQL and
  MongoDB engine connectors, fetched from the same release location the demo uses (override
  with `TAPSTATE_CONNECTORS_URL`); the other official ids have no published artifact yet and
  are registered by hand with `register` when they ship;
- contains **no sample data** — that is what the demo is for;
- is started **idempotently**: a second `up` finds the running one and does not start a
  second;
- lives in `~/.tapstate/local-stack/` (the compose file, a `.env` holding the generated
  admin password, the staged connector jars); the first start pulls images and says so;
- ends by printing where the stack lives on disk and the one command that stops it. There
  are no `stop` / `status` / `upgrade` verbs for it in this CLI, and there will not be.

**Both answers sign in.** The local stack's admin is created by the stack and signed in with
the generated password automatically. A server you point at asks `Username` (default `admin`)
and `Password`; non-interactively, pass `--user` and put the password in `TAPSTATE_PASSWORD`.
A remote server must be `https://` — plaintext is refused for anything but loopback. The
session is saved, so `up` needs no credential.

**Non-interactive runs never start containers silently.** With `--yes`, or anywhere there is no
terminal to ask at, `up` stops with a named error rather than starting anything, unless
`--start-local` was passed explicitly. Scripts either pass that flag, pass `--server <url>`, or
work in a directory that is already bound.

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
