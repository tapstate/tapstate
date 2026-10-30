---
status: engineering-draft
publication: handoff
target: https://tapstate.dev/docs/connectors/refreshing-the-catalog
---

# Refreshing the catalog

Every connector this release knows about is described by a row in a catalog that ships inside the
binary. Nothing reads a connector jar to answer "what fields does MySQL need" or "can this connector
do CDC" - the CLI reads the catalog, offline, and so does validation.

That catalog is **generated**, from a repository nobody here controls
([tapdata-connectors](https://github.com/tapdata/tapdata-connectors)). Two things there can move, and
they cost two orders of magnitude apart:

- a **specification** changes - a field renamed, a label reworded, a default adjusted. It is a JSON
  file. Reading it is free.
- a connector's **capabilities** change - which functions it registers when it is loaded. Nothing
  writes those down; the only way to find out is to build the connector and classload it. That is
  60-odd Maven modules and a large dependency graph.

So there are two lanes, on two schedules, and this page is about which one carries your change.

## The two lanes

```
                     upstream tapdata-connectors
                                 |
          +----------------------+----------------------+
          |                                             |
   specification files                    what a connector registers
   (JSON, ~1MB, free to read)             when it is classloaded
          |                                             |
  +-------v--------------+                  +-----------v-------------+
  | Catalog spec drift   |                  |    Catalog refresh      |
  | daily, 02:20 UTC     |                  | weekly, Tue 01:50 UTC   |
  | minutes; builds no   |                  | up to 3h; builds every  |
  | connector jar        |                  | connector and loads it  |
  +-------+--------------+                  +-----------+-------------+
          |                                             |
          | writes                                      | writes
          v                                             v
  index.json  specSha                        index.json  capabilitySha
  <id>.json   config, labels, defaults       <id>.json   modes, sink
                                             capability-bitmap.tsv
          |                                             |
          +----------------------+----------------------+
                                 |
                    one pull request, labelled `catalog`
```

The two lanes touch the same **files** but disjoint **fields**, which is why they can run on separate
schedules without fighting: the cheap lane never recomputes a mode, and the expensive lane never
invents a config field.

Each lane has its own workflow file, its own timeout and its own concurrency group. That is
deliberate: red in `Catalog spec drift` has exactly one meaning, *upstream really moved*, while red in
`Catalog refresh` is allowed to mean *a connector would not build this week*. A single lane failing
for both reasons tells you neither.

## What did you change, and what has to be rebuilt

| What moved | Which face | What has to run | Which lane carries it |
|---|---|---|---|
| A specification file: field name, label, default, options, `visibleWhen` | spec | `refresh-catalog.sh --spec-only` | daily `Catalog spec drift` |
| A `tapstate` block in an upstream specification (`tapstate.modes`) | spec | `refresh-catalog.sh --spec-only` | daily - but see the note below |
| What a connector registers in `registerCapabilities` | capability | `refresh-catalog.sh` (full) | weekly `Catalog refresh`, or a manual `workflow_dispatch` |
| A connector added, removed or renamed upstream | both | `refresh-catalog.sh` (full) | weekly `Catalog refresh` |
| This repository's own overlay declaration | neither, it is ours | `refresh-catalog.sh --spec-only`, to bake it into the shipped rows | no lane - it arrives in your own pull request. See [Declaring modes](declaring-modes.md) |
| Connector implementation only, no specification and no change to what it registers | neither | nothing | neither lane opens anything. The next full rebuild moves `capabilitySha` and no entry |

**The note.** An upstream `tapstate.modes` declaration is carried by the cheap lane like any other
specification content, but it does not always win: where this repository declares modes for the same
connector, ours outranks it, and the difference is reported rather than silently resolved. See
[Declaring modes](declaring-modes.md).

A spec-only refresh updates only connectors already in the bundled catalog. New upstream ids remain
in the drift report until a full refresh supplies their capability face; they are not added to the
catalog with empty modes. Existing catalog entries without bitmap records remain eligible for
specification updates.

## Running a refresh yourself

One script, both depths. It needs a connectors checkout - there is no way to regenerate without one,
including for an overlay-only change:

```sh
git clone --depth 1 https://github.com/tapdata/tapdata-connectors.git ../tapdata-connectors

# Spec face only. Reuses the checked-in capability bitmap, builds no jar, takes seconds.
scripts/refresh-catalog.sh --connectors ../tapdata-connectors --spec-only

# Everything. Builds every connector and classloads it. Budget hours, not minutes.
scripts/refresh-catalog.sh --connectors ../tapdata-connectors
```

It writes into your working tree - `core/core-catalog/src/main/resources/catalog/`,
`tools/catalog-assembler/ingest-report.md` and `tools/catalog-assembler/capability-bitmap.tsv` - and
prints the diff. Review it before committing.

`--dist <dir>` reuses jars you have already built. `--sha <sha>` stamps a revision when the checkout
is not a git working tree. `--help` lists the rest.

**Why a script rather than three Maven commands.** Each step of a refresh is a JUnit test that
*skips* when its inputs are absent - an absent checkout, an absent bitmap, a property name spelled
wrong. A skipped test is not a failure: Maven exits 0. Driven by hand, a refresh can regenerate
nothing, print no error and succeed. The script reads each step's own surefire report and refuses
unless the test actually executed.

### Adding a connector

1. Put the connector in your connectors checkout and make sure it builds there.
2. Run a **full** refresh - a new connector has no row in the checked-in bitmap, so `--spec-only` has
   no capability face to merge for it.
3. Check the row landed: `core/core-catalog/src/main/resources/catalog/<id>.json` exists and the id is
   in `index.json`. There is no offline verb that lists catalog connectors - `tapstate connectors` is
   the server's view of what it has been given, which is a different question.
4. Read `tools/catalog-assembler/ingest-report.md`, which is where a row that landed *empty* says so.
   Three buckets mean three different failures, and all three produce a row that looks ordinary in a
   diff:

   | Bucket | What happened |
   |---|---|
   | *Not built* | This repository could not build the connector at all, named with the reason |
   | *Not derived* | No built jar, or it did not classload - so nothing was derived from it |
   | *Unclassified* | It built and loaded, but registered nothing that resolves to a mode |

5. `tapstate validate` will accept the new id straight away.

**It will not be offered by `tapstate new` yet, and that is not a bug.** The authoring surfaces offer
only what the runtime register path would accept, and that set - `OfficialConnectors.IDS` - is a
support decision somebody makes by name, not something a refresh can infer. Adding to it is a
separate, reviewed commit.

### Changing a specification

1. Change the field in your connectors checkout.
2. `scripts/refresh-catalog.sh --connectors ../tapdata-connectors --spec-only`.
3. The diff should be small: the entry files that really changed, and `index.json` only when the
   upstream revision moved. If every entry moved, something else moved with your change - read it
   before committing.

`specSha` names the upstream revision the specifications were read at, so editing a checkout in place
leaves it alone: the revision has not moved, only your working tree has. That is worth knowing before
you commit such a refresh, because the catalog then claims a revision that does not contain what it
carries. Each entry's own `specContentHash` does move, so the two disagree and the claim is at least
detectable. A refresh from a committed revision - which is what the scheduled lanes always run - does
not have this problem.

## Reading the diff

A rebuild regenerates every row, but unchanged rows regenerate byte for byte, so the diff shows only
what really moved. Both shas live in the catalog **header**, one line each, rather than being stamped
onto all 78 rows - which is why a one-connector change is a two-file diff and not a whole-catalog one.

**The two shas are allowed to disagree.** `specSha` names the upstream revision the specifications
came from; `capabilitySha` names the revision the capabilities were derived at. The cheap lane moves
only the first. A catalog whose `capabilitySha` is weeks behind its `specSha` is reporting exactly
what happened, not a defect.

## Reviewing a pull request a lane opened

Both lanes open one pull request, labelled `catalog`, from a GitHub App rather than the default
token - so the required checks on it actually run.

**From the daily spec-drift lane**, read the entry diff: these are field and label changes, and they
are what users will see in `tapstate desc` and in the `new` wizard. Its body counts two things worth
a second look: rows whose specification is no longer at the path they record (a connector moved or
was deleted upstream) and connectors upstream carries that this catalog has no row for.

**From the weekly full rebuild**, read the **ingest report** diff first, not the entry diff.
Connectors that newly derive no capability at all, and connectors whose modes are derived with
nothing declared behind them, are how a connector that stopped building looks - and in an entry diff
that is indistinguishable from an ordinary edit.

You do not have to check by eye that a regeneration kept this repository's own declarations: the
ordinary build reconciles the shipped rows against the overlay on every pull request, and a
regeneration that dropped them is already red.

**Not every drift opens a pull request immediately.** A drift touching a connector this release
supports opens one at once; everything else accumulates and is swept up by the next pull request,
with a seven-day ceiling so an unsupported connector's specification cannot wait indefinitely. A scan
that has not opened anything for a few days is not evidence that it is broken.

**Two labels change what a lane does on a pull request.** Both lanes run on pull requests as well as
on their own schedules, because a lane that runs only on a schedule cannot run at all until it is on
the default branch - the wrong way round for the change that introduces one. On a pull request,
`catalog-full-rebuild` asks for the full rebuild, which is far too expensive to attach to every pull
request that happens to touch it. `catalog-drift-pr` lets the spec-drift lane open its catalog pull
request from that run, and moves the seven-day ceiling aside so that it can - a branch that has just
edited the catalog itself reads as zero days old, so the ceiling would otherwise hold every time.
Without that label a pull request run stops one step short of opening anything, deliberately: the
catalog it would carry was rebuilt from code that is still under review.
