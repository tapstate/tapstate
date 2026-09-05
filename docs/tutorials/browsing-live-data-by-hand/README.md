# Browsing live data by hand

Tapstate can show you the data it is keeping current, without a `mongosh` and without you knowing
where any of it is stored: five verbs in the CLI session, and three tools an agent can call over MCP.
This page runs all of them on a real terminal, against a real database, and says what to look at.

Four moments on this page are marked **look at this**. They are the ones a person has to be present
for: a test can assert that `watch` sent the right escape sequences, but only you can see whether the
screen redraws in place or scrolls; a test can close a socket, but only you can close a terminal
window. Run the page for what it teaches, and stop at those four.

Time: about 30 minutes. You need the stack from the [online quickstart](../../quickstart-online.md)
running - or the server and CLI from [an IDE](../running-from-an-ide/) - plus one MCP-capable client.

**Which build you are running matters, and it is easy to get wrong.** The quickstart stands up the
last *published* image, which is the right thing when you are using Tapstate. If you are checking a
change - your own, or a branch on its way to a release - that stack will not contain it, and every
observation you make will be about the released build while your notes say otherwise. Stand it up
from the source you mean to check instead: `docker compose -f docker-compose.yml -f
docker-compose.dev.yml up -d` from `deploy/quickstart` builds the server image from the checkout, or
run it [from an IDE](../running-from-an-ide/).

## 1. What you are going to run

| Verb | Form | What it answers |
|---|---|---|
| `show collections` | `show collections [<source>]` | What this source actually holds, asked of the source itself |
| `find` | `<source>.<collection>.find(...)` | A bounded preview of rows |
| `stats` | `<source>.<collection>.stats()` | How big the collection is, and what it is indexed by |
| `watch` | `watch <source>.<collection> [filter]` | One row, redrawn in place as it changes |
| `tail` | `tail <source>.<collection> [filter]` | Every change to the whole collection, as a stream |
| MCP | `data_browser_collections` / `_find` / `_stats` | The same three one-shot reads, for an agent |

`watch` and `tail` have no options - no `--stream`, no `--poll`, no `--limit`. Which one you want is
the whole choice: `watch` holds one row on the screen, `tail` prints changes as they arrive.

## 2. Get a source with rows in it

Any source you have declared will do. If you followed the online quickstart you already have one; if
not, the shortest path is a MongoDB you can write to:

```console
tapstate(offline:work)> connect http://127.0.0.1:8080
tapstate(127.0.0.1:8080)> login admin
tapstate(admin@127.0.0.1:8080)> register ../mongodb-connector.jar
tapstate(admin@127.0.0.1:8080)> apply source/shop.tap.yml
```

Put at least 25 rows in it, from outside Tapstate, so that a preview shows fewer rows than the
collection holds. One column should hold values that sort usefully - a status, a name, a number.

**Look at this (1 of 4): after this point, nothing asks you for a database address again.** The URI
appears once, in the source declaration, and never in a read. Not in `show collections`, not in
`find`, not in the MCP tools, not in an error message telling you to supply one. If any step below
asks you where the data lives, that is worth reporting - the read path is supposed to resolve that
from the source you already declared.

## 3. The three one-shot reads

```console
tapstate(admin@127.0.0.1:8080)> show collections shop
tapstate(admin@127.0.0.1:8080)> shop.orders.stats()
tapstate(admin@127.0.0.1:8080)> shop.orders.find()
```

`show collections` lists what the source really holds, not what you declared - a collection somebody
created by hand shows up here too. That is intended: this face reads the database, not the workspace.

The line under a `find` is the part to read:

```
showing 10 of ~25 · natural order — not stable, and not the newest · more rows remain
```

Three claims, each of which is false if left unsaid:

- **`of ~25`** - how many there are, approximately, and only for a read with no filter. Counting a
  filtered collection is a full scan, so the server declines rather than paying for it. Filter the
  read and the count disappears rather than becoming `0`.
- **`natural order`** - you did not ask for an order, so this is the database's, which is *not*
  stable between two identical reads and is not the newest first. Ask for one and the caveat is
  replaced by `ordered by \`status\` desc`.
- **`more rows remain`** - these are not all of them.

A read is one-shot. There is no next page and no continuation token, by design: `limit` is how you
ask for more, up to a cap of 200 - and the default, when you ask for nothing, is 10.

```console
tapstate(admin@127.0.0.1:8080)> shop.orders.find({status: 'paid'}).limit(25)
tapstate(admin@127.0.0.1:8080)> shop.orders.find().sort({field: 'status', dir: 'desc'})
tapstate(admin@127.0.0.1:8080)> shop.orders.find().limit(500)
```

The last one is refused with a code rather than served or quietly clamped, because a clamped page is
one you cannot tell apart from the page you asked for.

### Columns whose name contains a dot

A dot steps into a nested document, so `price.usd` means "the `usd` field inside `price`". A column
whose own name is literally `price.usd` is addressed by escaping the dot - `price\.usd` in the shell,
`"price\\.usd"` in JSON, where a lone backslash is not an escape the format allows.

Two costs come with such a column, and both are stated where you meet it. Sorting by it is refused
outright: an index is written the same way a path is, so a name holding a dot cannot have one, and
`find().sort()` has no spelling for it. Filtering on it works and reads every row, for the same
reason - there is no index for it to match against.

## 4. `watch`, on a real terminal

```console
tapstate(admin@127.0.0.1:8080)> watch shop.orders
```

`watch` holds one row on the screen - the first in natural order - and redraws it as it changes. The
footer says so, including that the row it is holding may be replaced by a different one, because
"first in natural order" is not a promise about which row that is.

It shows two versions of that row side by side:

```
+- watching shop.orders - one row - polling every 1s -----------------+
| field      | current                    | previous                   |
|------------|----------------------------|----------------------------|
|   order_no | "SO-1001"                  | "SO-1001"                  |
| ~ status   | "paid"                     | "pending"                  |
| ~ amount   | 696                        | 27                         |
+------------+----------------------------+----------------------------+
```

`current` is what the row holds; `previous` is what it held one version ago. A change enters on the
left and pushes the version it replaced across to the right, so `previous` is always exactly one step
behind - never the version the view opened on, which would drift further from useful the longer you
watched. On the first frame `previous` is empty, because there is nothing yet for a change to have
replaced.

A value too long for its column is cut in the middle rather than at the end, because what tells two
long values of the same kind apart is usually where they stop being alike - at the end. Widen the
window and more of it shows; the table re-measures on every redraw.

Every field appears in both columns, whether or not it moved; the mark beside a field name says which
ones did. A view that listed only what changed would make you work out which of two shapes you were
looking at before you could read either.

Now change that row from outside Tapstate and watch the screen follow.

**Look at this (2 of 4): three things only a person at a terminal can see.**

- **It redraws in place.** The screen updates where it is, rather than printing a fresh copy under
  the old one. If it scrolls, that is the failure - and it is invisible to any assertion about what
  was written to the stream, because both look identical in a captured buffer.
- **The row stays up while nothing is happening.** Leave it alone for a minute. The `checked` time
  keeps moving and the table stays where it is; a screen that empties out to a single line after a
  few quiet seconds is the failure this replaced.
- **`Ctrl-C` leaves a clean screen.** No half-drawn row, no lost cursor, no shell prompt printed over
  the top of the view.
- **Resizing the window mid-run does not garble it.** Drag the window narrower while it is running.
  Narrow enough and the `previous` column is dropped rather than wrapped - a table that wraps is harder to
  read than a table with one column less.

Run `watch` with its output redirected and it refuses instead of running: a view that redraws one row
in place has nothing to say to a file. The refusal names both alternatives rather than just stopping
you - `tail` for a stream you can pipe, `find` for a one-shot look.

## 5. `tail`, and closing the window

```console
tapstate(admin@127.0.0.1:8080)> tail shop.orders
```

Where `watch` holds one row, `tail` prints every change to the collection as it arrives - inserts,
updates and deletes alike. Make several changes from outside and confirm all of them appear.

Compare a row here against the same row in section 3: every value reads the same on both faces. An
`_id` is the hexadecimal string the database stores it as whether you read it or follow it, so a row
you found by reading is the row you recognise in the stream.

It used to differ. A read reported an `_id` as a document of `date` and `timestamp`, which kept only
the second the id was created in - so two rows written in the same second had ids a read could not
tell apart, while the same two rows were plainly distinct in a change. Nothing was ever lost on the
wire; the two faces spelled the same value differently, and only one of them said the whole of it.

**Look at this (3 of 4): close the terminal window - do not press `Ctrl-C`.** A `tail` holds a
connector instance open for as long as it is streaming. `Ctrl-C` is the polite exit and is well
covered; the case worth checking by hand is the impolite one, because that is what users actually do.

The server has no read-out of how many connector instances it is holding, so check it from the
database side. Before opening the `tail`, and again a few seconds after closing the window:

```sh
mongosh --quiet "mongodb://127.0.0.1:27017/?directConnection=true" --eval 'db.serverStatus().connections.current'
```

The count should go up while the `tail` is open and come back down after the window is gone. If it
stays up, the session leaked - the symptom of which, much later, is reads refused because every
instance is busy.

A follow that shows nothing for ten minutes is ended by the server, which says so rather than going
quiet:

```
error: data-browser.follow-idle
  Following stopped after 10 minutes with no changes; the connector instance it held was given back.
```

Ten minutes of *changes*, not of the connection being alive: somebody who walked away leaves a
connection that answers perfectly well, and that is the case the limit exists for. A collection that
is genuinely quiet ends the same way, which is the trade - the instance is worth more to the next
reader than to a screen nobody is watching. Follow it again to keep watching.

## 6. The same data, from an agent

Wire the sidecar into an MCP client exactly as the
[quickstart describes](../../quickstart-online.md#ai-driven-alternative-run-the-pipeline-through-mcp),
with a read-scoped token - no `--allow-write` is needed for anything on this page:

```console
tapstate(admin@127.0.0.1:8080)> token create --scope read
```

Then ask the agent to list the collections of your source and read one of them. It should reach for
`data_browser_collections` and then `data_browser_find`, and the rows it comes back with should be
the ones you saw in section 3.

The listing carries what your workspace said about each collection and nothing where it said nothing:
a collection a view declares comes back with its `kind` and `description`; one that nobody declared
comes back without those keys at all, rather than with empty ones. Absent and empty are different
answers, and the agent is entitled to tell them apart.

**Look at this (4 of 4): discovery without a restart.** Leave the agent session open. In another
terminal, `apply` a new source or create a new collection. Now ask the agent again - without
restarting the MCP client, and without it re-listing its tools. The new collection should be there.
The three read tools are a fixed set - there is no tool per collection, so there is nothing to
re-list; what they answer follows the registry instead. An agent that had to be restarted to see a
new collection would be an agent nobody would leave running.

## What this page does not check

- **It does not prove the read path is safe against a hostile caller.** Reads are confined to the
  connection the source resolved to, and that is covered by automated specifications, not by looking.
- **`stats` reports what the connector reports.** A size it declines to answer comes back as
  unreported rather than as zero, which is the honest form, but neither number is recomputed here.
- **Nothing here measures performance.** The one cost this page names - a filter on a dotted column
  reading every row - is a property of the column, not a measurement of your database.

## Writing down what you saw

Worth doing whether you are filing a bug or signing off a release check, and for the same reason: the
useful record is what the screen did, not that you reached the end. Sections 4 and 5 are about the
terminal itself, so where you ran them is part of the observation - a redraw that is clean in one
terminal emulator is not evidence about another, and macOS and Linux are worth doing separately.

| | What to write down |
|---|---|
| Where | Machine, OS, and which terminal emulator |
| Against what | MongoDB version, and which connector jar |
| Client | Which MCP client, and its version |
| The four | For each **look at this**, what you actually observed - not "passed" |
