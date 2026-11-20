---
status: engineering-draft
publication: handoff
target: https://tapstate.dev/docs/nest/structural-key-changes
---

# Nest structural key changes

A nest files every row by a key it reads off that row. When one of those keys is *edited*,
the row has not merely changed - it has changed **where it belongs**. This page is what
that costs, what you have to switch on for it to work at all, and what happens when you
leave it off.

Everything here is off by default. A tree that never edits its keys pays nothing for any
of it and needs nothing from its sources.

## The switch

```yaml
transform:
  nest:
    root:
      from: customer
      key: [customer_id]
      trackKeyChanges: true       # the root's own key may be edited
      embed:
        - from: policy
          on: { customer_id: customer_id }
          path: policies
          arrayKey: [policy_no]
          trackKeyChanges: true   # this embed's keys may be edited
```

The root declares its own because nothing above it does. Each embed declares its own
because the cost is per stream, and most streams do not need it.

## What it requires from the source

**Before images.** The switch is a promise that this stream's updates arrive carrying the
row as it was, not only as it now is. Without that, a row that moved and a row that had an
unrelated column edited look exactly the same - a row sitting where it now sits - and
following the first as if it were the second writes the row into its new place while
leaving it in the old one. The document then holds a copy the source does not have, and
nothing downstream can notice.

| source | what to set |
|---|---|
| MySQL / MariaDB | `binlog_row_image=FULL` (the default is `FULL`; `MINIMAL` will not do) |
| PostgreSQL | `REPLICA IDENTITY FULL` on the tables the tree reads |

With the switch off, none of this is needed: no comparison is made, so no source has to
send anything more.

**A source that cannot supply them stops the pipeline**, rather than letting it carry on: the
alternative is a document that silently disagrees with its source.

Which end refuses depends on the source, and the two read differently:

| what happens | when | what you see |
|---|---|---|
| the connector refuses to start | the server itself is configured without before images - `binlog_row_image=MINIMAL` on MySQL, for one | `TS0060024`, carrying the connector's own sentence naming the setting to change |
| the tree refuses the update | the source starts and streams, but an individual update arrives carrying no earlier row | `TS0490026`, naming the stream and the table |

On MySQL it is the first: the connector checks `binlog_row_image` as it starts and stops
there, so no update is produced for the tree to check. Set it to `FULL` (the default) and
the run proceeds normally.

> **Known limitation.** The check happens when the row arrives, not when you apply the
> pipeline. Refusing at apply time would need the connector catalog to say whether a source
> supplies before images, and it does not carry that - so a pipeline whose source is
> configured wrongly is accepted, starts, and stops on the first update of a tracked stream.
> The error names what to reconfigure. *(Provisional: whether the catalog should carry such
> a capability is a product decision that has been raised and not yet settled.)*

## What is followed

Four kinds of edit, all of them under the one switch:

| what was edited | what happens |
|---|---|
| an embed's **array key** - the value the document shows the element by | the element is moved to its new slot inside the same document, with everything under it |
| an embed's **join key** - the value pointing at its parent | the element leaves the document it was in and turns up in the document it now points at, with everything under it |
| the **root's key** | the whole document changes identity: the old key is removed downstream and everything it held turns up under the new one |
| **both at once** - where an embed's array key and its join key are the same column | the slot changes first, then the document; the order is fixed so that the element's identity and the move agree |

Rows beneath a moved element travel with it. They have to: the source edited one row and
considers the rest untouched, so nothing will ever send them again.

## What happens with the switch off

Nothing is detected, and the two edits differ in how they show:

- **Join key edited** - the element stays in the document it was in. The document and the
  source disagree about who the row belongs to, and the disagreement is visible: the row is
  there, under the wrong parent.
- **Array key edited** - the element may appear **twice**: once under the value it used to
  show and once under the new one. Nothing removes the first.

Both are deliberate. Following either one costs before images from the source, and a great
many trees never edit these columns at all.

## What is not followed even with the switch on

**A child that still points at the old key.** If your source has no `ON UPDATE CASCADE`,
editing a parent's key leaves the child rows naming a value that no longer exists. Those
rows arrive, find no parent, and are not shown anywhere.

This is not the nest failing to follow the move - it is the nest reporting, faithfully,
that the source's own rows are now orphaned. Nothing guesses that they meant to follow.

## Append-only roots cannot track key changes

`root.mode: append` and `trackKeyChanges` are refused together, when the pipeline is
checked, with `TS0490012`.

A move has to hold a document back until the rows land, and holding changes back is the one
thing an append-only root forbids: under append every change is a record of its own, so
folding several into one loses the ones in between. The two cannot both be honoured, so the
combination is refused rather than silently doing one of them.

## While a move is in flight

Rows crossing between two documents are, for a moment, in neither. They sit in a parking
area both sides can reach, and three things are true while they are there:

- **The durable position does not advance past the change that started the move.** If it
  did, a restart would resume above that change, nothing would replay the move, and the
  rows would be gone from both documents with the pipeline running and nothing reporting it.
- **The document gaining them is not sent until it has them.** A document with its tree
  missing is one the source never had, and a sink applying it has already written it.
- **The key being left is removed at once.** The row it named is gone from the source
  whatever else is in flight.

Two bounds apply, both per namespace and both settable:

| bound | default | what happens at it |
|---|---|---|
| how many changes one move may park | 50 000 | the pipeline stops with `TS0490023` |
| how long a move may sit uncollected | 10 minutes | the rows go to the dead-letter channel and the durable position is let go of |

Reaching the second one usually means the half of the move that collects the rows is not
being worked. Reaching the first means the structural change is bigger than this deployment
is set up for; it is worth finding out why before raising it, because those rows are in no
document while they sit there.

## Cost

A move is not free and does not need to be rare to be affordable, but it is worth knowing
what you are paying:

- **One extra delivery per tracked stream.** Every row of a tracked stream is delivered
  twice - once keyed by where it now is, once keyed by where it was - because the state
  being left is held somewhere the row's new key would never reach. Rows that did not move
  are discarded on arrival, so what this costs is delivery, not work.
- **Reading a subtree out and writing it back.** Paid per move, in proportion to how much
  hung beneath what moved.
- **A pause on the document gaining it**, for as long as the two halves take to meet.

None of it is paid by a tree with the switch off, and none of it is paid by a row whose
keys were not edited.
