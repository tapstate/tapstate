---
status: engineering-draft
publication: handoff
target: https://tapstate.dev/docs/start-checks/adding-a-check
---

# Adding a start check

A start check is a class, a line in the registry, its codes with their catalog text, and its cases.
No client changes: the CLI, the web console and the MCP tools render a finding from what it says - its
behavior, its message, the answers it offers - and never from which check made it. A client that met a
behavior or an outcome it does not know refuses the start rather than guessing.

Everything below lives in `control/control-core` unless it says otherwise.

## 1. Write the check

Implement `StartCheck`:

```java
final class SourceRetentionCheck implements StartCheck {

    @Override
    public String id() {
        return "source-retention";        // lower-kebab, part of the contract: never renamed
    }

    @Override
    public List<StartFinding> evaluate(StartCheckContext context) {
        // read context.definition(), context.plan(), context.probe(...); write nothing
        // return one finding per subject, or none
    }
}
```

What a check may look at is what `StartCheckContext` offers, and nothing there writes:

- `definition()` and `contentHash()` - the pipeline as the start would run it.
- `plan()` - how the start would load each target: a new full load, a resume, or change capture only,
  with the `on_full_load` in force. It throws the coded error that kept it from being worked out; let it.
- `probe(entries)` - what each target holds, read through the data browser's bounded reads, all targets
  at once under a time limit. Only connectors the data browser can read answer it.
- `deadline()` - every check has to have answered by then.

**A check that cannot tell throws the coded error that says why.** The start turns that into a finding of
its own that never passes - a warning, unless `whenUnavailable()` says the check refuses instead - and a
check that runs past the deadline is treated the same way. **Anything uncoded is a defect** and fails the
start with it: a check is never a place to swallow a programming error.

## 2. Make findings

A `StartFinding` is about one subject, and its key - `<check id>/<subject id>` - is what an answer names.
Keep subject ids stable for as long as the definition is, because every start is evaluated afresh and
answers are matched to the findings of this evaluation by key.

| Behavior | Use it when | Actions |
|---|---|---|
| `PASS` | there is nothing to say | none |
| `WARN` | the start should go ahead, and somebody should know | none |
| `CONFIRM` | going ahead as configured is acceptable, but only if somebody says so | exactly one `StartAction.acknowledge(...)`, plus any others |
| `BLOCK` | the start must not go ahead as configured | none |

If going ahead as configured is never acceptable, the finding refuses rather than asks. That is why every
question offers exactly one way to go ahead as configured, which the constructor enforces: it is what
lets `-y`, `up --yes` and a script answer any question without knowing the check.

An answer that changes the definition is `StartAction.changeDefinition(...)`: it lists the fields it
changes, for the person to read before choosing it, and carries the change as a function over the
definition. The start applies every chosen change in memory, evaluates again, and writes the definition
only once nothing is left to ask - through the same validation and audit an edit takes. Offer such an
answer only for what the pipeline's own definition declares; an element that comes from a shared
definition is changed there, not by a start.

## 3. Name the text

A finding's and an action's text come from codes in the `start-check` domain, in `StartCheckCode`. These
codes are never thrown; they name the text a report renders, which scripts read as well as people. For
each new code:

- add the constant with every placeholder it renders from, and give each a value in the finding's
  `params` (numbers stay numbers);
- add its message to `messages/src/main/resources/messages/en.yml`;
- add it to `arch-tests/src/test/resources/error-codes.golden`.

Codes are append-only: renaming or removing one is a breaking change.

## 4. Register it

Add it to `StartChecks.registered()`, in the order a report should list it. The list is written out
rather than discovered, so adding a check is a line somebody writes and a reviewer reads.

## 5. Prove it

- A unit case per behavior the check can produce, and one for each way it can fail to tell, in the
  manner of `TargetNotEmptyCheckTest`.
- An end-to-end case under `e2e/` that drives a real start into the situation the check is about and
  asserts the start's answer - its status, its code, the finding - and what was not written.
- If the check adds a behavior, an outcome, an action kind or a subject kind a client has never seen, add
  it to the versioned client fixture in `control/rest-api/src/test/resources/start-checks/` and raise
  its version, so every client's fixture case shows it still renders and answers what it was given.
