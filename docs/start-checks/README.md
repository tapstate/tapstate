---
status: engineering-draft
publication: handoff
target: https://tapstate.dev/docs/start-checks
---

# Start checks

A start looks before it leaps. When a pipeline is started - by `tapstate start`, `restart`, `up`, the
web console, the MCP `pipeline_start` tool, or `POST /api/pipelines/{id}:start` - the server first runs
its **start checks**: read-only judgements about the start it is about to make, evaluated before any
desired state is written. Each one reports **findings**, and each finding does one of four things to the
start:

| Behavior | What happens |
|---|---|
| `PASS` | Nothing to say; the start goes ahead. |
| `WARN` | The start goes ahead, and the finding is reported alongside it. |
| `CONFIRM` | The start stops and asks. It goes ahead once the question is answered with one of the answers the finding offers. |
| `BLOCK` | The start is refused, whatever is answered. Something has to change first. |

A check that cannot tell - a target it cannot read, or a check that did not answer in time - reports a
finding that never passes: by default a warning, marked as not evaluated.

## The checks

| Check | Asks when | Answers |
|---|---|---|
| `target-not-empty` | A new full load is about to go into a target that already holds rows and will not clear them first (`on_full_load: append`). A new full load cannot see rows the source deleted in the meantime, so they would stay. A target set to `fail` refuses the start instead. | `clear` - clear the target first, and record `on_full_load: clear` on the element that writes it. `keep` - keep the rows. |

[Existing rows in the target](../quickstart-online.md#existing-rows-in-the-target) is the walkthrough.

## Answering

Every question offers exactly one answer that goes ahead as configured, so a client can answer "as it is"
without knowing which check asked. Answers are matched to questions by the finding's key,
`<check>/<subject>`, and every start is evaluated afresh: an answer to a question that is no longer asked
is reported back as unused rather than applied.

- **CLI.** At a terminal the question is asked, and Enter cancels. Otherwise answer on the command line:
  `--decide <check>=<answer>` answers every question a check asks, `--decide <check>/<subject>=<answer>`
  answers one, and `-y` answers the rest by going ahead as configured. `--checks-only` shows what a start
  would ask and starts nothing; it exits 0 only when nothing would be asked or refused.
- **REST.** A start that is stopped answers `409` with `code`, `params` and `message` - the error every
  client already reads - plus the whole report under `startChecks`. Send the answers back as
  `{"decisions": [{"finding": "<key>", "action": "<answer>"}]}` with the report's `contentHash` in
  `If-Match`; a start carrying answers without it is refused with `428`, and one whose definition changed
  since with `412`. `GET /api/pipelines/{id}/start-checks?intent=start|rerun` reads the report without
  starting anything.
- **MCP.** `pipeline_start` returns the report in its error result; `pipeline_start` with `decisions` and
  `expectedContentHash` answers it, and `pipeline_start_checks` reads it. A model relays the questions to
  the person it acts for and sends back their answers.

An answer that changes the definition is applied the way an edit is: validated, audited, and written
together with the pipeline's web draft, and only while the stored definition is still the one the checks
read. Every start's audit record carries what the checks found and what was answered.

## What a start check never does

It never writes. A check reads the definition, how the start would load each target, and what a target
holds; an answer that changes the definition is handed back for the start to apply after every question
is answered. And it never runs on its own: a pipeline the product resumes by itself - after a failure, or
a server restarted onto the store it left - carries on from the position it recorded, and is neither
asked nor cleared.

[Adding a start check](adding-a-check.md) is for contributors.
