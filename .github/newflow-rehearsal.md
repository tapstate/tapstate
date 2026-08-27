# Rehearsal artifact - safe to delete

This file exists only so that a workstream rehearsing this project's execution
workflow has something to commit. It ships no behaviour: nothing builds it,
nothing imports it, and no CI gate reads it. It was placed in `.github/` rather
than `docs/` on purpose - a page under `docs/` has to declare where it is headed
for publication, and this one is headed nowhere.

Added by the `newflow-rehearsal` workstream, tracked in #73, together with an
equivalent file in one other repository. The exercise was to run the workflow's
five steps - open the execution issue, claim it, post progress, open the pull
requests, close the line - against real GitHub rather than against a test double.

## Removing it

Revert the merge commit that introduced it:

    git revert -m 1 <merge commit>

That restores the tree byte for byte. Nothing else has to be undone.

## Second revision

The first session end left one progress comment on the execution issue:
comment id `5434257064`, at
<https://github.com/tapstate/tapstate/issues/73#issuecomment-5434257064>.

This paragraph exists so that a second session end happens at all. The property
under test is silent: the tooling is supposed to EDIT that one comment rather
than post another, and posting another raises no error, fails no gate, and looks
correct until someone counts. Recording the id here means the next run can be
checked against a number rather than against an impression.
