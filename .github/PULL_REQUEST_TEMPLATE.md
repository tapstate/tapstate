<!--
Write this in whichever language you think in. Nothing checks the language of a pull request
opened from a fork, and nothing will refuse it over that. Pull requests from a branch in this
repository keep to English, which is our own habit rather than a bar for you.
See CONTRIBUTING.md, "Language".
-->

## Linked issue

<!--
Which issue this belongs to, and where its design is written: `Refs #123` — not `Fixes`, which
would close the issue on merge and take with it whatever else is still in flight under it.

For an external contribution this is the only context a reviewer has, so say which part of the
issue holds the design. A change that genuinely has no issue behind it (documentation, build and
CI configuration, scripts, test-only) says "none" and why. See CONTRIBUTING.md, "External
contributions".
-->

## What changed

<!-- What this changes, and why. -->

## End-to-end case

<!--
Name the case that covers this change: the e2e/examples/<name>/ directory, or the *IT.java.
If it is Java, say which specification word was missing — that is how the vocabulary grows.

Documentation, build and CI configuration, scripts and test-only changes need no case; say so
here. If a product change genuinely cannot carry one, say why and ask a maintainer for the
`no-e2e` label.

See CONTRIBUTING.md, "End-to-end cases".
-->

## Live verification scenario

<!--
How to see this work by hand: what to start, what to do, what you should see. Concrete enough
that a maintainer can follow it without asking you anything.

CI proves the assertions still hold. It does not prove anyone has watched the thing run — and the
failures that reach users are mostly the ones no assertion was written for.
-->

## Documentation impact

<!--
Answer all three, even when the answer is "none" — an unanswered checkbox is not a
decision. If follow-up is needed, add the `docs-needed` label to this PR; on merge, an
issue is opened in tapstate/docs, assigned to the docs owner, with a link back here.
Do not open that issue yourself.

Pages under docs/ carry a classification header saying where they are going, and a CI
check enforces it. See CONTRIBUTING.md, "Documentation".
-->

- [ ] This change needs documentation follow-up (add the `docs-needed` label)
- **Draft in this repository:** <!-- path under docs/, or "none" -->
- **Public page it is headed for:** <!-- URL under https://tapstate.dev/docs, or "none" -->

## Checks

- [ ] `mvn verify` is green locally
- [ ] An end-to-end case covers this change, and is named above
- [ ] New or changed e2e assertions carry mutation evidence: the mutation applied,
      the red observed, and that only it went red (see CONTRIBUTING.md,
      "Mutation evidence")
