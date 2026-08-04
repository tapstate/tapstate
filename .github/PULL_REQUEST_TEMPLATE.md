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

## Documentation impact

<!--
Does this change need follow-up in tapstate/docs? If yes, add the `docs-needed`
label to this PR. On merge, an issue is opened in tapstate/docs, assigned to the
docs owner, with a link back to this PR.
-->

- [ ] This change needs documentation follow-up (add the `docs-needed` label)

## Checks

- [ ] `mvn verify` is green locally
- [ ] An end-to-end case covers this change, and is named above
- [ ] New or changed e2e assertions carry mutation evidence: the mutation applied,
      the red observed, and that only it went red (see CONTRIBUTING.md,
      "Mutation evidence")
