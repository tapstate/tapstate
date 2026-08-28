# Contributing to Tapstate

Thanks for your interest in Tapstate! Contributions are welcome.

## Language

English is the working language here - issues, pull requests, and anything else written in the
open. That is not a rule about you. It is so the next person who hits the same error and searches
for it in English finds that it has already been reported.

**Write in whichever language you think in.** Open an issue in Chinese, Japanese, Korean, German,
anything at all - we take it exactly as it is. A bot adds an English translation as a comment
underneath, which says on its face that it is machine-generated and that your original is the
version that counts. We do not edit what you wrote. And machine-translated English is just as
welcome: if you ran your own text through a translator before posting, that is a normal way to
contribute here, not a lesser one.

No check will refuse your issue or your pull request over the language you wrote it in.
Enforcement reaches repository content and nothing else - files under version control, and the
commit messages that carry them - because those enter the project's history permanently and
cannot be corrected afterwards without rewriting it. Conversation is deliberately left out. (We also hold ourselves to English in the description of a pull request
opened from a branch in this repository - our own habit, checked because we are the ones who slip,
and not a bar applied to you.)

## Workflow

1. **Fork** the repository and clone your fork.
2. **Branch** off `main` for your change: `git checkout -b my-change`.
3. **Build and test** locally before opening a PR:
   ```sh
   mvn verify
   ```
   This compiles every module and runs the unit tests, enforcer rules and the
   architecture (ArchUnit) checks. If you change the CLI and want to exercise the
   native binary, also build it with `mvn -Pnative -pl cli -am -DskipTests package`
   (requires GraalVM for JDK 21).
4. **Open a pull request** against `main`. Describe what changed and why. CI runs
   the build and a few repository checks on every PR — make sure it's green.

## External contributions

You need access to nothing private to contribute here. There are two ways in, and they differ in
one thing: whether you intend to do the work yourself.

| You want to | Use | What happens |
| --- | --- | --- |
| Tell us something is wrong or missing | the **bug or idea** template | We triage it and reply on the issue either way. If we take it, we open a separate issue for the work and link it — and yours stays open until the fix merges, because your reproduction is the evidence and we do not overwrite it. |
| Build it yourself | the **proposal** template | Nothing gates you before you start. Write the design in the issue, and open the pull request when it is ready. |

The second lane has no approval step on purpose. Making the one person willing to do the work wait
for a verdict is how that person stops coming back. The gates are all on the pull request: CI,
review, and a live verification a maintainer can rerun.

### What triage promises

Triage is done by this repository's code owners - today **@ply0011** and **@feynmx**.
[`.github/CODEOWNERS`](.github/CODEOWNERS) is the copy of that list which is actually maintained;
if it disagrees with this paragraph, believe it.

- **Within 7 days of you opening it, one of them replies on the issue.** Not a label and not a
  reaction - a reply you can read. If it has been longer, say so on the issue. A late triage is
  our failure, and a nudge is the correct response to it rather than a rude one.
- **If we are not going to do it, we say why, in public, and close it.** "Not now" and "not ever"
  are different answers, and you get whichever one is true.
- **If we are going to do it, we open a separate issue for the work, link it from yours, and leave
  yours open until the fix merges.** What you wrote is the evidence - the steps you took, the
  output you saw. Rewriting that into a scope statement would throw away the part that made it
  useful, so we start a new issue instead of editing yours.

**If you want to do the work yourself on something you reported, say so on the issue and we will
assign it to you.** You cannot do that yourself, and the reason is worth stating rather than
leaving you hunting for a menu that is not there: GitHub will only assign an issue to a member of
the organisation or a collaborator on the repository, so the click exists for us and not for you.
That makes the waiting entirely our doing, and we hold ourselves to **3 days** on it.

### The Triage role

Once a second pull request of yours has merged, we offer you GitHub's **Triage** role here. It
carries no write access - you still cannot push and cannot merge. What it does carry is the
ability to be assigned issues, which is otherwise closed to you for the reason just above, and to
label, close and reopen them.

This is not a badge. Someone who has already fixed two things in this codebase can usually read
the third bug report faster than we can, and any report that waits on one of two people is a
report that waits. Declining changes nothing else about how your work is reviewed, and nothing
about it happens without your say-so.

### Does it need an issue first?

**A change to product behavior does** — not as a gate, since no check enforces it, but because the
issue is where the design gets read. Our planning documents are not public, so a pull request that
arrives with no issue arrives with no context, and the reviewer's first question is the one you
could have answered in a paragraph.

**These do not**: documentation, build and CI configuration, scripts, and test-only changes — the
same list that passes the end-to-end admission gate without a case. Send those as a pull request
directly.

### Linking a pull request to its issue

Write **`Refs #123`**, not `Fixes #123`.

`Fixes` closes the issue the moment the pull request merges, and one issue is regularly covered by
more than one pull request, sometimes by more than one person. The first merge would then close work
still in flight and take everyone else's remaining scope with it. Closing is a decision someone
makes once the work is actually done.

### Sign your commits (DCO)

**Every commit in an external pull request carries a `Signed-off-by:` line.** A check verifies it,
and it is required to merge.

Signing off is a statement about *origin*, not authorship: you certify that you wrote the patch, or
otherwise have the right to submit it under this project's license. The full text is the
[Developer Certificate of Origin](https://developercertificate.org/) — one paragraph, worth the
minute it takes to read. It is not a copyright assignment and it does not ask you to sign anything.

Git writes the line for you:

```sh
git commit -s -m "your message"
```

Forgot it? Nothing is lost. Rewrite and force-push to your own fork — harmless, because it is your
branch:

```sh
git commit --amend -s               # the last commit only
git rebase --signoff <base>         # every commit on your branch, e.g. --signoff main
git push --force-with-lease
```

The name and email come from your `user.name` and `user.email`, and should be an identity you can
be reached at — that is what the certificate is for.

### Two constraints nothing will catch for you

Most conventions here are enforced by something that turns red. These two are not, and both cost a
round of review when they are missed:

- **Everything lives under `io.tapstate`** — package names and the Maven `groupId` alike. A new
  module inherits that root rather than inventing its own.
- **A user-facing error carries a code; a bug crashes.** An error a user can act on — bad
  configuration, a rejected specification, a connector that will not start — is raised through the
  error-code system: a typed exception, an enum constant for the code, named parameters, and
  catalog text (English mandatory). Never a hand-written code string, and never a bare
  `RuntimeException` as something a user is meant to read. A programmer error is the opposite — a
  null that should not be null, an invariant that does not hold — so throw it bare and let it crash
  with a stack trace. Laundering one of those into an error code hides a defect behind a message
  that reads like a decision.

### Code of Conduct

Participation is governed by our [Code of Conduct](CODE_OF_CONDUCT.md). A security vulnerability has
its own private channel — see [SECURITY.md](SECURITY.md) — and never a public issue.

## End-to-end cases

**A change to product source is admitted only with an end-to-end case alongside it.**
A CI check enforces this on every pull request. It is not a coverage target: one
smoke-level case is the floor — a case that fails if your change is reverted.

**Write it declaratively first.** A case is a directory under `e2e/examples/`:

```
e2e/examples/<what-the-run-proves>/
  spec.e2e.yml       # the case itself: setup, seed, steps, assertions
  pipeline.tap.yml   # and the resources it applies
  ...
```

`spec.e2e.yml` is validated against `e2e/spec/e2e-spec.schema.json`, and every word
a specification may use is listed in `e2e/spec/matchers.json`. Prefer this form: it
runs the shipped product on both fidelity tiers and needs no Java.

**Fall back to Java when that vocabulary does not reach.** The list of words is
short, and a specification can only assert what something real answers — so you
will sometimes find no word for the behavior you changed. Then write
`e2e/src/test/java/<Something>IT.java` instead, and say in the pull request which
word was missing. That is how the vocabulary grows; a case you could not express
is a gap in the executor, not a reason to skip the case.

Out of scope, and passing without a case: documentation, build and CI
configuration, scripts, and test-only changes — including the shared scaffolding
in `test-support/`, which is test code all the way down. If a product change genuinely
cannot carry a case, a maintainer applies the `no-e2e` label and records why in
the review — it is a reviewed exception, not something you assert about your own
change. The check verifies a case is *present*; whether it is *adequate* is the
reviewer's call.

### Mutation evidence

**An assertion is only as good as the red it has been seen to produce.** A case
that has never failed proves it can run, not that it can catch anything — this
repository has shipped more than one green that was checked by nothing. So when
you introduce or change an end-to-end assertion, record three things in the
commit or pull request that carries it:

1. **The mutation you applied** — which product code you changed to make the
   assertion's claim false.
2. **The red you observed** — which assertion failed, and what it reported.
3. **That only it went red** — the failure did not take unrelated cases with it.

The evidence rides with the code; there is no separate ledger to keep in sync.
It expires: if the product path an assertion verifies changes, or the assertion
itself does, the recorded evidence no longer describes anything and the
assertion needs new evidence before the next release review. A release review
confirms, case by case for the witness manifest, that the evidence on record has
not expired; evidence that cannot be confirmed counts as absent.

### The witness manifest

`e2e/witness-manifest.txt` names every scenario a release must have seen execute
and pass. It is held to the published examples in both directions and to the
run's witness ledger by `.github/scripts/witness-gate.sh` — a scenario that was
skipped, aborted, or never discovered fails the gate by its absence. Editing the
manifest is editing the release contract: removing a line must say why in the
commit.

## Documentation

**Two places hold documentation, and they answer to different people.** This
repository holds what is written alongside the implementation: engineering drafts,
executable samples, and anything version-coupled. `https://tapstate.dev/docs` holds
the user documentation a documentation engineer has reviewed and published. A page
here is not user documentation yet, however finished it reads.

**So every reader-facing Markdown page under `docs/` says which it is, in a header
at the very top of the file.** One of exactly two shapes:

```
---
status: engineering-draft
publication: handoff
target: https://tapstate.dev/docs/<where-this-is-headed>
---
```

The page was written next to the code and has not been reviewed for publication.
Name where it is headed even if that page does not exist yet — the target is what
turns a draft into a handoff rather than a file somebody may one day notice.

```
---
status: canonical-pointer
canonical_url: https://tapstate.dev/docs/<the-published-page>
---
```

The published page is canonical. What stays here shrinks to a short pointer to it,
plus anything executable — sample data, workspace files, scripts — which belongs
with the code and moves nowhere.

A page carries one shape or the other, never fields from both: a page that names a
target *and* a canonical URL has not said which side of the handoff it is on.

**What is not a page is out of scope.** Sample data, workspace files, scripts,
diagrams and fixtures under `docs/` need no header. They are read by running them.

**A page nobody touches needs nothing.** The rule applies to pages a pull request
adds or modifies, so it arrives page by page rather than as one migration — and if
you are editing a page, you have just read it and are the cheapest person to say
where it is going.

`.github/scripts/docs-classification.sh` enforces this on every pull request, and
is held to its own cases.

### Handing a draft over

The chain is: **you write the draft, a documentation engineer reviews and publishes
it.** You are never asked to author directly in the documentation repository.

1. Write or update the draft here, classified as above.
2. Answer **Documentation impact** in the pull request: whether follow-up is needed,
   where your draft is, and which public page it is headed for. An unanswered
   checkbox is not an answer.
3. If follow-up is needed, add the `docs-needed` label. Do not open an issue in the
   documentation repository yourself and do not notify anyone at this stage.
4. On merge, an issue is opened in `tapstate/docs` automatically, assigned to the
   documentation owner, linking back to this pull request. A pull request closed
   without merging produces nothing.

## Guidelines

- **Java 21.** The build targets JDK 21; the native CLI requires GraalVM for JDK 21.
- **Comments and identifiers in English.** Keep code comments, Javadoc, test names
  and messages in English.
- **Keep commit messages clean.** Plain, descriptive messages; please don't paste
  automated-tool signature footers into commits or PR descriptions (a CI check
  rejects them).
- **Match the surrounding code.** Follow the conventions and structure of the
  module you're editing; the architecture tests enforce the dependency rules
  between modules.
- **Add tests** for behavior changes, and keep the existing ones green — including
  the end-to-end case described above.

## Reporting issues

Use the **bug or idea** template — the first lane in
[External contributions](#external-contributions). Include the version and, where relevant, a
minimal reproduction (for the CLI, the `.tap.yml` input and the exact command). For a security
vulnerability, do not open an issue at all: see [SECURITY.md](SECURITY.md).
