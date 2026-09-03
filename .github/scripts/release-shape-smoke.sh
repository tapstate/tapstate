#!/usr/bin/env bash
# The release workflow's shape, checked as a text.
#
# What this holds down is the one property the whole arrangement rests on and that nothing else can
# see: the acts that cannot be taken back -- creating the tag, publishing, pushing the image, and
# tagging the satellite repositories --
# are all downstream of the pause a person approves, and none of them happens before it. Every one of
# them is one line away from moving. A `push: true` on the image build, a second release action after
# the approval, a job that stops needing `approve`: each is a small, plausible edit, each leaves the
# workflow green, and each is only discovered by rejecting a release and finding something left over.
#
# The body assembled before the pause is the other half. Whoever approves may edit it on the Releases
# page, and re-sending an assembled body afterwards puts their edit back the way it was -- silently,
# with the release published and the run green. Nothing about that is visible except the body.
#
# Reads the workflow as text, attributing each line to the job it is under. Exits 0 if every case
# holds.
set -uo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
workflow="$here/../workflows/release.yml"
passed=0
failed=0

[ -f "$workflow" ] || { echo "no release workflow at $workflow"; exit 1; }

# Every line of one job, from its two-space key to the next one, with whole-line comments dropped.
# Dropping them is not tidiness: this file is heavily commented, and the comments say what the rules
# are -- "`--notes` is deliberately not passed" is a sentence about the absence of `--notes`. Match
# against the prose and every check here answers about the explanation rather than the workflow,
# which fails in both directions: a rule that was removed while its comment stayed reads as present,
# and a rule that is present reads as broken because something described it.
job() {
  awk -v want="  $1:" '
    $0 == want { inside = 1; next }
    /^  [a-z][a-z0-9_-]*:[ \t]*$/ { inside = 0 }
    inside && $0 !~ /^[ \t]*#/ { print }
  ' "$workflow"
}

jobs_list="$(awk '/^  [a-z][a-z0-9_-]*:[ \t]*$/ { gsub(/[ :]/, ""); print }' "$workflow")"

ok()   { printf '  ok    %s\n' "$1"; passed=$((passed + 1)); }
bad()  { printf '  FAIL  %s\n        %s\n' "$1" "$2"; failed=$((failed + 1)); }

has()    { if grep -qE -- "$3" <<<"$(job "$2")"; then ok "$1"; else bad "$1" "job '$2' has no line matching /$3/"; fi }
hasnt()  { if grep -qE -- "$3" <<<"$(job "$2")"; then bad "$1" "job '$2' still matches /$3/: $(grep -E -- "$3" <<<"$(job "$2")" | head -1)"; else ok "$1"; fi }

# --- the approval is upstream of everything irreversible -------------------------------------------
has "the publish job waits on the approval" publish 'needs:.*approve'

# Tagging three other repositories is the fourth act that cannot be taken back, and the one furthest
# from the approval -- it happens in repositories this workflow cannot see the inside of, so nothing
# on this side would notice it having gone out early. It is allowed to do that only downstream of the
# publish it announces: a satellite tagged first links to a release page that does not exist, and for
# the documentation repository the tag is what makes the documentation public.
has "the satellite job waits on the publish it announces" satellites 'needs:.*publish'
# One job retires the branches, and it runs however the attempt ended. An attempt is atomic: nothing
# is carried from one to the next, so a branch left behind is never a pin somebody still needs -- it
# is only something in the way of the next attempt, which refuses a branch of that name. Measured
# twice in one day: a release died before a draft existed, the cleanup was conditional on a draft
# having been assembled, and so it never ran; three refs in three repositories were deleted by hand.
has "the branches are retired by one job whatever happened" cleanup 'satellites[.]sh unbranch'
# shellcheck disable=SC2016  # the workflow's own text is `$branch`; expanding it here would search for ours
has "including this repository's own"                       cleanup 'refs/heads/\$branch'
has "and it runs even when the run failed"                  cleanup 'always\('
# The one state where the pin is still owed to somebody: the release published and the satellites did
# not finish being tagged. Their branches are then the only record of what they were meant to point
# at, and deleting them turns a half-finished release into one nobody can finish by hand.
has "except while a published release still owes its satellites" cleanup 'needs[.]satellites[.]result'
# Not in the satellite job any more, and not conditional on a draft. Those were the two shapes that
# between them left every failed attempt's branches standing.
hasnt "the publish path retires no branches of its own" satellites 'unbranch'

# Anything that pushes an image, publishes a release, or moves the floating pointer, in a job that
# does not need the approval, is the failure this file exists for. Checked over every job there is,
# so a new one is covered without this list being edited.
for j in $jobs_list; do
  case "$j" in publish|satellites) continue ;; esac
  body="$(job "$j")"
  if grep -qE 'imagetools create|docker push|push: true|draft=false|--latest=|make_latest|satellites[.]sh release|docs-release[.]sh settle' <<<"$body"; then
    bad "no irreversible act in '$j'" \
        "$(grep -E 'imagetools create|docker push|push: true|draft=false|--latest=|make_latest|satellites[.]sh release|docs-release[.]sh settle' <<<"$body" | head -1)"
  else
    ok "no irreversible act in '$j'"
  fi
done

# The image is built before the pause and pushed after it. Both halves, or the archive is pointless.
has   "the image is built into an archive"      server-image 'type=oci'
has   "and explicitly not pushed"               server-image 'push: false'
has   "and the archive is what gets pushed"     publish      'oci-layout://'
hasnt "the image is not rebuilt after approval" publish      'build-push-action'

# C6. The publish step edits the existing release; it never re-sends a body. Re-running the action
# that assembled the draft would overwrite whatever the approver wrote, and nothing would say so.
#
# It reaches that release by its id, which nothing can change, and never by its tag name, which the
# approver can. Editing a draft's body through the API without re-sending `tag_name` blanks the tag
# to `untagged-<hex>`, and editing that body at the approval point is one of the four things the
# design asks the approver to do. Measured 2026-09-02: one such edit took out the publish and the
# cleanup meant to catch it in the same run, because both looked the release up by a name it no
# longer answered to. It left an orphan draft nothing would collect, and spent the version.
has   "publishing edits the draft that was reviewed" publish 'releases/\$\{\{ needs\.draft\.outputs\.id \}\}'
hasnt "and never by the tag the approver can blank"  publish 'gh release (edit|view|delete)'
# Addressing by id alone would be worse than the bug it fixes: it finds the release and publishes it
# under `untagged-<hex>` -- a real tag on a real release, instead of a run that failed loudly. So the
# publish states the tag it means, and a blanked one is repaired on the way out.
# The field as sent, not the word: the step also reads `.tag_name` back out of the response, and a
# case matching the bare word was satisfied by that readback while the field itself was gone.
has   "and states the tag it means"                  publish '\-f tag_name='
hasnt "publishing re-sends no body"                  publish '\-\-notes|body_path|body:'
hasnt "and does not run the release action again"    publish 'action-gh-release'

# C16. Publishing is what the rest of the release is supposed to hear about, and the credential
# decides whether anybody hears it. An event caused by GITHUB_TOKEN starts no workflow run, so a
# draft published with it delivers `release: published` to nothing -- and the deployment that puts
# the newly released installer on the domain the README tells people to pipe into sh is triggered by
# exactly that event. Measured 2026-09-03: two releases went out that way and the site kept serving
# the version before them, with every run green, every pin correct, and nothing red anywhere. The one
# release that did reach it had been published by a person.
#
# So the token is pinned in both directions. Asserting only the app token would pass on a step that
# had both; asserting only the absence would pass on a step with no token at all, which fails in a
# way that at least stops.
hasnt "publishing does not use the credential GitHub makes inert" publish 'GH_TOKEN: \$\{\{ github\.token \}\}'
has   "and uses one whose events are delivered"                   publish 'GH_TOKEN: \$\{\{ steps\.token\.outputs\.token \}\}'
has   "minted in this job"                                        publish 'create-github-app-token'
# And minted before the image push, which is the first act in this job that cannot be taken back. A
# credential that cannot be minted is the one failure this job can still have, and having it after
# the push leaves an image in the registry for a release that never happened. Ordering inside a job
# is invisible to every other case here, and moving a step is the tidy-up that would do it.
tok_at="$(job publish | grep -n 'create-github-app-token' | head -1 | cut -d: -f1)"
push_at="$(job publish | grep -n 'imagetools create' | head -1 | cut -d: -f1)"
if [ -n "$tok_at" ] && [ -n "$push_at" ] && [ "$tok_at" -lt "$push_at" ]; then
  ok "and minted before anything irreversible"
else
  bad "and minted before anything irreversible" \
      "token step at line ${tok_at:-none} of the job, image push at ${push_at:-none}"
fi

# C7. Rejected, timed out, or failed on the way: the draft is the one thing a rejection can leave
# behind that still looks publishable. By id for the same reason publishing is: the edit that blanks
# the tag orphans the draft, and this is the step that was supposed to collect it.
has "a rejected run deletes its draft"        cleanup '\-X DELETE .*releases/\$\{\{ needs\.draft\.outputs\.id \}\}'
hasnt "and not by the tag either"             cleanup 'gh release (view|delete)'
has "and only when nothing was published"     cleanup "publish.result != 'success'"
# The documentation site was asked, in an issue, to publish a version that is now not being released.
# Nothing retired that request: two abandoned attempts at one version left two open issues asking for
# it, and the person they are assigned to had no way to tell either from a real one.
has "and retires the request it made of the documentation site" cleanup 'docs-release[.]sh retire'
# And the image, which is the third thing an abandoned attempt leaves behind and the one nothing
# used to collect. The design enumerated two endings after the approval -- everything goes out, or
# nothing is left -- and in neither of them does an image exist without a release, so no step was
# written for it. There is a third: approved, image pushed, and the publish that follows it in the
# same job fails. Measured 2026-09-02 -- `ghcr.io/<repo>:0.4.1` sat in the registry for hours with
# no release, no tag, and nothing anywhere that would ever have removed it.
#
# The image is pushed BEFORE the release is published and stays that way: publishing fires the
# install-site deployment and the clean-environment smoke, and both pull the image the moment the
# release exists. Pushing afterwards would redden those on a release that is actually fine. So the
# ordering stands, and the attempt is made atomic from the other end instead.
has "and retires the image it pushed"         cleanup 'packages/container/.*versions'
has "and holds the permission to do that"     cleanup 'packages: write'
# Every one of the three is guarded on nothing having been published, and the image is the one where
# getting that wrong deletes the image out from under a release that DID go out. A step-level `if:`
# is invisible to a grep over the whole job, so what is pinned is that there are three of them: drop
# the guard from the new step and this is 2.
if [ "$(job cleanup | grep -cE "publish[.]result != 'success'")" = 3 ]; then
  ok "each of the three is guarded on nothing having been published"
else
  bad "each of the three is guarded on nothing having been published" \
      "wanted 3 (draft, documentation request, image), got $(job cleanup | grep -cE "publish[.]result != 'success'")"
fi

# --- one dispatch: the pin is this workflow's own first job ----------------------------------------
# There is no separate workflow to dispatch first. The freeze lasts exactly as long as this job, which
# is why it builds nothing; a person watching the run sees a job go green rather than having to watch
# a step list inside a long one.
has "the release pins its own commit"     version 'git/refs'
has "and pins every satellite"            version 'satellites[.]sh branch'
has "and asks the documentation site"     version 'docs-release[.]sh open'
# The request is opened under the release credential and assigned under the documentation one,
# because putting somebody on an issue in that repository takes more permission than opening the
# issue does. Measured across three releases: every request was opened and every one of them arrived
# unassigned -- a request that reaches nobody, in a repository its owner does not watch. Both call
# sites are asserted, because losing the second credential is one deleted line and puts it straight
# back to being invisible.
has "and a credential that can assign it"  version    'DOCS_ASSIGN_TOKEN'
has "the follow-up request is assignable too" satellites 'DOCS_ASSIGN_TOKEN'
has "and refuses a branch that exists"    version 'already exists'

# Gate 2 is the only one of the six with no step of its own: it is the smokes in `cli-native`, and it
# blocks by being something the gate needs. Both halves are asserted, because either one going away
# retires the gate in silence -- drop the smokes and the job still builds and still passes; drop the
# dependency and a red smoke stops blocking anything downstream of it.
has "the smokes gate 2 is made of are run"    cli-native 'native-smoke\.sh'
has "and so is the sidecar's"                 cli-native 'mcp-smoke\.sh'
has "and a red smoke blocks the gate"         gates      'needs:.*cli-native'

# --- the gate is upstream of the draft -------------------------------------------------------------
has "the draft waits on the gate"             draft  'needs:.*gates'
has "the gate waits on the connector lane"    gates  'needs:.*connectors'
# The slow lane starts alongside the build, not after it, which is the only reason its ~38 minutes
# overlap the release rather than being added to it. "Not after the gate" is too weak a way to say
# that -- waiting on the build instead would cost the same and still satisfy it -- so what is pinned
# is that it waits on the version and nothing else.
if [ "$(job connectors | grep -cE '^    needs: version[ \t]*$')" = 1 ]; then
  ok "the connector lane starts as early as it can"
else
  bad "the connector lane starts as early as it can" \
      "wanted exactly 'needs: version', got: $(job connectors | grep -E '^    needs:' | head -1)"
fi
has  "the draft body comes from the gate"     draft  'body_path'
has  "and GitHub appends its own list"        draft  'generate_release_notes: true'

# The draft is a draft, and creates no tag until somebody publishes it.
has "the release starts as a draft"           draft  'draft: true'

# --- the roadmap is written after the release, and never holds one back -------------------------
# Both halves are one edit away from inverting. Drop `continue-on-error` and a board the credential
# cannot reach turns every release run red -- after the release is already out, so the red says to
# re-run a publish that must not be re-run. Move it off `publish` and it marks work Shipped that a
# rejected approval means nobody ever received.
has "the roadmap job is allowed to fail"        roadmap 'continue-on-error: true'
has "and it writes the board only once the release is out" roadmap 'needs: \[version, publish\]'

# --- the version write-back runs for the newest line and no other -------------------------------
# The condition is the whole job. Opening the write-back for a fix to an older line walks the default
# branch's version backwards -- 0.5.1 replacing 0.6.0 -- in a pull request where all six pins agree
# with each other, so every check that compares them is green and the person merging has no reason to
# look twice. Whether a release is the newest line is decided elsewhere and has its own cases; what is
# unheld without this one is that the job asks. Deleting the `if` leaves the workflow valid, every
# other case here passing, and the failure arriving as a version number nobody can explain.
has "the write-back only opens for the newest line" write-back "if: needs.version.outputs.is_latest == 'true'"

# --- the connector lane is judged the same way as every other required check --------------------
# It is the sixth gate, and the only one that judges a lane no merge gate covers, so the shape it is
# read with is the whole of it. Reading it any other way -- a count of failures, `gh pr checks`, a
# grep of the run list -- puts back exactly what the shared reader exists to refuse: a lane that never
# started on this commit produces no check-run at all, and every way of counting failures reports that
# as nothing wrong.
has "the connector lane is judged by the same reader as every other check" \
    gates 'checks-on-commit\.sh --sha .* --required real-connectors'

# --- everything is built from the commit the version job resolved -------------------------------
# The `commit` input is what makes a release from an older line possible at all, and it defaults to
# the ref the dispatch ran on. So a job that checked out `github.ref` instead is indistinguishable
# from a correct one on every dispatch from `main`: same tree, same artifacts, same green run. It
# diverges only on the case the input exists for, and there it builds the wrong line's code under the
# right line's version number and publishes it -- with nothing red anywhere.
for j in cli-native server-image connectors gates draft publish; do
  has "$j builds the commit the version job resolved" "$j" 'ref: \$\{\{ needs\.version\.outputs\.sha \}\}'
done
# And the release that comes out says so, which is the half a reader can check afterwards.
has "the draft is attached to that commit" draft 'target_commitish: \$\{\{ needs\.version\.outputs\.sha \}\}'

# --- every needs.<job>.outputs.<name> is one the job actually declares --------------------------
# An undeclared output is not an error anywhere: the expression resolves to the empty string, the
# step runs with a missing argument, and what fails is whatever the argument was for -- somewhere
# else, saying something else. This one shipped: the tag the version counted up from was written to
# the step's output and never lifted to the job's, so both jobs that read the release range would
# have received nothing and refused every release.
missing=""
while IFS= read -r ref; do
  [ -n "$ref" ] || continue
  from="$(printf '%s' "$ref" | cut -d. -f2)"
  what="$(printf '%s' "$ref" | cut -d. -f4)"
  job "$from" | awk '/^    outputs:$/ { inside = 1; next } /^    [a-z]/ { inside = 0 } inside' \
    > /tmp/rss-outputs.$$
  grep -qE "^      ${what}:" /tmp/rss-outputs.$$ || missing="${missing} ${ref}"
  rm -f /tmp/rss-outputs.$$
done <<EOF
$(grep -oE 'needs\.[a-z][a-z0-9_-]*\.outputs\.[a-z_][a-z0-9_]*' "$workflow" | sort -u)
EOF
if [ -z "$missing" ]; then
  ok "every output read from another job is one that job declares"
else
  bad "every output read from another job is one that job declares" "undeclared:${missing}"
fi

printf '\n%s passed, %s failed\n' "$passed" "$failed"
[ "$failed" = 0 ]
