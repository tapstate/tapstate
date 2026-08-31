#!/usr/bin/env bash
# The release workflow's shape, checked as a text.
#
# What this holds down is the one property the whole arrangement rests on and that nothing else can
# see: the three acts that cannot be taken back -- creating the tag, publishing, pushing the image --
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

# Anything that pushes an image, publishes a release, or moves the floating pointer, in a job that
# does not need the approval, is the failure this file exists for. Checked over every job there is,
# so a new one is covered without this list being edited.
for j in $jobs_list; do
  case "$j" in publish) continue ;; esac
  body="$(job "$j")"
  if grep -qE 'imagetools create|docker push|push: true|--draft=false|--latest=' <<<"$body"; then
    bad "no irreversible act in '$j'" \
        "$(grep -E 'imagetools create|docker push|push: true|--draft=false|--latest=' <<<"$body" | head -1)"
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
has   "publishing edits the draft that was reviewed" publish 'gh release edit'
hasnt "publishing re-sends no body"                  publish '\-\-notes|body_path|body:'
hasnt "and does not run the release action again"    publish 'action-gh-release'

# C7. Rejected, timed out, or failed on the way: the draft is the one thing a rejection can leave
# behind that still looks publishable.
has "a rejected run deletes its draft"        discard 'gh release delete'
has "and it runs even when the run failed"    discard 'always\(\)'
has "and only when nothing was published"     discard "publish.result != 'success'"

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
