#!/usr/bin/env bash
#
# The documentation site's part in a release, which is coordinated rather than pinned.
#
#   docs-release.sh open <version>                          ask for the site to be published
#   docs-release.sh settle <version> --notes-url <url>      after publishing: tag it, or ask again
#   docs-release.sh retire <version>                        the attempt is over: withdraw the request
#
# Why this repository is not on the satellite list. The site is built by Netlify from two branches:
# `next` is the preview and `main` is what the public sees. A release branch cut across that pair
# would either publish nothing or publish the wrong thing, so there is nothing here to pin. What the
# release needs from the documentation is an action by a person -- merge `next` into `main` -- and an
# action by a person is asked for in an issue, not modelled as a ref.
#
# Why it never blocks. Everything else in a release is machine-checkable and refuses when it is not
# satisfied. This is not: it waits on somebody's morning. A release that stops for it would make the
# whole train hostage to one person's calendar, so `settle` always exits 0 -- it either creates the
# tag because the work is done, or it asks again in a new issue and lets the release finish. That is
# a deliberate asymmetry, and it is the reason this is a separate script from satellites.sh: that one
# refuses on every failure, and mixing the two postures in one file is how one of them gets lost.
#
# Why `retire` exists. A release attempt is atomic: it either publishes or is thrown away whole. An
# attempt that is thrown away has already asked for a version that is now not coming, and nothing
# used to withdraw that request -- two abandoned attempts at one version left two open issues asking
# for it side by side, and the person they are assigned to had no way to tell either from a real one.
# It runs in the job that cleans up after every ending, so like `settle` it never refuses: a cleanup
# step that can go red leaves the rest of the cleanup undone.
#
# What the two outcomes are, at `settle`:
#
#   the issue is closed   the site is published, so the tag is ours to create -- it goes on `main`,
#                         which is what Netlify serves, and a comment on the issue names it
#   still open            a second issue is opened, naming the first: finish it, then create the tag
#                         yourself and write the version you used into the issue. The release has
#                         already gone out by then; this is the part that catches up afterwards
#
# The issue is found again by its exact title, because the two halves run in different workflow
# dispatches and nothing carries a number between them. A title that does not match exactly once is
# treated as "not done" rather than guessed at: asking twice costs a notification, tagging the wrong
# thing costs a published site.
#
# DOCS_OWNER names who is asked. Its default is also written into docs-followup.yml, which assigns
# the same person; the two are twins and have to move together. Neither is the ledger for who holds
# the role -- that is the repository's CODEOWNERS -- and pointing both at it is worth doing the day
# that file exists.
set -uo pipefail

verb="${1:-}"
[ $# -gt 0 ] && shift
version=""
case "$verb" in
    open|settle|retire)
        case "${1:-}" in ""|--*) ;; *) version="$1"; shift ;; esac ;;
esac

repo="${DOCS_REPO:-tapstate/docs}"
owner="${DOCS_OWNER:-heywalter}"
notes_url=""
plan=0
assume=""

while [ $# -gt 0 ]; do
    case "$1" in
        --notes-url) notes_url="${2:-}"; shift 2 ;;
        --plan) plan=1; shift ;;
        --assume-state) assume="${2:-}"; shift 2 ;;
        *) echo "docs-release.sh: unknown argument '$1'" >&2; exit 2 ;;
    esac
done

die() { echo "docs-release.sh: $1" >&2; exit "${2:-2}"; }

case "$verb" in
    open|settle|retire) ;;
    "") die "no verb. Expected 'open', 'settle' or 'retire'" ;;
    *)  die "unknown verb '$verb'. Expected 'open', 'settle' or 'retire'" ;;
esac
[ -n "$version" ] || die "$verb needs a version, as x.y.z"
case "$version" in
    *[!0-9.]*|*..*|.*|*.) die "'$version' is not a version. Expected x.y.z, with no leading v" ;;
esac
case "$version" in *.*.*) case "$version" in *.*.*.*) die "'$version' is not a version" ;; esac ;;
    *) die "'$version' is not a version. Expected x.y.z, with no leading v" ;;
esac
# --assume-state is a seam for the cases, which cannot create issues to look at. It is refused
# outside --plan so it can never decide anything in a real run.
[ -z "$assume" ] || [ "$plan" = 1 ] || die "--assume-state is only for --plan"
[ "$verb" != settle ] || [ -n "$notes_url" ] || die "settle needs --notes-url"

title="Release $version: publish the documentation site"
tag="v$version"

# Open a request in the documentation repository and put it in front of the person it is for.
#
# The create and the assignment are two calls on purpose. With `--assignee` on the create, a
# rejected assignment failed the whole command *after* the issue had already been opened, so a
# single exit code stood for two opposite states -- no request at all, and a request that reaches
# nobody -- and the message named the wrong one. Measured across three releases: the issue existed
# every time, unassigned, while the step said it could not be opened and settle then found it again
# by title and counted it as one of four.
#
# Every outcome goes to stdout, and gh's own error is kept rather than discarded. The caller tees
# stdout into the step summary; `>/dev/null 2>&1` is why three releases could say "could not" and
# never why, and the reason is the only part that tells anybody what to change.
#
# The assignment runs under DOCS_ASSIGN_TOKEN when there is one: assigning somebody on that
# repository takes more than opening an issue there does. Unset, it falls back to the ambient
# credential and the outcome is reported either way.
ask() {
    ask_title="$1"
    ask_body="$2"
    ask_done="$3"
    if ! ask_out="$(gh issue create --repo "$repo" --title "$ask_title" --body "$ask_body" 2>&1)"; then
        echo "::warning::$repo holds no request for $tag -- ask $owner by hand: $ask_out"
        echo "$repo  could not open \"$ask_title\" -- ask $owner by hand: $ask_out"
        return 0
    fi
    # gh prints the URL on success; anything it says besides that is not the issue. Taking the last
    # line would hand a warning to the assignment and report the request as unassignable.
    ask_url="$(printf '%s\n' "$ask_out" | grep -oE 'https://[^[:space:]]+/issues/[0-9]+' | tail -1)"
    if ask_err="$(GH_TOKEN="${DOCS_ASSIGN_TOKEN:-${GH_TOKEN:-}}" \
                  gh issue edit "${ask_url##*/}" --repo "$repo" --add-assignee "$owner" 2>&1 >/dev/null)"; then
        echo "$repo  $ask_done"
    else
        # The request exists and reaches nobody. It sits in a repository its owner does not watch,
        # nothing else in the release mentions it, and the release goes out either way -- so this
        # warning is the only thing between a silent request and a person.
        echo "::warning::$ask_url asks for $tag but could not be assigned to $owner -- tell them by hand: $ask_err"
        echo "$repo  opened $ask_url for $tag, unassigned -- tell $owner by hand: $ask_err"
    fi
}

if [ "$verb" = open ]; then
    if [ "$plan" = 1 ]; then
        echo "$repo  open issue \"$title\", assigned to $owner"
        exit 0
    fi
    ask "$title" \
"tapstate $tag is being released. The site is published from \`main\`, so publishing this release's
documentation means merging \`next\` into \`main\`.

**Close this issue once that merge is on \`main\`.** The release will check this issue when it
publishes:

- closed by then, and the \`$tag\` tag is created here for you;
- still open, and the release goes out anyway -- it is not held up for this -- and you get a second
  issue asking you to finish and then create \`$tag\` yourself.

Nothing here blocks the release. It does decide whether the tag is one less thing for you to do." \
        "asked $owner to publish the site for $tag"
    exit 0
fi

# Find the issue this version's `open` created, by its exact title -- the halves run in different
# jobs and nothing carries a number between them. Sets `state` and, when there is exactly one,
# `number`. Anything other than exactly one match is reported and treated as open, which is the
# answer that costs a notification rather than a wrongly published tag.
find_issue() {
    state="$assume"
    [ -z "$state" ] || return 0
    found="$(gh issue list --repo "$repo" --state all --limit 50 \
               --search "\"$title\" in:title" --json title,state,number 2>/dev/null || true)"
    n="$(printf '%s' "$found" | jq -r --arg t "$title" '[.[]|select(.title==$t)]|length' 2>/dev/null || echo 0)"
    if [ "$n" = 1 ]; then
        state="$(printf '%s' "$found" | jq -r --arg t "$title" 'first(.[]|select(.title==$t))|.state' | tr '[:upper:]' '[:lower:]')"
        number="$(printf '%s' "$found" | jq -r --arg t "$title" 'first(.[]|select(.title==$t))|.number')"
    else
        echo "$repo  no single issue titled \"$title\" ($n found); treating as not done" >&2
        state="open"
    fi
}

# retire: the attempt that opened this request is over and did not publish, so the request is for a
# version that is not coming. Nothing used to withdraw it -- two abandoned attempts at one version
# left two open issues asking for it side by side, and the person they are assigned to had no way to
# tell either from a real one.
#
# It never fails. This runs in the job that cleans up after every ending, including the ones that
# ended badly, and a cleanup step that can go red is one that leaves the rest of the cleanup undone.
if [ "$verb" = retire ]; then
    find_issue
    # One test, upstream of the --plan exit rather than repeated inside it. Written the other way --
    # a --plan branch that decides for itself and a live branch that decides again -- the cases can
    # only ever reach the first, and a mutation to the one that matters survives them all. Measured:
    # it did.
    if [ "$state" = closed ]; then
        echo "$repo  request for $tag was already closed; leaving it alone"
        exit 0
    fi
    if [ "$plan" = 1 ]; then
        echo "$repo  withdraw the request \"$title\""
        exit 0
    fi
    if [ -z "${number:-}" ]; then
        echo "::warning::$repo may still hold an open request for $tag; withdraw it by hand"
        echo "$repo  could not identify the request for $tag; withdraw it by hand if it is there"
        exit 0
    fi
    # Every outcome below goes to stdout, and that is the fix rather than a detail. The caller tees
    # stdout into the step summary; these failures used to go to stderr, which is teed nowhere -- so a
    # retire that wrote nothing and a retire that worked left the same green step and the same empty
    # summary. Measured on a real rejected release: neither the comment nor the close landed, and the
    # only trace anywhere was a line nothing reads.
    #
    # gh's own error is kept rather than discarded. `>/dev/null 2>&1` is why the old message could say
    # "could not" and never why, and whoever picks this up next needs the reason, not the fact.
    if comment_err="$(gh issue comment "$number" --repo "$repo" --body \
"Withdrawing this: the release attempt that opened it ended without publishing, so $tag is not
coming from it. **No documentation work is owed from this issue.**

When $tag is actually released, a new issue is opened here by that release, and that one is the real
request. Nothing is carried over from this one." 2>&1 >/dev/null)"; then
        commented=yes
    else
        commented=no
    fi
    if close_err="$(gh issue close "$number" --repo "$repo" --reason "not planned" 2>&1 >/dev/null)"; then
        if [ "$commented" = yes ]; then
            echo "$repo  withdrew the request for $tag (#$number)"
        else
            # Closed with no reason on it -- worse for the person it is assigned to than leaving it
            # open, because the request leaves their queue with nothing saying why.
            echo "::warning::$repo#$number was closed for $tag but the explanation could not be posted: $comment_err"
            echo "$repo  withdrew the request for $tag (#$number), but the explanation did not post"
        fi
    else
        echo "::warning::$repo#$number still asks for $tag, which is not coming -- withdraw it by hand: $close_err"
        echo "$repo  could not close #$number -- withdraw it by hand: $close_err"
    fi
    exit 0
fi

# settle
find_issue

if [ "$state" = closed ]; then
    if [ "$plan" = 1 ]; then
        echo "$repo  site is published: tag $tag on main, and say so on the issue"
        exit 0
    fi
    sha="$(gh api "repos/$repo/git/ref/heads/main" --jq '.object.sha' 2>/dev/null)"
    if [ -z "$sha" ]; then
        echo "$repo  cannot read main; leaving $tag to $owner" >&2
        exit 0
    fi
    if gh release create "$tag" --repo "$repo" --target "$sha" --title "$tag" \
         --notes "The documentation published with tapstate $tag. What changed is in the tapstate release: $notes_url" \
         >/dev/null 2>&1; then
        echo "$repo  $tag created on main"
        [ -z "${number:-}" ] || gh issue comment "$number" --repo "$repo" \
            --body "Released as \`$tag\`, cut from \`main\`. Nothing further needed here." >/dev/null 2>&1 || true
    else
        echo "$repo  could not create $tag -- $owner can create it by hand" >&2
    fi
    exit 0
fi

if [ "$plan" = 1 ]; then
    echo "$repo  site not published yet: open a second issue asking $owner to finish and tag $tag"
    exit 0
fi
ask "Still to do: publish the documentation for $tag" \
"tapstate $tag has been released. The documentation for it has not been published yet -- the earlier
issue asking for \`next\` to be merged into \`main\` is still open, and the release did not wait for
it, by design.

Two things, in this order:

1. Finish the earlier issue: merge \`next\` into \`main\`, and close it.
2. Create the \`$tag\` tag here yourself, on \`main\`, and write the version you tagged into that
   issue so there is a record of which commit the published site corresponds to.

The tag is yours this time rather than ours because the release has already gone; nothing here is
going to create it after the fact.

The tapstate release: $notes_url" \
   "site not published in time; asked $owner to finish and tag $tag themselves"
exit 0
