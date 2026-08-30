#!/usr/bin/env bash
# The documentation-impact section of a pull request, checked for having been answered at all, and
# for agreeing with the `docs-needed` label.
#
# Two states of this section are, in this repository today, character-for-character identical to
# anything downstream: the author thought about it and wrote "none", and the author scrolled past it.
# Neither carries the label, so `docs-followup.yml` -- which reads only the label -- treats them the
# same, and nothing anywhere reports the difference. This gate is the only place the two are told
# apart, which is why it reads the fields and not the label.
#
# The other half is agreement. A pull request naming a page with no `docs-needed` label is the worst
# shape here: the author has said where the documentation is going, believes that is the whole of
# their part, and the docs owner is never told, because the label is what opens the follow-up issue.
# The mirror -- "none" twice with the label on -- conjures an issue nobody wanted written.
#
# The two inconsistencies are refused separately and never share a sentence. They are repaired in
# opposite directions: one wants the label added, the other wants it removed, and being told the
# wrong one costs a round trip on a pull request that is already correct in the author's head.
#
# Reads PR_BODY, PR_LABELS (comma- or newline-separated) and PR_ACTOR. Exits 1 naming what is wrong.
set -uo pipefail

body="${PR_BODY:-}"
labels="${PR_LABELS:-}"
actor="${PR_ACTOR:-}"

here="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=.github/scripts/_pr-section.sh
. "$here/_pr-section.sh"

section="Documentation impact"
label="docs-needed"
# The two fields, exactly as the template writes them. Named here rather than matched loosely: a
# gate that accepted any bolded field would keep passing after the template was reworded, while the
# thing it reads had quietly become something else.
fields=("Draft in this repository" "Public page it is headed for")

# Bots do not get the template, and refusing them does not make anyone write documentation -- it
# makes somebody switch this check off for everyone. Matched on the trailing marker GitHub gives
# every app account, so a new bot needs no edit here.
case "$actor" in
  *'[bot]') echo "clean: $actor is a bot, and the template is not asked of one."; exit 0 ;;
esac

if ! grep -qE "^## ${section}[[:space:]]*$" <<<"$body"; then
  echo "::error::the pull request body has no \"## ${section}\" section — it is in the template, and removing it does not answer it"
  echo "Two answers are wanted, and \"none\" is one of them. See .github/PULL_REQUEST_TEMPLATE.md."
  exit 1
fi

answers="$(section_body "$section")"

# Whatever the author wrote after one field's colon: nothing (the prompt was left alone, or the line
# was deleted), the word "none", or a value.
field_value() {
  printf '%s\n' "$answers" | awk -v want="$1" '
    index($0, "**" want ":**") {
      sub(/^.*\*\*[^*]*:\*\*/, "")
      gsub(/^[ \t]+|[ \t]+$/, "")
      print
      exit
    }
  '
}

unanswered=()
declared=0        # at least one field names something, so follow-up exists
for name in "${fields[@]}"; do
  value="$(field_value "$name")"
  if [ -z "$value" ]; then
    unanswered+=("$name")
  elif ! is_none "$value"; then
    declared=1
  fi
done

if [ "${#unanswered[@]}" -eq "${#fields[@]}" ]; then
  echo "::error::\"## ${section}\" answered neither field — both are still the template's prompt, and an unanswered field is not a decision"
  echo "Write a path and a URL, or write \"none\" in both. \"none\" means you judged it, which is what this asks for."
  exit 1
fi

if [ "${#unanswered[@]}" -ne 0 ]; then
  for name in "${unanswered[@]}"; do
    echo "::error::\"${name}\" is still the template's prompt, with nothing written under it — write a value or write \"none\""
  done
  exit 1
fi

# Exactly the label, not a label that contains it. The list is built first and matched against as a
# whole word, rather than piped into `grep -q`: under `pipefail`, grep exiting the moment it matches
# kills the writer behind it, and the pipeline then reports that signal instead of the match. Which
# way the race falls depends on the machine, so the failure is a pull request refused for carrying a
# label it does carry, on some runs and not others.
has_label=0
label_list="$(printf '%s\n' "$labels" | tr ',' '\n' | sed 's/^[ \t]*//; s/[ \t]*$//')"
if grep -qx -- "$label" <<<"$label_list"; then has_label=1; fi

if [ "$declared" = 1 ] && [ "$has_label" = 0 ]; then
  echo "::error::this pull request names documentation to follow up on, but carries no \`${label}\` label — nothing opens the follow-up issue, and the docs owner is never told"
  echo "Add the \`${label}\` label. On merge it opens the issue in tapstate/docs, linked back here; do not open that issue by hand."
  exit 1
fi

if [ "$declared" = 0 ] && [ "$has_label" = 1 ]; then
  echo "::error::both fields say \"none\", yet this pull request carries \`${label}\` — the two contradict each other, and on merge the label opens a documentation issue nobody meant to ask for"
  echo "Remove the label, or say in the fields what the documentation is."
  exit 1
fi

echo "clean: the documentation impact is answered, and the \`${label}\` label agrees with it."
