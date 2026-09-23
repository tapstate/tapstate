#!/usr/bin/env bash
# A lane that installs must say the install is ours.
#
# The installer reports one event per completed install, and its default endpoint is the production
# one. A lane that installs on four platforms every night therefore feeds a steady floor of installs
# that no person performed into the one figure the funnel divides by. That happened: the floor ran
# for weeks, and nothing in a stored event could separate it from a real user afterwards.
#
# Two independent things keep it out, and this gate is what makes a lane carry both:
#
#   TAPSTATE_TELEMETRY_URL      point the send somewhere that is not production, so nothing is sent.
#   TAPSTATE_TELEMETRY_CHANNEL  declare the install as ours, so that IF the send ever does reach
#                               production -- a dropped override, a new step, a container that did
#                               not inherit the environment -- it arrives marked and stays out of
#                               the denominator rather than hiding inside it.
#
# Either alone fails silently in exactly the way this gate exists to end: the first leaves no trace
# when it is forgotten, and the second sends events nobody asked for. So both are required, and
# forgetting is a red check rather than a number that is quietly wrong for a month.
#
# What is deliberately NOT covered: the smoke suites. They run the installer too, but each one
# already refuses to start against the production endpoint and says so in a case of its own -- a
# gate they carry themselves, checked on every run rather than by a scan of their caller. Requiring
# their caller to export a channel as well would also hand the suites an inherited value, and their
# own default-channel case is precisely about an unset one.
set -euo pipefail

# The two ways a workflow reaches a real install: the published one-liner piped into a shell, and
# either script run straight out of the checkout. At the top because the liveness control runs these
# exact expressions -- a control with its own copy proves the copy works and says nothing about the
# ones that decide.
#
# Both scripts are executable, so `./install/install.sh` runs one without naming an interpreter; a
# pattern that demanded `sh` or `bash` in front would report a workflow that installs on every run as
# carrying no installer at all. The path has to arrive with a leading `/` or `./` to count as being
# run -- that is what separates executing it from naming it, as a `paths:` trigger or an argument does.
#
# The pipeline is looked for on a LOGICAL line, after backslash-continuations are joined: a `run: |`
# block may write the URL on one line and `| sh` on the next, and bash runs that as one command.
PIPED='install\.tapstate\.dev[^|]*\|'
SCRIPTS='(install/install\.sh|deploy/quickstart/quickstart\.sh)'
DIRECT="(^|[^[:alnum:]_.-])((sh|bash)[[:space:]]+[^|;&]*${SCRIPTS}|\.?/${SCRIPTS})"

REQUIRE_URL='TAPSTATE_TELEMETRY_URL:'
# Anchored at the end of the value, not merely at its start. The installer marks an install as ours
# only on the exact word, so `internal-test` is a community install -- and a fence pattern that
# accepted it would report a lane as declared while its events went into the denominator, which is
# the one thing this gate exists to make impossible.
REQUIRE_CHANNEL='TAPSTATE_TELEMETRY_CHANNEL:[[:space:]]*internal([[:space:]]|$)'

SELF='.github/scripts/no-unfenced-install-telemetry.sh'

die_detector() {
  echo "::error::$1"
  echo "Nothing this gate says about the repository can be trusted, so it is a failure rather than a pass."
  exit 1
}

# What the patterns above are matched against: the workflow as a shell would read it, not as a file
# viewer shows it. Three passes, in this order and for three different reasons.
#
#   1. Join backslash-continuations. A `run: |` block may end a line with `\` and put `| sh` on the
#      next; bash runs that as one command, and a per-line scan sees an installer that is never piped
#      anywhere and a pipe that installs nothing.
#   2. Drop whole-line comments. A commented-out one-liner runs nothing, and refusing it would be
#      refusing prose.
#   3. Remove lint invocations -- `shellcheck -s sh install/install.sh` reads the script and executes
#      nothing, so demanding a channel from the lane that lints would teach it to claim an install it
#      never performs. **Only the lint command, never the rest of the line.** Dropping the whole line
#      was the first attempt and it opened the hole it was meant to close: in
#      `shellcheck install/install.sh && ./install/install.sh` the install runs, and a line-wise
#      exclusion made the gate report that workflow as carrying no installer at all.
prepared() { # <file>
  sed -e :a -e '/\\$/N; s/\\\n//; ta' "$1" \
    | grep -vE '^[[:space:]]*#' \
    | sed -E 's/(^[[:space:]]*|[;&|][[:space:]]*)shellcheck[^;&|]*/\1/g'
}

installs() { # <file>
  grep -qE "$PIPED|$DIRECT" <(prepared "$1")
}

# Comments are stripped here for the same reason they are stripped above, in the other direction: a
# workflow that merely writes the two names in a comment -- this file's own header does exactly that
# -- would otherwise read as fenced while its install step inherits neither.
#
# What this deliberately does NOT do is resolve scope. It asks whether the workflow sets both, not
# whether the job holding the install step does, so a workflow whose only `env:` block sits on an
# unrelated job passes. Deciding that needs the job/step tree, and a YAML parser is not something this
# repository's gates can assume on a runner. The bound is stated rather than papered over: this
# catches forgetting, which is the failure that happened, and not a fence deliberately put in the
# wrong place. Both lanes it guards today set the variables at workflow level, where every job has them.
fenced() { # <file>
  local body; body="$(prepared "$1")"
  grep -qE "$REQUIRE_URL" <<<"$body" && grep -qE "$REQUIRE_CHANNEL" <<<"$body"
}

# A repository where no workflow installs reports nothing and exits 0 -- which is also exactly what
# this does once the detector stops matching. So before it decides anything here it decides three
# cases whose answers are already known: one that must be caught, and two that must not.
detector_alive() {
  local d; d="$(mktemp -d)"
  printf '%s\n' 'run: curl -sSL https://install.tapstate.dev/cli | sh' > "$d/unfenced.yml"
  {
    printf '%s\n' 'env:'
    printf '%s\n' '  TAPSTATE_TELEMETRY_URL: http://127.0.0.1:1/e'
    printf '%s\n' '  TAPSTATE_TELEMETRY_CHANNEL: internal'
    printf '%s\n' 'run: curl -sSL https://install.tapstate.dev/cli | sh'
  } > "$d/fenced.yml"
  # Reading the domain is not installing from it. The publishing lane names it on every run and
  # sends nothing, so a gate that matched the name alone would demand a channel from a job that
  # performs no install -- and the first person to add one would be teaching the lane a lie.
  printf '%s\n' '  SITE_DOMAIN: install.tapstate.dev' > "$d/mentions.yml"
  printf '%s\n' '        # curl -sSL https://install.tapstate.dev/cli | sh' > "$d/commented.yml"
  printf '%s\n' '          shellcheck -s sh install/install.sh' > "$d/linted.yml"
  # The pipe is on the next line. Bash reads one command; a per-line scan reads two harmless halves.
  {
    printf '          curl -sSL https://install.tapstate.dev/cli \\\n'
    printf '%s\n' '            | sh'
  } > "$d/continued.yml"
  # Lints, then installs. Excluding the line rather than the command hides the half that runs.
  printf '%s\n' '          shellcheck install/install.sh && ./install/install.sh' > "$d/chained-lint.yml"
  # Installs, and declares a channel the installer does not treat as ours.
  {
    printf '%s\n' 'env:'
    printf '%s\n' '  TAPSTATE_TELEMETRY_URL: http://127.0.0.1:1/e'
    printf '%s\n' '  TAPSTATE_TELEMETRY_CHANNEL: internal-test'
    printf '%s\n' 'run: curl -sSL https://install.tapstate.dev/cli | sh'
  } > "$d/suffix-fence.yml"
  # Executable, so no interpreter is named. A pattern demanding one calls this workflow clean.
  printf '%s\n' '          ./install/install.sh --print-platform' > "$d/direct.yml"
  # Names a script without running it: a path filter, and an argument to a tool that reads it.
  printf '%s\n' "  paths: ['install/install.sh']" > "$d/named.yml"
  # Installs, with the fence commented out -- the shape someone leaves behind while debugging. Both
  # lines have to be spelled the way the checks below spell them, or the control proves nothing about
  # them: an earlier draft wrote the names without their colons and passed against either version.
  {
    printf '%s\n' '# env:'
    printf '%s\n' '#   TAPSTATE_TELEMETRY_URL: http://127.0.0.1:1/e'
    printf '%s\n' '#   TAPSTATE_TELEMETRY_CHANNEL: internal'
    printf '%s\n' 'run: curl -sSL https://install.tapstate.dev/cli | sh'
  } > "$d/commented-fence.yml"

  installs "$d/unfenced.yml" || die_detector "the scan did not recognise a published one-liner piped into a shell."
  fenced "$d/unfenced.yml"   && die_detector "the scan called an unfenced control fenced."
  installs "$d/fenced.yml"   || die_detector "the scan did not recognise the one-liner in the fenced control."
  fenced "$d/fenced.yml"     || die_detector "the scan did not accept a control carrying both variables."
  installs "$d/mentions.yml" && die_detector "the scan treated a mention of the install domain as an install."
  installs "$d/commented.yml" && die_detector "the scan treated a commented-out one-liner as an install."
  installs "$d/linted.yml"    && die_detector "the scan treated a lint invocation as an install."
  installs "$d/continued.yml" || die_detector "the scan did not join a backslash-continued installer pipeline."
  installs "$d/chained-lint.yml" || die_detector "the scan lost an install chained after a lint command."
  fenced "$d/suffix-fence.yml" && die_detector "the scan accepted a channel value the installer does not treat as ours."
  installs "$d/direct.yml"    || die_detector "the scan did not recognise an executable installer run by path."
  installs "$d/named.yml"     && die_detector "the scan treated a path filter naming the script as an install."
  fenced "$d/commented-fence.yml" && die_detector "the scan accepted a fence that exists only in a comment."
  rm -rf "$d"
}

detector_alive

unfenced=()
while IFS= read -r f; do
  [ "$f" = "$SELF" ] && continue
  installs "$f" || continue
  fenced "$f" || unfenced+=("$f")
done < <(git ls-files '.github/workflows/*.yml' '.github/workflows/*.yaml')

if [ "${#unfenced[@]}" -gt 0 ]; then
  echo "::error::workflow(s) that install without declaring the install as ours:"
  printf '  %s\n' "${unfenced[@]}"
  echo
  echo "Add both to the workflow's env, so a forgotten override arrives marked instead of silently"
  echo "joining the figure the funnel divides by:"
  echo "  TAPSTATE_TELEMETRY_URL: http://127.0.0.1:1/e"
  echo "  TAPSTATE_TELEMETRY_CHANNEL: internal"
  exit 1
fi
echo "clean: every workflow that installs declares the install as ours."
