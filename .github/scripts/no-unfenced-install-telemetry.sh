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
PIPED='install\.tapstate\.dev[^|]*\|'
DIRECT='(^|[^[:alnum:]_.-])(sh|bash)[[:space:]]+[^|;&]*(install/install\.sh|deploy/quickstart/quickstart\.sh)'

REQUIRE_URL='TAPSTATE_TELEMETRY_URL:'
REQUIRE_CHANNEL='TAPSTATE_TELEMETRY_CHANNEL:[[:space:]]*internal'

SELF='.github/scripts/no-unfenced-install-telemetry.sh'

die_detector() {
  echo "::error::$1"
  echo "Nothing this gate says about the repository can be trusted, so it is a failure rather than a pass."
  exit 1
}

# A workflow that installs, ignoring two kinds of line that only look like one. A commented-out
# one-liner runs nothing, and a gate that counted it would be refusing prose. A lint invocation
# names the script as an argument -- `shellcheck -s sh install/install.sh` reads it and executes
# nothing -- and demanding a channel from the lane that lints would teach it to claim an install
# it never performs.
installs() { # <file>
  grep -qE "$PIPED|$DIRECT" <(grep -vE '^[[:space:]]*#|shellcheck' "$1")
}

fenced() { # <file>
  grep -qE "$REQUIRE_URL" "$1" && grep -qE "$REQUIRE_CHANNEL" "$1"
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

  installs "$d/unfenced.yml" || die_detector "the scan did not recognise a published one-liner piped into a shell."
  fenced "$d/unfenced.yml"   && die_detector "the scan called an unfenced control fenced."
  installs "$d/fenced.yml"   || die_detector "the scan did not recognise the one-liner in the fenced control."
  fenced "$d/fenced.yml"     || die_detector "the scan did not accept a control carrying both variables."
  installs "$d/mentions.yml" && die_detector "the scan treated a mention of the install domain as an install."
  installs "$d/commented.yml" && die_detector "the scan treated a commented-out one-liner as an install."
  installs "$d/linted.yml"    && die_detector "the scan treated a lint invocation as an install."
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
