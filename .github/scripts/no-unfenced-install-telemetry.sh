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

# The fence: the endpoint override declared, and the channel set to exactly this value. The installer
# marks an install as ours only on the exact word, so `internal-test` is a community install -- and a
# fence check that accepted it would report a lane as declared while its events went into the
# denominator, which is the one thing this gate exists to make impossible.
FENCE_URL='TAPSTATE_TELEMETRY_URL'
FENCE_CHANNEL='TAPSTATE_TELEMETRY_CHANNEL'
FENCE_VALUE='internal'

SELF='.github/scripts/no-unfenced-install-telemetry.sh'

die_detector() {
  echo "::error::$1"
  echo "Nothing this gate says about the repository can be trusted, so it is a failure rather than a pass."
  exit 1
}

# What the patterns above are matched against: the text of one place a workflow runs things from
# (`scopes` below), as a shell would read it rather than as a file viewer shows it. Three passes, in
# this order and for three different reasons.
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

# Where a workflow runs things from, and the env each place runs with. A step inherits the workflow's
# `env:`, overridden name by name by its job's and then by its own, and nothing from any other job --
# so a workflow whose only fence sits on a job that installs nothing still installs unfenced from the
# job that does. Asking whether the file names both variables, which is what this gate did first,
# finds them in that workflow and calls it clean.
#
# So each workflow is read as YAML and cut into places: every step; every job outside its steps, where
# a matrix value or a reusable workflow's input can carry a command; and the workflow outside its jobs.
# A place's own strings, and none from any other place, are what `installs` reads, and the place is
# fenced only if the env resolved for it carries both. YAML comments reach neither half, so a fence
# that exists only in a comment is no fence.
#
# The reader is python3 with PyYAML. On the runner that is the system Python's own copy, there because
# cloud-init, which the runner image boots with, depends on it. A machine without it fails the
# liveness control below rather than passing a repository nothing was read from.
#
# Prints one line per place that is not fenced: the workflow, a file holding all of that workflow's
# unfenced places together, a file holding this place's own strings, and where the place is. The
# file of all of them is what gets read first -- most workflows install nothing, and every read is a
# pipeline -- and a place is read on its own only to name it, once its workflow is known to install.
#
# Each place's strings end in a line holding only `#`. `prepared` drops it as a comment, and a place
# whose last line is a backslash-continuation joins it instead of the next place's first line -- or,
# in a sed that quits on a continuation with no line after it, instead of losing that last line.
scopes() { # <dir for the text files> <workflow>...
  python3 - "$FENCE_URL" "$FENCE_CHANNEL" "$FENCE_VALUE" "$@" <<'PY'
import os
import sys

try:
    import yaml
except ImportError:
    sys.exit('the workflows are read with PyYAML, and python3 here cannot import it')

# libyaml's loader where PyYAML was built with it: the same reading, several times faster.
Loader = getattr(yaml, 'CSafeLoader', yaml.SafeLoader)
url, channel, value, out = sys.argv[1:5]
count = 0


def strings(node):
    # Values at any depth, never keys: what a runner executes is a value.
    if isinstance(node, str):
        yield node
    elif isinstance(node, dict):
        for child in node.values():
            yield from strings(child)
    elif isinstance(node, list):
        for child in node:
            yield from strings(child)


def inherit(env, node):
    own = node.get('env')
    if own is None:
        return env
    if isinstance(own, dict):
        return {**env, **own}
    # An expression, decided at run time. It may override anything inherited, so nothing inherited
    # can be counted on.
    return {}


def fenced(env):
    # The installer reads an empty or null endpoint as unset and sends to its production default, so
    # an override that is blank, or that names production itself, stops nothing.
    target = env.get(url)
    return (isinstance(target, str) and target.strip() != '' and 'tapstate.dev' not in target
            and env.get(channel) == value)


def written(text):
    global count
    count += 1
    name = os.path.join(out, str(count))
    with open(name, 'w', encoding='utf-8') as f:
        f.write(text)
    return name


for path in sys.argv[5:]:
    try:
        with open(path, encoding='utf-8') as f:
            workflow = yaml.load(f, Loader=Loader) or {}
    except yaml.YAMLError as e:
        sys.exit(f'{path} is not readable as YAML: {e}')
    top = inherit({}, workflow)
    places = [(top, 'outside its jobs', {k: v for k, v in workflow.items() if k != 'jobs'})]
    for job_id, job in (workflow.get('jobs') or {}).items():
        # A job that calls a reusable workflow hands its inputs to a run whose env is the called
        # workflow's alone; the caller's env reaches none of it.
        env = {} if 'uses' in job else inherit(top, job)
        places.append((env, f'job {job_id}, outside its steps', {k: v for k, v in job.items() if k != 'steps'}))
        for number, step in enumerate(job.get('steps') or [], 1):
            name = ' '.join(str(step.get('name') or '').split())
            places.append((inherit(env, step), f'job {job_id}, step {number}' + (f' ({name})' if name else ''), step))
    unfenced = [(where, '\n'.join(strings(node)) + '\n#\n') for env, where, node in places
                if not fenced(env)]
    if unfenced:
        together = written(''.join(text for _, text in unfenced))
        for where, text in unfenced:
            print(f'{path}\t{together}\t{written(text)}\t{where}')
PY
}

# Every place in the given workflows that installs without the fence, one "<workflow>: <where>" per
# line. Fails, having reported nothing, when the workflows cannot be read: an empty report from a
# reader that never ran is the clean tree this gate must not fake.
unfenced_installs() { # <workflow>...
  local d status=0 wf together text where read_together="" together_installs=0
  d="$(mktemp -d)"
  scopes "$d" "$@" > "$d/places" || status=$?
  if [ "$status" -eq 0 ]; then
    while IFS=$'\t' read -r wf together text where; do
      if [ "$together" != "$read_together" ]; then
        read_together="$together"
        together_installs=0
        if installs "$together"; then together_installs=1; fi
      fi
      if [ "$together_installs" = 1 ] && installs "$text"; then
        printf '%s: %s\n' "$wf" "$where"
      fi
    done < "$d/places"
  fi
  rm -rf "$d"
  return "$status"
}

# Whether a report from unfenced_installs names a workflow.
names() { # <report> <workflow>
  grep -qF "$2: " <<<"$1"
}

# A repository where no workflow installs reports nothing and exits 0 -- which is also exactly what
# this does once the detector stops matching. So before it decides anything here it decides cases
# whose answers are already known: ones that must be caught, and ones that must not.
detector_alive() {
  local d report; d="$(mktemp -d)"
  printf '%s\n' 'run: curl -sSL https://install.tapstate.dev/cli | sh' > "$d/unfenced.yml"
  # Both variables at workflow level, where every job's steps inherit them: the shape both lanes use.
  {
    printf '%s\n' 'env:'
    printf '%s\n' '  TAPSTATE_TELEMETRY_URL: http://127.0.0.1:1/e'
    printf '%s\n' '  TAPSTATE_TELEMETRY_CHANNEL: internal'
    printf '%s\n' 'jobs:'
    printf '%s\n' '  install:'
    printf '%s\n' '    steps:'
    printf '%s\n' '      - run: curl -sSL https://install.tapstate.dev/cli | sh'
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
  # Installs marked as ours, with nothing pointing the send away from production. Either half alone
  # is refused.
  {
    printf '%s\n' 'env:'
    printf '%s\n' '  TAPSTATE_TELEMETRY_CHANNEL: internal'
    printf '%s\n' 'run: curl -sSL https://install.tapstate.dev/cli | sh'
  } > "$d/channel-only.yml"
  # Executable, so no interpreter is named. A pattern demanding one calls this workflow clean.
  printf '%s\n' '          ./install/install.sh --print-platform' > "$d/direct.yml"
  # Names a script without running it: a path filter, and an argument to a tool that reads it.
  printf '%s\n' "  paths: ['install/install.sh']" > "$d/named.yml"
  # Installs, with the fence commented out -- the shape someone leaves behind while debugging. Both
  # lines have to be a working fence once uncommented, or the control proves nothing about them: an
  # earlier draft wrote the names without their colons and passed against either version.
  {
    printf '%s\n' '# env:'
    printf '%s\n' '#   TAPSTATE_TELEMETRY_URL: http://127.0.0.1:1/e'
    printf '%s\n' '#   TAPSTATE_TELEMETRY_CHANNEL: internal'
    printf '%s\n' 'run: curl -sSL https://install.tapstate.dev/cli | sh'
  } > "$d/commented-fence.yml"
  # Two jobs, and the fence on the one that installs nothing. Both names are spelled right and neither
  # is in a comment, yet the install step inherits neither: a job's env reaches no other job.
  {
    printf '%s\n' 'jobs:'
    printf '%s\n' '  build:'
    printf '%s\n' '    env:'
    printf '%s\n' '      TAPSTATE_TELEMETRY_URL: http://127.0.0.1:1/e'
    printf '%s\n' '      TAPSTATE_TELEMETRY_CHANNEL: internal'
    printf '%s\n' '    steps:'
    printf '%s\n' '      - run: echo nothing is installed here'
    printf '%s\n' '  install:'
    printf '%s\n' '    steps:'
    printf '%s\n' '      - run: curl -sSL https://install.tapstate.dev/cli | sh'
  } > "$d/wrong-job.yml"
  # The same two jobs with the fence moved onto the one that installs, and a lone step that carries
  # its own. Each install inherits the fence where it runs, so neither may be refused.
  {
    printf '%s\n' 'jobs:'
    printf '%s\n' '  build:'
    printf '%s\n' '    steps:'
    printf '%s\n' '      - run: echo nothing is installed here'
    printf '%s\n' '  install:'
    printf '%s\n' '    env:'
    printf '%s\n' '      TAPSTATE_TELEMETRY_URL: http://127.0.0.1:1/e'
    printf '%s\n' '      TAPSTATE_TELEMETRY_CHANNEL: internal'
    printf '%s\n' '    steps:'
    printf '%s\n' '      - run: curl -sSL https://install.tapstate.dev/cli | sh'
  } > "$d/job-fence.yml"
  {
    printf '%s\n' 'jobs:'
    printf '%s\n' '  install:'
    printf '%s\n' '    steps:'
    printf '%s\n' '      - run: curl -sSL https://install.tapstate.dev/cli | sh'
    printf '%s\n' '        env:'
    printf '%s\n' '          TAPSTATE_TELEMETRY_URL: http://127.0.0.1:1/e'
    printf '%s\n' '          TAPSTATE_TELEMETRY_CHANNEL: internal'
  } > "$d/step-fence.yml"
  # Fenced for the whole workflow, and the install step sets a channel of its own over it. The nearer
  # value is the one the installer reads, so a check that asked whether any level carries the word
  # would pass an install that arrives as a person's.
  {
    printf '%s\n' 'env:'
    printf '%s\n' '  TAPSTATE_TELEMETRY_URL: http://127.0.0.1:1/e'
    printf '%s\n' '  TAPSTATE_TELEMETRY_CHANNEL: internal'
    printf '%s\n' 'jobs:'
    printf '%s\n' '  install:'
    printf '%s\n' '    steps:'
    printf '%s\n' '      - run: curl -sSL https://install.tapstate.dev/cli | sh'
    printf '%s\n' '        env:'
    printf '%s\n' '          TAPSTATE_TELEMETRY_CHANNEL: community'
  } > "$d/overridden.yml"
  # The endpoint named with no value. The installer reads a blank override as unset and sends to
  # production, so the name alone is no override.
  {
    printf '%s\n' 'env:'
    printf '%s\n' '  TAPSTATE_TELEMETRY_URL:'
    printf '%s\n' '  TAPSTATE_TELEMETRY_CHANNEL: internal'
    printf '%s\n' 'jobs:'
    printf '%s\n' '  install:'
    printf '%s\n' '    steps:'
    printf '%s\n' '      - run: curl -sSL https://install.tapstate.dev/cli | sh'
  } > "$d/blank-url.yml"
  # Fenced for the whole workflow, and the install handed as an input to a reusable workflow. The
  # called workflow runs with its own env only; the caller's reaches none of it.
  {
    printf '%s\n' 'env:'
    printf '%s\n' '  TAPSTATE_TELEMETRY_URL: http://127.0.0.1:1/e'
    printf '%s\n' '  TAPSTATE_TELEMETRY_CHANNEL: internal'
    printf '%s\n' 'jobs:'
    printf '%s\n' '  install:'
    printf '%s\n' '    uses: ./.github/workflows/run.yml'
    printf '%s\n' '    with:'
    printf '%s\n' "      how: 'curl -sSL https://install.tapstate.dev/cli | sh'"
  } > "$d/reusable.yml"
  # A step whose value ends in a backslash, then a step that installs. They are separate commands; a
  # continuation joined across them glues the install onto `echo x`, where the pattern cannot see it.
  {
    printf '%s\n' 'jobs:'
    printf '%s\n' '  install:'
    printf '%s\n' '    steps:'
    printf '      - run: echo x\\\n'
    printf '%s\n' '      - run: ./install/install.sh'
  } > "$d/dangling.yml"
  # The command comes from the matrix, and the step only names it. A scan that read `run:` values
  # alone would find nothing here; the job's own place carries it.
  {
    printf '%s\n' 'jobs:'
    printf '%s\n' '  install:'
    printf '%s\n' '    strategy:'
    printf '%s\n' '      matrix:'
    printf '%s\n' "        how: ['curl -sSL https://install.tapstate.dev/cli | sh']"
    printf '%s\n' '    steps:'
    # shellcheck disable=SC2016  # the workflow's own expression, left for the runner to expand
    printf '%s\n' '      - run: ${{ matrix.how }}'
  } > "$d/matrix.yml"

  installs "$d/unfenced.yml" || die_detector "the scan did not recognise a published one-liner piped into a shell."
  installs "$d/fenced.yml"   || die_detector "the scan did not recognise the one-liner in the fenced control."
  installs "$d/mentions.yml" && die_detector "the scan treated a mention of the install domain as an install."
  installs "$d/commented.yml" && die_detector "the scan treated a commented-out one-liner as an install."
  installs "$d/linted.yml"    && die_detector "the scan treated a lint invocation as an install."
  installs "$d/continued.yml" || die_detector "the scan did not join a backslash-continued installer pipeline."
  installs "$d/chained-lint.yml" || die_detector "the scan lost an install chained after a lint command."
  installs "$d/direct.yml"    || die_detector "the scan did not recognise an executable installer run by path."
  installs "$d/named.yml"     && die_detector "the scan treated a path filter naming the script as an install."

  report="$(unfenced_installs "$d/unfenced.yml" "$d/fenced.yml" "$d/suffix-fence.yml" "$d/channel-only.yml" \
    "$d/commented-fence.yml" "$d/wrong-job.yml" "$d/job-fence.yml" "$d/step-fence.yml" "$d/overridden.yml" \
    "$d/blank-url.yml" "$d/reusable.yml" "$d/dangling.yml" "$d/matrix.yml")" \
    || die_detector "the scan could not read its own controls as workflows; it needs python3 with PyYAML."
  names "$report" "$d/unfenced.yml"        || die_detector "the scan called an unfenced control fenced."
  names "$report" "$d/fenced.yml"          && die_detector "the scan did not accept a control carrying both variables."
  names "$report" "$d/suffix-fence.yml"    || die_detector "the scan accepted a channel value the installer does not treat as ours."
  names "$report" "$d/channel-only.yml"    || die_detector "the scan accepted a channel with no endpoint override beside it."
  names "$report" "$d/commented-fence.yml" || die_detector "the scan accepted a fence that exists only in a comment."
  names "$report" "$d/wrong-job.yml"       || die_detector "the scan accepted a fence on a job that does not install."
  names "$report" "$d/job-fence.yml"       && die_detector "the scan did not accept a fence on the job that installs."
  names "$report" "$d/step-fence.yml"      && die_detector "the scan did not accept a fence on the install step itself."
  names "$report" "$d/overridden.yml"      || die_detector "the scan accepted a fence the install step overrides."
  names "$report" "$d/blank-url.yml"       || die_detector "the scan accepted an endpoint override with no value."
  names "$report" "$d/reusable.yml"        || die_detector "the scan let a caller's env fence a reusable workflow it calls."
  names "$report" "$d/dangling.yml"        || die_detector "the scan joined one step's trailing continuation onto the next step's install."
  names "$report" "$d/matrix.yml"          || die_detector "the scan lost an install carried by a matrix value rather than a run: line."
  rm -rf "$d"
}

detector_alive

workflows=()
while IFS= read -r f; do
  [ "$f" = "$SELF" ] && continue
  workflows+=("$f")
done < <(git ls-files '.github/workflows/*.yml' '.github/workflows/*.yaml')

unfenced=""
if [ "${#workflows[@]}" -gt 0 ]; then
  unfenced="$(unfenced_installs "${workflows[@]}")" || {
    echo "::error::the workflows could not be read as YAML, so where they install from is unknown."
    exit 1
  }
fi

if [ -n "$unfenced" ]; then
  echo "::error::installs that run without declaring the install as ours:"
  while IFS= read -r line; do printf '  %s\n' "$line"; done <<<"$unfenced"
  echo
  echo "Declare both where the install runs -- in the workflow's env, which every job inherits, or in the"
  echo "env of the job or step that installs -- so a forgotten override arrives marked instead of silently"
  echo "joining the figure the funnel divides by:"
  echo "  TAPSTATE_TELEMETRY_URL: http://127.0.0.1:1/e"
  echo "  TAPSTATE_TELEMETRY_CHANNEL: internal"
  exit 1
fi
echo "clean: every install in a workflow runs where it is declared as ours."
