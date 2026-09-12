#!/usr/bin/env bash
# Cases for the connector build, run against a scratch checkout with a stub Maven on PATH.
#
# Two lanes depend on this script and they want different things from it: the nightly witness lane
# wants the handful of connectors its scenarios drive, a catalog refresh wants every connector the
# catalog carries. That made the module list an input, and an input has a default - which is the one
# thing here that can regress without anybody noticing, because the lane that would notice runs at
# night and reports on connectors rather than on which connectors it built. So the default list is
# pinned by name and by order below.
#
# The rest of the cases are the staging rules the witnesses depend on: exactly one jar per connector,
# the shaded one and not the thin one. Both were already load-bearing before the list became an input;
# neither had a case.
#
# Both halves of every answer are checked - the exit code and the reason given.
#
# Run it from anywhere. Exits 0 if every case holds.
set -uo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
builder="$here/build-real-connectors.sh"
scratch="$(mktemp -d)"
trap 'rm -rf "$scratch"' EXIT
passed=0
failed=0

# A stub Maven that stages whatever the real build would have produced for the modules it was asked
# for: one thin jar and one shaded jar per module, which is the pair the staging rule has to tell
# apart. It also records its own arguments, so a case can pin what was actually asked to be built.
make_shim() {
  local shim="$scratch/shim"
  rm -rf "$shim"; mkdir -p "$shim"
  cat > "$shim/mvn" <<'STUB'
#!/usr/bin/env bash
set -u
checkout=""; modules=""; settings=""
next=""
for arg in "$@"; do
  case "$next" in f) checkout="$(dirname "$arg")" ;; pl) modules="$arg" ;; s) settings="$arg" ;; esac
  next=""
  case "$arg" in -f) next=f ;; -pl) next=pl ;; -s) next=s ;; esac
done
case "$modules" in connectors-common/*) ;; *) echo "$modules" >> "$SMOKE_MODULES_SEEN" ;; esac
if [ -n "${SMOKE_BUILD_TRACE:-}" ]; then
  repository=""
  for arg in "$@"; do
    case "$arg" in -Dmaven.repo.local=*) repository="${arg#*=}" ;; esac
  done
  printf '%s\t%s\t%s\t%s\n' "$checkout" "$modules" "${!#}" "$repository" >> "$SMOKE_BUILD_TRACE"
fi
echo "${JAVA_HOME:-}" > "$SMOKE_JAVA_HOME_SEEN"
echo "$settings" > "${SMOKE_SETTINGS_SEEN:-/dev/null}"
case " $* " in *" clean "*) cleans=yes ;; *) cleans=no ;; esac
IFS=',' read -r -a mods <<< "$modules"
for module in "${mods[@]}"; do
  artifact="$(basename "$module")"
  mkdir -p "$checkout/$module/target"
  # Real Maven removes target/ on clean, which is what keeps one run's jars out of the next one's.
  [ "$cleans" = yes ] && rm -f "$checkout/$module/target"/*.jar
  [ "${SMOKE_NO_JAR:-no}" = yes ] && continue
  : > "$checkout/$module/target/$artifact-1.0.0.jar"        # the thin jar, which must not be staged
  : > "$checkout/$module/target/$artifact-v1.0.0.jar"       # the shaded jar, which must
  if [ "${SMOKE_TWO_SHADED:-no}" = yes ]; then
    : > "$checkout/$module/target/$artifact-v1.0.1.jar"
  fi
done
exit 0
STUB
  chmod +x "$shim/mvn"
  # On Apple Silicon the script probes whether the pinned protoc publishes an arm64 build before it
  # builds anything. Answering yes here keeps every case off the network and takes the same branch a
  # Linux runner takes by skipping the block entirely, so both hosts run the same cases.
  printf '#!/usr/bin/env bash\nexit 0\n' > "$shim/curl"
  chmod +x "$shim/curl"
  printf '%s' "$shim"
}

# A checkout with the three default modules plus one extra, so a case can ask for something outside
# the default list and be seen to get it.
fresh_checkout() {
  rm -rf "${scratch:?}/checkout" "${scratch:?}/enterprise" "${scratch:?}/dest"
  mkdir -p "$scratch/enterprise/connectors/oracle-connector" "$scratch/enterprise/connectors/mssql-connector"
  : > "$scratch/enterprise/pom.xml"
  for module in mysql-connector mongodb-connector postgres-connector redis-connector; do
    mkdir -p "$scratch/checkout/connectors/$module/src/main/resources"
  done
  mkdir -p "$scratch/checkout/connectors-common/debezium-bucket/debezium-bom"
  : > "$scratch/checkout/pom.xml"
  printf '<project><properties><version.com.google.protobuf>3.17.3</version.com.google.protobuf></properties></project>\n' \
      > "$scratch/checkout/connectors-common/debezium-bucket/debezium-bom/pom.xml"
}

expect() {
  local name="$1" want_code="$2" want_text="$3"; shift 3
  local -a env_pairs=()
  while [ "$#" -gt 0 ] && [[ "$1" == *=* && "$1" != -* && "$1" != /* ]]; do env_pairs+=("$1"); shift; done
  local shim out code
  shim="$(make_shim)"
  : > "$scratch/modules-seen"
  : > "$scratch/java-home-seen"
  : > "$scratch/settings-seen"
  out="$(env PATH="$shim:$PATH" SMOKE_MODULES_SEEN="$scratch/modules-seen" \
      SMOKE_JAVA_HOME_SEEN="$scratch/java-home-seen" \
      SMOKE_SETTINGS_SEEN="$scratch/settings-seen" \
      "${env_pairs[@]}" bash "$builder" "$@" 2>&1)"
  code=$?
  if [ "$code" = "$want_code" ] && printf '%s' "$out" | grep -qF -- "$want_text"; then
    printf '  ok    %s\n' "$name"
    passed=$((passed + 1))
  else
    printf '  FAIL  %s: wanted exit %s containing "%s", got exit %s:\n' \
      "$name" "$want_code" "$want_text" "$code"
    printf '%s\n' "$out" | sed 's/^/        /'
    failed=$((failed + 1))
  fi
}

# Pins what the build was actually asked to produce, which no message on stdout reports.
expect_modules() {
  local name="$1" want="$2"
  local seen; seen="$(cat "$scratch/modules-seen" 2>/dev/null)"
  if [ "$seen" = "$want" ]; then
    printf '  ok    %s\n' "$name"
    passed=$((passed + 1))
  else
    printf '  FAIL  %s: wanted modules "%s", got "%s"\n' "$name" "$want" "$seen"
    failed=$((failed + 1))
  fi
}

# A JDK that answers -version and nothing else. The guard reads a major version and compares it; it
# never runs one, so a real installation would only make the cases depend on what this host happens
# to have.
make_jdk() {
  local dir="$scratch/$1"
  rm -rf "$dir"; mkdir -p "$dir/bin"
  printf '#!/usr/bin/env bash\necho "javac %s"\n' "$2" > "$dir/bin/javac"
  chmod +x "$dir/bin/javac"
  printf '%s' "$dir"
}

# Pins which JDK the build was actually handed, which no message on stdout reports either. The whole
# point of the guard is that the connector build runs on a different JDK from the one this repository
# builds under, and an inherited JAVA_HOME looks identical to a chosen one from the outside.
expect_java_home() {
  local name="$1" want="$2"
  local seen; seen="$(cat "$scratch/java-home-seen" 2>/dev/null)"
  if [ "$seen" = "$want" ]; then
    printf '  ok    %s\n' "$name"
    passed=$((passed + 1))
  else
    printf '  FAIL  %s: wanted JAVA_HOME "%s", got "%s"\n' "$name" "$want" "$seen"
    failed=$((failed + 1))
  fi
}

# Pins which settings file the connector build was handed, which no message on stdout reports. The
# connectors resolve io.confluent, published to no repository their own pom names that a build off
# the upstream project's network can reach; this repository checks one in. Whether it arrives is
# invisible from the outside until it does not, and then it is a read timeout against a host that
# will never answer, ninety seconds in, naming neither the artifact nor the missing repository.
expect_settings() {
  local name="$1" want="$2"
  local seen; seen="$(cat "$scratch/settings-seen" 2>/dev/null)"
  if [ "$seen" = "$want" ]; then
    printf '  ok    %s\n' "$name"
    passed=$((passed + 1))
  else
    printf '  FAIL  %s: wanted settings "%s", got "%s"\n' "$name" "$want" "$seen"
    failed=$((failed + 1))
  fi
}

# --help prints the header block, and the block is delimited by where the comments stop rather than by
# a line number - a range drifts silently the first time anyone adds a paragraph, and what leaks out is
# shell, printed at whoever asked for help without anything going red.
help_prints_no_shell() {
  local name="$1"; shift
  local out; out="$("$@" --help 2>&1)"
  local leaked; leaked="$(printf '%s\n' "$out" | grep -cE '^(set -|readonly |[a-z_]+=|if |for )' )"
  if [ "$leaked" = 0 ] && printf '%s' "$out" | grep -q .; then
    printf '  ok    %s\n' "$name"
    passed=$((passed + 1))
  else
    printf '  FAIL  %s: %s line(s) of shell leaked into --help\n' "$name" "$leaked"
    failed=$((failed + 1))
  fi
}

echo "build-real-connectors cases:"

fresh_checkout
expect "no destination named" 1 "usage: build-real-connectors.sh"

fresh_checkout
expect "a module entry with no id" 2 "<id>=<module-path>" \
  --modules "connectors/mysql-connector" --checkout "$scratch/checkout" "$scratch/dest"

fresh_checkout
expect "a checkout that is not one" 2 "no connectors/ directory" \
  --modules "mysql=connectors/mysql-connector" --checkout "$scratch/checkout/connectors" "$scratch/dest"

# The default is what the nightly witness lane gets by passing no list at all. Pinned by name and by
# order: this is the one input whose regression the lane that uses it cannot report.
fresh_checkout
expect "the default list still builds the witness lane's connectors" 0 "Connector jars staged" \
  --checkout "$scratch/checkout" --checkout "$scratch/enterprise" "$scratch/dest"
expect_modules "the default list, by name and in order" \
  $'connectors/mysql-connector,connectors/mongodb-connector,connectors/postgres-connector\nconnectors/oracle-connector,connectors/mssql-connector'

fresh_checkout
expect "a named list is built instead of the default" 0 "Connector jars staged" \
  --modules "redis=connectors/redis-connector" --checkout "$scratch/checkout" "$scratch/dest"
expect_modules "only what was named" "connectors/redis-connector"

fresh_checkout
expect "an existing checkout is not cloned over" 0 "Building from the existing checkout" \
  --modules "mysql=connectors/mysql-connector" --checkout "$scratch/checkout" "$scratch/dest"

# The staging rules the witnesses and the catalog both resolve by.
fresh_checkout
expect "a module that built no jar" 1 "found 0" \
  SMOKE_NO_JAR=yes --modules "mysql=connectors/mysql-connector" --checkout "$scratch/checkout" "$scratch/dest"

fresh_checkout
expect "two shaded jars for one module" 1 "found 2" \
  SMOKE_TWO_SHADED=yes --modules "mysql=connectors/mysql-connector" --checkout "$scratch/checkout" "$scratch/dest"

# The thin jar shares the module's name and is not loadable as a connector. Staging both would make
# the destination ambiguous, which the witnesses refuse - so only the shaded one may land.
fresh_checkout
if out="$(env PATH="$(make_shim):$PATH" SMOKE_MODULES_SEEN="$scratch/modules-seen" \
    SMOKE_JAVA_HOME_SEEN="$scratch/java-home-seen" \
    bash "$builder" --modules "mysql=connectors/mysql-connector" --checkout "$scratch/checkout" \
    "$scratch/dest" 2>&1)"; then
  staged="$(find "$scratch/dest" -name '*.jar' -type f | wc -l | tr -d ' ')"
  shaded="$(find "$scratch/dest" -name 'mysql-connector-v*.jar' -type f | wc -l | tr -d ' ')"
  if [ "$staged" = 1 ] && [ "$shaded" = 1 ]; then
    printf '  ok    %s\n' "only the shaded jar is staged"
    passed=$((passed + 1))
  else
    printf '  FAIL  %s: staged %s jars, %s of them shaded\n' "only the shaded jar is staged" "$staged" "$shaded"
    failed=$((failed + 1))
  fi
else
  printf '  FAIL  %s: the build refused:\n' "only the shaded jar is staged"
  printf '%s\n' "$out" | sed 's/^/        /'
  failed=$((failed + 1))
fi

# Two consumers resolve this directory by two different rules, and only one of them was asserted.
# catalog-derive resolves a jar by its module name; the witnesses resolve by connector id. Those
# coincide for the three connectors the witness lane drives and diverge for most of the catalog -
# 'aliyun-db-mongodb' is built by aliyun-mongodb-connector - so a catalog refresh staged all of its
# jars and was then told the directory resolved none of them.
fresh_checkout
mkdir -p "$scratch/checkout/connectors/aliyun-mongodb-connector/src/main/resources"
expect "an id that is not its module's file prefix" 0 "Connector jars staged" \
  --modules "aliyun-db-mongodb=connectors/aliyun-mongodb-connector" \
  --checkout "$scratch/checkout" --checkout "$scratch/enterprise" "$scratch/dest"

# The public SQL Server id differs from the upstream Maven artifact.
fresh_checkout
expect "SQL Server stages by connector id" 0 "Staged sqlserver-connector-v1.0.0.jar" \
  --modules "sqlserver=connectors/mssql-connector" \
  --checkout "$scratch/checkout" --checkout "$scratch/enterprise" "$scratch/dest"

# The other rule still has to hold for the lane that uses it: the witnesses take the default list and
# resolve by id, so a sibling sharing an id's prefix makes their lookup ambiguous.
fresh_checkout
mkdir -p "$scratch/dest"
: > "$scratch/dest/mysql-pxc-connector-v1.0.0.jar"
expect "a sibling jar makes the witnesses' id lookup ambiguous" 1 "require exactly one" \
  --checkout "$scratch/checkout" --checkout "$scratch/enterprise" "$scratch/dest"

# The destination is not cleared, so a jar left there by an earlier run with a different version can
# sit beside the one just staged - and derive's rule fails loud on two, which is a refusal this
# script should give rather than pass on.
fresh_checkout
mkdir -p "$scratch/dest"
: > "$scratch/dest/mysql-connector-v0.9.0-old.jar"
expect "a stale jar for the same module in the destination" 1 "derive requires exactly one" \
  --modules "mysql=connectors/mysql-connector" --checkout "$scratch/checkout" "$scratch/dest"

# A refresh runs against a checkout somebody already built in, and upstream stamps a build timestamp
# into each shaded jar's name - so a second run leaves two of them side by side and the staging rule,
# which requires exactly one, refuses. Worse than the refusal: two jars in a tree built from two
# upstream revisions are two different connectors, and provenance is stamped per revision.
fresh_checkout
mkdir -p "$scratch/checkout/connectors/mysql-connector/target"
: > "$scratch/checkout/connectors/mysql-connector/target/mysql-connector-v1.0-SNAPSHOT-201001010000.jar"
expect "a jar left by an earlier run does not become a second shaded jar" 0 "Connector jars staged" \
  --modules "mysql=connectors/mysql-connector" --checkout "$scratch/checkout" "$scratch/dest"

# The JDK the connector build compiles with. Several connectors pin a Lombok that JDK 21 breaks, and
# the failure lands 37 modules into a 93-module reactor as a javac NoSuchFieldError - so what is
# pinned here is that the choice happens at all, before anything is built.
jdk17="$(make_jdk jdk17 17.0.12)"
jdk21="$(make_jdk jdk21 21.0.11)"

fresh_checkout
expect "an explicitly named JDK is used" 0 "Connector jars staged" \
  TAPSTATE_CONNECTOR_JAVA_HOME="$jdk17" JAVA_HOME="$jdk21" \
  --modules "mysql=connectors/mysql-connector" --checkout "$scratch/checkout" "$scratch/dest"
expect_java_home "the named JDK, not the ambient one" "$jdk17"

# The ambient JDK is fine when it is old enough, and this is the case that keeps the nightly lane and
# the tutorial working unchanged: they name no JDK and never will.
fresh_checkout
expect "an ambient JDK old enough is left alone" 0 "Connector jars staged" \
  JAVA_HOME="$jdk17" JAVA_HOME_17_X64= JAVA_HOME_17_ARM64= \
  --modules "mysql=connectors/mysql-connector" --checkout "$scratch/checkout" "$scratch/dest"
expect_java_home "the ambient JDK" "$jdk17"

fresh_checkout
expect "a JDK too new for the pinned Lombok is refused" 2 "Lombok" \
  JAVA_HOME="$jdk21" JAVA_HOME_17_X64= JAVA_HOME_17_ARM64= \
  --modules "mysql=connectors/mysql-connector" --checkout "$scratch/checkout" "$scratch/dest"

# The repository's own settings file, supplied by default rather than by an environment variable a
# single workflow happened to set. That arrangement built fine on the runner and nowhere else, which
# is the wrong way round: this script is meant to be run by a developer on their own machine.
repo_settings="$(cd "$(dirname "$builder")/.." && pwd)/.github/maven-settings-connectors.xml"

fresh_checkout
expect "the checked-in settings is supplied without being asked for" 0 "Connector jars staged" \
  --modules "mysql=connectors/mysql-connector" --checkout "$scratch/checkout" "$scratch/dest"
expect_settings "the connectors' settings, by default" "$repo_settings"

# A caller who named one keeps it. Two -s on one command line is a precedence nobody here has
# established, and the way to find out is not to guess on a build that takes an hour.
fresh_checkout
expect "a caller's own settings is not doubled" 0 "Connector jars staged" \
  "MAVEN_ARGS=-s $scratch/mine.xml" \
  --modules "mysql=connectors/mysql-connector" --checkout "$scratch/checkout" "$scratch/dest"
expect_settings "nothing added on top of the caller's" ""

# Missing, it refuses here and says so. The alternative is handing Maven a path to nothing, whose
# behaviour nobody here has established - and the one outcome that must not happen is the build
# proceeding without the repository and dying later on the timeout this whole arrangement exists to
# remove. Driven against a copy of the script whose settings file is absent, since the real one is
# checked in beside it.
rm -rf "$scratch/norepo"
mkdir -p "$scratch/norepo/scripts"
cp "$builder" "$scratch/norepo/scripts/"
fresh_checkout
if out="$(env PATH="$(make_shim):$PATH" SMOKE_MODULES_SEEN="$scratch/modules-seen" \
    SMOKE_JAVA_HOME_SEEN="$scratch/java-home-seen" \
    bash "$scratch/norepo/scripts/$(basename "$builder")" \
    --modules "mysql=connectors/mysql-connector" --checkout "$scratch/checkout" "$scratch/dest" 2>&1)"; then
  printf '  FAIL  %s: the build proceeded with no settings file\n' "a missing settings file is refused"
  failed=$((failed + 1))
elif printf '%s' "$out" | grep -qF "settings file is missing"; then
  printf '  ok    %s\n' "a missing settings file is refused"
  passed=$((passed + 1))
else
  printf '  FAIL  %s: refused for some other reason:\n' "a missing settings file is refused"
  printf '%s\n' "$out" | sed 's/^/        /'
  failed=$((failed + 1))
fi

# Cross-repository prerequisites are not available from remote Maven repositories.
# Require the OSS install before enterprise package and the same isolated repository.
fresh_checkout
mkdir -p "$scratch/enterprise/connectors/oracle-connector" "$scratch/enterprise/connectors/mssql-connector"
: > "$scratch/enterprise/pom.xml"
: > "$scratch/build-trace"
expect "both source reactors build into one dist" 0 "Staged sqlserver-connector-v1.0.0.jar" \
  "SMOKE_BUILD_TRACE=$scratch/build-trace" MAVEN_ARGS= \
  --modules "mysql=connectors/mysql-connector,oracle=connectors/oracle-connector,sqlserver=connectors/mssql-connector" \
  --checkout "$scratch/checkout" --checkout "$scratch/enterprise" "$scratch/dest"
if python3 - "$scratch" <<'CHECK'
from pathlib import Path
import sys
root = Path(sys.argv[1])
rows = [line.split('\t') for line in (root / 'build-trace').read_text().splitlines()]
assert [(Path(row[0]).name, row[1], row[2]) for row in rows] == [
    ('checkout', 'connectors/mysql-connector', 'package'),
    ('checkout', 'connectors-common/sql-core,connectors-common/read-partition', 'install'),
    ('enterprise', 'connectors/oracle-connector,connectors/mssql-connector', 'package'),
], rows
assert len({row[3] for row in rows}) == 1 and rows[0][3], rows
assert rows[0][3] != str(Path.home() / '.m2/repository'), rows
assert sorted(path.name for path in (root / 'dest').glob('*.jar')) == [
    'mysql-connector-v1.0.0.jar', 'oracle-connector-v1.0.0.jar', 'sqlserver-connector-v1.0.0.jar']
CHECK
then
  echo "  ok    prerequisites, build order, shared isolated repository and canonical jar ids"
  passed=$((passed + 1))
else
  echo "  FAIL  dual-source build order or provenance"
  failed=$((failed + 1))
fi

help_prints_no_shell "--help prints documentation, not source" bash "$builder"

echo
if [ "$failed" -gt 0 ]; then
  printf '%s passed, %s FAILED\n' "$passed" "$failed"
  exit 1
fi
printf '%s passed\n' "$passed"
