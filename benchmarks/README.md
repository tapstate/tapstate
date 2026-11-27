# Synchronization performance case catalog

From the repository root, use JDK 21 and Python 3 to generate the catalog or inspect one case.

```sh
python3 benchmarks/catalog.py --format json
python3 benchmarks/catalog.py --format markdown
python3 benchmarks/catalog.py --format markdown --case nest.snapshot.basic.standard
```

The declarations in `cases/*Cases.java` give each case its stable ID, version, classification
and specification reference. Related profiles and measurement modes share a file in
`cases/specs/`. The shared profile sizes, generator inputs, measurement protocols and
calibration status are in `cases/contracts.json`. Keep the case version in step with
changes to its load, correctness rules or measurement boundary.

The catalog records the source revision and whether the checkout has local changes.
The definition digest binds the metadata and resolved workload specification. The catalog digest
identifies the full inventory, including when the output is narrowed to one case.
These definitions describe candidates for calibration. Connector qualification, workload execution
and measured results are recorded separately.

Run the offline catalog and annotation checks with:

```sh
python3 -B -m unittest discover -s benchmarks/src/test
```

## MongoDB source workload

The e2e test harness contains the MongoDB source generator. Pass one catalog row to
`WorkloadRecipe.fromCatalog`, together with the resolved row count per input and an
optional Nest fanout for a paired arm. The recipe maps the logical primary key to
MongoDB's `_id`. It keeps each input's key range disjoint.

Use `MongoWorkloadGenerator.calibrateStringLengths()` for a calibration candidate,
then pass the chosen lengths to `prepare()` so subsequent runs use the same lengths.
The caller supplies an isolated source database and owns its cleanup. Preparation
returns the stored BSON statistics and the time spent writing and reading back.

Each preparation supports one change workload. After establishing reader state, use
`prewrite()` for a fixed backlog or `inject()` for paced warm-up and measurement
windows. Live writes stop at the scheduled deadline. Each phase reports actual
submissions and acknowledgements, rates over the active write window, and whether
the last in-window submission was acknowledged after the deadline. Target drain
and readback use the verified source history. Fixed backlog execution verifies the
full planned batch. Per-stream resume boundaries, operation counts, and ordering
digests also remain available for incomplete phases.

The live source protocol is a baseline comparison condition. Slow-target arms
with different source sequences retain their results and are ineligible for
paired performance comparisons.

Run the bounded replica-set witness from the repository root:

```sh
mvn -pl e2e -am -Dtest=MongoWorkloadGeneratorIT -Dsurefire.failIfNoSpecifiedTests=false test
```

## Select and execute bounded qualification runs

The common runner reads the same versioned catalog for selection, allocation, and
execution. Repeat a filter for alternatives within a dimension. Filters across
different dimensions are combined.

```sh
python3 -B benchmarks/runner.py list --where operators=nest --limit 10
python3 -B benchmarks/runner.py describe copy.snapshot.basic.standard@1
mvn -pl e2e -am -DskipTests package
python3 -B deploy/cloud/prepare-test-inputs.py --mongodb-only --with-dummy --output /tmp/perf-connectors
python3 -B benchmarks/runner.py run --suite basic --connectors /tmp/perf-connectors --output /tmp/perf-run
```

The basic qualification suite includes Dummy snapshot and MongoDB Copy snapshot,
live, and backlog. Each repeat uses an isolated OP process and separate data and
state databases. Results include workload and connector identities, phase timings,
target readback, and diagnostic metrics. Output directories must be fresh.
The production latency histogram is retained as diagnostic evidence. Qualified
window latency requires independently correlated source events and target
acknowledgements on a comparable clock.

The snapshot handoff case uses an owned replica set shared by its source and
target. It holds target snapshot writes while updating an existing source key,
then disables the hold before the live workload. Its target change stream must
show the original snapshot row and the subsequent write for that key. The repeat
also requires one captured update, one additional target acknowledgement, and
exact final readback. Missing handoff evidence leaves the repeat incomplete.

For deterministic allocation, provide whole-case estimates (including preparation,
all repeats, verification, and cleanup) measured in the declared environment.

```json
{"environment":"local-op","durations_seconds":{"copy.snapshot.basic.standard":90}}
```

```sh
python3 -B benchmarks/runner.py allocate --id copy.snapshot.basic.standard \
  --slots 1 --batch-seconds 300 --deadline-seconds 360 --reserve-seconds 60 \
  --environment local-op --estimates /tmp/estimates.json --output /tmp/perf-manifest.json
python3 -B benchmarks/runner.py run --manifest /tmp/perf-manifest.json --batch 0 \
  --connectors /tmp/perf-connectors --output /tmp/perf-replay
```

The manifest binds the exact checkout, definitions and effective load. A missing
estimate or case that does not fit is recorded in the allocation and blocks replay.

## Formal workloads on hosted runners

Pipeline Performance provides a separate manual dispatch. Select a branch or tag
and choose all cases, one primary group, dimension filters, or exact case IDs.
Scheduled runs select all cases after the first scheduled Nightly Test attempt
on the default branch completes.

Formal recipes live in `cases/formal-workloads.json`. Each `ID@version` entry
contains a recipe version, the case definition SHA-256 and every effective load
field. Pass that file as `--formal-workloads` to `run` for a local calibration
or to `allocate` for hosted execution. Qualification commands keep their explicit
load options. Formal workloads use the catalog's repeat count and operation mix.

`cases/hosted-runner-estimates.json` binds each estimate to a formal workload
digest and the `ubuntu-24.04-x64-op` environment. Entries with an
`observed_whole_case_seconds` field come from a completed same-workload run,
including all repeats and arms. A `projection` identifies its hosted reference,
the bounded qualification estimate and the work scheduled at the formal scale.
Live projections budget the full warm-up and scoring windows for every repeat,
arm and lifecycle cycle before adding the larger of the observed hosted overhead
and the remaining qualification estimate. Snapshot and backlog projections use
comparable hosted input volumes and the qualification estimate as a floor. The
already observed elapsed time of a matching single repeat can raise a
projection. The hosted planning margin is 25% plus 60 seconds per case.

The preparation summary shows each selected case version, profile, repeat count,
paired-control requirement, estimate and batch. The manifest preserves the
individual estimates with the frozen case definitions and load. A selection with
missing or mismatched recipes or estimates publishes an incomplete allocation
and leaves every measurement job skipped.

Initial snapshot sizes are 100,000 rows for small and standard, 25,000 for wide
and 10,000 for large. Individual calibrated recipes can retain other input
sizes. Formal live workloads normally use 100 events per second with a 30-second
warm-up and a 300-second scoring window. The two-input Union live case uses a
hosted-calibrated rate of 50 events per second across both streams. The paired
hotspot cases use lower, versioned rates to preserve the hot-region allocation
when mutations sample existing keys without replacement. Lifecycle repeats
retain three cycles with shorter scoring windows and matching idle observations.

The report artifact `performance-report-<attempt>` includes `report.json` and
`report.md`. The input and batch artifact names also carry the attempt. Reproduce
a result with its case version, tested SHA, workload, and environment profile.
Use **Run workflow** to start a new run for a repeat measurement. **Re-run all
jobs** can remove the first run's artifacts before the workflow rejects the new
attempt. If the first report upload succeeded and the report is gone after a
rerun, the health check records its removal and leaves notification state
unchanged.

The scheduled health workflow runs from the default branch. It sends through
the `PERFORMANCE_FEISHU_WEBHOOK` repository Actions secret for the synchronization
performance bot. The group notification shows run-wide coverage and links to
the same Actions attempt for the full evidence.

Delivery state advances after the webhook confirms acceptance. A definite
rejection leaves the previous state available for the next scheduled check.
If a receipt is missing or ambiguous, the workflow records the source health
run and retains the pending intent so it does not send a possible duplicate.

### Released baselines

At preparation, the runner reads `baselines/releases.json` from a pinned commit
in the public `tapstate-performance` repository. The version 1 release record
has an `active` mapping from environment profile to version and a `versions`
mapping whose entries specify `id`, `environment`, `code_sha`, `commit`, `path`,
and the `sha256` of the baseline file. The file at that exact commit includes
the same ID, version, environment and measured code SHA, declares `kind: accepted`,
and contains the `cases` mapping. A publisher advances `active` after verifying
the published content. Manual dispatch can select an earlier published version.

The preparation artifact preserves the chosen release identity and content.
When a baseline is unavailable or invalid, the report retains coverage and
execution results and records why comparison was unavailable.

## Walk through a delivery candidate on local OP

Allocation and manifest replay require a clean candidate checkout, including the
fixture source outside the benchmark directory. Use JDK 21, Maven, Python 3 and a running
Docker daemon. The runner starts a fresh single-node OP and its MongoDB test
replica set. Use a host with enough memory for Docker and an OP JVM with a
768 MiB heap. Check Docker access before building or running the walkthrough.

```sh
docker info
git switch --detach "$CANDIDATE_SHA"
git status --short
git rev-parse HEAD
java -version
mvn -B -pl app,cli,e2e -am -DskipTests package
java -jar cli/target/cli-0.8.0.jar --version

evidence=$(mktemp -d "${TMPDIR:-/tmp}/tapstate-perf.XXXXXXXX")
python3 -B deploy/cloud/prepare-test-inputs.py --mongodb-only --with-dummy \
  --output "$evidence/connectors"
git rev-parse HEAD > "$evidence/revision.txt"
shasum -a 256 app/target/app-0.8.0-boot.jar cli/target/cli-0.8.0.jar \
  "$evidence/connectors/"*.jar > "$evidence/artifacts.sha256"
python3 -B benchmarks/runner.py list --format json > "$evidence/catalog.json"
```

The connector preparer downloads the published artifacts and checks them against
the lock in the same checkout. Keep the CLI's neighboring `target/lib` directory.
The benchmark runner drives OP through the control plane. The product CLI is a
separate executable from the same candidate.

Describe each representative case to record the version and effective definition,
then run the snapshot composition, live Copy, fixed backlog Copy and paired Nest
memory case. The bounded overrides keep the walkthrough short. Run each command
separately so a failure still leaves evidence and the other cases can run.

```sh
for case_id in mixed.snapshot.double-map.standard copy.live.basic.standard \
  copy.backlog.fixed-iud.standard nest.snapshot.fanout.wide; do
  python3 -B benchmarks/runner.py describe "$case_id" --format json \
    > "$evidence/$case_id.definition.json"
  python3 -B benchmarks/runner.py run --id "$case_id" --repeats 1 \
    --rows 96 --backlog-events 20 --rate 8 --measurement-seconds 6 \
    --timeout-seconds 90 --boot-jar app/target/app-0.8.0-boot.jar \
    --connectors "$evidence/connectors" --output "$evidence/$case_id"
done
```

Record the operator, date, OS, architecture, JDK, Docker and MongoDB version, and
retain the request and summary from each command. Check the keys and values produced by the two Map nodes, Filter and script.
Check live warm-up and window counts and final readback,
and backlog source history, zero snapshot rows on resume, confirmed writes and
final readback. Review `measurement_status` independently from execution `status`.
Live Copy may report `insufficient-event-correlation` and backlog Copy may
report `insufficient-source-end-evidence`. Record those limits, including when
target contents and counts agree. For Nest, check the identical root counts and
the four versus sixteen children per root, then inspect `op-resource.jsonl`,
`gc.log`, and the per-window coverage in both arms. Re-run the same command with
a fresh output directory to reproduce the window under the same definition.

## Operator cases

List the operator definitions with readiness and blockers in structured output.

```sh
python3 -B benchmarks/runner.py list --suite operators --format json
python3 -B benchmarks/runner.py run --id map.snapshot.basic.standard \
  --connectors /tmp/perf-connectors --output /tmp/perf-map
```

Each run saves the applied graph and compares the MongoDB target with an independent
expectation over the source rows. Union validates each destination collection and
the full disjoint key set. Unwind compares parent and element identities. Join and
Nest validate complete joined or assembled rows. CDC results record captured changes
for each input and settled target evidence separately.

The row option controls bounded fixture sizing. Union divides the input across its
two streams. Nest takes a child count that is divisible by four. The paired Join
case uses the row option for dimensions and seeds ten times as many facts.
The Join hot-update and Nest child-update cases compare hotspot selections with
separately seeded uniform arms. The Nest fanout snapshot holds the root count
fixed and compares ordinary and high children-per-root counts.

The operator suite covers supported pipeline shapes. The JS and double-map cases
check that MongoDB preserves untouched Decimal128 values alongside script output.

## Memory qualification

List or run the versioned memory selection with the same entry point. The bounded
example uses 96 rows, eight source events per second and six seconds of live
measurement. Use enough input keys for the matched Join change batch.

```sh
python3 -B benchmarks/runner.py list --suite memory --format json
python3 -B benchmarks/runner.py run --suite memory --repeats 3 --rows 96 \
  --rate 8 --measurement-seconds 6 --timeout-seconds 90 \
  --connectors /tmp/perf-connectors --output /tmp/perf-memory
```

- Wide and large Copy cases retain their own data profile. Compare each with
  the same Case ID, effective row count and operating conditions.
- Nest compares four and sixteen children per root with the same root count
  and calibrated string lengths. Join compares uniform and hot dimension keys
  with the same initial state and operation counts. The generator reports
  the actual hotspot selections after the distinct-key constraint.
- Slow-target Copy uses an owned test replica set for each arm with the same
  topology. Only the target connector's write commands have an application and
  namespace filter. The source-minus-target deficit compares the measured source
  events with target events visible at the majority frontier marker. The
  failpoint release, delayed commands, eventual acknowledgements, and target
  readback are recorded separately.
- Steady Join updates existing keys and reports three equal measurement
  subwindows and logical cardinality. Lifecycle Join stops three times within
  one JVM and observes the target collection during each stopped window. It
  reports durable positions before stop and after any observed restart, along
  with snapshot rows read by that run. Independent repeats start new JVMs.

The repeat results name each resource window with start, end, scope and coverage.
The OP samples use process RSS from procfs on Linux and ps on macOS. Natural JVM
GC pauses are retained in `gc.log`, with identifiable post-pause heap samples
listed by pause type. RSS reads that cross a phase boundary remain in the raw
samples and are excluded from per-phase statistics. The generator's CPU time, actual write rates and history
verification are separate. The outer run writes timestamped, owner-filtered
Docker samples to `mongo-resource.jsonl` for the shared MongoDB containers.
Use the repeat and phase timestamps when inspecting this file. MongoDB resources
are instance totals. Queue release, physical state size and host OOM attribution
require a supported signal. Missing observations and unfrozen thresholds remain
visible in the report.

For node timing and overhead experiments, replay the same manifest from its
bound checkout and pass each matched OP build explicitly with `--boot-jar`.
The manifest verifies the fixture checkout. Each run records the actual OP JAR
digest, so record the build revision, digest, and environment for each runtime
artifact.
Use `mixed.snapshot.double-map.standard` to distinguish the two Map nodes,
`copy.snapshot.basic.standard` for batch and event units, Nest and Join for
amplification and state, and `copy.live.slow-target.large` for target wait.
Keep the normal backend and MongoDB topology fixed within a pair.

## Reconcile and compare a batch run

Use one allocated manifest and include every available batch summary, including
summaries from runs that exited nonzero. A partial summary preserves completed
repeats and records repeats that did not run. JSON
contains raw repeat results, comparisons, and per-case and per-arm statistics
for eligible metrics. Each comparable set reports the median, minimum,
maximum, and range (maximum minus minimum). The optional Markdown view shows
correctness, missing runs, measurement eligibility and resource evidence.

```sh
python3 -B benchmarks/runner.py report --manifest /tmp/perf-manifest.json \
  --summary /tmp/perf-replay/summary.json --output /tmp/perf-report.json \
  --markdown /tmp/perf-report.md
```

Supply an explicit `--baseline /tmp/baseline.json` to enable comparison. The
baseline object has `id`, `kind` (`fixture` or `accepted`), `environment`
and `cases` keyed by Case ID. Case records carry the same condition and
measurement fields as the report rows, including paired arm records when
applicable. Fixture deltas exercise the report without implying accepted
performance. Completed repeats and their paired arms must carry the manifest's
environment profile to qualify for statistics or comparison. Machine attributes
remain separate comparison conditions. Raw summaries produced before profile
binding can be read with the report tool from their own checkout. Changes in
workload, definition, environment, connector artifact or resource source can
make observations incompatible. The report emits
unavailable results and exits nonzero for failed, invalid or missing repeats.

Provide whole-case duration estimates for all three repeats including startup,
preparation, measurement, verification and cleanup for both arms of any pair.
Allocate with a reserve for build, shared setup and report generation. Review
the longest estimated batch plus reserve against the suite deadline. Keep the
sizing and evidence for scaled estimates explicit when the inputs were bounded.

During manifest replay with a positive reserve, Maven must start the benchmark
fixture within the reserve. The fixture then has the manifest execution limit.
Timeout handling starts by reserve plus execution limit. Process-group teardown
can then wait up to 15 seconds after SIGTERM and another two seconds after
SIGKILL, followed by the resource sampler join and summary writing. A missing
or zero reserve retains the combined process limit used by earlier manifests.
