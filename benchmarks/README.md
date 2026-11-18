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

The catalog records the source revision and whether the benchmark tree has local changes.
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
windows. The source reports contain separate per-stream resume boundaries, operation
counts, and ordering digests. Retain reports from incomplete phases and inspect their
submitted, acknowledged and observed counts before using them for measurement.

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
case uses the row option for dimensions and seeds ten times as many facts. Both
paired cases report selected hotspot keys against a separately seeded uniform arm.

The operator suite retains blocked cases. Scripts with the mixed-type payload need
MongoDB Decimal128 metadata at the target, and the Union branch case needs separate
output bindings. Catalog JSON exposes these reasons and execution checks them
before starting a selected batch.
