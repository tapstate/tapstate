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
