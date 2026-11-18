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
