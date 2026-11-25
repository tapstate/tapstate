# Configured Maven execution observations

`ci-build-observation.sh` compiles a bounded core extension in an invocation-owned temporary directory and runs the supplied complete `-DskipTests install` command. It adds one extension argument; the original reactor, install goals, test compilation, test JARs and packaging hooks remain in place. Observer compilation and collection are part of the preparation step's cost.

The worker and aggregate build use their existing jobs and input artifacts. Compiled observer classes, its JAR and the raw event JSON stay outside the checkout, Maven repository and input-artifact directory. Temporary compiled files are removed after the command. The hash-only summary is embedded in the existing input JSON; workers reuse the same immutable summary in pre- and post-test capture.

The summary records configured implementation names, effective-model hashes, active-profile identities, registered source-root identities, available artifact and plugin-realm file hashes, the Maven JVM's major-file identities, execution counts and event ordering. The configured listener runs after parameter injection and before the original Mojo. It never calls the Mojo or changes its parameters. Configuration trees and serialized metadata are bounded; observer errors retain partial evidence without changing plugin execution.

`ci-receipt.sh inputs --maven-observation /path/to/summary.json` validates source/tree, run/attempt/event and repository/workflow/job/shard declarations against that snapshot. The observer source digest is bound to tracked source bytes. Worker pre/post summaries must match, and the archive reader replays the same checks from the original input ZIP members. Role observations are kept separately because matrix workers and aggregate builds have different scoped executions. All-legacy absence retains the old comparator shape; mixed current and legacy peers reject.

Declared source/run scope is not independent proof of every consumed byte. Registered source roots do not prove compiler consumption. Fully injected parameter values, selected or forked compiler, annotation processors, external generators and whole JDK installation remain unverified. Missing nonregular-file identities remain explicit unknowns. The original raw event-byte hash is retained as a declaration; only the parsed metadata's canonical hash can be replayed from the summary. Whole-build provenance, capability, PR-first-sample and performance acceptance are never inferred from these facts.

Current input recording can derive a hash-only `resolved_maven_artifacts` field
from these existing observations when its source helpers are tracked. It matches
each original consumer POM identity and resolved default JAR/aggregate-POM record to the ordered
classpath's coordinates and bytes, retaining scopes and unresolved cases. Archive
readback repeats this derivation from the original metadata and source files. This
adds no observer event, Maven invocation or raw dependency upload; it does not
claim embedded producer identity, actual consumption or whole provenance.

For local use, create a task-owned work directory outside the checkout and dependency repository, then pass it as `--work-dir` with a new `--output` file and the explicit `--shard-id`. Pass the unchanged Maven command after `--`. The wrapper refuses narrowed reactor commands, extension overrides, dirty tracked source, reused output and incomplete or inconsistent event observations.

`ci-build-observation-smoke.sh` exercises scoped producer/consumer validation and original Git/ZIP archive readback. Actual full-product and Linux runner behavior must be recorded separately; offline controls do not establish CI or performance acceptance.

New native observations use metadata schema 4. Every observed Maven artifact
retains its exact `version` and records `base_version` directly from the same
Artifact API object. This distinguishes a timestamped resolved SNAPSHOT from
its repository/model base coordinates without deriving that association from
a filename, timestamp spelling or POM. Schema 3 archives retain their original
shape and exact-version rules; missing historical base facts are never added.

Resolved dependency capture uses the recorded base coordinates in schema 4 and
retains the exact recorded `resolved_versions` in each used dependency fact. Conflicting
versions, wrong base coordinates, missing consumer records or different file
bytes remain unresolved. A concrete version difference remains an input
difference even when the bytes match. This does not establish an effective
dependency model, archive producer identity or actual class consumption.
