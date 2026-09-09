# CI lane placement and cost review

Maintenance contract: every pull request answers the **CI cost delta and lane
placement** line in the pull request template. The author measures the delta and
explains the placement; the reviewer assesses the cost. `catalog-scripts` runs
`one-verify.sh` and its smoke cases against the actual workflows. Changes to the
trigger policy or test ownership must retain those regressions.

`ci.yml` owns one complete reactor test suite, distributed over its existing
shards and admitted by the `build` aggregate. Automatic feature-branch testing
runs on pull request creation and updates. `push` tests the integration branch
`main` and release tags `v*`; it does not also run the suite for a feature branch.
This avoids a second suite both when a pull request already exists and when it
is opened after a branch push. Before opening a pull request, explicitly run
`gh workflow run ci.yml --ref <branch>` when CI validation is needed. Manual runs
are deliberate additional executions and are not automatic duplicate runs.
Fork pull requests retain the same tests and explicit missing-secret Sonar
notice; manual trusted Sonar analysis remains a separate maintainer action.

Choose a location for new work before adding a workflow:

- Add ordinary unit, integration, or specification tests to the existing
  reactor. Source discovery assigns them to the existing shards; do not copy a
  full-reactor Maven test invocation into another automatic job or workflow.
- Use a separate path- or label-gated lane for a distinct, expensive capability
  whose evidence cannot come from the ordinary suite. Run only that capability's
  selected tests, and explain its trigger and additional runner cost in review.
- Put broad external-system, upstream-drift, and trend measurements in a
  scheduled lane, with manual dispatch for investigation and release consumers.
  Preserve a stable, non-matrix aggregate name when another workflow requires
  its check. New required checks must execute on every event that requires them;
  do not hide one behind a path or label filter.

Measure with the same tool for the current change and its baseline, for example:

```sh
.github/scripts/ci-timing.sh ci.yml --event pull_request --branch <branch> --runs 5 --json
```

Record the baseline and current run links, event, runner specification, sample
count, median wall clock, and signed delta in the pull request. Use comparable
successful first-attempt samples; this tool's wall clock includes queueing and
job dependencies. Identify a different runner, cache condition, or a rerun
instead of attributing that difference to the change. If fewer runs exist,
state the actual sample count; a missing measurement is not a zero delta.
For a new lane, also state its selected scope and why its placement fits above.

The timing delta is review evidence, never a fixed-duration pass/fail threshold.
The structural gate checks executable Maven lifecycle and shard-driver calls in
automatic push/PR workflows, rather than workflow comments or diagnostic text.
The existing shard and aggregate gates prove that the one logical suite covers
the entire discovered test set.

The analysis job caches only `~/.sonar/cache`, separately from Maven packages.
Every run reads the current server's plugin and scanner-engine indexes to key
the cache by their content hashes. A changed index can restore unchanged binaries
from the same server's older cache and save the updated cache under a new key.
The scanner still reads its own current indexes and downloads missing versions;
cache hits never skip report admission, analysis, or the quality gate. Cache
lookup failures fall back to ordinary analysis. `sonar-cache-smoke.sh` protects
the identity and workflow bindings, alongside the analysis-path regressions.
