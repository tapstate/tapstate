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
are deliberate additional executions and are not automatic duplicate runs. They
include Sonar analysis and its quality gate when repository secrets are present;
there is no separate cheap branch-push lane. CI and manual PR analysis share a
Sonar concurrency group with cancellation disabled so in-flight server analyses
finish. A newer PR update can therefore wait for the active run.
Fork pull requests retain the same tests and explicit missing-secret Sonar
notice; manual trusted Sonar analysis remains a separate maintainer action.

The release workflow registers the current Cloud runtime version only after the
Cloud image publisher verifies the public registry digest. Configure the two
variables in the `cloud-ghcr-release` environment and the production token in
the repository's Actions secrets before using that notification:

- Variable `CLOUD_RELEASE_API_URL`: the HTTPS endpoint ending in
  `/v1/api/cluster-versions`.
- Variable `CLOUD_RELEASE_CODE`: `ALPHA`, `BETA`, `RC`, or `STABLE`.
- Repository secret `CLOUD_CLUSTER_VERSION_RELEASE_TOKEN_PROD`: the operator
  token configured on the production Cloud control plane. The release workflow
  reads only this production key; `CLOUD_CLUSTER_VERSION_RELEASE_TOKEN_DEV`
  is reserved for other CI workflows.

The request carries the version and source revision; `git.extra` records the
image tag, immutable image reference, digest, release URL, and workflow URL.
Cloud provisioning continues to select the image by its version tag. Publication
and registration share a cross-version concurrency queue so a delayed older
insert cannot overtake the newer version's registration. Older
release lines and releases already overtaken when the notification checks
GitHub's latest marker are skipped. HTTP errors fail the notification visibly;
the image and release have already been published at that point. The API is
not idempotent: a timeout may have committed, and a 409 requires reconciling the
existing catalog entry before retrying. The notification never retries a POST
or treats a 409 as success without comparing the existing entry.
The helper is loaded from the workflow's commit in a separate sparse checkout,
so selecting an earlier product commit does not lose the notification tooling.
The approved image and its source revision still come from the selected release.

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

Artifact admission runs once in `build`, before coverage merging and analysis.
Shard artifacts carry reports, declared coverage data, and only their own
modules' test bytecode. Production bytecode comes from the aggregate build.
Source discovery uses Maven's default test naming patterns and rejects custom
Surefire/Failsafe includes or excludes (including file-based, execution, managed,
and profile configuration) rather than silently omitting tests.
