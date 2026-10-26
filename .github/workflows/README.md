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

## Real connector fixture inputs

The scheduled/manual Real connectors lane builds seven checked connector kinds,
including `mongodb-atlas` and `aws-rds-mysql`. Connector production uses Maven
3.9.16 with a verified distribution checksum. Its snapshot manifest, sealed JAR
set and witness admission must agree; adding unchecked files to a cache is not
a replacement for building the selected artifacts.

External managed Atlas acceptance is temporarily deferred under
[#702](https://github.com/tapstate/tapstate/issues/702). The exact four live classes
are listed in `.github/connector-witness-deferrals.json`; their unchanged tests
remain in the repository. Each plan records the policy bytes, deferred classes
and follow-up. Deferred cases are not execution or managed Atlas acceptance.
MongoDB witnesses, local replica-set Atlas PDK controls and all other selected
connector cases still require positive, zero-skip execution.

Before restoring the deferred classes, configure these repository Actions secrets:

- `TAPSTATE_ATLAS_TEST_URI`: a controlled real Atlas test deployment URI for
  managed-store, recovery and pipeline witnesses.
- `TAPSTATE_ATLAS_STANDARD_URI`: the standard MongoDB connection URI for the
  same controlled Atlas fixture, with the resolved hosts and connection options
  required by the source HTTP witness.

When the active source inventory includes external Atlas classes, the planning
job rejects missing inputs before connector production starts. Removing a class
from the deferral list restores its execution and this prerequisite automatically.
Each witness shard also stages the locked public Cloud release inputs and sets
`TAPSTATE_TEST_CLOUD_RELEASE_DIR`; this directory is prepared by CI, not supplied
as a secret. Zero-test and skipped witnesses remain admission failures.

Use a dedicated test deployment with room for the approximately 423 MB connector
release plus concurrent witness databases. A 512 MB shared deployment has
exhausted its quota during these tests. The lane does not provision or resize
an Atlas deployment.

## Cloud release version registration

The release workflow registers the current Cloud runtime version only after the
Cloud image publisher verifies the public registry digest. Configure the two
variables in the `cloud-ghcr-release` environment and the production token in
the repository's Actions secrets before using that notification:

- Variable `CLOUD_RELEASE_API_URL`: the HTTPS endpoint ending in
  `/v1/api/cluster-versions`.
- Variable `CLOUD_RELEASE_CODE`: `ALPHA`, `BETA`, `RC`, or `STABLE`.
- Repository secret `CLOUD_CLUSTER_VERSION_RELEASE_TOKEN_PROD`: the operator
  token configured on the production Cloud control plane. The release workflow
  production publication job reads only this production key;
  `CLOUD_CLUSTER_VERSION_RELEASE_TOKEN_DEV` is used by the dev validation job.

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

## Cloud cluster dev images

`release.yml` calls `cloud-cluster-dev.yml` after all release gates pass, using
the selected release version and frozen Tapstate and Web commits. Both Cloud
environments must allow the workflow's dispatch ref; the current default is
`main`, and `commit` can still select an older product commit. The dev job uses
the `cloud-ghcr-dev` configuration, verifies and pushes the image, and registers
it in the dev catalog before the existing `release` environment waits for human
approval. Validate the digest shown in the dev job summary, review the draft,
then choose **Approve and deploy** to continue production publication or
**Reject** to end the release. Rejection retires the draft and release branches;
the dev image and catalog entry remain available for diagnosis. The `release`
environment must retain its required reviewers.

The dev workflow also retains manual dispatch and accepts branch refs, not tags.
Dispatch it on `main` to build Tapstate and Web `main`, or on an allowlisted development
branch to build both repositories at the selected ref (the branch must exist in
both repositories). Select `minor`, `patch`, or `major`; the version calculator
uses the tagged `main` history so an unmerged feature branch does not need a
release tag ancestor. The selected ref still supplies the Tapstate and Web source.
Dev runs create no Git tags or releases. Main may replace the shared candidate
version's `-dev` tag; each feature build publishes a tag with the Tapstate source
SHA suffix for manual validation without changing main's image or the dev catalog.

| Lane | Image | Version tag policy |
| --- | --- | --- |
| Cloud dev from main | `ghcr.io/tapstate/tapstate-cloud:<version>-dev` | Replaced by another main dev build of that version |
| Cloud dev from a branch | `ghcr.io/tapstate/tapstate-cloud:<version>-dev-<tapstate-sha>` | Source-specific tag; cannot replace main's candidate |
| Cloud release | `ghcr.io/tapstate/tapstate-cloud:<version>` | Independently rebuilt by release; different bytes cannot overwrite an existing tag |

Configure the `cloud-ghcr-dev` environment with the dev `VITE_CLOUD_CONSOLE_URL`
and allow only the refs approved for image builds. Its branch policy must include
a selected feature branch before that run. The existing repository
`RELEASE_SATELLITE_TOKEN` reads the matching Web ref. Publishing uses the workflow
token with package write permission, or
the environment's `CLOUD_GHCR_USERNAME` / `CLOUD_GHCR_TOKEN` when explicitly set.
Reuse the existing Public `tapstate-cloud` package and its workflow write access;
the publisher refuses missing or non-public packages. Package access follows the
[GitHub package access and visibility instructions](https://docs.github.com/en/packages/learn-github-packages/configuring-a-packages-access-control-and-visibility).

Workflow-token authentication checks that the installation can access
`tapstate/tapstate`, reading every repository-list page before registry login.
It does not rely on personal `permissions.pull` flags in public repository
metadata. API errors or missing installation access fail closed; repository
lists and credentials are never logged. Explicit personal publishing credentials
retain their account-identity verification.

Version registration uses these additional `cloud-ghcr-dev` environment settings:

| Setting | Kind | Value |
| --- | --- | --- |
| `CLOUD_RELEASE_API_URL` | Variable | `https://dev.cloud.tapstate.com/v1/api/cluster-versions` |
| `CLOUD_RELEASE_CODE` | Variable | `BETA` (or `ALPHA` / `RC`) |
| `CLOUD_CLUSTER_VERSION_RELEASE_TOKEN_DEV` | Repository or environment Secret | The same static token configured on the dev Cloud control plane |

The workflow validates this configuration before building the image. After a
successful public push and digest readback, it registers `<version>-dev` with the
main and Web revisions, image tag, digest reference, and workflow run URL. The
caller rejects production endpoints and checks the returned record against the
published image. It never follows redirects, logs credential-bearing response
bodies, or retries a write whose delivery is uncertain. The API returns HTTP 409
with `cluster.version-duplicate` for an existing version; the dev caller reports
that it kept the first catalog record and completes without updating it. Other
conflicts and authentication failures still fail the workflow. Rebuilds can
replace the image tag while the catalog retains the first registration's Git
and image metadata; use the current run summary for the rebuilt image's digest.
The workflow maps `CLOUD_CLUSTER_VERSION_RELEASE_TOKEN_DEV` into the caller's
`CLOUD_CLUSTER_VERSION_RELEASE_TOKEN` environment variable. A GitHub Secret alone
does not configure the control plane's token.

The lane builds only the Cloud Web/Boot JAR and dual-platform image, verifies its
locked inputs, and uploads the checked OCI digest. It deploys no environment.
Use the digest reference in the run summary for dev validation; a numeric dev
tag can point to different bytes after a rebuild. Release rebuilds independently
with its own environment configuration and approval. This lane adds no automatic
PR/main test suite; its runner cost is incurred on manual dev dispatch and once
per release attempt after the release gates pass.

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
