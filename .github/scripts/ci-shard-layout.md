# Source-bound CI shard layouts

The default-reactor planner reads `.github/ci-shard-layout.json`. This ordinary
comparison commit keeps the fixed four-worker layout: two E2E IT workers,
`engine` and `rest`. The optional layouts described below remain available;
a source recipe and valid matrix are not performance acceptance.

The configuration requires `schema: 1`, `e2e_shards`, `rest_modules` and
`new_test_seconds`. The optional `app_shards` field accepts 2 or 4; omitting it
keeps App in its original residual module group. For example, a four-worker E2E candidate with app isolated
from the remaining residual modules is:

```json
{
  "schema": 1,
  "e2e_shards": 4,
  "rest_modules": {"app": ["app"]},
  "new_test_seconds": 30
}
```

Residual groups move complete modules; they cannot split their source classes.
Only modules with tests in the original residual set are accepted. Groups must
be disjoint, named with lowercase letters/digits/hyphens, and cannot use the
reserved `build`, `engine`, `rest` or `e2e-` namespace. A nonempty residual `rest` group
must remain. E2E IT classes and the engine module keep their original roles.

When `app_shards` is present, App moves into `app-1` through `app-N` workers,
balanced by complete top-level source class. Unit and integration selections
for the same source class stay on one worker; nested report classes remain
owned by that outer class. The other residual groups still move whole modules.
App cannot also appear in `rest_modules`, whose names cannot occupy `app-` in
this mode. There must be enough distinct App classes for nonempty workers and
a nonempty `rest` group. Omitting the optional App field preserves the legacy App module assignment.

Every App worker executes the original independent full pre-test install and
its selected verify, with original class-owned fixtures, real processes and
containers. It retains the whole module test bytecode tree for admission, its
own execution data, and Cloud preparation. These repeated preparation and
container costs must be included in capacity and complete-cost comparisons;
a valid layout or estimated duration is not an adoption result.

```sh
bash .github/scripts/ci-shard.sh plan --root . \
  --durations .github/ci-test-durations.json --output /tmp/shards.json
bash .github/scripts/ci-shard.sh matrix --root . --plan /tmp/shards.json
```

The matrix command reads back the exact source cohort, layout bytes and plan.
It emits one compact `include` JSON object used directly by the original CI
job. Missing/extra/empty workers, foreign module assignments and a layout
changed after planning reject. There is no independently maintained worker list.
Worker display names remain `test-shards (<id>)` independently of matrix metadata.
Every group of original residual tests receives the original Cloud preparation
environment, even after a module moves out of the worker named `rest`. That
preparation cost remains part of a candidate's actual execution cost.

Module-prefixed duration keys take priority over bare class keys. Nested JUnit
suites belong to their source outer class; main CI uses class-level timings,
including `PublishedExamplesIT`. The separate nightly connector per-example
duration map is unchanged. The planner lists missing hint identities and their
explicit fallback. The checked-in layout uses 30 seconds for new source tests;
this is an estimate, not a runtime bound. If the default layout file is absent,
the legacy two-worker/one-second fallback is explicit in the plan. A missing
custom layout, duplicate keys, nonfinite/boolean durations or invalid counts
reject.

The plan retains layout and duration-input hashes. Timing provenance and timer
aggregation limits accompany the duration map; nested suite timers can overlap.
The timing provenance records 1,285 source units from one complete,
successful optimized-source manual CI run. Its nine original selected verify
commands and validated worker packs retain their original source/run bindings.
The 1,290 XML reports include five additional nested-container suites; their
times are summed under the owning source outer class. This rollback recipe
selects 1,281 source units and reverses owned fixture mechanisms and added
controls. Its source-unit set and test-source bytes differ from that recorded
run; those records remain unchanged. The captured run includes 98 source
classes with no testcase rows (97 E2E) and ten with zero aggregate suite time.
Report presence or zero time cannot establish executed cases. These weights
are scheduling estimates, not an observed four-versus-six comparison, current
input equivalence, or PR performance acceptance.

Source discovery and the exact multiset assignment remain authoritative for
which tests run. Estimated balancing, historical main timings and a valid
matrix do not prove current byte/case/assertion equivalence, capacity, CI wall
time or runner-cost acceptance. Original event receipts and complete measured
comparisons are still required before selecting a candidate.
