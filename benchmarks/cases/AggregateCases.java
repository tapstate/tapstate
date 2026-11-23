package io.tapstate.benchmarks.cases;

import io.tapstate.benchmarks.PerfCase;

@PerfCase(
        id = "aggregate.snapshot.basic.small",
        version = 1,
        description = "Aggregate input rows into fixed groups and verify their counts and sums.",
        group = "aggregate",
        operators = {"aggregate"},
        phase = "snapshot",
        mode = "snapshot",
        profile = "small",
        profileVersion = 1,
        source = "mongodb",
        target = "mongodb",
        environments = {"single-node-op"},
        purposes = {"throughput"},
        workload = "finite-batch",
        dataFeatures = {"grouped"},
        spec = "specs/aggregate-basic.json"
)
@PerfCase(
        id = "aggregate.snapshot.basic.standard",
        version = 1,
        description = "Aggregate input rows into fixed groups and verify their counts and sums.",
        group = "aggregate",
        operators = {"aggregate"},
        phase = "snapshot",
        mode = "snapshot",
        profile = "standard",
        profileVersion = 1,
        source = "mongodb",
        target = "mongodb",
        environments = {"single-node-op"},
        purposes = {"throughput"},
        workload = "finite-batch",
        dataFeatures = {"grouped"},
        spec = "specs/aggregate-basic.json"
)
@PerfCase(
        id = "aggregate.snapshot.basic.wide",
        version = 1,
        description = "Aggregate input rows into fixed groups and verify their counts and sums.",
        group = "aggregate",
        operators = {"aggregate"},
        phase = "snapshot",
        mode = "snapshot",
        profile = "wide",
        profileVersion = 1,
        source = "mongodb",
        target = "mongodb",
        environments = {"single-node-op"},
        purposes = {"throughput"},
        workload = "finite-batch",
        dataFeatures = {"grouped"},
        spec = "specs/aggregate-basic.json"
)
@PerfCase(
        id = "aggregate.snapshot.basic.large",
        version = 1,
        description = "Aggregate input rows into fixed groups and verify their counts and sums.",
        group = "aggregate",
        operators = {"aggregate"},
        phase = "snapshot",
        mode = "snapshot",
        profile = "large",
        profileVersion = 1,
        source = "mongodb",
        target = "mongodb",
        environments = {"single-node-op"},
        purposes = {"throughput"},
        workload = "finite-batch",
        dataFeatures = {"grouped"},
        spec = "specs/aggregate-basic.json"
)
@PerfCase(
        id = "aggregate.live.basic.standard",
        version = 1,
        description = "Observe grouped Aggregate during live inserts, updates and deletes.",
        group = "aggregate",
        operators = {"aggregate"},
        phase = "cdc",
        mode = "live-latency",
        profile = "standard",
        profileVersion = 1,
        source = "mongodb",
        target = "mongodb",
        environments = {"single-node-op"},
        purposes = {"latency"},
        workload = "fixed-arrival-rate",
        dataFeatures = {"grouped"},
        spec = "specs/aggregate-basic.json"
)
@PerfCase(
        id = "aggregate.backlog.basic.standard",
        version = 1,
        description = "Observe grouped Aggregate catch-up from a finite change backlog.",
        group = "aggregate",
        operators = {"aggregate"},
        phase = "cdc",
        mode = "backlog-throughput",
        profile = "standard",
        profileVersion = 1,
        source = "mongodb",
        target = "mongodb",
        environments = {"single-node-op"},
        purposes = {"throughput"},
        workload = "finite-batch",
        dataFeatures = {"grouped"},
        spec = "specs/aggregate-basic.json"
)
final class AggregateCases {
    private AggregateCases() {
    }
}
