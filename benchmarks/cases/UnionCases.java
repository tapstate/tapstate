package io.tapstate.benchmarks.cases;

import io.tapstate.benchmarks.PerfCase;

@PerfCase(
        id = "union.snapshot.basic.small",
        version = 1,
        description = "Merge two disjoint input streams and verify both contributions.",
        group = "union",
        operators = {"union"},
        phase = "snapshot",
        mode = "snapshot",
        profile = "small",
        profileVersion = 1,
        source = "mongodb",
        target = "mongodb",
        environments = {"single-node-op"},
        purposes = {"throughput"},
        workload = "finite-batch",
        dataFeatures = {"multi-input"},
        spec = "specs/union-basic.json"
)
@PerfCase(
        id = "union.snapshot.basic.standard",
        version = 1,
        description = "Merge two disjoint input streams and verify both contributions.",
        group = "union",
        operators = {"union"},
        phase = "snapshot",
        mode = "snapshot",
        profile = "standard",
        profileVersion = 1,
        source = "mongodb",
        target = "mongodb",
        environments = {"single-node-op"},
        purposes = {"throughput"},
        workload = "finite-batch",
        dataFeatures = {"multi-input"},
        spec = "specs/union-basic.json"
)
@PerfCase(
        id = "union.snapshot.basic.wide",
        version = 1,
        description = "Merge two disjoint input streams and verify both contributions.",
        group = "union",
        operators = {"union"},
        phase = "snapshot",
        mode = "snapshot",
        profile = "wide",
        profileVersion = 1,
        source = "mongodb",
        target = "mongodb",
        environments = {"single-node-op"},
        purposes = {"throughput"},
        workload = "finite-batch",
        dataFeatures = {"multi-input"},
        spec = "specs/union-basic.json"
)
@PerfCase(
        id = "union.snapshot.basic.large",
        version = 1,
        description = "Merge two disjoint input streams and verify both contributions.",
        group = "union",
        operators = {"union"},
        phase = "snapshot",
        mode = "snapshot",
        profile = "large",
        profileVersion = 1,
        source = "mongodb",
        target = "mongodb",
        environments = {"single-node-op"},
        purposes = {"throughput"},
        workload = "finite-batch",
        dataFeatures = {"multi-input"},
        spec = "specs/union-basic.json"
)
@PerfCase(
        id = "union.live.basic.standard",
        version = 1,
        description = "Measure event-to-acknowledgement latency for union with live insert, update and delete.",
        group = "union",
        operators = {"union"},
        phase = "cdc",
        mode = "live-latency",
        profile = "standard",
        profileVersion = 1,
        source = "mongodb",
        target = "mongodb",
        environments = {"single-node-op"},
        purposes = {"latency"},
        workload = "fixed-arrival-rate",
        dataFeatures = {"multi-input"},
        spec = "specs/union-basic.json"
)
@PerfCase(
        id = "union.backlog.basic.standard",
        version = 1,
        description = "Measure catch-up for union against a fixed insert, update and delete backlog.",
        group = "union",
        operators = {"union"},
        phase = "cdc",
        mode = "backlog-throughput",
        profile = "standard",
        profileVersion = 1,
        source = "mongodb",
        target = "mongodb",
        environments = {"single-node-op"},
        purposes = {"throughput"},
        workload = "finite-batch",
        dataFeatures = {"multi-input"},
        spec = "specs/union-basic.json"
)
final class UnionCases {
    private UnionCases() {
    }
}
