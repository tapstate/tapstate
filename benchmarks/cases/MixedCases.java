package io.tapstate.benchmarks.cases;

import io.tapstate.benchmarks.PerfCase;

@PerfCase(
        id = "mixed.snapshot.double-map.standard",
        version = 1,
        description = "Run two separately identified Map nodes with a Filter and a script.",
        group = "mixed",
        operators = {"map", "filter", "js"},
        phase = "snapshot",
        mode = "snapshot",
        profile = "standard",
        profileVersion = 1,
        source = "mongodb",
        target = "mongodb",
        environments = {"single-node-op"},
        purposes = {"throughput"},
        workload = "finite-batch",
        dataFeatures = {"selective"},
        tags = {"node-timing"},
        spec = "specs/mixed-double-map.json"
)
@PerfCase(
        id = "mixed.snapshot.union-branches.standard",
        version = 1,
        description = "Merge two streams and verify both overlapping target branches.",
        group = "mixed",
        operators = {"union", "filter", "map"},
        phase = "snapshot",
        mode = "snapshot",
        profile = "standard",
        profileVersion = 1,
        source = "mongodb",
        target = "mongodb",
        environments = {"single-node-op"},
        purposes = {"throughput"},
        workload = "finite-batch",
        dataFeatures = {"multi-input", "multi-target"},
        spec = "specs/mixed-union-branches.json"
)
@PerfCase(
        id = "mixed.snapshot.map-nest.standard",
        version = 1,
        description = "Project the root key before building nested documents.",
        group = "mixed",
        operators = {"map", "nest"},
        phase = "snapshot",
        mode = "snapshot",
        profile = "standard",
        profileVersion = 1,
        source = "mongodb",
        target = "mongodb",
        environments = {"single-node-op"},
        purposes = {"throughput"},
        workload = "finite-batch",
        dataFeatures = {"multi-input", "stateful", "single-level", "fanout"},
        spec = "specs/mixed-map-nest.json"
)
final class MixedCases {
    private MixedCases() {
    }
}
