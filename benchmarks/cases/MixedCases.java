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
final class MixedCases {
    private MixedCases() {
    }
}
