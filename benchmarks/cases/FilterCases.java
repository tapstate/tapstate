package io.tapstate.benchmarks.cases;

import io.tapstate.benchmarks.PerfCase;

@PerfCase(
        id = "filter.snapshot.basic.small",
        version = 1,
        description = "Retain even keys and verify both retained and excluded rows.",
        group = "filter",
        operators = {"filter"},
        phase = "snapshot",
        mode = "snapshot",
        profile = "small",
        profileVersion = 1,
        source = "mongodb",
        target = "mongodb",
        environments = {"single-node-op"},
        purposes = {"throughput"},
        workload = "finite-batch",
        dataFeatures = {"selective"},
        spec = "specs/filter-basic.json"
)
@PerfCase(
        id = "filter.snapshot.basic.standard",
        version = 1,
        description = "Retain even keys and verify both retained and excluded rows.",
        group = "filter",
        operators = {"filter"},
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
        spec = "specs/filter-basic.json"
)
@PerfCase(
        id = "filter.snapshot.basic.wide",
        version = 1,
        description = "Retain even keys and verify both retained and excluded rows.",
        group = "filter",
        operators = {"filter"},
        phase = "snapshot",
        mode = "snapshot",
        profile = "wide",
        profileVersion = 1,
        source = "mongodb",
        target = "mongodb",
        environments = {"single-node-op"},
        purposes = {"throughput"},
        workload = "finite-batch",
        dataFeatures = {"selective"},
        spec = "specs/filter-basic.json"
)
@PerfCase(
        id = "filter.snapshot.basic.large",
        version = 1,
        description = "Retain even keys and verify both retained and excluded rows.",
        group = "filter",
        operators = {"filter"},
        phase = "snapshot",
        mode = "snapshot",
        profile = "large",
        profileVersion = 1,
        source = "mongodb",
        target = "mongodb",
        environments = {"single-node-op"},
        purposes = {"throughput"},
        workload = "finite-batch",
        dataFeatures = {"selective"},
        spec = "specs/filter-basic.json"
)
@PerfCase(
        id = "filter.live.basic.standard",
        version = 1,
        description = "Measure event-to-acknowledgement latency for filter with live insert, update and delete.",
        group = "filter",
        operators = {"filter"},
        phase = "cdc",
        mode = "live-latency",
        profile = "standard",
        profileVersion = 1,
        source = "mongodb",
        target = "mongodb",
        environments = {"single-node-op"},
        purposes = {"latency"},
        workload = "fixed-arrival-rate",
        dataFeatures = {"selective"},
        spec = "specs/filter-basic.json"
)
@PerfCase(
        id = "filter.backlog.basic.standard",
        version = 1,
        description = "Measure catch-up for filter against a fixed insert, update and delete backlog.",
        group = "filter",
        operators = {"filter"},
        phase = "cdc",
        mode = "backlog-throughput",
        profile = "standard",
        profileVersion = 1,
        source = "mongodb",
        target = "mongodb",
        environments = {"single-node-op"},
        purposes = {"throughput"},
        workload = "finite-batch",
        dataFeatures = {"selective"},
        spec = "specs/filter-basic.json"
)
final class FilterCases {
    private FilterCases() {
    }
}
