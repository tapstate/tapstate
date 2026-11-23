package io.tapstate.benchmarks.cases;

import io.tapstate.benchmarks.PerfCase;

@PerfCase(
        id = "map.snapshot.basic.small",
        version = 1,
        description = "Project, rename and calculate fields over a bounded collection.",
        group = "map",
        operators = {"map"},
        phase = "snapshot",
        mode = "snapshot",
        profile = "small",
        profileVersion = 1,
        source = "mongodb",
        target = "mongodb",
        environments = {"single-node-op"},
        purposes = {"throughput"},
        workload = "finite-batch",
        spec = "specs/map-basic.json"
)
@PerfCase(
        id = "map.snapshot.basic.standard",
        version = 1,
        description = "Project, rename and calculate fields over a bounded collection.",
        group = "map",
        operators = {"map"},
        phase = "snapshot",
        mode = "snapshot",
        profile = "standard",
        profileVersion = 1,
        source = "mongodb",
        target = "mongodb",
        environments = {"single-node-op"},
        purposes = {"throughput"},
        workload = "finite-batch",
        spec = "specs/map-basic.json"
)
@PerfCase(
        id = "map.snapshot.basic.wide",
        version = 1,
        description = "Project, rename and calculate fields over a bounded collection.",
        group = "map",
        operators = {"map"},
        phase = "snapshot",
        mode = "snapshot",
        profile = "wide",
        profileVersion = 1,
        source = "mongodb",
        target = "mongodb",
        environments = {"single-node-op"},
        purposes = {"throughput"},
        workload = "finite-batch",
        spec = "specs/map-basic.json"
)
@PerfCase(
        id = "map.snapshot.basic.large",
        version = 1,
        description = "Project, rename and calculate fields over a bounded collection.",
        group = "map",
        operators = {"map"},
        phase = "snapshot",
        mode = "snapshot",
        profile = "large",
        profileVersion = 1,
        source = "mongodb",
        target = "mongodb",
        environments = {"single-node-op"},
        purposes = {"throughput"},
        workload = "finite-batch",
        spec = "specs/map-basic.json"
)
@PerfCase(
        id = "map.live.basic.standard",
        version = 1,
        description = "Measure event-to-acknowledgement latency for map with live insert, update and delete.",
        group = "map",
        operators = {"map"},
        phase = "cdc",
        mode = "live-latency",
        profile = "standard",
        profileVersion = 1,
        source = "mongodb",
        target = "mongodb",
        environments = {"single-node-op"},
        purposes = {"latency"},
        workload = "fixed-arrival-rate",
        spec = "specs/map-basic.json"
)
@PerfCase(
        id = "map.backlog.basic.standard",
        version = 1,
        description = "Measure catch-up for map against a fixed insert, update and delete backlog.",
        group = "map",
        operators = {"map"},
        phase = "cdc",
        mode = "backlog-throughput",
        profile = "standard",
        profileVersion = 1,
        source = "mongodb",
        target = "mongodb",
        environments = {"single-node-op"},
        purposes = {"throughput"},
        workload = "finite-batch",
        spec = "specs/map-basic.json"
)
final class MapCases {
    private MapCases() {
    }
}
