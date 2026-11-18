package io.tapstate.benchmarks.cases;

import io.tapstate.benchmarks.PerfCase;

@PerfCase(
        id = "js.snapshot.basic.small",
        version = 1,
        description = "Apply a deterministic integer and string script to a bounded collection.",
        group = "js",
        operators = {"js"},
        phase = "snapshot",
        mode = "snapshot",
        profile = "small",
        profileVersion = 1,
        source = "mongodb",
        target = "mongodb",
        environments = {"single-node-op"},
        purposes = {"throughput"},
        workload = "finite-batch",
        spec = "specs/js-basic.json"
)
@PerfCase(
        id = "js.snapshot.basic.standard",
        version = 1,
        description = "Apply a deterministic integer and string script to a bounded collection.",
        group = "js",
        operators = {"js"},
        phase = "snapshot",
        mode = "snapshot",
        profile = "standard",
        profileVersion = 1,
        source = "mongodb",
        target = "mongodb",
        environments = {"single-node-op"},
        purposes = {"throughput"},
        workload = "finite-batch",
        spec = "specs/js-basic.json"
)
@PerfCase(
        id = "js.snapshot.basic.wide",
        version = 1,
        description = "Apply a deterministic integer and string script to a bounded collection.",
        group = "js",
        operators = {"js"},
        phase = "snapshot",
        mode = "snapshot",
        profile = "wide",
        profileVersion = 1,
        source = "mongodb",
        target = "mongodb",
        environments = {"single-node-op"},
        purposes = {"throughput"},
        workload = "finite-batch",
        spec = "specs/js-basic.json"
)
@PerfCase(
        id = "js.snapshot.basic.large",
        version = 1,
        description = "Apply a deterministic integer and string script to a bounded collection.",
        group = "js",
        operators = {"js"},
        phase = "snapshot",
        mode = "snapshot",
        profile = "large",
        profileVersion = 1,
        source = "mongodb",
        target = "mongodb",
        environments = {"single-node-op"},
        purposes = {"throughput"},
        workload = "finite-batch",
        spec = "specs/js-basic.json"
)
@PerfCase(
        id = "js.live.basic.standard",
        version = 1,
        description = "Measure event-to-acknowledgement latency for js with live insert, update and delete.",
        group = "js",
        operators = {"js"},
        phase = "cdc",
        mode = "live-latency",
        profile = "standard",
        profileVersion = 1,
        source = "mongodb",
        target = "mongodb",
        environments = {"single-node-op"},
        purposes = {"latency"},
        workload = "fixed-arrival-rate",
        spec = "specs/js-basic.json"
)
@PerfCase(
        id = "js.backlog.basic.standard",
        version = 1,
        description = "Measure catch-up for js against a fixed insert, update and delete backlog.",
        group = "js",
        operators = {"js"},
        phase = "cdc",
        mode = "backlog-throughput",
        profile = "standard",
        profileVersion = 1,
        source = "mongodb",
        target = "mongodb",
        environments = {"single-node-op"},
        purposes = {"throughput"},
        workload = "finite-batch",
        spec = "specs/js-basic.json"
)
final class JsCases {
    private JsCases() {
    }
}
