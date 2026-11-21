package io.tapstate.benchmarks.cases;

import io.tapstate.benchmarks.PerfCase;

@PerfCase(
        id = "unwind.snapshot.basic.small",
        version = 1,
        description = "Expand four distinct items per input row.",
        group = "unwind",
        operators = {"unwind"},
        phase = "snapshot",
        mode = "snapshot",
        profile = "small",
        profileVersion = 1,
        source = "mongodb",
        target = "mongodb",
        environments = {"single-node-op"},
        purposes = {"throughput"},
        workload = "finite-batch",
        dataFeatures = {"array", "fanout"},
        spec = "specs/unwind-basic.json"
)
@PerfCase(
        id = "unwind.snapshot.basic.standard",
        version = 1,
        description = "Expand four distinct items per input row.",
        group = "unwind",
        operators = {"unwind"},
        phase = "snapshot",
        mode = "snapshot",
        profile = "standard",
        profileVersion = 1,
        source = "mongodb",
        target = "mongodb",
        environments = {"single-node-op"},
        purposes = {"throughput"},
        workload = "finite-batch",
        dataFeatures = {"array", "fanout"},
        spec = "specs/unwind-basic.json"
)
@PerfCase(
        id = "unwind.snapshot.basic.wide",
        version = 1,
        description = "Expand four distinct items per input row.",
        group = "unwind",
        operators = {"unwind"},
        phase = "snapshot",
        mode = "snapshot",
        profile = "wide",
        profileVersion = 1,
        source = "mongodb",
        target = "mongodb",
        environments = {"single-node-op"},
        purposes = {"throughput"},
        workload = "finite-batch",
        dataFeatures = {"array", "fanout"},
        spec = "specs/unwind-basic.json"
)
@PerfCase(
        id = "unwind.snapshot.basic.large",
        version = 1,
        description = "Expand four distinct items per input row.",
        group = "unwind",
        operators = {"unwind"},
        phase = "snapshot",
        mode = "snapshot",
        profile = "large",
        profileVersion = 1,
        source = "mongodb",
        target = "mongodb",
        environments = {"single-node-op"},
        purposes = {"throughput"},
        workload = "finite-batch",
        dataFeatures = {"array", "fanout"},
        spec = "specs/unwind-basic.json"
)
@PerfCase(
        id = "unwind.live.basic.standard",
        version = 1,
        description = "Measure event-to-acknowledgement latency for unwind with live insert, update and delete.",
        group = "unwind",
        operators = {"unwind"},
        phase = "cdc",
        mode = "live-latency",
        profile = "standard",
        profileVersion = 1,
        source = "mongodb",
        target = "mongodb",
        environments = {"single-node-op"},
        purposes = {"latency"},
        workload = "fixed-arrival-rate",
        dataFeatures = {"array", "fanout"},
        spec = "specs/unwind-basic.json"
)
@PerfCase(
        id = "unwind.backlog.basic.standard",
        version = 1,
        description = "Measure catch-up for unwind against a fixed insert, update and delete backlog.",
        group = "unwind",
        operators = {"unwind"},
        phase = "cdc",
        mode = "backlog-throughput",
        profile = "standard",
        profileVersion = 1,
        source = "mongodb",
        target = "mongodb",
        environments = {"single-node-op"},
        purposes = {"throughput"},
        workload = "finite-batch",
        dataFeatures = {"array", "fanout"},
        spec = "specs/unwind-basic.json"
)
final class UnwindCases {
    private UnwindCases() {
    }
}
