package io.tapstate.benchmarks.cases;

import io.tapstate.benchmarks.PerfCase;

@PerfCase(
        id = "nest.snapshot.basic.small",
        version = 1,
        description = "Assemble single-level root documents with four children per root.",
        group = "nest",
        operators = {"nest"},
        phase = "snapshot",
        mode = "snapshot",
        profile = "small",
        profileVersion = 1,
        source = "mongodb",
        target = "mongodb",
        environments = {"single-node-op"},
        purposes = {"throughput"},
        workload = "finite-batch",
        dataFeatures = {"multi-input", "stateful", "single-level", "fanout"},
        spec = "specs/nest-basic.json"
)
@PerfCase(
        id = "nest.snapshot.basic.standard",
        version = 1,
        description = "Assemble single-level root documents with four children per root.",
        group = "nest",
        operators = {"nest"},
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
        spec = "specs/nest-basic.json"
)
@PerfCase(
        id = "nest.snapshot.basic.wide",
        version = 1,
        description = "Assemble single-level root documents with four children per root.",
        group = "nest",
        operators = {"nest"},
        phase = "snapshot",
        mode = "snapshot",
        profile = "wide",
        profileVersion = 1,
        source = "mongodb",
        target = "mongodb",
        environments = {"single-node-op"},
        purposes = {"throughput"},
        workload = "finite-batch",
        dataFeatures = {"multi-input", "stateful", "single-level", "fanout"},
        spec = "specs/nest-basic.json"
)
@PerfCase(
        id = "nest.snapshot.basic.large",
        version = 1,
        description = "Assemble single-level root documents with four children per root.",
        group = "nest",
        operators = {"nest"},
        phase = "snapshot",
        mode = "snapshot",
        profile = "large",
        profileVersion = 1,
        source = "mongodb",
        target = "mongodb",
        environments = {"single-node-op"},
        purposes = {"throughput"},
        workload = "finite-batch",
        dataFeatures = {"multi-input", "stateful", "single-level", "fanout"},
        spec = "specs/nest-basic.json"
)
@PerfCase(
        id = "nest.live.basic.standard",
        version = 1,
        description = "Measure event-to-acknowledgement latency for nest with live insert, update and delete.",
        group = "nest",
        operators = {"nest"},
        phase = "cdc",
        mode = "live-latency",
        profile = "standard",
        profileVersion = 1,
        source = "mongodb",
        target = "mongodb",
        environments = {"single-node-op"},
        purposes = {"latency"},
        workload = "fixed-arrival-rate",
        dataFeatures = {"multi-input", "stateful", "single-level", "fanout"},
        spec = "specs/nest-basic.json"
)
@PerfCase(
        id = "nest.backlog.basic.standard",
        version = 1,
        description = "Measure catch-up for nest against a fixed insert, update and delete backlog.",
        group = "nest",
        operators = {"nest"},
        phase = "cdc",
        mode = "backlog-throughput",
        profile = "standard",
        profileVersion = 1,
        source = "mongodb",
        target = "mongodb",
        environments = {"single-node-op"},
        purposes = {"throughput"},
        workload = "finite-batch",
        dataFeatures = {"multi-input", "stateful", "single-level", "fanout"},
        spec = "specs/nest-basic.json"
)
@PerfCase(
        id = "nest.live.child-update.standard",
        version = 1,
        description = "Compare uniform and hot-root child mutations with single-level nesting.",
        group = "nest",
        operators = {"nest"},
        phase = "cdc",
        mode = "live-latency",
        profile = "standard",
        profileVersion = 1,
        source = "mongodb",
        target = "mongodb",
        environments = {"single-node-op"},
        purposes = {"latency"},
        workload = "fixed-arrival-rate",
        dataFeatures = {"stateful", "multi-input", "single-level", "hot-keys"},
        tags = {"paired-control"},
        spec = "specs/nest-child-update.json"
)
@PerfCase(
        id = "nest.snapshot.fanout.wide",
        version = 1,
        description = "Compare ordinary and high Nest fanout with fixed root count.",
        group = "nest",
        operators = {"nest"},
        phase = "snapshot",
        mode = "snapshot",
        profile = "wide",
        profileVersion = 1,
        source = "mongodb",
        target = "mongodb",
        environments = {"single-node-op"},
        purposes = {"throughput", "memory-peak"},
        workload = "finite-batch",
        dataFeatures = {"stateful", "multi-input", "single-level", "fanout"},
        tags = {"memory-regression", "paired-control"},
        spec = "specs/nest-fanout.json"
)
final class NestCases {
    private NestCases() {
    }
}
