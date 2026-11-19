package io.tapstate.benchmarks.cases;

import io.tapstate.benchmarks.PerfCase;

@PerfCase(
        id = "join.snapshot.basic.small",
        version = 1,
        description = "Join facts to unique dimensions at a fixed match rate.",
        group = "join",
        operators = {"join"},
        phase = "snapshot",
        mode = "snapshot",
        profile = "small",
        profileVersion = 1,
        source = "mongodb",
        target = "mongodb",
        environments = {"single-node-op"},
        purposes = {"throughput"},
        workload = "finite-batch",
        dataFeatures = {"multi-input", "stateful", "uniform-keys"},
        spec = "specs/join-basic.json"
)
@PerfCase(
        id = "join.snapshot.basic.standard",
        version = 1,
        description = "Join facts to unique dimensions at a fixed match rate.",
        group = "join",
        operators = {"join"},
        phase = "snapshot",
        mode = "snapshot",
        profile = "standard",
        profileVersion = 1,
        source = "mongodb",
        target = "mongodb",
        environments = {"single-node-op"},
        purposes = {"throughput"},
        workload = "finite-batch",
        dataFeatures = {"multi-input", "stateful", "uniform-keys"},
        spec = "specs/join-basic.json"
)
@PerfCase(
        id = "join.snapshot.basic.wide",
        version = 1,
        description = "Join facts to unique dimensions at a fixed match rate.",
        group = "join",
        operators = {"join"},
        phase = "snapshot",
        mode = "snapshot",
        profile = "wide",
        profileVersion = 1,
        source = "mongodb",
        target = "mongodb",
        environments = {"single-node-op"},
        purposes = {"throughput"},
        workload = "finite-batch",
        dataFeatures = {"multi-input", "stateful", "uniform-keys"},
        spec = "specs/join-basic.json"
)
@PerfCase(
        id = "join.snapshot.basic.large",
        version = 1,
        description = "Join facts to unique dimensions at a fixed match rate.",
        group = "join",
        operators = {"join"},
        phase = "snapshot",
        mode = "snapshot",
        profile = "large",
        profileVersion = 1,
        source = "mongodb",
        target = "mongodb",
        environments = {"single-node-op"},
        purposes = {"throughput"},
        workload = "finite-batch",
        dataFeatures = {"multi-input", "stateful", "uniform-keys"},
        spec = "specs/join-basic.json"
)
@PerfCase(
        id = "join.live.basic.standard",
        version = 1,
        description = "Measure event-to-acknowledgement latency for join with live insert, update and delete.",
        group = "join",
        operators = {"join"},
        phase = "cdc",
        mode = "live-latency",
        profile = "standard",
        profileVersion = 1,
        source = "mongodb",
        target = "mongodb",
        environments = {"single-node-op"},
        purposes = {"latency"},
        workload = "fixed-arrival-rate",
        dataFeatures = {"multi-input", "stateful", "uniform-keys"},
        spec = "specs/join-basic.json"
)
@PerfCase(
        id = "join.backlog.basic.standard",
        version = 1,
        description = "Measure catch-up for join against a fixed insert, update and delete backlog.",
        group = "join",
        operators = {"join"},
        phase = "cdc",
        mode = "backlog-throughput",
        profile = "standard",
        profileVersion = 1,
        source = "mongodb",
        target = "mongodb",
        environments = {"single-node-op"},
        purposes = {"throughput"},
        workload = "finite-batch",
        dataFeatures = {"multi-input", "stateful", "uniform-keys"},
        spec = "specs/join-basic.json"
)
@PerfCase(
        id = "join.live.hot-update.standard",
        version = 1,
        description = "Compare uniform and hot dimension changes at the same load.",
        group = "join",
        operators = {"join"},
        phase = "cdc",
        mode = "live-latency",
        profile = "standard",
        profileVersion = 1,
        source = "mongodb",
        target = "mongodb",
        environments = {"single-node-op"},
        purposes = {"latency"},
        workload = "fixed-arrival-rate",
        dataFeatures = {"stateful", "multi-input", "hot-keys"},
        tags = {"paired-control"},
        spec = "specs/join-hot-update.json"
)
@PerfCase(
        id = "join.live.hot-update.wide",
        version = 2,
        description = "Measure memory amplification from hot dimension updates with wide rows.",
        group = "join",
        operators = {"join"},
        phase = "cdc",
        mode = "live-latency",
        profile = "wide",
        profileVersion = 1,
        source = "mongodb",
        target = "mongodb",
        environments = {"single-node-op"},
        purposes = {"latency", "memory-peak"},
        workload = "fixed-arrival-rate",
        dataFeatures = {"stateful", "multi-input", "hot-keys", "wide-rows"},
        tags = {"memory-regression", "paired-control"},
        spec = "specs/join-hot-update.json"
)
@PerfCase(
        id = "join.live.steady-state.standard",
        version = 2,
        description = "Measure resource growth under a fixed key set of active Join records.",
        group = "join",
        operators = {"join"},
        phase = "cdc",
        mode = "live-latency",
        profile = "standard",
        profileVersion = 1,
        source = "mongodb",
        target = "mongodb",
        environments = {"single-node-op"},
        purposes = {"memory-stability", "latency"},
        workload = "fixed-arrival-rate",
        dataFeatures = {"stateful", "multi-input", "fixed-keyspace"},
        tags = {"memory-regression", "steady-state"},
        spec = "specs/join-steady-state.json"
)
@PerfCase(
        id = "join.lifecycle.release.standard",
        version = 2,
        description = "Measure release of Join resources over three start-stop cycles in one JVM.",
        group = "join",
        operators = {"join"},
        phase = "snapshot-to-cdc",
        mode = "snapshot",
        profile = "standard",
        profileVersion = 1,
        source = "mongodb",
        target = "mongodb",
        environments = {"single-node-op"},
        purposes = {"resource-release"},
        workload = "bounded-lifecycle-cycles",
        dataFeatures = {"stateful", "multi-input"},
        tags = {"memory-regression", "repeated-lifecycle"},
        execution = "repeated-lifecycle",
        spec = "specs/join-lifecycle-release.json"
)
final class JoinCases {
    private JoinCases() {
    }
}
