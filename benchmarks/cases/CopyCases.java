package io.tapstate.benchmarks.cases;

import io.tapstate.benchmarks.PerfCase;

@PerfCase(
        id = "copy.snapshot.basic.small",
        version = 1,
        description = "Copy a bounded source collection and compare exact target rows.",
        group = "copy",
        operators = {},
        phase = "snapshot",
        mode = "snapshot",
        profile = "small",
        profileVersion = 1,
        source = "mongodb",
        target = "mongodb",
        environments = {"single-node-op"},
        purposes = {"throughput"},
        workload = "finite-batch",
        spec = "specs/copy-basic.json"
)
@PerfCase(
        id = "copy.snapshot.basic.standard",
        version = 1,
        description = "Copy a bounded source collection and compare exact target rows.",
        group = "copy",
        operators = {},
        phase = "snapshot",
        mode = "snapshot",
        profile = "standard",
        profileVersion = 1,
        source = "mongodb",
        target = "mongodb",
        environments = {"single-node-op"},
        purposes = {"throughput"},
        workload = "finite-batch",
        spec = "specs/copy-basic.json"
)
@PerfCase(
        id = "copy.snapshot.basic.wide",
        version = 2,
        description = "Copy a bounded source collection and compare exact target rows.",
        group = "copy",
        operators = {},
        phase = "snapshot",
        mode = "snapshot",
        profile = "wide",
        profileVersion = 1,
        source = "mongodb",
        target = "mongodb",
        environments = {"single-node-op"},
        purposes = {"throughput", "memory-peak"},
        workload = "finite-batch",
        dataFeatures = {"wide-rows"},
        tags = {"memory-regression"},
        spec = "specs/copy-basic.json"
)
@PerfCase(
        id = "copy.snapshot.basic.large",
        version = 2,
        description = "Copy a bounded source collection and compare exact target rows.",
        group = "copy",
        operators = {},
        phase = "snapshot",
        mode = "snapshot",
        profile = "large",
        profileVersion = 1,
        source = "mongodb",
        target = "mongodb",
        environments = {"single-node-op"},
        purposes = {"throughput", "memory-peak"},
        workload = "finite-batch",
        dataFeatures = {"wide-rows"},
        tags = {"memory-regression"},
        spec = "specs/copy-basic.json"
)
@PerfCase(
        id = "copy.live.basic.standard",
        version = 1,
        description = "Measure event-to-acknowledgement latency for copy with live insert, update and delete.",
        group = "copy",
        operators = {},
        phase = "cdc",
        mode = "live-latency",
        profile = "standard",
        profileVersion = 1,
        source = "mongodb",
        target = "mongodb",
        environments = {"single-node-op"},
        purposes = {"latency"},
        workload = "fixed-arrival-rate",
        spec = "specs/copy-basic.json"
)
@PerfCase(
        id = "copy.backlog.fixed-iud.standard",
        version = 1,
        description = "Measure catch-up for copy against a fixed insert, update and delete backlog.",
        group = "copy",
        operators = {},
        phase = "cdc",
        mode = "backlog-throughput",
        profile = "standard",
        profileVersion = 1,
        source = "mongodb",
        target = "mongodb",
        environments = {"single-node-op"},
        purposes = {"throughput"},
        workload = "finite-batch",
        spec = "specs/copy-basic.json"
)
@PerfCase(
        id = "copy.snapshot.dummy-baseline.standard",
        version = 1,
        description = "Measure the real Dummy-to-Dummy no-transform baseline.",
        group = "copy",
        operators = {},
        phase = "snapshot",
        mode = "snapshot",
        profile = "standard",
        profileVersion = 1,
        source = "dummy",
        target = "dummy",
        environments = {"single-node-op"},
        purposes = {"throughput"},
        workload = "finite-batch",
        tags = {"engine-baseline"},
        spec = "specs/copy-dummy-baseline.json"
)
@PerfCase(
        id = "copy.live.handoff.standard",
        version = 1,
        description = "Witness a lossless snapshot-to-CDC transition.",
        group = "copy",
        operators = {},
        phase = "snapshot-to-cdc",
        mode = "live-latency",
        profile = "standard",
        profileVersion = 1,
        source = "mongodb",
        target = "mongodb",
        environments = {"single-node-op"},
        purposes = {"latency"},
        workload = "snapshot-then-fixed-arrival-rate",
        tags = {"stage-handoff"},
        spec = "specs/copy-live-handoff.json"
)
@PerfCase(
        id = "copy.live.slow-target.large",
        version = 2,
        description = "Compare unlimited and rate-limited target writes under fixed injection.",
        group = "copy",
        operators = {},
        phase = "cdc",
        mode = "live-latency",
        profile = "large",
        profileVersion = 1,
        source = "mongodb",
        target = "mongodb",
        environments = {"single-node-op", "single-node-op-dedicated-target"},
        purposes = {"latency", "memory-peak"},
        workload = "fixed-arrival-rate",
        dataFeatures = {"wide-rows"},
        tags = {"memory-regression", "backpressure", "paired-control"},
        spec = "specs/copy-slow-target.json"
)
final class CopyCases {
    private CopyCases() {
    }
}
