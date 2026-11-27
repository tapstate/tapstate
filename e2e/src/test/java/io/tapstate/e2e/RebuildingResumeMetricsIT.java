package io.tapstate.e2e;

import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.testsupport.RequiresDocker;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

@RequiresDocker
class RebuildingResumeMetricsIT {
    private static final String PIPELINE = "resume_metrics";
    private static final Duration WAIT = Duration.ofMinutes(2);

    @Test
    void recordsOutIncludesWritesFromBeforeTheReplacement(@TempDir Path directory) throws IOException {
        var source = Files.createDirectory(directory.resolve("source"));
        var target = Files.createDirectory(directory.resolve("target"));
        var sourceAddress = EndpointAddress.uri(source.toString());
        var targetAddress = EndpointAddress.uri(target.toString());
        var tables = List.of("early", "late");
        var holds = new Holds(directory.resolve("holds"));
        var writes = new WriteWitness(directory.resolve("writes"));
        var readHold = Map.of("hold", holds.directory().toString());
        var writeLedger = Map.of("write_witness", writes.directory().toString());

        try (var storage = PreparedNativeStore.create(); var server = Tiers.IN_PROCESS.launch(storage);
                var files = new FileEndpoints()) {
            var control = new ControlPlane(server.baseUrl());
            storage.loginAndAwaitBusiness(control);
            byte[] connector = Files.readAllBytes(E2eConnectorJar.buildInto(directory));
            control.registerConnector(E2eConnectorJar.CONNECTOR_ID, connector);
            files.seed(sourceAddress, "early", SeedRows.generated(3));
            files.seed(sourceAddress, "late", SeedRows.generated(2));
            holds.read("late");
            control.apply(Map.of(
                    "source.tap.yml", Workspaces.cdcSourceYaml("src", source, tables, readHold),
                    "target.tap.yml", Workspaces.targetYaml("tgt", target, writeLedger),
                    "pipeline.tap.yml", Workspaces.pipelineYaml(PIPELINE, "src", "tgt", tables)));
            control.discoverSchema("src", E2eConnectorJar.CONNECTOR_ID, Map.of("uri", source.toString()));
            control.lifecycle(PIPELINE, LifecycleVerb.START);
            Await.answered("the second snapshot table to be held", WAIT, () -> holds.holderOfRead("late"));
            awaitRecordsOut(control, writes, 3);
            long original = control.executionGenerationOf(PIPELINE).orElseThrow();

            control.lifecycle(PIPELINE, LifecycleVerb.PAUSE);
            Await.until("the partial snapshot to pause", WAIT,
                    () -> control.state(PIPELINE).filter(PipelineState.PAUSED::equals).isPresent(),
                    () -> "state=" + control.state(PIPELINE));
            long beforeResume = writes.rows().size();
            control.lifecycle(PIPELINE, LifecycleVerb.RESUME);
            Await.answered("resume to allocate a replacement execution", WAIT,
                    () -> control.executionGenerationOf(PIPELINE).filter(generation -> generation > original));
            holds.releaseRead("late");
            Await.until("both rows from the held table to reach the target", WAIT,
                    () -> files.count(targetAddress, "late") == 2,
                    () -> "target rows=" + files.count(targetAddress, "late"));
            awaitRecordsOut(control, writes, beforeResume + 2);
        }
    }

    private static void awaitRecordsOut(ControlPlane control, WriteWitness writes, long minimum) {
        // CDC may replay snapshot rows. The writer's ledger counts those deliveries independently of the API.
        Await.until("records.out to match all rows written to the target", WAIT, () -> {
            long written = writes.rows().size();
            return written >= minimum && control.recordsOut(PIPELINE).filter(count -> count == written).isPresent();
        }, () -> "target writes=" + writes.rows().size() + ", records.out=" + control.recordsOut(PIPELINE));
    }
}
