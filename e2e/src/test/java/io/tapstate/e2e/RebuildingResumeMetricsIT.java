package io.tapstate.e2e;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.core.JobStatus;
import io.tapstate.core.lifecycle.CdcDeliveryTotals;
import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.runtime.engine.Engine;
import io.tapstate.testsupport.RequiresDocker;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;

@RequiresDocker
class RebuildingResumeMetricsIT {
    private static final String PIPELINE = "resume_metrics";
    private static final Duration WAIT = Duration.ofMinutes(2);

    @Test
    void recordsOutIncludesWritesFromBeforeTheReplacement(@TempDir Path directory) throws IOException {
        exercise(directory, false);
    }

    @Test
    void aSecondRebuildDropsTheTotalsFromBeforeAnUnpublishedProducerRestart(@TempDir Path directory) throws IOException {
        exercise(directory, true);
    }

    private static void exercise(Path directory, boolean restartProducer) throws IOException {
        var source = Files.createDirectory(directory.resolve("source"));
        var target = Files.createDirectory(directory.resolve("target"));
        var sourceAddress = EndpointAddress.uri(source.toString());
        var targetAddress = EndpointAddress.uri(target.toString());
        var tables = List.of("early", "late");
        var holds = new Holds(directory.resolve("holds"));
        var writes = new WriteWitness(directory.resolve("writes"));
        var readHold = Map.of("hold", holds.directory().toString());
        var writeLedger = Map.of("write_witness", writes.directory().toString());

        var existingMembers = Hazelcast.getAllHazelcastInstances();
        try (var storage = PreparedNativeStore.create(); var server = InProcessServer.startPrepared(storage);
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
            holds.read("early");
            control.lifecycle(PIPELINE, LifecycleVerb.RESUME);
            long replacement = Await.answered("resume to allocate a replacement execution", WAIT,
                    () -> control.executionGenerationOf(PIPELINE).filter(generation -> generation > original));
            awaitCdcCounter(control, () -> {});
            assertThat((long) writes.rows().size()).isEqualTo(beforeResume);
            assertThat(control.recordsOut(PIPELINE)).as("records.out before the replacement delivers a row")
                    .contains(beforeResume);
            if (restartProducer) {
                pauseAndRebuildAfterRestart(server, existingMembers, control, replacement);
                assertThat(control.recordsOut(PIPELINE)).as("records.out after rebuilding from the restarted producer")
                        .contains(0L);
                assertThat((long) writes.rows().size()).isEqualTo(beforeResume);
            }
            holds.releaseRead("early");
            holds.releaseRead("late");
            Await.until("both rows from the held table to reach the target", WAIT,
                    () -> {
                        if (restartProducer) server.publishObservationsOnce();
                        return files.count(targetAddress, "late") == 2;
                    },
                    () -> "target rows=" + files.count(targetAddress, "late"));
            awaitRecordsOut(control, writes, beforeResume + 2, restartProducer ? beforeResume : 0,
                    () -> { if (restartProducer) server.publishObservationsOnce(); });
        }
    }

    private static void pauseAndRebuildAfterRestart(InProcessServer server, Set<HazelcastInstance> existingMembers,
            ControlPlane control, long replacement) {
        var started = Hazelcast.getAllHazelcastInstances().stream()
                .filter(member -> !existingMembers.contains(member) && member.getJet().getJob(PIPELINE) != null).toList();
        assertThat(started).as("the member running the replacement Job").hasSize(1);
        var member = started.getFirst();
        var engine = new Engine(member);
        var job = member.getJet().getJob(PIPELINE);
        var priorStart = Await.answered("the first replacement's native output start", WAIT,
                () -> engine.captureRun(PIPELINE).flatMap(Engine.Run::metrics).flatMap(sample -> sample.delivered().start()));

        assertThat(server.stopPublishingObservations()).isEqualTo(1);
        job.restart();
        Await.answered("the same Job to report a new producer window", WAIT,
                () -> engine.captureRun(PIPELINE).filter(run -> run.jobId() == job.getId())
                        .flatMap(Engine.Run::metrics).flatMap(sample -> sample.delivered().start()).filter(priorStart::isBefore));
        control.lifecycle(PIPELINE, LifecycleVerb.PAUSE);
        assertThat(server.publishObservationsOnce()).isEqualTo(1);
        Await.until("the restarted Job to pause", WAIT,
                () -> job.getStatus() == JobStatus.SUSPENDED
                        && control.state(PIPELINE).filter(PipelineState.PAUSED::equals).isPresent(),
                () -> "Jet status=" + job.getStatus() + ", pipeline state=" + control.state(PIPELINE));
        control.lifecycle(PIPELINE, LifecycleVerb.RESUME);
        Await.answered("the second resume to allocate another execution", WAIT, () -> {
            server.publishObservationsOnce();
            return control.executionGenerationOf(PIPELINE).filter(generation -> generation > replacement);
        });
        awaitCdcCounter(control, () -> server.publishObservationsOnce());
    }

    private static void awaitCdcCounter(ControlPlane control, Runnable publish) {
        Await.until("the replacement sink to publish its CDC counter", WAIT, () -> {
            publish.run();
            return control.metricFacts(PIPELINE).stream().anyMatch(fact -> CdcDeliveryTotals.COUNT_METRIC.equals(fact.get("name")));
        }, () -> control.metrics(PIPELINE));
    }

    private static void awaitRecordsOut(ControlPlane control, WriteWitness writes, long minimum) {
        awaitRecordsOut(control, writes, minimum, 0, () -> {});
    }

    private static void awaitRecordsOut(ControlPlane control, WriteWitness writes, long minimum, long beforeWindow,
            Runnable publish) {
        // CDC may replay snapshot rows. The writer's ledger counts those deliveries independently of the API.
        Await.until("records.out to match all rows written to the target", WAIT, () -> {
            publish.run();
            long written = writes.rows().size();
            return written >= minimum && control.recordsOut(PIPELINE).filter(count -> count == written - beforeWindow).isPresent();
        }, () -> "target writes=" + writes.rows().size() + ", records.out=" + control.recordsOut(PIPELINE));
    }
}
