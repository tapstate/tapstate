package io.tapstate.control.core;

import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.logging.LogLine;
import io.tapstate.core.logging.LogSink;
import io.tapstate.core.logging.RingBufferLogSink;
import io.tapstate.core.model.Resource;
import io.tapstate.spi.store.ArtifactStore;
import io.tapstate.spi.store.ExecutionGenerationStore;
import io.tapstate.spi.store.WorkloadClaim;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

import static org.assertj.core.api.Assertions.assertThat;

class ScopedPipelineLogQueryTest {

    @Test
    void currentReadsOneExecutionAndIncarnationReadsOnlyTheCurrentResource() {
        RingBufferLogSink sink = new RingBufferLogSink(8, 8);
        Artifacts artifacts = new Artifacts();
        Generations generations = new Generations();
        PipelineLogQueryService query = new PipelineLogQueryService(sink, artifacts, generations, "cluster");
        sink.append("orders", new LogSink.Scope("resource-a", 9), line("earlier execution"));
        sink.append("orders", new LogSink.Scope("resource-a", 10), line("current execution"));

        assertThat(query.logs("orders").lines()).extracting(LogLine::message)
                .containsExactly("current execution");
        assertThat(query.logs("orders", PipelineLogQueryService.Scope.INCARNATION).lines())
                .extracting(LogLine::message).containsExactly("earlier execution", "current execution");

        artifacts.incarnation = "resource-b";
        generations.generation = 11;
        sink.append("orders", new LogSink.Scope("resource-b", 11), line("recreated resource"));
        assertThat(query.logs("orders").lines()).extracting(LogLine::message)
                .containsExactly("recreated resource");
        assertThat(query.logs("orders", PipelineLogQueryService.Scope.INCARNATION).lines())
                .extracting(LogLine::message).containsExactly("recreated resource");

        sink.clearIncarnation("orders", "resource-a");
        assertThat(query.logs("orders").lines()).extracting(LogLine::message)
                .containsExactly("recreated resource");
        artifacts.exists = false;
        assertThat(query.logs("orders").lines()).isEmpty();
    }

    @Test
    void upgradeEraLegacyLinesStopBeingVisibleWhenTheArtifactGainsAnIdentity() {
        RingBufferLogSink sink = new RingBufferLogSink(8, 8);
        Artifacts artifacts = new Artifacts();
        artifacts.incarnation = null;
        PipelineLogQueryService query = new PipelineLogQueryService(sink, artifacts, new Generations(), "cluster");
        sink.append("orders", line("legacy line"));
        assertThat(query.logs("orders").lines()).extracting(LogLine::message).containsExactly("legacy line");

        artifacts.incarnation = "resource-a";
        assertThat(query.logs("orders").lines()).isEmpty();
        assertThat(query.logs("orders", PipelineLogQueryService.Scope.INCARNATION).lines()).isEmpty();
    }

    private static LogLine line(String message) {
        return new LogLine(0, "INFO", message);
    }

    private static final class Artifacts implements ArtifactStore {
        private final Resource pipeline = new DslParser().parse("""
                version: tapstate/v1
                kind: pipeline
                id: orders
                source: source
                """);
        private String incarnation = "resource-a";
        private boolean exists = true;

        @Override public void saveAll(List<Resource> resources) { throw new UnsupportedOperationException(); }
        @Override public Optional<Resource> get(String id) {
            return exists && "orders".equals(id) ? Optional.of(pipeline) : Optional.empty();
        }
        @Override public List<Resource> list() { return exists ? List.of(pipeline) : List.of(); }
        @Override public Optional<String> pipelineIncarnationId(String id) {
            return Optional.ofNullable(incarnation);
        }
    }

    private static final class Generations implements ExecutionGenerationStore {
        private long generation = 10;

        @Override public Optional<WorkloadClaim> advanceUnderClaim(
                WorkloadClaim expected, long topologyRevision) {
            throw new UnsupportedOperationException();
        }
        @Override public OptionalLong advanceStandalone(String clusterId, String pipelineId) {
            throw new UnsupportedOperationException();
        }
        @Override public OptionalLong currentGeneration(String clusterId, String pipelineId) {
            return OptionalLong.of(generation);
        }
    }
}
