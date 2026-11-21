package io.tapstate.control.core;

import io.tapstate.core.logging.LogLine;
import io.tapstate.core.logging.RingBufferLogSink;
import io.tapstate.spi.store.PipelineStateMappings.LogWindow;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

/**
 * The logs read face: it tails the node-local log sink for one pipeline. Unlike status / metrics /
 * snapshot, a read of a pipeline with no captured lines is a benign empty tail, not a coded
 * {@code TS0400001} error — the absence of log lines is normal.
 */
class PipelineLogQueryServiceTest {

    private static LogLine line(String message) {
        return new LogLine(0L, "INFO", message);
    }

    @Test
    void projectsTheTailedLinesForThePipeline() {
        RingBufferLogSink sink = new RingBufferLogSink(8, 8);
        sink.append("orders_sync", line("submitted job"));
        sink.append("orders_sync", line("converged to RUNNING"));
        var service = new PipelineLogQueryService(sink);

        PipelineLogs logs = service.logs("orders_sync");

        assertThat(logs.pipelineId()).isEqualTo("orders_sync");
        assertThat(logs.lines()).extracting(LogLine::message)
                .containsExactly("submitted job", "converged to RUNNING");
    }

    @Test
    void appliesTheRequestedLimitToTheMostRecentLines() {
        RingBufferLogSink sink = new RingBufferLogSink(8, 8);
        sink.append("orders_sync", line("first"));
        sink.append("orders_sync", line("second"));
        sink.append("orders_sync", line("third"));
        var service = new PipelineLogQueryService(sink);

        PipelineLogs logs = service.logs("orders_sync", 2);

        assertThat(logs.lines()).extracting(LogLine::message)
                .containsExactly("second", "third");
    }

    @Test
    void scopesTheTailBeforeApplyingTheLimitAndHidesRetiredPipelines() {
        RingBufferLogSink sink = new RingBufferLogSink(8, 8);
        sink.append("orders_sync", 3, line("retired"));
        sink.append("orders_sync", 4, line("started"));
        sink.append("orders_sync", 5, line("resumed"));
        sink.append("orders_sync", 3, line("late predecessor"));
        sink.append("orders_sync", 6, line("different window"));
        sink.append("removed", 1, line("removed"));
        var service = PipelineLogQueryService.scoped(sink, id -> id.equals("orders_sync")
                ? Optional.of(new LogWindow(4, 5)) : Optional.empty());

        assertThat(service.logs("orders_sync", 1).lines()).extracting(LogLine::message).containsExactly("resumed");
        assertThat(service.logs("orders_sync").lines()).extracting(LogLine::message).containsExactly("started", "resumed");
        assertThat(service.logs("removed").lines()).isEmpty();
    }

    @Test
    void retriesWhenTheLifetimeChangesAfterTakingTheTail() {
        var sink = new RingBufferLogSink(8, 2);
        var scopeReads = new java.util.concurrent.atomic.AtomicInteger();
        sink.append("orders", 1, line("predecessor"));
        var service = PipelineLogQueryService.scoped(sink, id -> {
            if (scopeReads.incrementAndGet() == 1) return Optional.of(new LogWindow(1, 1));
            sink.append("orders", 2, line("successor"));
            return Optional.of(new LogWindow(2, 2));
        });

        assertThat(service.logs("orders").lines()).extracting(LogLine::message).containsOnly("successor");
        assertThat(scopeReads.get()).isEqualTo(3);
    }

    @Test
    void retriesWhenARebuildAdvancesOnlyTheUpperBound() {
        var sink = new RingBufferLogSink(8, 1);
        sink.append("orders", 2, line("rebuilt"));
        var scopeReads = new java.util.concurrent.atomic.AtomicInteger();
        var service = PipelineLogQueryService.scoped(sink, id -> Optional.of(
                scopeReads.incrementAndGet() == 1 ? new LogWindow(1, 1) : new LogWindow(1, 2)));

        assertThat(service.logs("orders").lines()).extracting(LogLine::message).containsExactly("rebuilt");
    }

    @Test
    void aPipelineWithNoLinesProjectsAnEmptyTailNotAnError() {
        var service = new PipelineLogQueryService(new RingBufferLogSink(8, 8));

        PipelineLogs logs = service.logs("never-logged");

        assertThat(logs.pipelineId()).isEqualTo("never-logged");
        assertThat(logs.lines()).isEmpty();
    }

    @Test
    void requiresPipelineId() {
        var service = new PipelineLogQueryService(new RingBufferLogSink(8, 8));

        assertThatNullPointerException().isThrownBy(() -> service.logs(null));
    }

    @Test
    void requiresALogSink() {
        assertThatNullPointerException().isThrownBy(() -> new PipelineLogQueryService(null));
    }
}
