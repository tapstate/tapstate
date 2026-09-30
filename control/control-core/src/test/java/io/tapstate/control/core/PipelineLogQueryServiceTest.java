package io.tapstate.control.core;

import io.tapstate.core.logging.LogLine;
import io.tapstate.core.logging.RingBufferLogSink;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

/**
 * The logs read face: it tails the node-local log sink for one pipeline. Unlike status / metrics /
 * snapshot, a read of a pipeline with no captured lines is a benign empty tail, not a coded
 * {@code monitor.no-observation} error — the absence of log lines is normal.
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
    void pagesStrictlyAfterTheReportedCursor() {
        RingBufferLogSink sink = new RingBufferLogSink(8, 8);
        sink.append("orders_sync", line("first"));
        sink.append("orders_sync", line("second"));
        sink.append("orders_sync", line("third"));
        var service = new PipelineLogQueryService(sink);

        PipelineLogs first = service.logs("orders_sync", null, 2);
        PipelineLogs second = service.logs("orders_sync", first.nextCursor(), 2);

        assertThat(first.lines()).extracting(LogLine::message).containsExactly("second", "third");
        assertThat(first.nextCursor()).isNotNull();
        assertThat(second.lines()).isEmpty();
        assertThat(second.nextCursor()).isEqualTo(first.nextCursor());
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
