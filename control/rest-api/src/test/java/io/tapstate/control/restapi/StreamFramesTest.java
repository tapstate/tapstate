package io.tapstate.control.restapi;

import io.tapstate.control.core.PipelineLogs;
import io.tapstate.control.core.PipelineStatus;
import io.tapstate.core.lifecycle.ObservationFailure;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.logging.LogLine;
import io.tapstate.messages.MessageCatalog;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The stream frame encoders: a status or logs frame rendered to the same compact JSON the one-shot
 * {@code GET} read faces return, so the CLI decodes a streamed frame with the exact decoder it uses for
 * a polled read. The message escaping is delegated to the core JSON writer, so an arbitrary log message
 * is rendered safely.
 */
class StreamFramesTest {

    private static final MessageCatalog CATALOG = MessageCatalog.bundled();

    @Test
    void statusFrameCarriesTheIdAndStateNameLikeTheReadFace() {
        String frame = StreamFrames.status(new PipelineStatus("orders", PipelineState.RUNNING), CATALOG);
        assertThat(frame).isEqualTo("{\"pipelineId\":\"orders\",\"state\":\"RUNNING\"}");
    }

    @Test
    void statusFrameOfAFailedPipelineCarriesTheCodedReasonLikeTheReadFace() {
        // A watcher must be able to tell why a pipeline died from the same frame that reports it dead --
        // otherwise the one frame that matters is reasonless and no later frame repeats it (the state
        // does not change again on its own).
        ObservationFailure failure = new ObservationFailure("TS0440008",
                Map.of("pipeline", "orders", "cause", "sink refused the batch"));
        String frame = StreamFrames.status(new PipelineStatus("orders", PipelineState.FAILED, failure), CATALOG);

        assertThat(frame).contains("\"pipelineId\":\"orders\"");
        assertThat(frame).contains("\"state\":\"FAILED\"");
        assertThat(frame).contains("\"failure\":{");
        assertThat(frame).contains("\"code\":\"TS0440008\"");
        assertThat(frame).contains("\"cause\":\"sink refused the batch\"");
        assertThat(frame).contains("\"message\":");
    }

    @Test
    void statusFrameKeepsTheSameCapturedDetailsAndSolutionAsThePolledFailure() {
        var diagnostic = Map.<String, Object>of("message", "captured owner",
                "cause", Map.of("message", "safe cause", "stackTrace", "safe frame"));
        var failure = new ObservationFailure("TS0060027",
                Map.of("connector", "demo", "detail", "safe detail"), diagnostic);
        var polled = ApiError.from(failure, CATALOG, java.util.Locale.ENGLISH);
        var parsed = (Map<?, ?>) io.tapstate.core.common.JsonReader.parse(
                StreamFrames.status(new PipelineStatus("orders", PipelineState.FAILED, failure), CATALOG));
        var streamed = (Map<?, ?>) parsed.get("failure");
        assertThat(streamed.get("code")).isEqualTo(polled.code());
        assertThat(streamed.get("params")).isEqualTo(polled.params());
        assertThat(streamed.get("message")).isEqualTo(polled.message());
        assertThat(polled.solution()).isNotBlank();
        assertThat(streamed.get("solution")).isEqualTo(polled.solution());
        assertThat(streamed.get("diagnostic")).isEqualTo(diagnostic).isEqualTo(polled.diagnostic());
    }

    @Test
    void statusFrameOfAHealthyPipelineOmitsFailureEntirely() {
        String frame = StreamFrames.status(new PipelineStatus("orders", PipelineState.RUNNING), CATALOG);
        assertThat(frame).doesNotContain("failure");
    }

    @Test
    void logsFrameCarriesTheIdAndLinesLikeTheReadFace() {
        PipelineLogs logs = new PipelineLogs("orders", List.of(
                new LogLine(1_700_000_000_000L, "INFO", "submitted job")));
        String frame = StreamFrames.logs(logs);
        assertThat(frame).isEqualTo(
                "{\"pipelineId\":\"orders\",\"lines\":[{\"timestampMillis\":1700000000000,"
                        + "\"level\":\"INFO\",\"message\":\"submitted job\"}]}");
    }

    @Test
    void logsFrameKeepsTheCapturedDiagnostic() {
        var diagnostic = java.util.Map.<String, Object>of("code", "28000", "codeSource", "jdbc-sqlstate", "rawMessage", "access denied");
        var frame = StreamFrames.logs(new PipelineLogs("p", List.of(new LogLine(1L, "WARN", "safe failure", diagnostic))));
        var parsed = (java.util.Map<?, ?>) io.tapstate.core.common.JsonReader.parse(frame);
        var line = (java.util.Map<?, ?>) ((List<?>) parsed.get("lines")).getFirst();
        assertThat(line.get("diagnostic")).isEqualTo(diagnostic);
    }

    @Test
    void logsFrameEscapesAnArbitraryMessage() {
        PipelineLogs logs = new PipelineLogs("p", List.of(
                new LogLine(1L, "WARN", "quote\" and newline\n")));
        String frame = StreamFrames.logs(logs);
        assertThat(frame).contains("\"message\":\"quote\\\" and newline\\n\"");
    }

    @Test
    void logsFrameOfNoLinesCarriesAnEmptyArray() {
        String frame = StreamFrames.logs(new PipelineLogs("p", List.of()));
        assertThat(frame).isEqualTo("{\"pipelineId\":\"p\",\"lines\":[]}");
    }
}
