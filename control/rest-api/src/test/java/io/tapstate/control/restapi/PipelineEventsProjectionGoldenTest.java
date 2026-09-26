package io.tapstate.control.restapi;

import io.tapstate.control.core.PipelineEvents;
import io.tapstate.core.common.JsonReader;
import io.tapstate.core.lifecycle.PipelineEvent;
import io.tapstate.core.lifecycle.PipelineState;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** The production REST projection matches each shared event consumer example. */
class PipelineEventsProjectionGoldenTest {

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final Instant FROM = Instant.parse("2026-09-20T10:00:00Z");
    private static final Instant TO = Instant.parse("2026-09-20T11:00:00Z");
    private static final Instant CUTOFF = Instant.parse("2026-09-05T11:00:00Z");

    @Test
    void failureRecoveryGapAndEmptyPagesUseTheExactPublicShape() throws Exception {
        PipelineEvents.Failure failure = new PipelineEvents.Failure("engine.job-failed",
                Map.of("pipeline", "orders", "cause", "sink refused the batch"),
                "Pipeline orders stopped because its job failed: sink refused the batch.");
        PipelineEvents failed = page(List.of(
                new PipelineEvents.Event("ev-a7", Instant.parse("2026-09-20T10:10:00Z"),
                        PipelineEvent.Kind.FAILURE, failure.message(), PipelineState.RUNNING,
                        PipelineState.FAILED, failure, null),
                new PipelineEvents.Event("ev-b2", Instant.parse("2026-09-20T10:12:00Z"),
                        PipelineEvent.Kind.EXECUTION_RECOVERED, "Pipeline execution recovered.",
                        PipelineState.FAILED, PipelineState.RUNNING, null, null)), List.of());
        PipelineEvents gap = page(List.of(new PipelineEvents.Event("ev-g4",
                Instant.parse("2026-09-20T10:20:00Z"), PipelineEvent.Kind.TELEMETRY_GAP,
                "Some pipeline events could not be recorded.", null, null, null, null)),
                List.of(new PipelineEvents.KnownGap("ev-g4",
                        Instant.parse("2026-09-20T09:58:00Z"), Instant.parse("2026-09-20T10:19:00Z"),
                        List.of(PipelineEvent.GapReason.QUEUE_FULL, PipelineEvent.GapReason.WRITE_FAILURE))));

        assertFixture("events-failure-recovery.golden.json", failed);
        assertFixture("events-known-gap.golden.json", gap);
        assertFixture("events-empty.golden.json", page(List.of(), List.of()));
    }

    private static PipelineEvents page(List<PipelineEvents.Event> events,
            List<PipelineEvents.KnownGap> gaps) {
        return new PipelineEvents("orders", FROM, TO, FROM, TO, CUTOFF,
                PipelineEvents.Completeness.BEST_EFFORT, events, gaps, null);
    }

    private static void assertFixture(String name, PipelineEvents events) throws IOException {
        String actual = JSON.writeValueAsString(PipelineEventsResponse.of(events));
        String expected;
        try (var input = PipelineEventsProjectionGoldenTest.class.getResourceAsStream(
                "/golden/observability-events/" + name)) {
            assertThat(input).as(name).isNotNull();
            expected = new String(input.readAllBytes(), StandardCharsets.UTF_8);
        }
        assertThat(JsonReader.parse(actual)).as(name).isEqualTo(JsonReader.parse(expected));
    }
}
