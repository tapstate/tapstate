package io.tapstate.control.restapi;

import io.tapstate.control.core.EffectiveHistoryResolution;
import io.tapstate.control.core.PipelineExplanation;
import io.tapstate.control.core.PipelineMetricsHistory;
import io.tapstate.control.core.PipelineMetricsHistory.Consistency;
import io.tapstate.control.core.PipelineMetricsHistory.Gap;
import io.tapstate.control.core.PipelineMetricsHistory.GapReason;
import io.tapstate.control.core.PipelineMetricsHistory.Lag;
import io.tapstate.control.core.PipelineMetricsHistory.Point;
import io.tapstate.control.core.PipelineMetricsHistory.Rate;
import io.tapstate.control.core.PipelineMetricsHistory.Segment;
import io.tapstate.control.core.PipelineMetricsHistory.StartReason;
import io.tapstate.control.core.PipelineMetricsHistory.Status;
import io.tapstate.control.core.PipelineMetricsHistory.Unavailable;
import io.tapstate.core.lifecycle.PipelineState;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Exact public consumer fixtures written by the same DTOs the HTTP controllers return. */
class ObservabilityWireContractTest {

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final Instant FROM = Instant.parse("2026-09-20T10:00:00Z");
    private static final Instant TO = Instant.parse("2026-09-20T11:00:00Z");
    private static final Instant CUTOFF = Instant.parse("2026-09-05T11:00:00Z");

    @Test
    void manifestPinsTheFirstBackendRevisionAndEveryFixture() throws Exception {
        Map<?, ?> manifest = JSON.readValue(golden("manifest.json"), Map.class);

        assertThat(manifest.get("contractVersion")).isEqualTo("v1");
        assertThat(manifest.get("firstSupportedProductVersion")).isEqualTo("0.5.0");
        assertThat(manifest.get("firstSupportedBackendRevision"))
                .isEqualTo("27472a5e8ecbfe3a3b35585ae6e203a752672cd6");
        assertThat(((List<?>) manifest.get("operations")).stream().map(String::valueOf).toList())
                .containsExactly("pipeline.metrics.history", "pipeline.explain");
        assertThat(((List<?>) manifest.get("fixtures")).stream().map(String::valueOf).toList()).containsExactly(
                "history-raw-page-1.golden.json",
                "history-raw-page-2.golden.json",
                "history-aggregate-boundaries.golden.json",
                "history-empty.golden.json",
                "explain-stale.golden.json",
                "explain-no-match.golden.json");
    }

    @Test
    void rawPagesMatchTheConsumerFixturesExactly() throws Exception {
        PipelineMetricsHistory first = history(EffectiveHistoryResolution.PT1M,
                List.of(segment("2026-09-20T09:59:00Z", "2026-09-20T10:01:00Z",
                        StartReason.WINDOW_START,
                        point("2026-09-20T09:59:00Z", "2026-09-20T10:01:00Z",
                                rate(1200, 10, 10), rate(240000, 2000, 2000),
                                List.of(lag("2026-09-20T10:01:00Z", 2, 2))))),
                List.of(), List.of(), "example-opaque-page-2");
        PipelineMetricsHistory second = history(EffectiveHistoryResolution.PT1M,
                List.of(segment("2026-09-20T10:01:00Z", "2026-09-20T10:02:00Z",
                        StartReason.CONTINUATION,
                        point("2026-09-20T10:01:00Z", "2026-09-20T10:02:00Z",
                                rate(300, 5, 5), rate(60000, 1000, 1000), List.of()))),
                List.of(), List.of(new Unavailable("lag", "public.orders")), null);

        assertGolden(PipelineHistoryResponse.of(first), "history-raw-page-1.golden.json");
        assertGolden(PipelineHistoryResponse.of(second), "history-raw-page-2.golden.json");
    }

    @Test
    void aggregateBoundariesAndEmptyHistoryMatchTheConsumerFixturesExactly() throws Exception {
        PipelineMetricsHistory aggregate = history(EffectiveHistoryResolution.PT30M,
                List.of(
                        segment("2026-09-20T10:00:00Z", "2026-09-20T10:05:00Z",
                                StartReason.WINDOW_START,
                                point("2026-09-20T10:00:00Z", "2026-09-20T10:05:00Z",
                                        rate(3000, 10, 25), null,
                                        List.of(lag("2026-09-20T10:04:00Z", 2, 12)))),
                        segment("2026-09-20T10:05:00Z", "2026-09-20T10:30:00Z",
                                StartReason.COUNTER_RESET,
                                point("2026-09-20T10:05:00Z", "2026-09-20T10:30:00Z",
                                        rate(7500, 5, 9), null, List.of())),
                        segment("2026-09-20T10:45:00Z", "2026-09-20T11:00:00Z",
                                StartReason.GAP,
                                point("2026-09-20T10:45:00Z", "2026-09-20T11:00:00Z",
                                        rate(4500, 5, 8), null, List.of()))),
                List.of(new Gap(Instant.parse("2026-09-20T10:30:00Z"),
                        Instant.parse("2026-09-20T10:45:00Z"), GapReason.SAMPLE_GAP)),
                List.of(new Unavailable("bytes.out", null)), null);
        PipelineMetricsHistory empty = new PipelineMetricsHistory(
                "orders", Instant.parse("2026-08-20T10:00:00Z"), Instant.parse("2026-08-20T11:00:00Z"),
                CUTOFF, CUTOFF, CUTOFF, EffectiveHistoryResolution.PT1M,
                Status.NO_RETAINED_SAMPLES, Consistency.EVENTUAL, List.of(), List.of(), List.of(), null);

        assertGolden(PipelineHistoryResponse.of(aggregate), "history-aggregate-boundaries.golden.json");
        assertGolden(PipelineHistoryResponse.of(empty), "history-empty.golden.json");
    }

    @Test
    void staleAndNoMatchExplanationsMatchTheConsumerFixturesExactly() throws Exception {
        PipelineExplanation stale = new PipelineExplanation(
                "orders", PipelineState.RUNNING, PipelineExplanation.Kind.OBSERVATION_STALE,
                "The latest observation is 45s old, so the publisher may have stopped.",
                FROM, 45_000L, PipelineExplanation.Freshness.STALE,
                List.of(new PipelineExplanation.Evidence(
                        PipelineExplanation.Source.STATUS, "observedAgeMillis", 45_000L)),
                List.of("What the pipeline is doing now is unknown: only the last published observation is available."),
                new PipelineExplanation.Next(PipelineExplanation.NextAction.CHECK_SERVER,
                        "Check that the server is running and converging."), null);
        PipelineExplanation noMatch = new PipelineExplanation(
                "orders", PipelineState.RUNNING, PipelineExplanation.Kind.NO_MATCH,
                "No diagnostic rule matched.", FROM, 1_000L, PipelineExplanation.Freshness.FRESH,
                List.of(
                        evidence(PipelineExplanation.Source.STATUS, "observedAgeMillis", 1_000L),
                        evidence(PipelineExplanation.Source.STATUS, "failure", null),
                        evidence(PipelineExplanation.Source.METRICS, "reconcileFailuresInARow", 0L),
                        evidence(PipelineExplanation.Source.METRICS, "recordCount", 1_200L),
                        evidence(PipelineExplanation.Source.METRICS, "frontierStalledMillis", Map.of()),
                        evidence(PipelineExplanation.Source.SNAPSHOT, "rowsDone", 1_200L)),
                List.of(
                        "Whether the source has changes waiting is not measured.",
                        "Whether an initial load is still running cannot be determined from loaded rows alone."),
                null, null);

        assertGolden(PipelineExplanationResponse.of(stale), "explain-stale.golden.json");
        assertGolden(PipelineExplanationResponse.of(noMatch), "explain-no-match.golden.json");
    }

    private static PipelineMetricsHistory history(
            EffectiveHistoryResolution resolution, List<Segment> segments, List<Gap> gaps,
            List<Unavailable> unavailable, String cursor) {
        return new PipelineMetricsHistory("orders", FROM, TO, FROM, TO, CUTOFF, resolution,
                Status.OK, Consistency.EVENTUAL, segments, gaps, unavailable, cursor);
    }

    private static Segment segment(String start, String end, StartReason reason, Point point) {
        return new Segment(Instant.parse(start), Instant.parse(end), reason, List.of(point));
    }

    private static Point point(String start, String end, Rate records, Rate bytes, List<Lag> lag) {
        return new Point(Instant.parse(start), Instant.parse(end), records, bytes, lag);
    }

    private static Rate rate(long delta, long average, long max) {
        return new Rate(BigDecimal.valueOf(delta), BigDecimal.valueOf(average), BigDecimal.valueOf(max));
    }

    private static Lag lag(String observedAt, long last, long max) {
        return new Lag("public.orders", Instant.parse(observedAt), last, max);
    }

    private static PipelineExplanation.Evidence evidence(
            PipelineExplanation.Source source, String field, Object value) {
        return new PipelineExplanation.Evidence(source, field, value);
    }

    private static void assertGolden(Object value, String name) throws Exception {
        assertThat(JSON.writeValueAsString(value)).isEqualTo(golden(name));
    }

    private static String golden(String name) throws IOException {
        try (var input = ObservabilityWireContractTest.class.getResourceAsStream(
                "/golden/observability/" + name)) {
            if (input == null) {
                throw new IOException("missing observability wire golden: " + name);
            }
            String file = new String(input.readAllBytes(), StandardCharsets.UTF_8);
            assertThat(file).doesNotContain("\r").endsWith("\n");
            return file.substring(0, file.length() - 1);
        }
    }
}
