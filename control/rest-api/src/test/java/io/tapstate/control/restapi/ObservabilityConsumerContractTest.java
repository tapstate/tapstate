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
import java.util.TreeMap;

import static org.assertj.core.api.Assertions.assertThat;

/** Exact public consumer fixtures written by the same DTOs the HTTP controllers return. */
class ObservabilityConsumerContractTest {

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
                "history-auto-page-1.golden.json",
                "history-aggregate-boundaries.golden.json",
                "history-single-metric-missing.golden.json",
                "history-empty.golden.json",
                "explain-stale.golden.json",
                "explain-coded-failure.golden.json",
                "explain-reconcile-failures.golden.json",
                "explain-no-movement.golden.json",
                "explain-frontier-stalled.golden.json",
                "explain-no-match.golden.json",
                "explain-unknown.golden.json",
                "explain-start-pending.golden.json");
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
                        segment("2026-09-20T10:05:00Z", "2026-09-20T10:06:00Z",
                                StartReason.COUNTER_RESET,
                                point("2026-09-20T10:05:00Z", "2026-09-20T10:06:00Z",
                                        rate(600, 10, 10), null, List.of())),
                        segment("2026-09-20T10:30:00Z", "2026-09-20T10:45:00Z",
                                StartReason.GAP,
                                point("2026-09-20T10:30:00Z", "2026-09-20T10:45:00Z",
                                        rate(9000, 10, 12), null, List.of()))),
                List.of(new Gap(Instant.parse("2026-09-20T10:06:00Z"),
                        Instant.parse("2026-09-20T10:30:00Z"), GapReason.SAMPLE_GAP)),
                List.of(new Unavailable("bytes.out", null)), null);
        PipelineMetricsHistory empty = new PipelineMetricsHistory(
                "orders", Instant.parse("2026-08-20T10:00:00Z"), Instant.parse("2026-08-20T11:00:00Z"),
                CUTOFF, CUTOFF, CUTOFF, EffectiveHistoryResolution.PT1M,
                Status.NO_RETAINED_SAMPLES, Consistency.EVENTUAL, List.of(), List.of(), List.of(), null);

        assertGolden(PipelineHistoryResponse.of(aggregate), "history-aggregate-boundaries.golden.json");
        assertGolden(PipelineHistoryResponse.of(empty), "history-empty.golden.json");
    }

    @Test
    void autoPaginationAndSingleMetricAbsenceMatchTheConsumerFixturesExactly() throws Exception {
        PipelineMetricsHistory auto = history(EffectiveHistoryResolution.PT30M,
                List.of(segment("2026-09-20T10:00:00Z", "2026-09-20T10:30:00Z",
                        StartReason.WINDOW_START,
                        point("2026-09-20T10:00:00Z", "2026-09-20T10:30:00Z",
                                rate(18000, 10, 25), rate(3600000, 2000, 4000), List.of()))),
                List.of(), List.of(), "example-auto-page-2");
        PipelineMetricsHistory missing = history(EffectiveHistoryResolution.PT1M,
                List.of(segment("2026-09-20T10:00:00Z", "2026-09-20T10:01:00Z",
                        StartReason.WINDOW_START,
                        point("2026-09-20T10:00:00Z", "2026-09-20T10:01:00Z",
                                rate(60, 1, 1), null, List.of()))),
                List.of(), List.of(new Unavailable("bytes.out", null)), null);

        assertGolden(PipelineHistoryResponse.of(auto), "history-auto-page-1.golden.json");
        assertGolden(PipelineHistoryResponse.of(missing), "history-single-metric-missing.golden.json");
    }

    @Test
    void historyRatesAlwaysSerializeAsPlainJsonNumbers() throws Exception {
        PipelineHistoryResponse.Rate rate = new PipelineHistoryResponse.Rate(
                new BigDecimal("1200"), new BigDecimal("0.000000001"), new BigDecimal("1000000000000"));

        assertThat(JSON.writeValueAsString(rate)).isEqualTo(
                "{\"delta\":1200,\"averageRate\":0.000000001,\"maxRate\":1000000000000}");
    }

    @Test
    void everyExplanationRuleAndOptionalStateMatchesTheConsumerFixturesExactly() throws Exception {
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
                        evidence(PipelineExplanation.Source.METRICS, "frontierStalledMillis",
                                new TreeMap<>(Map.of("orders", 196L, "shipments", 9L))),
                        evidence(PipelineExplanation.Source.SNAPSHOT, "rowsDone", 1_200L)),
                List.of(
                        "Whether the source has changes waiting is not measured.",
                        "Whether an initial load is still running cannot be determined from loaded rows alone."),
                null, null);
        Instant freshAt = Instant.parse("2026-09-20T10:00:43Z");
        PipelineExplanation coded = new PipelineExplanation(
                "orders", PipelineState.FAILED, PipelineExplanation.Kind.CODED_FAILURE,
                "The run failed, and said why: engine.job-failed.", freshAt, 2_000L,
                PipelineExplanation.Freshness.FRESH,
                List.of(evidence(PipelineExplanation.Source.STATUS, "failure",
                        new PipelineExplanation.Failure("engine.job-failed",
                                Map.of("pipeline", "orders", "cause", "the sink rejected the batch"),
                                "Pipeline orders stopped because its job failed: the sink rejected the batch."))),
                List.of(), new PipelineExplanation.Next(
                        PipelineExplanation.NextAction.OPEN_PIPELINE_LOGS,
                        "Open the logs for pipeline orders."), null);
        PipelineExplanation reconcile = new PipelineExplanation(
                "orders", PipelineState.NEW, PipelineExplanation.Kind.RECONCILE_FAILURES,
                "The server keeps failing to bring this pipeline up: 3 passes in a row have thrown.",
                freshAt, 2_000L, PipelineExplanation.Freshness.FRESH,
                List.of(
                        evidence(PipelineExplanation.Source.METRICS, "reconcileFailuresInARow", 3L),
                        evidence(PipelineExplanation.Source.STATUS, "state", PipelineState.NEW)),
                List.of("Whether the job itself is still alive is unknown; no failure was observed, "
                        + "so state remains new."),
                new PipelineExplanation.Next(PipelineExplanation.NextAction.CHECK_SERVER,
                        "Check that the server is running and converging."), null);
        PipelineExplanation noMovement = new PipelineExplanation(
                "orders", PipelineState.RUNNING, PipelineExplanation.Kind.NO_MOVEMENT,
                "Nothing has moved: no records driven and no rows loaded.", freshAt, 2_000L,
                PipelineExplanation.Freshness.FRESH,
                List.of(
                        evidence(PipelineExplanation.Source.METRICS, "recordCount", null),
                        evidence(PipelineExplanation.Source.SNAPSHOT, "rowsDone", 0L)),
                List.of("Whether the source has changes waiting is not measured."),
                new PipelineExplanation.Next(PipelineExplanation.NextAction.OPEN_PIPELINE_LOGS,
                        "Open the logs for pipeline orders."), null);
        PipelineExplanation stalled = new PipelineExplanation(
                "orders", PipelineState.RUNNING, PipelineExplanation.Kind.FRONTIER_STALLED,
                "A chain has stopped advancing: orders.", freshAt, 2_000L,
                PipelineExplanation.Freshness.FRESH,
                List.of(evidence(PipelineExplanation.Source.METRICS,
                        "frontierStalledMillis.orders", 96_000L)), List.of(),
                new PipelineExplanation.Next(PipelineExplanation.NextAction.CHECK_TARGET,
                        "Check that the target is accepting writes."), null);
        PipelineExplanation unknown = new PipelineExplanation(
                "orders", PipelineState.PAUSED, PipelineExplanation.Kind.NO_MATCH,
                "No diagnostic rule matched.", null, null, PipelineExplanation.Freshness.UNKNOWN,
                List.of(
                        evidence(PipelineExplanation.Source.STATUS, "observedAgeMillis", null),
                        evidence(PipelineExplanation.Source.STATUS, "failure", null),
                        evidence(PipelineExplanation.Source.METRICS, "reconcileFailuresInARow", null),
                        evidence(PipelineExplanation.Source.METRICS, "recordCount", 128L),
                        evidence(PipelineExplanation.Source.METRICS, "frontierStalledMillis", Map.of()),
                        evidence(PipelineExplanation.Source.SNAPSHOT, "rowsDone", 1L)),
                List.of(
                        "How old this observation is cannot be determined because it carries no observation time.",
                        "Whether the source has changes waiting is not measured.",
                        "Whether a paused run's job is still alive is not measured; failures are detected only "
                                + "while it is meant to run.",
                        "Whether an initial load is still running cannot be determined from loaded rows alone."),
                null, null);
        PipelineExplanation pending = new PipelineExplanation(
                "orders", PipelineState.NEW, PipelineExplanation.Kind.NO_MATCH,
                "No diagnostic rule matched.", FROM, 1_000L, PipelineExplanation.Freshness.FRESH,
                List.of(evidence(PipelineExplanation.Source.LIFECYCLE,
                        "pending", "START_CAPACITY")),
                List.of("Whether the source has changes waiting is not measured."), null,
                new PipelineExplanation.Pending(PipelineExplanation.PendingReason.START_CAPACITY));

        assertGolden(PipelineExplanationResponse.of(stale), "explain-stale.golden.json");
        assertGolden(PipelineExplanationResponse.of(coded), "explain-coded-failure.golden.json");
        assertGolden(PipelineExplanationResponse.of(reconcile), "explain-reconcile-failures.golden.json");
        assertGolden(PipelineExplanationResponse.of(noMovement), "explain-no-movement.golden.json");
        assertGolden(PipelineExplanationResponse.of(stalled), "explain-frontier-stalled.golden.json");
        assertGolden(PipelineExplanationResponse.of(noMatch), "explain-no-match.golden.json");
        assertGolden(PipelineExplanationResponse.of(unknown), "explain-unknown.golden.json");
        assertGolden(PipelineExplanationResponse.of(pending), "explain-start-pending.golden.json");
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
        try (var input = ObservabilityConsumerContractTest.class.getResourceAsStream(
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
