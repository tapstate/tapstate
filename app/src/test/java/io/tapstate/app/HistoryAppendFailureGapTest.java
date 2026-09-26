package io.tapstate.app;

import io.tapstate.control.core.ArtifactQueryService;
import io.tapstate.control.core.HistoryCursorCodec;
import io.tapstate.control.core.HistoryResolution;
import io.tapstate.control.core.PipelineHistoryQuery;
import io.tapstate.control.core.PipelineHistoryQueryService;
import io.tapstate.control.core.PipelineMetricsHistory;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.lifecycle.MetricFact;
import io.tapstate.core.lifecycle.MetricPoint;
import io.tapstate.core.lifecycle.MetricType;
import io.tapstate.core.lifecycle.Observation;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.model.Resource;
import io.tapstate.runtime.scheduler.RateSampler;
import io.tapstate.spi.store.ArtifactStore;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class HistoryAppendFailureGapTest {

    private static final Instant START = Instant.parse("2026-09-21T09:00:00Z");
    private static final Instant T0 = Instant.parse("2026-09-21T10:00:00Z");

    @Test
    void failedAppendThenQuickRecoveryProducesARawAndAggregateGap() {
        InMemoryRateHistoryStore history = new InMemoryRateHistoryStore();
        RateSampler sampler = new RateSampler(history, Duration.ofMinutes(1));
        assertThat(sampler.appendIfDue(moving(T0, 100))).isTrue();

        history.failNextAppend();
        assertThatThrownBy(() -> sampler.appendIfDue(moving(T0.plusSeconds(60), 160)))
                .hasMessage("injected history append failure");
        assertThat(sampler.appendIfDue(moving(T0.plusSeconds(61), 161))).isTrue();
        assertThat(sampler.appendIfDue(moving(T0.plusSeconds(121), 221))).isTrue();

        PipelineHistoryQueryService service = service(history);
        PipelineMetricsHistory raw = query(service, HistoryResolution.RAW);
        assertGap(raw);
        assertThat(raw.segments()).extracting(PipelineMetricsHistory.Segment::startReason)
                .containsExactly(PipelineMetricsHistory.StartReason.WINDOW_START,
                        PipelineMetricsHistory.StartReason.GAP);
        assertThat(raw.segments().get(1).points().getFirst().recordsOut()).isNull();
        assertThat(raw.segments().get(1).points().getLast().recordsOut().delta())
                .isEqualByComparingTo("60");

        PipelineMetricsHistory aggregate = query(service, HistoryResolution.PT5M);
        assertGap(aggregate);
        assertThat(aggregate.segments()).extracting(PipelineMetricsHistory.Segment::startReason)
                .containsExactly(PipelineMetricsHistory.StartReason.WINDOW_START,
                        PipelineMetricsHistory.StartReason.GAP);
        assertThat(aggregate.segments().get(1).points().getFirst().recordsOut().delta())
                .isEqualByComparingTo("60");
    }

    @Test
    void firstRecoveredSampleCarriesTheGapWithoutAPredecessor() {
        InMemoryRateHistoryStore history = new InMemoryRateHistoryStore();
        RateSampler sampler = new RateSampler(history, Duration.ofMinutes(1));
        history.failNextAppend();
        assertThatThrownBy(() -> sampler.appendIfDue(moving(T0, 100)))
                .hasMessage("injected history append failure");
        assertThat(sampler.appendIfDue(moving(T0.plusSeconds(1), 101))).isTrue();

        PipelineMetricsHistory raw = query(service(history), HistoryResolution.RAW);
        assertThat(raw.segments()).extracting(PipelineMetricsHistory.Segment::startReason)
                .containsExactly(PipelineMetricsHistory.StartReason.GAP);
        assertThat(raw.gaps()).singleElement().satisfies(gap -> {
            assertThat(gap.intervalStart()).isEqualTo(T0);
            assertThat(gap.intervalEnd()).isEqualTo(T0.plusSeconds(1));
        });
    }

    private static void assertGap(PipelineMetricsHistory history) {
        assertThat(history.gaps()).singleElement().satisfies(gap -> {
            assertThat(gap.intervalStart()).isEqualTo(T0);
            assertThat(gap.intervalEnd()).isEqualTo(T0.plusSeconds(61));
            assertThat(gap.reason()).isEqualTo(PipelineMetricsHistory.GapReason.SAMPLE_GAP);
        });
    }

    private static PipelineMetricsHistory query(PipelineHistoryQueryService service, HistoryResolution resolution) {
        return service.query(new PipelineHistoryQuery("orders", T0, T0.plusSeconds(180),
                resolution, 10, List.of(), null));
    }

    private static Observation moving(Instant at, long recordsOut) {
        MetricFact records = new MetricFact("tapstate.pipeline.records", MetricType.COUNTER, "{record}",
                List.of(MetricPoint.accumulated(Map.of("direction", "out"), START, at, recordsOut)));
        return new Observation("orders", PipelineState.RUNNING,
                Map.of("records.out", recordsOut, "bytes.out", recordsOut * 10),
                Map.of(), Map.of(), null, at, List.of(records));
    }

    private static PipelineHistoryQueryService service(InMemoryRateHistoryStore history) {
        Resource pipeline = new DslParser().parse("""
                version: tapstate/v1
                kind: pipeline
                id: orders
                source: source
                serve:
                  from: /.*/
                  sync:
                    - id: sink
                      source: target
                      write_mode: upsert
                      ddl: apply
                """);
        ArtifactQueryService artifacts = new ArtifactQueryService(new ArtifactStore() {
            @Override
            public void saveAll(List<Resource> resources) {
                throw new UnsupportedOperationException();
            }

            @Override
            public Optional<Resource> get(String id) {
                return pipeline.id().equals(id) ? Optional.of(pipeline) : Optional.empty();
            }

            @Override
            public List<Resource> list() {
                return List.of(pipeline);
            }
        });
        Clock clock = Clock.fixed(T0.plusSeconds(600), ZoneOffset.UTC);
        return new PipelineHistoryQueryService(artifacts, history, Duration.ofMinutes(1), clock,
                new HistoryCursorCodec("history-append-gap".getBytes(StandardCharsets.UTF_8), clock));
    }
}
