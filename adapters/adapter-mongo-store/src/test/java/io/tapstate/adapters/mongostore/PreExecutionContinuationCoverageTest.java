package io.tapstate.adapters.mongostore;

import io.tapstate.core.lifecycle.MetricAttributes;
import io.tapstate.core.lifecycle.MetricFact;
import io.tapstate.core.lifecycle.MetricPoint;
import io.tapstate.core.lifecycle.MetricType;
import io.tapstate.core.lifecycle.Observation;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.spi.store.ObservationContinuation;
import io.tapstate.spi.store.ObservationStore;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

class PreExecutionContinuationCoverageTest {
    private static final Instant START = Instant.parse("2026-10-08T00:00:00Z"), AT = START.plusSeconds(30);
    private static final ObservationStore.Scope SOURCE = new ObservationStore.Scope("inc-a", 1);

    @Test
    void aKnownFloorCannotHideANewerPublicTotalOrAnotherAccumulationStart() {
        assertThat(MongoPreExecutionFailureStorage.coversPublicSource(carrier(7), publicSource(7, START))).isTrue();
        assertThat(MongoPreExecutionFailureStorage.coversPublicSource(carrier(7), publicSource(8, START))).isFalse();
        assertThat(MongoPreExecutionFailureStorage.coversPublicSource(carrier(8), publicSource(8, START))).isTrue();
        assertThat(MongoPreExecutionFailureStorage.coversPublicSource(carrier(8), publicSource(7, START.plusSeconds(1)))).isFalse();
    }

    @Test
    void unknownIsNotZeroAndCannotOverwriteQualifiedKnownCounters() {
        var unknown = carrier(null);
        assertThat(MongoPreExecutionFailureStorage.coversPublicSource(unknown, publicSource(0, START))).isFalse();
        assertThat(MongoPreExecutionFailureStorage.coversPublicSource(unknown, publicSource(7, START))).isFalse();
        var absent = new ObservationStore.Stored(new Observation("orders", PipelineState.STOPPED,
                Map.of(), Map.of(), Map.of(), null, AT, List.of()), Optional.of(SOURCE));
        assertThat(MongoPreExecutionFailureStorage.coversPublicSource(unknown, absent)).isTrue();
    }

    private static ObservationStore.StoredContinuation carrier(Integer total) {
        var value = new ObservationContinuation("handoff", SOURCE, Optional.empty(), Optional.empty(),
                total == null ? List.of() : List.of(fact(total, START)), List.of());
        return new ObservationStore.StoredContinuation(value, new ObservationStore.ContinuationReceipt("orders",
                "revision", "a".repeat(64), 3, "handoff", SOURCE, Optional.empty(), Optional.empty(), total != null));
    }
    private static ObservationStore.Stored publicSource(int total, Instant start) {
        return new ObservationStore.Stored(new Observation("orders", PipelineState.STOPPED, Map.of("records.out", (long) total),
                Map.of(), Map.of(), null, AT, List.of(fact(total, start))), Optional.of(SOURCE));
    }
    private static MetricFact fact(long total, Instant start) {
        return MetricFact.single("tapstate.pipeline.records", MetricType.COUNTER, "{record}",
                MetricPoint.accumulated(Map.of(MetricAttributes.PIPELINE_ID, "orders", MetricAttributes.DIRECTION, "out",
                        MetricAttributes.TABLE_ID, "orders"), start, AT, total));
    }
}
