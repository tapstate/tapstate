package io.tapstate.e2e;

import io.tapstate.core.common.JsonReader;
import io.tapstate.core.common.JsonWriter;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Missing profiler measurements must never become successful zero-cost evidence. */
class HistoryQueryBenchmarkEvidenceTest {
    @ParameterizedTest
    @ValueSource(strings = {"keysExamined", "docsExamined", "responseLength"})
    void unavailableProfileMeasurementInvalidatesTheReading(String field) {
        assertThatThrownBy(() -> HistoryQueryBenchmarkIT.count(new Document(), field))
                .isInstanceOf(AssertionError.class).hasMessageContaining(field);
    }

    @ParameterizedTest
    @ValueSource(strings = {"keysExamined", "docsExamined", "responseLength"})
    void integralZeroIsARealMeasurementButMalformedOrNegativeCountsAreNot(String field) {
        assertThat(HistoryQueryBenchmarkIT.count(new Document(field, 0), field)).isZero();
        assertThat(HistoryQueryBenchmarkIT.count(new Document(field, 12L), field)).isEqualTo(12L);
        for (Object invalid : new Object[] {"0", 1.5, -1, -1L}) {
            assertThatThrownBy(() -> HistoryQueryBenchmarkIT.count(new Document(field, invalid), field))
                    .isInstanceOf(AssertionError.class).hasMessageContaining(field);
        }
    }

    @Test
    void allReadingsKeepTheirExecutionOrderLatenciesAndActualReplyBytes() {
        Map<String, Object> bounds = Map.of("effectiveFrom", "2026-09-10T00:00:00Z",
                "effectiveTo", "2026-09-11T00:00:00Z", "retentionCutoff", "2026-09-09T00:00:00Z");
        var first = new HistoryQueryBenchmarkIT.Reading(bounds, 900_000_000L, 401,
                new HistoryQueryBenchmarkIT.CollectionCost(2, 18, 16, 123, List.of()),
                new HistoryQueryBenchmarkIT.CollectionCost(1, 6, 5, 456, List.of()));
        var second = new HistoryQueryBenchmarkIT.Reading(bounds, 100_000_000L, 402,
                new HistoryQueryBenchmarkIT.CollectionCost(0, 0, 0, 0, List.of()),
                new HistoryQueryBenchmarkIT.CollectionCost(1, 8, 7, 789, List.of()));
        Object parsed = JsonReader.parse(JsonWriter.write(
                HistoryQueryBenchmarkIT.readingsEvidence(List.of(first, second))));
        assertThat(parsed).isInstanceOf(List.class);
        List<?> readings = (List<?>) parsed;
        assertThat(readings).hasSize(2);
        assertReading((Map<?, ?>) readings.get(0), 900_000_000L, 401, 123, 456);
        assertReading((Map<?, ?>) readings.get(1), 100_000_000L, 402, 0, 789);
    }

    private static void assertReading(Map<?, ?> reading, long nanos, int httpBytes,
            long rawReplyBytes, long rollupReplyBytes) {
        assertThat(((Number) reading.get("elapsedNanos")).longValue()).isEqualTo(nanos);
        assertThat(((Number) reading.get("httpBytes")).intValue()).isEqualTo(httpBytes);
        assertThat(reading.get("httpBytesSource")).isEqualTo("HTTP_RESPONSE_ENTITY_BODY");
        assertThat(reading.get("httpBytesScope")).isEqualTo("PAYLOAD_ONLY");
        assertThat(((Number) ((Map<?, ?>) reading.get("raw")).get("replyBytes")).longValue())
                .isEqualTo(rawReplyBytes);
        assertThat(((Number) ((Map<?, ?>) reading.get("rollup")).get("replyBytes")).longValue())
                .isEqualTo(rollupReplyBytes);
        assertThat(reading.get("effectiveFrom")).isEqualTo("2026-09-10T00:00:00Z");
        assertThat(reading.get("effectiveTo")).isEqualTo("2026-09-11T00:00:00Z");
        assertThat(reading.get("retentionCutoff")).isEqualTo("2026-09-09T00:00:00Z");
    }
}
