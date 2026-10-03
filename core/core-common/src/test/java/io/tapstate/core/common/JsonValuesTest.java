package io.tapstate.core.common;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class JsonValuesTest {

    @Test
    void normalizesPortableJsonValuesAndPrimitiveArrays() {
        assertThat(JsonValues.normalize(Map.of(
                "text", 'x', "date", Date.from(Instant.parse("2026-01-01T00:00:00Z")),
                "time", Instant.parse("2026-01-02T00:00:00Z"), "bytes", new byte[] {1, 2},
                "values", new Object[] {1, true, new int[] {2, 3}})))
                .isEqualTo(Map.of(
                        "text", "x", "date", "2026-01-01T00:00:00Z", "time", "2026-01-02T00:00:00Z",
                        "bytes", Map.of("$binary", "AQI="), "values", List.of(1, true, List.of(2, 3))));
    }

    @Test
    void measuresExactCompactUtf8JsonAndStopsAtTheLimit() {
        Object value = Map.of("quote", "a\n\"", "emoji", "\uD83D\uDE00", "number", 12);
        long exact = JsonValues.encodedSize(value, Long.MAX_VALUE);
        assertThat(exact).isPositive();
        assertThat(JsonValues.encodedSize(value, exact)).isEqualTo(exact);
        assertThat(JsonValues.encodedSize(value, exact - 1)).isEqualTo(exact);
        assertThat(JsonValues.encodedSize("long", 1)).isEqualTo(2);
        assertThat(JsonValues.encodedSize(null, 3)).isEqualTo(4);
        assertThat(JsonValues.encodedSize(new byte[] {1}, Long.MAX_VALUE)).isEqualTo(18);
    }

    @Test
    void rejectsInvalidNumbersKeysValuesDepthAndNegativeLimits() {
        assertThatThrownBy(() -> JsonValues.normalize(Double.NaN)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> JsonValues.normalize(Float.POSITIVE_INFINITY))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> JsonValues.normalize(Map.of(1, "value")))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> JsonValues.normalize(new Object()))
                .isInstanceOf(IllegalArgumentException.class);
        Object nested = "leaf";
        for (int index = 0; index < 130; index++) {
            nested = List.of(nested);
        }
        Object tooDeep = nested;
        assertThatThrownBy(() -> JsonValues.normalize(tooDeep)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> JsonValues.encodedSize("value", -1))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> JsonValues.encodedSize(Double.NaN, 100))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void handlesEveryJsonScalarAndStringEscapeInNormalizationAndSizeMeasurement() {
        String escaped = "\"\\\b\f\n\r\t\u0001é€";
        Object value = new Object[] {null, false, 'x', new boolean[] {true, false}, escaped,
                "\uD83D\uDE00", "\uD800"};

        assertThat(JsonValues.normalize(value)).isEqualTo(java.util.Arrays.asList(
                null, false, "x", List.of(true, false), escaped, "\uD83D\uDE00", "\uD800"));
        String json = io.tapstate.core.common.JsonWriter.write(JsonValues.normalize(value));
        assertThat(JsonValues.encodedSize(value, Long.MAX_VALUE))
                .isEqualTo(json.getBytes(java.nio.charset.StandardCharsets.UTF_8).length);
        assertThat(JsonValues.encodedSize(Map.of(), 0)).isEqualTo(1);
        assertThat(JsonValues.encodedSize(List.of(), 0)).isEqualTo(1);
        assertThat(JsonValues.encodedSize(new byte[0], 2)).isEqualTo(3);
    }

    @Test
    void boundsMeasurementDepthAndPreservesSaturatedLongLimit() {
        Object nested = "leaf";
        for (int index = 0; index < 130; index++) {
            nested = List.of(nested);
        }
        Object tooDeep = nested;
        assertThatThrownBy(() -> JsonValues.encodedSize(tooDeep, Long.MAX_VALUE))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(JsonValues.encodedSize("x", Long.MAX_VALUE)).isEqualTo(3);
        assertThat(JsonValues.encodedSize("x", 0)).isEqualTo(1);
    }
}
