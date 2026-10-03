package io.tapstate.core.event;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class EventJsonValuesTest {

    @Test
    void unwrapsConvertedValuesAndPreservesTaggedBytesAcrossNestedContainers() {
        Object value = Map.of("converted", new ConvertedValue(7, "varchar"), "binary", new Bytes((byte) 4,
                new byte[] {1, 2}), "nested", new Object[] {new int[] {3, 4}, true});
        assertThat(EventJsonValues.normalize(value)).isEqualTo(Map.of(
                "converted", 7, "binary", Map.of("$binary", "AQI=", "$tag", (byte) 4),
                "nested", List.of(List.of(3, 4), true)));
    }

    @Test
    void measuresPortableValuesWithEscapingDatesAndBoundedOutput() {
        Object value = List.of(new ConvertedValue(123, "int"), new Bytes((byte) 1, new byte[] {1}),
                Date.from(Instant.parse("2026-01-01T00:00:00Z")), "😀\n");
        long exact = EventJsonValues.encodedSize(value, Long.MAX_VALUE);
        assertThat(EventJsonValues.encodedSize(value, exact)).isEqualTo(exact);
        assertThat(EventJsonValues.encodedSize(value, exact - 1)).isEqualTo(exact);
        assertThat(EventJsonValues.encodedSize(null, 3)).isEqualTo(4);
        assertThat(EventJsonValues.encodedSize(new Bytes((byte) 0, new byte[0]), Long.MAX_VALUE)).isPositive();
    }

    @Test
    void rejectsNonStringFieldsUnsupportedValuesAndExcessiveNesting() {
        assertThatThrownBy(() -> EventJsonValues.normalize(Map.of(1, "value")))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> EventJsonValues.encodedSize(Map.of(1, "value"), 100))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> EventJsonValues.normalize(new Object()))
                .isInstanceOf(IllegalArgumentException.class);
        Object nested = "leaf";
        for (int index = 0; index < 130; index++) {
            nested = List.of(nested);
        }
        Object tooDeep = nested;
        assertThatThrownBy(() -> EventJsonValues.normalize(tooDeep)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> EventJsonValues.encodedSize(tooDeep, Long.MAX_VALUE))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> EventJsonValues.encodedSize("value", -1))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
