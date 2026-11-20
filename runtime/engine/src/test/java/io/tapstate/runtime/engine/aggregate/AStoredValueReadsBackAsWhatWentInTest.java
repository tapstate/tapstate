package io.tapstate.runtime.engine.aggregate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.Year;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * A total computed after a restart has to be the total that would have been computed without one, so
 * every value the step keeps must come back equal to what went in, of the same kind and scale.
 */
class AStoredValueReadsBackAsWhatWentInTest {

    @Test
    void valuesComeBackEqualAndOfTheSameKind() throws IOException {
        Instant instant = Instant.parse("2026-03-04T05:06:07.123456789Z");
        List<Object> same = Arrays.asList(
                null,
                Long.MIN_VALUE,
                new BigDecimal("-12345678901234567890.1200"),
                new BigDecimal("0E-9"),
                -0.0d,
                Double.NaN,
                "",
                "text with é and a surrogate pair 😀",
                true,
                false,
                instant,
                OffsetDateTime.of(2026, 3, 4, 5, 6, 7, 8, ZoneOffset.ofHoursMinutes(5, 30)),
                LocalDateTime.of(1969, 12, 31, 23, 59, 59, 999_999_999),
                LocalDate.of(-1, 1, 1),
                LocalTime.of(23, 59, 59, 1),
                Year.of(2026));

        for (Object value : same) {
            Object back = roundTrip(value);
            assertThat(back).isEqualTo(value);
            if (value != null) {
                assertThat(back).hasSameClassAs(value);
            }
        }
        // Equal decimals of different scales stay different: the scale is part of the value.
        assertThat(((BigDecimal) roundTrip(new BigDecimal("1.50"))).scale()).isEqualTo(2);
        assertThat((byte[]) roundTrip(new byte[] {0, -1, 127})).containsExactly(0, -1, 127);
    }

    @Test
    void narrowerKindsComeBackAsTheKindTheFunctionsComputeWith() throws IOException {
        assertThat(roundTrip(7)).isEqualTo(7L);
        assertThat(roundTrip((short) -7)).isEqualTo(-7L);
        assertThat(roundTrip((byte) 7)).isEqualTo(7L);
        assertThat(roundTrip(1.5f)).isEqualTo(1.5d);
        assertThat(roundTrip(new BigInteger("123456789012345678901234567890")))
                .isEqualTo(new BigDecimal("123456789012345678901234567890"));
        assertThat(roundTrip(new StringBuilder("built"))).isEqualTo("built");

        Instant instant = Instant.parse("2026-03-04T05:06:07.5Z");
        assertThat(roundTrip(ZonedDateTime.ofInstant(instant, ZoneId.of("Asia/Singapore")))).isEqualTo(instant);
        assertThat(roundTrip(Date.from(instant))).isEqualTo(instant);
    }

    @Test
    void listsAndMapsKeepTheirOrderAndWhatTheyHold() throws IOException {
        Map<Object, Object> map = new LinkedHashMap<>();
        map.put("z", 1);
        map.put("a", Arrays.asList(null, new BigDecimal("2.0"), List.of("deep")));
        map.put(3, null);

        Object back = roundTrip(List.of(map, "after"));

        Map<String, Object> expected = new LinkedHashMap<>();
        expected.put("z", 1L);
        expected.put("a", Arrays.asList(null, new BigDecimal("2.0"), List.of("deep")));
        expected.put("3", null);
        assertThat(back).isEqualTo(List.of(expected, "after"));
        @SuppressWarnings("unchecked")
        Map<String, Object> first = (Map<String, Object>) ((List<Object>) back).getFirst();
        assertThat(new ArrayList<>(first.keySet())).containsExactly("z", "a", "3");
    }

    @Test
    void aRowOfValuesComesBackWhole() throws IOException {
        List<Object> row = Arrays.asList("EU", null, 3L, new BigDecimal("9.90"));
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            StoredValues.writeAll(out, row);
        }

        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            assertThat(StoredValues.readAll(in)).isEqualTo(row);
            assertThat(in.available()).isZero();
        }
    }

    @Test
    void aValueOfAKindNotListedIsRefusedAndNotStoredAsItsText() {
        DataOutputStream out = new DataOutputStream(new ByteArrayOutputStream());

        assertThatThrownBy(() -> StoredValues.write(out, new Object()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("java.lang.Object")
                .hasMessageContaining("cannot be kept in aggregate state");
    }

    @Test
    void stateHoldingAnUnknownKindIsRefusedAndNotGuessedAt() {
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(new byte[] {(byte) 200}));

        assertThatThrownBy(() -> StoredValues.read(in))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("unknown kind 200");
    }

    private static Object roundTrip(Object value) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            StoredValues.write(out, value);
        }
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            Object back = StoredValues.read(in);
            assertThat(in.available()).isZero();
            return back;
        }
    }
}
