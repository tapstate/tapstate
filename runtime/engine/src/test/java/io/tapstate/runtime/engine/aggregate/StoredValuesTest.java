package io.tapstate.runtime.engine.aggregate;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

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
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class StoredValuesTest {

    @Test
    @DisplayName("every kind a row value can take comes back equal, of the same kind and scale")
    void everyKindComesBackEqual() throws IOException {
        Instant instant = Instant.parse("2026-03-04T05:06:07.123456789Z");
        OffsetDateTime offset = OffsetDateTime.of(2026, 3, 4, 5, 6, 7, 890, ZoneOffset.ofHours(-8));
        Map<String, Object> nested = new LinkedHashMap<>();
        nested.put("b", 2L);
        nested.put("a", Arrays.asList("x", null, new BigDecimal("1.50")));
        List<Object> values = Arrays.asList(
                null, 7L, new BigDecimal("12.3400"), new BigInteger("123456789012345678901234567890"),
                2.5d, true, false, "text", instant, offset,
                LocalDateTime.of(2026, 3, 4, 5, 6, 7, 8), LocalDate.of(2026, 3, 4), LocalTime.of(23, 59, 58, 7),
                Year.of(2026), List.of(1L, "two"), nested);

        List<Object> back = roundTrip(values);

        assertThat(back).hasSize(values.size());
        assertThat(back.get(0)).isNull();
        assertThat(back.get(1)).isEqualTo(7L);
        assertThat(back.get(2)).isEqualTo(new BigDecimal("12.3400"));
        assertThat(((BigDecimal) back.get(2)).scale()).isEqualTo(4);
        assertThat(back.get(3)).isEqualTo(new BigDecimal(new BigInteger("123456789012345678901234567890")));
        assertThat(back.subList(4, 10)).containsExactly(2.5d, true, false, "text", instant, offset);
        assertThat(back.subList(10, 16)).containsExactly(
                LocalDateTime.of(2026, 3, 4, 5, 6, 7, 8), LocalDate.of(2026, 3, 4), LocalTime.of(23, 59, 58, 7),
                Year.of(2026), List.of(1L, "two"), nested);
        assertThat(((Map<?, ?>) back.get(15)).keySet().stream().map(String::valueOf).toList()).containsExactly("b", "a");
    }

    @Test
    @DisplayName("narrower numbers come back as 64-bit, and zoned or old-style dates as their instant")
    void narrowerKindsWidenToTheirStoredForm() throws IOException {
        Instant instant = Instant.parse("2026-03-04T05:06:07Z");
        List<Object> back = roundTrip(Arrays.asList(
                Integer.valueOf(3), Short.valueOf((short) 4), Byte.valueOf((byte) 5), Float.valueOf(1.5f),
                ZonedDateTime.ofInstant(instant, ZoneId.of("America/Los_Angeles")), Date.from(instant)));

        assertThat(back).containsExactly(3L, 4L, 5L, 1.5d, instant, instant);
    }

    @Test
    @DisplayName("raw bytes come back with the same content")
    void bytesComeBackWithTheSameContent() throws IOException {
        List<Object> back = roundTrip(new ArrayList<>(List.of(new byte[] {1, 2, 3})));

        assertThat((byte[]) back.getFirst()).containsExactly(1, 2, 3);
    }

    @Test
    @DisplayName("a value of an unlisted kind is refused rather than stored as its text")
    void anUnlistedKindIsRefused() {
        DataOutputStream out = new DataOutputStream(new ByteArrayOutputStream());
        UUID unlisted = UUID.randomUUID();

        assertThatThrownBy(() -> StoredValues.write(out, unlisted))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(UUID.class.getName());
    }

    @Test
    @DisplayName("state naming an unknown kind or more bytes than are left fails to read")
    void corruptStateFailsToRead() {
        assertThatThrownBy(() -> StoredValues.read(input(new byte[] {99})))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("unknown kind 99");
        assertThatThrownBy(() -> StoredValues.read(input(new byte[] {12, 0, 0, 0, 9, 1})))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("9 bytes where fewer are left");
    }

    private static List<Object> roundTrip(List<Object> values) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        StoredValues.writeAll(new DataOutputStream(bytes), values);
        return StoredValues.readAll(input(bytes.toByteArray()));
    }

    private static DataInputStream input(byte[] bytes) {
        return new DataInputStream(new ByteArrayInputStream(bytes));
    }
}
