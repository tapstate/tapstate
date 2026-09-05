package io.tapstate.adapters.pdk;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.tapstate.core.common.TapstateException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * The encoding a connector's own notes are stored under. What matters is not that a value survives but
 * that it survives <em>as the type it was written as</em>: the callers that read these notes back branch
 * on the runtime type, so a codec that widens or narrows one is indistinguishable from a codec that does
 * not store at all - except that it fails later and somewhere else.
 */
class WhatAConnectorPutsInItsStateComesBackAsTheSameTypeTest {

    @Test
    void aStringComesBackAString() {
        assertThat(roundTrip("tapdata_cdc_599ce166")).isEqualTo("tapdata_cdc_599ce166");
    }

    @Test
    void aLongComesBackALongAndNotAnInteger() {
        Object back = roundTrip(1757030400000L);
        // The whole point of a tagged encoding rather than JSON: a reader doing `instanceof Long` gets
        // null from a widened Integer, and a reader casting gets a ClassCastException far from here.
        assertThat(back).isInstanceOf(Long.class).isEqualTo(1757030400000L);
    }

    @Test
    void anIntegerComesBackAnIntegerAndNotALong() {
        Object back = roundTrip(7);
        assertThat(back).isInstanceOf(Integer.class).isEqualTo(7);
    }

    @Test
    void aSmallLongIsStillALong() {
        // The pair that a size-based encoding would collapse: 7L and 7 must not meet in the middle.
        assertThat(roundTrip(7L)).isInstanceOf(Long.class);
        assertThat(roundTrip(7)).isInstanceOf(Integer.class);
    }

    @Test
    void aBooleanComesBackABoolean() {
        assertThat(roundTrip(true)).isInstanceOf(Boolean.class).isEqualTo(true);
        assertThat(roundTrip(false)).isInstanceOf(Boolean.class).isEqualTo(false);
    }

    @Test
    void aDoubleComesBackADouble() {
        assertThat(roundTrip(2.5d)).isInstanceOf(Double.class).isEqualTo(2.5d);
    }

    @Test
    void bytesComeBackAsBytesAndNotAsText() {
        // A compressed schema history is stored as bytes and read back with `instanceof byte[]`. Base64
        // through a text encoding reads back a String, the check fails, and the history is silently
        // ignored - a full re-read with nothing reported.
        byte[] history = {0x1f, (byte) 0x8b, 0x08, 0x00, 0x00};
        Object back = roundTrip(history);
        assertThat(back).isInstanceOf(byte[].class);
        assertThat((byte[]) back).containsExactly(history);
    }

    @Test
    void aListComesBackAListWithItsElementTypesIntact() {
        List<Object> written = List.of("orders", 3L, true);
        Object back = roundTrip(written);
        assertThat(back).isInstanceOf(List.class).isEqualTo(written);
        assertThat(((List<?>) back).get(1)).isInstanceOf(Long.class);
    }

    @Test
    void aMapComesBackAMapWithItsValueTypesIntact() {
        Map<String, Object> written = new LinkedHashMap<>();
        written.put("slot", "tapdata_cdc_1");
        written.put("cp", 12L);
        Object back = roundTrip(written);
        assertThat(back).isInstanceOf(Map.class).isEqualTo(written);
        assertThat(((Map<?, ?>) back).get("cp")).isInstanceOf(Long.class);
    }

    @Test
    void aNestedShapeSurvivesWhole() {
        Map<String, Object> inner = new LinkedHashMap<>();
        inner.put("n", 1);
        Object back = roundTrip(List.of(inner));
        assertThat(back).isEqualTo(List.of(inner));
    }

    @Test
    void aTypeOutsideTheSetIsRefusedRatherThanStringified() {
        // The closed set is the contract. Falling back to toString() would store something that reads
        // back as a String and looks plausible to whoever wrote a date or a BigDecimal.
        assertThatThrownBy(() -> ConnectorStateCodec.encode(new java.math.BigDecimal("1.5")))
                .isInstanceOf(TapstateException.class)
                .hasMessageContaining("java.math.BigDecimal");
    }

    @Test
    void aTagThisBuildDoesNotKnowIsRefusedRatherThanGuessed() {
        byte[] fromTheFuture = {0x01, 0x7f, 0x00};
        assertThatThrownBy(() -> ConnectorStateCodec.decode(fromTheFuture))
                .isInstanceOf(TapstateException.class);
    }

    @Test
    void aFormatVersionThisBuildDoesNotKnowIsRefused() {
        byte[] fromTheFuture = {0x02, 0x01, 0x61};
        assertThatThrownBy(() -> ConnectorStateCodec.decode(fromTheFuture))
                .isInstanceOf(TapstateException.class);
    }

    private static Object roundTrip(Object value) {
        return ConnectorStateCodec.decode(ConnectorStateCodec.encode(value));
    }
}
