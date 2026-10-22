package io.tapstate.adapters.pdk;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.Bytes;
import io.tapstate.core.event.ConvertedValue;
import io.tapstate.core.event.Envelope;
import io.tapstate.spi.sink.TargetTable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MongoIdentityKeysTest {
    static final TargetIdentityPlan PLAN = new TargetIdentityPlan("automatic", "generated-target-identity",
            new TargetTable("orders", List.of()), "_id", "original", List.of("original", "k"), false, true);

    @Test
    void everyImageAndEveryKeyRejectsValuesThatNativeEqualityCanCollapse() {
        var valid = Map.<String, Object>of("_id", "p", "k", 1L);
        for (Object invalid : Arrays.asList(null, List.of("a"), Map.of("x", "a"), new Object[] {"a"},
                new byte[] {1}, 1, (short) 1, (byte) 1, 1.0, 1.0f, BigDecimal.ONE, BigInteger.ONE,
                Instant.ofEpochSecond(0, 1), new ConvertedValue("a", "date", "TapDateValue", null, null),
                new ConvertedValue("a", null, "TapRawValue", null, null), new ConvertedValue(1L, null, "unknown", null, null),
                new ConvertedValue("A12345678901234567890123A", "OBJECT_ID"))) {
            for (String field : List.of("_id", "k")) {
                Map<String, Object> bad = new LinkedHashMap<>(valid);
                bad.put(field, invalid);
                for (Envelope event : List.of(Envelope.insert(1L, "orders", bad, null),
                        Envelope.update(2L, "orders", bad, valid, null),
                        Envelope.update(2L, "orders", valid, bad, null), Envelope.delete(3L, "orders", bad, null))) {
                    assertThatThrownBy(() -> TargetIdentityProjection.apply(PLAN, event))
                            .isInstanceOfSatisfying(TapstateException.class, error ->
                                    assertThat(error.args()).containsEntry("reason", "key-representation-unsupported"));
                }
            }
        }
    }

    @Test
    void extendedJsonKeepsIntegerObjectIdBinaryAndLiteralStringPredicatesDistinct() {
        assertThat(key(Long.MAX_VALUE)).isEqualTo(Map.of("$eq", Map.of("$numberLong", Long.toString(Long.MAX_VALUE))));
        assertThat(key(true)).isEqualTo(Map.of("$eq", true));
        assertThat(key("$literal")).isEqualTo(Map.of("$eq", "$literal"));
        String hex = "012345678901234567890123";
        assertThat(key(new ConvertedValue(hex, "OBJECT_ID", "TapStringValue", null, null)))
                .isEqualTo(Map.of("$eq", Map.of("$oid", hex))).isNotEqualTo(key(hex));
        assertThat(key(new ConvertedValue(new Bytes((byte) 0x80, new byte[] {1, 2}), "BINARY", "TapBinaryValue", null, null)))
                .isEqualTo(Map.of("$eq", Map.of("$binary", Map.of("base64", "AQI=", "subType", "80"))));
        for (Object value : List.of(Long.MIN_VALUE, new ConvertedValue(42L, "LONG", "TapNumberValue", null, null),
                new ConvertedValue(true, "BOOLEAN", "TapBooleanValue", null, null))) assertThat(key(value)).isNotNull();
    }

    @Test
    void absentKeysImagesAndNonLiteralFieldNamesCannotBypassTheGuard() {
        assertThatThrownBy(() -> TargetIdentityProjection.row(PLAN, Map.of("_id", "p"))).isInstanceOf(TapstateException.class);
        assertThatThrownBy(() -> TargetIdentityProjection.apply(PLAN, Envelope.delete(1L, "orders", null, null)))
                .isInstanceOf(TapstateException.class);
        assertThatThrownBy(() -> TargetIdentityProjection.apply(PLAN, Envelope.insert(1L, "orders", null, null)))
                .isInstanceOf(TapstateException.class);
        for (String field : List.of("a.b", "$key", "", "a\0b")) {
            var plan = new TargetIdentityPlan("automatic", "generated-target-identity", PLAN.target(), "_id", "original",
                    List.of("original", field), false, true);
            assertThatThrownBy(() -> MongoIdentityKeys.key(plan, Map.of("original", "p", field, "x")))
                    .isInstanceOfSatisfying(TapstateException.class, error -> assertThat(error.args()).containsEntry("reason", "invalid-field-name"));
        }
    }

    @Test
    void temporalKeysRequireMillisecondPrecisionAndConsistentCarriedTime() {
        var instant = Instant.ofEpochMilli(1700000000123L);
        var expected = Map.of("$eq", Map.of("$date", Map.of("$numberLong", "1700000000123")));
        assertThat(key(instant)).isEqualTo(expected);
        assertThat(key(new ConvertedValue(instant, "DATE"))).isEqualTo(expected);
        String time = "{\"seconds\":1700000000,\"nano\":123000000,\"origin\":0,\"fraction\":3}";
        assertThat(key(new ConvertedValue(instant, "DATE", "TapDateTimeValue", null, time))).isEqualTo(expected);
        for (Object invalid : List.of(Instant.MAX, new ConvertedValue(instant, "DATE", "TapDateTimeValue", null, "{}"),
                new ConvertedValue(instant, "DATE", "TapDateTimeValue", null, time.replace("123000000", "124000000")),
                new ConvertedValue(instant, "DATE", "TapDateValue", null, "invalid"),
                new ConvertedValue("not a date", "DATE", "TapStringValue", null, time))) {
            assertThatThrownBy(() -> key(invalid)).isInstanceOf(TapstateException.class);
        }
    }

    @Test
    void nativeKeyValidationChecksActualTypeAndContentWithoutTrustingDeclaredOrigin() {
        var conversions = new PdkValueConversions(new io.tapdata.entity.codec.TapCodecsRegistry());
        var portable = Map.<String, Object>of("original", "p", "k", 1L);
        var envelope = Envelope.insert(1L, "orders", portable, null);
        MongoIdentityKeys.verifyEncoded(conversions, PLAN, envelope,
                io.tapdata.entity.event.dml.TapInsertRecordEvent.create().table("orders").after(portable));
        for (Object different : List.of(2L, 1.0, "1")) {
            assertThatThrownBy(() -> MongoIdentityKeys.verifyEncoded(conversions, PLAN, envelope,
                    io.tapdata.entity.event.dml.TapInsertRecordEvent.create().table("orders").after(Map.of("original", "p", "k", different))))
                    .isInstanceOf(TapstateException.class);
        }
        var objectId = Map.<String, Object>of("original", "p", "k", new ConvertedValue("012345678901234567890123", "OBJECT_ID"));
        assertThatThrownBy(() -> MongoIdentityKeys.verifyEncoded(conversions, PLAN,
                Envelope.insert(1L, "orders", objectId, null), io.tapdata.entity.event.dml.TapInsertRecordEvent.create().table("orders")
                        .after(Map.of("original", "p", "k", "012345678901234567890123"))))
                .isInstanceOf(TapstateException.class);
    }

    @Test
    void updatePredicatesCannotChangeOnlyTheirNativeRepresentation() {
        String hex = "012345678901234567890123";
        var before = Map.<String, Object>of("_id", "p", "k", new ConvertedValue(hex, "OBJECT_ID"));
        var after = Map.<String, Object>of("_id", "p", "k", hex);
        assertThatThrownBy(() -> TargetIdentityProjection.apply(PLAN, Envelope.update(1L, "orders", before, after, null)))
                .isInstanceOf(TapstateException.class);
        assertThatThrownBy(() -> TargetIdentityProjection.apply(PLAN, Envelope.update(1L, "orders", after, before, null)))
                .isInstanceOf(TapstateException.class);
    }

    private static Object key(Object value) {
        return MongoIdentityKeys.key(PLAN, Map.of("original", "p", "k", value)).get("k");
    }
}
