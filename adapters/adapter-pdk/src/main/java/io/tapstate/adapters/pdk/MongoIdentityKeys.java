package io.tapstate.adapters.pdk;

import io.tapstate.core.event.Bytes;
import io.tapstate.core.event.Envelope;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import java.time.Instant;
import io.tapstate.core.event.ConvertedValue;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Canonical scalar keys that retain distinct identities under document equality and unique indexes. */
final class MongoIdentityKeys {
    private MongoIdentityKeys() { }

    static Map<String, Object> key(TargetIdentityPlan plan, Map<String, Object> image) {
        Map<String, Object> key = new LinkedHashMap<>();
        for (String field : plan.writeKey()) {
            if (field.isBlank() || field.contains(".") || field.startsWith("$") || field.indexOf('\0') >= 0) {
                throw TargetIdentityProjection.failure(plan, "invalid-field-name");
            }
            key.put(field, Map.of("$eq", scalar(plan, image.get(field))));
        }
        return key;
    }

    private static Object scalar(TargetIdentityPlan plan, Object carried) {
        ConvertedValue carrier = carried instanceof ConvertedValue converted ? converted : null;
        Object value = carrier == null ? carried : carrier.value();
        String kind = carrier == null ? null : carrier.valueKind();
        if (value instanceof Instant instant) return instant(plan, carrier, instant);
        if (carrier != null && carrier.temporalDescription() != null) throw unsupported(plan);
        if (value instanceof Long number && kindMatches(kind, "TapNumberValue")) {
            return Map.of("$numberLong", number.toString());
        }
        if (value instanceof Boolean bool && kindMatches(kind, "TapBooleanValue")) return bool;
        if (value instanceof String text && kindMatches(kind, "TapStringValue")) return string(plan, carrier, text);
        if (value instanceof Bytes bytes && carrier != null && kindMatches(kind, "TapBinaryValue")) {
            return Map.of("$binary", Map.of("base64", Base64.getEncoder().encodeToString(bytes.value()),
                    "subType", String.format(java.util.Locale.ROOT, "%02x", bytes.tag() & 0xff)));
        }
        throw unsupported(plan);
    }

    private static Object string(TargetIdentityPlan plan, ConvertedValue carrier, String text) {
        if (carrier == null || !"OBJECT_ID".equals(carrier.originType())) return text;
        if (!text.matches("[0-9a-f]{24}")) throw unsupported(plan);
        return Map.of("$oid", text);
    }

    private static Object instant(TargetIdentityPlan plan, ConvertedValue carrier, Instant instant) {
        if (carrier == null) return instantValue(plan, instant);
        String kind = carrier.valueKind();
        if (kind == null) {
            if (carrier.temporalDescription() != null) throw unsupported(plan);
            return instantValue(plan, instant);
        }
        if (!List.of("TapDateTimeValue", "TapDateValue", "TapTimeValue").contains(kind)) throw unsupported(plan);
        validateCarriedTime(plan, carrier, instant);
        return instantValue(plan, instant);
    }

    private static void validateCarriedTime(TargetIdentityPlan plan, ConvertedValue carrier, Instant instant) {
        try {
            Object parsed = io.tapstate.core.common.JsonReader.parse(carrier.temporalDescription());
            if (!(parsed instanceof Map<?, ?> state) || state.get("illegal") != null
                    || !(state.get("seconds") instanceof Number seconds)
                    || !(state.get("nano") instanceof Number nano)
                    || !instant.equals(Instant.ofEpochSecond(seconds.longValue(), nano.longValue()))) throw unsupported(plan);
        } catch (RuntimeException invalid) {
            throw unsupported(plan);
        }
    }

    private static Object instantValue(TargetIdentityPlan plan, Instant instant) {
        if (instant.getNano() % 1_000_000 != 0) throw unsupported(plan);
        try {
            return Map.of("$date", Map.of("$numberLong", Long.toString(instant.toEpochMilli())));
        } catch (ArithmeticException invalid) {
            throw unsupported(plan);
        }
    }

    static void verifyEncoded(PdkValueConversions conversions, TargetIdentityPlan plan, Envelope projected,
            TapRecordEvent encoded) {
        if (encoded instanceof TapInsertRecordEvent insert) verifyImage(conversions, plan, projected.after(), insert.getAfter());
        else if (encoded instanceof TapDeleteRecordEvent delete) verifyImage(conversions, plan, projected.before(), delete.getBefore());
        else if (encoded instanceof TapUpdateRecordEvent update) {
            verifyImage(conversions, plan, projected.before(), update.getBefore());
            verifyImage(conversions, plan, projected.after(), update.getAfter());
        }
    }

    private static void verifyImage(PdkValueConversions conversions, TargetIdentityPlan plan,
            Map<String, Object> projected, Map<String, Object> nativeImage) {
        if (projected == null && nativeImage == null) return;
        if (projected == null || nativeImage == null) throw unsupported(plan);
        Map<String, Object> nativeKey = new LinkedHashMap<>();
        plan.writeKey().forEach(field -> nativeKey.put(field, nativeImage.get(field)));
        try {
            // Decode only the actual encoded keys through the same frozen conversion contract.
            // This verifies binary subtype/content and temporal precision without importing driver classes.
            var decoded = TapEventCodec.decodeSnapshotRow(TapInsertRecordEvent.create().table(plan.target().name())
                    .after(nativeKey), conversions, PdkRowSchema.EMPTY).after();
            for (String field : plan.writeKey()) {
                Object carried = projected.get(field);
                Object portable = ConvertedValue.unwrap(carried);
                if (!java.util.Objects.equals(portable, ConvertedValue.unwrap(decoded.get(field)))
                        || !nativeTypeMatches(carried, portable, nativeImage.get(field))) throw unsupported(plan);
            }
        } catch (RuntimeException invalid) {
            throw unsupported(plan);
        }
    }

    private static boolean nativeTypeMatches(Object carried, Object portable, Object encoded) {
        if (encoded == null) return false;
        if (portable instanceof String text) {
            boolean objectId = carried instanceof ConvertedValue carrier && "OBJECT_ID".equals(carrier.originType());
            return objectId ? driverInstance("org.bson.types.ObjectId", encoded) && text.equals(encoded.toString())
                    : encoded instanceof String && text.equals(encoded);
        }
        if (portable instanceof Long || portable instanceof Boolean) return portable.equals(encoded);
        if (portable instanceof Instant instant) {
            return encoded instanceof Instant time && instant.equals(time)
                    || encoded instanceof java.util.Date date && instant.equals(date.toInstant());
        }
        // Content and subtype were compared after the frozen codec decoded this native value.
        return portable instanceof Bytes && driverInstance("org.bson.types.Binary", encoded);
    }

    private static boolean driverInstance(String className, Object value) {
        try {
            // Driver types belong to the connector's isolated loader, not the host's dependencies.
            return Class.forName(className, false, value.getClass().getClassLoader()).isInstance(value);
        } catch (ClassNotFoundException unavailable) {
            return false;
        }
    }

    private static boolean kindMatches(String actual, String expected) {
        return actual == null || actual.equals(expected);
    }

    static Map<String, Object> storedTypes(TargetIdentityPlan plan) {
        List<Object> tests = plan.writeKey().stream().<Object>map(field -> Map.of("$in", List.of(
                Map.of("$type", "$" + field), List.of("string", "int", "long", "bool", "objectId", "binData", "date"))))
                .toList();
        return Map.of("$and", tests);
    }

    private static io.tapstate.core.common.TapstateException unsupported(TargetIdentityPlan plan) {
        return TargetIdentityProjection.failure(plan, "key-representation-unsupported");
    }
}
