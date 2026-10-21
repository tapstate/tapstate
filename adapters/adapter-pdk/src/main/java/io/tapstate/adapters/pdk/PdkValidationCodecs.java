package io.tapstate.adapters.pdk;

import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.codec.ToTapValueCodec;
import io.tapdata.entity.schema.type.TapType;
import io.tapdata.entity.schema.type.TapNumber;
import io.tapdata.entity.schema.type.TapJson;
import io.tapdata.entity.schema.type.TapBinary;
import io.tapdata.entity.schema.value.ByteData;
import io.tapdata.entity.schema.value.TapRawValue;
import io.tapdata.entity.schema.value.TapValue;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.ConvertedValue;
import io.tapstate.core.validation.ValidationError;
import io.tapstate.core.validation.ValidationValues;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Selects exactly one type-conversion lane in an independent reader's registry. Results stay inside
 * this adapter until the value decoder applies logical-type rules, budgets and portable ownership.
 * Containers in the built-in lane still need their children decoded independently. No production
 * codec contract is changed, and a selected result is never an equality oracle.
 */
final class PdkValidationCodecs {
    enum Origin { NULL, ALREADY_CONVERTED, CUSTOM, BUILTIN, DEFAULT }
    record Selected(Object value, Origin origin, String codecBinding) {}

    private final String connectorId;
    private final String binding;
    private final TapCodecsRegistry registry;

    PdkValidationCodecs(String connectorId, String binding, TapCodecsRegistry registry) {
        this.connectorId = Objects.requireNonNull(connectorId, "connectorId");
        this.binding = Objects.requireNonNull(binding, "binding");
        this.registry = Objects.requireNonNull(registry, "registry");
        if (connectorId.isBlank() || binding.isBlank()) throw new IllegalArgumentException("codec identity is required");
    }

    Selected select(String field, Object value, TapType modelType) {
        Objects.requireNonNull(field, "field");
        if (value == null) return new Selected(null, Origin.NULL, binding);
        if (value instanceof TapRawValue) throw unavailable(field);
        if (value instanceof ValidationValues.Typed typed && !binding.equals(typed.codecBinding())) {
            throw unavailable(field);
        }
        if (value instanceof TapValue<?, ?> || value instanceof ConvertedValue
                || value instanceof ValidationValues.Typed || value instanceof ValidationValues.Binary) {
            return new Selected(value, Origin.ALREADY_CONVERTED, binding);
        }

        ToTapValueCodec<?> custom;
        try { custom = registry.getCustomToTapValueCodec(value.getClass()); }
        catch (RuntimeException failed) { throw failed(field, failed); }
        if (custom != null) return converted(field, value, modelType, custom, Origin.CUSTOM);

        // Exact Java numbers must not pass through the default TapNumberValue/Double conversion.
        // This is an adapter-local lane; mutable binary/container values are owned by the next stage.
        if (builtin(value)) return new Selected(value, Origin.BUILTIN, binding);

        ToTapValueCodec<?> fallback;
        try { fallback = registry.getToTapValueCodec(value.getClass()); }
        catch (RuntimeException failed) { throw failed(field, failed); }
        if (fallback == null) throw unavailable(field);
        return converted(field, value, modelType, fallback, Origin.DEFAULT);
    }

    /**
     * Admits a selected value only when the caller requires an exact numeric representation. A floating
     * payload is insufficient evidence even when this particular rounded value looks integral: the
     * original may have been a neighboring integer or a higher-precision decimal. No second codec,
     * origin-value recovery, string parsing or inferred rounding rule runs here. Model conformance is
     * still a separate check. Null is a valid selected value and remains null.
     */
    Number exactNumber(String field, Selected selected) {
        Objects.requireNonNull(field, "field");
        Objects.requireNonNull(selected, "selected");
        if (!binding.equals(selected.codecBinding())) throw unavailable(field);
        Object value = selected.value();
        for (int wrappers = 0; wrappers < 64; wrappers++) {
            if (value == null) return null;
            if (value instanceof ValidationValues.Typed typed) {
                if (!binding.equals(typed.codecBinding()) || !typed.logicalType().equals("exact-number")) {
                    throw unavailable(field);
                }
                value = typed.value();
            } else if (value instanceof ConvertedValue converted) {
                value = converted.value();
            } else if (value instanceof TapValue<?, ?> converted) {
                Class<? extends TapType> kind;
                try { kind = converted.tapTypeClass(); }
                catch (RuntimeException failure) { throw failed(field, failure); }
                if (kind != TapNumber.class) throw unavailable(field);
                try { value = converted.getValue(); }
                catch (RuntimeException failure) { throw failed(field, failure); }
            } else if (value instanceof Byte || value instanceof Short || value instanceof Integer || value instanceof Long
                    || value.getClass() == BigInteger.class || value.getClass() == BigDecimal.class) {
                return (Number) value;
            } else {
                throw unavailable(field);
            }
        }
        throw new TapstateException(ValidationError.VALUE_LIMIT_EXCEEDED, Map.of("limit", "codecWrappers"), null);
    }

    /**
     * Parses a selected textual JSON representation only when the caller's fixed rule declares JSON.
     * A custom text wrapper is not JSON merely because its contents look like an object. Already
     * structured portable values use their normal ownership path and must not enter this text lane.
     */
    Object jsonText(String field, Selected selected, ValidationValues.Limits limits) {
        Objects.requireNonNull(field, "field");
        Objects.requireNonNull(selected, "selected");
        Objects.requireNonNull(limits, "limits");
        if (!binding.equals(selected.codecBinding())) throw unavailable(field);
        Object value = selected.value();
        for (int wrappers = 0; wrappers < 64; wrappers++) {
            if (value == null) return null;
            if (value instanceof ValidationValues.Typed typed) {
                if (!binding.equals(typed.codecBinding()) || !typed.logicalType().equals("json")) throw unavailable(field);
                value = typed.value();
            } else if (value instanceof ConvertedValue converted) {
                value = converted.value();
            } else if (value instanceof TapValue<?, ?> converted) {
                Class<? extends TapType> kind;
                try { kind = converted.tapTypeClass(); }
                catch (RuntimeException failure) { throw failed(field, failure); }
                if (kind != TapJson.class) throw unavailable(field);
                try { value = converted.getValue(); }
                catch (RuntimeException failure) { throw failed(field, failure); }
            } else if (value instanceof String text) {
                return PdkValidationJson.parse(text, connectorId, field, limits);
            } else throw unavailable(field);
        }
        throw new TapstateException(ValidationError.VALUE_LIMIT_EXCEEDED, Map.of("limit", "codecWrappers"), null);
    }

    /**
     * Owns selected binary bytes without a second codec or a text interpretation. The caller reserves
     * this charge before entry and retains it until the owned value is released. Raw byte arrays and
     * buffers have no supplied subtype (-1); ByteData's observed unsigned subtype, including zero,
     * is preserved. Its format does not distinguish omitted subtype from explicit zero.
     */
    ValidationValues.Binary binary(String field, Selected selected, long maxValueBytes) {
        Objects.requireNonNull(field, "field");
        Objects.requireNonNull(selected, "selected");
        if (maxValueBytes < 1 || maxValueBytes > 32L * 1024 * 1024) throw new IllegalArgumentException("invalid binary reservation");
        if (!binding.equals(selected.codecBinding())) throw unavailable(field);
        Object value = selected.value();
        for (int wrappers = 0; wrappers < 64; wrappers++) {
            if (value == null) return null;
            if (value instanceof ValidationValues.Typed typed) {
                if (!binding.equals(typed.codecBinding()) || !typed.logicalType().equals("binary")) throw unavailable(field);
                value = typed.value();
            } else if (value instanceof ConvertedValue converted) {
                value = converted.value();
            } else if (value instanceof TapValue<?, ?> converted) {
                Class<? extends TapType> kind;
                try { kind = converted.tapTypeClass(); }
                catch (RuntimeException failure) { throw failed(field, failure); }
                if (kind != TapBinary.class) throw unavailable(field);
                try { value = converted.getValue(); }
                catch (RuntimeException failure) { throw failed(field, failure); }
            } else if (value instanceof ValidationValues.Binary binary) {
                binaryBudget(binary.length(), maxValueBytes);
                return binary;
            } else if (value instanceof byte[] bytes) {
                binaryBudget(bytes.length, maxValueBytes);
                return new ValidationValues.Binary(-1, bytes);
            } else if (value instanceof ByteBuffer buffer) {
                ByteBuffer view = buffer.asReadOnlyBuffer();
                binaryBudget(view.remaining(), maxValueBytes);
                return ValidationValues.Binary.fromBuffer(-1, view);
            } else if (value instanceof ByteData binary) {
                byte[] bytes;
                int subtype;
                try { bytes = binary.getValue(); subtype = Byte.toUnsignedInt(binary.getType()); }
                catch (RuntimeException failure) { throw failed(field, failure); }
                if (bytes == null) throw unavailable(field);
                binaryBudget(bytes.length, maxValueBytes);
                return new ValidationValues.Binary(subtype, bytes);
            } else throw unavailable(field);
        }
        throw new TapstateException(ValidationError.VALUE_LIMIT_EXCEEDED, Map.of("limit", "codecWrappers"), null);
    }

    private static void binaryBudget(int length, long limit) {
        if (64L + length > limit) throw new TapstateException(ValidationError.VALUE_LIMIT_EXCEEDED,
                Map.of("limit", "binaryBytes"), null);
    }

    private Selected converted(String field, Object value, TapType modelType,
            ToTapValueCodec<?> codec, Origin origin) {
        TapValue<?, ?> converted;
        try { converted = codec.toTapValue(value, modelType); }
        catch (RuntimeException failed) { throw failed(field, failed); }
        // The pinned SPI has no explicit not-applicable sentinel for a custom null result. Do not
        // invent a fallback convention. A non-null wrapper with a null payload is a distinct result.
        if (converted == null || converted instanceof TapRawValue) throw unavailable(field);
        return new Selected(converted, origin, binding);
    }

    private static boolean builtin(Object value) {
        return value instanceof String || value instanceof Boolean
                || value instanceof Byte || value instanceof Short || value instanceof Integer || value instanceof Long
                || value instanceof Float || value instanceof Double
                || value.getClass() == BigInteger.class || value.getClass() == BigDecimal.class
                || value instanceof Instant || value instanceof LocalDate || value instanceof LocalTime
                || value instanceof LocalDateTime || value instanceof OffsetDateTime || value instanceof ZonedDateTime
                || value instanceof byte[] || value instanceof ByteBuffer || value instanceof Map<?, ?>
                || value instanceof List<?>;
    }

    private TapstateException unavailable(String field) {
        return new TapstateException(ValidationError.CODEC_VALUE_UNAVAILABLE,
                Map.of("connector", connectorId, "field", field), null);
    }

    private TapstateException failed(String field, RuntimeException cause) {
        return new TapstateException(ValidationError.CODEC_FAILED,
                Map.of("connector", connectorId, "field", field), cause);
    }
}
