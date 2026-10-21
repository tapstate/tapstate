package io.tapstate.adapters.pdk;

import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.codec.ToTapValueCodec;
import io.tapdata.entity.schema.type.TapType;
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
