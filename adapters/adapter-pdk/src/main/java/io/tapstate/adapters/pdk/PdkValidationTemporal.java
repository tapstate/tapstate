package io.tapstate.adapters.pdk;

import io.tapdata.entity.schema.type.TapDate;
import io.tapdata.entity.schema.type.TapDateTime;
import io.tapdata.entity.schema.type.TapTime;
import io.tapdata.entity.schema.type.TapType;
import io.tapdata.entity.schema.value.DateTime;
import io.tapdata.entity.schema.value.TapValue;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.ConvertedValue;
import io.tapstate.core.validation.ValidationError;
import io.tapstate.core.validation.ValidationValues;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Map;
import java.util.Objects;

/**
 * Lossless temporal payload extraction after codec selection. Calendar and instant domains remain
 * separate. A legacy DateTime is ambiguous without a fixed reader rule: it can encode either a wall
 * value using UTC fields or epoch seconds. The caller must establish that encoding from the bound
 * connector/model contract, never from private origin tags, samples or the machine's default zone.
 * No precision rounding or zone/DST inference runs here; source representation metadata remains the
 * caller's diagnostic responsibility. This is not model-conformance or freshness verification.
 */
final class PdkValidationTemporal {
    enum Kind {
        DATE("date", TapDate.class), TIME("time", TapTime.class),
        LOCAL_DATE_TIME("local-date-time", TapDateTime.class), INSTANT("instant", TapDateTime.class);
        private final String logicalType;
        private final Class<? extends TapType> wrapper;
        Kind(String logicalType, Class<? extends TapType> wrapper) { this.logicalType = logicalType; this.wrapper = wrapper; }
    }
    enum LegacyEncoding { UTC_WALL_FIELDS, EPOCH_SECONDS }

    private PdkValidationTemporal() {}

    static Object decode(String connector, String binding, String field, PdkValidationCodecs.Selected selected,
            Kind kind, LegacyEncoding legacyEncoding) {
        Objects.requireNonNull(connector, "connector");
        Objects.requireNonNull(binding, "binding");
        Objects.requireNonNull(field, "field");
        Objects.requireNonNull(selected, "selected");
        Objects.requireNonNull(kind, "kind");
        if (!binding.equals(selected.codecBinding())) throw unavailable(connector, field, null);
        Object value = selected.value();
        for (int wrappers = 0; wrappers < 64; wrappers++) {
            if (value == null) return null;
            if (value instanceof ValidationValues.Typed typed) {
                if (!binding.equals(typed.codecBinding()) || !kind.logicalType.equals(typed.logicalType()))
                    throw unavailable(connector, field, null);
                value = typed.value();
            } else if (value instanceof ConvertedValue converted) {
                value = converted.value();
            } else if (value instanceof TapValue<?, ?> converted) {
                Class<? extends TapType> actual;
                try { actual = converted.tapTypeClass(); }
                catch (RuntimeException failed) { throw failed(connector, field, failed); }
                if (actual != kind.wrapper) throw unavailable(connector, field, null);
                try { value = converted.getValue(); }
                catch (RuntimeException failed) { throw failed(connector, field, failed); }
            } else if (value instanceof DateTime legacy) {
                return legacy(connector, field, legacy, kind, legacyEncoding);
            } else {
                return switch (kind) {
                    case DATE -> value instanceof LocalDate ? value : reject(connector, field);
                    case TIME -> value instanceof LocalTime ? value : reject(connector, field);
                    case LOCAL_DATE_TIME -> value instanceof LocalDateTime ? value : reject(connector, field);
                    case INSTANT -> {
                        if (value instanceof Instant) yield value;
                        if (value instanceof OffsetDateTime offset) yield offset.toInstant();
                        if (value instanceof ZonedDateTime zoned) yield zoned.toInstant();
                        yield reject(connector, field);
                    }
                };
            }
        }
        throw new TapstateException(ValidationError.VALUE_LIMIT_EXCEEDED, Map.of("limit", "codecWrappers"), null);
    }

    private static Object legacy(String connector, String field, DateTime value, Kind kind, LegacyEncoding encoding) {
        if (encoding == null || (kind == Kind.INSTANT) != (encoding == LegacyEncoding.EPOCH_SECONDS))
            throw unavailable(connector, field, null);
        Long seconds;
        Integer nanos;
        boolean illegal;
        try { seconds = value.getSeconds(); nanos = value.getNano(); illegal = value.isContainsIllegal(); }
        catch (RuntimeException failed) { throw failed(connector, field, failed); }
        if (illegal || seconds == null || nanos == null || nanos < 0 || nanos > 999_999_999)
            throw unavailable(connector, field, null);
        try {
            if (kind == Kind.INSTANT) return Instant.ofEpochSecond(seconds, nanos);
            LocalDateTime wall = LocalDateTime.ofEpochSecond(seconds, nanos, ZoneOffset.UTC);
            return switch (kind) {
                case LOCAL_DATE_TIME -> wall;
                case DATE -> {
                    if (!wall.toLocalTime().equals(LocalTime.MIDNIGHT)) throw unavailable(connector, field, null);
                    yield wall.toLocalDate();
                }
                case TIME -> {
                    // An extra date/day component may represent an interval, not a time of day.
                    if (!wall.toLocalDate().equals(LocalDate.ofEpochDay(0))) throw unavailable(connector, field, null);
                    yield wall.toLocalTime();
                }
                case INSTANT -> throw new IllegalStateException("instant decoding must not enter the wall-time lane");
            };
        } catch (DateTimeException invalid) { throw unavailable(connector, field, invalid); }
    }

    private static Object reject(String connector, String field) { throw unavailable(connector, field, null); }
    private static TapstateException unavailable(String connector, String field, Throwable cause) {
        return new TapstateException(ValidationError.CODEC_VALUE_UNAVAILABLE, Map.of("connector", connector, "field", field), cause);
    }
    private static TapstateException failed(String connector, String field, RuntimeException cause) {
        return new TapstateException(ValidationError.CODEC_FAILED, Map.of("connector", connector, "field", field), cause);
    }
}
