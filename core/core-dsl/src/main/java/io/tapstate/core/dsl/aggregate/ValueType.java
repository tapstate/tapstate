package io.tapstate.core.dsl.aggregate;

import io.tapstate.core.common.TapstateType;

import java.io.Serializable;
import java.util.Objects;

/**
 * A value's type as the library reasons about it: the tapstate type, and for a decimal the scale it was
 * declared with. A null scale means none was declared, not zero.
 */
public record ValueType(TapstateType type, Integer scale) implements Serializable {

    private static final long serialVersionUID = 1L;

    public static final ValueType UNKNOWN = new ValueType(TapstateType.UNKNOWN, null);
    public static final ValueType INT64 = new ValueType(TapstateType.INT64, null);
    public static final ValueType DOUBLE = new ValueType(TapstateType.DOUBLE, null);

    public ValueType {
        Objects.requireNonNull(type, "type");
        if (scale != null && type != TapstateType.DECIMAL) {
            throw new IllegalArgumentException("only a decimal has a scale, not " + type);
        }
    }

    public static ValueType of(TapstateType type) {
        return new ValueType(type, null);
    }

    public static ValueType decimal(Integer scale) {
        return new ValueType(TapstateType.DECIMAL, scale);
    }

    public boolean numeric() {
        return type == TapstateType.INT64 || type == TapstateType.DECIMAL || type == TapstateType.DOUBLE;
    }

    /** Whether values of this type can be ordered: numbers, strings, temporals, booleans, or not yet known. */
    public boolean comparable() {
        return switch (type) {
            case INT64, DECIMAL, DOUBLE, STRING, DATE, TIME, DATETIME, YEAR, BOOLEAN, UNKNOWN -> true;
            default -> false;
        };
    }

    /** How a refusal names this type: {@code decimal(38,2)}, {@code int64}. */
    public String spelled() {
        if (type == TapstateType.DECIMAL) {
            return scale == null ? "decimal" : "decimal(38," + scale + ")";
        }
        return type.name().toLowerCase(java.util.Locale.ROOT);
    }
}
