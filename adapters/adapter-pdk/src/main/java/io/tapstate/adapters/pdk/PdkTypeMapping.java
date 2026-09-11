package io.tapstate.adapters.pdk;

import io.tapdata.entity.schema.type.TapArray;
import io.tapdata.entity.schema.type.TapBinary;
import io.tapdata.entity.schema.type.TapBoolean;
import io.tapdata.entity.schema.type.TapDate;
import io.tapdata.entity.schema.type.TapDateTime;
import io.tapdata.entity.schema.type.TapJson;
import io.tapdata.entity.schema.type.TapMap;
import io.tapdata.entity.schema.type.TapNumber;
import io.tapdata.entity.schema.type.TapRaw;
import io.tapdata.entity.schema.type.TapString;
import io.tapdata.entity.schema.type.TapTime;
import io.tapdata.entity.schema.type.TapType;
import io.tapdata.entity.schema.type.TapYear;
import io.tapstate.core.common.TapstateType;

import java.math.BigDecimal;
import io.tapstate.core.common.NumericType;

/**
 * Maps a PDK type onto the tapstate type namespace: the normalization step that turns what a connector
 * declared about a column into what every other layer reasons about.
 *
 * <p>The mapping is a projection, not a judgement - it says what a column is, never what may be done
 * with it. A shape this does not name maps onto unknown, which is deliberate in both directions: a
 * connector type nothing here covers must not arrive somewhere as one of the named types, and it must
 * arrive as something a caller has to decide about rather than as an absence.
 */
final class PdkTypeMapping {

    /** The widest domain a 64-bit integer covers: signed at the bottom, unsigned at the top. */
    private static final BigDecimal SIGNED_64_MIN = BigDecimal.valueOf(Long.MIN_VALUE);
    private static final BigDecimal UNSIGNED_64_MAX = new BigDecimal("18446744073709551615");

    private PdkTypeMapping() {
    }

    /**
     * What a PDK type resolved to and, where nothing did, which unknown it is.
     *
     * <p><b>The reason is the whole point of the pair.</b> The several ways a column arrives without a
     * type want different things done about them - a connector that declared nothing is a connector
     * problem, a spelling with no member here is this mapping being short a case, and a number
     * described in a way that names no width is neither. As one bare UNKNOWN they read alike, and the
     * one they read as is the one nobody investigates.
     */
    record Resolved(TapstateType type, String unknownBecause) {

        static Resolved of(TapstateType type) {
            return new Resolved(type, null);
        }

        static Resolved unknown(String because) {
            return new Resolved(TapstateType.UNKNOWN, because);
        }
    }

    /**
     * The tapstate type for a filled PDK type, and the reason where it is unknown. One switch, so the
     * type and its attribution cannot answer differently.
     */
    static Resolved resolve(TapType type) {
        return switch (type) {
            case TapNumber number -> number(number);
            case TapString ignored -> Resolved.of(TapstateType.STRING);
            case TapBoolean ignored -> Resolved.of(TapstateType.BOOLEAN);
            case TapDate ignored -> Resolved.of(TapstateType.DATE);
            case TapTime ignored -> Resolved.of(TapstateType.TIME);
            case TapDateTime ignored -> Resolved.of(TapstateType.DATETIME);
            case TapYear ignored -> Resolved.of(TapstateType.YEAR);
            case TapBinary ignored -> Resolved.of(TapstateType.BINARY);
            case TapJson ignored -> Resolved.of(TapstateType.JSON);
            case TapArray ignored -> Resolved.of(TapstateType.ARRAY);
            case TapMap ignored -> Resolved.of(TapstateType.MAP);
            case null -> Resolved.unknown("the connector declared no type for this column");
            // Named rather than counted: which shape arrived is what says this mapping is short a case
            // rather than the connector being at fault, and it is the one fact nobody can recover later.
            default -> Resolved.unknown(
                    "the connector's " + type.getClass().getSimpleName()
                            + " has no member in the tapstate type namespace");
        };
    }

    /** Copies every declared numeric attribute before the framework descriptor leaves discovery. */
    static NumericType numericType(TapType type) {
        if (!(type instanceof TapNumber number)) {
            return null;
        }
        return new NumericType(number.getBit(), number.getFixed(), number.getUnsigned(), number.getZerofill(),
                number.getMinValue(), number.getMaxValue(), number.getPrecision(), number.getScale());
    }

    /** Restores the source descriptor without inventing bounds or a destination SQL spelling. */
    static TapType targetType(TapstateType type, NumericType number) {
        if (number == null || (type != TapstateType.DECIMAL && type != TapstateType.INT64 && type != TapstateType.DOUBLE)) {
            return targetType(type);
        }
        return new TapNumber().bit(number.bit()).fixed(number.fixed()).unsigned(number.unsigned())
                .zerofill(number.zerofill()).minValue(number.minValue()).maxValue(number.maxValue())
                .precision(number.precision()).scale(number.scale());
    }

    /** Projects the inferred portable type into the PDK vocabulary; database types remain PDK-owned. */
    static TapType targetType(TapstateType type) {
        return switch (type) {
            case STRING -> new TapString();
            case INT64 -> new TapNumber().bit(64).scale(0).minValue(SIGNED_64_MIN)
                    .maxValue(BigDecimal.valueOf(Long.MAX_VALUE));
            case DECIMAL -> throw new IllegalArgumentException(
                    "decimal target requires declared numeric attributes; rediscover the source schema. "
                            + "Computed decimal columns have no declared numeric metadata");
            case DOUBLE -> new TapNumber().fixed(false).bit(64)
                    .minValue(BigDecimal.valueOf(-Double.MAX_VALUE)).maxValue(BigDecimal.valueOf(Double.MAX_VALUE));
            case BOOLEAN -> new TapBoolean();
            case DATE -> new TapDate();
            case TIME -> new TapTime();
            case DATETIME -> new TapDateTime();
            case YEAR -> new TapYear();
            case BINARY -> new TapBinary();
            case JSON -> new TapJson();
            case ARRAY -> new TapArray();
            case MAP -> new TapMap();
            case UNKNOWN -> new TapRaw();
        };
    }

    /** The tapstate type alone, for callers that reason about the column and not about its attribution. */
    static TapstateType of(TapType type) {
        return resolve(type).type();
    }

    /**
     * Splits a number by how the source holds it. Where a connector marks the split itself it is taken as
     * stated: a type marked fixed is exact and scaled, one marked not fixed is binary floating point.
     *
     * <p>Marking it is optional and a great many connectors do not, so the absence of the mark cannot be
     * read as anything on its own. A type that declares a scale is scaled whatever else it left out, and
     * which of the two scaled kinds it is nobody said: calling it exact would refuse arithmetic that is
     * fine, calling it approximate would permit arithmetic that drops digits. A declared scale of zero is
     * different in kind - it is the column stating it holds no fractional part, rather than a scale it is
     * held to - and that is an integer.
     *
     * <p>Left with a number the connector marked neither way and gave no scale for, the question is what
     * it did say. Something integral - a width, a precision, the range of values it holds - is the
     * connector describing a whole number, and that is taken as one. Nothing at all is a connector that
     * named the type and stopped, which is how a real binary floating point column arrives from more than
     * one connector in the set: reading that silence as "integer" is what makes such a column come out
     * computable, so the gate admits arithmetic over it and the value that reaches the expression at run
     * time is the double the driver delivered. Unknown is the only answer that neither guesses nor
     * silently permits - it is refused by name and the author rules on it.
     */
    private static Resolved number(TapNumber number) {
        Boolean fixed = number.getFixed();
        if (fixed != null) {
            return Resolved.of(fixed ? TapstateType.DECIMAL : TapstateType.DOUBLE);
        }
        Integer scale = number.getScale();
        if (scale != null) {
            return scale == 0
                    ? Resolved.of(TapstateType.INT64)
                    : Resolved.unknown("the connector declared a scale of " + scale
                            + " without saying whether the column is exact or approximate");
        }
        return describesAWholeNumber(number)
                ? Resolved.of(TapstateType.INT64)
                : Resolved.unknown(
                        "the connector named a number type and declared no scale, width or value range");
    }

    /**
     * Whether what the connector said about this number describes a whole number.
     *
     * <p>The range it declares is the thing to read, because it is the same question the type namespace
     * asks: a column stating it holds values up to 1.8e308 is not a 64-bit integer whatever else it is,
     * while one stating it holds up to 2^64-1 is - even unsigned, at the very top of the width. The
     * upper bound is the unsigned one so an unsigned bigint is not thrown out for exceeding the signed
     * range it never claimed to sit in.
     *
     * <p>A declared width answers it too, and is the fallback where the range is absent. It is only the
     * fallback: where both are stated the range is what the column actually holds, and a width beside a
     * range that overflows it is the width describing storage rather than domain.
     */
    private static boolean describesAWholeNumber(TapNumber number) {
        BigDecimal min = number.getMinValue();
        BigDecimal max = number.getMaxValue();
        if (min != null && max != null) {
            return min.compareTo(SIGNED_64_MIN) >= 0 && max.compareTo(UNSIGNED_64_MAX) <= 0;
        }
        return number.getBit() != null;
    }
}
