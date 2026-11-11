package io.tapstate.core.dsl.aggregate;

import io.tapstate.core.common.TapstateType;
import io.tapstate.core.model.RollupResultType;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * The value rules every function shares: what kind of number a value is, how two values order, when two
 * values are the same for {@code count_distinct}, and the exact conversions {@code type:} and
 * {@code default:} are allowed.
 *
 * <p>Ordering is fixed rather than inherited from wherever a value came from: numbers by numeric value
 * ({@code 2.50} equals {@code 2.5}), strings by code point rather than any source collation, temporals by
 * instant where they name one. Two values with no common order are refused rather than ordered by an
 * accident of their representation.
 */
public final class Values {

    /** Decimal digits a decimal rollup result may carry. */
    public static final int DECIMAL_PRECISION = 38;

    /** The least scale a decimal or integer average is given. */
    public static final int AVG_MIN_SCALE = 6;

    private Values() {
    }

    /** The numeric type a runtime value is, or null when it is not a number. */
    public static TapstateType numericKind(Object value) {
        return switch (value) {
            case Long ignored -> TapstateType.INT64;
            case Integer ignored -> TapstateType.INT64;
            case Short ignored -> TapstateType.INT64;
            case Byte ignored -> TapstateType.INT64;
            case BigDecimal ignored -> TapstateType.DECIMAL;
            case BigInteger ignored -> TapstateType.DECIMAL;
            case Double ignored -> TapstateType.DOUBLE;
            case Float ignored -> TapstateType.DOUBLE;
            case null, default -> null;
        };
    }

    /** How a refusal names what a value turned out to be. */
    public static String spelled(Object value) {
        TapstateType kind = numericKind(value);
        if (kind != null) {
            return kind.name().toLowerCase(java.util.Locale.ROOT);
        }
        return value == null ? "null" : value.getClass().getSimpleName();
    }

    /** {@code value} as an exact decimal; it must be a number. */
    public static BigDecimal decimal(Object value) {
        return switch (value) {
            case BigDecimal d -> d;
            case BigInteger i -> new BigDecimal(i);
            case Double d -> new BigDecimal(d);
            case Float f -> new BigDecimal(f);
            case Number n -> BigDecimal.valueOf(n.longValue());
            default -> throw new IllegalArgumentException("not a number: " + value);
        };
    }

    /**
     * The order of two non-null values, or a {@link RollupError#VALUE_TYPE_MISMATCH} failure when they have
     * none in common.
     */
    public static int compare(Object a, Object b) {
        if (a instanceof Number x && b instanceof Number y) {
            if (!finite(x) || !finite(y)) {
                return Double.compare(x.doubleValue(), y.doubleValue());
            }
            return decimal(x).compareTo(decimal(y));
        }
        if (a instanceof CharSequence x && b instanceof CharSequence y) {
            return compareCodePoints(x.toString(), y.toString());
        }
        Instant ia = instantOf(a);
        Instant ib = instantOf(b);
        if (ia != null && ib != null) {
            return ia.compareTo(ib);
        }
        if (a instanceof Boolean x && b instanceof Boolean y) {
            return Boolean.compare(x, y);
        }
        if (a.getClass() == b.getClass() && a instanceof Comparable<?>) {
            @SuppressWarnings("unchecked")
            Comparable<Object> comparable = (Comparable<Object>) a;
            return comparable.compareTo(b);
        }
        throw new AggregateFailure(RollupError.VALUE_TYPE_MISMATCH,
                Map.of("declared", spelled(a), "actual", spelled(b)));
    }

    /** Two orderings compared left to right; a later entry only breaks ties of the earlier ones. */
    public static int compareAll(List<Object> a, List<Object> b) {
        int n = Math.min(a.size(), b.size());
        for (int i = 0; i < n; i++) {
            int c = compare(a.get(i), b.get(i));
            if (c != 0) {
                return c;
            }
        }
        return Integer.compare(a.size(), b.size());
    }

    /**
     * Two element keys compared left to right. Keys are compared only to break ties and to fix a fold order,
     * so a pair with no common order falls back to their text rather than refusing: the order is arbitrary
     * but fixed, which is all a tie-break owes.
     */
    public static int compareKeys(List<Object> a, List<Object> b) {
        int n = Math.min(a.size(), b.size());
        for (int i = 0; i < n; i++) {
            Object x = a.get(i);
            Object y = b.get(i);
            int c;
            if (x == null || y == null) {
                c = x == null ? (y == null ? 0 : -1) : 1;
            } else {
                try {
                    c = compare(x, y);
                } catch (AggregateFailure incomparable) {
                    c = compareCodePoints(String.valueOf(x), String.valueOf(y));
                }
            }
            if (c != 0) {
                return c;
            }
        }
        return Integer.compare(a.size(), b.size());
    }

    /** The form two values must share to count once in {@code count_distinct}. */
    public static Object distinctForm(Object value) {
        if (value instanceof Number n && finite(n)) {
            BigDecimal d = decimal(n);
            return d.signum() == 0 ? BigDecimal.ZERO : d.stripTrailingZeros();
        }
        if (value instanceof CharSequence s) {
            return s.toString();
        }
        Instant instant = instantOf(value);
        return instant != null ? instant : value;
    }

    /**
     * {@code value} converted exactly to {@code to}, or a {@link RollupError#RESULT_NOT_CONVERTIBLE} failure.
     * A conversion to double is not exact and is allowed because the field asked for it.
     */
    public static Object convert(Object value, RollupResultType to) {
        if (value == null || to == null) {
            return value;
        }
        TapstateType from = numericKind(value);
        if (from == null) {
            throw notConvertible(spelled(value), to);
        }
        return switch (to) {
            case DOUBLE -> ((Number) value).doubleValue();
            case DECIMAL -> {
                if (!finite((Number) value)) {
                    throw notConvertible(spelled(value), to);
                }
                yield decimal(value);
            }
            case INT64 -> {
                if (from == TapstateType.INT64) {
                    yield ((Number) value).longValue();
                }
                if (!finite((Number) value)) {
                    throw notConvertible(spelled(value), to);
                }
                try {
                    yield decimal(value).longValueExact();
                } catch (ArithmeticException notIntegralOrTooWide) {
                    throw notConvertible(spelled(value), to);
                }
            }
        };
    }

    /**
     * A declared {@code default:} typed against the field's final result type: it must already be of that
     * type, except that an integer becomes a decimal at the result's scale. Returns the typed default, or
     * the reason it cannot be one.
     */
    public static Object typedDefault(Object written, ValueType result) {
        if (written == null) {
            return null;
        }
        switch (result.type()) {
            case INT64 -> {
                if (numericKind(written) == TapstateType.INT64) {
                    return ((Number) written).longValue();
                }
            }
            case DECIMAL -> {
                TapstateType kind = numericKind(written);
                if (kind == TapstateType.INT64 || (kind == TapstateType.DOUBLE && finite((Number) written))) {
                    BigDecimal d = kind == TapstateType.INT64
                            ? BigDecimal.valueOf(((Number) written).longValue())
                            : new BigDecimal(written.toString());
                    if (result.scale() == null) {
                        return d;
                    }
                    try {
                        return d.setScale(result.scale(), java.math.RoundingMode.UNNECESSARY);
                    } catch (ArithmeticException moreDigitsThanTheScale) {
                        throw new IllegalArgumentException("default " + written + " has more fractional digits "
                                + "than the result's scale " + result.scale());
                    }
                }
            }
            case DOUBLE -> {
                if (written instanceof Double || written instanceof Float) {
                    return ((Number) written).doubleValue();
                }
            }
            case STRING -> {
                if (written instanceof String) {
                    return written;
                }
            }
            case BOOLEAN -> {
                if (written instanceof Boolean) {
                    return written;
                }
            }
            case UNKNOWN -> {
                return written;
            }
            default -> {
                // a temporal or structured result: no YAML literal is of that type
            }
        }
        throw new IllegalArgumentException("default " + written + " (" + spelled(written) + ") is not a "
                + result.spelled() + "; only an integer widens, to a decimal");
    }

    private static AggregateFailure notConvertible(String from, RollupResultType to) {
        return new AggregateFailure(RollupError.RESULT_NOT_CONVERTIBLE, Map.of("from", from, "to", to.yaml()));
    }

    static boolean finite(Number n) {
        if (n instanceof Double d) {
            return Double.isFinite(d);
        }
        if (n instanceof Float f) {
            return Float.isFinite(f);
        }
        return true;
    }

    private static Instant instantOf(Object value) {
        return switch (value) {
            case Instant i -> i;
            case OffsetDateTime o -> o.toInstant();
            case ZonedDateTime z -> z.toInstant();
            case Date d -> d.toInstant();
            case null, default -> null;
        };
    }

    private static int compareCodePoints(String a, String b) {
        int i = 0;
        int j = 0;
        while (i < a.length() && j < b.length()) {
            int x = a.codePointAt(i);
            int y = b.codePointAt(j);
            if (x != y) {
                return Integer.compare(x, y);
            }
            i += Character.charCount(x);
            j += Character.charCount(y);
        }
        return Integer.compare(a.length() - i, b.length() - j);
    }
}
