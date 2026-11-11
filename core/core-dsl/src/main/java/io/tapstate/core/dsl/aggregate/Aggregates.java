package io.tapstate.core.dsl.aggregate;

import io.tapstate.core.common.TapstateType;
import io.tapstate.core.model.RollupFunction;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The function library: {@code count}, {@code count_distinct}, {@code sum}, {@code avg}, {@code min},
 * {@code max}, {@code first} and {@code last}, with SQL null semantics and an exact numeric contract.
 *
 * <p>Results depend only on the set of inputs, never on the order they were added in: a double sum is the one
 * place addition order shows, and its caller adds in element-key order so every fold of one set gives the same
 * bits. Integer and decimal arithmetic is exact, and a value that does not fit fails with a code rather than
 * wrapping or rounding. The only rounding is an average's, at a scale and mode that are part of its type.
 */
public final class Aggregates {

    private static final Map<RollupFunction, AggregateFunction> LIBRARY = library();

    private Aggregates() {
    }

    public static AggregateFunction of(RollupFunction function) {
        return LIBRARY.get(function);
    }

    private static Map<RollupFunction, AggregateFunction> library() {
        Map<RollupFunction, AggregateFunction> library = new EnumMap<>(RollupFunction.class);
        library.put(RollupFunction.COUNT, new Count());
        library.put(RollupFunction.COUNT_DISTINCT, new CountDistinct());
        library.put(RollupFunction.SUM, new Sum());
        library.put(RollupFunction.AVG, new Avg());
        library.put(RollupFunction.MIN, new Extreme(RollupFunction.MIN));
        library.put(RollupFunction.MAX, new Extreme(RollupFunction.MAX));
        library.put(RollupFunction.FIRST, new Pick(RollupFunction.FIRST));
        library.put(RollupFunction.LAST, new Pick(RollupFunction.LAST));
        return Map.copyOf(library);
    }

    // ---- count --------------------------------------------------------------------------------

    private record Count() implements AggregateFunction {
        public RollupFunction function() {
            return RollupFunction.COUNT;
        }

        public StateKind stateKind() {
            return StateKind.VALUE;
        }

        public boolean retractable() {
            return true;
        }

        public ValueType resultType(ValueType argument) {
            return ValueType.INT64;
        }

        public Accumulator init(ValueType argument) {
            return new Accumulator() {
                private long n;

                public void add(Contribution input) {
                    n++;
                }

                public void retract(Contribution input) {
                    n--;
                }

                public boolean contributed() {
                    return n > 0;
                }

                public Object result() {
                    return n;
                }
            };
        }
    }

    private record CountDistinct() implements AggregateFunction {
        public RollupFunction function() {
            return RollupFunction.COUNT_DISTINCT;
        }

        public StateKind stateKind() {
            return StateKind.MATERIALIZED_INPUT;
        }

        public boolean retractable() {
            return false;
        }

        public ValueType resultType(ValueType argument) {
            return ValueType.INT64;
        }

        public Accumulator init(ValueType argument) {
            return new Accumulator() {
                private final Set<Object> seen = new HashSet<>();

                public void add(Contribution input) {
                    seen.add(Values.distinctForm(input.value()));
                }

                public boolean contributed() {
                    return !seen.isEmpty();
                }

                public Object result() {
                    return (long) seen.size();
                }
            };
        }
    }

    // ---- sum and avg --------------------------------------------------------------------------

    private record Sum() implements AggregateFunction {
        public RollupFunction function() {
            return RollupFunction.SUM;
        }

        public StateKind stateKind() {
            return StateKind.VALUE;
        }

        public boolean retractable() {
            return true;
        }

        public ValueType resultType(ValueType argument) {
            return switch (argument.type()) {
                case INT64 -> ValueType.INT64;
                case DECIMAL -> ValueType.decimal(argument.scale());
                case DOUBLE -> ValueType.DOUBLE;
                default -> throw new IllegalArgumentException(
                        "sum takes a number, and this argument is " + argument.spelled());
            };
        }

        public Accumulator init(ValueType argument) {
            return switch (argument.type()) {
                case INT64 -> new IntegerSum();
                case DECIMAL -> new DecimalSum(argument.scale());
                case DOUBLE -> new DoubleSum();
                default -> throw new IllegalStateException("sum over " + argument.spelled() + " was not refused");
            };
        }
    }

    private static final class IntegerSum implements Accumulator {
        private long total;
        private boolean any;

        public void add(Contribution input) {
            long v = integer(input.value());
            try {
                total = Math.addExact(total, v);
            } catch (ArithmeticException beyond64Bits) {
                throw new AggregateFailure(RollupError.OVERFLOW, Map.of("type", "int64"));
            }
            any = true;
        }

        public void retract(Contribution input) {
            total = Math.subtractExact(total, integer(input.value()));
        }

        public boolean contributed() {
            return any;
        }

        public Object result() {
            return any ? total : null;
        }
    }

    private static final class DecimalSum implements Accumulator {
        private final Integer scale;
        private BigDecimal total;

        DecimalSum(Integer scale) {
            this.scale = scale;
        }

        public void add(Contribution input) {
            BigDecimal v = decimalAt(input.value(), scale);
            total = total == null ? v : total.add(v);
            if (total.precision() > Values.DECIMAL_PRECISION) {
                throw new AggregateFailure(RollupError.OVERFLOW,
                        Map.of("type", ValueType.decimal(scale).spelled()));
            }
        }

        public void retract(Contribution input) {
            total = total.subtract(decimalAt(input.value(), scale));
        }

        public boolean contributed() {
            return total != null;
        }

        public Object result() {
            return total;
        }
    }

    private static final class DoubleSum implements Accumulator {
        private double total;
        private boolean any;

        public void add(Contribution input) {
            total += floating(input.value());
            any = true;
        }

        public void retract(Contribution input) {
            total -= floating(input.value());
        }

        public boolean contributed() {
            return any;
        }

        public Object result() {
            return any ? total : null;
        }
    }

    private record Avg() implements AggregateFunction {
        public RollupFunction function() {
            return RollupFunction.AVG;
        }

        public StateKind stateKind() {
            return StateKind.VALUE;
        }

        public boolean retractable() {
            return true;
        }

        public ValueType resultType(ValueType argument) {
            return switch (argument.type()) {
                case INT64 -> ValueType.decimal(Values.AVG_MIN_SCALE);
                case DECIMAL -> ValueType.decimal(argument.scale() == null ? null
                        : Math.max(argument.scale(), Values.AVG_MIN_SCALE));
                case DOUBLE -> ValueType.DOUBLE;
                default -> throw new IllegalArgumentException(
                        "avg takes a number, and this argument is " + argument.spelled());
            };
        }

        public Accumulator init(ValueType argument) {
            return switch (argument.type()) {
                case INT64, DECIMAL -> new ExactAvg(argument);
                case DOUBLE -> new DoubleAvg();
                default -> throw new IllegalStateException("avg over " + argument.spelled() + " was not refused");
            };
        }
    }

    /**
     * An integer or decimal average: the exact sum divided by the count, rounded half-even at the result's
     * scale. An integer sum is kept as a decimal so the average of values near the 64-bit edge is still exact.
     */
    private static final class ExactAvg implements Accumulator {
        private final ValueType argument;
        private BigDecimal total = BigDecimal.ZERO;
        private long n;
        private int widestScale;

        ExactAvg(ValueType argument) {
            this.argument = argument;
        }

        public void add(Contribution input) {
            BigDecimal v = argument.type() == TapstateType.INT64
                    ? BigDecimal.valueOf(integer(input.value()))
                    : decimalAt(input.value(), argument.scale());
            total = total.add(v);
            widestScale = Math.max(widestScale, v.scale());
            n++;
        }

        public void retract(Contribution input) {
            total = total.subtract(argument.type() == TapstateType.INT64
                    ? BigDecimal.valueOf(integer(input.value()))
                    : decimalAt(input.value(), argument.scale()));
            n--;
        }

        public boolean contributed() {
            return n > 0;
        }

        public Object result() {
            if (n == 0) {
                return null;
            }
            int scale = argument.type() == TapstateType.INT64 ? Values.AVG_MIN_SCALE
                    : Math.max(argument.scale() == null ? widestScale : argument.scale(), Values.AVG_MIN_SCALE);
            BigDecimal average = total.divide(BigDecimal.valueOf(n), scale, RoundingMode.HALF_EVEN);
            if (average.precision() > Values.DECIMAL_PRECISION) {
                throw new AggregateFailure(RollupError.OVERFLOW,
                        Map.of("type", ValueType.decimal(scale).spelled()));
            }
            return average;
        }
    }

    private static final class DoubleAvg implements Accumulator {
        private double total;
        private long n;

        public void add(Contribution input) {
            total += floating(input.value());
            n++;
        }

        public void retract(Contribution input) {
            total -= floating(input.value());
            n--;
        }

        public boolean contributed() {
            return n > 0;
        }

        public Object result() {
            return n == 0 ? null : total / n;
        }
    }

    // ---- min and max --------------------------------------------------------------------------

    private record Extreme(RollupFunction function) implements AggregateFunction {
        public StateKind stateKind() {
            return StateKind.MATERIALIZED_INPUT;
        }

        public boolean retractable() {
            return false;
        }

        public ValueType resultType(ValueType argument) {
            if (!argument.comparable()) {
                throw new IllegalArgumentException(function.yaml() + " takes a number, a string or a temporal, "
                        + "and this argument is " + argument.spelled());
            }
            return argument;
        }

        public Accumulator init(ValueType argument) {
            int sign = function == RollupFunction.MIN ? -1 : 1;
            return new Accumulator() {
                private Object best;

                public void add(Contribution input) {
                    if (best == null || Integer.signum(Values.compare(input.value(), best)) == sign) {
                        best = input.value();
                    }
                }

                public boolean contributed() {
                    return best != null;
                }

                public Object result() {
                    return best;
                }
            };
        }
    }

    // ---- first and last -----------------------------------------------------------------------

    /**
     * {@code first} and {@code last}: the element with the smallest or greatest ordering wins, and the field is
     * its value - null when that value is null, with no fall back to the next element. An element whose
     * ordering has a null is no candidate. A tie that survives every ordering expression goes to the smallest
     * element key for {@code first} and the greatest for {@code last}, so the winner is the same however the
     * elements arrived.
     */
    private record Pick(RollupFunction function) implements AggregateFunction {
        public StateKind stateKind() {
            return StateKind.MATERIALIZED_INPUT;
        }

        public boolean retractable() {
            return false;
        }

        public ValueType resultType(ValueType argument) {
            return argument;
        }

        public Accumulator init(ValueType argument) {
            int sign = function == RollupFunction.FIRST ? -1 : 1;
            return new Accumulator() {
                private Contribution winner;

                public void add(Contribution input) {
                    for (Object part : input.by()) {
                        if (part == null) {
                            return;
                        }
                    }
                    if (winner == null) {
                        winner = input;
                        return;
                    }
                    int byOrder = Values.compareAll(input.by(), winner.by());
                    int order = byOrder != 0 ? byOrder : Values.compareKeys(input.key(), winner.key());
                    if (Integer.signum(order) == sign) {
                        winner = input;
                    }
                }

                public boolean contributed() {
                    return winner != null;
                }

                public Object result() {
                    return winner == null ? null : winner.value();
                }
            };
        }
    }

    // ---- what a declared type admits ----------------------------------------------------------

    private static long integer(Object value) {
        if (Values.numericKind(value) != TapstateType.INT64) {
            throw mismatch("int64", value);
        }
        return ((Number) value).longValue();
    }

    private static double floating(Object value) {
        if (Values.numericKind(value) != TapstateType.DOUBLE) {
            throw mismatch("double", value);
        }
        return ((Number) value).doubleValue();
    }

    /**
     * A decimal input at the declared scale. A value with fewer fractional digits is the same number and is
     * written at the scale; one with more would have to be rounded to fit and is not of the declared type.
     */
    private static BigDecimal decimalAt(Object value, Integer scale) {
        BigDecimal d = switch (value) {
            case BigDecimal b -> b;
            case BigInteger i -> new BigDecimal(i);
            case null, default -> throw mismatch(ValueType.decimal(scale).spelled(), value);
        };
        if (scale == null) {
            return d;
        }
        try {
            return d.setScale(scale, RoundingMode.UNNECESSARY);
        } catch (ArithmeticException moreDigits) {
            throw mismatch(ValueType.decimal(scale).spelled(), value);
        }
    }

    private static AggregateFailure mismatch(String declared, Object value) {
        String actual = Values.spelled(value);
        if (value instanceof BigDecimal d) {
            actual = "decimal with scale " + d.scale();
        }
        return new AggregateFailure(RollupError.VALUE_TYPE_MISMATCH, Map.of("declared", declared, "actual", actual));
    }

    /** The whole library, for a reader that has to enumerate it. */
    public static List<AggregateFunction> all() {
        return List.copyOf(LIBRARY.values());
    }
}
