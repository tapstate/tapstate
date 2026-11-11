package io.tapstate.core.dsl.aggregate;

import io.tapstate.core.common.TapstateType;
import io.tapstate.core.model.RollupFunction;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.util.Map;

/**
 * A {@code count}, {@code sum} or {@code avg} kept across changes: inputs are added and taken back, and the
 * value is asked for whenever it is needed.
 *
 * <p><b>The value depends only on the inputs still in, never on what came and went.</b> Totals are held
 * exactly and wider than any result: an integer total has no upper bound, a decimal total keeps every digit,
 * and a floating total is the exact sum of its inputs. Whether a result fits its type is therefore judged
 * when it is asked for, so a total that left the range and came back is a value again, and adding and
 * retracting the same input always returns to exactly where it started. A floating result is rounded once,
 * from the exact total.
 *
 * <p>A null never reaches {@link #add} or {@link #retract}: nulls do not contribute, and the caller skips
 * them. A value that is not of the declared type is refused before it changes anything.
 */
public abstract sealed class RunningValue {

    private RunningValue() {
    }

    /** A fresh value of {@code function} over arguments declared as {@code argument}. */
    public static RunningValue of(RollupFunction function, ValueType argument) {
        return switch (function) {
            case COUNT -> new Count();
            case SUM -> switch (argument.type()) {
                case INT64 -> new IntegerSum();
                case DECIMAL -> new DecimalSum(argument.scale());
                case DOUBLE -> new FloatingSum(false);
                default -> throw new IllegalArgumentException("sum takes a number, not " + argument.spelled());
            };
            case AVG -> switch (argument.type()) {
                case INT64, DECIMAL -> new ExactAverage(argument);
                case DOUBLE -> new FloatingSum(true);
                default -> throw new IllegalArgumentException("avg takes a number, not " + argument.spelled());
            };
            default -> throw new IllegalArgumentException(function.yaml()
                    + " keeps its inputs rather than a running value");
        };
    }

    /** The value {@link #state()} was taken from. */
    public static RunningValue restore(RollupFunction function, ValueType argument, byte[] state) {
        RunningValue value = of(function, argument);
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(state))) {
            value.read(in);
            if (in.available() != 0) {
                throw new IllegalArgumentException("state of " + function.yaml() + " has bytes left over");
            }
        } catch (IOException truncated) {
            throw new IllegalArgumentException("state of " + function.yaml() + " is cut short", truncated);
        }
        return value;
    }

    public abstract void add(Object value);

    public abstract void retract(Object value);

    /** How many inputs are in. */
    public abstract long inputs();

    /** Whether any input is in, which is what decides that a field's {@code default:} applies. */
    public final boolean contributed() {
        return inputs() > 0;
    }

    /**
     * The value: null when no input is in, except for a count, which is 0. Fails with
     * {@link RollupError#OVERFLOW} when the value does not fit its type; the inputs are kept, and the value
     * is back as soon as it fits again.
     */
    public abstract Object result();

    /** Everything needed to carry on from here, for {@link #restore}. */
    public final byte[] state() {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            write(out);
        } catch (IOException impossible) {
            throw new UncheckedIOException(impossible);
        }
        return bytes.toByteArray();
    }

    abstract void write(DataOutputStream out) throws IOException;

    abstract void read(DataInputStream in) throws IOException;

    private static final class Count extends RunningValue {
        private long n;

        public void add(Object value) {
            n++;
        }

        public void retract(Object value) {
            n--;
        }

        public long inputs() {
            return n;
        }

        public Object result() {
            return n;
        }

        void write(DataOutputStream out) throws IOException {
            out.writeLong(n);
        }

        void read(DataInputStream in) throws IOException {
            n = in.readLong();
        }
    }

    private static final class IntegerSum extends RunningValue {
        private BigInteger total = BigInteger.ZERO;
        private long n;

        public void add(Object value) {
            total = total.add(BigInteger.valueOf(integer(value)));
            n++;
        }

        public void retract(Object value) {
            total = total.subtract(BigInteger.valueOf(integer(value)));
            n--;
        }

        public long inputs() {
            return n;
        }

        public Object result() {
            if (n == 0) {
                return null;
            }
            if (total.bitLength() > 63) {
                throw new AggregateFailure(RollupError.OVERFLOW, Map.of("type", "int64"));
            }
            return total.longValue();
        }

        void write(DataOutputStream out) throws IOException {
            out.writeLong(n);
            integer(out, total);
        }

        void read(DataInputStream in) throws IOException {
            n = in.readLong();
            total = integer(in);
        }
    }

    /**
     * A decimal total. With a declared scale the result is at that scale. Without one it is written with no
     * trailing zeros, since the widest scale among the inputs still in is not something a total remembers.
     */
    private static final class DecimalSum extends RunningValue {
        private final Integer scale;
        private BigDecimal total = BigDecimal.ZERO;
        private long n;

        DecimalSum(Integer scale) {
            this.scale = scale;
        }

        public void add(Object value) {
            total = total.add(decimalAt(value, scale));
            n++;
        }

        public void retract(Object value) {
            total = total.subtract(decimalAt(value, scale));
            n--;
        }

        public long inputs() {
            return n;
        }

        public Object result() {
            if (n == 0) {
                return null;
            }
            BigDecimal value = scale != null ? total.setScale(scale, RoundingMode.UNNECESSARY) : plain(total);
            if (value.precision() > Values.DECIMAL_PRECISION) {
                throw new AggregateFailure(RollupError.OVERFLOW,
                        Map.of("type", ValueType.decimal(scale).spelled()));
            }
            return value;
        }

        void write(DataOutputStream out) throws IOException {
            out.writeLong(n);
            decimal(out, total);
        }

        void read(DataInputStream in) throws IOException {
            n = in.readLong();
            total = decimal(in);
        }
    }

    /**
     * An integer or decimal average: the exact total divided by the count, rounded half-even at the result's
     * scale.
     */
    private static final class ExactAverage extends RunningValue {
        private final ValueType argument;
        private BigDecimal total = BigDecimal.ZERO;
        private long n;

        ExactAverage(ValueType argument) {
            this.argument = argument;
        }

        private BigDecimal exact(Object value) {
            return argument.type() == TapstateType.INT64
                    ? BigDecimal.valueOf(integer(value)) : decimalAt(value, argument.scale());
        }

        public void add(Object value) {
            total = total.add(exact(value));
            n++;
        }

        public void retract(Object value) {
            total = total.subtract(exact(value));
            n--;
        }

        public long inputs() {
            return n;
        }

        public Object result() {
            if (n == 0) {
                return null;
            }
            int scale = argument.type() == TapstateType.INT64 ? Values.AVG_MIN_SCALE
                    : Math.max(argument.scale() != null ? argument.scale() : plain(total).scale(),
                            Values.AVG_MIN_SCALE);
            BigDecimal average = total.divide(BigDecimal.valueOf(n), scale, RoundingMode.HALF_EVEN);
            if (average.precision() > Values.DECIMAL_PRECISION) {
                throw new AggregateFailure(RollupError.OVERFLOW,
                        Map.of("type", ValueType.decimal(scale).spelled()));
            }
            return average;
        }

        void write(DataOutputStream out) throws IOException {
            out.writeLong(n);
            decimal(out, total);
        }

        void read(DataInputStream in) throws IOException {
            n = in.readLong();
            total = decimal(in);
        }
    }

    /**
     * A floating sum or average. Finite inputs are summed exactly; an infinity or a not-a-number cannot be
     * subtracted back out of a total, so each is counted beside it. The result follows floating addition:
     * any not-a-number, or both infinities, give not-a-number; one infinity gives that infinity; otherwise
     * the exact total rounded to the nearest double.
     */
    private static final class FloatingSum extends RunningValue {
        private final boolean average;
        private BigDecimal total = BigDecimal.ZERO;
        private long n;
        private long notNumbers;
        private long positiveInfinities;
        private long negativeInfinities;

        FloatingSum(boolean average) {
            this.average = average;
        }

        private void change(Object value, int by) {
            double d = floating(value);
            if (Double.isNaN(d)) {
                notNumbers += by;
            } else if (d == Double.POSITIVE_INFINITY) {
                positiveInfinities += by;
            } else if (d == Double.NEGATIVE_INFINITY) {
                negativeInfinities += by;
            } else {
                BigDecimal exact = new BigDecimal(d);
                total = by > 0 ? total.add(exact) : total.subtract(exact);
            }
            n += by;
        }

        public void add(Object value) {
            change(value, 1);
        }

        public void retract(Object value) {
            change(value, -1);
        }

        public long inputs() {
            return n;
        }

        public Object result() {
            if (n == 0) {
                return null;
            }
            double sum;
            if (notNumbers > 0 || (positiveInfinities > 0 && negativeInfinities > 0)) {
                sum = Double.NaN;
            } else if (positiveInfinities > 0) {
                sum = Double.POSITIVE_INFINITY;
            } else if (negativeInfinities > 0) {
                sum = Double.NEGATIVE_INFINITY;
            } else {
                sum = total.doubleValue();
            }
            return average ? sum / n : sum;
        }

        void write(DataOutputStream out) throws IOException {
            out.writeLong(n);
            out.writeLong(notNumbers);
            out.writeLong(positiveInfinities);
            out.writeLong(negativeInfinities);
            decimal(out, total);
        }

        void read(DataInputStream in) throws IOException {
            n = in.readLong();
            notNumbers = in.readLong();
            positiveInfinities = in.readLong();
            negativeInfinities = in.readLong();
            total = decimal(in);
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

    /** A decimal input: any scale when none is declared, otherwise no more fractional digits than declared. */
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
        String actual = value instanceof BigDecimal d ? "decimal with scale " + d.scale() : Values.spelled(value);
        return new AggregateFailure(RollupError.VALUE_TYPE_MISMATCH, Map.of("declared", declared, "actual", actual));
    }

    /** A decimal with no trailing zeros and no negative scale: {@code 300}, never {@code 3E+2}. */
    private static BigDecimal plain(BigDecimal value) {
        if (value.signum() == 0) {
            return BigDecimal.ZERO;
        }
        BigDecimal stripped = value.stripTrailingZeros();
        return stripped.scale() < 0 ? stripped.setScale(0) : stripped;
    }

    private static void integer(DataOutputStream out, BigInteger value) throws IOException {
        byte[] bytes = value.toByteArray();
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    private static BigInteger integer(DataInputStream in) throws IOException {
        int length = in.readInt();
        if (length <= 0 || length > in.available()) {
            throw new IOException("a number of " + length + " bytes does not fit what is left");
        }
        return new BigInteger(in.readNBytes(length));
    }

    private static void decimal(DataOutputStream out, BigDecimal value) throws IOException {
        out.writeInt(value.scale());
        integer(out, value.unscaledValue());
    }

    private static BigDecimal decimal(DataInputStream in) throws IOException {
        int scale = in.readInt();
        return new BigDecimal(integer(in), scale);
    }
}
