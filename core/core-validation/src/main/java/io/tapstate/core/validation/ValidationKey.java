package io.tapstate.core.validation;

import io.tapstate.core.common.TapstateException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * An immutable logical key. Physical query fields and connector encodings live separately.
 * Ordering is usable only after both readers have established compatible native ordering.
 */
public record ValidationKey(List<Part> parts) implements Comparable<ValidationKey> {
    public static final int MAX_COMPONENTS = 32;
    public static final int MAX_COMPONENT_BYTES = 16 * 1024;
    public static final int MAX_KEY_BYTES = 64 * 1024;

    public ValidationKey {
        if (parts == null || parts.isEmpty()) throw ValidationError.invalid("key.parts");
        if (parts.size() > MAX_COMPONENTS) throw limit("components");
        long bytes = 32;
        for (Part part : parts) {
            if (part == null) throw ValidationError.invalid("key.component");
            bytes += part.retainedBytes;
            if (bytes > MAX_KEY_BYTES) throw limit("keyBytes");
        }
        parts = List.copyOf(parts);
    }

    /** Conservative owned-key charge, available without exposing or copying binary components. */
    public long retainedBytes() {
        long bytes = 32L + 8L * parts.size();
        for (Part part : parts) bytes += part.retainedBytes;
        return bytes;
    }

    @Override public int compareTo(ValidationKey other) {
        Objects.requireNonNull(other, "other");
        if (parts.size() != other.parts.size()) throw incompatible(-1);
        // Check the complete domain before ordering; an earlier unequal value cannot hide a bad tail.
        for (int i = 0; i < parts.size(); i++) {
            if (!parts.get(i).sameDomain(other.parts.get(i))) throw incompatible(i);
        }
        for (int i = 0; i < parts.size(); i++) {
            int order = parts.get(i).compareValue(other.parts.get(i));
            if (order != 0) return order;
        }
        return 0;
    }

    @Override public String toString() { return "ValidationKey[components=" + parts.size() + "]"; }

    private static TapstateException limit(String limit) {
        return new TapstateException(ValidationError.KEY_LIMIT_EXCEEDED, Map.of("limit", limit), null);
    }

    private static TapstateException incompatible(int component) {
        return new TapstateException(ValidationError.INCOMPATIBLE_KEY, Map.of("component", component), null);
    }

    public enum Kind { EXACT_NUMBER, TEXT, BOOLEAN, BINARY, DATE, TIME, LOCAL_DATE_TIME, INSTANT }

    /** Explicit logical domains prevent string guessing and lossy numeric coercion. */
    public static final class Part {
        private final Kind kind;
        private final Object value;
        private final int binarySubtype;
        private final long retainedBytes;

        private Part(Kind kind, Object value, int binarySubtype, long retainedBytes) {
            this.kind = kind;
            this.value = Objects.requireNonNull(value, "value");
            this.binarySubtype = binarySubtype;
            this.retainedBytes = retainedBytes;
        }

        public static Part number(long value) { return number(BigDecimal.valueOf(value)); }

        public static Part number(BigInteger value) {
            Objects.requireNonNull(value, "value");
            checkSize(64L + ((long) value.bitLength() + 7) / 8);
            return number(new BigDecimal(value));
        }

        public static Part number(BigDecimal value) {
            Objects.requireNonNull(value, "value");
            long bytes = 64L + ((long) value.unscaledValue().bitLength() + 7) / 8;
            checkSize(bytes);
            // Canonical scale makes equality, hash and compare agree without narrowing precision.
            BigDecimal normalized;
            try {
                normalized = value.signum() == 0 ? BigDecimal.ZERO : value.stripTrailingZeros();
            } catch (ArithmeticException unsupportedScale) {
                throw ValidationError.invalid("key.numericScale");
            }
            return new Part(Kind.EXACT_NUMBER, normalized, -1, bytes);
        }

        public static Part text(String value) {
            Objects.requireNonNull(value, "value");
            long bytes = 48L + 2L * value.length();
            checkSize(bytes);
            return new Part(Kind.TEXT, value, -1, bytes);
        }

        public static Part bool(boolean value) { return new Part(Kind.BOOLEAN, value, -1, 32); }

        public static Part binary(int subtype, byte[] value) {
            Objects.requireNonNull(value, "value");
            if (subtype < 0 || subtype > 255) throw ValidationError.invalid("key.binarySubtype");
            checkSize(48L + value.length);
            return new Part(Kind.BINARY, value.clone(), subtype, 48L + value.length);
        }

        public static Part date(LocalDate value) { return new Part(Kind.DATE, value, -1, 48); }
        public static Part time(LocalTime value) { return new Part(Kind.TIME, value, -1, 48); }
        public static Part localDateTime(LocalDateTime value) {
            return new Part(Kind.LOCAL_DATE_TIME, value, -1, 80);
        }
        public static Part instant(Instant value) { return new Part(Kind.INSTANT, value, -1, 48); }

        public Kind kind() { return kind; }
        public int binarySubtype() { return binarySubtype; }
        public Object value() { return value instanceof byte[] bytes ? bytes.clone() : value; }

        private static void checkSize(long bytes) {
            if (bytes > MAX_COMPONENT_BYTES) throw limit("componentBytes");
        }

        private boolean sameDomain(Part other) {
            return kind == other.kind && binarySubtype == other.binarySubtype;
        }

        private int compareValue(Part other) {
            return switch (kind) {
                case EXACT_NUMBER -> ((BigDecimal) value).compareTo((BigDecimal) other.value);
                case TEXT -> ((String) value).compareTo((String) other.value);
                case BOOLEAN -> Boolean.compare((Boolean) value, (Boolean) other.value);
                case BINARY -> Arrays.compareUnsigned((byte[]) value, (byte[]) other.value);
                case DATE -> ((LocalDate) value).compareTo((LocalDate) other.value);
                case TIME -> ((LocalTime) value).compareTo((LocalTime) other.value);
                case LOCAL_DATE_TIME -> ((LocalDateTime) value).compareTo((LocalDateTime) other.value);
                case INSTANT -> ((Instant) value).compareTo((Instant) other.value);
            };
        }

        @Override public boolean equals(Object other) {
            if (!(other instanceof Part part) || !sameDomain(part)) return false;
            return kind == Kind.BINARY ? Arrays.equals((byte[]) value, (byte[]) part.value)
                    : value.equals(part.value);
        }

        @Override public int hashCode() {
            int valueHash = kind == Kind.BINARY ? Arrays.hashCode((byte[]) value) : value.hashCode();
            return Objects.hash(kind.name(), binarySubtype, valueHash);
        }

        @Override public String toString() { return "Part[kind=" + kind + "]"; }
    }
}
