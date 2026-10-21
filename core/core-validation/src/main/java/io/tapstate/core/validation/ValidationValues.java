package io.tapstate.core.validation;

import io.tapstate.core.common.TapstateException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Bounded ownership of already portable read facts. No codec or business transformation runs here.
 * The reader must separately reserve memory before driver materialization; this boundary only bounds
 * the copies it makes. Accounted bytes are conservative local charges, not measured heap size.
 */
public final class ValidationValues {
    private ValidationValues() {}

    public record Limits(long bytes, int nodes, int depth) {
        public Limits {
            if (bytes < 1 || bytes > 32L * 1024 * 1024 || nodes < 1 || nodes > 65536
                    || depth < 1 || depth > 64) throw ValidationError.invalid("value.limits");
        }
    }

    /** Codec and logical type provenance is retained even within arrays. */
    public record Typed(String logicalType, String nativeType, String codecBinding, Object value) {
        public Typed {
            logicalType = ValidationError.identifier(logicalType, "logicalType");
            if (nativeType != null) nativeType = ValidationError.identifier(nativeType, "nativeType");
            codecBinding = ValidationError.identifier(codecBinding, "codecBinding");
        }
    }

    /** An immutable binary value; -1 means no subtype was supplied by the portable reader. */
    public static final class Binary {
        private final int subtype;
        private final byte[] bytes;

        public Binary(int subtype, byte[] bytes) {
            Objects.requireNonNull(bytes, "bytes");
            if (subtype < -1 || subtype > 255) throw ValidationError.invalid("binary.subtype");
            if (bytes.length > 32 * 1024 * 1024) throw limit("bytes");
            this.subtype = subtype;
            this.bytes = bytes.clone();
        }

        public int subtype() { return subtype; }
        public byte[] bytes() { return bytes.clone(); }
        public int length() { return bytes.length; }
        @Override public String toString() { return "Binary[subtype=" + subtype + ", length=" + bytes.length + "]"; }
    }

    /** A deeply owned row, with a size charge the caller can retain with its reservation. */
    public static final class Row {
        private final Map<String, Object> fields;
        private final long accountedBytes;
        private final int nodes;

        private Row(Map<String, Object> fields, long accountedBytes, int nodes) {
            this.fields = fields;
            this.accountedBytes = accountedBytes;
            this.nodes = nodes;
        }

        public Map<String, Object> fields() { return fields; }
        public long accountedBytes() { return accountedBytes; }
        public int nodes() { return nodes; }
        @Override public String toString() { return "Row[fields=" + fields.size() + ", bytes=" + accountedBytes + "]"; }
    }

    public static Row ownRow(Map<String, ?> fields, Limits limits) {
        Objects.requireNonNull(fields, "fields");
        Meter meter = new Meter(Objects.requireNonNull(limits, "limits"));
        @SuppressWarnings("unchecked")
        Map<String, Object> copy = (Map<String, Object>) freeze(fields, 1, meter, new IdentityHashMap<>());
        return new Row(copy, meter.bytes, meter.nodes);
    }

    private static Object freeze(Object value, int depth, Meter meter, IdentityHashMap<Object, Boolean> active) {
        meter.node(depth);
        if (value == null) return null;
        if (value instanceof String text) {
            meter.charge(24L + 2L * text.length());
            return text;
        }
        if (value.getClass() == BigInteger.class) {
            BigInteger integer = (BigInteger) value;
            meter.charge(32L + ((long) integer.bitLength() + 7) / 8);
            return integer;
        }
        if (value.getClass() == BigDecimal.class) {
            BigDecimal decimal = (BigDecimal) value;
            meter.charge(48L + ((long) decimal.unscaledValue().bitLength() + 7) / 8);
            return decimal;
        }
        if (value instanceof Byte || value instanceof Short || value instanceof Integer || value instanceof Long
                || value instanceof Float || value instanceof Double || value instanceof Boolean
                || value instanceof LocalDate || value instanceof LocalTime || value instanceof LocalDateTime
                || value instanceof Instant) {
            meter.charge(48);
            return value;
        }
        if (value instanceof Binary binary) {
            meter.charge(48L + binary.length());
            return binary;
        }
        if (!(value instanceof Typed || value instanceof Map<?, ?> || value instanceof List<?>)) {
            throw new TapstateException(ValidationError.VALUE_TYPE_UNSUPPORTED,
                    Map.of("type", value.getClass().getName()), null);
        }
        if (active.put(value, Boolean.TRUE) != null) throw ValidationError.invalid("value.cycle");
        try {
            if (value instanceof Typed typed) {
                meter.charge(64L + 2L * typed.logicalType().length() + 2L * typed.codecBinding().length()
                        + (typed.nativeType() == null ? 0 : 2L * typed.nativeType().length()));
                return new Typed(typed.logicalType(), typed.nativeType(), typed.codecBinding(),
                        freeze(typed.value(), depth + 1, meter, active));
            }
            if (value instanceof Map<?, ?> map) {
                meter.reserveChildren(map.size());
                meter.charge(64L + 48L * map.size());
                Map<String, Object> copy = new LinkedHashMap<>();
                for (var entry : map.entrySet()) {
                    if (!(entry.getKey() instanceof String name)) throw ValidationError.invalid("value.fieldName");
                    if (copy.containsKey(name)) throw ValidationError.invalid("value.duplicateField");
                    meter.charge(24L + 2L * name.length());
                    copy.put(name, freeze(entry.getValue(), depth + 1, meter, active));
                }
                return Collections.unmodifiableMap(copy);
            }
            List<?> list = (List<?>) value;
            meter.reserveChildren(list.size());
            meter.charge(32L + 8L * list.size());
            List<Object> copy = new ArrayList<>(list.size());
            for (Object child : list) copy.add(freeze(child, depth + 1, meter, active));
            return Collections.unmodifiableList(copy);
        } finally {
            active.remove(value);
        }
    }

    private static TapstateException limit(String dimension) {
        return new TapstateException(ValidationError.VALUE_LIMIT_EXCEEDED, Map.of("limit", dimension), null);
    }

    private static final class Meter {
        private final Limits limits;
        private long bytes;
        private int nodes;

        private Meter(Limits limits) { this.limits = limits; }
        private void node(int depth) {
            if (depth > limits.depth()) throw limit("depth");
            if (nodes == limits.nodes()) throw limit("nodes");
            nodes++;
            charge(16);
        }
        private void reserveChildren(int count) {
            if (count > limits.nodes() - nodes) throw limit("nodes");
        }
        private void charge(long amount) {
            if (amount > limits.bytes() - bytes) throw limit("bytes");
            bytes += amount;
        }
    }
}
