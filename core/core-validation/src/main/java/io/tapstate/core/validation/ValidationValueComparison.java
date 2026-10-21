package io.tapstate.core.validation;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiPredicate;

/**
 * Exact structural comparison of already owned portable facts. Codec bindings and native names are
 * provenance, not equality; logical domains must match or have an explicitly compiled compatibility
 * rule. No parsing, business transformation, tolerance, timezone inference or Nest array matching is
 * performed here. Declared model width/precision and field obligations are separate caller checks.
 * A first mismatch does not claim that every later field was checked.
 */
public final class ValidationValueComparison {
    private ValidationValueComparison() {}

    public enum Kind { VALUE, LOGICAL_TYPE, MISSING_SOURCE, MISSING_TARGET }

    /** String field segments and integer array ordinals remain separate, including literal dots. */
    public record Difference(Kind kind, List<Object> path) {
        public Difference {
            Objects.requireNonNull(kind, "kind");
            path = List.copyOf(path);
        }
    }

    public static Optional<Difference> compare(ValidationValues.Row source, ValidationValues.Row target) {
        return compare(source, target, String::equals);
    }

    public static Optional<Difference> compare(ValidationValues.Row source, ValidationValues.Row target,
            BiPredicate<String, String> compatible) {
        Objects.requireNonNull(source, "source");
        Objects.requireNonNull(target, "target");
        Objects.requireNonNull(compatible, "compatible");
        return Optional.ofNullable(compare(source.fields(), target.fields(), compatible, new ArrayList<>()));
    }

    private static Difference compare(Object source, Object target, BiPredicate<String, String> compatible,
            List<Object> path) {
        // SQL/JSON null is a presence fact; a wrapper's provenance does not turn null into a value.
        if (isNull(source) || isNull(target)) {
            return isNull(source) && isNull(target) ? null : difference(Kind.VALUE, path);
        }
        String sourceType = logicalType(source);
        String targetType = logicalType(target);
        if (!sourceType.equals(targetType) && !compatible.test(sourceType, targetType)) return difference(Kind.LOGICAL_TYPE, path);
        if (source instanceof ValidationValues.Typed || target instanceof ValidationValues.Typed) {
            Object left = source instanceof ValidationValues.Typed typed ? typed.value() : source;
            Object right = target instanceof ValidationValues.Typed typed ? typed.value() : target;
            // Check the underlying representation as well. A logical mapping cannot turn text into a
            // number, reinterpret a wall time as an instant or erase an inner typed domain.
            return compare(left, right, compatible, path);
        }
        if (source instanceof Map<?, ?> left && target instanceof Map<?, ?> right) {
            for (var field : left.entrySet()) {
                path.add(field.getKey());
                try {
                    if (!right.containsKey(field.getKey())) return difference(Kind.MISSING_TARGET, path);
                    Difference unequal = compare(field.getValue(), right.get(field.getKey()), compatible, path);
                    if (unequal != null) return unequal;
                } finally { path.removeLast(); }
            }
            for (Object name : right.keySet()) {
                if (!left.containsKey(name)) {
                    path.add(name);
                    try { return difference(Kind.MISSING_SOURCE, path); }
                    finally { path.removeLast(); }
                }
            }
            return null;
        }
        if (source instanceof List<?> left && target instanceof List<?> right) {
            int common = Math.min(left.size(), right.size());
            for (int i = 0; i < common; i++) {
                path.add(i);
                try {
                    Difference unequal = compare(left.get(i), right.get(i), compatible, path);
                    if (unequal != null) return unequal;
                } finally { path.removeLast(); }
            }
            if (left.size() != right.size()) {
                path.add(common);
                try { return difference(left.size() < right.size() ? Kind.MISSING_SOURCE : Kind.MISSING_TARGET, path); }
                finally { path.removeLast(); }
            }
            return null;
        }
        if (source instanceof Number left && target instanceof Number right) {
            return numericEquals(left, right) ? null : difference(Kind.VALUE, path);
        }
        if (source instanceof ValidationValues.Binary left && target instanceof ValidationValues.Binary right) {
            return left.sameContent(right) ? null : difference(Kind.VALUE, path);
        }
        // Different representation families need an explicit conversion before this boundary. Merely
        // declaring their logical domains compatible must not make Java equals the conversion rule.
        if (!representation(source).equals(representation(target))) return difference(Kind.LOGICAL_TYPE, path);
        return source.equals(target) ? null : difference(Kind.VALUE, path);
    }

    private static boolean numericEquals(Number left, Number right) {
        boolean floatingLeft = left instanceof Float || left instanceof Double;
        boolean floatingRight = right instanceof Float || right instanceof Double;
        if (floatingLeft && floatingRight) {
            double a = left.doubleValue();
            double b = right.doubleValue();
            return a == b || Double.isNaN(a) && Double.isNaN(b);
        }
        if (floatingLeft && !Double.isFinite(left.doubleValue())
                || floatingRight && !Double.isFinite(right.doubleValue())) return false;
        // BigDecimal(double) is intentional: it captures the exact binary value, unlike valueOf's
        // rounded decimal rendering. This lane is reachable across domains only with explicit consent.
        BigDecimal a = floatingLeft ? new BigDecimal(left.doubleValue()) : exact(left);
        BigDecimal b = floatingRight ? new BigDecimal(right.doubleValue()) : exact(right);
        return a.compareTo(b) == 0;
    }

    private static BigDecimal exact(Number value) {
        if (value instanceof BigDecimal decimal) return decimal;
        if (value instanceof BigInteger integer) return new BigDecimal(integer);
        return BigDecimal.valueOf(value.longValue());
    }

    private static boolean isNull(Object value) {
        while (value instanceof ValidationValues.Typed typed) value = typed.value();
        return value == null;
    }

    private static String logicalType(Object value) {
        return value instanceof ValidationValues.Typed typed ? typed.logicalType() : representation(value);
    }

    private static String representation(Object value) {
        if (value instanceof Byte || value instanceof Short || value instanceof Integer || value instanceof Long
                || value instanceof BigInteger || value instanceof BigDecimal) return "exact-number";
        if (value instanceof Float) return "float32";
        if (value instanceof Double) return "float64";
        if (value instanceof String) return "text";
        if (value instanceof Boolean) return "boolean";
        if (value instanceof LocalDate) return "date";
        if (value instanceof LocalTime) return "time";
        if (value instanceof LocalDateTime) return "local-date-time";
        if (value instanceof Instant) return "instant";
        if (value instanceof ValidationValues.Binary) return "binary";
        if (value instanceof Map<?, ?>) return "object";
        if (value instanceof List<?>) return "array";
        // Rows can contain only the portable shapes checked by ValidationValues.
        throw new IllegalStateException("unrecognized owned validation value");
    }

    private static Difference difference(Kind kind, List<Object> path) {
        return new Difference(kind, path);
    }
}
