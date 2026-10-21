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
import java.util.Set;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import io.tapstate.core.common.TapstateException;

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

    /** Bounds local traversal; callers separately reserve reader, retained-result and callback work. */
    public record TraversalLimits(ValidationValues.Limits row, long operations) {
        public TraversalLimits {
            Objects.requireNonNull(row, "row");
            if (operations < 1 || operations > 64L * 1024 * 1024) throw ValidationError.invalid("comparison.operations");
        }
    }

    /**
     * Visits every independently comparable field, without accumulating differences. A missing or
     * incompatible parent is one structural conclusion; its unavailable subtree is not fabricated.
     * Normal return is a completed traversal. Budget and callback failures propagate unchanged.
     * Ordinary arrays remain ordered; declared Nest arrays require their separate member matcher.
     */
    public static long visitDifferences(ValidationValues.Row source, ValidationValues.Row target,
                                       BiPredicate<String, String> compatible, TraversalLimits limits,
                                       Consumer<Difference> consumer) {
        return visitKnownDifferences(source, target, compatible, limits, List.of(), consumer);
    }

    /** Package boundary: the field checker first requires recorded source blocks for every path. */
    static long visitKnownDifferences(ValidationValues.Row source, ValidationValues.Row target,
            BiPredicate<String, String> compatible, TraversalLimits limits, List<List<Object>> unavailable,
            Consumer<Difference> consumer) {
        return visitKnownDifferences(source, target, compatible, limits, unavailable, Set.of(), consumer);
    }

    static long visitKnownDifferences(ValidationValues.Row source, ValidationValues.Row target,
            BiPredicate<String, String> compatible, TraversalLimits limits, List<List<Object>> unavailable,
            Set<String> definedFields, Consumer<Difference> consumer) {
        Objects.requireNonNull(source, "source");
        Objects.requireNonNull(target, "target");
        Objects.requireNonNull(compatible, "compatible");
        Objects.requireNonNull(limits, "limits");
        Objects.requireNonNull(consumer, "consumer");
        for (ValidationValues.Row row : List.of(source, target)) {
            if (row.accountedBytes() > limits.row().bytes() || row.nodes() > limits.row().nodes()
                    || row.depth() > limits.row().depth()) throw comparisonLimit("row");
        }
        Walker walker = new Walker(compatible, limits.operations(), unavailable, definedFields, consumer);
        walker.walk(source.fields(), target.fields(), new ArrayList<>());
        return walker.differences;
    }

    private static final class Walker {
        private final BiPredicate<String, String> compatible;
        private final Consumer<Difference> consumer;
        private final List<List<Object>> unavailable;
        private final Set<String> definedFields;
        private long remaining;
        private long differences;
        private Walker(BiPredicate<String, String> compatible, long operations, List<List<Object>> unavailable, Set<String> definedFields, Consumer<Difference> consumer) {
            this.compatible = compatible;
            this.unavailable = unavailable;
            this.definedFields = definedFields;
            this.remaining = operations;
            this.consumer = consumer;
        }
        private void work(long amount) {
            if (amount > remaining) throw comparisonLimit("operations");
            remaining -= amount;
        }
        private void emit(Kind kind, List<Object> path) {
            if (unavailable(path)) return;
            work(1L + path.size());
            consumer.accept(difference(kind, path));
            differences++;
        }
        private boolean nullValue(Object value) {
            while (value instanceof ValidationValues.Typed typed) {
                work(1);
                value = typed.value();
            }
            return value == null;
        }
        private void walk(Object source, Object target, List<Object> path) {
            work(1);
            if (unavailable(path)) return;
            boolean sourceNull = nullValue(source);
            boolean targetNull = nullValue(target);
            if (sourceNull || targetNull) {
                if (sourceNull != targetNull) emit(Kind.VALUE, path);
                return;
            }
            String sourceType = logicalType(source);
            String targetType = logicalType(target);
            work((long) sourceType.length() + targetType.length());
            if (!sourceType.equals(targetType) && !compatible.test(sourceType, targetType)) {
                emit(Kind.LOGICAL_TYPE, path);
                return;
            }
            if (source instanceof ValidationValues.Typed || target instanceof ValidationValues.Typed) {
                walk(source instanceof ValidationValues.Typed typed ? typed.value() : source,
                        target instanceof ValidationValues.Typed typed ? typed.value() : target, path);
                return;
            }
            if (source instanceof Map<?, ?> left && target instanceof Map<?, ?> right) {
                for (var field : left.entrySet()) {
                    work(1L + ((String) field.getKey()).length());
                    path.add(field.getKey());
                    try {
                        if (!right.containsKey(field.getKey())) emit(Kind.MISSING_TARGET, path);
                        else walk(field.getValue(), right.get(field.getKey()), path);
                    } finally { path.removeLast(); }
                }
                for (Object name : right.keySet()) {
                    work(1L + ((String) name).length());
                    if (!left.containsKey(name)) {
                        path.add(name);
                        try { emit(Kind.MISSING_SOURCE, path); }
                        finally { path.removeLast(); }
                    }
                }
                return;
            }
            if (source instanceof List<?> left && target instanceof List<?> right) {
                int total = Math.max(left.size(), right.size());
                for (int i = 0; i < total; i++) {
                    work(1);
                    path.add(i);
                    try {
                        if (i >= left.size()) emit(Kind.MISSING_SOURCE, path);
                        else if (i >= right.size()) emit(Kind.MISSING_TARGET, path);
                        else walk(left.get(i), right.get(i), path);
                    } finally { path.removeLast(); }
                }
                return;
            }
            if (source instanceof Number left && target instanceof Number right) {
                work(numberWork(left) + numberWork(right));
                if (!numericEquals(left, right)) emit(Kind.VALUE, path);
                return;
            }
            if (source instanceof ValidationValues.Binary left && target instanceof ValidationValues.Binary right) {
                work(1L + Math.max(left.length(), right.length()));
                if (!left.sameContent(right)) emit(Kind.VALUE, path);
                return;
            }
            if (!representation(source).equals(representation(target))) {
                emit(Kind.LOGICAL_TYPE, path);
                return;
            }
            if (source instanceof String left && target instanceof String right) work(1L + Math.max(left.length(), right.length()));
            if (!source.equals(target)) emit(Kind.VALUE, path);
        }
        private boolean unavailable(List<Object> path) {
            for (var blocked : unavailable) {
                work(1);
                if (blocked.isEmpty() && !definedFields.isEmpty()) {
                    if (path.isEmpty()) continue;
                    if (path.getFirst() instanceof String field) {
                        work(1L + field.length());
                        if (definedFields.contains(field)) continue;
                    }
                }
                if (blocked.size() > path.size()) continue;
                boolean prefix = true;
                for (int i = 0; i < blocked.size(); i++) {
                    Object expected = blocked.get(i); Object actual = path.get(i);
                    work(1L + (expected instanceof String left && actual instanceof String right
                            ? Math.min(left.length(), right.length()) : 0));
                    if (!expected.equals(actual)) { prefix = false; break; }
                }
                if (prefix) return true;
            }
            return false;
        }
        private static long numberWork(Number number) {
            if (number instanceof BigInteger integer) return 1L + integer.bitLength();
            if (number instanceof BigDecimal decimal) return 1L + decimal.unscaledValue().bitLength();
            return 64;
        }
    }

    private static TapstateException comparisonLimit(String dimension) {
        return new TapstateException(ValidationError.VALUE_LIMIT_EXCEEDED, Map.of("limit", "comparison." + dimension), null);
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
