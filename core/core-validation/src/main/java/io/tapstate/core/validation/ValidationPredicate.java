package io.tapstate.core.validation;

import io.tapstate.core.common.TapstateException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayDeque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** A bounded independent current-row predicate. It contains no expression-runtime or event-envelope API. */
public final class ValidationPredicate {
    public sealed interface Expr permits Constant, Field, Present, Compare, And, Or, Not {}
    /** Only immutable scalar constants are admitted by compile. */
    public record Constant(Object value) implements Expr {}
    public record Field(List<String> path) implements Expr { public Field { path = ValidationPredicate.path(path); } }
    public record Present(List<String> path) implements Expr { public Present { path = ValidationPredicate.path(path); } }
    public enum Operator { EQ, NE, LT, LE, GT, GE }
    public record Compare(Operator operator, Expr left, Expr right) implements Expr {
        public Compare { Objects.requireNonNull(operator); Objects.requireNonNull(left); Objects.requireNonNull(right); }
    }
    public record And(Expr left, Expr right) implements Expr {
        public And { Objects.requireNonNull(left); Objects.requireNonNull(right); }
    }
    public record Or(Expr left, Expr right) implements Expr {
        public Or { Objects.requireNonNull(left); Objects.requireNonNull(right); }
    }
    public record Not(Expr operand) implements Expr { public Not { Objects.requireNonNull(operand); } }
    public enum ProblemKind { MISSING_FIELD, INVALID_PARENT, INCOMPATIBLE_TYPE, UNSUPPORTED_VALUE, NOT_BOOLEAN }
    public record Problem(ProblemKind kind, List<String> path) {
        public Problem {
            Objects.requireNonNull(kind);
            if (path == null || path.size() > 32) throw ValidationError.invalid("predicate.problemPath");
            path.forEach(segment -> ValidationError.identifier(segment, "predicate.problemPath"));
            path = List.copyOf(path);
        }
    }
    /** Exactly one of matches and problem is present; a data problem must not be treated as false. */
    public record Result(Boolean matches, Problem problem) {
        public Result {
            if ((matches == null) == (problem == null)) throw new IllegalArgumentException("one predicate outcome is required");
        }
    }
    /** Numeric compatibility must come from fixed admitted rules, never inferred from row samples. */
    public record Limits(ValidationValues.Limits row, int maxOperations, boolean compatibleNumericDomains) {
        public Limits {
            Objects.requireNonNull(row);
            if (maxOperations < 1 || maxOperations > 1_000_000) throw ValidationError.invalid("predicate.maxOperations");
        }
    }

    private record Frame(Expr expression, int depth) {}
    private record Value(Object value, List<String> path, Problem problem) {}
    private record Scalar(Object value, String domain, List<String> path, Problem problem) {}
    private static final int MAX_NODES = 256;
    private static final int MAX_DEPTH = 32;
    private final String node;
    private final Expr root;
    private final Limits limits;
    private final long retainedBytes;

    private ValidationPredicate(String node, Expr root, Limits limits, long retainedBytes) {
        this.node = node; this.root = root; this.limits = limits; this.retainedBytes = retainedBytes;
    }

    /** The owner reserves compilation copies and retained program bytes before this boundary. */
    public static ValidationPredicate compile(String node, Expr expression, Limits limits) {
        node = ValidationError.identifier(node, "predicate.node");
        Objects.requireNonNull(expression); Objects.requireNonNull(limits);
        var pending = new ArrayDeque<Frame>(); pending.push(new Frame(expression, 1));
        var constants = new LinkedHashMap<String, Object>();
        int nodes = 0;
        long metadata = 256L + 2L * node.length();
        while (!pending.isEmpty()) {
            var frame = pending.pop();
            if (++nodes > MAX_NODES || frame.depth() > MAX_DEPTH) throw limit("filter.ast");
            metadata += 64;
            switch (frame.expression()) {
                case Constant constant -> {
                    if (constant.value() != null && domain(constant.value()) == null) throw ValidationError.invalid("predicate.literal");
                    constants.put(Integer.toString(nodes), constant.value());
                }
                case Field field -> metadata += pathBytes(field.path());
                case Present present -> metadata += pathBytes(present.path());
                case Compare compare -> children(pending, frame, compare.left(), compare.right());
                case And and -> children(pending, frame, and.left(), and.right());
                case Or or -> children(pending, frame, or.left(), or.right());
                case Not not -> pending.push(new Frame(not.operand(), frame.depth() + 1));
            }
            if (metadata > limits.row().bytes()) throw limit("filter.programBytes");
        }
        long bytes = metadata + ValidationValues.ownRow(constants, limits.row()).accountedBytes();
        if (bytes > limits.row().bytes()) throw limit("filter.programBytes");
        return new ValidationPredicate(node, expression, limits, bytes);
    }

    public String node() { return node; }
    public long retainedBytes() { return retainedBytes; }

    /** Resource/protocol/authorization errors are thrown, never represented as absorbable data problems. */
    public Result evaluate(ValidationValues.Row row) {
        Objects.requireNonNull(row);
        if (row.accountedBytes() > limits.row().bytes() || row.nodes() > limits.row().nodes() || row.depth() > limits.row().depth())
            throw limit("filter.input");
        var budget = new Budget(limits.maxOperations());
        Value value = bool(evaluate(root, row.fields(), budget), budget);
        return value.problem() == null ? new Result((Boolean)value.value(), null) : new Result(null, value.problem());
    }

    private Value evaluate(Expr expression, Map<String, Object> row, Budget budget) {
        budget.spend(1);
        return switch (expression) {
            case Constant constant -> value(constant.value());
            case Field field -> field(row, field.path(), false, budget);
            case Present present -> field(row, present.path(), true, budget);
            case Not not -> {
                var operand = bool(evaluate(not.operand(), row, budget), budget);
                yield operand.problem() == null ? value(!(Boolean)operand.value()) : operand;
            }
            case And and -> logical(and.left(), and.right(), false, row, budget);
            case Or or -> logical(or.left(), or.right(), true, row, budget);
            case Compare compare -> compare(compare.operator(), evaluate(compare.left(), row, budget), evaluate(compare.right(), row, budget), budget);
        };
    }

    private Value logical(Expr left, Expr right, boolean decisive, Map<String, Object> row, Budget budget) {
        var a = bool(evaluate(left, row, budget), budget);
        if (a.problem() == null && a.value().equals(decisive)) return value(decisive);
        var b = bool(evaluate(right, row, budget), budget);
        if (b.problem() == null && b.value().equals(decisive)) return value(decisive);
        if (a.problem() != null) return a;
        if (b.problem() != null) return b;
        return value(!decisive);
    }

    private Value compare(Operator operator, Value left, Value right, Budget budget) {
        var a = scalar(left, budget); var b = scalar(right, budget);
        if (a.problem() != null) return problem(a.problem());
        if (b.problem() != null) return problem(b.problem());
        boolean equality = operator == Operator.EQ || operator == Operator.NE;
        if (a.value() == null || b.value() == null) {
            if (!equality) return problem(ProblemKind.INCOMPATIBLE_TYPE, a.value() == null ? a.path() : b.path());
            boolean same = a.value() == null && b.value() == null;
            return value(operator == Operator.EQ ? same : !same);
        }
        boolean numeric = number(a.domain()) && number(b.domain());
        if (!a.domain().equals(b.domain()) && !(numeric && limits.compatibleNumericDomains()))
            return problem(ProblemKind.INCOMPATIBLE_TYPE, a.path().isEmpty() ? b.path() : a.path());
        int order;
        if (numeric) {
            if (nan(a.value()) || nan(b.value())) {
                if (!equality) return problem(ProblemKind.UNSUPPORTED_VALUE, nan(a.value()) ? a.path() : b.path());
                boolean same = nan(a.value()) && nan(b.value());
                return value(operator == Operator.EQ ? same : !same);
            }
            order = numericOrder((Number)a.value(), (Number)b.value(), budget);
        } else if (a.value() instanceof String x && b.value() instanceof String y) {
            order = textOrder(x, y, budget);
        } else if (a.value() instanceof Boolean x && b.value() instanceof Boolean y && equality) {
            order = Boolean.compare(x, y);
        } else return problem(ProblemKind.INCOMPATIBLE_TYPE, a.path());
        return value(switch (operator) {
            case EQ -> order == 0; case NE -> order != 0; case LT -> order < 0;
            case LE -> order <= 0; case GT -> order > 0; case GE -> order >= 0;
        });
    }

    private static Value field(Map<String, Object> row, List<String> path, boolean presence, Budget budget) {
        Object current = row;
        for (int i = 0; i < path.size(); i++) {
            budget.spend(1);
            while (current instanceof ValidationValues.Typed typed) {
                budget.spend(1);
                if (!typed.logicalType().equals("object") && !typed.logicalType().equals("json"))
                    return problem(ProblemKind.INVALID_PARENT, path.subList(0, i));
                current = typed.value();
            }
            if (!(current instanceof Map<?, ?> object)) return problem(ProblemKind.INVALID_PARENT, path.subList(0, i));
            boolean exists = object.containsKey(path.get(i));
            if (presence && i == path.size() - 1) return value(exists);
            if (!exists) return problem(ProblemKind.MISSING_FIELD, path.subList(0, i + 1));
            current = object.get(path.get(i));
        }
        return new Value(current, path, null);
    }

    private static Value bool(Value value, Budget budget) {
        var scalar = scalar(value, budget);
        if (scalar.problem() != null) return problem(scalar.problem());
        return scalar.value() instanceof Boolean ? new Value(scalar.value(), value.path(), null)
                : problem(ProblemKind.NOT_BOOLEAN, value.path());
    }

    private static Scalar scalar(Value value, Budget budget) {
        if (value.problem() != null) return new Scalar(null, null, value.path(), value.problem());
        Object raw = value.value();
        String declared = null;
        while (raw instanceof ValidationValues.Typed typed) {
            budget.spend(1);
            if (declared != null && !declared.equals(typed.logicalType()))
                return badScalar(value, ProblemKind.INCOMPATIBLE_TYPE);
            declared = typed.logicalType(); raw = typed.value();
        }
        if (raw == null) return new Scalar(null, "null", value.path(), null);
        String actual = domain(raw);
        if (actual == null) return badScalar(value, ProblemKind.UNSUPPORTED_VALUE);
        if (declared != null && !declared.equals(actual)) return badScalar(value, ProblemKind.INCOMPATIBLE_TYPE);
        if (raw instanceof String text && !validText(text, budget)) return badScalar(value, ProblemKind.UNSUPPORTED_VALUE);
        return new Scalar(raw, actual, value.path(), null);
    }

    private static Scalar badScalar(Value value, ProblemKind kind) {
        return new Scalar(null, null, value.path(), new Problem(kind, value.path()));
    }
    private static String domain(Object value) {
        if (value instanceof Byte || value instanceof Short || value instanceof Integer || value instanceof Long
                || value instanceof BigInteger || value instanceof BigDecimal) return "exact-number";
        if (value instanceof Float) return "float32";
        if (value instanceof Double) return "float64";
        if (value instanceof String) return "text";
        if (value instanceof Boolean) return "boolean";
        return null;
    }
    private static boolean number(String domain) { return domain.equals("exact-number") || domain.equals("float32") || domain.equals("float64"); }
    private static boolean nan(Object value) { return value instanceof Double d && d.isNaN() || value instanceof Float f && f.isNaN(); }
    private static boolean infinite(Number value) { return (value instanceof Float || value instanceof Double) && Double.isInfinite(value.doubleValue()); }
    private static int numericOrder(Number left, Number right, Budget budget) {
        if (infinite(left) || infinite(right)) {
            if (infinite(left) && infinite(right)) return Double.compare(left.doubleValue(), right.doubleValue());
            return infinite(left) ? (left.doubleValue() < 0 ? -1 : 1) : (right.doubleValue() < 0 ? 1 : -1);
        }
        chargeNumber(left, budget); chargeNumber(right, budget);
        return exact(left).compareTo(exact(right));
    }
    private static void chargeNumber(Number value, Budget budget) {
        if (value instanceof BigDecimal d) budget.spend(1L + d.unscaledValue().bitLength() / 32L + Math.abs((long)d.scale()));
        else if (value instanceof BigInteger i) budget.spend(1L + i.bitLength() / 32L);
        else budget.spend(1);
    }
    private static BigDecimal exact(Number value) {
        if (value instanceof BigDecimal d) return d;
        if (value instanceof BigInteger i) return new BigDecimal(i);
        if (value instanceof Float || value instanceof Double) return new BigDecimal(value.doubleValue());
        return BigDecimal.valueOf(value.longValue());
    }
    private static boolean validText(String text, Budget budget) {
        for (int i = 0; i < text.length(); i++) {
            budget.spend(1);
            char next = text.charAt(i);
            if (Character.isHighSurrogate(next)) {
                if (++i == text.length() || !Character.isLowSurrogate(text.charAt(i))) return false;
            } else if (Character.isLowSurrogate(next)) return false;
        }
        return true;
    }

    private static int textOrder(String left, String right, Budget budget) {
        int a = 0, b = 0;
        while (a < left.length() && b < right.length()) {
            budget.spend(1);
            int x = left.codePointAt(a), y = right.codePointAt(b);
            if (x != y) return Integer.compare(x, y);
            a += Character.charCount(x); b += Character.charCount(y);
        }
        return Integer.compare(left.length() - a, right.length() - b);
    }
    private static List<String> path(List<String> path) {
        if (path == null || path.isEmpty() || path.size() > 32) throw ValidationError.invalid("predicate.path");
        path.forEach(segment -> ValidationError.identifier(segment, "predicate.path"));
        return List.copyOf(path);
    }
    private static long pathBytes(List<String> path) { return path.stream().mapToLong(s -> 48L + 2L * s.length()).sum(); }
    private static void children(ArrayDeque<Frame> pending, Frame frame, Expr left, Expr right) {
        pending.push(new Frame(right, frame.depth() + 1)); pending.push(new Frame(left, frame.depth() + 1));
    }
    private static Value value(Object value) { return new Value(value, List.of(), null); }
    private static Value problem(ProblemKind kind, List<String> path) { return problem(new Problem(kind, path)); }
    private static Value problem(Problem problem) { return new Value(null, problem.path(), problem); }
    private static TapstateException limit(String dimension) {
        return new TapstateException(ValidationError.VALUE_LIMIT_EXCEEDED, Map.of("limit", dimension), null);
    }
    private static final class Budget {
        private long remaining;
        Budget(long remaining) { this.remaining = remaining; }
        void spend(long units) { if (units > remaining) throw limit("filter.operations"); remaining -= units; }
    }
}
