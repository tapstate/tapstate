package io.tapstate.core.sql;

import io.tapstate.core.common.TapstateType;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * What a plan's output columns are computed with: the operators a select item may be written from, and
 * the evaluation of one against the rows a match is made of.
 *
 * <p><b>One set, read by both sides.</b> {@link #SUPPORTED} is what the front end admits while the SQL
 * is still text and what {@link #evaluate} knows how to work out. Two lists would drift, and the drift
 * has two shapes and both are silent: an operator admitted but not evaluated crashes a running job on a
 * row that happens to reach it, and an operator evaluated but not admitted is a capability nobody can
 * use. A case below ties the two together by walking the set.
 *
 * <p><b>NULL is three-valued, because a left outer join manufactures nulls by design.</b> Every
 * unmatched dimension row arrives as nulls, so an arithmetic or a comparison that treated null as a
 * value would produce wrong numbers on exactly the rows the join exists to keep. Arithmetic with a null
 * operand is null; a comparison against null is unknown, carried as null; {@code AND} and {@code OR}
 * follow SQL's tables, so {@code false AND unknown} is false and {@code true OR unknown} is true.
 *
 * <p><b>Exact arithmetic is done in {@link BigDecimal} and then coerced to the column's declared
 * type.</b> The declared type is derived from the SQL by the same front end that built the expression,
 * so it is the one answer both halves already agree on - working the Java type out a second time from
 * the operand values is how a column declared one thing comes to hold another, and nothing compares the
 * two afterwards.
 */
public final class Expressions {

    /**
     * The operators a select item may be written from, spelled as SQL spells them. Everything else is
     * refused while the SQL is still text - see the class note on why this is one set and not two.
     */
    public static final Set<String> SUPPORTED = Set.of(
            "+", "-", "*", "/",
            "=", "<>", "<", "<=", ">", ">=",
            "AND", "OR", "NOT",
            "IS NULL", "IS NOT NULL",
            "CASE", "COALESCE",
            "||", "UPPER", "LOWER", "CHAR_LENGTH", "SUBSTRING");

    private Expressions() {
    }

    /**
     * What {@code expression} works out to over {@code sources}, which maps each source's plan name to
     * the row of it this match is made of. A source with no matching row is mapped to null, or is
     * absent - both mean the same thing and both make its columns null, because that is what an
     * unmatched outer side is.
     */
    public static Object evaluate(Expr expression, Map<String, Map<String, Object>> sources) {
        return switch (expression) {
            case Expr.Literal literal -> literal.value();
            case Expr.Column column -> {
                Map<String, Object> row = sources.get(column.ref().source());
                yield row == null ? null : row.get(column.ref().column());
            }
            case Expr.Call call -> call(call, sources);
        };
    }

    /**
     * {@code value} as the column's declared type holds it. A null stays null: a type says what a
     * present value is, never that one is present.
     */
    public static Object coerce(Object value, TapstateType type) {
        if (value == null) {
            return null;
        }
        return switch (type) {
            case INT64 -> value instanceof Number number ? number.longValue() : value;
            case DECIMAL -> decimal(value);
            case DOUBLE -> value instanceof Number number ? number.doubleValue() : value;
            case STRING -> value instanceof String ? value : String.valueOf(value);
            case BOOLEAN -> value;
            default -> value;
        };
    }

    private static Object call(Expr.Call call, Map<String, Map<String, Object>> sources) {
        List<Expr> args = call.arguments();
        switch (call.operator()) {
            // These four decide whether to look at an argument at all, so none of them may start by
            // evaluating them: CASE evaluates one branch, COALESCE stops at the first non-null, and
            // the two connectives are short-circuit in SQL as they are anywhere else.
            case "CASE":
                return caseWhen(args, sources);
            case "COALESCE":
                for (Expr arg : args) {
                    Object value = evaluate(arg, sources);
                    if (value != null) {
                        return value;
                    }
                }
                return null;
            case "AND":
                return and(args, sources);
            case "OR":
                return or(args, sources);
            default:
                break;
        }
        Object left = evaluate(args.get(0), sources);
        switch (call.operator()) {
            case "IS NULL":
                return left == null;
            case "IS NOT NULL":
                return left != null;
            case "NOT":
                return left == null ? null : !(Boolean) left;
            case "UPPER":
                return left == null ? null : String.valueOf(left).toUpperCase(java.util.Locale.ROOT);
            case "LOWER":
                return left == null ? null : String.valueOf(left).toLowerCase(java.util.Locale.ROOT);
            case "CHAR_LENGTH":
                return left == null ? null : (long) String.valueOf(left).length();
            case "-":
                if (args.size() == 1) {
                    return left == null ? null : decimal(left).negate();
                }
                break;
            default:
                break;
        }
        if (call.operator().equals("SUBSTRING")) {
            return substring(left, args, sources);
        }
        if (call.operator().equals("||")) {
            StringBuilder text = new StringBuilder();
            for (Expr arg : args) {
                Object part = evaluate(arg, sources);
                if (part == null) {
                    return null;
                }
                text.append(part);
            }
            return text.toString();
        }
        Object right = evaluate(args.get(1), sources);
        if (left == null || right == null) {
            return null;
        }
        return switch (call.operator()) {
            case "+" -> decimal(left).add(decimal(right));
            case "-" -> decimal(left).subtract(decimal(right));
            case "*" -> decimal(left).multiply(decimal(right));
            // A scale of its own, because exact division has no exact answer in general: 1/3 would
            // throw rather than answer, and throwing on a value some row happens to hold is a job that
            // dies on the data instead of on the SQL.
            case "/" -> decimal(left).divide(decimal(right), DIVISION_SCALE, java.math.RoundingMode.HALF_UP);
            case "=" -> compare(left, right) == 0;
            case "<>" -> compare(left, right) != 0;
            case "<" -> compare(left, right) < 0;
            case "<=" -> compare(left, right) <= 0;
            case ">" -> compare(left, right) > 0;
            case ">=" -> compare(left, right) >= 0;
            default -> throw new IllegalStateException(
                    "the plan holds operator '" + call.operator() + "', which nothing evaluates");
        };
    }

    /** How many places an exact division keeps. Enough for money and for a ratio read by a person. */
    private static final int DIVISION_SCALE = 16;

    /**
     * The arguments of a CASE are the whens, then the thens, then the else - the shape the SQL library
     * hands over, kept rather than rearranged so that nothing has to agree about a second one.
     */
    private static Object caseWhen(List<Expr> args, Map<String, Map<String, Object>> sources) {
        int branches = (args.size() - 1) / 2;
        for (int branch = 0; branch < branches; branch++) {
            Object when = evaluate(args.get(branch), sources);
            if (Boolean.TRUE.equals(when)) {
                return evaluate(args.get(branches + branch), sources);
            }
        }
        return evaluate(args.get(args.size() - 1), sources);
    }

    /** False if any argument is false, null if any is null and none false, true otherwise. */
    private static Object and(List<Expr> args, Map<String, Map<String, Object>> sources) {
        boolean unknown = false;
        for (Expr arg : args) {
            Object value = evaluate(arg, sources);
            if (Boolean.FALSE.equals(value)) {
                return false;
            }
            unknown |= value == null;
        }
        return unknown ? null : Boolean.TRUE;
    }

    /** True if any argument is true, null if any is null and none true, false otherwise. */
    private static Object or(List<Expr> args, Map<String, Map<String, Object>> sources) {
        boolean unknown = false;
        for (Expr arg : args) {
            Object value = evaluate(arg, sources);
            if (Boolean.TRUE.equals(value)) {
                return true;
            }
            unknown |= value == null;
        }
        return unknown ? null : Boolean.FALSE;
    }

    /** SQL counts from one and takes a length, not an end. */
    private static Object substring(Object text, List<Expr> args,
            Map<String, Map<String, Object>> sources) {
        if (text == null) {
            return null;
        }
        Object from = evaluate(args.get(1), sources);
        if (from == null) {
            return null;
        }
        String value = String.valueOf(text);
        int start = Math.max(0, ((Number) from).intValue() - 1);
        if (start >= value.length()) {
            return "";
        }
        if (args.size() < 3) {
            return value.substring(start);
        }
        Object length = evaluate(args.get(2), sources);
        if (length == null) {
            return null;
        }
        int end = Math.min(value.length(), start + Math.max(0, ((Number) length).intValue()));
        return value.substring(start, end);
    }

    /**
     * Two values compared the way a database compares them. Numbers are compared as numbers whatever
     * Java types they arrived in, because a fact row's 1 and a dimension row's 1L are the same value to
     * everyone but Java.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private static int compare(Object left, Object right) {
        if (left instanceof Number && right instanceof Number) {
            return decimal(left).compareTo(decimal(right));
        }
        if (left instanceof Comparable comparable && left.getClass().isInstance(right)) {
            return comparable.compareTo(right);
        }
        return String.valueOf(left).compareTo(String.valueOf(right));
    }

    private static BigDecimal decimal(Object value) {
        return switch (value) {
            case BigDecimal decimal -> decimal;
            case BigInteger integer -> new BigDecimal(integer);
            case Double number -> BigDecimal.valueOf(number);
            case Float number -> BigDecimal.valueOf(number);
            case Number number -> BigDecimal.valueOf(number.longValue());
            default -> new BigDecimal(String.valueOf(value));
        };
    }
}
