package io.tapstate.core.dsl;

import com.google.common.collect.ImmutableList;
import com.google.common.primitives.UnsignedLong;
import dev.cel.common.CelAbstractSyntaxTree;
import dev.cel.common.CelFunctionDecl;
import dev.cel.common.CelOptions;
import dev.cel.common.CelOverloadDecl;
import dev.cel.common.ast.CelConstant;
import dev.cel.common.ast.CelExpr;
import dev.cel.common.ast.CelReference;
import dev.cel.common.internal.DefaultMessageFactory;
import dev.cel.common.internal.DynamicProto;
import dev.cel.common.navigation.CelNavigableAst;
import dev.cel.common.navigation.CelNavigableExpr;
import dev.cel.common.types.CelType;
import dev.cel.common.types.OpaqueType;
import dev.cel.common.types.SimpleType;
import dev.cel.parser.Operator;
import dev.cel.runtime.CelFunctionBinding;
import dev.cel.runtime.CelFunctionResolver;
import dev.cel.runtime.CelLateFunctionBindings;
import dev.cel.runtime.CelRuntimeBuilder;
import dev.cel.runtime.CelStandardFunctions;
import dev.cel.runtime.ProtoMessageRuntimeEquality;
import dev.cel.runtime.RuntimeEquality;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.regex.Pattern;

/**
 * Exact numerical comparison declarations and runtime bindings for tapstate.decimal.
 *
 * <p>CEL's standard library does not declare comparison overloads on opaque types, so an exact
 * fixed-point column previously could not be compared in typed expressions without being refused as
 * unsupported. These declarations and matching runtime bindings provide exact comparisons with other
 * decimals, 64-bit integers, unsigned integers, and floating-point values in both operand orders,
 * without silently converting decimal columns through binary floating point.
 */
public final class DecimalComparisonFunctions {

    public static final CelType DECIMAL_TYPE = OpaqueType.create("tapstate.decimal");
    static final String EXACT_LITERAL_RANGE = "the numeric literal is outside the exact decimal comparison range";

    private static final String OP_EQUALS = "equals";
    private static final String OP_NOT_EQUALS = "not_equals";
    private static final String SUFFIX_DECIMAL_DECIMAL = "_decimal_decimal";
    private static final String SUFFIX_DECIMAL_INT64 = "_decimal_int64";
    private static final String SUFFIX_INT64_DECIMAL = "_int64_decimal";
    private static final String SUFFIX_DECIMAL_UINT64 = "_decimal_uint64";
    private static final String SUFFIX_UINT64_DECIMAL = "_uint64_decimal";
    private static final String SUFFIX_DECIMAL_DOUBLE = "_decimal_double";
    private static final String SUFFIX_DOUBLE_DECIMAL = "_double_decimal";

    private enum ComparisonOp {
        LT(Operator.LESS, "less"),
        LE(Operator.LESS_EQUALS, "less_equals"),
        GT(Operator.GREATER, "greater"),
        GE(Operator.GREATER_EQUALS, "greater_equals"),
        EQ(Operator.EQUALS, OP_EQUALS),
        NE(Operator.NOT_EQUALS, OP_NOT_EQUALS);

        private final Operator operator;
        private final String prefix;

        ComparisonOp(Operator operator, String prefix) {
            this.operator = operator;
            this.prefix = prefix;
        }

        boolean equality() {
            return this == EQ || this == NE;
        }
    }

    private static final Map<String, ComparisonOp> OPERATORS = Map.of(
            Operator.LESS.getFunction(), ComparisonOp.LT,
            Operator.LESS_EQUALS.getFunction(), ComparisonOp.LE,
            Operator.GREATER.getFunction(), ComparisonOp.GT,
            Operator.GREATER_EQUALS.getFunction(), ComparisonOp.GE,
            Operator.EQUALS.getFunction(), ComparisonOp.EQ,
            Operator.NOT_EQUALS.getFunction(), ComparisonOp.NE);

    private static final Pattern FLOAT_TOKEN = Pattern.compile(
            "(?:\\d+(?:\\.\\d*)?|\\.\\d+)(?:[eE][+-]?\\d+)?");

    private static final RuntimeEquality DEFAULT_EQUALITY =
            ProtoMessageRuntimeEquality.create(DynamicProto.create(DefaultMessageFactory.INSTANCE), CelOptions.DEFAULT);

    private static final ImmutableList<CelFunctionDecl> ORDERING_DECLARATIONS = buildOrderingDeclarations();
    private static final ImmutableList<CelFunctionDecl> CROSS_TYPE_EQUALITY_DECLARATIONS = buildCrossTypeEqualityDeclarations();
    private static final ImmutableList<CelFunctionDecl> ALL_DECLARATIONS = ImmutableList.<CelFunctionDecl>builder()
            .addAll(ORDERING_DECLARATIONS)
            .addAll(CROSS_TYPE_EQUALITY_DECLARATIONS)
            .build();

    private DecimalComparisonFunctions() {
    }

    /** Declarations for all comparison operators (&lt;, &lt;=, &gt;, &gt;=, ==, !=). */
    public static ImmutableList<CelFunctionDecl> declarations() {
        return ALL_DECLARATIONS;
    }

    /** Declarations for ordering operators (&lt;, &lt;=, &gt;, &gt;=). */
    public static ImmutableList<CelFunctionDecl> orderingDeclarations() {
        return ORDERING_DECLARATIONS;
    }

    /** Declarations for cross-type equality operators (==, !=) between decimals and int/uint/double. */
    public static ImmutableList<CelFunctionDecl> crossTypeEqualityDeclarations() {
        return CROSS_TYPE_EQUALITY_DECLARATIONS;
    }

    /**
     * Installs decimal-aware equality alongside CEL's standard runtime functions.
     *
     * <p>Equality fallback for non-decimal operands defaults to {@link CelOptions#DEFAULT} and
     * standard protobuf message equality. Callers configuring runtime builders with non-default
     * options should note that non-decimal equality fallback uses default CEL equality behavior.
     */
    public static CelRuntimeBuilder configureRuntime(CelRuntimeBuilder builder) {
        return builder
                .setStandardEnvironmentEnabled(false)
                .setStandardFunctions(CelStandardFunctions.newBuilder()
                        .excludeFunctions(
                                CelStandardFunctions.StandardFunction.EQUALS,
                                CelStandardFunctions.StandardFunction.NOT_EQUALS)
                        .build())
                .addFunctionBindings(
                        CelFunctionBinding.from(OP_EQUALS, Object.class, Object.class,
                                DecimalComparisonFunctions::objectEquals),
                        CelFunctionBinding.from(OP_NOT_EQUALS, Object.class, Object.class,
                                (Object x, Object y) -> !objectEquals(x, y)));
    }

    private static ImmutableList<CelFunctionDecl> buildOrderingDeclarations() {
        return Arrays.stream(ComparisonOp.values()).filter(op -> !op.equality())
                .map(op -> buildOrderingDecl(op.operator, op.prefix)).collect(ImmutableList.toImmutableList());
    }

    private static ImmutableList<CelFunctionDecl> buildCrossTypeEqualityDeclarations() {
        return Arrays.stream(ComparisonOp.values()).filter(ComparisonOp::equality)
                .map(op -> buildCrossTypeEqualityDecl(op.operator, op.prefix)).collect(ImmutableList.toImmutableList());
    }

    private static CelFunctionDecl buildOrderingDecl(Operator operator, String prefix) {
        return CelFunctionDecl.newFunctionDeclaration(
                operator.getFunction(),
                CelOverloadDecl.newGlobalOverload(prefix + SUFFIX_DECIMAL_DECIMAL, SimpleType.BOOL, DECIMAL_TYPE, DECIMAL_TYPE),
                CelOverloadDecl.newGlobalOverload(prefix + SUFFIX_DECIMAL_INT64, SimpleType.BOOL, DECIMAL_TYPE, SimpleType.INT),
                CelOverloadDecl.newGlobalOverload(prefix + SUFFIX_INT64_DECIMAL, SimpleType.BOOL, SimpleType.INT, DECIMAL_TYPE),
                CelOverloadDecl.newGlobalOverload(prefix + SUFFIX_DECIMAL_UINT64, SimpleType.BOOL, DECIMAL_TYPE, SimpleType.UINT),
                CelOverloadDecl.newGlobalOverload(prefix + SUFFIX_UINT64_DECIMAL, SimpleType.BOOL, SimpleType.UINT, DECIMAL_TYPE),
                CelOverloadDecl.newGlobalOverload(prefix + SUFFIX_DECIMAL_DOUBLE, SimpleType.BOOL, DECIMAL_TYPE, SimpleType.DOUBLE),
                CelOverloadDecl.newGlobalOverload(prefix + SUFFIX_DOUBLE_DECIMAL, SimpleType.BOOL, SimpleType.DOUBLE, DECIMAL_TYPE)
        );
    }

    private static CelFunctionDecl buildCrossTypeEqualityDecl(Operator operator, String prefix) {
        return CelFunctionDecl.newFunctionDeclaration(
                operator.getFunction(),
                CelOverloadDecl.newGlobalOverload(prefix + SUFFIX_DECIMAL_INT64, SimpleType.BOOL, DECIMAL_TYPE, SimpleType.INT),
                CelOverloadDecl.newGlobalOverload(prefix + SUFFIX_INT64_DECIMAL, SimpleType.BOOL, SimpleType.INT, DECIMAL_TYPE),
                CelOverloadDecl.newGlobalOverload(prefix + SUFFIX_DECIMAL_UINT64, SimpleType.BOOL, DECIMAL_TYPE, SimpleType.UINT),
                CelOverloadDecl.newGlobalOverload(prefix + SUFFIX_UINT64_DECIMAL, SimpleType.BOOL, SimpleType.UINT, DECIMAL_TYPE),
                CelOverloadDecl.newGlobalOverload(prefix + SUFFIX_DECIMAL_DOUBLE, SimpleType.BOOL, DECIMAL_TYPE, SimpleType.DOUBLE),
                CelOverloadDecl.newGlobalOverload(prefix + SUFFIX_DOUBLE_DECIMAL, SimpleType.BOOL, SimpleType.DOUBLE, DECIMAL_TYPE)
        );
    }

    /** A runtime AST and the decimal overloads it can dispatch after standard CEL dispatch. */
    public record RuntimeExpression(CelAbstractSyntaxTree ast, CelFunctionResolver functions) {
    }

    /** CEL-only value for a Decimal128 special carried as a Double. */
    public static final class DecimalSpecialValue extends Number {
        private final double value;

        private DecimalSpecialValue(double value) {
            this.value = value;
        }

        public static Number bind(double value) {
            return !Double.isFinite(value) || Double.doubleToRawLongBits(value) == Long.MIN_VALUE
                    ? new DecimalSpecialValue(value) : value;
        }

        @Override public int intValue() { return (int) value; }
        @Override public long longValue() { return (long) value; }
        @Override public float floatValue() { return (float) value; }
        @Override public double doubleValue() { return value; }
    }

    /**
     * Constant doubles in the AST have already been rounded by CEL. Keep the source token beside
     * comparison calls that can see a decimal, leaving the original operands and standard overloads
     * in place. An ordinary DOUBLE column uses its binary value, not this source-token path.
     */
    public static RuntimeExpression forRuntime(CelAbstractSyntaxTree ast) {
        Map<Long, CelReference> references = new HashMap<>(ast.getReferenceMap());
        List<CelFunctionBinding> overloads = new ArrayList<>();
        for (CelExpr expression : comparisonCalls(ast)) {
            var call = expression.call();
            ComparisonOp op = OPERATORS.get(call.function());
            LiteralOperand literal = literalOperand(ast, call, true);
            var reference = ast.getReferenceOrThrow(expression.id());
            List<String> candidates = reference.overloadIds();
            if (literal == null && candidates.stream().noneMatch(id -> id.contains("_decimal"))) {
                continue;
            }
            String id = "tapstate_decimal_comparison_" + expression.id();
            overloads.add(CelFunctionBinding.from(id, Object.class, Object.class,
                    (Object left, Object right) -> compareOperands(left, right, literal, op, candidates)));
            var replacement = CelReference.newBuilder().setName(reference.name()).addOverloadIds(id);
            if (literal == null || !op.equality()) {
                // Standard CEL dispatch runs before the late bindings. Only equality with a
                // literal needs to replace the built-in catch-all and provide its own fallback.
                replacement.addOverloadIds(candidates);
            }
            references.put(expression.id(), replacement.build());
        }
        CelAbstractSyntaxTree runtimeAst = overloads.isEmpty() ? ast
                : CelAbstractSyntaxTree.newCheckedAst(ast.getExpr(), ast.getSource(), references, ast.getTypeMap());
        return new RuntimeExpression(runtimeAst, CelLateFunctionBindings.from(overloads));
    }

    static String validationError(CelAbstractSyntaxTree ast) {
        for (CelExpr expression : comparisonCalls(ast)) {
            var call = expression.call();
            if (!OPERATORS.get(call.function()).equality()
                    && call.args().stream().anyMatch(arg -> SimpleType.NULL_TYPE.equals(
                            ast.getType(arg.id()).orElse(SimpleType.DYN)))) {
                return "an ordering comparison cannot have a statically null operand";
            }
            LiteralOperand literal = literalOperand(ast, call, false);
            if (literal != null && literal.value() == null) {
                return EXACT_LITERAL_RANGE;
            }
        }
        return null;
    }

    private static List<CelExpr> comparisonCalls(CelAbstractSyntaxTree ast) {
        return CelNavigableAst.fromAst(ast).getRoot().allNodes()
                .map(CelNavigableExpr::expr)
                .filter(expr -> expr.getKind() == CelExpr.ExprKind.Kind.CALL)
                .filter(expr -> expr.call().args().size() == 2 && OPERATORS.containsKey(expr.call().function()))
                .toList();
    }

    private record LiteralOperand(int index, BigDecimal value) {
    }

    private static LiteralOperand literalOperand(CelAbstractSyntaxTree ast, CelExpr.CelCall call,
            boolean includeDynamic) {
        for (int i = 0; i < 2; i++) {
            CelType type = ast.getType(call.args().get(1 - i).id()).orElse(SimpleType.DYN);
            if (type.equals(DECIMAL_TYPE) || (includeDynamic && type.equals(SimpleType.DYN))) {
                FloatingConstant literal = floatingConstant(ast, call.args().get(i));
                if (literal != null) return new LiteralOperand(i, literal.exact());
            }
        }
        return null;
    }

    /** A null exact value denotes a CEL literal outside BigDecimal's exponent range. */
    record FloatingConstant(double binary, BigDecimal exact) {}

    static FloatingConstant floatingConstant(CelAbstractSyntaxTree ast, CelExpr expr) {
        if (expr.getKind() == CelExpr.ExprKind.Kind.CALL
                && expr.call().function().equals(Operator.NEGATE.getFunction())) {
            FloatingConstant operand = floatingConstant(ast, expr.call().args().getFirst());
            if (operand == null) return null;
            return new FloatingConstant(-operand.binary(), operand.exact() == null ? null : operand.exact().negate());
        }
        if (expr.getKind() != CelExpr.ExprKind.Kind.CONSTANT
                || expr.constant().getKind() != CelConstant.Kind.DOUBLE_VALUE) {
            return null;
        }
        int position = ast.getSource().getPositionsMap().get(expr.id());
        var content = ast.getSource().getContent();
        int end = position;
        while (end < content.size() && isFloatTokenCharacter(content.get(end))) end++;
        var matcher = FLOAT_TOKEN.matcher(content.slice(position, end).toString());
        if (!matcher.lookingAt()) {
            throw new IllegalStateException("CEL double literal has no matching source token");
        }
        double binary = expr.constant().doubleValue();
        try {
            BigDecimal value = new BigDecimal(matcher.group());
            return new FloatingConstant(binary, Double.doubleToRawLongBits(binary) < 0 ? value.negate() : value);
        } catch (NumberFormatException outsideDecimalRange) {
            return new FloatingConstant(binary, null);
        }
    }

    private static boolean isFloatTokenCharacter(int codePoint) {
        return codePoint >= '0' && codePoint <= '9' || codePoint == '.' || codePoint == 'e'
                || codePoint == 'E' || codePoint == '+' || codePoint == '-';
    }

    private static boolean compareOperands(Object left, Object right, LiteralOperand literal,
            ComparisonOp op, List<String> candidates) {
        boolean literalEquality = literal != null && op.equality();
        if (literalEquality && !isDecimal(left) && !isDecimal(right)) {
            return (op == ComparisonOp.EQ) == objectEquals(left, right);
        }
        if (!isNumeric(left) || !isNumeric(right)
                || (!literalEquality && !matchesDecimalOverload(op, candidates, left, right))) {
            throw new IllegalArgumentException("No matching decimal comparison operands");
        }
        if (literal == null) return compare((Number) left, (Number) right, op);
        if (literal.value() == null) {
            if (isDecimal(left) || isDecimal(right)) throw new IllegalArgumentException(EXACT_LITERAL_RANGE);
            return compare((Number) left, (Number) right, op);
        }
        return literal.index() == 0 ? compare(literal.value(), (Number) right, op)
                : compare((Number) left, literal.value(), op);
    }

    private static boolean matchesDecimalOverload(ComparisonOp op, List<String> candidates, Object left, Object right) {
        String a = numericKind(left);
        String b = numericKind(right);
        if (candidates.contains(op.prefix + "_" + a + "_" + b)) return true;
        // A Decimal128 special value travels as a portable Double. Use it as a decimal only
        // if ordinary CEL dispatch did not match and the checker admitted its decimal position.
        boolean specialLeft = isSpecialDouble(left);
        boolean specialRight = isSpecialDouble(right);
        return (specialLeft && candidates.contains(op.prefix + "_decimal_" + b))
                || (specialRight && candidates.contains(op.prefix + "_" + a + "_decimal"))
                || (specialLeft && specialRight && candidates.contains(op.prefix + SUFFIX_DECIMAL_DECIMAL));
    }

    private static String numericKind(Object value) {
        if (isDecimal(value)) return "decimal";
        if (value instanceof UnsignedLong) return "uint64";
        return value instanceof Double || value instanceof Float ? "double" : "int64";
    }

    private static boolean isDecimal(Object value) {
        return value instanceof BigDecimal || value instanceof BigInteger || value instanceof DecimalSpecialValue;
    }

    private static boolean isSpecialDouble(Object value) {
        return value instanceof Double number
                && (!Double.isFinite(number) || Double.doubleToRawLongBits(number) == Long.MIN_VALUE);
    }

    private static boolean isNumeric(Object value) {
        return isDecimal(value) || value instanceof Double || value instanceof Float
                || value instanceof UnsignedLong || value instanceof Long || value instanceof Integer
                || value instanceof Short || value instanceof Byte;
    }

    private static boolean objectEquals(Object x, Object y) {
        if (isDecimal(x) || isDecimal(y)) {
            return isNumeric(x) && isNumeric(y) && compare((Number) x, (Number) y, ComparisonOp.EQ);
        }
        if (x == y) return true;
        if (x instanceof List<?> left && y instanceof List<?> right) {
            var other = right.iterator();
            return left.size() == right.size() && every(left.iterator(), value -> objectEquals(value, other.next()));
        }
        if (x instanceof Map<?, ?> left && y instanceof Map<?, ?> right) {
            return left.size() == right.size() && every(left.entrySet().iterator(), entry ->
                    DEFAULT_EQUALITY.findInMap(right, entry.getKey())
                            .filter(value -> objectEquals(entry.getValue(), value)).isPresent());
        }
        return DEFAULT_EQUALITY.objectEquals(x, y);
    }

    // Match CEL's aggregate equality: a false element overrides an error in an earlier element.
    private static <T> boolean every(Iterator<T> values, Predicate<T> equals) {
        IllegalArgumentException error = null;
        while (values.hasNext()) {
            try {
                if (!equals.test(values.next())) return false;
            } catch (IllegalArgumentException e) {
                error = e;
            }
        }
        if (error != null) throw error;
        return true;
    }

    private static boolean compare(Number a, Number b, ComparisonOp op) {
        if (isNan(a) || isNan(b)) return op == ComparisonOp.NE;
        int infiniteA = infinity(a);
        int infiniteB = infinity(b);
        int result = infiniteA != 0 || infiniteB != 0
                ? Integer.compare(infiniteA, infiniteB) : finiteDecimal(a).compareTo(finiteDecimal(b));
        return evaluateResult(result, op);
    }

    private static boolean isNan(Number value) {
        return isFloating(value) && Double.isNaN(value.doubleValue());
    }

    private static int infinity(Number value) {
        if (!isFloating(value)) return 0;
        double number = value.doubleValue();
        if (number == Double.POSITIVE_INFINITY) return 1;
        return number == Double.NEGATIVE_INFINITY ? -1 : 0;
    }

    private static boolean isFloating(Number value) {
        return value instanceof Double || value instanceof Float || value instanceof DecimalSpecialValue;
    }

    @SuppressWarnings("java:S2111")
    private static BigDecimal finiteDecimal(Number value) {
        if (isFloating(value)) {
            // BigDecimal(double) captures the exact binary magnitude needed for row comparisons.
            return new BigDecimal(value.doubleValue());
        }
        return switch (value) {
            case BigDecimal decimal -> decimal;
            case BigInteger integer -> new BigDecimal(integer);
            case UnsignedLong unsigned -> new BigDecimal(unsigned.bigIntegerValue());
            default -> BigDecimal.valueOf(value.longValue());
        };
    }

    private static boolean evaluateResult(int cmp, ComparisonOp op) {
        return switch (op) {
            case LT -> cmp < 0;
            case LE -> cmp <= 0;
            case GT -> cmp > 0;
            case GE -> cmp >= 0;
            case EQ -> cmp == 0;
            case NE -> cmp != 0;
        };
    }
}
