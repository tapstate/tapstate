package io.tapstate.core.dsl;

import com.google.common.collect.ImmutableList;
import com.google.common.primitives.UnsignedLong;
import dev.cel.common.CelFunctionDecl;
import dev.cel.common.CelOptions;
import dev.cel.common.CelOverloadDecl;
import dev.cel.common.types.CelType;
import dev.cel.common.types.OpaqueType;
import dev.cel.common.types.SimpleType;
import dev.cel.parser.Operator;
import dev.cel.runtime.CelFunctionBinding;
import dev.cel.runtime.CelRuntimeBuilder;
import dev.cel.runtime.CelStandardFunctions;
import dev.cel.runtime.RuntimeEquality;
import dev.cel.runtime.RuntimeHelpers;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

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
        LT, LE, GT, GE, EQ, NE
    }

    private static final RuntimeEquality DEFAULT_EQUALITY =
            RuntimeEquality.create(RuntimeHelpers.create(), CelOptions.DEFAULT);

    private static final ImmutableList<CelFunctionDecl> ORDERING_DECLARATIONS = buildOrderingDeclarations();
    private static final ImmutableList<CelFunctionDecl> CROSS_TYPE_EQUALITY_DECLARATIONS = buildCrossTypeEqualityDeclarations();
    private static final ImmutableList<CelFunctionDecl> ALL_DECLARATIONS = ImmutableList.<CelFunctionDecl>builder()
            .addAll(ORDERING_DECLARATIONS)
            .addAll(CROSS_TYPE_EQUALITY_DECLARATIONS)
            .build();

    private static final ImmutableList<CelFunctionBinding> BINDINGS = buildBindings();

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

    /** Runtime bindings for all decimal comparisons. */
    public static ImmutableList<CelFunctionBinding> bindings() {
        return BINDINGS;
    }

    /**
     * Configures the CEL runtime builder with decimal comparison bindings and exact equality.
     */
    public static CelRuntimeBuilder configureRuntime(CelRuntimeBuilder builder) {
        return builder
                .setStandardEnvironmentEnabled(false)
                .setStandardFunctions(CelStandardFunctions.newBuilder()
                        .excludeFunctions(
                                CelStandardFunctions.StandardFunction.EQUALS,
                                CelStandardFunctions.StandardFunction.NOT_EQUALS)
                        .build())
                .addFunctionBindings(BINDINGS);
    }

    private static ImmutableList<CelFunctionDecl> buildOrderingDeclarations() {
        List<CelFunctionDecl> decls = new ArrayList<>();
        decls.add(buildOrderingDecl(Operator.LESS, "less"));
        decls.add(buildOrderingDecl(Operator.LESS_EQUALS, "less_equals"));
        decls.add(buildOrderingDecl(Operator.GREATER, "greater"));
        decls.add(buildOrderingDecl(Operator.GREATER_EQUALS, "greater_equals"));
        return ImmutableList.copyOf(decls);
    }

    private static ImmutableList<CelFunctionDecl> buildCrossTypeEqualityDeclarations() {
        List<CelFunctionDecl> decls = new ArrayList<>();
        decls.add(buildCrossTypeEqualityDecl(Operator.EQUALS, OP_EQUALS));
        decls.add(buildCrossTypeEqualityDecl(Operator.NOT_EQUALS, OP_NOT_EQUALS));
        return ImmutableList.copyOf(decls);
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

    private static ImmutableList<CelFunctionBinding> buildBindings() {
        List<CelFunctionBinding> bindings = new ArrayList<>();
        addOrderingBindings(bindings, "less", ComparisonOp.LT);
        addOrderingBindings(bindings, "less_equals", ComparisonOp.LE);
        addOrderingBindings(bindings, "greater", ComparisonOp.GT);
        addOrderingBindings(bindings, "greater_equals", ComparisonOp.GE);

        // Standard equals / not_equals overload replacement to handle exact equality:
        bindings.add(CelFunctionBinding.from(OP_EQUALS, Object.class, Object.class, DecimalComparisonFunctions::objectEquals));
        bindings.add(CelFunctionBinding.from(OP_NOT_EQUALS, Object.class, Object.class,
                (Object x, Object y) -> !objectEquals(x, y)));

        // Concrete cross-type equality bindings matching typed check overloads:
        addCrossTypeBindings(bindings, OP_EQUALS, ComparisonOp.EQ);
        addCrossTypeBindings(bindings, OP_NOT_EQUALS, ComparisonOp.NE);

        return ImmutableList.copyOf(bindings);
    }

    private static boolean objectEquals(Object x, Object y) {
        if (x instanceof BigDecimal a) {
            return decimalEquals(a, y);
        }
        if (y instanceof BigDecimal b) {
            return decimalEquals(b, x);
        }
        return DEFAULT_EQUALITY.objectEquals(x, y);
    }

    private static boolean decimalEquals(BigDecimal dec, Object other) {
        if (other instanceof BigDecimal b) {
            return dec.compareTo(b) == 0;
        }
        if (other instanceof Long b) {
            return dec.compareTo(BigDecimal.valueOf(b)) == 0;
        }
        if (other instanceof Integer b) {
            return dec.compareTo(BigDecimal.valueOf(b)) == 0;
        }
        if (other instanceof Double b) {
            return compareDecimalDouble(dec, b, ComparisonOp.EQ);
        }
        if (other instanceof Float b) {
            return compareDecimalDouble(dec, b.doubleValue(), ComparisonOp.EQ);
        }
        if (other instanceof UnsignedLong b) {
            return compareDecimalUint64(dec, b, ComparisonOp.EQ);
        }
        return DEFAULT_EQUALITY.objectEquals(dec, other);
    }

    private static void addOrderingBindings(List<CelFunctionBinding> bindings, String prefix, ComparisonOp op) {
        bindings.add(CelFunctionBinding.from(
                prefix + SUFFIX_DECIMAL_DECIMAL, BigDecimal.class, BigDecimal.class,
                (BigDecimal x, BigDecimal y) -> compareDecimalDecimal(x, y, op)));

        addCrossTypeBindings(bindings, prefix, op);
    }

    private static void addCrossTypeBindings(List<CelFunctionBinding> bindings, String prefix, ComparisonOp op) {
        bindings.add(CelFunctionBinding.from(
                prefix + SUFFIX_DECIMAL_INT64, BigDecimal.class, Long.class,
                (BigDecimal x, Long y) -> compareDecimalInt64(x, y, op)));

        bindings.add(CelFunctionBinding.from(
                prefix + SUFFIX_INT64_DECIMAL, Long.class, BigDecimal.class,
                (Long x, BigDecimal y) -> compareInt64Decimal(x, y, op)));

        bindings.add(CelFunctionBinding.from(
                prefix + SUFFIX_DECIMAL_UINT64, BigDecimal.class, UnsignedLong.class,
                (BigDecimal x, UnsignedLong y) -> compareDecimalUint64(x, y, op)));

        bindings.add(CelFunctionBinding.from(
                prefix + SUFFIX_UINT64_DECIMAL, UnsignedLong.class, BigDecimal.class,
                (UnsignedLong x, BigDecimal y) -> compareUint64Decimal(x, y, op)));

        bindings.add(CelFunctionBinding.from(
                prefix + SUFFIX_DECIMAL_DOUBLE, BigDecimal.class, Double.class,
                (BigDecimal x, Double y) -> compareDecimalDouble(x, y, op)));

        bindings.add(CelFunctionBinding.from(
                prefix + SUFFIX_DOUBLE_DECIMAL, Double.class, BigDecimal.class,
                (Double x, BigDecimal y) -> compareDoubleDecimal(x, y, op)));
    }

    private static boolean compareDecimalDecimal(BigDecimal a, BigDecimal b, ComparisonOp op) {
        int cmp = a.compareTo(b);
        return evaluateResult(cmp, op);
    }

    private static boolean compareDecimalInt64(BigDecimal a, Long b, ComparisonOp op) {
        int cmp = a.compareTo(BigDecimal.valueOf(b));
        return evaluateResult(cmp, op);
    }

    private static boolean compareInt64Decimal(Long a, BigDecimal b, ComparisonOp op) {
        int cmp = BigDecimal.valueOf(a).compareTo(b);
        return evaluateResult(cmp, op);
    }

    private static boolean compareDecimalUint64(BigDecimal a, UnsignedLong b, ComparisonOp op) {
        int cmp = a.compareTo(new BigDecimal(b.bigIntegerValue()));
        return evaluateResult(cmp, op);
    }

    private static boolean compareUint64Decimal(UnsignedLong a, BigDecimal b, ComparisonOp op) {
        int cmp = new BigDecimal(a.bigIntegerValue()).compareTo(b);
        return evaluateResult(cmp, op);
    }

    @SuppressWarnings("java:S2111")
    private static boolean compareDecimalDouble(BigDecimal decimal, Double d, ComparisonOp op) {
        if (d.isNaN()) {
            return op == ComparisonOp.NE;
        }
        if (Double.isInfinite(d)) {
            if (d > 0) {
                return switch (op) {
                    case LT, LE, NE -> true;
                    case GT, GE, EQ -> false;
                };
            } else {
                return switch (op) {
                    case GT, GE, NE -> true;
                    case LT, LE, EQ -> false;
                };
            }
        }
        int cmp = decimal.compareTo(new BigDecimal(d));
        return evaluateResult(cmp, op);
    }

    @SuppressWarnings("java:S2111")
    private static boolean compareDoubleDecimal(Double d, BigDecimal decimal, ComparisonOp op) {
        if (d.isNaN()) {
            return op == ComparisonOp.NE;
        }
        if (Double.isInfinite(d)) {
            if (d > 0) {
                return switch (op) {
                    case GT, GE, NE -> true;
                    case LT, LE, EQ -> false;
                };
            } else {
                return switch (op) {
                    case LT, LE, NE -> true;
                    case GT, GE, EQ -> false;
                };
            }
        }
        int cmp = new BigDecimal(d).compareTo(decimal);
        return evaluateResult(cmp, op);
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
