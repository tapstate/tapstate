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
        decls.add(buildCrossTypeEqualityDecl(Operator.EQUALS, "equals"));
        decls.add(buildCrossTypeEqualityDecl(Operator.NOT_EQUALS, "not_equals"));
        return ImmutableList.copyOf(decls);
    }

    private static CelFunctionDecl buildOrderingDecl(Operator operator, String prefix) {
        return CelFunctionDecl.newFunctionDeclaration(
                operator.getFunction(),
                CelOverloadDecl.newGlobalOverload(prefix + "_decimal_decimal", SimpleType.BOOL, DECIMAL_TYPE, DECIMAL_TYPE),
                CelOverloadDecl.newGlobalOverload(prefix + "_decimal_int64", SimpleType.BOOL, DECIMAL_TYPE, SimpleType.INT),
                CelOverloadDecl.newGlobalOverload(prefix + "_int64_decimal", SimpleType.BOOL, SimpleType.INT, DECIMAL_TYPE),
                CelOverloadDecl.newGlobalOverload(prefix + "_decimal_uint64", SimpleType.BOOL, DECIMAL_TYPE, SimpleType.UINT),
                CelOverloadDecl.newGlobalOverload(prefix + "_uint64_decimal", SimpleType.BOOL, SimpleType.UINT, DECIMAL_TYPE),
                CelOverloadDecl.newGlobalOverload(prefix + "_decimal_double", SimpleType.BOOL, DECIMAL_TYPE, SimpleType.DOUBLE),
                CelOverloadDecl.newGlobalOverload(prefix + "_double_decimal", SimpleType.BOOL, SimpleType.DOUBLE, DECIMAL_TYPE)
        );
    }

    private static CelFunctionDecl buildCrossTypeEqualityDecl(Operator operator, String prefix) {
        return CelFunctionDecl.newFunctionDeclaration(
                operator.getFunction(),
                CelOverloadDecl.newGlobalOverload(prefix + "_decimal_int64", SimpleType.BOOL, DECIMAL_TYPE, SimpleType.INT),
                CelOverloadDecl.newGlobalOverload(prefix + "_int64_decimal", SimpleType.BOOL, SimpleType.INT, DECIMAL_TYPE),
                CelOverloadDecl.newGlobalOverload(prefix + "_decimal_uint64", SimpleType.BOOL, DECIMAL_TYPE, SimpleType.UINT),
                CelOverloadDecl.newGlobalOverload(prefix + "_uint64_decimal", SimpleType.BOOL, SimpleType.UINT, DECIMAL_TYPE),
                CelOverloadDecl.newGlobalOverload(prefix + "_decimal_double", SimpleType.BOOL, DECIMAL_TYPE, SimpleType.DOUBLE),
                CelOverloadDecl.newGlobalOverload(prefix + "_double_decimal", SimpleType.BOOL, SimpleType.DOUBLE, DECIMAL_TYPE)
        );
    }

    private static ImmutableList<CelFunctionBinding> buildBindings() {
        List<CelFunctionBinding> bindings = new ArrayList<>();
        addOrderingBindings(bindings, "less", ComparisonOp.LT);
        addOrderingBindings(bindings, "less_equals", ComparisonOp.LE);
        addOrderingBindings(bindings, "greater", ComparisonOp.GT);
        addOrderingBindings(bindings, "greater_equals", ComparisonOp.GE);

        // Standard equals / not_equals overload replacement to handle exact equality:
        bindings.add(CelFunctionBinding.from("equals", Object.class, Object.class, DecimalComparisonFunctions::objectEquals));
        bindings.add(CelFunctionBinding.from("not_equals", Object.class, Object.class,
                (Object x, Object y) -> !objectEquals(x, y)));

        // Concrete cross-type equality bindings matching typed check overloads:
        addCrossTypeBindings(bindings, "equals", ComparisonOp.EQ);
        addCrossTypeBindings(bindings, "not_equals", ComparisonOp.NE);

        return ImmutableList.copyOf(bindings);
    }

    private static boolean objectEquals(Object x, Object y) {
        if (x instanceof BigDecimal a) {
            if (y instanceof BigDecimal b) {
                return a.compareTo(b) == 0;
            }
            if (y instanceof Long b) {
                return a.compareTo(BigDecimal.valueOf(b)) == 0;
            }
            if (y instanceof Integer b) {
                return a.compareTo(BigDecimal.valueOf(b)) == 0;
            }
            if (y instanceof Double b) {
                return compareDecimalDouble(a, b, ComparisonOp.EQ);
            }
            if (y instanceof Float b) {
                return compareDecimalDouble(a, b.doubleValue(), ComparisonOp.EQ);
            }
            if (y instanceof UnsignedLong b) {
                return compareDecimalUint64(a, b, ComparisonOp.EQ);
            }
        } else if (y instanceof BigDecimal b) {
            if (x instanceof Long a) {
                return BigDecimal.valueOf(a).compareTo(b) == 0;
            }
            if (x instanceof Integer a) {
                return BigDecimal.valueOf(a).compareTo(b) == 0;
            }
            if (x instanceof Double a) {
                return compareDoubleDecimal(a, b, ComparisonOp.EQ);
            }
            if (x instanceof Float a) {
                return compareDoubleDecimal(a.doubleValue(), b, ComparisonOp.EQ);
            }
            if (x instanceof UnsignedLong a) {
                return compareUint64Decimal(a, b, ComparisonOp.EQ);
            }
        }
        return DEFAULT_EQUALITY.objectEquals(x, y);
    }

    private static void addOrderingBindings(List<CelFunctionBinding> bindings, String prefix, ComparisonOp op) {
        bindings.add(CelFunctionBinding.from(
                prefix + "_decimal_decimal", BigDecimal.class, BigDecimal.class,
                (BigDecimal x, BigDecimal y) -> compareDecimalDecimal(x, y, op)));

        addCrossTypeBindings(bindings, prefix, op);
    }

    private static void addCrossTypeBindings(List<CelFunctionBinding> bindings, String prefix, ComparisonOp op) {
        bindings.add(CelFunctionBinding.from(
                prefix + "_decimal_int64", BigDecimal.class, Long.class,
                (BigDecimal x, Long y) -> compareDecimalInt64(x, y, op)));

        bindings.add(CelFunctionBinding.from(
                prefix + "_int64_decimal", Long.class, BigDecimal.class,
                (Long x, BigDecimal y) -> compareInt64Decimal(x, y, op)));

        bindings.add(CelFunctionBinding.from(
                prefix + "_decimal_uint64", BigDecimal.class, UnsignedLong.class,
                (BigDecimal x, UnsignedLong y) -> compareDecimalUint64(x, y, op)));

        bindings.add(CelFunctionBinding.from(
                prefix + "_uint64_decimal", UnsignedLong.class, BigDecimal.class,
                (UnsignedLong x, BigDecimal y) -> compareUint64Decimal(x, y, op)));

        bindings.add(CelFunctionBinding.from(
                prefix + "_decimal_double", BigDecimal.class, Double.class,
                (BigDecimal x, Double y) -> compareDecimalDouble(x, y, op)));

        bindings.add(CelFunctionBinding.from(
                prefix + "_double_decimal", Double.class, BigDecimal.class,
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
