package io.tapstate.core.dsl;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.primitives.UnsignedLong;
import dev.cel.common.CelAbstractSyntaxTree;
import dev.cel.common.types.SimpleType;
import dev.cel.compiler.CelCompiler;
import dev.cel.compiler.CelCompilerFactory;
import dev.cel.runtime.CelEvaluationException;
import dev.cel.runtime.CelRuntime;
import dev.cel.runtime.CelRuntimeFactory;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.math.BigDecimal;
import java.util.Map;

class DecimalComparisonFunctionsTest {

    private static final CelCompiler COMPILER = CelCompilerFactory.standardCelCompilerBuilder()
            .addVar("dec1", DecimalComparisonFunctions.DECIMAL_TYPE)
            .addVar("dec2", DecimalComparisonFunctions.DECIMAL_TYPE)
            .addVar("i", SimpleType.INT)
            .addVar("u", SimpleType.UINT)
            .addVar("d", SimpleType.DOUBLE)
            .addFunctionDeclarations(DecimalComparisonFunctions.declarations())
            .setResultType(SimpleType.BOOL)
            .build();

    private static final CelRuntime RUNTIME = DecimalComparisonFunctions.configureRuntime(
            CelRuntimeFactory.standardCelRuntimeBuilder()).build();

    private static boolean eval(String expr, Map<String, Object> vars) {
        try {
            CelAbstractSyntaxTree ast = COMPILER.compile(expr).getAst();
            return (Boolean) RUNTIME.createProgram(ast).eval(vars);
        } catch (Exception e) {
            throw new RuntimeException("failed evaluating: " + expr + " with vars: " + vars, e);
        }
    }

    // ---- Decimal vs Decimal ----------------------------------------------------------------

    @Test
    @DisplayName("decimal vs decimal equality ignores trailing scale zeros")
    void decimalVsDecimalEqualityIgnoresScale() {
        Map<String, Object> vars = Map.of(
                "dec1", new BigDecimal("123.4500"),
                "dec2", new BigDecimal("123.45"));

        assertThat(eval("dec1 == dec2", vars)).isTrue();
        assertThat(eval("dec1 != dec2", vars)).isFalse();
        assertThat(eval("dec1 >= dec2", vars)).isTrue();
        assertThat(eval("dec1 <= dec2", vars)).isTrue();
        assertThat(eval("dec1 > dec2", vars)).isFalse();
        assertThat(eval("dec1 < dec2", vars)).isFalse();
    }

    @Test
    @DisplayName("decimal vs decimal ordering with subtle differences and large precision")
    void decimalVsDecimalOrdering() {
        Map<String, Object> vars = Map.of(
                "dec1", new BigDecimal("10000000000000000000000000000000000000000.00000000000000000001"),
                "dec2", new BigDecimal("10000000000000000000000000000000000000000.00000000000000000002"));

        assertThat(eval("dec1 < dec2", vars)).isTrue();
        assertThat(eval("dec1 <= dec2", vars)).isTrue();
        assertThat(eval("dec1 > dec2", vars)).isFalse();
        assertThat(eval("dec1 >= dec2", vars)).isFalse();
        assertThat(eval("dec1 == dec2", vars)).isFalse();
        assertThat(eval("dec1 != dec2", vars)).isTrue();
    }

    @Test
    @DisplayName("negative decimal comparisons")
    void negativeDecimalComparisons() {
        Map<String, Object> vars = Map.of(
                "dec1", new BigDecimal("-25.50"),
                "dec2", new BigDecimal("-25.40"));

        assertThat(eval("dec1 < dec2", vars)).isTrue();
        assertThat(eval("dec1 > dec2", vars)).isFalse();
        assertThat(eval("dec2 > dec1", vars)).isTrue();
        assertThat(eval("dec1 == dec2", vars)).isFalse();
        assertThat(eval("dec1 != dec2", vars)).isTrue();
    }

    // ---- Decimal vs Integer (both operand orders) -------------------------------------------

    @Test
    @DisplayName("decimal vs int64 at Long.MAX_VALUE and Long.MIN_VALUE boundaries")
    void decimalVsInt64Boundaries() {
        long max = Long.MAX_VALUE;
        long min = Long.MIN_VALUE;

        // Equal to Long.MAX_VALUE
        Map<String, Object> atMax = Map.of(
                "dec1", new BigDecimal(Long.toString(max)),
                "i", max);
        assertThat(eval("dec1 == i", atMax)).isTrue();
        assertThat(eval("i == dec1", atMax)).isTrue();
        assertThat(eval("dec1 >= i", atMax)).isTrue();
        assertThat(eval("i >= dec1", atMax)).isTrue();
        assertThat(eval("dec1 <= i", atMax)).isTrue();
        assertThat(eval("i <= dec1", atMax)).isTrue();
        assertThat(eval("dec1 > i", atMax)).isFalse();
        assertThat(eval("i > dec1", atMax)).isFalse();

        // 1 greater than Long.MAX_VALUE
        Map<String, Object> aboveMax = Map.of(
                "dec1", new BigDecimal(Long.toString(max)).add(BigDecimal.ONE),
                "i", max);
        assertThat(eval("dec1 > i", aboveMax)).isTrue();
        assertThat(eval("i < dec1", aboveMax)).isTrue();
        assertThat(eval("dec1 == i", aboveMax)).isFalse();

        // Equal to Long.MIN_VALUE
        Map<String, Object> atMin = Map.of(
                "dec1", new BigDecimal(Long.toString(min)),
                "i", min);
        assertThat(eval("dec1 == i", atMin)).isTrue();
        assertThat(eval("i == dec1", atMin)).isTrue();

        // 1 less than Long.MIN_VALUE
        Map<String, Object> belowMin = Map.of(
                "dec1", new BigDecimal(Long.toString(min)).subtract(BigDecimal.ONE),
                "i", min);
        assertThat(eval("dec1 < i", belowMin)).isTrue();
        assertThat(eval("i > dec1", belowMin)).isTrue();
        assertThat(eval("dec1 == i", belowMin)).isFalse();
    }

    @ParameterizedTest
    @CsvSource({
            "25.000, 25, true, false, true, true, false, false",
            "25.001, 25, false, true, true, false, true, false",
            "24.999, 25, false, true, false, true, false, true",
            "-10.00, -10, true, false, true, true, false, false",
            "-10.01, -10, false, true, false, true, false, true",
            "-9.99, -10, false, true, true, false, true, false",
            "0.0000, 0, true, false, true, true, false, false"
    })
    void decimalAndIntegerTruthTable(
            String decimalStr, long intVal,
            boolean eq, boolean ne, boolean ge, boolean le, boolean gt, boolean lt) {
        Map<String, Object> vars = Map.of("dec1", new BigDecimal(decimalStr), "i", intVal);

        assertThat(eval("dec1 == i", vars)).isEqualTo(eq);
        assertThat(eval("i == dec1", vars)).isEqualTo(eq);
        assertThat(eval("dec1 != i", vars)).isEqualTo(ne);
        assertThat(eval("i != dec1", vars)).isEqualTo(ne);
        assertThat(eval("dec1 >= i", vars)).isEqualTo(ge);
        assertThat(eval("i <= dec1", vars)).isEqualTo(ge);
        assertThat(eval("dec1 <= i", vars)).isEqualTo(le);
        assertThat(eval("i >= dec1", vars)).isEqualTo(le);
        assertThat(eval("dec1 > i", vars)).isEqualTo(gt);
        assertThat(eval("i < dec1", vars)).isEqualTo(gt);
        assertThat(eval("dec1 < i", vars)).isEqualTo(lt);
        assertThat(eval("i > dec1", vars)).isEqualTo(lt);
    }

    // ---- Decimal vs Uint64 (both operand orders) ---------------------------------------------

    @Test
    @DisplayName("decimal vs uint64 at UnsignedLong boundaries")
    void decimalVsUint64() {
        UnsignedLong max = UnsignedLong.MAX_VALUE;
        Map<String, Object> atMax = Map.of(
                "dec1", new BigDecimal(max.bigIntegerValue()),
                "u", max);

        assertThat(eval("dec1 == u", atMax)).isTrue();
        assertThat(eval("u == dec1", atMax)).isTrue();
        assertThat(eval("dec1 >= u", atMax)).isTrue();
        assertThat(eval("dec1 <= u", atMax)).isTrue();

        Map<String, Object> aboveMax = Map.of(
                "dec1", new BigDecimal(max.bigIntegerValue()).add(BigDecimal.ONE),
                "u", max);
        assertThat(eval("dec1 > u", aboveMax)).isTrue();
        assertThat(eval("u < dec1", aboveMax)).isTrue();
        assertThat(eval("dec1 == u", aboveMax)).isFalse();
    }

    // ---- Decimal vs Double (both operand orders, exact binary float, NaN, Infinities) -------

    @Test
    @DisplayName("decimal vs finite double preserves exact binary values")
    void decimalVsFiniteDouble() {
        double d = 25.5; // Exactly representable in binary floating point
        Map<String, Object> vars = Map.of(
                "dec1", new BigDecimal("25.5"),
                "d", d);

        assertThat(eval("dec1 == d", vars)).isTrue();
        assertThat(eval("d == dec1", vars)).isTrue();
        assertThat(eval("dec1 >= d", vars)).isTrue();
        assertThat(eval("d >= dec1", vars)).isTrue();
        assertThat(eval("dec1 <= d", vars)).isTrue();
        assertThat(eval("d <= dec1", vars)).isTrue();
        assertThat(eval("dec1 > d", vars)).isFalse();
        assertThat(eval("d > dec1", vars)).isFalse();
        assertThat(eval("dec1 < d", vars)).isFalse();
        assertThat(eval("d < dec1", vars)).isFalse();
    }

    @Test
    @DisplayName("decimal vs Double.NaN returns false for ordering and equality, true for !=")
    void decimalVsNan() {
        Map<String, Object> vars = Map.of(
                "dec1", new BigDecimal("42.0"),
                "d", Double.NaN);

        assertThat(eval("dec1 == d", vars)).isFalse();
        assertThat(eval("d == dec1", vars)).isFalse();
        assertThat(eval("dec1 != d", vars)).isTrue();
        assertThat(eval("d != dec1", vars)).isTrue();
        assertThat(eval("dec1 < d", vars)).isFalse();
        assertThat(eval("d < dec1", vars)).isFalse();
        assertThat(eval("dec1 <= d", vars)).isFalse();
        assertThat(eval("d <= dec1", vars)).isFalse();
        assertThat(eval("dec1 > d", vars)).isFalse();
        assertThat(eval("d > dec1", vars)).isFalse();
        assertThat(eval("dec1 >= d", vars)).isFalse();
        assertThat(eval("d >= dec1", vars)).isFalse();
    }

    @Test
    @DisplayName("decimal vs positive and negative infinity")
    void decimalVsInfinities() {
        Map<String, Object> posInf = Map.of("dec1", new BigDecimal("999999999999999999999"), "d", Double.POSITIVE_INFINITY);

        assertThat(eval("dec1 < d", posInf)).isTrue();
        assertThat(eval("dec1 <= d", posInf)).isTrue();
        assertThat(eval("d > dec1", posInf)).isTrue();
        assertThat(eval("d >= dec1", posInf)).isTrue();
        assertThat(eval("dec1 > d", posInf)).isFalse();
        assertThat(eval("d < dec1", posInf)).isFalse();
        assertThat(eval("dec1 == d", posInf)).isFalse();
        assertThat(eval("dec1 != d", posInf)).isTrue();

        Map<String, Object> negInf = Map.of("dec1", new BigDecimal("-999999999999999999999"), "d", Double.NEGATIVE_INFINITY);

        assertThat(eval("dec1 > d", negInf)).isTrue();
        assertThat(eval("dec1 >= d", negInf)).isTrue();
        assertThat(eval("d < dec1", negInf)).isTrue();
        assertThat(eval("d <= dec1", negInf)).isTrue();
        assertThat(eval("dec1 < d", negInf)).isFalse();
        assertThat(eval("d > dec1", negInf)).isFalse();
        assertThat(eval("dec1 == d", negInf)).isFalse();
        assertThat(eval("dec1 != d", negInf)).isTrue();
    }
}
