package io.tapstate.core.validation;

import io.tapstate.core.common.TapstateException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import static io.tapstate.core.validation.ValidationPredicate.*;
import static org.assertj.core.api.Assertions.*;

class ValidationPredicateTest {
    private static final ValidationValues.Limits ROW = new ValidationValues.Limits(32768, 512, 32);
    private static final Limits LIMITS = new Limits(ROW, 10000, false);

    @Test void exactNumbersRetainScaleIndependentEqualityAndEveryRangeOperator() {
        for (Operator operator : Operator.values()) {
            var predicate = compile("filter", new Compare(operator, field("amount"), new Constant(9007199254740992L)), LIMITS);
            boolean expected = operator == Operator.NE || operator == Operator.GT || operator == Operator.GE;
            assertThat(predicate.evaluate(row("amount", new BigDecimal("9007199254740993.000"))).matches()).isEqualTo(expected);
        }
        assertMatches(compare(Operator.EQ, field("n"), new Constant(new BigDecimal("1.00"))), row("n", BigInteger.ONE), true);
        assertMatches(compare(Operator.EQ, field("n"), new Constant(1L)), row("n", new BigDecimal("1.5")), false);
    }

    @Test void floatingComparisonsRequireExplicitCrossDomainCompatibilityAndNeverUseTolerance() {
        var expression = compare(Operator.EQ, field("n"), new Constant(9007199254740992D));
        assertProblem(expression, row("n", 9007199254740993L), ProblemKind.INCOMPATIBLE_TYPE);
        assertThat(compile("filter", expression, new Limits(ROW, 10000, true))
                .evaluate(row("n", 9007199254740993L)).matches()).isFalse();
        assertMatches(compare(Operator.EQ, field("n"), new Constant(1D)), row("n", Math.nextUp(1D)), false);
        assertMatches(compare(Operator.EQ, field("n"), new Constant(0D)), row("n", -0D), true);
        assertMatches(compare(Operator.EQ, field("n"), new Constant(Double.NaN)), row("n", Double.NaN), true);
        assertProblem(compare(Operator.LT, field("n"), new Constant(1D)), row("n", Double.NaN), ProblemKind.UNSUPPORTED_VALUE);
        assertMatches(compare(Operator.GT, field("n"), new Constant(1D)), row("n", Double.POSITIVE_INFINITY), true);
    }

    @Test void textUsesExactCodePointOrderingAndBooleanOnlyEquality() {
        assertMatches(compare(Operator.EQ, field("text"), new Constant("001")), row("text", " 001 "), false);
        String supplemental = new String(Character.toChars(0x10000));
        assertMatches(compare(Operator.GT, field("text"), new Constant(String.valueOf((char)0xE000))), row("text", supplemental), true);
        assertMatches(compare(Operator.NE, field("flag"), new Constant(true)), row("flag", false), true);
        assertProblem(compare(Operator.LT, field("flag"), new Constant(true)), row("flag", false), ProblemKind.INCOMPATIBLE_TYPE);
        assertProblem(compare(Operator.EQ, field("text"), new Constant(1L)), row("text", "1"), ProblemKind.INCOMPATIBLE_TYPE);
    }

    @Test void unpairedSurrogatesCannotCreateAStringRangeDecision() {
        assertProblem(compare(Operator.GT, field("text"), new Constant("a")),
                row("text", String.valueOf((char)0xD800)), ProblemKind.UNSUPPORTED_VALUE);
    }

    @Test void presenceDistinguishesMissingNullAndInvalidParentPaths() {
        assertMatches(new Present(List.of("x")), row("x", null), true);
        assertMatches(new Present(List.of("x")), row("other", 1L), false);
        assertProblem(new Present(List.of("parent", "x")), row("other", 1L), ProblemKind.MISSING_FIELD);
        assertProblem(new Present(List.of("parent", "x")), row("parent", null), ProblemKind.INVALID_PARENT);
        assertMatches(new Present(List.of("parent", "x")), row("parent", Map.of()), false);
        assertMatches(compare(Operator.EQ, field("x"), new Constant(null)), row("x", null), true);
        assertMatches(compare(Operator.NE, field("x"), new Constant(null)), row("x", 1L), true);
        assertProblem(compare(Operator.GE, field("x"), new Constant(0L)), row("x", null), ProblemKind.INCOMPATIBLE_TYPE);
    }

    @Test void dataProblemsAreAbsorbedOnlyWhenAnotherBooleanProvesTheResult() {
        Expr missing = compare(Operator.EQ, field("missing"), new Constant(1L));
        for (boolean problemFirst : List.of(false, true)) {
            assertMatches(problemFirst ? new And(missing, new Constant(false)) : new And(new Constant(false), missing), row("x", 1L), false);
            assertMatches(problemFirst ? new Or(missing, new Constant(true)) : new Or(new Constant(true), missing), row("x", 1L), true);
            assertProblem(problemFirst ? new And(missing, new Constant(true)) : new And(new Constant(true), missing), row("x", 1L), ProblemKind.MISSING_FIELD);
            assertProblem(problemFirst ? new Or(missing, new Constant(false)) : new Or(new Constant(false), missing), row("x", 1L), ProblemKind.MISSING_FIELD);
        }
        assertProblem(new Not(missing), row("x", 1L), ProblemKind.MISSING_FIELD);
        assertProblem(field("x"), row("x", 1L), ProblemKind.NOT_BOOLEAN);
    }

    @Test void executionBudgetErrorsAreNeverAbsorbedAsBooleanDataProblems() {
        var predicate = compile("filter", new Or(compare(Operator.EQ, field("x"), new Constant(1L)), new Constant(true)), new Limits(ROW, 2, false));
        assertThatThrownBy(() -> predicate.evaluate(row("x", 1L))).isInstanceOfSatisfying(TapstateException.class, failure -> {
            assertThat(failure.code()).isEqualTo(ValidationError.VALUE_LIMIT_EXCEEDED);
            assertThat(failure.args()).containsEntry("limit", "filter.operations");
        });
    }

    @Test void ownedTypesAreCheckedWithoutUsingNativeNamesOrCodecIdentityForEquality() {
        var number = new ValidationValues.Typed("exact-number", "decimal", "left", "custom", new BigDecimal("10.00"));
        assertMatches(compare(Operator.GE, field("amount"), new Constant(10L)), row("amount", number), true);
        var customer = new ValidationValues.Typed("object", "document", "left", "custom", Map.of("vip", true));
        assertMatches(new Field(List.of("customer", "vip")), row("customer", customer), true);
        var wrong = new ValidationValues.Typed("exact-number", "bad", "left", "custom", "10");
        assertProblem(compare(Operator.EQ, field("n"), new Constant(10L)), row("n", wrong), ProblemKind.INCOMPATIBLE_TYPE);
        assertProblem(compare(Operator.EQ, field("n"), new Constant(1L)), row("n", List.of(1L)), ProblemKind.UNSUPPORTED_VALUE);
    }

    @Test void fieldProblemsKeepTheActualNestedPathAndDoNotMutateRows() {
        var input = row("customer", Map.of("vip", "yes"));
        var result = compile("filter", new Field(List.of("customer", "vip")), LIMITS).evaluate(input);
        assertThat(result.matches()).isNull();
        assertThat(result.problem()).isEqualTo(new Problem(ProblemKind.NOT_BOOLEAN, List.of("customer", "vip")));
        assertThat(input.fields()).containsExactly(entry("customer", Map.of("vip", "yes")));
    }

    @Test void programAndInputLimitsAreCheckedSeparatelyFromBooleanEvaluation() {
        Expr deep = new Constant(true);
        for (int i = 0; i < 33; i++) deep = new Not(deep);
        Expr expression = deep;
        assertThatThrownBy(() -> compile("filter", expression, LIMITS)).isInstanceOf(TapstateException.class);
        assertThatThrownBy(() -> compile("filter", new Constant(Map.of("x", 1)), LIMITS))
                .isInstanceOfSatisfying(TapstateException.class, failure -> assertThat(failure.code()).isEqualTo(ValidationError.INVALID_CONTRACT));
        var small = new ValidationValues.Limits(800, 50, 8);
        var predicate = compile("filter", new Constant(true), new Limits(small, 10, false));
        assertThatThrownBy(() -> predicate.evaluate(row("large", "x".repeat(600))))
                .isInstanceOfSatisfying(TapstateException.class, failure -> assertThat(failure.args()).containsEntry("limit", "filter.input"));
    }

    private static Compare compare(Operator operator, Expr left, Expr right) { return new Compare(operator, left, right); }
    private static Field field(String field) { return new Field(List.of(field)); }
    private static ValidationValues.Row row(String field, Object value) {
        var map = new LinkedHashMap<String, Object>(); map.put(field, value); return ValidationValues.ownRow(map, ROW);
    }
    private static void assertMatches(Expr expr, ValidationValues.Row row, boolean expected) {
        assertThat(compile("filter", expr, LIMITS).evaluate(row)).isEqualTo(new Result(expected, null));
    }
    private static void assertProblem(Expr expr, ValidationValues.Row row, ProblemKind expected) {
        var result = compile("filter", expr, LIMITS).evaluate(row);
        assertThat(result.matches()).isNull(); assertThat(result.problem().kind()).isEqualTo(expected);
    }
}
