package io.tapstate.core.dsl;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.validation.ValidationError;
import io.tapstate.core.validation.ValidationPredicate;
import io.tapstate.core.validation.ValidationValues;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class ValidationPredicateCompilerTest {
    private static final ValidationValues.Limits ROW = new ValidationValues.Limits(32768, 512, 32);
    private static final ValidationPredicate.Limits LIMITS = new ValidationPredicate.Limits(ROW, 10000, false);

    @Test void compilesCurrentRowFinitePredicatesWithoutAnExpressionRuntime() {
        var predicate = compile("has(after.amount) && after.amount >= 10 && !(after.tag != 'checked')");
        assertThat(predicate.evaluate(row(Map.of("amount", 10L, "tag", "checked"))).matches()).isTrue();
        assertThat(predicate.evaluate(row(Map.of("amount", 9L, "tag", "checked"))).matches()).isFalse();
        assertThat(predicate.evaluate(row(Map.of("tag", "checked"))).matches()).isFalse();
        assertThat(compile("after.customer.vip || after.enabled").evaluate(row(Map.of("customer", Map.of("vip", false), "enabled", true))).matches()).isTrue();
    }

    @Test void hasKeepsNullPresentAndNestedMissingParentsRemainProblems() {
        var values = new LinkedHashMap<String, Object>(); values.put("x", null);
        assertThat(compile("has(after.x) && after.x == null").evaluate(row(values)).matches()).isTrue();
        assertThat(compile("has(after.customer.vip)").evaluate(row(Map.of())).problem().kind())
                .isEqualTo(ValidationPredicate.ProblemKind.MISSING_FIELD);
    }

    @Test void checkedSyntaxDoesNotAdmitHistoryIndexFunctionsArithmeticOrCollections() {
        for (String text : List.of("before.x == 1", "op == 'i'", "ts > 0", "has(schema.x)",
                "after.x[0] == 1", "after['x'] == 1", "now() == now()", "after.x + 1 > 2",
                "string(after.x) == '1'", "after.x.startsWith('a')", "after.x ? true : false",
                "after.items.size() == 0",
                "after.items.all(x, x == 1)", "after.items.map(x, x).size() == 0")) {
            assertThatThrownBy(() -> compile(text)).as(text).isInstanceOfSatisfying(TapstateException.class,
                    failure -> assertThat(failure.code()).isEqualTo(ValidationError.RULE_UNSUPPORTED));
        }
    }

    @Test void syntaxAndClassificationErrorsAreCodedAndPointAtTheExpression() {
        assertThatThrownBy(() -> compile("after.x >")).isInstanceOfSatisfying(TapstateException.class, failure -> {
            assertThat(failure.code()).isEqualTo(ValidationError.RULE_UNSUPPORTED);
            assertThat(failure.args()).containsEntry("node", "filter-node").containsEntry("rule", "filter.syntax");
            assertThat(failure.getCause()).isInstanceOf(IllegalArgumentException.class);
        });
        assertThatThrownBy(() -> compile("before.x == 1")).isInstanceOfSatisfying(TapstateException.class,
                failure -> assertThat(failure.args().get("field").toString()).startsWith("expr@"));
    }

    @Test void literalKindsAndMultipleLogicalTermsSurviveClassification() {
        assertThat(compile("after.x == -1 || after.x == 0 || after.x == 1 || after.x == 2")
                .evaluate(row(Map.of("x", 2L))).matches()).isTrue();
        assertThat(compile("after.x == 18446744073709551615u")
                .evaluate(row(Map.of("x", new java.math.BigInteger("18446744073709551615")))).matches()).isTrue();
        assertThat(compile("after.x > 0.5").evaluate(row(Map.of("x", Math.nextUp(0.5D)))).matches()).isTrue();
    }

    @Test void sourceAndAstLimitsStopComplexInputBeforeRuntime() {
        assertThatThrownBy(() -> compile("after.x == '" + "x".repeat(16384) + "'"))
                .isInstanceOfSatisfying(TapstateException.class, failure -> assertThat(failure.args()).containsEntry("limit", "filter.sourceLength"));
        assertThatThrownBy(() -> compile("after" + ".x".repeat(40)))
                .isInstanceOfSatisfying(TapstateException.class, failure -> assertThat(failure.code()).isEqualTo(ValidationError.VALUE_LIMIT_EXCEEDED));
    }

    private static ValidationPredicate compile(String source) { return ValidationPredicateCompiler.compile("filter-node", source, LIMITS); }
    private static ValidationValues.Row row(Map<String, ?> fields) { return ValidationValues.ownRow(fields, ROW); }
}
