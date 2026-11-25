package io.tapstate.core.validation;

import io.tapstate.core.common.TapstateException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import static io.tapstate.core.validation.ValidationPredicate.*;
import static org.assertj.core.api.Assertions.*;

class ValidationArrayPredicateTest {
    private static final ValidationValues.Limits ROW = new ValidationValues.Limits(32768, 512, 32);
    private static final Limits LIMITS = new Limits(ROW, 10000, false);

    @Test void sizeUsesTheCompleteArrayAndDistinguishesEmptyFromNullOrWrongShape() {
        var empty = new Compare(Operator.EQ, new ArraySize(field("items")), new Constant(0L));
        assertThat(evaluate(empty, List.of()).matches()).isTrue();
        assertThat(evaluate(empty, List.of(1L)).matches()).isFalse();
        assertThat(evaluate(empty, null).problem().kind()).isEqualTo(ProblemKind.INCOMPATIBLE_TYPE);
        assertThat(evaluate(empty, "text").problem().kind()).isEqualTo(ProblemKind.INCOMPATIBLE_TYPE);
        assertThat(evaluate(empty, Map.of()).problem().kind()).isEqualTo(ProblemKind.INCOMPATIBLE_TYPE);
    }

    @Test void existsReturnsFalseForEmptyAndFindsMembersWithoutDependingOnOrder() {
        var exists = exists(new Compare(Operator.EQ, member(), new Constant(3L)));
        assertThat(evaluate(exists, List.of()).matches()).isFalse();
        assertThat(evaluate(exists, List.of(1L, 2L)).matches()).isFalse();
        assertThat(evaluate(exists, List.of(1L, 2L, 3L)).matches()).isTrue();
        assertThat(evaluate(exists, List.of(3L, 2L, 1L)).matches()).isTrue();
    }

    @Test void aKnownTrueMemberMayAbsorbDataProblemsButOtherwiseKeepsTheirOrdinal() {
        var exists = exists(new Field(0, List.of("keep")));
        assertThat(evaluate(exists, List.of(Map.of(), Map.of("keep", true))).matches()).isTrue();
        var unresolved = evaluate(exists, List.of(Map.of("keep", false), Map.of()));
        assertThat(unresolved.matches()).isNull();
        assertThat(unresolved.problem()).isEqualTo(new Problem(ProblemKind.MISSING_FIELD, List.of("items", 1, "keep")));
        assertThat(evaluate(exists, Arrays.asList(null, Map.of("keep", true))).matches()).isTrue();
        assertThat(evaluate(exists, Arrays.asList(null, Map.of("keep", false))).problem())
                .isEqualTo(new Problem(ProblemKind.INVALID_PARENT, List.of("items", 0)));
    }

    @Test void nestedBindingsKeepParentScopeAndTheFullActualMemberPath() {
        var nested = new Exists(0, field("items"), new Exists(1, new Field(0, List.of("children")),
                new And(new Field(0, List.of("enabled")), new Field(1, List.of("keep")))));
        var input = List.of(Map.of("enabled", true, "children", List.of(Map.of("keep", false), Map.of())));
        var result = evaluate(nested, input);
        assertThat(result.matches()).isNull();
        assertThat(result.problem()).isEqualTo(new Problem(ProblemKind.MISSING_FIELD, List.of("items", 0, "children", 1, "keep")));
        assertThat(evaluate(nested, List.of(Map.of("enabled", true, "children", List.of(Map.of("keep", true))))).matches()).isTrue();
    }

    @Test void hasAndNullChecksWorkOnTheOriginalMemberWithoutReindexing() {
        var expr = exists(new And(new Present(0, List.of("value")), new Compare(Operator.EQ, new Field(0, List.of("value")), new Constant(null))));
        var nullable = new LinkedHashMap<String, Object>(); nullable.put("value", null);
        assertThat(evaluate(expr, List.of(Map.of(), nullable)).matches()).isTrue();
        assertThat(evaluate(exists(new Compare(Operator.EQ, member(), new Constant(null))), Arrays.asList(1L, null)).matches()).isTrue();
    }

    @Test void collectionOperationsPreserveTypedDomainsInsteadOfAcceptingAnyWrappedList() {
        var size = new Compare(Operator.EQ, new ArraySize(field("items")), new Constant(2L));
        assertThat(evaluate(size, new ValidationValues.Typed("array", "native-array", "codec", List.of(1, 2))).matches()).isTrue();
        assertThat(evaluate(size, new ValidationValues.Typed("json", "json", "codec", List.of(1, 2))).matches()).isTrue();
        assertThat(evaluate(size, new ValidationValues.Typed("text", "wrong", "codec", List.of(1, 2))).problem().kind())
                .isEqualTo(ProblemKind.INCOMPATIBLE_TYPE);
    }

    @Test void allMembersAndNestedPredicatesShareOneBudgetThatCannotBeAbsorbed() {
        var many = java.util.Collections.nCopies(30, 0L);
        var expression = new Or(exists(new Compare(Operator.EQ, member(), new Constant(1L))), new Constant(true));
        var predicate = compile("filter", expression, new Limits(ROW, 25, false));
        assertThatThrownBy(() -> predicate.evaluate(row(many))).isInstanceOfSatisfying(TapstateException.class, failure -> {
            assertThat(failure.code()).isEqualTo(ValidationError.VALUE_LIMIT_EXCEEDED);
            assertThat(failure.args()).containsEntry("limit", "filter.operations");
        });
        assertThat(compile("filter", new Or(new Constant(true), expression), new Limits(ROW, 2, false)).evaluate(row(many)).matches()).isTrue();
    }

    @Test void unboundOrShadowedCoreSlotsAreRefusedBeforeEvaluation() {
        for (Expr expression : List.of(new Field(0, List.of()),
                new Exists(0, new Field(0, List.of()), new Constant(true)),
                new Exists(0, field("items"), new Exists(0, field("items"), new Constant(true))))) {
            assertThatThrownBy(() -> compile("filter", expression, LIMITS)).isInstanceOfSatisfying(TapstateException.class,
                    failure -> assertThat(failure.code()).isEqualTo(ValidationError.INVALID_CONTRACT));
        }
    }

    private static Field field(String name) { return new Field(List.of(name)); }
    private static Field member() { return new Field(0, List.of()); }
    private static Exists exists(Expr predicate) { return new Exists(0, field("items"), predicate); }
    private static Result evaluate(Expr expression, Object items) { return compile("filter", expression, LIMITS).evaluate(row(items)); }
    private static ValidationValues.Row row(Object items) {
        var fields = new LinkedHashMap<String, Object>(); fields.put("items", items); return ValidationValues.ownRow(fields, ROW);
    }
}
