package io.tapstate.core.validation;

import io.tapstate.core.common.TapstateException;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import static io.tapstate.core.validation.ValidationPredicate.*;
import static org.assertj.core.api.Assertions.*;

class ValidationPredicateUnavailableTest {
    private static final ValidationValues.Limits ROW = new ValidationValues.Limits(32768, 512, 32);
    private static final Limits LIMITS = new Limits(ROW, 10000, false);

    @Test void unresolvedReferencePresenceIsUnknownWhetherItsPartialViewIsAbsentOrContainsAValue() {
        for (var fields : List.of(Map.<String, Object>of(), Map.<String, Object>of("customer", Map.of("vip", true)))) {
            var row = row(fields);
            for (Expr expression : List.of(new Present(List.of("customer")), new Not(new Present(List.of("customer"))),
                    new Field(List.of("customer", "vip")))) {
                assertUnavailable(expression, row, List.of(List.of("customer")), List.of("customer"));
            }
        }
    }

    @Test void knownParentPresenceAndUnrelatedFieldsRemainUsableWithAnUnavailableDescendant() {
        var row = row(Map.of("customer", Map.of("name", "A"), "active", true));
        var unknown = List.<List<Object>>of(List.of("customer", "vip"));
        assertThat(check(new Present(List.of("customer")), row, unknown)).isEqualTo(new Result(true, null));
        assertThat(check(new Field(List.of("active")), row, unknown)).isEqualTo(new Result(true, null));
        assertThat(check(new Compare(Operator.EQ, new Field(List.of("customer", "name")), new Constant("A")), row, unknown))
                .isEqualTo(new Result(true, null));
        assertUnavailable(new Present(List.of("customer", "vip")), row, unknown, List.of("customer", "vip"));
    }

    @Test void unknownArrayMembershipDoesNotBecomeAnEmptyArrayOrAFalseExistential() {
        var row = row(Map.of("items", List.of()));
        var unknown = List.<List<Object>>of(List.of("items"));
        assertUnavailable(new Compare(Operator.EQ, new ArraySize(new Field(List.of("items"))), new Constant(0L)), row, unknown, List.of("items"));
        assertUnavailable(new Exists(0, new Field(List.of("items")), new Constant(true)), row, unknown, List.of("items"));
    }

    @Test void completeArrayMembershipCanCountAndFindKnownWitnessesDespiteAnUnknownMemberField() {
        var row = row(Map.of("items", List.of(Map.of("vip", false), Map.of(), Map.of("vip", true))));
        var unknown = List.<List<Object>>of(List.of("items", 1, "vip"));
        assertThat(check(new Compare(Operator.EQ, new ArraySize(new Field(List.of("items"))), new Constant(3L)), row, unknown))
                .isEqualTo(new Result(true, null));
        var exists = new Exists(0, new Field(List.of("items")), new Field(0, List.of("vip")));
        assertThat(check(exists, row, unknown)).isEqualTo(new Result(true, null));
        var noWitness = row(Map.of("items", List.of(Map.of("vip", false), Map.of(), Map.of("vip", false))));
        assertUnavailable(exists, noWitness, unknown, List.of("items", 1, "vip"));
        assertThat(check(exists, row(Map.of("items", List.of(Map.of("vip", false), Map.of("vip", false), Map.of("vip", false)))), List.of()))
                .isEqualTo(new Result(false, null));
    }

    @Test void nestedExistentialsPreserveActualAncestorOrdinalsAndSiblingPaths() {
        var row = row(Map.of("parents", List.of(Map.of("children", List.of(Map.of("ok", false))), Map.of("children", List.of(Map.of()))),
                "siblings", List.of(Map.of("ok", true))));
        var expression = new Exists(0, new Field(List.of("parents")),
                new Exists(1, new Field(0, List.of("children")), new Field(1, List.of("ok"))));
        var unknown = List.<List<Object>>of(List.of("parents", 1, "children", 0, "ok"));
        assertUnavailable(expression, row, unknown, List.of("parents", 1, "children", 0, "ok"));
        assertThat(check(new Exists(0, new Field(List.of("siblings")), new Field(0, List.of("ok"))), row, unknown))
                .isEqualTo(new Result(true, null));
    }

    @Test void unknownDependenciesStillPermitBooleanConclusionsProvedWithoutThem() {
        var unknown = List.<List<Object>>of(List.of("customer"));
        Expr unresolved = new Present(List.of("customer"));
        for (boolean first : List.of(true, false)) {
            assertThat(check(first ? new And(unresolved, new Constant(false)) : new And(new Constant(false), unresolved), row(Map.of()), unknown))
                    .isEqualTo(new Result(false, null));
            assertThat(check(first ? new Or(unresolved, new Constant(true)) : new Or(new Constant(true), unresolved), row(Map.of()), unknown))
                    .isEqualTo(new Result(true, null));
            assertUnavailable(first ? new Or(unresolved, new Constant(false)) : new Or(new Constant(false), unresolved), row(Map.of()), unknown, List.of("customer"));
        }
    }

    @Test void missingAndNullRemainOrdinaryFactsOutsideUnavailableScopes() {
        var fields = new java.util.HashMap<String, Object>(); fields.put("customer", null);
        assertThat(check(new Present(List.of("customer")), row(fields), List.of())).isEqualTo(new Result(true, null));
        assertThat(check(new Present(List.of("customer")), row(Map.of()), List.of())).isEqualTo(new Result(false, null));
        assertThat(check(new Field(List.of("customer")), row(Map.of()), List.of()).problem().kind()).isEqualTo(ProblemKind.MISSING_FIELD);
        assertUnavailable(new Field(List.of("active")), row(Map.of("active", true)), List.of(List.of()), List.of());
    }

    @Test void malformedAvailabilityAndItsHardBudgetsAreNeverAbsorbedByAConstantResult() {
        var predicate = compile("filter", new Constant(true), LIMITS);
        assertThatThrownBy(() -> predicate.evaluate(row(Map.of()), List.of(List.of("items", -1))))
                .isInstanceOfSatisfying(TapstateException.class, error -> assertThat(error.code()).isEqualTo(ValidationError.INVALID_CONTRACT));
        assertThatThrownBy(() -> predicate.evaluate(row(Map.of()), java.util.Collections.nCopies(257, List.of("x"))))
                .isInstanceOf(TapstateException.class);
        var small = compile("filter", new Constant(true), new Limits(new ValidationValues.Limits(800, 50, 8), 10000, false));
        assertThatThrownBy(() -> small.evaluate(row(Map.of()), List.of(List.of("x".repeat(400)))))
                .isInstanceOfSatisfying(TapstateException.class, error -> assertThat(error.args()).containsEntry("limit", "filter.input"));
        var tight = compile("filter", new Or(new Present(List.of("x")), new Constant(true)), new Limits(ROW, 3, false));
        assertThatThrownBy(() -> tight.evaluate(row(Map.of()), List.of(List.of("x"))))
                .isInstanceOfSatisfying(TapstateException.class, error -> assertThat(error.args()).containsEntry("limit", "filter.operations"));
    }

    @Test void unavailableJsonMemberNamesDoNotInvalidateIndependentKnownFields() {
        var row = row(Map.of("active", true, "payload", Map.of()));
        for (String field : List.of("", "  ")) {
            var unknown = List.<List<Object>>of(List.of("payload", field));
            assertThat(check(new Field(List.of("active")), row, unknown)).isEqualTo(new Result(true, null));
            assertThat(check(new Present(List.of("payload")), row, unknown)).isEqualTo(new Result(true, null));
        }
    }

    private static Result check(Expr expression, ValidationValues.Row row, List<List<Object>> unknown) {
        return compile("filter", expression, LIMITS).evaluate(row, unknown);
    }
    private static void assertUnavailable(Expr expression, ValidationValues.Row row, List<List<Object>> unknown, List<Object> expectedPath) {
        assertThat(check(expression, row, unknown)).isEqualTo(new Result(null, new Problem(ProblemKind.UNAVAILABLE_FIELD, expectedPath)));
    }
    private static ValidationValues.Row row(Map<String, ?> fields) { return ValidationValues.ownRow(fields, ROW); }
}
