package io.tapstate.runtime.engine.validation;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.FieldRule;
import io.tapstate.core.model.TransformBody;
import io.tapstate.core.validation.ValidationError;
import io.tapstate.core.validation.ValidationValueComparison;
import io.tapstate.core.validation.ValidationValues;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class ValidationStaticMapTest {
    private static final ValidationValues.Limits LIMITS = new ValidationValues.Limits(8192, 150, 16);

    @Test void chainedMapsProduceOnlyFinalShapeWithoutChangingReadFactsOrTypedValues() {
        var amount = new ValidationValues.Typed("exact-number", "decimal(20,2)", "source-codec", "custom", new BigDecimal("10.00"));
        var source = row(Map.of("id", 7L, "amount", amount, "old", "discarded"));
        var first = compile(rules("price", FieldRule.rename("amount"), "tag", FieldRule.literal("checked"), "old", FieldRule.drop()));
        var second = compile(rules("total", FieldRule.rename("price")));
        var intermediate = first.apply(source);
        var output = second.apply(intermediate);
        assertThat(source.fields()).containsOnlyKeys("id", "amount", "old");
        assertThat(intermediate.fields()).containsOnlyKeys("id", "price", "tag");
        assertThat(output.fields()).containsOnlyKeys("id", "total", "tag");
        assertThat(output.fields().get("total")).isEqualTo(amount);
        assertThat(ValidationValueComparison.compare(output, row(Map.of("id", 7, "total", 10L, "tag", "checked")))).isEmpty();
        var staleTarget = row(Map.of("id", 7, "total", 10L, "tag", "checked", "price", 10L));
        assertThat(ValidationValueComparison.compare(output, staleTarget)).hasValueSatisfying(diff -> {
            assertThat(diff.kind()).isEqualTo(ValidationValueComparison.Kind.MISSING_SOURCE);
            assertThat(diff.path()).containsExactly("price");
        });
        assertThat(first.executionReservationBytes(source)).isEqualTo(first.retainedBytes() + source.accountedBytes() + 3L * LIMITS.bytes());
    }

    @Test void allAssignmentsReadTheOriginalNodeInputEvenWhenOutputsOverlap() {
        var map = compile(rules("a", FieldRule.rename("b"), "b", FieldRule.rename("a"),
                "alias", FieldRule.rename("a"), "seed", FieldRule.literal("new"), "original_seed", FieldRule.rename("seed")));
        assertThat(map.apply(row(Map.of("a", 1L, "b", 2L, "seed", "old", "untouched", true))).fields())
                .containsExactly(entry("a", 2L), entry("b", 1L), entry("alias", 1L), entry("seed", "new"),
                        entry("original_seed", "old"), entry("untouched", true));
    }

    @Test void missingRenameFallsThroughWhilePresentNullIsAProducedValue() {
        var input = new LinkedHashMap<String, Object>();
        input.put("destination", "keep"); input.put("source", null); input.put("nullable_destination", "replace");
        var map = compile(rules("destination", FieldRule.rename("missing"), "absent", FieldRule.rename("also_missing"),
                "nullable_destination", FieldRule.rename("source")));
        assertThat(map.apply(row(input)).fields()).containsOnlyKeys("destination", "nullable_destination")
                .containsEntry("destination", "keep").containsEntry("nullable_destination", null);
    }

    @Test void dropDoesNotChangeAnotherRuleReadAndDotsRemainLiteralFieldNames() {
        var map = compile(rules("a.b", FieldRule.drop(), "copied", FieldRule.rename("a.b"), "removed", FieldRule.drop()));
        var output = map.apply(row(Map.of("a.b", "literal", "a", Map.of("b", "nested"), "removed", true)));
        assertThat(output.fields()).containsExactly(entry("copied", "literal"), entry("a", Map.of("b", "nested")));
    }

    @Test void literalTreesAreFrozenAtCompileAndNoCallerMutationChangesLaterRows() {
        var items = new ArrayList<Object>(List.of("first", new BigDecimal("9007199254740993")));
        var literal = new LinkedHashMap<String, Object>(); literal.put("items", items); literal.put("nullable", null);
        var map = compile(rules("constant", FieldRule.literal(literal)));
        items.clear(); literal.put("extra", "late");
        var output = map.apply(row(Map.of()));
        var expected = new LinkedHashMap<String, Object>();
        expected.put("items", List.of("first", new BigDecimal("9007199254740993"))); expected.put("nullable", null);
        assertThat(output.fields()).containsExactly(entry("constant", expected));
        assertThatThrownBy(() -> ((Map<String, Object>)output.fields().get("constant")).put("x", true))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test void everyComputedRuleIsRefusedIncludingConstantsAndUnrelatedFields() {
        for (String expression : List.of("1", "after.amount", "after.amount + 1", "now()")) {
            assertThatThrownBy(() -> compile(rules("unrelated", FieldRule.computed(expression))))
                    .isInstanceOfSatisfying(TapstateException.class, failure -> {
                        assertThat(failure.code()).isEqualTo(ValidationError.RULE_UNSUPPORTED);
                        assertThat(failure.args()).containsEntry("node", "map-node").containsEntry("field", "unrelated")
                                .containsEntry("rule", "map.computed");
                    });
        }
    }

    @Test void compileBoundsTheAggregateConstantsAndMetadataBeforeOwningTheProgram() {
        var small = new ValidationValues.Limits(1500, 100, 10);
        var fields = rules("a", FieldRule.literal("x".repeat(250)), "b", FieldRule.literal("y".repeat(250)),
                "c", FieldRule.literal("z".repeat(250)));
        assertLimit(() -> ValidationStaticMap.compile("map-node", new TransformBody.MapProjection(fields), small));
        var excessive = new LinkedHashMap<String, FieldRule>();
        for (int i = 0; i < 4097; i++) excessive.put("f" + i, FieldRule.drop());
        assertLimit(() -> compile(excessive));
        assertThatThrownBy(() -> compile(rules("x".repeat(513), FieldRule.drop())))
                .isInstanceOfSatisfying(TapstateException.class, failure -> assertThat(failure.code()).isEqualTo(ValidationError.INVALID_CONTRACT));
    }

    @Test void repeatedRenamesCannotMultiplyAValuePastTheWorkingRowLimit() {
        var limits = new ValidationValues.Limits(1800, 50, 8);
        var source = ValidationValues.ownRow(Map.of("payload", "x".repeat(450)), limits);
        var map = ValidationStaticMap.compile("map-node", new TransformBody.MapProjection(
                rules("left", FieldRule.rename("payload"), "right", FieldRule.rename("payload"))), limits);
        assertLimit(() -> map.apply(source));
        assertThat(source.fields()).containsExactly(entry("payload", "x".repeat(450)));
    }

    @Test void oversizedInputCannotBeAdmittedByAProjectionThatWouldDropIt() {
        var small = new ValidationValues.Limits(1000, 50, 8);
        var map = ValidationStaticMap.compile("map-node", new TransformBody.MapProjection(rules("payload", FieldRule.drop())), small);
        var input = row(Map.of("payload", "x".repeat(600)));
        assertLimit(() -> map.apply(input));
        assertLimit(() -> map.executionReservationBytes(input));
    }

    @Test void emptyProjectionPreservesAllOwnedFacts() {
        var input = row(Map.of("record", Map.of("children", List.of(1L, 2L)), "text", " 001 "));
        assertThat(compile(Map.of()).apply(input).fields()).isEqualTo(input.fields());
    }

    private static ValidationStaticMap compile(Map<String, FieldRule> rules) {
        return ValidationStaticMap.compile("map-node", new TransformBody.MapProjection(rules), LIMITS);
    }
    private static ValidationValues.Row row(Map<String, ?> values) { return ValidationValues.ownRow(values, LIMITS); }
    private static Map<String, FieldRule> rules(Object... pairs) {
        var result = new LinkedHashMap<String, FieldRule>();
        for (int i = 0; i < pairs.length; i += 2) result.put((String)pairs[i], (FieldRule)pairs[i + 1]);
        return result;
    }
    private static void assertLimit(Runnable action) {
        assertThatThrownBy(action::run).isInstanceOfSatisfying(TapstateException.class,
                failure -> assertThat(failure.code()).isEqualTo(ValidationError.VALUE_LIMIT_EXCEEDED));
    }
}
