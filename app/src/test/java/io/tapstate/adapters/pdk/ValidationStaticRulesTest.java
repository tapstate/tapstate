package io.tapstate.adapters.pdk;

import io.tapstate.core.dsl.ValidationPredicateCompiler;
import io.tapstate.core.model.FieldRule;
import io.tapstate.core.model.TransformBody;
import io.tapstate.core.validation.ValidationPredicate;
import io.tapstate.core.validation.ValidationKey;
import io.tapstate.core.validation.ValidationValueComparison;
import io.tapstate.core.validation.ValidationValues;
import io.tapstate.runtime.engine.validation.ValidationStaticMap;
import io.tapstate.runtime.engine.validation.ValidationOrdinalExpansion;
import java.math.BigDecimal;
import java.util.LinkedHashMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

/** Shared owned facts cross the parser and independent rules; not job, reader or root-result acceptance. */
class ValidationStaticRulesTest {
    private static final ValidationValues.Limits ROW = new ValidationValues.Limits(8192, 150, 16);
    private static final ValidationPredicate.Limits PREDICATE = new ValidationPredicate.Limits(ROW, 2000, false);

    @Test void staticMapThenFilterThenMapChecksTheFinalProjectionFromSourceFacts() {
        var first = ValidationStaticMap.compile("rename", new TransformBody.MapProjection(
                Map.of("price", FieldRule.rename("amount"), "tag", FieldRule.literal("checked"))), ROW);
        var filter = ValidationPredicateCompiler.compile("eligible", "after.price >= 10 && after.tag == 'checked'", PREDICATE);
        var last = ValidationStaticMap.compile("final", new TransformBody.MapProjection(Map.of("total", FieldRule.rename("price"))), ROW);
        var amount = new ValidationValues.Typed("exact-number", "decimal(20,2)", "source", "custom", new BigDecimal("10.00"));
        var source = row(Map.of("id", 7L, "amount", amount));
        var working = first.apply(source);
        assertThat(filter.evaluate(working).matches()).isTrue();
        var expected = last.apply(working);
        var target = row(Map.of("id", 7L, "total", 10L, "tag", "checked"));
        assertThat(ValidationValueComparison.compare(expected, target)).isEmpty();
        assertThat(expected.fields()).containsOnlyKeys("id", "total", "tag");
        assertThat(source.fields()).containsOnlyKeys("id", "amount").containsEntry("amount", amount);
    }

    @Test void aWrongTargetValueCannotMakeAnExcludedSourceParticipate() {
        var first = ValidationStaticMap.compile("rename", new TransformBody.MapProjection(Map.of("price", FieldRule.rename("amount"))), ROW);
        var filter = ValidationPredicateCompiler.compile("eligible", "after.price >= 10", PREDICATE);
        var source = row(Map.of("id", 8L, "amount", 9L));
        var wrongTarget = row(Map.of("id", 8L, "price", 999L));
        assertThat(filter.evaluate(first.apply(source)).matches()).isFalse();
        // This contrasting fact makes the fixture discriminate a future target-as-oracle wiring error.
        assertThat(filter.evaluate(wrongTarget).matches()).isTrue();
        assertThat(source.fields()).containsEntry("amount", 9L);
        assertThat(wrongTarget.fields()).containsEntry("price", 999L);
    }

    @Test void missingOrNullStaticInputsDoNotBecomeDefaultNumbersOrFalseParticipation() {
        var first = ValidationStaticMap.compile("rename", new TransformBody.MapProjection(Map.of("price", FieldRule.rename("amount"))), ROW);
        var filter = ValidationPredicateCompiler.compile("eligible", "after.price >= 10", PREDICATE);
        var missing = filter.evaluate(first.apply(row(Map.of("id", 9L))));
        assertThat(missing.matches()).isNull();
        assertThat(missing.problem().kind()).isEqualTo(ValidationPredicate.ProblemKind.MISSING_FIELD);
        var values = new LinkedHashMap<String, Object>(); values.put("id", 9L); values.put("amount", null);
        var nullable = filter.evaluate(first.apply(row(values)));
        assertThat(nullable.matches()).isNull();
        assertThat(nullable.problem().kind()).isEqualTo(ValidationPredicate.ProblemKind.INCOMPATIBLE_TYPE);
        var guarded = ValidationPredicateCompiler.compile("guarded", "has(after.price) && after.price != null && after.price >= 10", PREDICATE);
        assertThat(guarded.evaluate(first.apply(row(values))).matches()).isFalse();
    }

    @Test void aMemberPredicateSelectsTheRootWithoutRemovingOrReindexingItsArray() {
        var rename = ValidationStaticMap.compile("rename", new TransformBody.MapProjection(Map.of("items", FieldRule.rename("lines"))), ROW);
        var filter = ValidationPredicateCompiler.compile("eligible",
                "size(after.items) > 0 && after.items.exists(item, item.amount >= 10)", PREDICATE);
        var items = List.of(Map.of("amount", 9L), Map.of("amount", 11L));
        var source = row(Map.of("id", 7L, "lines", items));
        var working = rename.apply(source);
        assertThat(filter.evaluate(working).matches()).isTrue();
        assertThat(working.fields()).containsEntry("items", items);
        assertThat(source.fields()).containsEntry("lines", items).doesNotContainKey("items");
        assertThat(ValidationValueComparison.compare(working, row(Map.of("id", 7L, "items", items)))).isEmpty();
        // This is an ordinary array, so a root-level exists decision does not erase its ordering duty.
        assertThat(ValidationValueComparison.compare(working,
                row(Map.of("id", 7L, "items", List.of(Map.of("amount", 11L), Map.of("amount", 9L)))))).isPresent();
    }

    @Test void filteringExpandedMembersKeepsTheOriginalOrdinalAndComparisonIdentity() {
        var expansion = ValidationOrdinalExpansion.compile("expand", new TransformBody.Unwind("items", "position", false, null, null), ROW, 10);
        var filter = ValidationPredicateCompiler.compile("keep", "after.items.keep", PREDICATE);
        var rename = ValidationStaticMap.compile("rename", new TransformBody.MapProjection(Map.of("item", FieldRule.rename("items"))), ROW);
        var parentKey = new ValidationKey(List.of(ValidationKey.Part.number(7)));
        var source = row(Map.of("id", 7L, "items", List.of(Map.of("keep", false), Map.of("keep", true))));
        var kept = new ArrayList<Long>();
        try (var cursor = ((ValidationOrdinalExpansion.Ready)expansion.prepare(source, parentKey)).cursor()) {
            while (true) {
                var next = cursor.next();
                if (next.isEmpty()) break;
                try (var output = next.orElseThrow()) {
                    var participation = filter.evaluate(output.values());
                    assertThat(participation.problem()).isNull();
                    if (participation.matches()) {
                        kept.add(output.ordinal());
                        assertThat(output.comparisonKey()).isEqualTo(new ValidationKey(List.of(
                                ValidationKey.Part.number(7), ValidationKey.Part.number(1))));
                        var expected = rename.apply(output.values());
                        assertThat(ValidationValueComparison.compare(expected,
                                row(Map.of("id", 7L, "position", 1L, "item", Map.of("keep", true))))).isEmpty();
                    }
                }
            }
        }
        assertThat(kept).containsExactly(1L);
        assertThat(source.fields()).doesNotContainKeys("position", "item");
    }

    private static ValidationValues.Row row(Map<String, ?> fields) { return ValidationValues.ownRow(fields, ROW); }
}
