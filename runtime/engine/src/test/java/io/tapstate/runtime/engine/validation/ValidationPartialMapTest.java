package io.tapstate.runtime.engine.validation;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.FieldRule;
import io.tapstate.core.model.TransformBody;
import io.tapstate.core.validation.ValidationError;
import io.tapstate.core.validation.ValidationPredicate;
import io.tapstate.core.validation.ValidationValues;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class ValidationPartialMapTest {
    private static final ValidationValues.Limits ROW = new ValidationValues.Limits(32768, 512, 32);

    @Test void renamedUnknownReferenceCannotBecomeKnownAbsentAfterAMapAndFilter() {
        var original = row(Map.of("id", 7));
        var first = compile(Map.of("client", FieldRule.rename("customer"))).apply(original, List.of(List.of("customer")));
        var second = compile(Map.of("account", FieldRule.rename("client"))).apply(first.row(), first.unavailablePaths());
        assertThat(second.unavailablePaths()).containsExactly(List.of("account"));
        var has = ValidationPredicate.compile("filter", new ValidationPredicate.Present(List.of("account")), new ValidationPredicate.Limits(ROW, 10000, false));
        assertThat(has.evaluate(second.row(), second.unavailablePaths()).problem().kind()).isEqualTo(ValidationPredicate.ProblemKind.UNAVAILABLE_FIELD);
        assertThat(original.fields()).containsExactly(entry("id", 7));
    }

    @Test void everyRenameReadsOriginalUnknownDescendantsIncludingAllArrayOrdinals() {
        var input = row(Map.of("left", List.of(Map.of()), "right", List.of(Map.of("known", true))));
        var map = compile(Map.of("left", FieldRule.rename("right"), "right", FieldRule.rename("left"), "copy", FieldRule.rename("left")));
        var projected = map.apply(input, List.of(List.of("left", 0, "vip")));
        assertThat(projected.unavailablePaths()).containsExactlyInAnyOrder(List.of("right", 0, "vip"), List.of("copy", 0, "vip"));
        assertThat(projected.row().fields().get("left")).isEqualTo(List.of(Map.of("known", true)));
        assertThat(projected.accountedBytes()).isGreaterThan(projected.row().accountedBytes());
    }

    @Test void literalsAndDropsRemoveTheirUnknownContributionsButRenameStillReadsTheOriginalInput() {
        var projection = compile(Map.of("customer", FieldRule.drop(), "copy", FieldRule.rename("customer"), "flag", FieldRule.literal(true)))
                .apply(row(Map.of()), List.of(List.of("customer"), List.of("flag")));
        assertThat(projection.unavailablePaths()).containsExactly(List.of("copy"));
        assertThat(projection.row().fields()).containsExactly(entry("flag", true));
        var overwrite = compile(Map.of("customer", FieldRule.literal(Map.of("vip", true))))
                .apply(row(Map.of()), List.of(List.of("customer", "vip")));
        assertThat(overwrite.unavailablePaths()).isEmpty();
        assertThat(overwrite.row().fields()).containsEntry("customer", Map.of("vip", true));
    }

    @Test void missingRenameFallsBackToTheOriginalDestinationWhileAnUnknownSourceKeepsItUnknown() {
        var input = row(Map.of("destination", "retained"));
        var map = compile(Map.of("destination", FieldRule.rename("source")));
        var missing = map.apply(input, List.of(List.of("destination")));
        assertThat(missing.row().fields()).containsEntry("destination", "retained");
        assertThat(missing.unavailablePaths()).containsExactly(List.of("destination"));
        var unknown = map.apply(input, List.of(List.of("source")));
        assertThat(unknown.unavailablePaths()).containsExactly(List.of("destination"));
        var replaced = map.apply(row(Map.of("source", "known", "destination", "old")), List.of(List.of("destination")));
        assertThat(replaced.row().fields()).containsOnly(entry("destination", "known"));
        assertThat(replaced.unavailablePaths()).isEmpty();
    }

    @Test void literalFieldNamesAndBlankJsonMembersAreNotSplitOrNormalized() {
        var projection = compile(Map.of("renamed", FieldRule.rename("a.b")))
                .apply(row(Map.of("a.b", Map.of(), "a", Map.of("b", true))), List.of(List.of("a.b", ""), List.of("a.b", "  ")));
        assertThat(projection.unavailablePaths()).containsExactlyInAnyOrder(List.of("renamed", ""), List.of("renamed", "  "));
        assertThat(projection.row().fields()).containsEntry("a", Map.of("b", true));
    }

    @Test void wholeUnknownViewsRemainUnknownAndAllSuppliedMetadataIsStillValidated() {
        var map = compile(Map.of("flag", FieldRule.literal(true)));
        var projection = map.apply(row(Map.of()), List.of(List.of()));
        assertThat(projection.unavailablePaths()).containsExactly(List.of());
        assertThatThrownBy(() -> map.apply(row(Map.of()), List.of(List.of(), List.of("x", -1))))
                .isInstanceOfSatisfying(TapstateException.class, error -> assertThat(error.code()).isEqualTo(ValidationError.INVALID_CONTRACT));
    }

    @Test void fanoutAndAggregateMetadataBudgetsAbortInsteadOfDroppingUnknownPaths() {
        var rules = new java.util.LinkedHashMap<String, FieldRule>();
        for (int i = 0; i < 257; i++) rules.put("out" + i, FieldRule.rename("source"));
        var roomy = new ValidationValues.Limits(1024 * 1024, 10000, 32);
        var fanout = ValidationStaticMap.compile("map", new TransformBody.MapProjection(rules), roomy);
        assertThatThrownBy(() -> fanout.apply(row(Map.of()), List.of(List.of("source"))))
                .isInstanceOfSatisfying(TapstateException.class, error -> assertThat(error.args()).containsEntry("limit", "map.unavailablePaths"));
        var small = ValidationStaticMap.compile("map", new TransformBody.MapProjection(Map.of()), new ValidationValues.Limits(1000, 50, 8));
        assertThatThrownBy(() -> small.apply(row(Map.of()), List.of(List.of("x".repeat(450)))))
                .isInstanceOfSatisfying(TapstateException.class, error -> assertThat(error.args()).containsEntry("limit", "map.unavailableBytes"));
    }

    private static ValidationStaticMap compile(Map<String, FieldRule> rules) {
        return ValidationStaticMap.compile("map", new TransformBody.MapProjection(rules), ROW);
    }
    private static ValidationValues.Row row(Map<String, ?> values) { return ValidationValues.ownRow(values, ROW); }
}
