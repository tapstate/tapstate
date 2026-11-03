package io.tapstate.core.validation;

import io.tapstate.core.common.TapstateException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import static io.tapstate.core.validation.ValidationRootCheck.*;
import static org.assertj.core.api.Assertions.*;

class ValidationPartialFieldCheckTest {
    private static final ValidationValues.Limits ROW = new ValidationValues.Limits(2 * 1024 * 1024, 1000, 16);
    private static final ValidationObservation OBSERVATION = new ValidationObservation(new ValidationRunIdentity(
            new ValidationOwner("cluster", "pipeline", "node", "boot", 1, 1, 0), "task", "run", ValidationMode.FULL, "spec", 1, "rules"),
            new ValidationRoot("mapping", "output", new ValidationKey(List.of(ValidationKey.Part.number(1))), List.of()), 1);

    @Test void unknownMissingFieldsDoNotBecomeTargetExtrasButIndependentValuesRemainChecked() {
        var paths = List.<List<Object>>of(List.of("unknown"));
        var root = blocked(paths);
        var checked = check(Map.of("known", 1), Map.of("known", 2, "unknown", Map.of("unproved", true)), paths, root, 10000);
        assertThat(checked.differences()).isEqualTo(1);
        var result = finish(root);
        assertThat(result.counts()).containsExactlyInAnyOrderEntriesOf(Map.of(Kind.SOURCE_FIELD_INVALID, 1L, Kind.TARGET_VALUE, 1L));
        assertThat(result.findings().keySet()).filteredOn(finding -> !finding.kind().sourceProblem())
                .allSatisfy(finding -> assertThat(finding.location().fieldPath()).containsExactly("known"));
        assertThat(result.observedAgreement()).isFalse();
    }

    @Test void anUnknownDescendantDoesNotHideKnownParentPresenceOrType() {
        var paths = List.<List<Object>>of(List.of("parent", "unknown"));
        for (var target : List.<Map<String, Object>>of(Map.of(), Map.of("parent", "wrong type"))) {
            var root = blocked(paths);
            check(Map.of("parent", Map.of("known", 1)), target, paths, root, 10000);
            var result = finish(root);
            assertThat(result.counts()).containsEntry(target.isEmpty() ? Kind.TARGET_MISSING : Kind.TARGET_TYPE, 1L);
            assertThat(result.findings().keySet()).filteredOn(finding -> !finding.kind().sourceProblem())
                    .singleElement().satisfies(finding -> assertThat(finding.location().fieldPath()).containsExactly("parent"));
        }
        var root = blocked(paths);
        check(Map.of("parent", Map.of("known", 1)), Map.of("parent", Map.of("known", 2, "unknown", 3)), paths, root, 10000);
        assertThat(finish(root).counts()).containsEntry(Kind.TARGET_VALUE, 1L).doesNotContainKey(Kind.TARGET_EXTRA);
    }

    @Test void ordinaryArrayOrdinalsAndLiteralFieldNamesRemainDistinct() {
        var paths = List.<List<Object>>of(List.of("items", 0, "x"), List.of("a.b"));
        var root = blocked(paths);
        check(Map.of("items", List.of(Map.of("x", 1), Map.of("x", 2)), "a", Map.of("b", 1)),
                Map.of("items", List.of(Map.of("x", 9), Map.of("x", 3)), "a", Map.of("b", 2), "a.b", 99), paths, root, 10000);
        var result = finish(root);
        assertThat(result.counts()).containsEntry(Kind.TARGET_VALUE, 2L).doesNotContainKey(Kind.TARGET_EXTRA);
        assertThat(result.findings().keySet()).filteredOn(finding -> finding.kind() == Kind.TARGET_VALUE)
                .extracting(finding -> finding.location().fieldPath()).containsExactlyInAnyOrder(List.of("items", 1, "x"), List.of("a", "b"));
    }

    @Test void wholeViewUnavailabilityCannotBeMistakenForSuccessfulAgreement() {
        var paths = List.<List<Object>>of(List.of()); var root = blocked(paths);
        assertThat(check(Map.of(), Map.of("anything", List.of(1, 2, 3)), paths, root, 10000).differences()).isZero();
        var result = finish(root);
        assertThat(result.counts()).containsExactly(entry(Kind.SOURCE_FIELD_INVALID, 1L));
        assertThat(result.checks()).containsEntry(Check.STRUCTURE, Status.BLOCKED).containsEntry(Check.VALUES, Status.BLOCKED);
        assertThat(result.observedAgreement()).isFalse();
    }

    @Test void onlyExactExistingSourceBlocksAuthorizeSkippingFields() {
        var paths = List.<List<Object>>of(List.of("unknown"));
        var unproved = builder();
        assertThatThrownBy(() -> check(Map.of(), Map.of("unknown", 9), paths, unproved, 10000)).isInstanceOf(TapstateException.class);
        assertThatThrownBy(unproved::finish).isInstanceOf(TapstateException.class);
        var partial = builder(); var cause = cause(); var location = new Location(OBSERVATION.root(), List.of("unknown"));
        partial.finding(cause).notCompared(Check.VALUES, location, cause);
        assertThatThrownBy(() -> check(Map.of(), Map.of("unknown", 9), paths, partial, 10000)).isInstanceOf(TapstateException.class);
        var wrongPath = blocked(List.of(List.of("parent")));
        assertThatThrownBy(() -> check(Map.of(), Map.of(), List.of(List.of("parent", "unknown")), wrongPath, 10000))
                .isInstanceOf(TapstateException.class);
    }

    @Test void unknownSubtreesAreSkippedBeforeValueTraversalButMaskWorkStillHasALimit() {
        var paths = List.<List<Object>>of(List.of("unknown"));
        var root = blocked(paths);
        assertThat(check(Map.of("known", 1, "unknown", "a".repeat(500000)),
                Map.of("known", 2, "unknown", "b".repeat(500000)), paths, root, 500).differences()).isEqualTo(1);
        var limited = blocked(paths);
        assertThatThrownBy(() -> check(Map.of(), Map.of("unknown", 1), paths, limited, 1))
                .isInstanceOfSatisfying(TapstateException.class, error -> assertThat(error.code()).isEqualTo(ValidationError.VALUE_LIMIT_EXCEEDED));
        assertThatThrownBy(limited::finish).isInstanceOf(IllegalStateException.class);
    }

    @Test void unknownSourceIdDoesNotQualifyAsKnownAbsentForGeneratedIdExemption() {
        var paths = List.<List<Object>>of(List.of("_id")); var root = blocked(paths);
        var permit = ValidationFieldCheck.generatedId(OBSERVATION.run(), "mapping", "output", true, false, false, value -> value instanceof String);
        var checked = ValidationFieldCheck.checkPartial(OBSERVATION, List.of(), row(Map.of("known", 1)), row(Map.of("known", 2, "_id", "generated")),
                Optional.empty(), Optional.empty(), permit, String::equals, new ValidationValueComparison.TraversalLimits(ROW, 10000), paths, root);
        assertThat(checked.generatedIdValueExempt()).isFalse(); assertThat(checked.differences()).isEqualTo(1);
        assertThat(finish(root).counts()).containsEntry(Kind.TARGET_VALUE, 1L).doesNotContainKey(Kind.TARGET_EXTRA);
    }

    @Test void maskMetadataHasAHardByteBudgetEvenWhenEveryBlockIsAlreadyProven() {
        var paths = new java.util.ArrayList<List<Object>>();
        for (int i = 0; i < 10; i++) paths.add(List.of("x".repeat(500) + i));
        var root = blocked(paths);
        var tiny = new ValidationValues.Limits(2048, 100, 16);
        assertThatThrownBy(() -> ValidationFieldCheck.checkPartial(OBSERVATION, List.of(), row(Map.of()), row(Map.of()),
                Optional.empty(), Optional.empty(), Optional.empty(), String::equals,
                new ValidationValueComparison.TraversalLimits(tiny, 10000), paths, root))
                .isInstanceOfSatisfying(TapstateException.class, error -> assertThat(error.args()).containsEntry("limit", "fieldCheck.unavailablePaths"));
    }

    private static ValidationFieldCheck.Result check(Map<String, ?> source, Map<String, ?> target, List<List<Object>> paths, Builder root, long operations) {
        return ValidationFieldCheck.checkPartial(OBSERVATION, List.of(), row(source), row(target), Optional.empty(), Optional.empty(), Optional.empty(),
                String::equals, new ValidationValueComparison.TraversalLimits(ROW, operations), paths, root);
    }
    private static ValidationValues.Row row(Map<String, ?> value) { return ValidationValues.ownRow(value, ROW); }
    private static Finding cause() { return new Finding(Kind.SOURCE_FIELD_INVALID, new Location(OBSERVATION.root(), List.of("source-problem"))); }
    private static Builder builder() {
        var root = ValidationRootCheck.builder(OBSERVATION, ValidationMethod.FULL_COLUMN, ValidationCheckEvent.Participation.SHOULD_EXIST,
                TargetPresence.PRESENT, new Limits(4 * 1024 * 1024, 100, 512, 10, 0));
        root.read(Side.SOURCE, new ValidationCheckEvent.ReadBoundary("source", "complete", Instant.EPOCH, Instant.EPOCH));
        root.read(Side.TARGET, new ValidationCheckEvent.ReadBoundary("target", "complete", Instant.EPOCH, Instant.EPOCH));
        return root;
    }
    private static Builder blocked(List<List<Object>> paths) {
        var root = builder(); var cause = cause(); root.finding(cause);
        for (var path : paths) {
            var location = new Location(OBSERVATION.root(), path);
            root.notCompared(Check.STRUCTURE, location, cause).notCompared(Check.VALUES, location, cause);
        }
        return root;
    }
    private static ValidationRootCheck finish(Builder root) {
        return root.checked(Check.IDENTITY).checked(Check.PARTICIPATION).checked(Check.STRUCTURE).checked(Check.VALUES).finish();
    }
}
