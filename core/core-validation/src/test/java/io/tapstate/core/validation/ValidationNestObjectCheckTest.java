package io.tapstate.core.validation;

import io.tapstate.core.common.TapstateException;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import static io.tapstate.core.validation.ValidationRootCheck.*;
import static org.assertj.core.api.Assertions.*;

class ValidationNestObjectCheckTest {
    private static final ValidationValues.Limits ROW = new ValidationValues.Limits(8192, 100, 16);
    private static final List<String> PATH = List.of("contact", "customer");

    @Test void emptyGatheringRequiresOmissionWithoutRequiringAnUnusedParentPrefix() {
        for (var fields : List.of(Map.<String, Object>of(), Map.<String, Object>of("contact", "independent-base-value"),
                Map.<String, Object>of("contact", Map.of()))) {
            try (var root = builder()) {
                var source = source(observation(1), PATH, 0, root);
                check(observation(1), PATH, source, Optional.of(target(fields)), ROW, root, (left, right) -> fail("No child exists"));
                assertThat(finish(root).observedAgreement()).isTrue();
            }
        }
        for (var child : List.of(Map.of(), Map.of("name", "unexpected"))) {
            try (var root = builder()) {
                var source = source(observation(1), PATH, 0, root);
                check(observation(1), PATH, source, Optional.of(target(Map.of("contact", Map.of("customer", child)))), ROW, root,
                        (left, right) -> fail("No source child exists"));
                assertThat(finish(root).counts()).containsOnlyKeys(Kind.TARGET_STRUCTURE);
            }
        }
    }

    @Test void uniqueSourceRequiresAnObjectAndDistinguishesMissingFromInvalidPrefixes() {
        for (var fields : List.of(Map.<String, Object>of(), Map.<String, Object>of("contact", "wrong"),
                Map.<String, Object>of("contact", nullable("customer")))) {
            try (var root = builder()) {
                var source = source(observation(1), PATH, 1, root);
                check(observation(1), PATH, source, Optional.of(target(fields)), ROW, root, (left, right) -> fail("Shape is not comparable"));
                var checked = finish(root);
                Kind expected = fields.isEmpty() ? Kind.TARGET_MISSING : Kind.TARGET_STRUCTURE;
                assertThat(checked.counts()).containsOnlyKeys(expected);
                assertThat(checked.notApplicableScopes()).hasSize(2).allSatisfy(skipped -> assertThat(skipped.location().fieldPath()).containsExactlyElementsOf(PATH));
                if (fields.get("contact") instanceof String) assertThat(checked.findings().keySet()).singleElement().satisfies(finding ->
                        assertThat(finding.location().fieldPath()).containsExactly("contact"));
            }
        }
    }

    @Test void matchingShapeDelegatesWorkingPayloadAndLeavesRawDiagnosticsAndObligationsIntact() {
        try (var root = builder()) {
            var source = source(observation(1), PATH, 1, root);
            var visited = new AtomicInteger();
            var document = target(Map.of("id", 102, "contact", Map.of("customer", Map.of("name", "current"))));
            check(observation(1), PATH, source, Optional.of(document), ROW, root, (left, right) -> {
                visited.incrementAndGet();
                assertThat(left.original().fields()).containsKey("raw_name").doesNotContainKey("name");
                assertThat(ValidationValueComparison.compare(left.working(), right)).isEmpty();
                assertThat(document.physical().row().fields()).containsKey("contact");
            });
            assertThat(visited).hasValue(1);
            // A shape callback must still complete identity, payload and descendant obligations.
            assertThatThrownBy(root::finish).isInstanceOf(TapstateException.class);
        }
    }

    @Test void blockedSourceNeverChoosesATargetAndMustRetainItsCauseInThisRoot() {
        try (var root = builder()) {
            var blocked = source(observation(1), PATH, 2, root);
            check(observation(1), PATH, blocked, Optional.empty(), ROW, root, (left, right) -> fail("No winner is allowed"));
            assertThat(finish(root).counts()).containsOnlyKeys(Kind.SOURCE_CARDINALITY);
            try (var unrelatedStaging = builder()) {
                assertThatThrownBy(() -> check(observation(1), PATH, blocked, Optional.empty(), ROW, unrelatedStaging, (left, right) -> {}))
                        .isInstanceOf(TapstateException.class);
                assertThatThrownBy(unrelatedStaging::finish).isInstanceOf(TapstateException.class);
            }
        }
    }

    @Test void sourceObservationAndOutputLocationCannotBeReboundToAnotherObject() {
        for (boolean revision : List.of(true, false)) {
            try (var prior = builder(); var root = builder()) {
                var location = revision ? PATH : List.of("other");
                var observation = observation(revision ? 2 : 1);
                var previous = revision ? ValidationRootCheck.builder(observation, ValidationMethod.FULL_COLUMN,
                        ValidationCheckEvent.Participation.SHOULD_EXIST, TargetPresence.PRESENT, new Limits(1024 * 1024, 100, 100, 20, 5)) : prior;
                try (previous) {
                    var facts = source(observation, location, 0, previous);
                    assertThatThrownBy(() -> check(observation(1), PATH, facts, Optional.of(target(Map.of())), ROW, root, (left, right) -> {}))
                            .isInstanceOf(TapstateException.class);
                    assertThatThrownBy(root::finish).isInstanceOf(IllegalStateException.class);
                }
            }
        }
    }

    @Test void callbackErrorsAndResourceFailuresCannotFinishPartialRootObservations() {
        var document = target(Map.of("contact", Map.of("customer", Map.of("name", "current"))));
        var fatal = new AssertionError("descendant failed");
        try (var root = builder()) {
            var source = source(observation(1), PATH, 1, root);
            assertThat(catchThrowable(() -> check(observation(1), PATH, source, Optional.of(document), ROW, root,
                    (left, right) -> { throw fatal; }))).isSameAs(fatal);
            assertThatThrownBy(root::finish).isInstanceOf(IllegalStateException.class);
        }
        try (var root = builder()) {
            var source = source(observation(1), PATH, 1, root);
            assertThatThrownBy(() -> check(observation(1), PATH, source, Optional.of(document), new ValidationValues.Limits(1, 1, 1), root,
                    (left, right) -> {})).isInstanceOfSatisfying(TapstateException.class,
                            error -> assertThat(error.code()).isEqualTo(ValidationError.VALUE_LIMIT_EXCEEDED));
            assertThatThrownBy(root::finish).isInstanceOf(IllegalStateException.class);
        }
    }

    private static void check(ValidationObservation parent, List<String> path, ValidationNestSingleSource.Result source,
            Optional<ValidationNestObjectCheck.Target> target, ValidationValues.Limits limits, Builder root,
            java.util.function.BiConsumer<ValidationRelationRows.Sample, ValidationValues.Row> matched) {
        ValidationNestObjectCheck.check(parent, path, source, target, Set.of("map", "json"), limits, root, matched);
    }
    private static ValidationNestSingleSource.Result source(ValidationObservation observation, List<String> path, int count, Builder root) {
        var location = new Location(observation.root(), new java.util.ArrayList<>(path));
        try (var rows = new ValidationRelationRows(observation, "child", new ValidationRelationRows.Limits(32768, ROW))) {
            for (int i = 0; i < count; i++) rows.accept(observation, "child", ValidationRelationRows.Disposition.INCLUDED,
                    new ValidationRelationRows.Sample(ValidationValues.ownRow(Map.of("id", 7 + i, "raw_name", "current"), ROW),
                            ValidationValues.ownRow(Map.of("name", "current"), ROW), Optional.of(physical("source", 7 + i))));
            rows.complete(observation, boundary("child"));
            return ValidationNestSingleSource.check(rows.finish(), ValidationNestSingleSource.Relation.GATHER, Optional.empty(),
                    location, List.of(location), List.of(), root);
        }
    }
    private static ValidationNestObjectCheck.Target target(Map<String, ?> fields) {
        var row = ValidationValues.ownRow(fields, ROW);
        return new ValidationNestObjectCheck.Target(new ValidationCheckEvent.Side(physical("target", 102), row, 1,
                ValidationCheckEvent.GroupStatus.COMPLETE, boundary("target")), row);
    }
    private static Builder builder() {
        return ValidationRootCheck.builder(observation(1), ValidationMethod.FULL_COLUMN, ValidationCheckEvent.Participation.SHOULD_EXIST,
                TargetPresence.PRESENT, new Limits(1024 * 1024, 100, 100, 20, 5)).read(Side.TARGET, boundary("target"));
    }
    private static ValidationRootCheck finish(Builder root) { for (Check check : Check.values()) root.checked(check); return root.finish(); }
    private static Map<String, Object> nullable(String field) { var values = new HashMap<String, Object>(); values.put(field, null); return values; }
    private static ValidationPhysicalIdentity physical(String side, long key) {
        return new ValidationPhysicalIdentity(side, "db", null, "table", List.of(List.of("id")), key(key));
    }
    private static ValidationCheckEvent.ReadBoundary boundary(String id) { return new ValidationCheckEvent.ReadBoundary(id, "complete", Instant.EPOCH, Instant.EPOCH); }
    private static ValidationObservation observation(long revision) {
        return new ValidationObservation(new ValidationRunIdentity(new ValidationOwner("cluster", "pipeline", "node", "boot", 1, 1, 0),
                "task", "run", ValidationMode.FULL, "spec", 1, "rules"), new ValidationRoot("mapping", "output", key(102), List.of()), revision);
    }
    private static ValidationKey key(long value) { return new ValidationKey(List.of(ValidationKey.Part.number(value))); }
}
