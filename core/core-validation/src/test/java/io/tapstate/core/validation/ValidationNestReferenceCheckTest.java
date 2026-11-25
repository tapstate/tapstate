package io.tapstate.core.validation;

import io.tapstate.core.common.TapstateException;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import static io.tapstate.core.validation.ValidationNestReferenceCheck.Shape.*;
import static io.tapstate.core.validation.ValidationRootCheck.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ValidationNestReferenceCheckTest {
    private static final ValidationValues.Limits ROW = new ValidationValues.Limits(8192, 100, 16);
    private static final Limits ROOT = new Limits(1024 * 1024, 100, 100, 20, 4);
    private static final ValidationReferenceFields FIELDS = new ValidationReferenceFields(List.of("customer_id"), ROW);

    @Test void nullObjectReferenceRequiresAnOmittedFieldNotNullOrAnEmptyObject() {
        var source = nullable("customer_id");
        assertThat(run(OBJECT, source, Map.of()).observedAgreement()).isTrue();
        for (var target : List.of(nullable("customer"), Map.<String, Object>of("customer", Map.of()),
                Map.<String, Object>of("customer", Map.of("id", 19)))) {
            var result = run(OBJECT, source, target);
            assertThat(result.counts()).containsEntry(Kind.TARGET_STRUCTURE, 1L);
            assertThat(result.notApplicableScopes()).hasSize(2);
        }
    }

    @Test void nullArrayReferenceRequiresAnExplicitEmptyArray() {
        var source = nullable("customer_id");
        for (Object empty : List.of(List.of(), new ValidationValues.Typed("array", null, "codec", List.of()),
                new ValidationValues.Typed("json", "json", "codec", List.of()))) {
            assertThat(run(ARRAY, source, Map.of("customer", empty)).observedAgreement()).isTrue();
        }
        for (var target : List.of(Map.<String, Object>of(), nullable("customer"), Map.<String, Object>of("customer", Map.of()),
                Map.<String, Object>of("customer", List.of(Map.of("id", 19))),
                Map.<String, Object>of("customer", new ValidationValues.Typed("text", null, "codec", List.of())))) {
            assertThat(run(ARRAY, source, target).counts()).containsEntry(Kind.TARGET_STRUCTURE, 1L);
        }
    }

    @Test void missingReferenceFieldsBlockOnlyTheirOutputAndDoNotJudgeTargetShape() {
        var root = observation(List.of(), 1);
        var builder = builder(root, TargetPresence.PRESENT);
        var target = side("target", Map.of("customer", "unusable"));
        var reference = ValidationNestReferenceCheck.check(root, List.of("customer"), OBJECT, FIELDS,
                side("source", Map.of("other", 7)), Optional.of(target), builder);
        assertThat(reference).isEqualTo(new ValidationReferenceFields.Missing(List.of("customer_id")));
        var result = finish(builder);
        assertThat(result.counts()).containsOnlyKeys(Kind.SOURCE_FIELD_MISSING);
        assertThat(result.checks()).containsEntry(Check.STRUCTURE, Status.BLOCKED).containsEntry(Check.VALUES, Status.BLOCKED);
        assertThat(result.notCompared()).hasSize(2).allSatisfy(blocked ->
                assertThat(blocked.location().fieldPath()).containsExactly("customer"));
    }

    @Test void aMissingCompositeComponentStillWinsOverAnotherNullComponent() {
        var root = observation(List.of(), 1);
        var builder = builder(root, TargetPresence.PRESENT);
        var fields = new ValidationReferenceFields(List.of("tenant", "customer_id"), ROW);
        var reference = ValidationNestReferenceCheck.check(root, List.of("customer"), OBJECT, fields,
                side("source", nullable("tenant")), Optional.of(side("target", Map.of("customer", Map.of("id", 7)))), builder);
        assertThat(reference).isEqualTo(new ValidationReferenceFields.Missing(List.of("customer_id")));
        assertThat(finish(builder).counts()).containsOnlyKeys(Kind.SOURCE_FIELD_MISSING);
    }

    @Test void completeReferencesReturnOriginalComponentsForTheActualLookup() {
        for (Object value : List.of(0, false, "", 19)) {
            var root = observation(List.of(), 1);
            try (var builder = builder(root, TargetPresence.PRESENT)) {
                var result = ValidationNestReferenceCheck.check(root, List.of("customer"), OBJECT, FIELDS,
                        side("source", Map.of("customer_id", value)), Optional.of(side("target", Map.of("customer", "not checked yet"))), builder);
                assertThat(result).isInstanceOf(ValidationReferenceFields.Complete.class);
                assertThat(((ValidationReferenceFields.Complete) result).components()).containsExactly(value);
                // The required lookup and descendant checks have not completed, so do not finish this root.
                assertThatThrownBy(builder::finish).isInstanceOf(TapstateException.class);
            }
        }
    }

    @Test void nestedOutputLocationsRetainTheParentAncestorAndDoNotInventAChildKey() {
        var root = observation(List.of(), 1);
        var parent = observation(List.of(new ValidationRoot.Ancestor(List.of("items"), key(11))), 1);
        var builder = builder(root, TargetPresence.PRESENT);
        ValidationNestReferenceCheck.check(parent, List.of("address", "customer"), ARRAY, FIELDS,
                side("source", nullable("customer_id")), Optional.of(side("target", Map.of())), builder);
        var result = finish(builder);
        var location = result.findings().keySet().iterator().next().location();
        assertThat(location.scope().ancestors()).containsExactly(new ValidationRoot.Ancestor(List.of("items"), key(11)));
        assertThat(location.fieldPath()).containsExactly("address", "customer");
        var omitted = builder(root, TargetPresence.PRESENT);
        ValidationNestReferenceCheck.check(parent, List.of("address", "customer"), OBJECT, FIELDS,
                side("source", nullable("customer_id")), Optional.of(side("target", Map.of())), omitted);
        assertThat(finish(omitted).observedAgreement()).isTrue();
    }

    @Test void unknownParticipationCanRetainMissingSourceFactsWithoutAFabricatedTargetRead() {
        var root = observation(List.of(), 1);
        var builder = ValidationRootCheck.builder(root, ValidationMethod.FULL_COLUMN,
                ValidationCheckEvent.Participation.UNKNOWN, TargetPresence.NOT_READ, ROOT);
        ValidationNestReferenceCheck.check(root, List.of("customer"), OBJECT, FIELDS, side("source", Map.of()), Optional.empty(), builder);
        var cause = new Finding(Kind.SOURCE_FIELD_MISSING, new Location(root.root(), List.of("customer_id")));
        builder.notCompared(Check.PARTICIPATION, new Location(root.root(), List.of()), cause).checked(Check.IDENTITY);
        assertThat(builder.finish().reads()).hasSize(1);
    }

    @Test void absentTargetRootIsNotMistakenForAnOmittedObjectFieldAndStaleCallbacksAbort() {
        var root = observation(List.of(), 1);
        var missingRoot = builder(root, TargetPresence.ABSENT);
        assertThatThrownBy(() -> ValidationNestReferenceCheck.check(root, List.of("customer"), OBJECT, FIELDS,
                side("source", nullable("customer_id")), Optional.empty(), missingRoot)).isInstanceOf(TapstateException.class);
        assertThatThrownBy(missingRoot::finish).isInstanceOf(IllegalStateException.class);
        var stale = builder(root, TargetPresence.PRESENT);
        assertThatThrownBy(() -> ValidationNestReferenceCheck.check(observation(List.of(), 2), List.of("customer"), OBJECT, FIELDS,
                side("source", nullable("customer_id")), Optional.of(side("target", Map.of())), stale)).isInstanceOf(TapstateException.class);
    }

    @Test void openParentGroupsAndResourceFailuresCannotBecomeOrdinaryMissingReferenceFindings() {
        var root = observation(List.of(), 1);
        var source = side("source", Map.of());
        var open = new ValidationCheckEvent.Side(source.identity(), source.row(), 1, ValidationCheckEvent.GroupStatus.OPEN, source.boundary());
        var builder = builder(root, TargetPresence.PRESENT);
        assertThatThrownBy(() -> ValidationNestReferenceCheck.check(root, List.of("customer"), OBJECT, FIELDS,
                open, Optional.of(side("target", Map.of())), builder)).isInstanceOf(TapstateException.class);
        var bounded = builder(root, TargetPresence.PRESENT);
        var fields = new ValidationReferenceFields(List.of("missing"), new ValidationValues.Limits(1, 1, 1));
        assertThatThrownBy(() -> ValidationNestReferenceCheck.check(root, List.of("customer"), OBJECT, fields,
                source, Optional.of(side("target", Map.of())), bounded)).isInstanceOf(TapstateException.class);
        assertThatThrownBy(bounded::finish).isInstanceOf(IllegalStateException.class);
    }

    @Test void workingReferenceFieldsDoNotReplaceRawDiagnosticRowsOrPhysicalIdentity() {
        var observation = observation(List.of(), 1);
        var source = side("source", Map.of("raw_customer", 19));
        var builder = builder(observation, TargetPresence.PRESENT);
        var complete = ValidationNestReferenceCheck.check(observation, List.of("customer"), OBJECT, FIELDS,
                source, ValidationValues.ownRow(Map.of("customer_id", 19), ROW), Optional.empty(), builder);
        assertThat(((ValidationReferenceFields.Complete) complete).components()).containsExactly(19);
        builder.close();

        var missing = builder(observation, TargetPresence.PRESENT);
        ValidationNestReferenceCheck.check(observation, List.of("customer"), OBJECT, FIELDS, source,
                ValidationValues.ownRow(Map.of("dropped", 19), ROW), Optional.of(side("target", Map.of())), missing);
        assertThat(finish(missing).samples()).singleElement().satisfies(sample -> {
            assertThat(sample.source().orElseThrow().row()).isSameAs(source.row());
            assertThat(sample.source().orElseThrow().identity()).isEqualTo(source.identity());
            assertThat(sample.source().orElseThrow().row().fields()).containsEntry("raw_customer", 19).doesNotContainKey("dropped");
        });
    }

    private static ValidationRootCheck run(ValidationNestReferenceCheck.Shape shape, Map<String, ?> source, Map<String, ?> target) {
        var root = observation(List.of(), 1);
        var builder = builder(root, TargetPresence.PRESENT);
        assertThat(ValidationNestReferenceCheck.check(root, List.of("customer"), shape, FIELDS,
                side("source", source), Optional.of(side("target", target)), builder)).isEqualTo(ValidationReferenceFields.NoReference.INSTANCE);
        return finish(builder);
    }
    private static Builder builder(ValidationObservation root, TargetPresence target) {
        return ValidationRootCheck.builder(root, ValidationMethod.FULL_COLUMN, ValidationCheckEvent.Participation.SHOULD_EXIST, target, ROOT);
    }
    private static ValidationRootCheck finish(Builder builder) {
        for (Check check : Check.values()) builder.checked(check);
        return builder.finish();
    }
    private static ValidationCheckEvent.Side side(String name, Map<String, ?> fields) {
        return new ValidationCheckEvent.Side(new ValidationPhysicalIdentity(name, "database", null, "table", List.of(List.of("id")), key(102)),
                ValidationValues.ownRow(fields, ROW), 1, ValidationCheckEvent.GroupStatus.COMPLETE,
                new ValidationCheckEvent.ReadBoundary(name, "complete-v1", Instant.EPOCH, Instant.EPOCH));
    }
    private static Map<String, Object> nullable(String field) {
        var fields = new HashMap<String, Object>(); fields.put(field, null); return fields;
    }
    private static ValidationObservation observation(List<ValidationRoot.Ancestor> ancestors, long revision) {
        return new ValidationObservation(new ValidationRunIdentity(new ValidationOwner("cluster", "pipeline", "node", "boot", 1, 1, 0),
                "task", "run", ValidationMode.FULL, "spec", 1, "rules"),
                new ValidationRoot("mapping", "output", key(102), ancestors), revision);
    }
    private static ValidationKey key(long value) { return new ValidationKey(List.of(ValidationKey.Part.number(value))); }
}
