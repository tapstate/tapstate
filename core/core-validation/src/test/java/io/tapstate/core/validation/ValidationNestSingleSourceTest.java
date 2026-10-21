package io.tapstate.core.validation;

import io.tapstate.core.common.TapstateException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import static io.tapstate.core.validation.ValidationNestSingleSource.*;
import static io.tapstate.core.validation.ValidationRootCheck.*;
import static org.assertj.core.api.Assertions.*;

class ValidationNestSingleSourceTest {
    private static final ValidationValues.Limits ROW = new ValidationValues.Limits(8192, 100, 16);
    private static final Limits ROOT = new Limits(1024 * 1024, 100, 100, 20, 4);

    @Test void completeGatheringDistinguishesEmptyUniqueAndCompetingRows() {
        for (int count : List.of(0, 1, 2, 4)) {
            var observation = observation(List.of(), 1);
            var builder = builder();
            var result = check(rows(observation, count, 0), Relation.GATHER, Optional.empty(), location(observation),
                    affected(observation), List.of(), builder);
            assertThat(result.state()).isEqualTo(count == 0 ? State.EMPTY : count == 1 ? State.UNIQUE : State.BLOCKED);
            assertThat(result.unique().isPresent()).isEqualTo(count == 1);
            var root = finish(builder);
            if (count < 2) assertThat(root.findings()).isEmpty();
            else {
                assertThat(root.counts()).containsOnlyKeys(Kind.SOURCE_CARDINALITY);
                assertThat(root.findings().keySet()).singleElement().satisfies(finding ->
                        assertThat(finding.multiplicity()).contains(new Multiplicity(count, true)));
                assertThat(root.notCompared()).hasSize(4).allSatisfy(blocked ->
                        assertThat(blocked.location().fieldPath()).isIn(List.of("customer"), List.of("total")));
            }
        }
    }

    @Test void aNonNullReferenceWithNoMatchIsASourceProblemAndNeverATargetDeletion() {
        var observation = observation(List.of(), 1);
        var builder = builder();
        var result = check(rows(observation, 0, 0), Relation.REFERENCE, Optional.of(reference()), location(observation),
                affected(observation), List.of(), builder);
        assertThat(result.state()).isEqualTo(State.BLOCKED);
        assertThat(result.unique()).isEmpty();
        var root = finish(builder);
        assertThat(root.counts()).containsOnlyKeys(Kind.SOURCE_REFERENCE_MISSING);
        assertThat(root.samples()).singleElement().satisfies(sample -> {
            assertThat(sample.source().orElseThrow().identity()).isEqualTo(parent().identity());
            assertThat(sample.source().orElseThrow().row().fields()).containsExactlyInAnyOrderEntriesOf(parent().row().fields());
            assertThat(sample.source().orElseThrow().readId()).isEqualTo("parent");
            assertThat(sample.target()).isEmpty();
        });
        assertThat(root.notCompared()).hasSize(4);
    }

    @Test void aReferenceWithMultipleMatchesNeverChoosesAWinner() {
        var observation = observation(List.of(), 1);
        var builder = builder();
        var result = check(rows(observation, 2, 0), Relation.REFERENCE, Optional.of(reference()), location(observation),
                affected(observation), List.of(), builder);
        assertThat(result.state()).isEqualTo(State.BLOCKED);
        assertThat(result.unique()).isEmpty();
        assertThat(finish(builder).counts()).containsOnlyKeys(Kind.SOURCE_DUPLICATE);
    }

    @Test void uncertainParticipationDoesNotProveEmptyOrUniqueAndMultiplicityIsOnlyALowerBound() {
        for (int count : List.of(0, 1, 2)) {
            var observation = observation(List.of(), 1);
            var builder = builder();
            var cause = new Finding(Kind.SOURCE_FIELD_MISSING, new Location(observation.root(), List.of("active")));
            var result = check(rows(observation, count, 1), Relation.GATHER, Optional.empty(), location(observation),
                    affected(observation), List.of(cause), builder);
            assertThat(result.state()).isEqualTo(State.BLOCKED);
            assertThat(result.unique()).isEmpty();
            var root = finish(builder);
            assertThat(root.counts()).containsEntry(Kind.SOURCE_FIELD_MISSING, 1L);
            assertThat(root.counts().keySet()).allMatch(Kind::sourceProblem);
            if (count == 2) assertThat(root.findings().keySet()).anySatisfy(finding ->
                    assertThat(finding.multiplicity()).contains(new Multiplicity(2, false)));
            else assertThat(root.counts()).containsOnlyKeys(Kind.SOURCE_FIELD_MISSING);
        }
    }

    @Test void uniqueResultsKeepRawWorkingAndPhysicalFactsSeparate() {
        var observation = observation(List.of(), 1);
        var builder = builder();
        var result = check(rows(observation, 1, 0), Relation.GATHER, Optional.empty(), location(observation),
                affected(observation), List.of(), builder);
        var sample = result.unique().orElseThrow();
        assertThat(sample.original().fields()).containsEntry("amount", 9).doesNotContainKey("price");
        assertThat(sample.working().fields()).containsEntry("price", 9).doesNotContainKey("amount");
        assertThat(sample.physical()).contains(physical(0));
        // Cardinality alone does not complete the root's payload or identity obligations.
        assertThatThrownBy(builder::finish).isInstanceOf(TapstateException.class);
    }

    @Test void duplicateDiagnosticsKeepOriginalRowsAndCanOmitUnavailablePhysicalIdentity() {
        var observation = observation(List.of(), 1);
        var builder = builder();
        check(rows(observation, 2, 0), Relation.GATHER, Optional.empty(), location(observation), affected(observation), List.of(), builder);
        assertThat(finish(builder).samples()).singleElement().satisfies(sample -> {
            assertThat(sample.source().orElseThrow().row().fields()).containsEntry("amount", 9).doesNotContainKey("price");
            assertThat(sample.source().orElseThrow().identity()).isEqualTo(physical(0));
            assertThat(sample.source().orElseThrow().readId()).isEqualTo("relation");
        });
        var unknown = builder();
        try (var rows = new ValidationRelationRows(observation, "relation", new ValidationRelationRows.Limits(32 * 1024, ROW))) {
            var row = ValidationValues.ownRow(Map.of("value", 2), ROW);
            for (int i = 0; i < 2; i++) rows.accept(observation, "relation", ValidationRelationRows.Disposition.INCLUDED,
                    new ValidationRelationRows.Sample(row, row, Optional.empty()));
            rows.complete(observation, boundary("relation"));
            check(rows.finish(), Relation.GATHER, Optional.empty(), location(observation), affected(observation), List.of(), unknown);
        }
        var root = finish(unknown);
        assertThat(root.counts()).containsEntry(Kind.SOURCE_CARDINALITY, 1L);
        assertThat(root.samples()).isEmpty();
    }

    @Test void nestedParentScopeSurvivesAndForeignEmptyOrUniqueLocationsAbortTheRoot() {
        var parent = observation(List.of(new ValidationRoot.Ancestor(List.of("items"), key(11))), 1);
        var builder = builder();
        check(rows(parent, 2, 0), Relation.GATHER, Optional.empty(), location(parent), affected(parent), List.of(), builder);
        assertThat(finish(builder).findings().keySet()).allSatisfy(finding -> assertThat(finding.location().scope()).isEqualTo(parent.root()));
        for (int count : List.of(0, 1)) {
            var invalid = builder();
            assertThatThrownBy(() -> check(rows(parent, count, 0), Relation.GATHER, Optional.empty(), location(observation(List.of(), 1)),
                    affected(parent), List.of(), invalid)).isInstanceOf(TapstateException.class);
            assertThatThrownBy(invalid::finish).isInstanceOf(IllegalStateException.class);
            var wrongAffected = builder();
            assertThatThrownBy(() -> check(rows(parent, count, 0), Relation.GATHER, Optional.empty(), location(parent),
                    affected(observation(List.of(), 1)), List.of(), wrongAffected)).isInstanceOf(TapstateException.class);
            assertThatThrownBy(wrongAffected::finish).isInstanceOf(IllegalStateException.class);
        }
    }

    @Test void wrongRevisionAndSiblingUncertaintyCannotSupplyCurrentParentFacts() {
        var stale = builder();
        var old = observation(List.of(), 2);
        assertThatThrownBy(() -> check(rows(old, 1, 0), Relation.GATHER, Optional.empty(), location(old), affected(old), List.of(), stale))
                .isInstanceOf(TapstateException.class);
        assertThatThrownBy(stale::finish).isInstanceOf(IllegalStateException.class);
        var parent = observation(List.of(new ValidationRoot.Ancestor(List.of("items"), key(11))), 1);
        var sibling = observation(List.of(new ValidationRoot.Ancestor(List.of("items"), key(12))), 1);
        var builder = builder();
        var cause = new Finding(Kind.SOURCE_FIELD_MISSING, new Location(sibling.root(), List.of("active")));
        assertThatThrownBy(() -> check(rows(parent, 0, 1), Relation.GATHER, Optional.empty(), location(parent), affected(parent), List.of(cause), builder))
                .isInstanceOf(TapstateException.class);
        assertThatThrownBy(builder::finish).isInstanceOf(IllegalStateException.class);
    }

    @Test void referencesRequireProvenNonNullFieldsAndTheActualParentReadEvenWhenAMatchExists() {
        var observation = observation(List.of(), 1);
        var absentFields = builder();
        assertThatThrownBy(() -> check(rows(observation, 1, 0), Relation.REFERENCE, Optional.empty(), location(observation),
                affected(observation), List.of(), absentFields)).isInstanceOf(TapstateException.class);
        for (int count : List.of(0, 1, 2)) {
            var missingRead = ValidationRootCheck.builder(observation, ValidationMethod.FULL_COLUMN,
                    ValidationCheckEvent.Participation.SHOULD_EXIST, TargetPresence.PRESENT, ROOT);
            Throwable failure = catchThrowable(() -> check(rows(observation, count, 0), Relation.REFERENCE, Optional.of(reference()), location(observation),
                    affected(observation), List.of(), missingRead));
            assertThat(failure).isInstanceOf(TapstateException.class);
            assertThat(catchThrowable(missingRead::finish)).isSameAs(failure);
        }
    }

    @Test void uniqueKeyModeBlocksStructureWithoutClaimingPayloadComparison() {
        var observation = observation(List.of(), 1);
        var builder = ValidationRootCheck.builder(observation, ValidationMethod.UNIQUE_KEY,
                ValidationCheckEvent.Participation.SHOULD_EXIST, TargetPresence.PRESENT, ROOT).read(Side.TARGET, boundary("target"));
        check(rows(observation, 2, 0), Relation.GATHER, Optional.empty(), location(observation), affected(observation), List.of(), builder);
        builder.checked(Check.IDENTITY).checked(Check.PARTICIPATION).checked(Check.STRUCTURE);
        var root = builder.finish();
        assertThat(root.checks()).containsEntry(Check.VALUES, Status.NOT_APPLICABLE);
        assertThat(root.notCompared()).hasSize(2).allSatisfy(blocked -> assertThat(blocked.check()).isEqualTo(Check.STRUCTURE));
    }

    private static ValidationRelationRows.Result rows(ValidationObservation observation, int included, int unknown) {
        try (var rows = new ValidationRelationRows(observation, "relation", new ValidationRelationRows.Limits(32 * 1024, ROW))) {
            for (int i = 0; i < included + unknown; i++) {
                rows.accept(observation, "relation", i < included ? ValidationRelationRows.Disposition.INCLUDED : ValidationRelationRows.Disposition.INDETERMINATE,
                        new ValidationRelationRows.Sample(ValidationValues.ownRow(Map.of("id", i, "amount", 9), ROW),
                                ValidationValues.ownRow(Map.of("id", i, "price", 9), ROW), Optional.of(physical(i))));
            }
            rows.complete(observation, boundary("relation"));
            return rows.finish();
        }
    }
    private static Reference reference() {
        var fields = new ValidationReferenceFields(List.of("customer_id"), ROW).read(parent().row());
        return new Reference((ValidationReferenceFields.Complete) fields, parent());
    }
    private static SampledSide parent() {
        return new SampledSide(physical(102), ValidationValues.ownRow(Map.of("id", 102, "customer_id", 7), ROW), "parent");
    }
    private static Builder builder() {
        return ValidationRootCheck.builder(observation(List.of(), 1), ValidationMethod.FULL_COLUMN,
                ValidationCheckEvent.Participation.SHOULD_EXIST, TargetPresence.PRESENT, ROOT)
                .read(Side.SOURCE, boundary("parent")).read(Side.TARGET, boundary("target"));
    }
    private static ValidationRootCheck finish(Builder builder) {
        for (Check check : Check.values()) builder.checked(check);
        return builder.finish();
    }
    private static Location location(ValidationObservation observation) { return new Location(observation.root(), List.of("customer")); }
    private static List<Location> affected(ValidationObservation observation) {
        return List.of(location(observation), new Location(observation.root(), List.of("total")));
    }
    private static ValidationPhysicalIdentity physical(long value) {
        return new ValidationPhysicalIdentity("source", "database", null, "table", List.of(List.of("id")), key(value));
    }
    private static ValidationObservation observation(List<ValidationRoot.Ancestor> ancestors, long revision) {
        return new ValidationObservation(new ValidationRunIdentity(new ValidationOwner("cluster", "pipeline", "node", "boot", 1, 1, 0),
                "task", "run", ValidationMode.FULL, "spec", 1, "rules"), new ValidationRoot("mapping", "output", key(102), ancestors), revision);
    }
    private static ValidationKey key(long value) { return new ValidationKey(List.of(ValidationKey.Part.number(value))); }
    private static ValidationCheckEvent.ReadBoundary boundary(String id) {
        return new ValidationCheckEvent.ReadBoundary(id, "complete", Instant.EPOCH, Instant.EPOCH);
    }
}
