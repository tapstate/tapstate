package io.tapstate.core.validation;

import io.tapstate.core.common.TapstateException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

import static io.tapstate.core.validation.ValidationCheckEvent.Participation.*;
import static io.tapstate.core.validation.ValidationRootCheck.*;
import static org.assertj.core.api.Assertions.*;

class ValidationRootCheckTest {
    private static final Limits LIMITS = new Limits(1024 * 1024, 32, 32, 32, 2);

    @Test void completedAgreementKeepsOriginalIdentityAndReadFactsWithoutConfirmation() {
        var observation = observation(ValidationMode.FULL, 7);
        var builder = builder(observation, ValidationMethod.FULL_COLUMN, SHOULD_EXIST, TargetPresence.PRESENT, LIMITS);
        complete(builder);
        var result = builder.finish();
        assertThat(result.observation()).isEqualTo(observation);
        assertThat(result.observedAgreement()).isTrue();
        assertThat(result.checks()).containsOnlyKeys(Check.values());
        assertThat(result.reads()).containsExactly(new Read(Side.SOURCE, boundary("s")), new Read(Side.TARGET, boundary("t")));
        assertThatThrownBy(builder::finish).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> result.checks().clear()).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> result.reads().clear()).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test void partialRootAndClosedStagingCannotReturnSuccessfulObservations() {
        var partial = basic(SHOULD_EXIST, TargetPresence.PRESENT);
        partial.read(Side.SOURCE, boundary("s")).checked(Check.IDENTITY);
        Throwable first = catchThrowable(partial::finish);
        assertThat(first).isInstanceOf(TapstateException.class);
        assertThat(catchThrowable(partial::finish)).isSameAs(first);
        var abandoned = basic(SHOULD_EXIST, TargetPresence.PRESENT);
        abandoned.close();
        assertThatThrownBy(abandoned::finish).isInstanceOf(IllegalStateException.class);
    }

    @Test void countChildFragmentAndUnsupportedMethodModeCannotPretendToBeRootChecks() {
        var observation = observation(ValidationMode.FULL, 1);
        assertThatThrownBy(() -> builder(observation, ValidationMethod.COUNT, SHOULD_EXIST, TargetPresence.PRESENT, LIMITS))
                .isInstanceOf(TapstateException.class);
        var nested = new ValidationObservation(observation.run(), childScope(11), 1);
        assertThatThrownBy(() -> builder(nested, ValidationMethod.FULL_COLUMN, SHOULD_EXIST, TargetPresence.PRESENT, LIMITS))
                .isInstanceOf(TapstateException.class);
        assertThatThrownBy(() -> builder(observation(ValidationMode.INCREMENTAL, 1), ValidationMethod.UNIQUE_KEY,
                SHOULD_EXIST, TargetPresence.PRESENT, LIMITS)).isInstanceOf(TapstateException.class);
    }

    @Test void boundedSamplesDoNotEraseSourceProblemsCountsOrUncheckedScopes() {
        var builder = basic(SHOULD_EXIST, TargetPresence.PRESENT);
        complete(builder);
        Finding problem = new Finding(Kind.SOURCE_FIELD_MISSING, location(childScope(11), "customer_id"));
        var mutable = new java.util.HashMap<String, Object>();
        mutable.put("nullable", null);
        var sample = ValidationValues.ownRow(mutable, new ValidationValues.Limits(1024, 20, 4));
        for (int i = 0; i < 10; i++) builder.finding(problem, Optional.of(sample), Optional.empty());
        builder.notCompared(Check.VALUES, location(childScope(11), "customer"), problem).checked(Check.VALUES);
        mutable.put("nullable", 19);
        var result = builder.finish();
        assertThat(result.findings()).containsEntry(problem, 10L);
        assertThat(result.counts()).containsEntry(Kind.SOURCE_FIELD_MISSING, 10L);
        assertThat(result.samples()).hasSize(2);
        assertThat(result.samplesTruncated()).isTrue();
        assertThat(result.samples().getFirst().source().orElseThrow().fields()).containsEntry("nullable", null);
        assertThat(result.checks()).containsEntry(Check.VALUES, Status.BLOCKED);
        assertThat(result.notCompared()).containsExactly(new Blocked(Check.VALUES, location(childScope(11), "customer"), problem));
        assertThat(result.observedAgreement()).isFalse();
    }

    @Test void unknownParticipationIsExplicitlyBlockedAndNeverRequiresInventedTargetFacts() {
        var builder = basic(UNKNOWN, TargetPresence.NOT_READ);
        Finding cause = new Finding(Kind.SOURCE_FIELD_MISSING, location(root(), "customer_id"));
        builder.read(Side.SOURCE, boundary("s")).checked(Check.IDENTITY).finding(cause);
        builder.notCompared(Check.PARTICIPATION, location(root()), cause)
                .notCompared(Check.STRUCTURE, location(root()), cause).notCompared(Check.VALUES, location(root()), cause);
        var result = builder.finish();
        assertThat(result.participation()).isEqualTo(UNKNOWN);
        assertThat(result.targetPresence()).isEqualTo(TargetPresence.NOT_READ);
        assertThat(result.reads()).hasSize(1);
        assertThat(result.observedAgreement()).isFalse();
    }

    @Test void unknownParticipationCannotClaimTargetDifferenceOrCompleteParticipation() {
        var invalid = basic(UNKNOWN, TargetPresence.PRESENT);
        complete(invalid);
        assertThatThrownBy(invalid::finish).isInstanceOf(TapstateException.class);
        var blocked = basic(UNKNOWN, TargetPresence.PRESENT);
        complete(blocked);
        Finding cause = new Finding(Kind.SOURCE_FIELD_MISSING, location(root(), "customer_id"));
        blocked.finding(cause).notCompared(Check.PARTICIPATION, location(root()), cause);
        blocked.finding(new Finding(Kind.TARGET_VALUE, location(root(), "customer")));
        assertThatThrownBy(blocked::finish).isInstanceOf(TapstateException.class);
    }

    @Test void missingOrExtraWholeRootsRequireTheirOwnPresenceFinding() {
        for (var participation : List.of(SHOULD_EXIST, SHOULD_NOT_EXIST)) {
            var presence = participation == SHOULD_EXIST ? TargetPresence.ABSENT : TargetPresence.PRESENT;
            var missing = presenceOnly(participation, presence);
            assertThatThrownBy(missing::finish).isInstanceOf(TapstateException.class);
            var valid = presenceOnly(participation, presence);
            Kind kind = participation == SHOULD_EXIST ? Kind.TARGET_MISSING : Kind.TARGET_EXTRA;
            valid.finding(new Finding(kind, location(root())));
            assertThat(valid.finish().observedAgreement()).isFalse();
            var childOnly = presenceOnly(participation, presence).finding(new Finding(kind, location(childScope(11))));
            assertThatThrownBy(childOnly::finish).isInstanceOf(TapstateException.class);
        }
        assertThat(presenceOnly(SHOULD_NOT_EXIST, TargetPresence.ABSENT).finish().observedAgreement()).isTrue();
    }

    @Test void presentParticipatingRootsCannotSkipStructureOrValues() {
        var invalid = presenceOnly(SHOULD_EXIST, TargetPresence.PRESENT);
        assertThatThrownBy(invalid::finish).isInstanceOf(TapstateException.class);
        var unknown = presenceOnly(UNKNOWN, TargetPresence.PRESENT);
        assertThatThrownBy(unknown::finish).isInstanceOf(TapstateException.class);
    }

    @Test void unknownRootParticipationBlocksDependentContentEvenWithoutTargetDifferences() {
        var builder = basic(UNKNOWN, TargetPresence.PRESENT);
        complete(builder);
        var cause = new Finding(Kind.SOURCE_FIELD_MISSING, location(root(), "required"));
        builder.finding(cause).notCompared(Check.PARTICIPATION, location(root()), cause);
        assertThatThrownBy(builder::finish).isInstanceOf(TapstateException.class);
    }

    @Test void absentRootsCannotCarryContentConclusions() {
        var builder = presenceOnly(SHOULD_EXIST, TargetPresence.ABSENT);
        builder.finding(new Finding(Kind.TARGET_MISSING, location(root())))
                .finding(new Finding(Kind.TARGET_VALUE, location(root(), "name")));
        assertThatThrownBy(builder::finish).isInstanceOf(TapstateException.class);
    }

    @Test void uniqueKeyDoesNotClaimFullColumnChecks() {
        var builder = builder(observation(ValidationMode.FULL, 1), ValidationMethod.UNIQUE_KEY,
                SHOULD_EXIST, TargetPresence.PRESENT, LIMITS);
        builder.read(Side.SOURCE, boundary("s")).read(Side.TARGET, boundary("t"))
                .checked(Check.IDENTITY).checked(Check.PARTICIPATION).checked(Check.STRUCTURE);
        assertThat(builder.finish().checks()).containsEntry(Check.VALUES, Status.NOT_APPLICABLE);
        var invalid = builder(observation(ValidationMode.FULL, 1), ValidationMethod.UNIQUE_KEY,
                SHOULD_EXIST, TargetPresence.PRESENT, LIMITS);
        assertThatThrownBy(() -> invalid.finding(new Finding(Kind.TARGET_VALUE, location(root(), "name"))))
                .isInstanceOf(TapstateException.class);
    }

    @Test void ancestorsIsolateReusedElementKeysAndKeepOriginalOrdinals() {
        var builder = basic(SHOULD_EXIST, TargetPresence.PRESENT);
        complete(builder);
        var path = new ArrayList<Object>(List.of("splits", 1, "amount"));
        Finding first = new Finding(Kind.TARGET_VALUE, new Location(childScope(11), path));
        Finding second = new Finding(Kind.TARGET_VALUE, new Location(childScope(12), path));
        builder.finding(first).finding(second);
        path.clear();
        var result = builder.finish();
        assertThat(result.findings()).hasSize(2).containsEntry(first, 1L).containsEntry(second, 1L);
        assertThat(first.location().fieldPath()).containsExactly("splits", 1, "amount");
        assertThat(result.counts()).containsEntry(Kind.TARGET_VALUE, 2L);
    }

    @Test void foreignRootAndUnobservedOrTargetBlockersCannotBeSmuggledIntoResults() {
        var otherRoot = new ValidationRoot("mapping", "output", key(999), List.of());
        assertThatThrownBy(() -> basic(SHOULD_EXIST, TargetPresence.PRESENT)
                .finding(new Finding(Kind.TARGET_EXTRA, location(otherRoot)))).isInstanceOf(TapstateException.class);
        Finding cause = new Finding(Kind.SOURCE_FIELD_MISSING, location(root(), "missing"));
        assertThatThrownBy(() -> basic(SHOULD_EXIST, TargetPresence.PRESENT)
                .notCompared(Check.VALUES, location(root()), cause)).isInstanceOf(TapstateException.class);
        Finding target = new Finding(Kind.TARGET_MISSING, location(root()));
        assertThatThrownBy(() -> new Blocked(Check.VALUES, location(root()), target)).isInstanceOf(TapstateException.class);
    }

    @Test void identicalReadIdsAreIdempotentOnlyForIdenticalFactsOnTheSameSide() {
        var builder = basic(SHOULD_EXIST, TargetPresence.PRESENT);
        complete(builder);
        builder.read(Side.SOURCE, boundary("s")).read(Side.TARGET, boundary("s"));
        assertThat(builder.finish().reads()).hasSize(3);
        var conflict = basic(SHOULD_EXIST, TargetPresence.PRESENT).read(Side.SOURCE, boundary("s"));
        assertThatThrownBy(() -> conflict.read(Side.SOURCE,
                new ValidationCheckEvent.ReadBoundary("s", "fresh", Instant.EPOCH, Instant.EPOCH.plusSeconds(1))))
                .isInstanceOf(TapstateException.class);
        assertThatThrownBy(conflict::finish).isInstanceOf(TapstateException.class);
    }

    @Test void hardBudgetsPoisonStagingWhileZeroSampleBudgetOnlyDropsDiagnostics() {
        var builder = builder(observation(ValidationMode.FULL, 1), ValidationMethod.FULL_COLUMN,
                SHOULD_EXIST, TargetPresence.PRESENT, new Limits(1024 * 1024, 1, 1, 2, 0));
        complete(builder);
        var sample = ValidationValues.ownRow(Map.of("v", 1), new ValidationValues.Limits(1024, 10, 4));
        builder.finding(new Finding(Kind.TARGET_VALUE, location(root(), "a")), Optional.of(sample), Optional.empty());
        var result = builder.finish();
        assertThat(result.samples()).isEmpty();
        assertThat(result.samplesTruncated()).isTrue();
        assertThat(result.observedAgreement()).isFalse();

        var bounded = builder(observation(ValidationMode.FULL, 1), ValidationMethod.FULL_COLUMN,
                SHOULD_EXIST, TargetPresence.PRESENT, new Limits(1024 * 1024, 1, 1, 2, 0));
        bounded.finding(new Finding(Kind.TARGET_VALUE, location(root(), "a")));
        Throwable failure = catchThrowable(() -> bounded.finding(new Finding(Kind.TARGET_VALUE, location(root(), "b"))));
        assertThat(failure).isInstanceOfSatisfying(TapstateException.class,
                error -> assertThat(error.code()).isEqualTo(ValidationError.VALUE_LIMIT_EXCEEDED));
        assertThat(catchThrowable(bounded::finish)).isSameAs(failure);
        assertThatThrownBy(() -> builder(observation(ValidationMode.FULL, 1), ValidationMethod.FULL_COLUMN,
                SHOULD_EXIST, TargetPresence.PRESENT, new Limits(1024, 1, 1, 1, 0))).isInstanceOf(TapstateException.class);
    }

    @Test void duplicateMultiplicityRetainsLowerBoundWithoutConfusingItWithFindingCount() {
        var builder = basic(SHOULD_EXIST, TargetPresence.PRESENT);
        complete(builder);
        var duplicate = new Finding(Kind.TARGET_DUPLICATE, location(childScope(11)), Optional.of(new Multiplicity(2, false)));
        builder.finding(duplicate);
        var result = builder.finish();
        assertThat(result.counts()).containsEntry(Kind.TARGET_DUPLICATE, 1L);
        assertThat(result.findings().keySet().iterator().next().multiplicity()).contains(new Multiplicity(2, false));
        assertThatThrownBy(() -> new Finding(Kind.SOURCE_DUPLICATE, location(root()))).isInstanceOf(TapstateException.class);
        assertThatThrownBy(() -> new Multiplicity(1, true)).isInstanceOf(TapstateException.class);
    }

    private static Builder basic(ValidationCheckEvent.Participation participation, TargetPresence presence) {
        return builder(observation(ValidationMode.FULL, 1), ValidationMethod.FULL_COLUMN, participation, presence, LIMITS);
    }
    private static void complete(Builder builder) {
        builder.read(Side.SOURCE, boundary("s")).read(Side.TARGET, boundary("t"));
        for (Check check : Check.values()) builder.checked(check);
    }
    private static Builder presenceOnly(ValidationCheckEvent.Participation participation, TargetPresence presence) {
        return basic(participation, presence).read(Side.SOURCE, boundary("s")).read(Side.TARGET, boundary("t"))
                .checked(Check.IDENTITY).checked(Check.PARTICIPATION).notApplicable(Check.STRUCTURE).notApplicable(Check.VALUES);
    }
    private static ValidationCheckEvent.ReadBoundary boundary(String id) {
        return new ValidationCheckEvent.ReadBoundary(id, "fresh", Instant.EPOCH, Instant.EPOCH);
    }
    private static Location location(ValidationRoot scope, Object... path) { return new Location(scope, List.of(path)); }
    private static ValidationRoot root() { return new ValidationRoot("mapping", "output", key(102), List.of()); }
    private static ValidationRoot childScope(int line) {
        return new ValidationRoot("mapping", "output", key(102), List.of(
                new ValidationRoot.Ancestor(List.of("lines"), key(line)),
                new ValidationRoot.Ancestor(List.of("splits"), key(501))));
    }
    private static ValidationKey key(int value) { return new ValidationKey(List.of(ValidationKey.Part.number(value))); }
    private static ValidationObservation observation(ValidationMode mode, long revision) {
        return new ValidationObservation(new ValidationRunIdentity(
                new ValidationOwner("cluster", "pipeline", "node", "boot", 1, 1, 0),
                "task", "run", mode, "spec", 1, "rules"), root(), revision);
    }
}
