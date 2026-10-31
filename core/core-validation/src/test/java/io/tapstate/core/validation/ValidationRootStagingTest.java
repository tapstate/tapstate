package io.tapstate.core.validation;

import io.tapstate.core.common.TapstateException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import static io.tapstate.core.validation.ValidationRootCheck.*;
import static org.assertj.core.api.Assertions.*;

class ValidationRootStagingTest {
    private static final Limits LIMITS = new Limits(1024 * 1024, 100, 100, 20, 4);

    @Test void sourceFactsAndTheirBlockedScopesSurviveBindingTheFinalOutcome() {
        try (var root = pending()) {
            var location = new Location(observation().root(), List.of("customer"));
            var cause = new Finding(Kind.SOURCE_FIELD_MISSING, new Location(observation().root(), List.of("customer_id")));
            root.read(Side.SOURCE, boundary("source"));
            root.finding(cause).notCompared(Check.STRUCTURE, location, cause).notCompared(Check.VALUES, location, cause);
            root.bindOutcome(ValidationCheckEvent.Participation.SHOULD_EXIST, TargetPresence.PRESENT).read(Side.TARGET, boundary("target"));
            for (Check check : Check.values()) root.checked(check);
            var result = root.finish();
            assertThat(result.participation()).isEqualTo(ValidationCheckEvent.Participation.SHOULD_EXIST);
            assertThat(result.findings()).containsEntry(cause, 1L);
            assertThat(result.notCompared()).hasSize(2);
            assertThat(result.checks()).containsEntry(Check.STRUCTURE, Status.BLOCKED).containsEntry(Check.VALUES, Status.BLOCKED);
        }
    }

    @Test void settledExclusionDoesNotBecomeTargetMissingAndUncertaintyCanRemainExplicit() {
        try (var root = pending()) {
            root.read(Side.SOURCE, boundary("source")).bindOutcome(ValidationCheckEvent.Participation.SHOULD_NOT_EXIST, TargetPresence.ABSENT)
                    .read(Side.TARGET, boundary("target")).checked(Check.IDENTITY).checked(Check.PARTICIPATION)
                    .notApplicable(Check.STRUCTURE).notApplicable(Check.VALUES);
            assertThat(root.finish().observedAgreement()).isTrue();
        }
        try (var root = pending()) {
            var location = new Location(observation().root(), List.of());
            var cause = new Finding(Kind.SOURCE_FIELD_INVALID, new Location(observation().root(), List.of("active")));
            root.read(Side.SOURCE, boundary("source")).finding(cause).notCompared(Check.STRUCTURE, location, cause)
                    .notCompared(Check.VALUES, location, cause).bindOutcome(ValidationCheckEvent.Participation.UNKNOWN, TargetPresence.NOT_READ)
                    .notCompared(Check.PARTICIPATION, location, cause).checked(Check.IDENTITY);
            var result = root.finish();
            assertThat(result.targetPresence()).isEqualTo(TargetPresence.NOT_READ);
            assertThat(result.observedAgreement()).isFalse();
            assertThat(result.reads()).hasSize(1);
        }
    }

    @Test void sourceOnlyPhaseRejectsTargetReadsFindingsAndPrematureCompletion() {
        for (int action = 0; action < 5; action++) {
            try (var root = pending()) {
                root.read(Side.SOURCE, boundary("source"));
                int selected = action;
                assertThatThrownBy(() -> {
                    switch (selected) {
                        case 0 -> root.read(Side.TARGET, boundary("target"));
                        case 1 -> root.finding(new Finding(Kind.TARGET_MISSING, new Location(observation().root(), List.of())));
                        case 2 -> root.checked(Check.IDENTITY);
                        case 3 -> root.notApplicable(Check.VALUES);
                        default -> root.finish();
                    }
                }).isInstanceOf(TapstateException.class);
                assertThatThrownBy(root::finish).isInstanceOf(TapstateException.class);
            }
        }
    }

    @Test void theSourcePhaseCannotSetRootParticipationBeforeAllSourceFactsSettle() {
        try (var root = pending()) {
            var location = new Location(observation().root(), List.of());
            var cause = new Finding(Kind.SOURCE_FIELD_INVALID, location);
            root.read(Side.SOURCE, boundary("source")).finding(cause);
            assertThatThrownBy(() -> root.notCompared(Check.PARTICIPATION, location, cause)).isInstanceOf(TapstateException.class);
        }
    }

    @Test void bindingRequiresASourceReadAndIsSingleUseForPendingBuildersOnly() {
        try (var root = pending()) {
            assertThatThrownBy(() -> root.bindOutcome(ValidationCheckEvent.Participation.SHOULD_EXIST, TargetPresence.PRESENT)).isInstanceOf(TapstateException.class);
        }
        try (var root = pending()) {
            root.read(Side.SOURCE, boundary("source")).bindOutcome(ValidationCheckEvent.Participation.SHOULD_EXIST, TargetPresence.PRESENT);
            assertThatThrownBy(() -> root.bindOutcome(ValidationCheckEvent.Participation.SHOULD_NOT_EXIST, TargetPresence.ABSENT)).isInstanceOf(TapstateException.class);
        }
        try (var fixed = ValidationRootCheck.builder(observation(), ValidationMethod.FULL_COLUMN,
                ValidationCheckEvent.Participation.SHOULD_EXIST, TargetPresence.PRESENT, LIMITS)) {
            fixed.read(Side.SOURCE, boundary("source"));
            assertThatThrownBy(() -> fixed.bindOutcome(ValidationCheckEvent.Participation.SHOULD_NOT_EXIST, TargetPresence.ABSENT)).isInstanceOf(TapstateException.class);
        }
    }

    @Test void targetAbsenceStillNeedsItsActualReadAndTheRequiredRootFinding() {
        try (var root = pending()) {
            root.read(Side.SOURCE, boundary("source")).bindOutcome(ValidationCheckEvent.Participation.SHOULD_EXIST, TargetPresence.ABSENT);
            root.finding(new Finding(Kind.TARGET_MISSING, new Location(observation().root(), List.of())))
                    .checked(Check.IDENTITY).checked(Check.PARTICIPATION).notApplicable(Check.STRUCTURE).notApplicable(Check.VALUES);
            assertThatThrownBy(root::finish).isInstanceOf(TapstateException.class);
        }
        try (var root = pending()) {
            root.read(Side.SOURCE, boundary("source")).bindOutcome(ValidationCheckEvent.Participation.SHOULD_EXIST, TargetPresence.ABSENT)
                    .read(Side.TARGET, boundary("target")).checked(Check.IDENTITY).checked(Check.PARTICIPATION)
                    .notApplicable(Check.STRUCTURE).notApplicable(Check.VALUES);
            assertThatThrownBy(root::finish).isInstanceOf(TapstateException.class);
        }
    }

    @Test void sourceSamplesKeepTheirOriginalReadIdentityAcrossThePhaseBoundary() {
        var row = ValidationValues.ownRow(Map.of("id", 7), new ValidationValues.Limits(8192, 100, 16));
        var sample = new SampledSide(new ValidationPhysicalIdentity("source", "db", null, "table", List.of(List.of("id")), key()), row, "source");
        try (var root = pending()) {
            var location = new Location(observation().root(), List.of("missing"));
            var cause = new Finding(Kind.SOURCE_FIELD_MISSING, location);
            root.read(Side.SOURCE, boundary("source")).finding(cause, Optional.of(sample), Optional.empty())
                    .notCompared(Check.STRUCTURE, location, cause).notCompared(Check.VALUES, location, cause)
                    .bindOutcome(ValidationCheckEvent.Participation.SHOULD_EXIST, TargetPresence.PRESENT).read(Side.TARGET, boundary("target"));
            for (Check check : Check.values()) root.checked(check);
            assertThat(root.finish().samples()).singleElement().satisfies(retained -> assertThat(retained.source()).contains(sample));
        }
    }

    @Test void unfinishedSourceSamplesRequireTheirOwnCompletedReadEvenAfterSampleExhaustion() {
        for (int samples : List.of(0, 1)) {
            var cause = new Finding(Kind.SOURCE_FIELD_MISSING, new Location(observation().root(), List.of("required")));
            var raw = ValidationValues.ownRow(Map.of("id", 7), new ValidationValues.Limits(8192, 100, 16));
            var sample = new SampledSide(new ValidationPhysicalIdentity("source", "db", null, "table", List.of(List.of("id")), key()), raw, "unfinished");
            try (var root = ValidationRootCheck.pending(observation(), ValidationMethod.FULL_COLUMN,
                    new Limits(1024 * 1024, 100, 100, 20, samples))) {
                root.sourceFinding(cause, Optional.of(sample)).read(Side.SOURCE, boundary("unrelated"));
                assertThatThrownBy(() -> root.bindOutcome(ValidationCheckEvent.Participation.UNKNOWN, TargetPresence.NOT_READ))
                        .isInstanceOf(TapstateException.class);
            }
            try (var root = ValidationRootCheck.pending(observation(), ValidationMethod.FULL_COLUMN,
                    new Limits(1024 * 1024, 100, 100, 20, samples))) {
                root.sourceFinding(cause, Optional.of(sample)).read(Side.SOURCE, boundary("unfinished"))
                        .bindOutcome(ValidationCheckEvent.Participation.UNKNOWN, TargetPresence.NOT_READ).checked(Check.IDENTITY);
                for (var check : List.of(Check.PARTICIPATION, Check.STRUCTURE, Check.VALUES)) root.notCompared(check, new Location(observation().root(), List.of()), cause);
                var result = root.finish();
                assertThat(result.samples()).hasSize(samples);
                assertThat(result.samplesTruncated()).isEqualTo(samples == 0);
            }
        }
    }

    @Test void pendingSourceDiagnosticsCannotBeAddedToABoundOutcomeOrContainTargetFindings() {
        try (var root = pending()) {
            assertThatThrownBy(() -> root.sourceFinding(new Finding(Kind.TARGET_EXTRA, new Location(observation().root(), List.of())), Optional.empty()))
                    .isInstanceOf(TapstateException.class);
        }
        try (var root = pending()) {
            root.read(Side.SOURCE, boundary("source")).bindOutcome(ValidationCheckEvent.Participation.UNKNOWN, TargetPresence.NOT_READ);
            assertThatThrownBy(() -> root.sourceFinding(new Finding(Kind.SOURCE_FIELD_INVALID, new Location(observation().root(), List.of())), Optional.empty()))
                    .isInstanceOf(TapstateException.class);
        }
    }

    private static Builder pending() { return ValidationRootCheck.pending(observation(), ValidationMethod.FULL_COLUMN, LIMITS); }
    private static ValidationCheckEvent.ReadBoundary boundary(String id) { return new ValidationCheckEvent.ReadBoundary(id, "complete", Instant.EPOCH, Instant.EPOCH); }
    private static ValidationObservation observation() {
        return new ValidationObservation(new ValidationRunIdentity(new ValidationOwner("cluster", "pipeline", "node", "boot", 1, 1, 0),
                "task", "run", ValidationMode.FULL, "spec", 1, "rules"), new ValidationRoot("mapping", "output", key(), List.of()), 1);
    }
    private static ValidationKey key() { return new ValidationKey(List.of(ValidationKey.Part.number(7))); }
}
