package io.tapstate.core.validation;

import io.tapstate.core.common.TapstateException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class ValidationCheckEventTest {
    @Test void sourceOnlyDoesNotClaimMissingTargetBeforeParticipationIsKnown() {
        var source = side("source", "source_id", 1, ValidationCheckEvent.GroupStatus.COMPLETE);
        var event = new ValidationCheckEvent(observation(), key(), Optional.of(source), Optional.empty(),
                ValidationCheckEvent.Participation.UNKNOWN);
        assertThat(event.pairState()).isEqualTo(ValidationCheckEvent.PairState.SOURCE_ONLY);
        assertThat(event.participation()).isEqualTo(ValidationCheckEvent.Participation.UNKNOWN);
        assertThat(event.target()).isEmpty();
    }

    @Test void matchedAndTargetOnlyFactsKeepActualSidesAndDuplicateMultiplicity() {
        var source = side("source", "source_id", 2, ValidationCheckEvent.GroupStatus.COMPLETE);
        var target = side("destination", "renamed_id", 3, ValidationCheckEvent.GroupStatus.COMPLETE);
        var event = new ValidationCheckEvent(observation(), key(), Optional.of(source), Optional.of(target),
                ValidationCheckEvent.Participation.SHOULD_EXIST);
        assertThat(event.pairState()).isEqualTo(ValidationCheckEvent.PairState.MATCHED);
        assertThat(event.source().orElseThrow().observedOccurrences()).isEqualTo(2);
        assertThat(event.target().orElseThrow().observedOccurrences()).isEqualTo(3);
        assertThat(event.source().orElseThrow().row().fields()).containsKey("source_id").doesNotContainKey("renamed_id");
        assertThat(event.target().orElseThrow().row().fields()).containsKey("renamed_id").doesNotContainKey("source_id");
        assertThat(new ValidationCheckEvent(observation(), key(), Optional.empty(), Optional.of(target),
                ValidationCheckEvent.Participation.UNKNOWN).pairState())
                .isEqualTo(ValidationCheckEvent.PairState.TARGET_ONLY);
    }

    @Test void openGroupsStayIncompleteAndNoSideCannotBecomeAnEvent() {
        var open = side("source", "id", 1, ValidationCheckEvent.GroupStatus.OPEN);
        assertThat(new ValidationCheckEvent(observation(), key(), Optional.of(open), Optional.empty(),
                ValidationCheckEvent.Participation.UNKNOWN).groupsComplete()).isFalse();
        assertThatThrownBy(() -> new ValidationCheckEvent(observation(), key(), Optional.empty(), Optional.empty(),
                ValidationCheckEvent.Participation.UNKNOWN)).isInstanceOf(TapstateException.class);
    }

    @Test void physicalIdentityRequiresActualFieldArityAndUnambiguousPaths() {
        assertThatThrownBy(() -> new ValidationPhysicalIdentity("src", "db", null, "table", List.of(), key()))
                .isInstanceOf(TapstateException.class);
        var dotted = new ValidationPhysicalIdentity("src", "db", null, "table", List.of(List.of("a.b")), key());
        var nested = new ValidationPhysicalIdentity("src", "db", null, "table", List.of(List.of("a", "b")), key());
        assertThat(dotted).isNotEqualTo(nested);
    }

    private static ValidationCheckEvent.Side side(String source, String field, int occurrences,
                                                   ValidationCheckEvent.GroupStatus status) {
        return new ValidationCheckEvent.Side(
                new ValidationPhysicalIdentity(source, "database", null, "table", List.of(List.of(field)), key()),
                ValidationValues.ownRow(Map.of(field, 1), new ValidationValues.Limits(1024, 20, 4)), occurrences,
                status, new ValidationCheckEvent.ReadBoundary("read-" + source, "fresh-v1", Instant.EPOCH, Instant.EPOCH));
    }

    private static ValidationObservation observation() {
        return new ValidationObservation(new ValidationRunIdentity(
                new ValidationOwner("cluster", "pipeline", "node", "boot", 1, 1, 0),
                "task", "run", ValidationMode.FULL, "spec", 1, "rules-v1"),
                new ValidationRoot("mapping", "output", key(), List.of()), 1);
    }

    private static ValidationKey key() { return new ValidationKey(List.of(ValidationKey.Part.number(1))); }
}
