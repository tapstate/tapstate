package io.tapstate.core.validation;

import io.tapstate.core.common.TapstateException;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import static io.tapstate.core.validation.ValidationRelationRows.*;
import static org.assertj.core.api.Assertions.*;

class ValidationRelationRowsTest {
    private static final ValidationValues.Limits ROW = new ValidationValues.Limits(8192, 100, 16);
    private static final Limits LIMITS = new Limits(32 * 1024, ROW);

    @Test void aLargeCompletedRelationRetainsOnlyTwoParticipatingSamples() {
        var observation = observation(1);
        var sample = sample(Map.of("id", 1));
        try (var rows = new ValidationRelationRows(observation, "read", LIMITS)) {
            for (int i = 0; i < 20000; i++) rows.accept(observation, "read", Disposition.INCLUDED, sample);
            rows.complete(observation, boundary());
            var result = rows.finish();
            assertThat(result.included()).isEqualTo(20000);
            assertThat(result.includedSamples()).hasSize(2);
            assertThat(result.atLeastTwo()).isTrue();
            assertThat(result.participationComplete()).isTrue();
            assertThat(result.unique()).isEmpty();
            assertThat(result.accountedBytes()).isLessThan(32 * 1024);
        }
    }

    @Test void unknownParticipationCannotTurnIntoAnEmptyOrUniqueRelation() {
        var observation = observation(1);
        for (int included : List.of(0, 1, 2)) {
            try (var rows = new ValidationRelationRows(observation, "read", LIMITS)) {
                for (int i = 0; i < included; i++) rows.accept(observation, "read", Disposition.INCLUDED, sample(Map.of("id", i)));
                rows.accept(observation, "read", Disposition.INDETERMINATE, sample(Map.of("missing-required-field", true)));
                rows.accept(observation, "read", Disposition.EXCLUDED, sample(Map.of("id", 99)));
                rows.complete(observation, boundary());
                var result = rows.finish();
                assertThat(result.included()).isEqualTo(included);
                assertThat(result.excluded()).isEqualTo(1);
                assertThat(result.indeterminate()).isEqualTo(1);
                assertThat(result.definitelyEmpty()).isFalse();
                assertThat(result.unique()).isEmpty();
                assertThat(result.participationComplete()).isFalse();
                assertThat(result.atLeastTwo()).isEqualTo(included == 2);
            }
        }
    }

    @Test void excludedRowsDoNotAddMembersAndZeroRequiresCompleteQuery() {
        var observation = observation(1);
        try (var rows = new ValidationRelationRows(observation, "read", LIMITS)) {
            for (int i = 0; i < 50; i++) rows.accept(observation, "read", Disposition.EXCLUDED, sample(Map.of("id", i)));
            rows.complete(observation, boundary());
            var result = rows.finish();
            assertThat(result.definitelyEmpty()).isTrue();
            assertThat(result.excluded()).isEqualTo(50);
            assertThat(result.includedSamples()).isEmpty();
            assertThat(result.indeterminateSamples()).isEmpty();
        }
        var incomplete = new ValidationRelationRows(observation, "read", LIMITS);
        assertThatThrownBy(incomplete::finish).isInstanceOf(TapstateException.class);
    }

    @Test void uniqueSampleRetainsRawWorkingAndActualPhysicalIdentitySeparately() {
        var observation = observation(1);
        var data = new HashMap<String, Object>(); data.put("id", 1); data.put("amount", 9);
        var original = ValidationValues.ownRow(data, ROW);
        var working = ValidationValues.ownRow(Map.of("id", 1, "price", 9, "tag", "checked"), ROW);
        var physical = new ValidationPhysicalIdentity("connection", "database", null, "table", List.of(List.of("id")), key(1));
        try (var rows = new ValidationRelationRows(observation, "read", LIMITS)) {
            rows.accept(observation, "read", Disposition.INCLUDED, new Sample(original, working, Optional.of(physical)));
            data.put("amount", 900);
            rows.complete(observation, boundary());
            var sample = rows.finish().unique().orElseThrow();
            assertThat(sample.original().fields()).containsEntry("amount", 9).doesNotContainKey("price");
            assertThat(sample.working().fields()).containsEntry("price", 9).doesNotContainKey("amount");
            assertThat(sample.physical()).contains(physical);
        }
    }

    @Test void anotherQueryOrRevisionCannotSupplyRowsOrCompletion() {
        var observation = observation(1);
        for (boolean stale : List.of(true, false)) {
            var rows = new ValidationRelationRows(observation, "read", LIMITS);
            assertThatThrownBy(() -> rows.accept(stale ? observation(2) : observation, stale ? "read" : "other",
                    Disposition.INCLUDED, sample(Map.of("id", 1)))).isInstanceOf(TapstateException.class);
            assertThatThrownBy(rows::finish).isInstanceOf(TapstateException.class);
        }
        var wrongCompletion = new ValidationRelationRows(observation, "read", LIMITS);
        assertThatThrownBy(() -> wrongCompletion.complete(observation,
                new ValidationCheckEvent.ReadBoundary("other", "complete", Instant.EPOCH, Instant.EPOCH))).isInstanceOf(TapstateException.class);
    }

    @Test void completionIsIdempotentButLateRowsAndChangedBoundariesPoisonTheFrame() {
        var observation = observation(1);
        var rows = new ValidationRelationRows(observation, "read", LIMITS);
        rows.complete(observation, boundary()); rows.complete(observation, boundary());
        Throwable failure = catchThrowable(() -> rows.accept(observation, "read", Disposition.INCLUDED, sample(Map.of("id", 1))));
        assertThat(failure).isInstanceOf(TapstateException.class);
        assertThat(catchThrowable(rows::finish)).isSameAs(failure);
        var changed = new ValidationRelationRows(observation, "read", LIMITS);
        changed.complete(observation, boundary());
        assertThatThrownBy(() -> changed.complete(observation,
                new ValidationCheckEvent.ReadBoundary("read", "complete", Instant.EPOCH, Instant.EPOCH.plusSeconds(1))))
                .isInstanceOf(TapstateException.class);
    }

    @Test void discardedAndUnknownRowsStillRespectInputBudgetsAndClosingIsNotCompletion() {
        var observation = observation(1);
        for (Disposition disposition : Disposition.values()) {
            var rows = new ValidationRelationRows(observation, "read", new Limits(32 * 1024, new ValidationValues.Limits(1, 1, 1)));
            assertThatThrownBy(() -> rows.accept(observation, "read", disposition, sample(Map.of("id", 1))))
                    .isInstanceOfSatisfying(TapstateException.class, error -> assertThat(error.code()).isEqualTo(ValidationError.VALUE_LIMIT_EXCEEDED));
            assertThatThrownBy(rows::finish).isInstanceOf(TapstateException.class);
        }
        var closed = new ValidationRelationRows(observation, "read", LIMITS);
        closed.close();
        assertThatThrownBy(closed::finish).isInstanceOf(IllegalStateException.class);
    }

    private static Sample sample(Map<String, ?> fields) {
        var row = ValidationValues.ownRow(fields, ROW);
        return new Sample(row, row, Optional.empty());
    }
    private static ValidationObservation observation(long revision) {
        return new ValidationObservation(new ValidationRunIdentity(new ValidationOwner("cluster", "pipeline", "node", "boot", 1, 1, 0),
                "task", "run", ValidationMode.FULL, "spec", 1, "rules"),
                new ValidationRoot("mapping", "output", key(102), List.of()), revision);
    }
    private static ValidationKey key(long value) { return new ValidationKey(List.of(ValidationKey.Part.number(value))); }
    private static ValidationCheckEvent.ReadBoundary boundary() {
        return new ValidationCheckEvent.ReadBoundary("read", "complete", Instant.EPOCH, Instant.EPOCH);
    }
}
