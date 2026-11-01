package io.tapstate.adapters.mongostore;

import static org.assertj.core.api.Assertions.*;

import java.util.List;
import org.bson.Document;
import org.junit.jupiter.api.Test;

class CaptureOpeningIntervalsTest {
    @Test void aCrashedOpeningDoesNotRequireExitBeforeANewOwnerCanBeRecorded() {
        var old = new Document("sequence", 1L).append("openingId", "crashed");
        var next = new Document("sequence", 2L).append("openingId", "survivor");
        var state = CaptureOpeningIntervals.append(CaptureOpeningIntervals.empty(), 0, old);
        state = CaptureOpeningIntervals.append(state, 1, next);

        assertThat(state.unresolved()).containsExactly(old, next);
        assertThat(state.closedPrefixThrough()).isZero();
        state = CaptureOpeningIntervals.close(state, next);
        assertThat(state.unresolved()).containsExactly(old);
        assertThat(state.closedPrefixThrough()).as("the new owner does not close the old owner").isZero();
        assertThat(state.ranges()).containsExactly(new CaptureOpeningIntervals.Range(2, 2));
    }

    @Test void aPermanentOldHoleAndManyLaterCleanOwnersHaveBoundedHistory() {
        var lost = new Document("sequence", 1L).append("openingId", "unresolved");
        var state = CaptureOpeningIntervals.append(CaptureOpeningIntervals.empty(), 0, lost);
        for (long sequence = 2; sequence <= 10000; sequence++) {
            var current = new Document("sequence", sequence);
            state = CaptureOpeningIntervals.append(state, sequence - 1, current);
            state = CaptureOpeningIntervals.close(state, current);
        }
        assertThat(state.unresolved()).containsExactly(lost);
        assertThat(state.ranges()).containsExactly(new CaptureOpeningIntervals.Range(2, 10000));
        assertThat(state.closedPrefixThrough()).isZero();
        state = CaptureOpeningIntervals.close(state, lost);
        assertThat(state.closedPrefixThrough()).isEqualTo(10000);
        assertThat(state.unresolved()).isEmpty();
        assertThat(state.ranges()).isEmpty();
    }

    @Test void aLateOrDifferentCloseCannotEraseAnotherOpening() {
        var old = new Document("sequence", 1L).append("openingId", "old");
        var newer = new Document("sequence", 2L).append("openingId", "new");
        var state = CaptureOpeningIntervals.append(CaptureOpeningIntervals.empty(), 0, old);
        state = CaptureOpeningIntervals.append(state, 1, newer);
        state = CaptureOpeningIntervals.close(state, old);
        var closed = state;
        assertThat(CaptureOpeningIntervals.close(state, old)).isSameAs(closed);
        assertThat(CaptureOpeningIntervals.close(state, new Document("sequence", 2L).append("openingId", "wrong")))
                .isSameAs(closed);
        assertThat(state.unresolved()).containsExactly(newer);
        assertThatThrownBy(() -> CaptureOpeningIntervals.append(closed, 0, old)).isInstanceOf(io.tapstate.core.common.TapstateException.class);
    }

    @Test void missingOverlappingAndUnmergedPartitionsAreNeverProofOfClosure() {
        assertThatThrownBy(() -> new CaptureOpeningIntervals.State(2, 0, List.of(), List.of(new Document("sequence", 2L))))
                .isInstanceOf(io.tapstate.core.common.TapstateException.class);
        assertThatThrownBy(() -> new CaptureOpeningIntervals.State(2, 1, List.of(), List.of(new Document("sequence", 1L))))
                .isInstanceOf(io.tapstate.core.common.TapstateException.class);
        assertThatThrownBy(() -> new CaptureOpeningIntervals.State(2, 1, List.of(new CaptureOpeningIntervals.Range(2, 2)), List.of()))
                .isInstanceOf(io.tapstate.core.common.TapstateException.class);
    }

    @Test void aRoundTripRetainsEveryUncertainOpening() {
        var state = CaptureOpeningIntervals.append(CaptureOpeningIntervals.empty(), 0,
                new Document("sequence", 1L).append("openingId", "still-unknown"));
        assertThat(CaptureOpeningIntervals.read(state.document())).isEqualTo(state);
    }
}
