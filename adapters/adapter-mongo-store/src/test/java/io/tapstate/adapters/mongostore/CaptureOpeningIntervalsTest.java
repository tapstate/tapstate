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
    @Test void malformedCountersRangesAndAllocationCoordinatesNeverPartitionIssuedSpace() {
        for(long[] bounds:List.of(new long[]{0,1},new long[]{2,1},new long[]{1,Long.MAX_VALUE}))
            assertThatThrownBy(() -> new CaptureOpeningIntervals.Range(bounds[0],bounds[1])).isInstanceOf(io.tapstate.core.common.TapstateException.class);
        for(long[] counters:List.of(new long[]{-1,0},new long[]{Long.MAX_VALUE,0},new long[]{0,-1},new long[]{1,2}))
            assertThatThrownBy(() -> new CaptureOpeningIntervals.State(counters[0],counters[1],List.of(),List.of())).isInstanceOf(io.tapstate.core.common.TapstateException.class);
        assertThatThrownBy(() -> new CaptureOpeningIntervals.State(1,0,List.of(),List.of(new Document("sequence",1)))).isInstanceOf(io.tapstate.core.common.TapstateException.class);
        assertThatThrownBy(() -> new CaptureOpeningIntervals.State(2,0,List.of(),List.of(new Document("sequence",1L)))).isInstanceOf(io.tapstate.core.common.TapstateException.class);
        var full=new java.util.ArrayList<Document>();for(long n=1;n<=128;n++)full.add(new Document("sequence",n));
        var state=new CaptureOpeningIntervals.State(128,0,List.of(),full);
        assertThatThrownBy(() -> CaptureOpeningIntervals.append(state,128,new Document("sequence",129L))).isInstanceOf(IllegalStateException.class);
        full.add(new Document("sequence",129L));
        assertThatThrownBy(() -> new CaptureOpeningIntervals.State(129,0,List.of(),full)).isInstanceOf(io.tapstate.core.common.TapstateException.class);
        var ranges=java.util.Collections.nCopies(130,new CaptureOpeningIntervals.Range(2,2));
        assertThatThrownBy(() -> new CaptureOpeningIntervals.State(260,0,ranges,List.of())).isInstanceOf(io.tapstate.core.common.TapstateException.class);
        var exhausted=new CaptureOpeningIntervals.State(Long.MAX_VALUE-1,Long.MAX_VALUE-1,List.of(),List.of());
        assertThatThrownBy(() -> CaptureOpeningIntervals.append(exhausted,Long.MAX_VALUE-1,new Document("sequence",Long.MAX_VALUE))).isInstanceOf(io.tapstate.core.common.TapstateException.class);
        assertThatThrownBy(() -> CaptureOpeningIntervals.append(CaptureOpeningIntervals.empty(),0,new Document("sequence",1))).isInstanceOf(io.tapstate.core.common.TapstateException.class);
        assertThatThrownBy(() -> CaptureOpeningIntervals.append(CaptureOpeningIntervals.empty(),0,new Document("sequence",2L))).isInstanceOf(io.tapstate.core.common.TapstateException.class);
    }
    @Test void storedPartitionKeysAndBsonTypesMustMatchExactly() {
        var valid=new CaptureOpeningIntervals.State(2,0,List.of(new CaptureOpeningIntervals.Range(2,2)),List.of(new Document("sequence",1L))).document();
        assertThat(CaptureOpeningIntervals.read(valid).document()).isEqualTo(valid);
        for(String damage:List.of("extra","issued-type","prefix-type","range-extra","from-type","through-type","missing-ranges")) {
            var row=validCopy(valid);var range=row.getList("closedRanges",Document.class).getFirst();
            switch(damage) {
                case "extra" -> row.put("extra",true);
                case "issued-type" -> row.put("issuedThrough",2);
                case "prefix-type" -> row.put("closedPrefixThrough",0);
                case "range-extra" -> range.put("extra",true);
                case "from-type" -> range.put("from",2);
                case "through-type" -> range.put("through",2);
                case "missing-ranges" -> row.put("closedRanges",null);
                default -> throw new AssertionError(damage);
            }
            var damaged=row;assertThatThrownBy(() -> CaptureOpeningIntervals.read(damaged)).isInstanceOf(io.tapstate.core.common.TapstateException.class);
        }
        assertThatThrownBy(() -> CaptureOpeningIntervals.read(null)).isInstanceOf(io.tapstate.core.common.TapstateException.class);
    }
    private static Document validCopy(Document valid) {
        return new Document(valid).append("closedRanges",valid.getList("closedRanges",Document.class).stream().map(Document::new).toList());
    }
}
