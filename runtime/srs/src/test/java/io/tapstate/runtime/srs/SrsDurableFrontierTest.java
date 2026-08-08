package io.tapstate.runtime.srs;

import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.spi.store.ConsumerOffset;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The durable-frontier bound on a source-read-offset advance: the persisted offset must never pass the
 * slowest consumer's sink-acked source position, so every change past the offset is still re-minable from
 * the source after a restart (the volatile ring is gone; the idempotent sink is the only durable landing).
 *
 * <p>Positions are ranked by the order the engine assigned, never by the token. The tests carry tokens
 * whose text disagrees with that order — the token the reader is furthest along on sorts lowest as a
 * string — so anything that fell back to comparing the tokens themselves clamps the wrong way round.
 */
class SrsDurableFrontierTest {

    /** A position at ring sequence {@code seq}, with a token whose text sorts against that order. */
    private static ChainPosition at(long seq) {
        return new ChainPosition(new SourceOrder(1, seq), "w" + (1000 - seq));
    }

    private static ConsumerOffset acked(String pipelineId, ChainPosition sinkAcked) {
        return new ConsumerOffset(pipelineId, Map.of(), sinkAcked);
    }

    @Test
    void advancesToTheReaderCandidateWhenItTrailsEveryConsumer() {
        List<ConsumerOffset> consumers = List.of(acked("p1", at(5)), acked("p2", at(8)));
        // The reader has read up to 4, behind the slowest consumer's ack (5) -> its full progress is safe.
        assertThat(SrsDurableFrontier.safeAdvance(at(4), consumers)).hasValue(at(4).token());
    }

    @Test
    void clampsToTheSlowestConsumerSoUnackedEventsStayReplayable() {
        List<ConsumerOffset> consumers = List.of(acked("p1", at(5)), acked("p2", at(8)));
        // The reader has read up to 10, but the slowest consumer has only acked 5. Persisting 10 would let
        // a restart re-mine from 10 and lose 6..9 (only ever in the volatile ring). The offset is clamped
        // to 5, so a restart re-mines 6..10 -> replayable, none lost.
        assertThat(SrsDurableFrontier.safeAdvance(at(10), consumers)).hasValue(at(5).token());
    }

    @Test
    void refusesToAdvanceWhenAnyConsumerHasAckedNothing() {
        List<ConsumerOffset> consumers = List.of(acked("p1", at(8)), acked("p2", null));
        // p2 has sunk nothing, so its acked frontier is below the origin: the offset must not advance at
        // all, or a restart would strand every change p2 has yet to land.
        assertThat(SrsDurableFrontier.safeAdvance(at(10), consumers)).isEmpty();
    }

    @Test
    void refusesToAdvanceWhenThereAreNoConsumers() {
        // No consumer holds the data durably yet -> nothing is safe to advance past.
        assertThat(SrsDurableFrontier.safeAdvance(at(10), List.of())).isEmpty();
    }

    @Test
    void boundsByTheSingleConsumersAck() {
        assertThat(SrsDurableFrontier.safeAdvance(at(10), List.of(acked("p1", at(7)))))
                .hasValue(at(7).token());
    }

    @Test
    void ranksPositionsByTheEngineOrderAndNotByTheirTokens() {
        // The reader is at ring sequence 100 and the consumer has acked 9, so the advance must clamp to the
        // consumer's. Their tokens read the other way round as text ("w900" precedes "w991"), which is the
        // whole reason the order and the token are separate quantities: a token is a connector value with no
        // order defined on it, and reading one into it clamps to the reader's own position - persisting an
        // offset past changes no sink has landed.
        ChainPosition ahead = new ChainPosition(new SourceOrder(1, 100), "w900");
        ChainPosition behind = new ChainPosition(new SourceOrder(1, 9), "w991");
        assertThat(SrsDurableFrontier.safeAdvance(ahead, List.of(acked("p1", behind))))
                .hasValue("w991");
    }

    @Test
    void refusesToAdvanceOnAPositionWithNoTokenToWriteDown() {
        // A frontier that has reached snapshot rows and no change has nothing a read could resume from.
        ChainPosition snapshotRow = new ChainPosition(SourceOrder.snapshotRow(1), null);
        assertThat(SrsDurableFrontier.safeAdvance(at(10), List.of(acked("p1", snapshotRow)))).isEmpty();
    }

    @Test
    void ranksAHigherGenerationAboveALowerOne() {
        // A rebuilt ring numbers from zero again, so sequence 0 of the newer generation is ahead of
        // sequence 900 of the older one. Comparing sequences alone would clamp the advance to a position
        // the consumer has long passed.
        ChainPosition rebuilt = new ChainPosition(new SourceOrder(2, 0), "w2000");
        ChainPosition older = new ChainPosition(new SourceOrder(1, 900), "w1900");
        assertThat(SrsDurableFrontier.safeAdvance(rebuilt, List.of(acked("p1", older))))
                .hasValue("w1900");
        assertThat(SrsDurableFrontier.safeAdvance(older, List.of(acked("p1", rebuilt))))
                .hasValue("w1900");
    }

    @Test
    void rejectsANullCandidate() {
        assertThatThrownBy(() -> SrsDurableFrontier.safeAdvance(null, List.of()))
                .isInstanceOf(NullPointerException.class);
    }
}
