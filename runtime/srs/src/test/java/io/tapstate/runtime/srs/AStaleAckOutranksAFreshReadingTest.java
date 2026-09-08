package io.tapstate.runtime.srs;

import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.spi.store.ConsumerOffset;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * What the clamp does with an ack left behind by an earlier generation — the mechanism that makes moving a
 * chain's read offset backwards require letting those acks go.
 *
 * <p>Positions rank by generation first, so an ack recorded in the generation a previous run used ranks
 * below anything the current run has read. The clamp takes the lowest, so it takes the ack — and in source
 * terms that ack sits <em>ahead</em>. In the ordinary case that is right and harmless: a resumed run starts
 * at or behind what its sink confirmed, so being clamped to it advances the offset to a position already
 * landed. It stops being harmless the moment the offset was put behind that ack on purpose.
 *
 * <p>Recorded here rather than argued in prose because it is the whole reason a write-back releases the
 * acks on the chain it moves. Take that release away and this is what happens instead: one change goes
 * past, the clamp hands back the old ack, and the moved offset is gone from the record.
 */
class AStaleAckOutranksAFreshReadingTest {

    @Test
    void anAckFromAnEarlierGenerationClampsAReadingThatIsBehindItInTheSource() {
        // What this pipeline's sink confirmed, in the generation the previous run used.
        ConsumerOffset ackedLastRun = new ConsumerOffset("orders_sync", Map.of(),
                new ChainPosition(new SourceOrder(3L, 91201L), "mysql-bin.000004:154"));
        // Where the run that came up has read to, in a generation of its own, well behind that in the
        // source's own log -- the state a write-back leaves.
        ChainPosition readSoFar = new ChainPosition(new SourceOrder(4L, 1L), "mysql-bin.000001:9");

        Optional<ChainPosition> safe = SrsDurableFrontier.safeAdvance(readSoFar, List.of(ackedLastRun));

        // Not the reading: the older generation ranks lower, so the ack wins the minimum and would be
        // written down -- ahead of where the read actually is.
        assertThat(safe).contains(ackedLastRun.sinkAcked());
    }

    @Test
    void withNoAckRecordedNothingIsWrittenUntilASinkConfirmsSomethingAgain() {
        ChainPosition readSoFar = new ChainPosition(new SourceOrder(4L, 1L), "mysql-bin.000001:9");

        ConsumerOffset released = new ConsumerOffset("orders_sync", Map.of("orders", 12L), null);

        assertThat(SrsDurableFrontier.safeAdvance(readSoFar, List.of(released))).isEmpty();
    }
}
