package io.tapstate.runtime.srs;

import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.spi.capture.SourcePosition;
import io.tapstate.spi.store.ConsumerOffset;
import io.tapstate.spi.store.SchemaVersion;
import io.tapstate.spi.store.SrsMeta;
import io.tapstate.spi.store.SrsMetaStore;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The persisted source read offset never moves backwards, and a tail reading its source directly stops
 * writing it once another pipeline is on the chain.
 *
 * <p>The offset is written down as the source's own token, but it is <em>ranked</em> by the order the
 * engine assigned as it read -- the pair (generation, sequence). A direct tail numbers only its own changes,
 * under a generation of its own; its positions mean nothing against another pipeline's, and a direct tail
 * that went on writing the chain's offset beside one ranked the two by accident -- falling back as soon as a
 * pipeline further behind joined, while nothing about the falling was visible at the time. So it writes the
 * chain's offset only while its pipeline is the only one on the chain, and its own position always.
 *
 * <p>The store's own guarantee that this value only ever moves forward still stands behind every writer; the
 * real one is held to it against a live database by {@code MongoSrsMetaStoreIT}.
 */
class SourceReadOffsetOnlyMovesForwardTest {

    private static final String CHAIN = "chain";
    private static final long GENERATION = 1L;

    @Test
    void aDirectTailLeavesTheChainsOffsetAloneOnceAnotherPipelineJoins() {
        AdvanceOnlyMeta meta = new AdvanceOnlyMeta();
        PhysicalSourcePrefix account = PhysicalSourcePrefix.direct(meta, CHAIN, GENERATION, "p1", new CaptureHealth());
        try {
            account.start(Optional.of(new SourcePosition("s0")));
            for (int run = 1; run <= 5; run++) {
                landAndRecord(meta, account, run);
            }
            assertThat(meta.current()).isEqualTo("s5");

            // A second pipeline joins the chain and its sink is further behind: its position is a place this
            // chain has already read past, and counted in another sequence than this tail's.
            meta.consumers.add(new ConsumerOffset(
                    "p2", Map.of("orders", 1L), new ChainPosition(new SourceOrder(GENERATION, 1), "s2")));
            landAndRecord(meta, account, 6);
        } finally {
            account.close();
        }

        assertThat(meta.advances)
                .as("the chain's offset, written while the direct pipeline was the only one on the chain")
                .containsExactly("s1", "s2", "s3", "s4", "s5");
        assertThat(meta.current()).isEqualTo("s5");
    }

    /**
     * Has the direct pipeline land run {@code run} -- one change of {@code orders}, the source naming
     * s{@code run} for it -- and records the run, which the account then lets go of at once.
     */
    private static void landAndRecord(AdvanceOnlyMeta meta, PhysicalSourcePrefix account, int run) {
        long seq = run - 1L;
        ChainPosition landed = new ChainPosition(new SourceOrder(GENERATION, seq), "s" + run);
        List<ConsumerOffset> others = meta.consumers.stream().filter(c -> !c.pipelineId().equals("p1")).toList();
        meta.consumers.clear();
        meta.consumers.add(new ConsumerOffset("p1", Map.of("orders", seq), landed, List.of(), null, 0L,
                List.of(), GENERATION, Map.of("orders", landed)));
        meta.consumers.addAll(others);
        account.admitted(Map.of("orders", seq), "s" + run);
    }

    /**
     * A meta store holding only the source read offset, and honouring the one guarantee its contract makes
     * about it: it only ever moves forward. A position that does not rank after the recorded one is
     * ignored, silently and successfully.
     */
    private static final class AdvanceOnlyMeta implements SrsMetaStore {
        ChainPosition recorded;
        final List<String> advances = new ArrayList<>();
        /** Every consumer on the chain as the account reads them: the direct pipeline first, then the rest. */
        final List<ConsumerOffset> consumers = new java.util.concurrent.CopyOnWriteArrayList<>();

        @Override
        public List<ConsumerOffset> consumerOffsets(String miningChainId) {
            return List.copyOf(consumers);
        }

        @Override
        public boolean establishPhysicalAnchor(String miningChainId, ChainPosition position) {
            // A direct tail carries on whether or not the record takes its first position as an anchor.
            return false;
        }

        String current() {
            return recorded == null ? null : recorded.token();
        }

        @Override
        public void rewindSourceReadOffset(String miningChainId, String token) {
            // No test on this double writes a position back; a call here is a wiring mistake, not a case.
            throw new UnsupportedOperationException("rewindSourceReadOffset");
        }

        @Override
        public void advanceSourceReadOffset(String miningChainId, ChainPosition position) {
            advances.add(position.token());
            if (recorded != null && position.order().compareTo(recorded.order()) <= 0) {
                return;
            }
            recorded = position;
        }

        @Override
        public Optional<SrsMeta> read(String miningChainId) {
            return Optional.of(new SrsMeta(miningChainId, recorded, List.copyOf(consumers), List.of(), null,
                    GENERATION));
        }

        @Override
        public void create(String miningChainId, String retention) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void upsertConsumerOffset(String miningChainId, ConsumerOffset offset) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void advanceConsumerReadSeq(String miningChainId, String pipelineId, String table, long lastReadSeq) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void advanceSinkAcked(String miningChainId, String pipelineId, ChainPosition position) {
            // The consumers are this case's to set; the account's own record of p1 is not what is under test.
        }

        @Override
        public void setCdcStart(
                String miningChainId, String pipelineId, String cdcStartPosition, long snapshotEpoch) {
            // Where the direct pipeline's own stream began; not what is under test.
        }

        @Override
        public long openEpoch(String miningChainId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void appendSchemaVersion(String miningChainId, SchemaVersion version) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void markSnapshotComplete(String miningChainId, String pipelineId, String table) {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<String> miningChainIdsWithConsumer(String pipelineId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void dropChain(String miningChainId) {
            throw new UnsupportedOperationException(
                    "chain removal is not exercised by this double");
        }

        @Override
        public void detachConsumer(String miningChainId, String pipelineId) {
            throw new UnsupportedOperationException();
        }
    }

}
