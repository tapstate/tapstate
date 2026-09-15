package io.tapstate.runtime.srs;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.RingbufferConfig;
import com.hazelcast.config.SerializerConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.ringbuffer.Ringbuffer;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.Op;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.spi.capture.CaptureBatch;
import io.tapstate.spi.capture.CaptureConfig;
import io.tapstate.spi.capture.CaptureListener;
import io.tapstate.spi.capture.CapturePort;
import io.tapstate.spi.capture.CaptureStart;
import io.tapstate.spi.capture.ConnectionReport;
import io.tapstate.spi.capture.DiscoveredSchema;
import io.tapstate.spi.capture.SourcePosition;
import io.tapstate.spi.capture.Subscription;
import io.tapstate.spi.store.ConsumerOffset;
import io.tapstate.spi.store.SchemaVersion;
import io.tapstate.spi.store.SrsMeta;
import io.tapstate.spi.store.SrsMetaStore;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The cdc phase drives an unbounded change stream through the headroom gate into the per-table change
 * ring: each change event (ops {@code i} / {@code u} / {@code d} / {@code ddl}) is projected to a ring
 * item, admitted only while a consumer's unread change is not at risk, and the source read offset is
 * advanced — clamped to the slowest consumer's sink-acked position — as writes land. The ring runs over a
 * single embedded Hazelcast member sized to the L1 hot-buffer shape (capacity 8), matching the write-gate
 * tests.
 *
 * <p>The per-event source position is threaded here at the cdc seam: the event envelope carries no
 * position slot, so at L1 a mock monotonic watermark supplies each change its opaque position (D10); real
 * per-event position threading from the connector is a later concern.
 */
class CdcPhaseTest {

    private static HazelcastInstance hz;

    /** Orders the mock positions {@code w1 < w2 < w3 < ...} by their numeric suffix; a source position is never ordered lexically. */
    /** The generation the ring under test is running under; every order on it carries this first. */
    private static final long RING_GENERATION = 1L;

    @BeforeAll
    static void startMember() {
        Config config = new Config();
        // Isolated, structurally undiscoverable single member -- never merge with anything on the LAN.
        config.setClusterName("srs-cdc-test-" + System.nanoTime());
        config.setProperty("hazelcast.phone.home.enabled", "false");
        config.setProperty("hazelcast.shutdownhook.enabled", "false");
        config.getNetworkConfig().getInterfaces().setEnabled(true).addInterface("127.0.0.1");
        config.getNetworkConfig().getJoin().getAutoDetectionConfig().setEnabled(false);
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config.getJetConfig().setEnabled(false);
        config.addRingBufferConfig(new RingbufferConfig("srs.*")
                .setCapacity(8)
                .setInMemoryFormat(InMemoryFormat.OBJECT)
                .setTimeToLiveSeconds(0)
                .setBackupCount(0));
        config.getSerializationConfig().addSerializerConfig(
                new SerializerConfig().setImplementation(new SrsItemSerializer()).setTypeClass(SrsItem.class));
        hz = Hazelcast.newHazelcastInstance(config);
    }

    @AfterAll
    static void stopMember() {
        if (hz != null) {
            hz.shutdown();
        }
    }

    private static CaptureConfig config() {
        return new CaptureConfig("mysql", Map.of(), List.of("orders"));
    }

    /**
     * The positions this fake source states for the changes it streams: w1, w2, ... one per change.
     *
     * <p>They are the <em>source's</em>, handed over with each change, which is what the phase under test
     * now reads. Nothing on the write side hands positions out any more — a generator there is what makes
     * a restart's positions begin again from w1 while the ring generation rises.
     */
    private static Supplier<SourcePosition> sourceStatedPositions() {
        AtomicLong n = new AtomicLong();
        return () -> new SourcePosition("w" + n.incrementAndGet());
    }

    /** A change-ring item used to fill the ring in the backpressure test; a plain insert at the given position. */
    private static SrsItem cdcItem(String token) {
        return new SrsItem(new SourcePosition(token), Op.INSERT, 1L, null, Map.of("id", 1), 0L);
    }

    /** Polls {@code thread}'s state until it reaches {@code target} or the timeout elapses; the final state decides. */
    private static boolean awaitState(Thread thread, Thread.State target, Duration timeout) throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            if (thread.getState() == target) {
                return true;
            }
            Thread.sleep(5);
        }
        return thread.getState() == target;
    }

    @Test
    void projectsEachCdcChangeToTheRingInOrder() throws Exception {
        Ringbuffer<SrsItem> ring = hz.getRingbuffer("srs.chain.order");
        SrsWriteGate gate = new SrsWriteGate(new SrsRingbuffer(ring));
        CdcChain chain = new CdcChain(gate, new RecordingMeta(), "chain", RING_GENERATION, 0L);
        FakeCdcPort port = new FakeCdcPort(List.of(
                Envelope.insert(1, "orders", Map.of("id", 1), Map.of()),
                Envelope.update(2, "orders", Map.of("id", 1), Map.of("id", 1, "n", 9), Map.of()),
                Envelope.delete(3, "orders", Map.of("id", 1), Map.of())));

        CdcPhase.run(port, config(), chain, () -> List.of(keepingUp()), new CaptureHealth());

        assertThat(ring.tailSequence()).isEqualTo(2L);
        SrsItem first = ring.readOne(0);
        SrsItem second = ring.readOne(1);
        SrsItem third = ring.readOne(2);
        assertThat(first.op()).isEqualTo(Op.INSERT);
        assertThat(first.srcPos()).isEqualTo(new SourcePosition("w1"));
        assertThat(first.after()).containsEntry("id", 1);
        assertThat(second.op()).isEqualTo(Op.UPDATE);
        assertThat(second.srcPos()).isEqualTo(new SourcePosition("w2"));
        assertThat(second.before()).containsEntry("id", 1);
        assertThat(third.op()).isEqualTo(Op.DELETE);
        assertThat(third.srcPos()).isEqualTo(new SourcePosition("w3"));
        assertThat(third.before()).containsEntry("id", 1);
    }

    @Test
    void advancesTheSourceReadOffsetClampedToTheSlowestSinkAckedPosition() {
        SrsWriteGate gate = new SrsWriteGate(new SrsRingbuffer(hz.getRingbuffer("srs.chain.offset")));
        RecordingMeta meta = new RecordingMeta();
        // One consumer has durably acked its sink up to w2; the persisted read offset must never pass it.
        List<ConsumerOffset> consumers = List.of(new ConsumerOffset("p1", Map.of("orders", 9L), new ChainPosition(new SourceOrder(RING_GENERATION, 1), "w2")));
        CdcChain chain = new CdcChain(gate, meta, "chain", RING_GENERATION, 0L);
        FakeCdcPort port = new FakeCdcPort(List.of(
                Envelope.insert(1, "orders", Map.of("id", 1), Map.of()),
                Envelope.insert(2, "orders", Map.of("id", 2), Map.of()),
                Envelope.insert(3, "orders", Map.of("id", 3), Map.of())));

        CdcPhase.run(port, config(), chain, () -> consumers, new CaptureHealth());

        // Written at w1, then clamped to the slowest sink-acked position w2 -- the persisted offset never
        // passes a change no consumer has durably landed. The third change resolves that same clamped
        // position again, and writing it a second time would tell the record what it already holds, so
        // nothing is written for it: three changes, two writes.
        assertThat(meta.advances).containsExactly("w1", "w2");
    }

    @Test
    void oneRunOfChangesTouchesTheRecordOncePerChangeAndNeverForTheWholeOfIt() {
        SrsWriteGate gate = new SrsWriteGate(new SrsRingbuffer(hz.getRingbuffer("srs.chain.cost")));
        CountingMeta meta = new CountingMeta(List.of(new ConsumerOffset(
                "p1", Map.of("orders", 9L),
                new ChainPosition(new SourceOrder(RING_GENERATION, 9), "w9"))));
        CdcChain chain = new CdcChain(gate, meta, "chain", RING_GENERATION, 0L);
        FakeCdcPort port = new FakeCdcPort(List.of(
                Envelope.insert(1, "orders", Map.of("id", 1), Map.of()),
                Envelope.insert(2, "orders", Map.of("id", 2), Map.of()),
                Envelope.insert(3, "orders", Map.of("id", 3), Map.of()),
                Envelope.insert(4, "orders", Map.of("id", 4), Map.of()),
                Envelope.insert(5, "orders", Map.of("id", 5), Map.of())));

        // Wired the way the runtime wires it: the cursors are fetched through the store, not handed in
        // as a constant. A supplier that closed over a list would make this case unable to see a read.
        CdcPhase.run(port, config(), chain, () -> meta.consumerOffsets("chain"), new CaptureHealth());

        assertThat(meta.cursorReads)
                .as("one cursor read per change, and no more: this is the figure a machine's speed cannot "
                        + "move, which is why it is counted rather than timed")
                .isEqualTo(5);
        assertThat(meta.wholeRecordReads)
                .as("and never the whole record, which carries a schema history that grows per DDL and is "
                        + "not read on this path; falling back to it is a cost that grows with the chain")
                .isZero();
        assertThat(meta.writes)
                .as("at most one write per change, and fewer once a position resolves to what the record "
                        + "already holds")
                .isLessThanOrEqualTo(5);
    }


    /**
     * A burst larger than the buffer is carried whole, and costs one durable write rather than one per
     * change.
     *
     * <p>This is the shape a cleanup transaction makes: a source hands over thousands of changes in a
     * single delivery. Two things have to be true of it and they pull in opposite directions -- every
     * change has to get through, and getting them through must not cost a round trip each. An
     * implementation that wrote per change satisfies the first and fails the second; one that dropped
     * what would not fit satisfies the second and fails the first. Neither reading alone is worth
     * anything, which is why both are here.
     *
     * <p>The first half is witnessed: admitting only the first piece of a burst reddens it, alone among
     * the cases in this file. That is where the decision is -- a delivery larger than the buffer is taken
     * in a piece at a time, each waiting for room, so what makes a burst arrive whole is that loop and not
     * the policy on any one append. Refusing an append that will not fit was tried first and left this
     * green, which is the wrong site rather than an absent one: the pieces are cut to the buffer's size,
     * so no append ever does not fit.
     *
     * <p>The second half is not witnessed and this says so. It rests on where the advance sits -- outside
     * the loop over a delivery's changes, so it happens once however many there are -- and moving it
     * inside is a restructuring rather than an edit. For that half this is a regression guard on a shape
     * that is currently right, which is less than a witness and is not written up as one.
     *
     * <p>Counted rather than timed. What a burst costs in microseconds is a fact about the machine that
     * ran it, and the figure it would be compared against is from another one; what it costs in writes to
     * the record is the same everywhere. And that every change got through is read off the sequence the
     * buffer assigned, not off a clock: it says all of them were admitted whether or not any is still in
     * memory, which for a burst past the buffer's size is the only honest way to ask.
     */
    @Test
    void aBurstPastTheBufferGetsThroughWholeAndCostsOneWrite() {
        SrsRingbuffer ring = new SrsRingbuffer(hz.getRingbuffer("srs.chain.burst"));
        SrsWriteGate gate = new SrsWriteGate(ring);
        // A consumer that is well ahead of anything this delivers. Without one the frontier has nothing
        // to take a minimum over and never moves at all -- measured: with no consumers both readings
        // below are nought, which would read as "it wrote nothing" and mean "nobody asked it to".
        CountingMeta meta = new CountingMeta(List.of(keepingUp()));
        CdcChain chain = new CdcChain(gate, meta, "chain", RING_GENERATION, 0L);

        int burst = (int) ring.capacity() * 6;
        BatchingCdcPort port = new BatchingCdcPort(burst, burst);

        CdcPhase.run(port, config(), chain, () -> meta.consumerOffsets("chain"), new CaptureHealth());

        assertThat(ring.tailSequence() + 1)
                .as("changes the buffer took in from a burst of %d -- %d is its whole capacity, so this "
                        + "asks whether the delivery got through rather than whether it still fits",
                        burst, ring.capacity())
                .isEqualTo(burst);
        assertThat(meta.writes)
                .as("writes to the record that burst cost: the offset is advanced once at the close of a "
                        + "delivery, so a burst is one write however many changes it carries. One per "
                        + "change would be %d", burst)
                .isEqualTo(1);
    }

    /**
     * The offset is written at the close of every delivery, which is what bounds how much a restart redoes.
     *
     * <p>A run that stops carries on from the last offset it wrote, so everything after that one is
     * delivered a second time. What that costs is therefore decided entirely by how often the offset is
     * written: at the close of each delivery, the most that can be redone is the delivery that was in
     * flight. Written once at the end of the whole run instead, everything since the run began is redone
     * -- unbounded in the only sense that matters, since a tail does not end.
     *
     * <p>So the reading is the number of advances against the number of deliveries. It is the mechanism
     * rather than the consequence, and deliberately: measuring the redo directly needs a run to be cut at
     * a chosen instant, and an instant chosen by a test is not the one a failure picks. The count of
     * advances says the bound holds wherever the cut lands.
     */
    @Test
    void theOffsetIsWrittenPerDeliveryWhichIsWhatBoundsARedo() {
        SrsWriteGate gate = new SrsWriteGate(new SrsRingbuffer(hz.getRingbuffer("srs.chain.redo")));
        RecordingMeta meta = new RecordingMeta();
        CdcChain chain = new CdcChain(gate, meta, "chain", RING_GENERATION, 0L);

        int perDelivery = 4;
        int deliveries = 5;
        BatchingCdcPort port = new BatchingCdcPort(perDelivery * deliveries, perDelivery);

        CdcPhase.run(port, config(), chain, () -> List.of(keepingUp()), new CaptureHealth());

        assertThat(meta.advances)
                .as("positions written over %d deliveries of %d changes each: one at the close of each, so "
                        + "a run cut anywhere redoes at most the %d that were in flight. Written once at "
                        + "the end instead, this is a single entry and the redo is the whole run",
                        deliveries, perDelivery, perDelivery)
                .hasSize(deliveries);
    }

    /**
     * A write that would take an unread change's place waits for the reader instead.
     *
     * <p>The buffer is bounded and a delivery can be larger than it, so something has to give when the
     * reader is behind: either the write waits, or it goes in and the change nobody has read yet is gone.
     * The second loses data with nothing to show for it -- the ring reports a healthy tail, the reader
     * carries on from where it was, and the changes in between were never anywhere else.
     *
     * <p>The reader here has read nothing for its first few looks and then catches up, which is what a
     * slow consumer is from the writer's side. What says the write waited is how often the reader was
     * asked: the admission re-reads the cursors on every attempt -- it has to, since that is the only
     * thing that can tell a parked write that room has appeared -- so more looks than there are pieces to
     * write means attempts that were refused. Without the bound the pieces go straight in and the count
     * is one look each.
     *
     * <p>And nothing was lost while it waited: the whole delivery is in the ring at the end. Both are
     * needed. A write that simply refused for ever would satisfy the first reading and fail this one.
     *
     * <p>Beside the case that drives the gate directly, not instead of it. That one says the gate refuses
     * and is asked again; this one says a delivery larger than the buffer gets through anyway -- it is cut
     * into pieces the buffer can hold and each waits its turn, which is a property of the admission above
     * the gate and not of the gate. Each case is green with the other deleted.
     */
    @Test
    void aWriteThatWouldTakeAnUnreadChangesPlaceWaitsForTheReader() {
        SrsRingbuffer ring = new SrsRingbuffer(hz.getRingbuffer("srs.chain.burstwait"));
        SrsWriteGate gate = new SrsWriteGate(ring);
        RecordingMeta meta = new RecordingMeta();
        CdcChain chain = new CdcChain(gate, meta, "chain", RING_GENERATION, 0L);

        int capacity = (int) ring.capacity();
        int burst = capacity * 2;
        int pieces = burst / capacity;
        AtomicInteger looks = new AtomicInteger();

        // Behind for its first few looks, then caught up. The count is what the case reads, so the reader
        // has to move eventually: one that never did would park the writer for the length of the run.
        Supplier<Collection<ConsumerOffset>> reader = () -> {
            long readTo = looks.incrementAndGet() < 5 ? 0L : burst;
            return List.of(new ConsumerOffset("p1", Map.of("orders", readTo),
                    new ChainPosition(new SourceOrder(RING_GENERATION, readTo), "w" + readTo)));
        };

        CdcPhase.run(new BatchingCdcPort(burst, burst), config(), chain, reader, new CaptureHealth());

        assertThat(looks.get())
                .as("times the writer asked what the reader had reached, over %d pieces: it re-reads on "
                        + "every attempt, so more than one look per piece is attempts that were refused. "
                        + "With no bound the pieces go straight in and this is %d", pieces, pieces)
                .isGreaterThan(pieces);
        assertThat(ring.tailSequence() + 1)
                .as("changes in the buffer once the reader caught up: waiting is only right if nothing was "
                        + "dropped while it waited, and a writer that refused for ever would pass the "
                        + "reading above and fail this one")
                .isEqualTo(burst);
    }

    /** A consumer acked far past anything these cases deliver, so the clamp never decides the reading. */
    private static ConsumerOffset keepingUp() {
        return new ConsumerOffset("p1", Map.of("orders", 99_999L),
                new ChainPosition(new SourceOrder(RING_GENERATION, 99_999), "w99999"));
    }

    /**
     * A source that hands its changes over in deliveries of a chosen size, each with one position.
     *
     * <p>The port beside this one hands every change over on its own, which cannot tell "once per
     * delivery" from "once per change" -- with one change per delivery the two are the same number. The
     * size is what makes them different figures, so it is a parameter here.
     */
    private static final class BatchingCdcPort implements CapturePort {

        private final int changes;
        private final int perDelivery;
        private final Supplier<SourcePosition> positions = sourceStatedPositions();

        BatchingCdcPort(int changes, int perDelivery) {
            this.changes = changes;
            this.perDelivery = perDelivery;
        }

        @Override
        public Subscription cdc(CaptureConfig config, CaptureStart start, CaptureListener listener) {
            List<Envelope> delivery = new ArrayList<>(perDelivery);
            for (int change = 1; change <= changes; change++) {
                delivery.add(Envelope.insert(change, "orders", Map.of("id", change), Map.of()));
                if (delivery.size() == perDelivery) {
                    listener.onBatch(List.copyOf(delivery), Optional.of(positions.get()));
                    delivery.clear();
                }
            }
            if (!delivery.isEmpty()) {
                listener.onBatch(List.copyOf(delivery), Optional.of(positions.get()));
            }
            return () -> { };
        }

        @Override
        public CaptureBatch snapshot(CaptureConfig config) {
            throw new UnsupportedOperationException();
        }

        @Override
        public ConnectionReport testConnection(CaptureConfig config) {
            throw new UnsupportedOperationException();
        }

        @Override
        public DiscoveredSchema discoverSchema(CaptureConfig config) {
            throw new UnsupportedOperationException();
        }
    }
    /**
     * A store that answers the cursor read directly, the way the shipped one does, and counts every way
     * it is touched. The distinction between the two read counters is the point: a store that dropped the
     * override would still be correct and would answer through the whole record instead, which this can
     * tell apart and a single total cannot.
     */
    private static final class CountingMeta implements SrsMetaStore {

        private final List<ConsumerOffset> consumers;
        int cursorReads;
        int wholeRecordReads;
        int writes;

        CountingMeta(List<ConsumerOffset> consumers) {
            this.consumers = consumers;
        }

        @Override
        public List<ConsumerOffset> consumerOffsets(String miningChainId) {
            cursorReads++;
            return consumers;
        }

        @Override
        public Optional<SrsMeta> read(String miningChainId) {
            wholeRecordReads++;
            return Optional.empty();
        }

        @Override
        public void advanceSourceReadOffset(String miningChainId, ChainPosition position) {
            writes++;
        }

        @Override
        public void rewindSourceReadOffset(String miningChainId, String token) {
            throw new UnsupportedOperationException("no write-back on this path");
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
        public void advanceConsumerReadSeq(
                String miningChainId, String pipelineId, String table, long lastReadSeq) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void advanceSinkAcked(String miningChainId, String pipelineId, ChainPosition position) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setCdcStart(String miningChainId, String cdcStartPosition, long snapshotEpoch) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long openEpoch(String miningChainId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void appendSchemaVersion(String miningChainId, io.tapstate.spi.store.SchemaVersion version) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void markSnapshotComplete(String miningChainId, String pipelineId, String table) {
            throw new UnsupportedOperationException();
        }

        @Override
        public java.util.List<String> miningChainIdsWithConsumer(String pipelineId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void detachConsumer(String miningChainId, String pipelineId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void dropChain(String miningChainId) {
            throw new UnsupportedOperationException();
        }
    }

    @Test
    void cutsTheDurableLogBackToWhatEveryConsumerHasLanded() {
        SrsWriteGate gate = new SrsWriteGate(new SrsRingbuffer(hz.getRingbuffer("srs.chain.trim")));
        RecordingMeta meta = new RecordingMeta();
        // One consumer, durably acked at ring sequence 1.
        List<ConsumerOffset> consumers = List.of(new ConsumerOffset(
                "p1", Map.of("orders", 9L), new ChainPosition(new SourceOrder(RING_GENERATION, 1), "w2")));
        CdcChain chain = new CdcChain(gate, meta, "chain", RING_GENERATION, 0L);
        List<Long> cuts = new ArrayList<>();
        Map<String, CdcPhase.TableRoute> routes = Map.of("orders", new CdcPhase.TableRoute(
                chain, () -> consumers, cuts::add));
        FakeCdcPort port = new FakeCdcPort(List.of(
                Envelope.insert(1, "orders", Map.of("id", 1), Map.of()),
                Envelope.insert(2, "orders", Map.of("id", 2), Map.of()),
                Envelope.insert(3, "orders", Map.of("id", 3), Map.of())));

        CdcPhase.run(port, config(), routes, new CaptureHealth());

        // The cut is the same clamped frontier the offset advance uses: sequence 0 while the reader is
        // still behind the ack, then the ack itself once it is not. A log that is never cut grows without
        // bound, and a cut that ran ahead of this would delete a change a consumer has not landed.
        assertThat(cuts)
                .as("the cut rides the frontier, so it is asked for whenever that moves and never passes it")
                .containsExactly(0L, 1L);
    }

    @Test
    void backpressuresARefusedWriteAndRetriesRatherThanDropIt() throws Exception {
        Ringbuffer<SrsItem> raw = hz.getRingbuffer("srs.chain.backpressure");
        SrsWriteGate gate = new SrsWriteGate(new SrsRingbuffer(raw));
        // Fill the ring to capacity with changes no consumer has read (min read seq -1).
        for (int i = 0; i < 8; i++) {
            assertThat(gate.append(cdcItem("f" + i), -1L)).isPresent();
        }
        // The slowest consumer has read nothing on the first poll (the write is refused), then advances to
        // seq 0 on the next -- modeling the source read pausing until a consumer frees a slot.
        AtomicLong polls = new AtomicLong();
        // The bound is read off the consumer cursors, so this is a consumer that has read nothing of orders
        // on the first poll and has reached seq 0 by the next. It has acked nothing, which is why no offset
        // is written here: this case is about the refused write being retried, not about the frontier.
        Supplier<Collection<ConsumerOffset>> minRead = () -> List.of(new ConsumerOffset(
                "p1", polls.getAndIncrement() == 0 ? Map.of() : Map.of("orders", 0L), null));
        CdcChain chain = new CdcChain(gate, new RecordingMeta(), "chain", RING_GENERATION, 0L);
        FakeCdcPort port = new FakeCdcPort(List.of(Envelope.insert(9, "orders", Map.of("id", 9), Map.of())));

        CdcPhase.run(port, config(), chain, minRead, new CaptureHealth());

        // The change is not dropped: it lands at seq 8 once headroom frees, and the write was retried
        // (polled more than once) rather than silently overwriting the still-unread seq 0.
        assertThat(raw.tailSequence()).isEqualTo(8L);
        assertThat(raw.readOne(8).srcPos()).isEqualTo(new SourcePosition("w1"));
        assertThat(polls.get()).isGreaterThan(1L);
    }

    @Test
    void parksOffCpuWhileBackpressuredRatherThanBusySpinning() throws Exception {
        Ringbuffer<SrsItem> raw = hz.getRingbuffer("srs.chain.park");
        SrsWriteGate gate = new SrsWriteGate(new SrsRingbuffer(raw));
        // Fill the ring to capacity with changes no consumer has read, so the next cdc write is refused.
        for (int i = 0; i < 8; i++) {
            assertThat(gate.append(cdcItem("f" + i), -1L)).isPresent();
        }
        // The slowest consumer reads nothing until the test frees a slot: the write stays backpressured.
        AtomicBoolean freed = new AtomicBoolean(false);
        Supplier<Collection<ConsumerOffset>> minRead = () -> List.of(new ConsumerOffset(
                "p1", freed.get() ? Map.of("orders", 0L) : Map.of(), null));
        CdcChain chain = new CdcChain(gate, new RecordingMeta(), "chain", RING_GENERATION, 0L);
        FakeCdcPort port = new FakeCdcPort(List.of(Envelope.insert(9, "orders", Map.of("id", 9), Map.of())));

        Thread writer = new Thread(() -> CdcPhase.run(port, config(), chain, minRead, new CaptureHealth()), "cdc-writer");
        writer.setDaemon(true);
        try {
            writer.start();
            // A backpressured write parks off-CPU (TIMED_WAITING); it does not burn a core spinning (RUNNABLE).
            boolean parked = awaitState(writer, Thread.State.TIMED_WAITING, Duration.ofSeconds(2));
            // Freeing a slot lets the parked writer wake, land the change and finish -- paused, never dropped.
            freed.set(true);
            writer.join(2000);

            assertThat(parked).isTrue();
            assertThat(writer.isAlive()).isFalse();
            assertThat(raw.tailSequence()).isEqualTo(8L);
            assertThat(raw.readOne(8).srcPos()).isEqualTo(new SourcePosition("w1"));
        } finally {
            freed.set(true);
            writer.interrupt();
        }
    }

    @Test
    void stopsTheStreamThroughTheReturnedSubscription() {
        SrsWriteGate gate = new SrsWriteGate(new SrsRingbuffer(hz.getRingbuffer("srs.chain.sub")));
        CdcChain chain = new CdcChain(gate, new RecordingMeta(), "chain", RING_GENERATION, 0L);
        FakeCdcPort port = new FakeCdcPort(List.of());

        Subscription sub = CdcPhase.run(port, config(), chain, List::of, new CaptureHealth());
        sub.close();

        // The phase hands back the port's own subscription; closing it stops the stream.
        assertThat(port.closed).isTrue();
    }

    @Test
    void rejectsIncompleteWiringBeforeStartingTheStream() {
        SrsWriteGate gate = new SrsWriteGate(new SrsRingbuffer(hz.getRingbuffer("srs.chain.guard")));
        CdcChain chain = new CdcChain(gate, new RecordingMeta(), "chain", RING_GENERATION, 0L);
        FakeCdcPort port = new FakeCdcPort(List.of(Envelope.insert(1, "orders", Map.of("id", 1), Map.of())));

        assertThatThrownBy(() -> CdcPhase.run(port, config(), chain, null, new CaptureHealth()))
                .isInstanceOf(NullPointerException.class);

        // Args are validated up front: the stream is never started when the wiring is incomplete.
        assertThat(port.subscribed).isFalse();
    }

    @Test
    void recordsAStreamFailureOnTheHealthSoTheRunCanSurfaceIt() {
        CaptureHealth health = new CaptureHealth();
        RuntimeException boom = new RuntimeException("stream boom");
        SrsWriteGate gate = new SrsWriteGate(new SrsRingbuffer(hz.getRingbuffer("srs.chain.fail")));
        CdcChain chain = new CdcChain(gate, new RecordingMeta(), "chain", RING_GENERATION, 0L);

        CdcPhase.run(FakeCdcPort.failing(boom), config(), chain, List::of, health);

        // The stream reported a failure rather than a change; the phase records it on the health so the run
        // can surface a dead tail that the change ring merely going quiet would otherwise hide.
        assertThat(health.failure()).contains(boom);
    }

    @Test
    void rejectsCdcEventsForTablesOutsideTheConfiguredSelection() {
        FakeCdcPort port = new FakeCdcPort(List.of(Envelope.insert(1, "customers", Map.of("id", 1), Map.of())));

        assertThatThrownBy(() -> CdcPhase.run(port, config(), Map.of(), new CaptureHealth()))
                .isInstanceOfSatisfying(TapstateException.class, exception -> {
                    assertThat(exception.code()).isEqualTo(CaptureError.EVENT_TABLE_NOT_SELECTED);
                    assertThat(exception.args()).containsEntry("table", "customers");
                });
    }

    /**
     * A cdc port that drives a fixed list of change events into the listener when the stream starts,
     * stating a position for each one the way a source does.
     */
    private static final class FakeCdcPort implements CapturePort {
        private final List<Envelope> events;
        private final Throwable error;
        private final Supplier<SourcePosition> positions = sourceStatedPositions();
        boolean subscribed;
        boolean closed;

        FakeCdcPort(List<Envelope> events) {
            this(events, null);
        }

        private FakeCdcPort(List<Envelope> events, Throwable error) {
            this.events = events;
            this.error = error;
        }

        /** A port whose stream reports a failure instead of delivering changes. */
        static FakeCdcPort failing(Throwable error) {
            return new FakeCdcPort(List.of(), error);
        }

        @Override
        public Subscription cdc(CaptureConfig config, CaptureStart start, CaptureListener listener) {
            subscribed = true;
            if (error != null) {
                listener.onError(error);
                return () -> closed = true;
            }
            // Each change is handed over as a run of its own, each with its own position -- the shape a
            // source that names a position per change produces.
            for (Envelope e : events) {
                listener.onBatch(List.of(e), Optional.of(positions.get()));
            }
            return () -> closed = true;
        }

        @Override
        public CaptureBatch snapshot(CaptureConfig config) {
            throw new UnsupportedOperationException();
        }

        @Override
        public ConnectionReport testConnection(CaptureConfig config) {
            throw new UnsupportedOperationException();
        }

        @Override
        public DiscoveredSchema discoverSchema(CaptureConfig config) {
            throw new UnsupportedOperationException();
        }
    }

    /** A meta store that records the sequence of source-read-offset advances; the other facets are unused here. */
    private static final class RecordingMeta implements SrsMetaStore {
        @Override
        public java.util.List<String> miningChainIdsWithConsumer(String pipelineId) {
            throw new UnsupportedOperationException("consumer detachment is not exercised by this double");
        }

        @Override
        public void dropChain(String miningChainId) {
            throw new UnsupportedOperationException(
                    "chain removal is not exercised by this double");
        }

        @Override
        public void detachConsumer(String miningChainId, String pipelineId) {
            throw new UnsupportedOperationException("consumer detachment is not exercised by this double");
        }

        final List<String> advances = new ArrayList<>();

        @Override
        public void rewindSourceReadOffset(String miningChainId, String token) {
            // No test on this double writes a position back; a call here is a wiring mistake, not a case.
            throw new UnsupportedOperationException("rewindSourceReadOffset");
        }

        @Override
        public void advanceSourceReadOffset(String miningChainId, ChainPosition position) {
            advances.add(position.token());
        }

        @Override
        public Optional<SrsMeta> read(String miningChainId) {
            throw new UnsupportedOperationException();
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
            throw new UnsupportedOperationException();
        }

        @Override
        public void setCdcStart(String miningChainId, String cdcStartPosition, long snapshotEpoch) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long openEpoch(String miningChainId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void markSnapshotComplete(String miningChainId, String pipelineId, String table) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void appendSchemaVersion(String miningChainId, SchemaVersion version) {
            throw new UnsupportedOperationException();
        }
    }
}
