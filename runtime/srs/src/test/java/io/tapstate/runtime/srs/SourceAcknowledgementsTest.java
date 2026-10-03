package io.tapstate.runtime.srs;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.spi.capture.SourcePosition;
import io.tapstate.spi.capture.Subscription;
import io.tapstate.spi.store.IoError;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * What a tail's source is told it may release: the chain's durable source read offset, read on its own, and
 * nothing any one writer of it says on the way.
 */
class SourceAcknowledgementsTest {

    private static final String CHAIN = "chain-acknowledged";

    private CountingMeta meta;
    private RecordingTail tail;
    private final CaptureHealth health = new CaptureHealth();
    private Subscription followed;

    @BeforeEach
    void seedTheChain() {
        meta = new CountingMeta();
        meta.create(CHAIN, null);
        meta.openEpoch(CHAIN);
        tail = new RecordingTail();
    }

    @AfterEach
    void stopFollowing() {
        if (followed != null) {
            followed.close();
        }
    }

    /**
     * A tail is told the position it starts from as soon as it is followed, and after that every position the
     * chain comes to hold -- once each, whoever moved it there. A tail told nothing until its first change
     * lands would leave a source that restarted behind an earlier release holding its whole log meanwhile.
     */
    @Test
    void aTailIsToldWhereItStartsAndThenEveryNewDurablePositionOnce() {
        meta.advanceSourceReadOffset(CHAIN, at(-1, "t0"));
        followed = SourceAcknowledgements.follow(meta, CHAIN, tail, health);
        assertThat(tail.told).containsExactly("t0");

        handOver();
        assertThat(tail.told).as("unchanged, so not told again").containsExactly("t0");

        meta.advanceSourceReadOffset(CHAIN, at(4, "t4"));
        handOver();
        meta.rewindSourceReadOffset(CHAIN, "set-by-hand");
        handOver();
        assertThat(tail.told).containsExactly("t0", "t4", "set-by-hand");
    }

    /**
     * The position is read with the durable read and only that: a record the store could still roll back must
     * never be told to a source, which would have let go of what the rolled-back record asks for again.
     */
    @Test
    void thePositionIsReadDurablyAndNeverOffTheWholeRecord() {
        meta.advanceSourceReadOffset(CHAIN, at(2, "t2"));
        int recordReads = meta.recordReads.get();

        followed = SourceAcknowledgements.follow(meta, CHAIN, tail, health);
        handOver();

        assertThat(meta.durableReads.get()).isEqualTo(2);
        assertThat(meta.recordReads.get()).isEqualTo(recordReads);
        assertThat(tail.told).containsExactly("t2");
    }

    /** A chain with no position yet tells the source nothing; there is nothing it may release. */
    @Test
    void aChainWithNoPositionTellsNothing() {
        followed = SourceAcknowledgements.follow(meta, CHAIN, tail, health);
        handOver();

        assertThat(tail.told).isEmpty();
    }

    /**
     * A read that fails is counted on the run's health as a failed acknowledgement and never fails the run --
     * every pipeline reading the capture would fail with it, over a release that is only late.
     */
    @Test
    void aReadThatFailsIsCountedAndNeverFailsTheRun() {
        meta.advanceSourceReadOffset(CHAIN, at(2, "t2"));
        meta.failDurableReads.set(true);

        followed = SourceAcknowledgements.follow(meta, CHAIN, tail, health);
        handOver();

        assertThat(health.consecutiveAcknowledgeFailures()).isEqualTo(2);
        assertThat(health.lastAcknowledgeFailureCode()).contains(IoError.STORE_UNAVAILABLE.code());
        assertThat(health.failure()).isEmpty();
        assertThat(tail.told).isEmpty();

        meta.failDurableReads.set(false);
        handOver();
        assertThat(tail.told).containsExactly("t2");
    }

    /** Once the tail is closed nothing more is read for it, and the tail itself is closed. */
    @Test
    void closingStopsTheFollowingAndTheTail() {
        followed = SourceAcknowledgements.follow(meta, CHAIN, tail, health);
        followed.close();
        meta.advanceSourceReadOffset(CHAIN, at(9, "t9"));
        handOver();

        assertThat(tail.closed).isTrue();
        assertThat(tail.told).isEmpty();
    }

    /**
     * The schedule reads on a thread of its own: not the thread the source hands its changes over on, which
     * must not wait on the store, and not the thread that releases runs, which holds the source back when slow.
     */
    @Test
    void theScheduledReadsRunOnAThreadOfTheirOwn() throws Exception {
        followed = SourceAcknowledgements.follow(meta, CHAIN, tail, health);
        meta.advanceSourceReadOffset(CHAIN, at(3, "t3"));

        long deadline = System.nanoTime()
                + SourceAcknowledgements.INTERVAL.toNanos() + TimeUnit.SECONDS.toNanos(5);
        while (tail.told.isEmpty() && System.nanoTime() < deadline) {
            Thread.sleep(50);
        }

        assertThat(tail.told).containsExactly("t3");
        assertThat(tail.threads).containsExactly("tapstate-source-acknowledge");
    }

    private void handOver() {
        ((SourceAcknowledgements.Followed) followed).handOverQuietly();
    }

    private static ChainPosition at(long seq, String token) {
        return new ChainPosition(new SourceOrder(1, seq), token);
    }

    /** A subscription that records what it is told, and on which thread. */
    private static final class RecordingTail implements Subscription {
        final List<String> told = new CopyOnWriteArrayList<>();
        final List<String> threads = new CopyOnWriteArrayList<>();
        volatile boolean closed;

        @Override
        public void acknowledge(SourcePosition durable) {
            told.add(durable.token());
            threads.add(Thread.currentThread().getName());
        }

        @Override
        public void close() {
            closed = true;
        }
    }

    /** The run unit's in-memory store, counting how the source read offset is read. */
    private static final class CountingMeta extends CaptureRunUnitTest.InMemoryMeta {
        final AtomicInteger durableReads = new AtomicInteger();
        final AtomicInteger recordReads = new AtomicInteger();
        final AtomicBoolean failDurableReads = new AtomicBoolean();
        private String rewound;

        @Override
        public synchronized Optional<io.tapstate.spi.store.SrsMeta> read(String miningChainId) {
            recordReads.incrementAndGet();
            return super.read(miningChainId);
        }

        @Override
        public Optional<ChainPosition> durableSourceRead(String miningChainId) {
            durableReads.incrementAndGet();
            if (failDurableReads.get()) {
                throw new TapstateException(IoError.STORE_UNAVAILABLE, Map.of("detail", "not now"), null);
            }
            if (rewound != null) {
                return Optional.of(new ChainPosition(null, rewound));
            }
            return super.read(miningChainId).map(io.tapstate.spi.store.SrsMeta::sourceRead);
        }

        @Override
        public void rewindSourceReadOffset(String miningChainId, String token) {
            rewound = token;
        }
    }
}
