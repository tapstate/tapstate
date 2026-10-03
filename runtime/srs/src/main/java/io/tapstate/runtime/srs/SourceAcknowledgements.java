package io.tapstate.runtime.srs;

import io.tapstate.core.event.ChainPosition;
import io.tapstate.spi.capture.SourcePosition;
import io.tapstate.spi.capture.Subscription;
import io.tapstate.spi.store.SrsMetaStore;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Tells the source of every tail this process runs how far it may release its change log.
 *
 * <p>What a source is told is the chain's source read offset as it stands durably -- read with a majority read
 * concern, so a write the store could still roll back is never told to a source that would then have let go of
 * changes the rolled-back record asks for again. It is the position a restart resumes from, and nothing else: not
 * what the reader has read, not what reached a ring, not what one table's sink confirmed. Every party that moves
 * that offset -- the reader releasing a run, a direct tail, the start a stream laid down, a position set by
 * hand -- is seen through this one read, so none of them can be left out.
 *
 * <p>A tail is told its starting position the moment it is followed, and after that on a schedule of its own:
 * every interval its chain is read once, and the position is handed over when it differs from the one handed
 * last. A subscription applies it on its source's own delivery thread, so handing it over here only records it.
 * The reads run on a thread of their own -- not on the thread a source hands its changes over on, which must not
 * wait on the store, and not on the thread that releases runs, which holds the source back when it is slow.
 *
 * <p>A read that fails is tried again at the next interval. It costs the source some log for a while longer and
 * nothing else, so it is counted on the run's health as a failed acknowledgement and never fails the run.
 *
 * <p>A tail reading its source directly for one pipeline is told that pipeline's own position instead: where
 * the last of its changes that landed sat, or the chain's offset where its own stream wrote that -- as it does
 * while its pipeline is the only one on the chain, runs that carried no change included -- whichever its stream
 * reached later. The chain's offset moved by anybody else says nothing about what this pipeline landed: beside
 * a shared reader it is that reader's, released on its own pipelines' word.
 */
final class SourceAcknowledgements {

    /** How often each followed tail's chain is read for a new durable position. */
    static final Duration INTERVAL = Duration.ofSeconds(5);

    private static final Set<Followed> FOLLOWED = ConcurrentHashMap.newKeySet();
    private static final ScheduledExecutorService READER = Executors.newSingleThreadScheduledExecutor(task -> {
        Thread thread = new Thread(task, "tapstate-source-acknowledge");
        thread.setDaemon(true);
        return thread;
    });

    static {
        long every = INTERVAL.toMillis();
        READER.scheduleWithFixedDelay(() -> FOLLOWED.forEach(Followed::handOverQuietly),
                every, every, TimeUnit.MILLISECONDS);
    }

    private SourceAcknowledgements() {
    }

    /**
     * Starts following {@code tail}, a tail of {@code chainId} whose run reports on {@code health}: hands it the
     * chain's durable position now, and every interval after. Answers the subscription to hold instead of
     * {@code tail} -- closing it stops the following as well as the tail.
     */
    static Subscription follow(SrsMetaStore meta, String chainId, Subscription tail, CaptureHealth health) {
        return follow(() -> meta.durableSourceRead(chainId), tail, health);
    }

    /**
     * Starts following {@code tail}, a tail reading {@code chainId}'s source directly for {@code pipelineId}
     * alone, under generation {@code epoch}: hands it that pipeline's own durable position now, and every
     * interval after.
     */
    static Subscription followDirect(SrsMetaStore meta, String chainId, String pipelineId, long epoch,
            Subscription tail, CaptureHealth health) {
        return follow(() -> {
            Optional<ChainPosition> own = meta.durableSinkAcked(chainId, pipelineId);
            Optional<ChainPosition> chain = meta.durableSourceRead(chainId)
                    .filter(read -> read.order() != null && read.order().epoch() == epoch);
            if (chain.isEmpty()) {
                return own;
            }
            // The chain's offset was written by this very stream, while its pipeline was the only one on the
            // chain; once another arrived it stopped moving, and what the pipeline lands goes on moving its own.
            // Both are counted by this stream alone, so the later of the two is how far it has gone.
            if (own.isPresent() && own.get().order() != null && own.get().order().epoch() == epoch
                    && own.get().order().seq() > chain.get().order().seq()) {
                return own;
            }
            return chain;
        }, tail, health);
    }

    private static Subscription follow(Supplier<Optional<ChainPosition>> durable, Subscription tail,
            CaptureHealth health) {
        Followed followed = new Followed(durable, tail, health);
        FOLLOWED.add(followed);
        followed.handOverQuietly();
        return followed;
    }

    /** One tail being told its durable position. */
    static final class Followed implements Subscription {

        private final Supplier<Optional<ChainPosition>> durablePosition;
        private final Subscription tail;
        private final CaptureHealth health;
        private ChainPosition handed;
        private volatile boolean closed;

        private Followed(Supplier<Optional<ChainPosition>> durablePosition, Subscription tail, CaptureHealth health) {
            this.durablePosition = Objects.requireNonNull(durablePosition, "durablePosition");
            this.tail = Objects.requireNonNull(tail, "tail");
            this.health = Objects.requireNonNull(health, "health");
        }

        /** Reads the durable position once, and hands it over when it is one the tail was not handed. */
        private synchronized void handOver() {
            if (closed) {
                return;
            }
            Optional<ChainPosition> durable = durablePosition.get();
            if (durable.isEmpty() || durable.get().token() == null || durable.get().equals(handed)) {
                return;
            }
            tail.acknowledge(new SourcePosition(durable.get().token()));
            handed = durable.get();
        }

        /** One read of the schedule: {@link #handOver}, with a failed read counted on the run's health. */
        void handOverQuietly() {
            try {
                handOver();
            } catch (RuntimeException failure) {
                health.acknowledgeFailed(failure);
            }
        }

        /** The tail being followed. */
        Subscription tail() {
            return tail;
        }

        @Override
        public void acknowledge(SourcePosition durable) {
            tail.acknowledge(durable);
        }

        @Override
        public void close() {
            closed = true;
            FOLLOWED.remove(this);
            tail.close();
        }
    }
}
