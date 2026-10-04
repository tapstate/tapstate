package io.tapstate.runtime.srs;

import io.tapstate.core.event.ChainPosition;
import io.tapstate.spi.capture.SourcePosition;
import io.tapstate.spi.capture.Subscription;
import io.tapstate.spi.store.DurableSourceRead;
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
 * changes the rolled-back record asks for again. It is the position the capture resumes from after a restart,
 * and nothing else: not what the reader has read, not what reached a ring, not what one table's sink confirmed.
 * Where the capture's rings write through to the recoverable change log, that is the checkpoint written once a
 * batch is in the log, and a consumer that has not landed the batch replays it from there; on a direct channel
 * it is bounded by what the channel's targets have confirmed. Every party that moves the offset -- the capture
 * checkpointing a batch, a direct channel settling one, a position set by hand -- is seen through this one
 * read, so none of them can be left out.
 *
 * <p>A capture that writes through is told only a write-through checkpoint. An offset on its chain that is not
 * one was bounded by something else, and a capture running in this mode does not resume from it.
 *
 * <p>A tail is told its starting position the moment it is followed, and after that on a schedule of its own:
 * every interval its chain is read once, and the position is handed over when it differs from the one handed
 * last. A subscription applies it on its source's own delivery thread, so handing it over here only records it.
 * The reads run on a thread of their own -- not on the thread a source hands its changes over on, which must not
 * wait on the store.
 *
 * <p>A read that fails is tried again at the next interval. It costs the source some log for a while longer and
 * nothing else, so it is counted on the run's health as a failed acknowledgement and never fails the run.
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
     * chain's durable position now, and every interval after. With {@code writtenThroughOnly}, an offset that is
     * not a write-through checkpoint is not handed over. Answers the subscription to hold instead of
     * {@code tail} -- closing it stops the following as well as the tail.
     */
    static Subscription follow(SrsMetaStore meta, String chainId, Subscription tail, CaptureHealth health,
            boolean writtenThroughOnly) {
        Objects.requireNonNull(meta, "meta");
        Objects.requireNonNull(chainId, "chainId");
        Followed followed = new Followed(() -> meta.durableSourceRead(chainId)
                .filter(read -> !writtenThroughOnly || read.writtenThrough())
                .map(DurableSourceRead::position), tail, health);
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
