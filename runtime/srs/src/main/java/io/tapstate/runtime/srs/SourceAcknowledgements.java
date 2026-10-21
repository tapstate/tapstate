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
 * Tells the source of every tail one capture run unit runs how far it may release its change log.
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
 * nothing else, so it is counted on the run's health as a failed acknowledgement and never fails the run. It is
 * also said, at most once a minute for a tail: a source that is never told anything keeps its whole log, and the
 * health readings are not where anybody watching the source would look.
 *
 * <p>The tails followed, and the thread that reads for them, belong to the unit that made this and end when it
 * is closed. A process that runs several units in turn -- a test JVM starting one server after another -- would
 * otherwise go on reading, every interval, for tails whose servers and stores are gone.
 */
final class SourceAcknowledgements implements AutoCloseable {

    /** How often each followed tail's chain is read for a new durable position. */
    static final Duration INTERVAL = Duration.ofSeconds(5);

    private static final System.Logger LOG = System.getLogger(SourceAcknowledgements.class.getName());

    /** The least time between two warnings about one tail's reads failing; each failure is still counted. */
    private static final long WARNING_INTERVAL_NANOS = Duration.ofMinutes(1).toNanos();

    private final Set<Followed> followed = ConcurrentHashMap.newKeySet();
    private final ScheduledExecutorService reader = Executors.newSingleThreadScheduledExecutor(task -> {
        Thread thread = new Thread(task, "tapstate-source-acknowledge");
        thread.setDaemon(true);
        return thread;
    });

    SourceAcknowledgements() {
        long every = INTERVAL.toMillis();
        reader.scheduleWithFixedDelay(() -> followed.forEach(Followed::handOverQuietly),
                every, every, TimeUnit.MILLISECONDS);
    }

    /**
     * Starts following {@code tail}, a tail of {@code chainId} whose run reports on {@code health}: hands it the
     * chain's durable position now, and every interval after. With {@code writtenThroughOnly}, an offset that is
     * not a write-through checkpoint is not handed over. Answers the subscription to hold instead of
     * {@code tail} -- closing it stops the following as well as the tail.
     */
    Subscription follow(SrsMetaStore meta, String chainId, Subscription tail, CaptureHealth health,
            boolean writtenThroughOnly) {
        Objects.requireNonNull(meta, "meta");
        Objects.requireNonNull(chainId, "chainId");
        Followed one = new Followed(chainId, () -> meta.durableSourceRead(chainId)
                .filter(read -> !writtenThroughOnly || read.writtenThrough())
                .map(DurableSourceRead::position), tail, health, followed);
        followed.add(one);
        one.handOverQuietly();
        return one;
    }

    /**
     * Stops the thread that reads for every tail followed here. The tails themselves are left as they are: each
     * is closed by the run that holds it.
     */
    @Override
    public void close() {
        reader.shutdownNow();
    }

    /** Whether the thread that reads for the tails followed here has stopped. */
    boolean stopped() {
        return reader.isTerminated();
    }

    /** One tail being told its durable position. */
    static final class Followed implements Subscription {

        private final String chainId;
        private final Supplier<Optional<ChainPosition>> durablePosition;
        private final Subscription tail;
        private final CaptureHealth health;
        /** The tails followed alongside this one, which this one leaves once it is closed. */
        private final Set<Followed> registry;
        private ChainPosition handed;
        private volatile boolean closed;
        private long lastWarnedNanos;
        private boolean warned;

        private Followed(String chainId, Supplier<Optional<ChainPosition>> durablePosition, Subscription tail,
                CaptureHealth health, Set<Followed> registry) {
            this.chainId = chainId;
            this.durablePosition = Objects.requireNonNull(durablePosition, "durablePosition");
            this.tail = Objects.requireNonNull(tail, "tail");
            this.health = Objects.requireNonNull(health, "health");
            this.registry = Objects.requireNonNull(registry, "registry");
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

        /**
         * One read of the schedule: {@link #handOver}, with a failed read counted on the run's health and said at
         * most once a minute.
         */
        void handOverQuietly() {
            try {
                handOver();
            } catch (RuntimeException failure) {
                health.acknowledgeFailed(failure);
                warn(failure);
            }
        }

        private synchronized void warn(RuntimeException failure) {
            long now = System.nanoTime();
            if (warned && now - lastWarnedNanos < WARNING_INTERVAL_NANOS) {
                return;
            }
            warned = true;
            lastWarnedNanos = now;
            LOG.log(System.Logger.Level.WARNING, "Could not read how far the source of chain " + chainId
                    + " may release its change log; it keeps that log until a read succeeds: "
                    + failure.getMessage(), failure);
        }

        @Override
        public void acknowledge(SourcePosition durable) {
            tail.acknowledge(durable);
        }

        @Override
        public void close() {
            closed = true;
            registry.remove(this);
            tail.close();
        }
    }
}
