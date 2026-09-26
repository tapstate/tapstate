package io.tapstate.runtime.srs;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.spi.capture.SourcePosition;
import io.tapstate.spi.store.ConsumerOffset;
import io.tapstate.spi.store.SrsMeta;
import io.tapstate.spi.store.SrsMetaStore;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

/**
 * One capture owner's bounded account of the physical source batches it has admitted. Each table still
 * numbers its own ring; this account records which ring sequences a batch reached and releases the source
 * token only when every consumer that selected those tables at admission has confirmed them. A lost owner
 * discards this volatile account and resumes from the last durable token, so an interrupted release replays
 * work rather than skipping it.
 */
final class PhysicalSourcePrefix implements AutoCloseable {

    static final int MAX_PENDING_BATCHES = 256;
    private static final long POLL_NANOS = TimeUnit.MILLISECONDS.toNanos(1);
    private static final Set<PhysicalSourcePrefix> ACTIVE = ConcurrentHashMap.newKeySet();
    private static final ScheduledExecutorService TICK = Executors.newSingleThreadScheduledExecutor(task -> {
        Thread thread = new Thread(task, "tapstate-physical-prefix");
        thread.setDaemon(true);
        return thread;
    });

    static {
        TICK.scheduleWithFixedDelay(() -> ACTIVE.forEach(PhysicalSourcePrefix::tickSafely),
                100, 100, TimeUnit.MILLISECONDS);
    }

    private final SrsMetaStore meta;
    private final String chainId;
    private final long epoch;
    private final CaptureHealth health;
    private final Deque<Batch> pending = new ArrayDeque<>();
    private long nextBatch;
    private boolean anchored;
    private boolean closed;
    private RuntimeException failure;

    PhysicalSourcePrefix(SrsMetaStore meta, String chainId, long epoch, CaptureHealth health) {
        this.meta = Objects.requireNonNull(meta, "meta");
        this.chainId = Objects.requireNonNull(chainId, "chainId");
        this.health = Objects.requireNonNull(health, "health");
        if (epoch < 1) {
            throw new IllegalArgumentException("a physical source prefix needs an open generation");
        }
        this.epoch = epoch;
        SrsMeta stored = meta.read(chainId)
                .orElseThrow(() -> new IllegalStateException("physical prefix has no chain: " + chainId));
        if (stored.epoch() != epoch || ((stored.sourceRead() != null || epoch > 1)
                && !meta.physicalPrefixTrusted(chainId))) {
            throw new TapstateException(CaptureError.SHARED_POSITION_UNVERIFIED,
                    Map.of("chain", chainId), null);
        }
        SourceOrder prior = stored.sourceRead() == null ? null : stored.sourceRead().order();
        nextBatch = prior != null && prior.epoch() == epoch ? Math.addExact(prior.seq(), 1) : 0L;
        ACTIVE.add(this);
    }

    /** Records the connector's actual start boundary before admitting any volatile change. */
    synchronized void anchor(Optional<SourcePosition> start) {
        checkOpen();
        String token = start.map(SourcePosition::token).orElseThrow(() ->
                new TapstateException(CaptureError.RESUME_ANCHOR_UNAVAILABLE,
                        Map.of("chain", chainId), null));
        ChainPosition boundary = new ChainPosition(new SourceOrder(epoch, -1L), token);
        if (!meta.establishPhysicalAnchor(chainId, boundary)) {
            throw new TapstateException(CaptureError.SHARED_POSITION_UNVERIFIED,
                    Map.of("chain", chainId), null);
        }
        anchored = true;
    }

    /** Stops source callbacks before they admit a batch that has no room for its recovery barrier. */
    void awaitRoom() {
        while (true) {
            synchronized (this) {
                checkOpen();
                if (!anchored) {
                    throw new TapstateException(CaptureError.RESUME_ANCHOR_UNAVAILABLE,
                            Map.of("chain", chainId), null);
                }
                if (pending.size() < MAX_PENDING_BATCHES) {
                    return;
                }
            }
            tick();
            LockSupport.parkNanos(POLL_NANOS);
            if (Thread.currentThread().isInterrupted()) {
                throw new CancellationException("physical source prefix stopped while backpressured");
            }
        }
    }

    /** Records one whole source callback after every table share reached its durable ring. */
    synchronized void admitted(Map<String, Long> lastRingSeqByTable, String token) {
        checkOpen();
        if (!anchored) {
            throw new TapstateException(CaptureError.RESUME_ANCHOR_UNAVAILABLE,
                    Map.of("chain", chainId), null);
        }
        if (lastRingSeqByTable.isEmpty()) {
            throw new IllegalArgumentException("a physical batch must contain changes");
        }
        if (pending.size() == MAX_PENDING_BATCHES) {
            throw new IllegalStateException("physical batch admitted without recovery-barrier capacity");
        }
        Collection<ConsumerOffset> consumers = meta.consumerOffsets(chainId);
        Map<String, Map<String, Long>> required = new LinkedHashMap<>();
        for (ConsumerOffset consumer : consumers) {
            Map<String, Long> tables = new LinkedHashMap<>();
            lastRingSeqByTable.forEach((table, seq) -> {
                if (consumer.selectedTables() == null || consumer.selectedTables().contains(table)) {
                    tables.put(table, seq);
                }
            });
            required.put(consumer.pipelineId(), Map.copyOf(tables));
        }
        ChainPosition physical = new ChainPosition(new SourceOrder(epoch, nextBatch++), token);
        pending.addLast(new Batch(physical, Map.copyOf(required)));
        drain(consumers);
    }

    /** An idle source still learns about sink acknowledgements through the one shared background tick. */
    synchronized void tick() {
        if (!closed && !pending.isEmpty()) {
            checkOpen();
            drain(meta.consumerOffsets(chainId));
        }
    }

    private void tickSafely() {
        try {
            tick();
        } catch (RuntimeException error) {
            synchronized (this) {
                failure = error;
            }
            health.fail(error);
        }
    }

    private void drain(Collection<ConsumerOffset> consumers) {
        Map<String, ConsumerOffset> current = new LinkedHashMap<>();
        consumers.forEach(consumer -> current.put(consumer.pipelineId(), consumer));
        while (!pending.isEmpty() && !current.isEmpty()) {
            Batch first = pending.peekFirst();
            if (!confirmed(first, current)) {
                return;
            }
            ChainPosition physical = first.physical();
            if (physical.token() != null) {
                for (String pipelineId : first.required().keySet()) {
                    if (current.containsKey(pipelineId)) {
                        meta.advanceSinkAcked(chainId, pipelineId, physical);
                    }
                }
                meta.advanceSourceReadOffset(chainId, physical);
            }
            pending.removeFirst();
        }
    }

    private static boolean confirmed(Batch batch, Map<String, ConsumerOffset> current) {
        for (Map.Entry<String, Map<String, Long>> obligation : batch.required().entrySet()) {
            ConsumerOffset consumer = current.get(obligation.getKey());
            if (consumer == null) {
                continue;
            }
            for (Map.Entry<String, Long> table : obligation.getValue().entrySet()) {
                ChainPosition ack = consumer.sinkAckedByTable().get(table.getKey());
                if (ack == null || ack.order().epoch() != batch.physical().order().epoch()
                        || ack.order().seq() < table.getValue()) {
                    return false;
                }
            }
        }
        return true;
    }

    private void checkOpen() {
        if (closed) {
            throw new CancellationException("physical source prefix is closed");
        }
        if (failure != null) {
            throw failure;
        }
    }

    @Override
    public synchronized void close() {
        closed = true;
        pending.clear();
        ACTIVE.remove(this);
    }

    private record Batch(ChainPosition physical, Map<String, Map<String, Long>> required) {
    }
}
