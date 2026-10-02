package io.tapstate.runtime.srs;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.pipeline.StreamSource;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.spi.capture.CaptureConfig;
import io.tapstate.spi.capture.CaptureListener;
import io.tapstate.spi.capture.CapturePort;
import io.tapstate.spi.capture.CaptureStart;
import io.tapstate.spi.capture.SourcePosition;
import io.tapstate.spi.capture.Subscription;
import io.tapstate.spi.store.ConsumerOffset;
import io.tapstate.spi.store.SrsLogStore;
import io.tapstate.spi.store.SrsMeta;
import io.tapstate.spi.store.SrsMetaStore;
import java.time.Instant;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/**
 * Assembles one source's capture run: it reads the pipeline's {@link ConsumptionPlan} and dispatches the
 * snapshot phase, the cdc phase, the self-built Jet ring source and the mining-chain coordinator into a
 * single run, wiring the durable meta as it goes.
 *
 * <p>The dispatch is driven entirely by the plan (read mode x {@code srs.enabled}): a snapshot phase drains
 * straight to the pass-through sink; a shared-ring tail provisions the mining chain, attaches the consumer,
 * writes the change ring and exposes a Jet source over it; an srs-disabled tail provisions and attaches the
 * same way and streams straight to the one consumer, with no ring. See {@link #start} for the exact ordering.
 *
 * <p><strong>{@code srs.enabled} decides the buffering and nothing else.</strong> Any tail opens the chain
 * and keeps its durable record, so where a tail resumes from does not depend on the flag: a pipeline that
 * turns the buffering off keeps the position it had, and one that turns it back on finds it still there.
 * The alternative is a second account to move a position between, and the move is the step that loses one.
 *
 * <p>What the flag does decide is where a run with nothing recorded begins, because the two paths read
 * {@code start_from} in different coordinates: a direct tail resolves it against the source's own log,
 * while a buffered one resolves it against changes already mined into the ring and leaves the shared
 * miner on the present. That is a difference in what the setting points into, not in whether it is
 * honoured -- and it holds only for a first run, which is the one a recorded position does not outrank.
 *
 * <p>A shared-ring run reads every configured stream through one connector subscription and routes each
 * stream into its own per-table ring. Where the tail begins and what position each change carries are the
 * source's own, read back from the durable record and learned from the changes respectively.
 */
public final class CaptureRunUnit {

    /**
     * The member user-context key under which the durable coordination store is bound, so a ring source's
     * reader can resolve it member-side to publish its read cursor. The assembly layer binds the store under
     * this key when it makes the member SRS-capable.
     */
    public static final String SRS_META_USER_CONTEXT_KEY = "tapstate.srs.meta";

    /**
     * The member user-context key under which the durable change log is bound. The log is reached through
     * the member rather than through this class's constructor because the member is where it already lives
     * -- the rings resolve it from their own configuration -- and a run without a store binds nothing,
     * which reads here as "there is no log to cut".
     */
    public static final String SRS_LOG_USER_CONTEXT_KEY = "tapstate.srs.log";

    private final CapturePort port;
    private final SrsCoordinator coordinator;
    private final SrsMetaStore meta;
    private final HazelcastInstance hz;
    private final SnapshotBuffer snapshotBuffer;
    private final SnapshotWorkers snapshotWorkers;
    /** The fallback for direct callers that do not supply the product's durable per-pipeline generation. */
    private final AtomicLong chainlessSnapshotEpoch = new AtomicLong();

    public CaptureRunUnit(CapturePort port, SrsCoordinator coordinator, SrsMetaStore meta, HazelcastInstance hz) {
        this(port, coordinator, meta, hz, null, null);
    }

    public CaptureRunUnit(CapturePort port, SrsCoordinator coordinator, SrsMetaStore meta, HazelcastInstance hz,
            SnapshotBuffer snapshotBuffer, SnapshotWorkers snapshotWorkers) {
        this.port = Objects.requireNonNull(port, "port");
        this.coordinator = Objects.requireNonNull(coordinator, "coordinator");
        this.meta = Objects.requireNonNull(meta, "meta");
        this.hz = Objects.requireNonNull(hz, "hz");
        if ((snapshotBuffer == null) != (snapshotWorkers == null)) {
            throw new IllegalArgumentException("snapshot buffer and workers must be configured together");
        }
        this.snapshotBuffer = snapshotBuffer;
        this.snapshotWorkers = snapshotWorkers;
    }

    /**
     * Starts the source run for {@code spec}, draining any snapshot rows to {@code passthrough}, and returns
     * a handle on the assembled pieces. The steps run in a fixed order so the meta preconditions hold:
     *
     * <ol>
     *   <li>a tail — buffered or direct — provisions the mining chain first, seeding its meta: the
     *       precondition for recording the cdc-start position, and for resuming at one;</li>
     *   <li>the pipeline attaches to the chain as a consumer, and a shared-ring run exposes the Jet source
     *       over its ring;</li>
     *   <li>the snapshot phase drains to the pass-through sink: on a shared-ring run it records the cdc-start
     *       position at the seam, otherwise it is a pure drain with no chain to position;</li>
     *   <li>a shared-ring tail then runs the cdc phase into the change ring; an srs-disabled tail streams
     *       straight to the pass-through sink, behind the load. Both begin where the durable record says,
     *       and where it says nothing the direct one begins where {@code start_from} asked while the shared
     *       miner takes the present — see {@link #sourceStart}.</li>
     * </ol>
     *
     * <p>All of it happens on the calling thread, rows included. {@link #begin} is the same run with the rows
     * and the tail left to a thread of its own.
     */
    public CaptureRun start(CaptureRunSpec spec, Consumer<Envelope> passthrough) {
        return start(spec, passthrough, true);
    }

    /**
     * Starts one pipeline attachment and starts the shared tail only when this member owns its capture claim.
     *
     * <p>An attachment that starts no tail is the same attachment wherever it is made: the pipeline's own
     * load, as its own record says it is owed, and its membership of the chain whose ring it reads. What
     * differs is only who writes that ring. On the member holding the capture it is this member's own tail;
     * anywhere else it is another member's, so the chain is joined under the generation that tail writes
     * under rather than opened -- see {@link SrsCoordinator#joinSource}.
     */
    public CaptureRun start(CaptureRunSpec spec, Consumer<Envelope> passthrough, boolean startTail) {
        Objects.requireNonNull(passthrough, "passthrough");
        return start(spec, CaptureHandoff.of(passthrough), startTail);
    }

    /** As {@link #start(CaptureRunSpec, Consumer)}, telling {@code handoff} as each table's load is in. */
    public CaptureRun start(CaptureRunSpec spec, CaptureHandoff handoff) {
        return start(spec, handoff, true);
    }

    /**
     * As {@link #start(CaptureRunSpec, Consumer, boolean)}, telling {@code handoff} as each table's load is in.
     * The load is read on the calling thread and the run is handed back once it is over, so {@code handoff}
     * has to take every row without waiting for anything this caller does next.
     */
    public CaptureRun start(CaptureRunSpec spec, CaptureHandoff handoff, boolean startTail) {
        return open(spec, handoff, startTail, false);
    }

    /** As {@link #begin(CaptureRunSpec, CaptureHandoff, boolean)}, starting the tail as well. */
    public CaptureRun begin(CaptureRunSpec spec, CaptureHandoff handoff) {
        return begin(spec, handoff, true);
    }

    /**
     * Begins the source run for {@code spec} and hands it back as soon as its load has been opened, reading
     * the load -- and then starting the tail -- on a thread of the run's own.
     *
     * <p>Everything a job assembled from this run reads is done before this returns, in the order
     * {@link #start} does it: the chain and its generation, its membership of the chain, where this pipeline
     * arrives on each ring, and the seam its load began at. Only the rows are left, and the rows are what
     * a large load cannot afford to read before the job that takes them is running: they go into a hand-off
     * that holds a few thousand of them, and it is the job that makes room. Read here, they would fill it
     * and wait for a job nobody can submit until this returns.
     *
     * <p>A run that owes no load has nothing to read, and is started as {@link #start} starts it, tail
     * included. A failure of what is left is reported through the run's {@link CaptureRun#failure()}.
     */
    public CaptureRun begin(CaptureRunSpec spec, CaptureHandoff handoff, boolean startTail) {
        return open(spec, handoff, startTail, true);
    }

    private CaptureRun open(CaptureRunSpec spec, CaptureHandoff handoff, boolean startTail, boolean inBackground) {
        Objects.requireNonNull(spec, "spec");
        Objects.requireNonNull(handoff, "handoff");
        ConsumptionPlan plan = ConsumptionPlan.of(spec.readMode(), spec.srsEnabled());
        List<String> tables = spec.config().streams();
        if (tables == null || tables.isEmpty()) {
            throw new IllegalArgumentException("capture config must select at least one stream");
        }
        OpenState state = new OpenState();
        SnapshotWorkers.Reservation reservation = plan.snapshot() && snapshotWorkers != null
                ? snapshotWorkers.reserve().orElseThrow(SnapshotCapacityUnavailable::new) : null;
        try {
            provisionChain(spec, plan, startTail, state);
            // Opened before the load, not with the tail: the load's rows are this run's too, and an
            // account opened after them would report a run that had read nothing until its first change.
            CaptureHealth health = new CaptureHealth();
            selectConsumer(spec, plan, startTail, tables, state);
            attachConsumer(spec, plan, startTail, tables, state);
            markPipelineArrival(spec, plan, tables, state);
            Optional<StreamSource<SrsItem>> ringSource = ringSource(spec, plan, tables, state);
            state.load = reservation != null && state.chainId == null ? null
                    : openLoad(spec, plan, tables, state.chainId, state.epoch, state.cursorWriterToken);
            // The seam this run's own load began at, for the tail that follows it -- null when no load ran
            // here. Carried from the phase rather than read back off the chain, because the chain records
            // one seam for however many pipelines load from it: read back, a pipeline new to the chain
            // gets whichever load reached the record first and starts its tail where that one began.
            if (reservation != null) {
                return startDeferredSnapshot(spec, handoff, startTail, plan, state.chainId, state.merged,
                        state.epoch, state.cursorWriterToken, tables, health, reservation, state.load, ringSource);
            }
            String ownSeam = tailSeam(state.load);
            Supplier<Optional<Subscription>> tail =
                    () -> openTail(spec, plan, state.chainId, state.epoch, ownSeam,
                            startTail, health, handoff);

            Optional<CaptureRun> backgroundRun = beginBackgroundLoad(
                    spec, handoff, inBackground, tail, state, ringSource, health);
            if (backgroundRun.isPresent()) {
                return backgroundRun.get();
            }

            SnapshotRead snapshot = readSnapshot(state.load, handoff, health);
            state.subscription = tail.get();
            return new CaptureRun(
                    Optional.ofNullable(state.chainId), state.merged, snapshot.count(), snapshot.counts(),
                    ringSource, state.subscription, health);
        } catch (RuntimeException | Error failure) {
            if (reservation != null) { reservation.close(); }
            if (state.load != null) {
                state.load.close();
            }
            RuntimeException cleanupFailure = rollbackStartFailure(
                    state.chainId, spec.pipelineId(), state.chainCreated,
                    state.consumerAttached, state.subscription);
            if (cleanupFailure != null) {
                failure.addSuppressed(cleanupFailure);
            }
            throw failure;
        }
    }

    private void provisionChain(
            CaptureRunSpec spec, ConsumptionPlan plan, boolean startTail, OpenState state) {
        // Any tail opens the chain, buffered or not. The flag chooses whether changes go through the
        // shared ring; it does not choose whether this source has a durable record, because that record
        // is what the next run reads to know where to start -- a question the flag has no bearing on.
        if (!plan.tail()) {
            return;
        }
        state.chainId = MiningChainId.resolve(spec.config(), spec.srsKey());
        // Only the member that runs the tail opens a generation; an attachment reads the one that tail
        // writes under.
        ProvisionOutcome provisioned = startTail
                ? coordinator.provisionSource(
                        spec.sourceId(), state.chainId, spec.config().streams(), spec.retention())
                : coordinator.joinSource(spec.sourceId(), state.chainId, spec.config().streams());
        state.merged = provisioned.merged();
        state.epoch = provisioned.epoch();
        state.chainCreated = !state.merged;
    }

    private void selectConsumer(CaptureRunSpec spec, ConsumptionPlan plan, boolean startTail,
            List<String> tables, OpenState state) {
        if (!plan.sharedRing()) { return; }
        String cid = state.chainId.value();
        if (startTail) {
            for (String table : tables) {
                SrsRingbuffer ring = new SrsRingbuffer(hz.getRingbuffer(SrsRingbuffer.ringName(cid, table)));
                if (meta.establishRingGenerationStartAfter(cid, table, state.epoch, ring.tailSequence()).isEmpty()) {
                    throw new PhysicalRingNotReady(cid, table);
                }
            }
        } else {
            SrsMetaStore.PhysicalSelection selection = meta.physicalSelection(cid).orElse(null);
            if (selection != null && selection.epoch() == state.epoch && !selection.tables().containsAll(tables)) {
                throw new TapstateException(CaptureError.SHARED_SELECTION_RESTART_REQUIRED, Map.of("chain", cid), null);
            }
        }
        List<String> selected = spec.selectedChainTables() == null ? tables : spec.selectedChainTables();
        state.cursorWriterToken = spec.cursorWriterToken() == null ? java.util.UUID.randomUUID().toString() : spec.cursorWriterToken();
        state.previousConsumer = meta.consumerOffsets(cid).stream().filter(offset -> offset.pipelineId().equals(spec.pipelineId()))
                .findFirst().orElse(null);
        if (state.previousConsumer == null || !selected.equals(state.previousConsumer.selectedTables())
                || !Objects.equals(state.previousConsumer.selectedTablesEpoch(), state.epoch)
                || !state.cursorWriterToken.equals(state.previousConsumer.cursorWriterToken())) {
            meta.selectConsumerTables(cid, spec.pipelineId(), selected, state.epoch, state.cursorWriterToken);
        }
        ConsumerOffset current = meta.consumerOffsets(cid).stream().filter(offset -> offset.pipelineId().equals(spec.pipelineId()))
                .findFirst().orElseThrow();
        for (String table : selected) {
            if (!current.perTableSeq().containsKey(table)) {
                meta.advanceConsumerReadSeq(cid, spec.pipelineId(), table, state.epoch, state.cursorWriterToken, -1L);
            }
        }
    }

    private void markPipelineArrival(
            CaptureRunSpec spec, ConsumptionPlan plan, List<String> tables, OpenState state) {
        // Where this pipeline starts in each ring, marked now: after it is on the chain, and before its
        // own load reads a row or any tail this run opens mines a change -- so everything it is owed lands
        // above the mark. A cdc-only read from the earliest change or from an instant is placed by that
        // start instead, when its reader opens. A pipeline coming back keeps the place it had.
        if (plan.sharedRing() && (plan.snapshot() || spec.startFrom() instanceof StartFrom.Latest)) {
            markWhereThisPipelineArrives(state.chainId.value(), spec.pipelineId(), state.epoch, tables,
                    state.previousConsumer, plan.snapshot());
        }
    }

    private void attachConsumer(
            CaptureRunSpec spec,
            ConsumptionPlan plan,
            boolean startTail,
            List<String> tables,
            OpenState state) {
        // On the chain before the load rather than after it. Membership is what keeps the chain open:
        // the last consumer to give it back closes it, and a load read while the job runs gives any
        // other pipeline on the chain the whole length of the load in which to stop.
        if (plan.sharedRing()) {
            coordinator.attachConsumer(state.chainId, spec.pipelineId());
            state.consumerAttached = true;
            // A selected table protects its ring before arrival is sampled. If a writer gets there first,
            // the later sample moves arrival past what it wrote; once this registration lands, the writer
            // is constrained until that sample and its cursor are published together. Raising the floor
            // to -1 also leaves an already advanced cursor where a returning run left it.
            // The complete selected table set and unread cursors were published before this attachment.
        } else if (plan.directTail() && startTail) {
            coordinator.attachConsumer(
                    Objects.requireNonNull(state.chainId, "a tail resolves its chain before it runs"),
                    spec.pipelineId());
            state.consumerAttached = true;
        }
    }

    private Optional<StreamSource<SrsItem>> ringSource(
            CaptureRunSpec spec, ConsumptionPlan plan, List<String> tables, OpenState state) {
        if (!plan.sharedRing()) {
            return Optional.empty();
        }
        String firstTable = tables.getFirst();
        String firstRing = SrsRingbuffer.ringName(state.chainId.value(), firstTable);
        return Optional.of(SrsRingSource.create(
                firstRing, spec.startFrom(),
                readCursorPublisher(state.chainId.value(), spec.pipelineId(), firstTable, state.epoch, state.cursorWriterToken), spec.retention()));
    }

    private SnapshotPhase.Load openLoad(
            CaptureRunSpec spec,
            ConsumptionPlan plan,
            List<String> tables,
            MiningChainId chainId,
            long epoch, String cursorWriterToken) {
        // Which tables a resuming run still owes is asked once, by the snapshot phase, of this
        // pipeline's own record on the chain. A chainless read instead gets a run generation of its own.
        if (!plan.snapshot()) {
            return null;
        }
        if (chainId != null) {
            return SnapshotPhase.open(
                    port, spec.config(), chainId.value(), spec.pipelineId(), tables, epoch, meta, cursorWriterToken);
        }
        long snapshotEpoch = spec.snapshotEpoch() > 0
                ? spec.snapshotEpoch()
                : chainlessSnapshotEpoch.incrementAndGet();
        return SnapshotPhase.openChainless(port, spec.config(), snapshotEpoch);
    }

    private Optional<CaptureRun> beginBackgroundLoad(
            CaptureRunSpec spec,
            CaptureHandoff handoff,
            boolean inBackground,
            Supplier<Optional<Subscription>> tail,
            OpenState state,
            Optional<StreamSource<SrsItem>> ringSource,
            CaptureHealth health) {
        if (state.load == null || !inBackground || !state.load.readsAnything()) {
            return Optional.empty();
        }
        BackgroundLoad reading = new BackgroundLoad(
                state.load, handoff, tail, health,
                "tapstate-load-" + spec.pipelineId() + "-" + spec.sourceId());
        // Started before the run that owns it is made: a reader that cannot start then leaves no
        // run behind, only the read it opened, which the failure path closes with the chain.
        reading.start();
        return Optional.of(new CaptureRun(
                Optional.ofNullable(state.chainId), state.merged, ringSource, health, reading));
    }

    private static SnapshotRead readSnapshot(
            SnapshotPhase.Load load, CaptureHandoff handoff, CaptureHealth health) {
        Map<String, Long> counts = new LinkedHashMap<>();
        if (load == null) {
            return new SnapshotRead(0, counts);
        }
        long count = load.read(event -> {
            // Two tallies of rows that overlap, kept apart because they answer different questions:
            // this one is what the load read; the health count goes on climbing over the following tail.
            counts.merge(event.src(), 1L, Long::sum);
            health.received(event);
            handoff.accept(event);
        }, handoff::loaded);
        load.close();
        return new SnapshotRead(count, counts);
    }

    private static String tailSeam(SnapshotPhase.Load load) {
        return load == null ? null : load.tailSeam();
    }

    private record SnapshotRead(long count, Map<String, Long> counts) {
    }

    private static final class OpenState {
        private MiningChainId chainId;
        private boolean merged;
        private boolean chainCreated;
        private boolean consumerAttached;
        private long epoch;
        private String cursorWriterToken;
        private ConsumerOffset previousConsumer;
        private Optional<Subscription> subscription = Optional.empty();
        private SnapshotPhase.Load load;
    }

    private CaptureRun startDeferredSnapshot(CaptureRunSpec spec, CaptureHandoff handoff, boolean startTail,
            ConsumptionPlan plan, MiningChainId chainId, boolean merged, long epoch, String cursorWriterToken,
            List<String> tables, CaptureHealth health, SnapshotWorkers.Reservation reservation,
            SnapshotPhase.Load load, Optional<StreamSource<SrsItem>> ringSource) {
        MiningChainId rings = chainId != null ? chainId : MiningChainId.resolve(spec.config(), spec.srsKey());
        String token = spec.cursorWriterToken() != null ? spec.cursorWriterToken()
                : cursorWriterToken != null ? cursorWriterToken : java.util.UUID.randomUUID().toString();
        Map<String, String> ringByTable = new LinkedHashMap<>();
        for (String table : tables) {
            String ringName = SrsRingbuffer.ringName(rings.value(), table);
            snapshotBuffer.beginSnapshot(spec.pipelineId(), ringName, token);
            ringByTable.put(table, ringName);
        }
        DeferredCapture deferred = new DeferredCapture(reservation, snapshotBuffer, spec.pipelineId(),
                ringByTable.values(), token, health, load);
        deferred.prepare(() -> {
            Consumer<Envelope> receive = event -> {
                if (deferred.closed.get() || deferred.loadAbandoned.get()) {
                    throw new CancellationException("the reserved snapshot was closed before row delivery");
                }
                if (!ringByTable.containsKey(event.src())) {
                    throw new TapstateException(CaptureError.EVENT_TABLE_NOT_SELECTED, Map.of("table", event.src()), null);
                }
                snapshotBuffer.appendSnapshot(spec.pipelineId(), ringByTable.get(event.src()), token, event);
                if (deferred.closed.get() || deferred.loadAbandoned.get()) {
                    throw new CancellationException("the reserved snapshot was closed before its handoff callback");
                }
                deferred.received(event);
                health.received(event);
                handoff.accept(event);
            };
            Consumer<String> loaded = table -> {
                if (deferred.closed.get() || deferred.loadAbandoned.get()) {
                    throw new CancellationException("the reserved snapshot was closed before table completion");
                }
                handoff.loaded(table);
                snapshotBuffer.completeSnapshot(spec.pipelineId(), ringByTable.get(table), token);
            };
            try {
                if (load == null) {
                    long snapshotEpoch = spec.snapshotEpoch() > 0 ? spec.snapshotEpoch() : chainlessSnapshotEpoch.incrementAndGet();
                    SnapshotPhase.drain(port, spec.config(), snapshotEpoch, receive);
                    for (String table : tables) { loaded.accept(table); }
                } else {
                    load.read(receive, loaded);
                }
            } catch (RuntimeException ended) {
                if (!deferred.loadAbandoned.get()) { throw ended; }
            } finally {
                if (load != null) { load.close(); }
            }
            if (!deferred.closed.get()) {
                if (plan.sharedRing() && startTail) {
                    deferred.attach(startSharedTail(spec, chainId, epoch, load == null ? null : load.tailSeam(), tables, health, false));
                } else if (plan.directTail() && startTail) {
                    deferred.attach(startDirectTail(spec, chainId, epoch, load == null ? null : load.tailSeam(), health, handoff));
                }
            }
        });
        return new CaptureRun(Optional.ofNullable(chainId), merged, 0L, Map.of(), ringSource, Optional.of(deferred), health);
    }

    private static final class DeferredCapture implements SnapshotActivation {
        private final SnapshotWorkers.Reservation reservation;
        private final SnapshotBuffer buffer;
        private final String pipelineId;
        private final List<String> ringNames;
        private final String token;
        private final CaptureHealth health;
        private final SnapshotPhase.Load load;
        private final AtomicBoolean activated = new AtomicBoolean(), closed = new AtomicBoolean(), loadAbandoned = new AtomicBoolean();
        private final AtomicReference<Subscription> tail = new AtomicReference<>();
        private final AtomicLong rows = new AtomicLong();
        private final java.util.concurrent.ConcurrentMap<String, Long> byTable = new java.util.concurrent.ConcurrentHashMap<>();
        private final java.util.concurrent.CountDownLatch finished = new java.util.concurrent.CountDownLatch(1);
        private Runnable read;

        private DeferredCapture(SnapshotWorkers.Reservation reservation, SnapshotBuffer buffer, String pipelineId,
                Collection<String> ringNames, String token, CaptureHealth health, SnapshotPhase.Load load) {
            this.reservation = reservation; this.buffer = buffer; this.pipelineId = pipelineId;
            this.ringNames = List.copyOf(ringNames); this.token = token; this.health = health; this.load = load;
        }
        private void prepare(Runnable read) { this.read = Objects.requireNonNull(read, "read"); }
        private void received(Envelope event) { rows.incrementAndGet(); byTable.merge(event.src(), 1L, Long::sum); }
        @Override public long snapshotRows() { return rows.get(); }
        @Override public Map<String, Long> snapshotRowsByTable() { return Map.copyOf(byTable); }
        @Override public boolean loading() { return finished.getCount() != 0; }
        @Override public boolean awaitLoaded(java.time.Duration timeout) throws InterruptedException {
            return finished.await(timeout.toNanos(), java.util.concurrent.TimeUnit.NANOSECONDS);
        }
        @Override public void activateSnapshot() {
            if (closed.get() || !activated.compareAndSet(false, true)) { return; }
            reservation.activate(() -> {
                try {
                    if (!closed.get()) { read.run(); }
                } catch (RuntimeException | Error failure) {
                    if (!closed.get()) {
                        health.fail(failure);
                        for (String ringName : ringNames) {
                            try { buffer.failSnapshot(pipelineId, ringName, token, failure); }
                            catch (CancellationException replaced) { }
                        }
                    }
                    if (failure instanceof Error defect) { throw defect; }
                } finally { finished.countDown(); }
            });
        }
        private void attach(Subscription started) {
            if (closed.get()) { started.close(); return; }
            if (!tail.compareAndSet(null, started)) {
                started.close(); throw new IllegalStateException("a snapshot run opened more than one CDC tail");
            }
            if (closed.get()) { Subscription late = tail.getAndSet(null); if (late != null) { late.close(); } }
        }
        @Override public void abandonLoad() {
            if (!loading() || !loadAbandoned.compareAndSet(false, true)) { return; }
            if (load != null) { load.close(); }
            for (String ringName : ringNames) { buffer.releaseSnapshot(pipelineId, ringName, token); }
            if (!activated.get()) { activateSnapshot(); }
        }
        @Override public void close() {
            if (!closed.compareAndSet(false, true)) { return; }
            if (load != null) { load.close(); }
            for (String ringName : ringNames) { buffer.releaseSnapshot(pipelineId, ringName, token); }
            reservation.close();
            Subscription started = tail.getAndSet(null); if (started != null) { started.close(); }
            finished.countDown();
        }
    }

    private void registerConsumerTables(String chainId, String pipelineId, List<String> tables) {
        for (String table : tables) {
            meta.advanceConsumerReadSeq(chainId, pipelineId, table, -1L);
        }
    }

    /**
     * Opens the tail that follows a load, or nothing where this run starts no tail.
     *
     * <p>A shared-ring tail runs the cdc phase into the change ring; an srs-disabled one streams straight to
     * the pipeline through {@code passthrough}, behind the load it follows. Both begin where the durable
     * record says, and where it says nothing the direct one begins where {@code start_from} asked while the
     * shared miner takes the present -- see {@link #sourceStart}.
     */
    private Optional<Subscription> openTail(CaptureRunSpec spec, ConsumptionPlan plan, MiningChainId chainId,
            long epoch, String ownSeam, boolean startTail, CaptureHealth health, Consumer<Envelope> passthrough) {
        if (!startTail) { return Optional.empty(); }
        if (plan.sharedRing()) {
            return Optional.of(startSharedTail(spec, chainId, epoch, ownSeam, spec.config().streams(), health, false));
        }
        if (plan.directTail()) { return Optional.of(startDirectTail(spec, chainId, epoch, ownSeam, health, passthrough)); }
        return Optional.empty();
    }

    public CaptureRun reopenPhysicalTail(CaptureRunSpec spec, CaptureRun previous) {
        Objects.requireNonNull(spec, "spec");
        Objects.requireNonNull(previous, "previous");
        if (!spec.srsEnabled() || spec.readMode() == io.tapstate.core.model.ReadMode.SNAPSHOT_ONLY) {
            throw new IllegalArgumentException("only a shared CDC tail can expand its physical selection");
        }
        MiningChainId chainId = MiningChainId.resolve(spec.config(), spec.srsKey());
        if (!previous.chainId().equals(Optional.of(chainId)) || previous.cdcSubscription().isEmpty()) {
            throw new IllegalArgumentException("physical tail replacement needs the matching live capture");
        }
        SrsMeta record = meta.read(chainId.value()).orElseThrow();
        SrsMetaStore.PhysicalSelection current = meta.physicalSelection(chainId.value()).orElseThrow();
        if (current.epoch() != record.epoch() || record.sourceReadOffset() == null) {
            throw new TapstateException(CaptureError.RESUME_ANCHOR_UNAVAILABLE,
                    Map.of("chain", chainId.value()), null);
        }
        LinkedHashSet<String> needed = new LinkedHashSet<>(current.tables());
        needed.addAll(meta.requestedPhysicalTables(chainId.value()));
        if (needed.size() == current.tables().size()) {
            return previous;
        }
        previous.close();
        Subscription replacement = startSharedTail(spec, chainId, record.epoch(), null,
                spec.config().streams(), previous.health(), true);
        return new CaptureRun(previous.chainId(), previous.merged(), previous.snapshotCount(),
                previous.snapshotCounts(), previous.ringSource(), Optional.of(replacement), previous.health());
    }

    private Subscription startSharedTail(CaptureRunSpec spec, MiningChainId chainId, long epoch,
            String ownSeam, List<String> tables, CaptureHealth health, boolean replacing) {
        String cid = chainId.value();
        LinkedHashSet<String> union = new LinkedHashSet<>(tables);
        SrsMetaStore.PhysicalSelection current = meta.physicalSelection(cid).orElse(null);
        if (current != null && current.epoch() == epoch) {
            union.addAll(current.tables());
        }
        List<String> requested = meta.requestedPhysicalTables(cid);
        union.addAll(requested);
        for (ConsumerOffset consumer : meta.consumerOffsets(cid)) {
            if (consumer.selectedTables() != null) {
                union.addAll(consumer.selectedTables());
            }
        }
        List<String> physicalTables = union.stream().sorted().toList();
        // The cursors alone, not the whole record: this is read on every run of changes, and the
        // record also carries a schema history that grows per DDL and is never read here.
        Supplier<Collection<ConsumerOffset>> consumers = () -> meta.consumerOffsets(cid);
        SrsLogStore log = hz.getUserContext().get(SRS_LOG_USER_CONTEXT_KEY) instanceof SrsLogStore
                bound ? bound : null;
        PerTableLogTrimmer trimmer = log == null ? null : new PerTableLogTrimmer(consumers, log, epoch);
        Map<String, CdcPhase.TableRoute> routes = new LinkedHashMap<>();
        for (String table : physicalTables) {
            String ringName = SrsRingbuffer.ringName(cid, table);
            SrsRingbuffer ring = new SrsRingbuffer(hz.getRingbuffer(ringName));
            if (meta.establishRingGenerationStartAfter(cid, table, epoch, ring.tailSequence()).isEmpty()) {
                throw new PhysicalRingNotReady(cid, table);
            }
            SrsWriteGate gate = new SrsWriteGate(ring);
            CdcChain chain = new CdcChain(
                    gate, meta, cid, epoch, spec.schemaVer(), spec.captureFence());
            LongConsumer trim = trimmer == null ? seq -> { } : seq -> {
                trimmer.observed(table, ringName, seq);
                if (physicalTables.size() == 1) {
                    trimmer.trim();
                }
            };
            routes.put(table, new CdcPhase.TableRoute(chain, consumers, trim));
        }
        CaptureStart minerStart = tailStart(
                meta, cid, spec.pipelineId(), ownSeam, CaptureStart.present());
        refuseAnInstantThisBufferWillNeverReach(spec.startFrom(), minerStart, spec.retention());
        PhysicalSourcePrefix prefix = physicalTables.size() > 1
                ? new PhysicalSourcePrefix(meta, cid, epoch, health,
                        trimmer == null ? offsets -> { } : trimmer::trim) : null;
        if (current != null && current.epoch() == epoch
                && !current.tables().equals(physicalTables) && !replacing) {
            if (prefix != null) {
                prefix.close();
            }
            throw new TapstateException(CaptureError.SHARED_SELECTION_RESTART_REQUIRED,
                    Map.of("chain", cid), null);
        }
        CaptureConfig physicalConfig = new CaptureConfig(spec.config().connectorId(),
                spec.config().settings(), physicalTables, spec.config().node());
        SrsMeta stored = meta.read(cid).orElseThrow();
        if (stored.epoch() != epoch || (physicalTables.size() == 1 && stored.sourceRead() == null
                && epoch > 1 && !meta.physicalPrefixTrusted(cid))) {
            if (prefix != null) {
                prefix.close();
            }
            throw new TapstateException(CaptureError.SHARED_POSITION_UNVERIFIED,
                    Map.of("chain", cid), null);
        }
        Consumer<Optional<SourcePosition>> singleTableAnchor = physicalTables.size() == 1
                ? start -> {
                    String token = start.map(SourcePosition::token).orElseThrow(() ->
                            new TapstateException(CaptureError.RESUME_ANCHOR_UNAVAILABLE,
                                    Map.of("chain", cid), null));
                    if (meta.read(cid).orElseThrow().sourceRead() == null
                            && !meta.establishPhysicalAnchor(cid,
                                    new ChainPosition(new SourceOrder(epoch, -1L), token))) {
                        throw new TapstateException(CaptureError.RESUME_ANCHOR_UNAVAILABLE,
                                Map.of("chain", cid), null);
                    }
                } : null;
        Subscription source = CdcPhase.run(port, physicalConfig, minerStart, routes, health, prefix,
                singleTableAnchor);
        try {
            boolean published;
            if (current != null && current.epoch() == epoch) {
                published = current.tables().equals(physicalTables)
                        || meta.replacePhysicalSelection(cid, current,
                                new SrsMetaStore.PhysicalSelection(
                                        epoch, Math.addExact(current.revision(), 1L), physicalTables));
            } else {
                published = meta.publishPhysicalSelection(cid,
                        new SrsMetaStore.PhysicalSelection(epoch, physicalTables));
            }
            if (!published) {
                throw new TapstateException(CaptureError.SHARED_SELECTION_RESTART_REQUIRED,
                        Map.of("chain", cid), null);
            }
            if (!requested.isEmpty()) {
                meta.clearPhysicalRequests(cid, epoch, physicalTables);
            }
            return source;
        } catch (RuntimeException | Error failure) {
            try {
                source.close();
            } catch (RuntimeException unclosed) {
                failure.addSuppressed(unclosed);
            }
            throw failure;
        }
    }

    private Subscription startDirectTail(CaptureRunSpec spec, MiningChainId chainId, long epoch,
            String ownSeam, CaptureHealth health, Consumer<Envelope> passthrough) {
        String cid = chainId.value();
        Supplier<Collection<ConsumerOffset>> consumers = () -> meta.consumerOffsets(cid);
        AtomicLong forwarded = new AtomicLong();
        AtomicReference<ChainPosition> lastWritten = new AtomicReference<>();
        AtomicBoolean anchored = new AtomicBoolean();
        return port.cdc(spec.config(), tailStart(
                        meta, cid, spec.pipelineId(), ownSeam, sourceStart(spec.startFrom())),
                health.recording(new CaptureListener() {
                    @Override
                    public void onStart(Optional<SourcePosition> position) {
                        String token = position.map(SourcePosition::token).orElseThrow(() ->
                                new TapstateException(CaptureError.RESUME_ANCHOR_UNAVAILABLE,
                                        Map.of("chain", cid), null));
                        if (meta.read(cid).orElseThrow().sourceRead() == null
                                && !meta.establishPhysicalAnchor(cid,
                                        new ChainPosition(new SourceOrder(epoch, -1L), token))) {
                            throw new TapstateException(CaptureError.RESUME_ANCHOR_UNAVAILABLE,
                                    Map.of("chain", cid), null);
                        }
                        anchored.set(true);
                    }

                    @Override
                    public void onBatch(List<Envelope> events, Optional<SourcePosition> position) {
                        if (!anchored.get()) {
                            throw new TapstateException(CaptureError.RESUME_ANCHOR_UNAVAILABLE,
                                    Map.of("chain", cid), null);
                        }
                        forwardDirect(events, position, cid, epoch, forwarded,
                                consumers, lastWritten, passthrough);
                    }
                }));
    }

    /**
     * Forwards one run of changes straight to the consumer and records how far the source has been read.
     *
     * <p>Each change is stamped with its order before it leaves. A direct tail has no ring, and a buffered
     * change takes its order from the ring's sequence, so the count of changes this run has forwarded
     * stands in for it: monotonic within the generation the chain opened, and taken afresh whenever a new
     * one is. Leaving the order off is not the neutral choice it looks like -- every node that ranks
     * positions drops one carrying none, so an unstamped tail is one nothing downstream can ever confirm,
     * and an account nothing confirms never moves.
     *
     * <p><strong>A forwarded count and a ring sequence are not the same quantity.</strong> A chain read
     * both ways at once therefore has two consumers counting differently, and the only thing ever done
     * with the two is to take the lower: the chain reads as the slower of them, which re-mines more than
     * it has to and can never skip. That direction is the one that cannot lose data, which is why the
     * mismatch is affordable and worth saying out loud.
     *
     * <p>The position the source named for the run rides with the change that closes it and no other,
     * exactly as it does through the ring. Carried on the earlier ones it would say of each that the source
     * had already read past the last, and a run interrupted between them would resume past changes never
     * delivered.
     *
     * <p>The offset then advances, clamped so it never passes what a consumer has durably landed. A direct
     * tail buffers nothing, so a change it forwarded that no sink wrote is gone with the process; an offset
     * that had passed it would step over it on the way back, and nothing would ever fetch it again.
     */
    /**
     * Forwards one run of changes straight to the consumer and records how far the source has been read.
     *
     * <p>Each change is stamped with its order before it leaves. A direct tail has no ring, and a buffered
     * change takes its order from the ring's sequence, so the count of changes this run has forwarded
     * stands in for it: monotonic within the generation the chain opened, and taken afresh whenever a new
     * one is. Leaving the order off is not the neutral choice it looks like -- every node that ranks
     * positions drops one carrying none, so an unstamped tail is one nothing downstream can ever confirm,
     * and an account nothing confirms never moves.
     *
     * <p><strong>A forwarded count and a ring sequence are not the same quantity.</strong> A chain read
     * both ways at once therefore has two consumers counting differently, and the only thing ever done
     * with the two is to take the lower: the chain reads as the slower of them, which re-mines more than
     * it has to and can never skip. That direction is the one that cannot lose data, which is why the
     * mismatch is affordable and worth saying out loud.
     *
     * <p>The position the source named for the run rides with the change that closes it and no other,
     * exactly as it does through the ring. Carried on the earlier ones it would say of each that the source
     * had already read past the last, and a run interrupted between them would resume past changes never
     * delivered.
     *
     * <p>The offset then advances, clamped so it never passes what a consumer has durably landed. A direct
     * tail buffers nothing, so a change it forwarded that no sink wrote is gone with the process; an offset
     * that had passed it would step over it on the way back, and nothing would ever fetch it again.
     */
    private void forwardDirect(
            List<Envelope> events,
            Optional<SourcePosition> position,
            String miningChainId,
            long epoch,
            AtomicLong forwarded,
            Supplier<Collection<ConsumerOffset>> consumers,
            AtomicReference<ChainPosition> lastWritten,
            Consumer<Envelope> passthrough) {
        if (events.isEmpty()) {
            // The source handed over only events carrying no change -- a heartbeat and its like. There is
            // nothing to forward, and nothing has been read past, so the offset does not move either.
            return;
        }
        int last = events.size() - 1;
        String token = position.map(SourcePosition::token).orElse(null);
        long closingSeq = -1;
        for (int i = 0; i < events.size(); i++) {
            closingSeq = forwarded.getAndIncrement();
            passthrough.accept(events.get(i).withPosition(
                    new ChainPosition(new SourceOrder(epoch, closingSeq), i == last ? token : null)));
        }
        ChainPosition read = new ChainPosition(new SourceOrder(epoch, closingSeq), token);
        SrsDurableFrontier.safeAdvance(read, consumers.get()).ifPresent(safe -> {
            // Unchanged from the run before means the slowest sink has landed nothing since, so this would
            // write the record the value it already holds -- a synchronous round trip, on the thread the
            // source reads on, to say nothing. The pair is compared, not the token alone: a token that
            // repeats across generations is a different position, and comparing halves would skip it.
            if (safe.equals(lastWritten.get())) {
                return;
            }
            meta.advanceSourceReadOffset(miningChainId, safe);
            lastWritten.set(safe);
        });
    }

    private RuntimeException rollbackStartFailure(
            MiningChainId chainId,
            String pipelineId,
            boolean chainCreated,
            boolean consumerAttached,
            Optional<Subscription> subscription) {
        RuntimeException failure = null;
        if (subscription.isPresent()) {
            failure = runCleanup(subscription.get()::close, failure);
        }
        if (chainId != null && consumerAttached) {
            failure = runCleanup(() -> coordinator.detachConsumer(chainId, pipelineId), failure);
        }
        if (chainId != null && chainCreated) {
            failure = runCleanup(() -> coordinator.teardownSource(chainId), failure);
        }
        return failure;
    }

    private static RuntimeException runCleanup(Runnable cleanup, RuntimeException firstFailure) {
        try {
            cleanup.run();
        } catch (RuntimeException failure) {
            if (firstFailure == null) {
                return failure;
            }
            firstFailure.addSuppressed(failure);
        }
        return firstFailure;
    }

    /**
     * Where this pipeline run's tail begins, read back from the durable record rather than assumed.
     *
     * <p>Four states, in this order, and the order is the whole of it:
     *
     * <ol>
     *   <li>a load that just ran here — {@code ownSnapshotSeam} is where it began, and the tail has to
     *       cover every change since, or a row this load read and the source then changed is left at the
     *       value the load saw;</li>
     *   <li>a recorded read offset — the tail ran before and got this far, so it picks up there;</li>
     *   <li>no read offset but this pipeline's recorded seam — its snapshot ran and the tail has not
     *       advanced past where that snapshot began, so it starts at the seam and the idempotent sink
     *       absorbs the overlap;</li>
     *   <li>none of those — nothing has read this chain, so {@code firstRun} decides: the start the
     *       caller resolved for a run that has no position to pick up from.</li>
     * </ol>
     *
     * <p>This run's own seam outranks the recorded read offset, and the order matters in exactly one
     * shape: a chain someone else is already mining. That offset moves as they mine, so by the time this
     * run's load finishes it can name a point later than the seam this load began at — and starting there
     * skips the changes in between. They are in the shared ring, mined by whoever is already on the
     * chain, but this run's own reader enters that ring at its own cursor and never looks behind it.
     * Starting at the earlier of the two only ever costs an overlap the idempotent sink absorbs.
     *
     * <p>Taking the present in any of the first three states is the silent loss this exists to prevent:
     * the tail comes up healthy, and every change between where it had reached and now is simply gone.
     */
    private static CaptureStart tailStart(
            SrsMetaStore meta,
            String miningChainId,
            String pipelineId,
            String ownSnapshotSeam,
            CaptureStart firstRun) {
        if (ownSnapshotSeam != null) {
            return CaptureStart.resume(new SourcePosition(ownSnapshotSeam));
        }
        return meta.read(miningChainId)
                .map(record -> {
                    if (record.sourceReadOffset() != null) {
                        return CaptureStart.resume(new SourcePosition(record.sourceReadOffset()));
                    }
                    return record.consumerOffset(pipelineId)
                            .map(consumer -> consumer.cdcStartPosition() == null
                                    ? firstRun
                                    : CaptureStart.resume(new SourcePosition(consumer.cdcStartPosition())))
                            .orElse(firstRun);
                })
                .orElse(firstRun);
    }

    /**
     * Refuses a {@code start_from} instant this buffer will never reach back to.
     *
     * <p>It fires in one state and no other: the miner is starting at the present, which is what a chain
     * with nothing recorded resolves to. Nothing from before this moment is buffered, and nothing from
     * before it ever will be, so the ask cannot be met by waiting -- it can only be met by not having
     * asked. The reader that positions the consumer cannot make this call for itself: it sees an empty
     * ring, and an empty ring is the same shape whether the change it wants aged out or has simply not
     * been written yet, so refusing there would race the miner it shares a run with. Here the miner's own
     * start is in hand and there is no race to lose.
     *
     * <p>Every other state is left alone, and deliberately. A miner resuming from a recorded position
     * reaches back to that position -- the source's own opaque token, which says nothing about a moment --
     * so how far back the buffer will go is not knowable here, and a refusal on a guess would fail runs
     * that were going to be served. A start that lands inside a buffer holding something is the reader's
     * refusal instead, which is the exact one: it compares against a change that is really there.
     *
     * <p>Serving such a start rather than refusing it is a silent loss, and the silence is the whole of
     * it: the pipeline comes up, reports running, and reads only what is written from now on, while every
     * change between the instant asked for and the moment it came up is gone with nothing thrown and
     * nothing logged. Where the boundary sits is this member's clock against the instant the author wrote,
     * so a member whose clock disagrees with the source's moves it by that difference -- bounded, named,
     * and not a reason to prefer the silence.
     */
    private static void refuseAnInstantThisBufferWillNeverReach(
            StartFrom startFrom, CaptureStart minerStart, String retention) {
        if (!(startFrom instanceof StartFrom.At at) || !(minerStart instanceof CaptureStart.Present)) {
            return;
        }
        Instant bufferedFrom = Instant.now();
        if (!at.instant().isBefore(bufferedFrom)) {
            return;
        }
        throw new TapstateException(CaptureError.START_FROM_OUTSIDE_WINDOW, Map.of(
                "requested", at.instant().toString(),
                "earliest", bufferedFrom.toString(),
                "retention", retention == null ? "unset" : retention), null);
    }

    /**
     * The start a {@code start_from} setting names in the source's own log, for a tail that reads the
     * source directly. Every form is an ask only the source can answer -- which of its positions is
     * the oldest it still retains, or which one a given moment corresponds to -- so each crosses the
     * port as itself rather than as a position worked out here.
     *
     * <p>It resolves a first run and nothing later: a recorded position outranks it, because the
     * setting says where a read begins rather than where every run of it begins. Applied again on the
     * way back it would re-read the stretch already read after every restart, and asking for the whole
     * source again is a separate request.
     *
     * <p><strong>Only a direct tail resolves it here.</strong> Through the shared buffer the same
     * setting is this one pipeline's cursor into changes already mined, and the miner is shared by
     * every consumer of the chain -- so a miner that honoured one consumer's ask would move where all
     * the others' changes came from. {@code latest} is the one form the two readings agree on only by
     * accident: with no buffer holding what was already mined, "only what is written from now on" and
     * "the source's present moment" are the same point, and through the buffer they are not.
     */
    private static CaptureStart sourceStart(StartFrom startFrom) {
        return switch (startFrom) {
            case StartFrom.Earliest ignored -> CaptureStart.earliest();
            case StartFrom.Latest ignored -> CaptureStart.present();
            case StartFrom.At at -> CaptureStart.at(at.instant());
        };
    }

    /**
     * Marks, for each of the pipeline's tables, where it starts in that table's ring: just past what the ring
     * already holds. Read from the ring itself, which numbers on across rebuilds, so the mark names the same
     * place for every member that reads it. A refusal while the cluster is still forming surfaces as an
     * uncoded failure of this start, which the next pass tries again.
     */
    private void markWhereThisPipelineArrives(
            String chainId, String pipelineId, long epoch, List<String> tables,
            ConsumerOffset previous, boolean snapshot) {
        for (String table : tables) {
            SrsRingbuffer ring = new SrsRingbuffer(hz.getRingbuffer(SrsRingbuffer.ringName(chainId, table)));
            boolean selectedBefore = previous != null
                    && (previous.selectedTables() == null || previous.selectedTables().contains(table));
            boolean completedLoad = !snapshot
                    || (previous != null && previous.snapshotCompletedTables().contains(table));
            long after = ring.tailSequence();
            boolean doneInThisGeneration = previous != null
                    && Objects.equals(previous.selectedTablesEpoch(), epoch)
                    && previous.ringDoneThrough().containsKey(table);
            if (selectedBefore && completedLoad && !doneInThisGeneration) {
                OptionalLong boundary = meta.ringGenerationStartAfter(chainId, table, epoch);
                if (boundary.isEmpty()) {
                    throw new PhysicalRingNotReady(chainId, table);
                }
                after = boundary.getAsLong();
            }
            meta.startRingAfter(chainId, pipelineId, table, epoch, after);
        }
    }

    /**
     * The read-cursor publisher factory for one consumer's reader over one table's ring: carried onto the
     * Jet source, it resolves the coordination store from the member's user context and binds a sink that
     * advances that consumer's durable {@code perTableSeq} as the reader drains, without clobbering its
     * sink-ack. It closes over only the chain, pipeline, table, ring generation and cursor-writer token —
     * never the store — so it stays serializable; a member with no store bound resolves to a no-op sink.
     * The generation and token fence reports from earlier readers.
     */
    public static SrsReadCursorPublisherFactory readCursorPublisher(
            String miningChainId, String pipelineId, String table, long epoch, String cursorWriterToken) {
        return member -> {
            Object bound = member.getUserContext().get(SRS_META_USER_CONTEXT_KEY);
            if (!(bound instanceof SrsMetaStore memberMeta)) {
                return lastReadSeq -> { };
            }
            return lastReadSeq -> memberMeta.advanceConsumerReadSeq(
                    miningChainId, pipelineId, table, epoch, cursorWriterToken, lastReadSeq);
        };
    }
}
