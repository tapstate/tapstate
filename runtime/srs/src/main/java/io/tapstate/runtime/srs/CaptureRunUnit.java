package io.tapstate.runtime.srs;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.pipeline.StreamSource;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.core.model.PipelineNode;
import io.tapstate.spi.capture.CaptureConfig;
import io.tapstate.spi.capture.CaptureListener;
import io.tapstate.spi.capture.CapturePort;
import io.tapstate.spi.capture.CaptureStart;
import io.tapstate.spi.capture.SharedNotes;
import io.tapstate.spi.capture.SourcePosition;
import io.tapstate.spi.capture.Subscription;
import io.tapstate.spi.store.ConsumerOffset;
import io.tapstate.spi.store.SrsLogStore;
import io.tapstate.spi.store.SrsMetaStore;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
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
    /** The fallback for direct callers that do not supply the product's durable per-pipeline generation. */
    private final AtomicLong chainlessSnapshotEpoch = new AtomicLong();

    public CaptureRunUnit(CapturePort port, SrsCoordinator coordinator, SrsMetaStore meta, HazelcastInstance hz) {
        this.port = Objects.requireNonNull(port, "port");
        this.coordinator = Objects.requireNonNull(coordinator, "coordinator");
        this.meta = Objects.requireNonNull(meta, "meta");
        this.hz = Objects.requireNonNull(hz, "hz");
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
        try {
            provisionChain(spec, plan, startTail, state);
            // Opened before the load, not with the tail: the load's rows are this run's too, and an
            // account opened after them would report a run that had read nothing until its first change.
            CaptureHealth health = new CaptureHealth();
            attachConsumer(spec, plan, startTail, tables, state);
            markPipelineArrival(spec, plan, tables, state.chainId);
            Optional<StreamSource<SrsItem>> ringSource = ringSource(spec, plan, tables, state.chainId);
            state.load = openLoad(spec, plan, tables, state.chainId, state.epoch);
            // The seam this run's own load began at, for the tail that follows it -- null when no load ran
            // here. Carried from the phase rather than read back off the chain, because the chain records
            // one seam for however many pipelines load from it: read back, a pipeline new to the chain
            // gets whichever load reached the record first and starts its tail where that one began.
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

    private void markPipelineArrival(
            CaptureRunSpec spec, ConsumptionPlan plan, List<String> tables, MiningChainId chainId) {
        // Where this pipeline starts in each ring, marked now: after it is on the chain, and before its
        // own load reads a row or any tail this run opens mines a change -- so everything it is owed lands
        // above the mark. A cdc-only read from the earliest change or from an instant is placed by that
        // start instead, when its reader opens. A pipeline coming back keeps the place it had.
        if (plan.sharedRing() && (plan.snapshot() || spec.startFrom() instanceof StartFrom.Latest)) {
            markWhereThisPipelineArrives(chainId.value(), spec.pipelineId(), tables);
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
            // Asked before the selection is recorded: a pipeline given back here must not be left on the
            // record as one the reader waits for, reading nothing.
            if (!startTail) {
                askTheReaderToServeIt(state.chainId.value(), state.epoch, tables);
            }
            selectConsumerTables(spec, state);
            // A selected table protects its ring before arrival is sampled. If a writer gets there first,
            // the later sample moves arrival past what it wrote; once this registration lands, the writer
            // is constrained until that sample and its cursor are published together. Raising the floor
            // to -1 also leaves an already advanced cursor where a returning run left it.
            registerConsumerTables(state.chainId.value(), spec.pipelineId(), tables);
        } else if (plan.directTail() && startTail) {
            coordinator.attachConsumer(
                    Objects.requireNonNull(state.chainId, "a tail resolves its chain before it runs"),
                    spec.pipelineId());
            state.consumerAttached = true;
            selectConsumerTables(spec, state);
        }
    }

    /**
     * Records which tables this pipeline reads from the chain, in the generation it attached under, before
     * anything of this run can be acknowledged. The chain's durable prefix is released per table and per
     * consumer, so an acknowledgement that arrived before its reader's selection would be judged against the
     * selection of a reader that is gone.
     */
    private void selectConsumerTables(CaptureRunSpec spec, OpenState state) {
        meta.selectConsumerTables(state.chainId.value(), spec.pipelineId(), spec.chainSelection(), state.epoch);
    }

    /**
     * Asks the chain's reader for every table this source reads, and gives the start back while the reader
     * that is running does not read them yet: the source is not reading those tables at all, so their rings
     * would stay empty and the pipeline would run healthy over tables that never change -- and a load of them
     * would miss every change made before the reader took them on.
     *
     * <p>The request goes in first. A reader that has not published yet cannot publish without it, so it is
     * safe to go on; one that has published before it is found not to serve the tables, and takes them on the
     * next time it looks.
     */
    private void askTheReaderToServeIt(String chainId, long epoch, List<String> tables) {
        meta.requestPhysicalTables(chainId, epoch, tables);
        meta.physicalSelection(chainId)
                .filter(published -> published.epoch() == epoch && !published.tables().containsAll(tables))
                .ifPresent(published -> {
                    throw new ReaderNotServingYet(chainId, tables);
                });
    }

    private Optional<StreamSource<SrsItem>> ringSource(
            CaptureRunSpec spec, ConsumptionPlan plan, List<String> tables, MiningChainId chainId) {
        if (!plan.sharedRing()) {
            return Optional.empty();
        }
        String firstTable = tables.getFirst();
        String firstRing = SrsRingbuffer.ringName(chainId.value(), firstTable);
        return Optional.of(SrsRingSource.create(
                firstRing, spec.startFrom(),
                readCursorPublisher(chainId.value(), spec.pipelineId(), firstTable), spec.retention()));
    }

    private SnapshotPhase.Load openLoad(
            CaptureRunSpec spec,
            ConsumptionPlan plan,
            List<String> tables,
            MiningChainId chainId,
            long epoch) {
        // Which tables a resuming run still owes is asked once, by the snapshot phase, of this
        // pipeline's own record on the chain. A chainless read instead gets a run generation of its own.
        if (!plan.snapshot()) {
            return null;
        }
        if (chainId != null) {
            // Over the notes the tail reads through: the seam a load samples is a position on the stream the
            // tail goes on to read, and sampling one can be what sets that stream up on the source.
            CaptureConfig config = plan.sharedRing()
                    ? spec.config().sharing(sharedNotes(spec, chainId.value())) : spec.config();
            return SnapshotPhase.open(port, config, chainId.value(), spec.pipelineId(), tables, epoch, meta);
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
        private Optional<Subscription> subscription = Optional.empty();
        private SnapshotPhase.Load load;
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
    private Optional<Subscription> openTail(
            CaptureRunSpec spec,
            ConsumptionPlan plan,
            MiningChainId chainId,
            long epoch,
            String ownSeam,
            boolean startTail,
            CaptureHealth health,
            Consumer<Envelope> passthrough) {
        if (!startTail) {
            return Optional.empty();
        }
        if (plan.sharedRing()) {
            return Optional.of(openSharedTail(spec, chainId.value(), epoch, ownSeam, health));
        }
        if (plan.directTail()) {
            return Optional.of(openDirectTail(spec, chainId.value(), epoch, ownSeam, health, passthrough));
        }
        return Optional.empty();
    }

    /**
     * Opens the chain's one reader: a single subscription over every table any pipeline on the chain reads
     * through its ring, each change routed to its table's ring, and the chain's durable positions moved only
     * as the account behind it releases runs everyone has landed. See {@link SharedTail}.
     */
    private Subscription openSharedTail(
            CaptureRunSpec spec, String chainId, long epoch, String ownSeam, CaptureHealth health) {
        SharedTail tail = new SharedTail(spec, chainId, epoch, health);
        tail.open(ownSeam);
        return SourceAcknowledgements.follow(meta, chainId, tail, health);
    }

    /**
     * Has the chain reader {@code run} carries take on every table pipelines have since asked it for, by
     * replacing its subscription within the same generation; answers whether it did. A run that is not the
     * chain's reader, or whose reader has not opened yet, changes nothing: a reader that opens later reads
     * every request when it does.
     */
    public boolean widen(CaptureRun run) {
        Objects.requireNonNull(run, "run");
        return run.cdcSubscription()
                .map(tail -> tail instanceof SourceAcknowledgements.Followed followed ? followed.tail() : tail)
                .filter(SharedTail.class::isInstance)
                .map(tail -> ((SharedTail) tail).widen())
                .orElse(false);
    }

    /**
     * The chain's one reader, as the subscription the run that opened it holds.
     *
     * <p>What it subscribes to is the union of every recorded selection and every request, this pipeline's
     * included -- not only the tables of the source that happened to start it. The chain is read once for
     * everyone on it, so a table another pipeline reads and this one does not still has to reach its ring.
     * The union is published before the stream starts, and a publication that does not include every table
     * requested by then is refused and taken again, so no pipeline arriving meanwhile is left unserved.
     *
     * <p>A pipeline arriving later with a table the reader does not read asks for it, and the reader takes it
     * on by stopping its stream and starting another over the wider union, in the same generation, from
     * where the chain has been released to. What the first stream had read past that point is read again
     * and handed over twice, which idempotent writes absorb; what it had not is read by the second. Rings
     * are kept, and so is every reader of them.
     *
     * <p>Each table's durable log is cut as a run is released, through the last sequence the run reached in
     * that table: everyone who reads that ring has landed everything up to there, and a sequence is only ever
     * compared with another of the same ring.
     */
    private final class SharedTail implements Subscription {

        /** How many times a publication raced by a newer request is taken again before the start gives up. */
        private static final int PUBLISH_ATTEMPTS = 8;

        private final CaptureRunSpec spec;
        private final String chainId;
        private final long epoch;
        private final CaptureHealth health;
        private final SharedNotes notes;
        private SrsMetaStore.PhysicalSelection published;
        /** The stream running now; read without the lock, so an acknowledgement never waits on a widening. */
        private final AtomicReference<Subscription> stream = new AtomicReference<>();
        private boolean closed;

        SharedTail(CaptureRunSpec spec, String chainId, long epoch, CaptureHealth health) {
            this.spec = spec;
            this.chainId = chainId;
            this.epoch = epoch;
            this.health = health;
            this.notes = sharedNotes(spec, chainId);
        }

        /** Opens the generation's first subscription, beginning where {@code ownSeam} or the record says. */
        synchronized void open(String ownSeam) {
            CaptureStart start = tailStart(meta, chainId, spec.pipelineId(), ownSeam, false, CaptureStart.present());
            refuseAnInstantThisBufferWillNeverReach(spec.startFrom(), start, spec.retention());
            for (int attempt = 1; ; attempt++) {
                List<String> tables = physicalSelection(spec, chainId, epoch);
                PhysicalSourcePrefix prefix = prefixOver(tables);
                SrsMetaStore.PhysicalSelection selection = new SrsMetaStore.PhysicalSelection(epoch, tables);
                if (meta.publishPhysicalSelection(chainId, selection)) {
                    begin(selection, prefix, start);
                    return;
                }
                prefix.close();
                if (attempt == PUBLISH_ATTEMPTS) {
                    throw new TapstateException(
                            CaptureError.SHARED_SELECTION_RESTART_REQUIRED, Map.of("chain", chainId), null);
                }
            }
        }

        /** Replaces the running subscription with one over every table asked for since; see the class. */
        synchronized boolean widen() {
            if (closed || stream.get() == null) {
                return false;
            }
            for (int attempt = 1; ; attempt++) {
                List<String> tables = physicalSelection(spec, chainId, epoch);
                if (published.tables().containsAll(tables)) {
                    meta.clearPhysicalRequests(chainId, tables);
                    return false;
                }
                // Stopped on the first attempt; a retry after a refused publication finds nothing left to stop.
                Subscription running = stream.getAndSet(null);
                if (running != null) {
                    running.close();
                }
                PhysicalSourcePrefix prefix = prefixOver(tables);
                SrsMetaStore.PhysicalSelection wider =
                        new SrsMetaStore.PhysicalSelection(epoch, published.revision() + 1, tables);
                if (meta.replacePhysicalSelection(chainId, published, wider)) {
                    begin(wider, prefix,
                            tailStart(meta, chainId, spec.pipelineId(), null, false, CaptureStart.present()));
                    return true;
                }
                prefix.close();
                if (attempt == PUBLISH_ATTEMPTS
                        || meta.physicalSelection(chainId).filter(published::equals).isEmpty()) {
                    // Nothing reads the chain for this generation now; whoever replaced what it published owns it.
                    closed = true;
                    throw new TapstateException(
                            CaptureError.SHARED_SELECTION_RESTART_REQUIRED, Map.of("chain", chainId), null);
                }
            }
        }

        private PhysicalSourcePrefix prefixOver(List<String> tables) {
            SrsLogStore log = hz.getUserContext().get(SRS_LOG_USER_CONTEXT_KEY) instanceof SrsLogStore
                    bound ? bound : null;
            return PhysicalSourcePrefix.shared(meta, chainId, epoch, tables, health, log == null
                    ? (table, seq) -> { }
                    : (table, seq) -> log.trim(SrsRingbuffer.ringName(chainId, table), seq));
        }

        private void begin(SrsMetaStore.PhysicalSelection selection, PhysicalSourcePrefix prefix, CaptureStart start) {
            published = selection;
            // The cursors alone, not the whole record: this is read on every run of changes, and the record
            // also carries a schema history that grows per DDL and is never read here.
            Supplier<Collection<ConsumerOffset>> consumers = () -> meta.consumerOffsets(chainId);
            Map<String, CdcPhase.TableRoute> routes = new LinkedHashMap<>();
            for (String table : selection.tables()) {
                SrsWriteGate gate = new SrsWriteGate(
                        new SrsRingbuffer(hz.getRingbuffer(SrsRingbuffer.ringName(chainId, table))));
                CdcChain chain = new CdcChain(gate, meta, chainId, epoch, spec.schemaVer(), spec.captureFence());
                routes.put(table, new CdcPhase.TableRoute(chain, consumers));
            }
            CaptureConfig physical = spec.config().over(selection.tables()).sharing(notes);
            stream.set(CdcPhase.run(port, physical, start, routes, health, prefix));
            meta.clearPhysicalRequests(chainId, selection.tables());
        }

        /**
         * Hands {@code durable} to the stream running now. A stream replaced by a wider one is told nothing
         * more; the one that replaced it is told the positions from then on, which only move forward.
         */
        @Override
        public void acknowledge(SourcePosition durable) {
            Subscription current = stream.get();
            if (current != null) {
                current.acknowledge(durable);
            }
        }

        @Override
        public synchronized void close() {
            closed = true;
            Subscription current = stream.get();
            if (current != null) {
                current.close();
            }
        }
    }

    /**
     * The notes the chain's one change stream keeps, whichever pipeline opens it: the chain's own, carried over
     * key by key from the notes this pipeline's node kept before there were any, then from the node of the
     * same source in every other pipeline recorded reading the chain through its ring.
     *
     * <p>A pipeline recorded reading the chain directly is left out, as is one with nothing recorded yet: a
     * direct reader's notes are its own, in use, and carrying from them would hand the chain's stream the
     * replication slot another reader is streaming from.
     */
    private SharedNotes sharedNotes(CaptureRunSpec spec, String chainId) {
        PipelineNode own = spec.config().node();
        List<PipelineNode> carriedFrom = new ArrayList<>(List.of(own));
        for (ConsumerOffset consumer : meta.consumerOffsets(chainId)) {
            if (!consumer.pipelineId().equals(spec.pipelineId())
                    && consumer.selectedTables() != null && !consumer.selectedTables().isEmpty()) {
                carriedFrom.add(new PipelineNode(consumer.pipelineId(), own.nodeId()));
            }
        }
        return new SharedNotes(chainId, carriedFrom);
    }

    /**
     * Lets go of what {@code spec}'s source connector set up on the source to read changes, and of the notes
     * it kept to find that again: the chain's, for a source read through the chain's shared ring, and the
     * node's own for one read directly. A read with no tail set nothing up. Answers what the source refused
     * to let go of, for the caller to report.
     *
     * <p>For the caller to call when that state is being cleared: a chain's once the last pipeline reading it
     * clears its own, and a direct reader's whenever its pipeline does. Released while anyone still reads
     * through them, a stream would go on over a slot that is gone.
     */
    public Optional<TapstateException> release(CaptureRunSpec spec) {
        Objects.requireNonNull(spec, "spec");
        ConsumptionPlan plan = ConsumptionPlan.of(spec.readMode(), spec.srsEnabled());
        if (!plan.tail()) {
            return Optional.empty();
        }
        if (!plan.sharedRing()) {
            return port.release(spec.config());
        }
        // Nobody else is on the chain by now, so only this pipeline's own earlier notes can hold what the
        // chain's notes never took over.
        String chainId = MiningChainId.resolve(spec.config(), spec.srsKey()).value();
        return port.release(spec.config().sharing(new SharedNotes(chainId, List.of(spec.config().node()))));
    }

    /**
     * The tables the chain's reader subscribes to: this source's own, every table a pipeline on the chain has
     * recorded selecting through the ring or asked the reader for, and what a reader of this same generation
     * already published.
     */
    private List<String> physicalSelection(CaptureRunSpec spec, String chainId, long epoch) {
        Set<String> union = new TreeSet<>(spec.config().streams());
        for (ConsumerOffset consumer : meta.consumerOffsets(chainId)) {
            if (consumer.selectedTables() != null) {
                union.addAll(consumer.selectedTables());
            }
        }
        union.addAll(meta.requestedPhysicalTables(chainId));
        meta.physicalSelection(chainId)
                .filter(published -> published.epoch() == epoch)
                .ifPresent(published -> union.addAll(published.tables()));
        return List.copyOf(union);
    }

    /**
     * Opens a direct tail: the source streamed straight to this one pipeline, with no shared ring.
     *
     * <p>The ring is the whole of what the flag decides -- the chain is open and its record is kept either
     * way -- so this tail begins where that record says, exactly as a buffered one does. Taking the present
     * here instead is a silent loss: the tail comes up healthy and every change between where it had reached
     * and now is gone. Its runs go through an account of their own, owed to this pipeline alone, so a
     * position is written down only once every change before it landed on every table the tail reads.
     */
    private Subscription openDirectTail(CaptureRunSpec spec, String chainId, long epoch, String ownSeam,
            CaptureHealth health, Consumer<Envelope> passthrough) {
        PhysicalSourcePrefix prefix = PhysicalSourcePrefix.direct(meta, chainId, epoch, spec.pipelineId(), health);
        AtomicLong forwarded = new AtomicLong();
        Subscription stream;
        try {
            stream = port.cdc(spec.config(),
                    tailStart(meta, chainId, spec.pipelineId(), ownSeam, true, sourceStart(spec.startFrom())),
                    health.recording(new CaptureListener() {
                        @Override
                        public void onStart(Optional<SourcePosition> position) {
                            prefix.start(position);
                        }

                        @Override
                        public void onBatch(List<Envelope> events, Optional<SourcePosition> position) {
                            forwardDirect(events, position, epoch, forwarded, prefix, passthrough);
                        }
                    }));
        } catch (RuntimeException | Error failure) {
            prefix.close();
            throw failure;
        }
        return SourceAcknowledgements.follow(meta, chainId, CdcPhase.closingWith(stream, prefix), health);
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
     * <p>The run is then recorded with the tail's account, with the last count it reached in each table, and
     * the offset moves only once the pipeline's sink has landed everything up to it on every table. A direct
     * tail buffers nothing, so a change it forwarded that no sink wrote is gone with the process; an offset
     * that had passed it would step over it on the way back, and nothing would ever fetch it again. A run
     * carrying no change is recorded when it names a position, for the same reason the shared reader records
     * one: it may be the only position a quiet source ever names.
     */
    private static void forwardDirect(
            List<Envelope> events,
            Optional<SourcePosition> position,
            long epoch,
            AtomicLong forwarded,
            PhysicalSourcePrefix prefix,
            Consumer<Envelope> passthrough) {
        String token = position.map(SourcePosition::token).orElse(null);
        if (events.isEmpty() && token == null) {
            return;
        }
        prefix.checkStillRecording();
        try {
            int last = events.size() - 1;
            Map<String, Long> lastSeqByTable = new LinkedHashMap<>();
            for (int i = 0; i < events.size(); i++) {
                long seq = forwarded.getAndIncrement();
                Envelope event = events.get(i);
                passthrough.accept(event.withPosition(
                        new ChainPosition(new SourceOrder(epoch, seq), i == last ? token : null)));
                lastSeqByTable.put(event.src(), seq);
            }
            prefix.admitted(lastSeqByTable, token);
        } catch (RuntimeException | Error failure) {
            // Handed over and not recorded whole: nothing after it may be released past it.
            prefix.abandon(failure);
            throw failure;
        }
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
     *   <li>a recorded resume point — the chain's reader ran before and got this far, so it picks up there.
     *       That is the last released run that carried a change, or where the stream began: a run carrying no
     *       change moves how far the source may release, but a source can name it in a form that, resumed
     *       from, passes over the change that follows. Every change after it is still owed to somebody on
     *       the chain -- a pipeline stopped with its state kept among them -- so it outranks even a load that
     *       just ran here: that load's seam was sampled moments ago, and starting there would skip every
     *       change between where the holder stopped and that seam, for the holder and for good. What this
     *       run's own load already covered of that stretch is delivered again, which the idempotent sink
     *       absorbs; a source whose log no longer reaches back that far refuses the start, as it would
     *       refuse the holder's;</li>
     *   <li>a load that just ran here, on a chain with nothing recorded — {@code ownSnapshotSeam} is where it
     *       began, and the tail has to cover every change since, or a row this load read and the source then
     *       changed is left at the value the load saw;</li>
     *   <li>no read offset but this pipeline's recorded seam — its snapshot ran and the tail has not
     *       advanced past where that snapshot began, so it starts at the seam and the idempotent sink
     *       absorbs the overlap. A direct tail that loads nothing records where it began here too, when
     *       another pipeline is on the chain and its start is therefore not written down as the chain's;</li>
     *   <li>none of those — nothing has read this chain, so {@code firstRun} decides: the start the
     *       caller resolved for a run that has no position to pick up from.</li>
     * </ol>
     *
     * <p>That order is the shared reader's. The resume point cannot have run past this run's own seam while its
     * load ran: the pipeline is on the chain, with the tables it reads selected and asked of the reader, before
     * its load samples the seam, so every run handed over after that owes it those tables' changes and is not
     * released before it lands them, and a direct tail starting beside it does not write its start down as the
     * chain's position.
     *
     * <p>A direct tail ({@code seamFirst}) puts its own seam first instead. It serves its own pipeline alone,
     * and the chain's runs do not wait for that pipeline: a shared reader on the same chain may have moved the
     * resume point past the seam while the load ran, and beginning there would skip, for this pipeline, the
     * changes in between, which its load did not cover.
     *
     * <p>Taking the present in any of the first three states is the silent loss this exists to prevent:
     * the tail comes up healthy, and every change between where it had reached and now is simply gone.
     */
    private static CaptureStart tailStart(
            SrsMetaStore meta,
            String miningChainId,
            String pipelineId,
            String ownSnapshotSeam,
            boolean seamFirst,
            CaptureStart firstRun) {
        if (seamFirst && ownSnapshotSeam != null) {
            return CaptureStart.resume(new SourcePosition(ownSnapshotSeam));
        }
        Optional<String> resumeFrom = meta.resumeOffset(miningChainId);
        if (resumeFrom.isPresent()) {
            return CaptureStart.resume(new SourcePosition(resumeFrom.get()));
        }
        if (ownSnapshotSeam != null) {
            return CaptureStart.resume(new SourcePosition(ownSnapshotSeam));
        }
        return meta.read(miningChainId)
                .flatMap(record -> record.consumerOffset(pipelineId))
                .map(consumer -> consumer.cdcStartPosition() == null
                        ? firstRun
                        : CaptureStart.resume(new SourcePosition(consumer.cdcStartPosition())))
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
    private void markWhereThisPipelineArrives(String chainId, String pipelineId, List<String> tables) {
        for (String table : tables) {
            SrsRingbuffer ring = new SrsRingbuffer(hz.getRingbuffer(SrsRingbuffer.ringName(chainId, table)));
            meta.startRingAfter(chainId, pipelineId, table, ring.tailSequence());
        }
    }

    /**
     * The read-cursor publisher factory for one consumer's reader over one table's ring: carried onto the
     * Jet source, it resolves the coordination store from the member's user context and binds a sink that
     * advances that consumer's durable {@code perTableSeq} as the reader drains, without clobbering its
     * sink-ack. It closes over only the chain, pipeline and table coordinates — never the store — so it
     * stays serializable; a member with no store bound resolves to a no-op sink.
     */
    public static SrsReadCursorPublisherFactory readCursorPublisher(
            String miningChainId, String pipelineId, String table) {
        return member -> {
            Object bound = member.getUserContext().get(SRS_META_USER_CONTEXT_KEY);
            if (!(bound instanceof SrsMetaStore memberMeta)) {
                return lastReadSeq -> { };
            }
            return lastReadSeq -> memberMeta.advanceConsumerReadSeq(miningChainId, pipelineId, table, lastReadSeq);
        };
    }
}
