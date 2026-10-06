package io.tapstate.app;

import io.tapstate.adapters.pdk.ConnectorStateNamespace;
import io.tapstate.adapters.pdk.PdkCapturePort;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.Op;
import io.tapstate.core.model.PipelineNode;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.ReadMode;
import io.tapstate.core.model.Settings;
import io.tapstate.core.model.SourceRef;
import io.tapstate.core.model.SourceResource;
import io.tapstate.runtime.srs.CaptureHandoff;
import io.tapstate.runtime.srs.CaptureHealth;
import io.tapstate.runtime.srs.CaptureRun;
import io.tapstate.runtime.srs.CaptureError;
import io.tapstate.runtime.srs.CaptureId;
import io.tapstate.runtime.srs.CaptureRunSpec;
import io.tapstate.runtime.srs.MiningChainId;
import io.tapstate.runtime.srs.SnapshotBuffer;
import io.tapstate.runtime.srs.SnapshotPhase;
import io.tapstate.runtime.srs.SrsCoordinator;
import io.tapstate.runtime.srs.StartFrom;
import io.tapstate.spi.capture.CapturePlan;
import io.tapstate.spi.store.ArtifactStore;
import io.tapstate.spi.store.ConsumerOffset;
import io.tapstate.spi.store.SrsMeta;
import io.tapstate.spi.store.SrsConsumerId;
import io.tapstate.spi.store.SourceModel;
import io.tapstate.spi.store.SourceTable;
import io.tapstate.spi.store.StorePort;
import io.tapstate.core.lifecycle.CaptureReading;
import io.tapstate.core.lifecycle.SnapshotReading;
import io.tapstate.core.lifecycle.TableSnapshot;

import java.time.Duration;
import java.time.Instant;

import java.util.ArrayList;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The store-backed capture coordinator: it resolves a pipeline and the sources it reads from the store,
 * attaches every pipeline to its source contract and holds the live handles so a stop can tear them down.
 * One local run owns the shared tail for a normalized source/read contract; later local pipelines attach
 * without opening the source again. In cluster mode, a CAPTURE claim prevents another member from tailing it.
 * The claim decides who tails a source and nothing about how a pipeline reads it: a pipeline driven by a
 * member that does not hold the claim attaches there exactly as it would beside the tail -- its own load
 * where its record says one is owed, then the changes the holder's tail writes into the shared ring.
 * It derives each source run spec identically to how the topology builder derives the ring the run fills,
 * through the shared source resolution, so the capture and the reader agree on the ring.
 *
 * <p>No positions are supplied here. A run's seam and its per-change positions are the source's own and are
 * learned from it as the read happens, so there is nothing for this layer to stand in with. Snapshot rows
 * drain to a shared buffer keyed by the source's change-ring name; the source vertex reading that ring
 * drains the buffer and emits its rows through the same transform-to-sink chain as cdc, strictly before it.
 */
final class StoreBackedPipelineCaptureCoordinator implements PipelineCaptureCoordinator {

    private static final Logger LOG = LoggerFactory.getLogger(StoreBackedPipelineCaptureCoordinator.class);

    /** The schema version stamped on ring items at L1 (schema evolution is a later increment). */
    private static final long MOCK_SCHEMA_VER = 0L;

    private final StorePort storePort;
    private final CaptureStarter captureStarter;
    private final CaptureAttacher captureAttacher;
    private final CaptureOwnership ownership;
    private final Duration claimRenewInterval;
    private final boolean managedOwnership;
    private final SrsCoordinator srsCoordinator;
    private final SnapshotBuffer snapshotBuffer;
    private final Map<String, List<PipelineRun>> runsByPipeline = new ConcurrentHashMap<>();
    private final Map<CaptureId, OwnedCapture> ownedCaptures = new ConcurrentHashMap<>();
    private final KeyedLocks<String> pipelineLocks = new KeyedLocks<>();
    private final KeyedLocks<CaptureId> captureLocks = new KeyedLocks<>();
    private final Map<String, PendingStart> pendingStarts = new ConcurrentHashMap<>();
    /** One aborted assembly per pipeline, retained only until its actual old handles close. */
    private final Map<String, List<PipelineRun>> abortedRunsByPipeline = new ConcurrentHashMap<>();
    private static final Duration SHUTDOWN_BUDGET = Duration.ofSeconds(30);
    private final Object startGate = new Object();
    private final CompletableFuture<Void> shutdown = new CompletableFuture<>();
    private final LifecycleWorkDispatcher shutdownDispatcher;
    private final Duration shutdownBudget;
    private volatile boolean closing;
    private volatile long shutdownDeadline;

    /**
     * The pipelines here reading a capture another member holds, by capture. Kept for the moment this
     * member takes such a capture over, by a start of its own or because nobody tails it any more: its
     * tail then runs for these pipelines as well, and has to outlast the last of them rather than the
     * pipeline that happened to take the claim.
     */
    private final Map<CaptureId, JoinedCapture> joinedCaptures = new ConcurrentHashMap<>();

    /** The captures starts here were given back over, for as long as they are still not ready. */
    private final Map<CaptureId, RingWait> ringWaits = new ConcurrentHashMap<>();

    /** Looks for captures pipelines here read and nobody tails; started with the first capture joined. */
    private volatile ScheduledExecutorService takeovers;

    private final Map<CaptureId, RingWait> servingWaits = new ConcurrentHashMap<>();

    /** How far each running pipeline's load has got, keyed by pipeline; dropped when it stops. */
    private final Map<String, PipelineLoad> loadsByPipeline = new ConcurrentHashMap<>();
    private final Map<String, LiveSnapshot> liveSnapshotsByPipeline = new ConcurrentHashMap<>();

    /**
     * The tables each running pipeline's snapshot covers, per chain; dropped when it stops. Only the
     * durable record can say whether a load reached the target, and it answers per pipeline, per chain,
     * per table -- so what is kept here is the question rather than the answer: which tables to ask about
     * and on which chain to ask. The load above cannot stand in for it. Its published tables are keyed by
     * table name, qualified on collision, and a table still being read has no entry yet.
     */
    private final Map<String, List<SnapshotOnChain>> snapshotTablesByPipeline = new ConcurrentHashMap<>();

    StoreBackedPipelineCaptureCoordinator(
            StorePort storePort, CaptureStarter captureStarter, SrsCoordinator srsCoordinator,
            SnapshotBuffer snapshotBuffer) {
        this(storePort, captureStarter, srsCoordinator, snapshotBuffer, LifecycleWorkDispatcher.inline(), SHUTDOWN_BUDGET);
    }

    StoreBackedPipelineCaptureCoordinator(
            StorePort storePort, CaptureStarter captureStarter, SrsCoordinator srsCoordinator,
            SnapshotBuffer snapshotBuffer, LifecycleWorkDispatcher shutdownDispatcher, Duration shutdownBudget) {
        this.shutdownDispatcher = Objects.requireNonNull(shutdownDispatcher, "shutdownDispatcher");
        this.shutdownBudget = positiveShutdownBudget(shutdownBudget);
        this.storePort = Objects.requireNonNull(storePort, "storePort");
        this.captureStarter = Objects.requireNonNull(captureStarter, "captureStarter");
        this.captureAttacher = null;
        this.ownership = CaptureOwnership.single();
        this.claimRenewInterval = Duration.ZERO;
        this.managedOwnership = false;
        this.srsCoordinator = Objects.requireNonNull(srsCoordinator, "srsCoordinator");
        this.snapshotBuffer = Objects.requireNonNull(snapshotBuffer, "snapshotBuffer");
    }

    /** A single member shares its tails locally, without a cluster claim or lease renewal. */
    StoreBackedPipelineCaptureCoordinator(
            StorePort storePort, CaptureAttacher captureAttacher, SrsCoordinator srsCoordinator,
            SnapshotBuffer snapshotBuffer) {
        this(storePort, captureAttacher, srsCoordinator, snapshotBuffer,
                CaptureOwnership.single(), Duration.ZERO);
    }

    StoreBackedPipelineCaptureCoordinator(
            StorePort storePort, CaptureAttacher captureAttacher, SrsCoordinator srsCoordinator,
            SnapshotBuffer snapshotBuffer, LifecycleWorkDispatcher shutdownDispatcher) {
        this(storePort, captureAttacher, srsCoordinator, snapshotBuffer, CaptureOwnership.single(), Duration.ZERO,
                shutdownDispatcher, SHUTDOWN_BUDGET);
    }

    StoreBackedPipelineCaptureCoordinator(
            StorePort storePort,
            CaptureAttacher captureAttacher,
            SrsCoordinator srsCoordinator,
            SnapshotBuffer snapshotBuffer,
            CaptureOwnership ownership,
            Duration claimRenewInterval) {
        this(storePort, captureAttacher, srsCoordinator, snapshotBuffer, ownership, claimRenewInterval,
                LifecycleWorkDispatcher.inline(), SHUTDOWN_BUDGET);
    }

    StoreBackedPipelineCaptureCoordinator(
            StorePort storePort, CaptureAttacher captureAttacher, SrsCoordinator srsCoordinator,
            SnapshotBuffer snapshotBuffer, CaptureOwnership ownership, Duration claimRenewInterval,
            LifecycleWorkDispatcher shutdownDispatcher) {
        this(storePort, captureAttacher, srsCoordinator, snapshotBuffer, ownership, claimRenewInterval,
                shutdownDispatcher, SHUTDOWN_BUDGET);
    }

    StoreBackedPipelineCaptureCoordinator(
            StorePort storePort, CaptureAttacher captureAttacher, SrsCoordinator srsCoordinator,
            SnapshotBuffer snapshotBuffer, CaptureOwnership ownership, Duration claimRenewInterval,
            LifecycleWorkDispatcher shutdownDispatcher, Duration shutdownBudget) {
        this.shutdownDispatcher = Objects.requireNonNull(shutdownDispatcher, "shutdownDispatcher");
        this.shutdownBudget = positiveShutdownBudget(shutdownBudget);
        this.storePort = Objects.requireNonNull(storePort, "storePort");
        this.captureStarter = null;
        this.captureAttacher = Objects.requireNonNull(captureAttacher, "captureAttacher");
        this.ownership = Objects.requireNonNull(ownership, "ownership");
        this.claimRenewInterval = Objects.requireNonNull(claimRenewInterval, "claimRenewInterval");
        this.managedOwnership = true;
        this.srsCoordinator = Objects.requireNonNull(srsCoordinator, "srsCoordinator");
        this.snapshotBuffer = Objects.requireNonNull(snapshotBuffer, "snapshotBuffer");
    }

    private static Duration positiveShutdownBudget(Duration budget) {
        Objects.requireNonNull(budget, "shutdownBudget");
        if (budget.isNegative() || budget.isZero()) { throw new IllegalArgumentException("shutdown budget must be positive"); }
        return budget;
    }

    private void checkOpen() {
        if (closing) { throw new CancellationException("capture coordinator is closing"); }
    }

    @Override
    public void startCapture(String pipelineId) {
        checkOpen();
        startCapture(pipelineId, artifacts());
    }

    @Override
    public void startCapture(String pipelineId, ArtifactStore artifactSnapshot) {
        startCapture(pipelineId, artifactSnapshot, UUID.randomUUID().toString());
    }

    @Override
    public void startCapture(
            String pipelineId, ArtifactStore artifactSnapshot, String cursorWriterToken) {
        try (KeyedLocks.Hold<String> ignored = pipelineLocks.acquireInterruptibly(pipelineId)) {
            checkOpen();
            closeAbortedCapture(pipelineId, abortedRunsByPipeline.get(pipelineId));
            PendingStart pending;
            synchronized (startGate) {
                checkOpen();
                // A duplicate start observes the first one's published handles rather than opening its sources.
                if (runsByPipeline.containsKey(pipelineId)) { return; }
                pending = new PendingStart(Thread.currentThread());
                pendingStarts.put(pipelineId, pending);
            }
            try {
                startCaptureLocked(pipelineId, artifactSnapshot, cursorWriterToken, pending);
            } catch (RuntimeException | Error failure) {
                pending.failure = failure;
                throw failure;
            } finally {
                pending.finish();
                pendingStarts.remove(pipelineId, pending);
            }
        }
    }

    private static final class PendingStart {
        private final Thread thread;
        private volatile Set<CaptureId> captures = Set.of();
        private boolean cancelled;
        private boolean finished;
        private volatile Throwable failure;

        private PendingStart(Thread thread) {
            this.thread = thread;
        }

        void covers(Collection<CaptureId> captureIds) {
            captures = Set.copyOf(captureIds);
        }

        boolean covers(CaptureId captureId) {
            return captures.contains(captureId);
        }

        synchronized void cancel() {
            if (!finished) {
                cancelled = true;
                thread.interrupt();
            }
        }

        synchronized void finish() {
            finished = true;
        }

        synchronized void check() {
            if (cancelled || thread.isInterrupted()) {
                throw new CancellationException("capture start cancelled");
            }
        }
    }

    private List<KeyedLocks.Hold<CaptureId>> lockCaptures(
            Collection<CaptureId> captureIds, boolean interruptibly) {
        List<KeyedLocks.Hold<CaptureId>> holds = new ArrayList<>();
        try {
            captureIds.stream().distinct().sorted(Comparator.comparing(CaptureId::value))
                    .forEach(captureId -> holds.add(interruptibly
                            ? captureLocks.acquireInterruptibly(captureId) : captureLocks.acquire(captureId)));
            return holds;
        } catch (RuntimeException | Error failure) {
            releaseCaptures(holds);
            throw failure;
        }
    }

    private static void releaseCaptures(List<KeyedLocks.Hold<CaptureId>> holds) {
        for (int i = holds.size() - 1; i >= 0; i--) {
            holds.get(i).close();
        }
    }

    /** A short-lived lock per live key, without retaining every pipeline or capture ever encountered. */
    private static final class KeyedLocks<K> {
        private final ConcurrentHashMap<K, Cell> cells = new ConcurrentHashMap<>();

        private static final class Cell {
            private final ReentrantLock lock = new ReentrantLock();
            private int users;
        }

        private Cell retain(K key) {
            return cells.compute(key, (ignored, current) -> {
                Cell cell = current == null ? new Cell() : current;
                cell.users++;
                return cell;
            });
        }

        private void release(K key, Cell expected) {
            cells.compute(key, (ignored, current) -> {
                if (current != expected || current.users <= 0) {
                    throw new IllegalStateException("keyed lock reference changed while held");
                }
                return --current.users == 0 ? null : current;
            });
        }

        Hold<K> acquire(K key) {
            Cell cell = retain(key);
            cell.lock.lock();
            return new Hold<>(this, key, cell);
        }

        Hold<K> acquireInterruptibly(K key) {
            Cell cell = retain(key);
            try {
                cell.lock.lockInterruptibly();
            } catch (InterruptedException interrupted) {
                release(key, cell);
                Thread.currentThread().interrupt();
                throw new CancellationException("capture start interrupted while waiting for a lock");
            }
            return new Hold<>(this, key, cell);
        }

        Hold<K> tryAcquire(K key) {
            Cell cell = retain(key);
            if (!cell.lock.tryLock()) {
                release(key, cell);
                return null;
            }
            return new Hold<>(this, key, cell);
        }

        private static final class Hold<K> implements AutoCloseable {
            private final KeyedLocks<K> owner;
            private final K key;
            private final Cell cell;
            private boolean closed;

            private Hold(KeyedLocks<K> owner, K key, Cell cell) {
                this.owner = owner;
                this.key = key;
                this.cell = cell;
            }

            @Override
            public void close() {
                if (!closed) {
                    closed = true;
                    cell.lock.unlock();
                    owner.release(key, cell);
                }
            }
        }
    }

    private void startCaptureLocked(
            String pipelineId, ArtifactStore artifactSnapshot, String cursorWriterToken, PendingStart pending) {
        pending.check();
        ArtifactStore captured = Objects.requireNonNull(artifactSnapshot, "artifactSnapshot");
        PipelineResource pipeline = StoredArtifacts.requirePipeline(captured, pipelineId);
        ReadMode readMode = readModeOf(pipeline.settings());
        // A snapshot-only run has no change chain to supply a generation. Advance its own durable order
        // once for the whole pipeline run and above every retained chain generation, so every source in
        // one assembly is comparable and the first run after a mode switch also outranks preserved state.
        long snapshotEpoch = readMode == ReadMode.SNAPSHOT_ONLY
                ? SnapshotRunOrder.next(storePort.keyedState(), pipelineId, retainedChainGeneration(pipelineId))
                : 1L;
        // Every source's capture is settled before any source is opened, so a start that has to be given
        // back is given back having opened nothing: a source opened first would otherwise read its whole
        // load again on every pass until the last one was ready.
        List<SourcePlan> plans = withSnapshotTokens(
                plan(pipelineId, pipeline, captured, snapshotEpoch),
                Objects.requireNonNull(cursorWriterToken, "cursorWriterToken"));
        pending.covers(plans.stream().map(SourcePlan::captureId).toList());
        // Capture locks are sorted so multi-source starts cannot deadlock. Holding one only blocks other
        // pipelines that actually share that capture; an unrelated snapshot never holds the whole member.
        List<KeyedLocks.Hold<CaptureId>> captureHolds = lockCaptures(plans.stream()
                .map(SourcePlan::captureId).distinct().toList(), true);
        try {
            startPlannedCapture(pipelineId, plans, pending);
        } finally {
            releaseCaptures(captureHolds);
        }
    }





    private void startPlannedCapture(String pipelineId, List<SourcePlan> plans, PendingStart pending) {
        Map<CaptureId, OpeningClaim> permits = new LinkedHashMap<>();
        List<PipelineRun> runs = new ArrayList<>();
        List<SnapshotOnChain> snapshotTables = new ArrayList<>();
        // Taken before the first source is opened, because the load is what these totals accumulate from
        // and its first row is read inside the loop below. Stamping it afterwards would date the whole
        // load to the moment it ended, and a rate computed across the first two scrapes would divide by a
        // window that had already closed.
        Instant loadBegan = Instant.now();
        LiveSnapshot liveSnapshot = LiveSnapshot.of(plans, loadBegan);
        if (liveSnapshot != null) {
            liveSnapshotsByPipeline.put(pipelineId, liveSnapshot);
        }
        PipelineLoad load = PipelineLoad.of(plans, loadBegan);
        try {
            settlePermits(pipelineId, plans, permits, pending);
            requestSharedSelections(plans);
            requireRemoteSelections(pipelineId, plans, permits);
            for (SourcePlan plan : plans) {
                pending.check();
                SourceCaptureResolution resolution = plan.resolution();
                CaptureRunSpec spec = plan.spec();
                CaptureId captureId = plan.captureId();
                // Written by the thread reading the load and read by whoever asks how far it has got.
                Map<String, Long> observedSnapshotCounts = new ConcurrentHashMap<>();
                if (CapturePlan.forReadMode(spec.readMode()).snapshot()) {
                    // Before the run starts, and so before the job that takes its rows is even assembled: the
                    // source vertex reads the declaration when it starts, and one that found none would read
                    // its ring at once, ahead of snapshot rows still to come.
                    resolution.tables().forEach(
                            table -> snapshotBuffer.declareSnapshot(pipelineId, resolution.ringName(table)));
                }
                CaptureHandoff handoff = snapshotPassthrough(pipelineId, plan, observedSnapshotCounts, load, liveSnapshot);
                CaptureRun run;
                CaptureRunSpec openedSpec = spec;
                if (!managedOwnership) {
                    run = captureStarter.start(spec, handoff);
                    runs.add(PipelineRun.unmanaged(run, spec));
                } else {
                    OwnedCapture existing = ownedCaptures.get(captureId);
                    if (existing != null) {
                        serveFromTheReaderHere(existing, spec);
                        openedSpec = spec.withCaptureFence(existing.permit.fence());
                        run = captureAttacher.start(openedSpec, handoff, false);
                        existing.pipelines.add(pipelineId);
                    } else {
                        OpeningClaim opening = permits.get(captureId);
                        if (opening.permit.acquired()) {
                            // The lease already renews this claim before the source can block in snapshot.
                            permits.remove(captureId);
                            CaptureRun started = null;
                            try {
                                opening.check();
                                openedSpec = spec.withCaptureFence(opening.permit.fence());
                                started = captureAttacher.start(openedSpec, handoff, true);
                                opening.check();
                                pending.check();
                                run = started;
                                // Pipelines here that joined this capture while another member held it read
                                // the tail this member now runs, so it outlives the pipeline taking the claim.
                                Set<String> pipelines = new LinkedHashSet<>(List.of(pipelineId));
                                JoinedCapture joined = joinedCaptures.remove(captureId);
                                if (joined != null) {
                                    pipelines.addAll(joined.pipelines);
                                }
                                opening.publish(run, pipelines, openedSpec, handoff);
                            } catch (RuntimeException | Error failure) {
                                boolean interrupted = Thread.interrupted();
                                try {
                                    if (started != null) {
                                        PipelineRun rejected = PipelineRun.managed(captureId, started,
                                                openedSpec.srsEnabled() && openedSpec.readMode() != ReadMode.SNAPSHOT_ONLY,
                                                openedSpec);
                                        runs.add(rejected);
                                        try { closeRun(rejected, pipelineId); }
                                        catch (RuntimeException | Error unclosed) {
                                            if (unclosed != failure) { failure.addSuppressed(unclosed); }
                                        }
                                    }
                                    try {
                                        opening.close();
                                    } catch (RuntimeException unreleased) {
                                        failure.addSuppressed(unreleased);
                                    }
                                } finally {
                                    if (interrupted) {
                                        Thread.currentThread().interrupt();
                                    }
                                }
                                throw failure;
                            }
                        } else {
                            // Another member tails this source. The pipeline reads it no differently for
                            // that: its own load where its record says one is owed, then the changes the
                            // other member's tail writes into the shared ring.
                            run = captureAttacher.start(spec, handoff, false);
                            joinedCaptures.computeIfAbsent(captureId, ignored -> new JoinedCapture(spec, handoff))
                                    .pipelines.add(pipelineId);
                            lookForCapturesNobodyTails();
                        }
                    }
                    runs.add(PipelineRun.managed(
                            captureId, run, openedSpec.srsEnabled() && openedSpec.readMode() != ReadMode.SNAPSHOT_ONLY,
                            openedSpec));
                }
                pending.check();
                if (run.loadOverWhenHandedBack()) {
                    // Handed back with its load already over -- read on this thread, or none owed. What the
                    // run said about each table as it went is said again here, so a starter that says
                    // nothing still leaves every table reported and every declared load ended.
                    //
                    // Only such a run. A load read behind the run reports each table itself, as its rows
                    // are all in, and one that has ended by the time this looks may have ended by failing:
                    // what arrived of the table it failed on is a prefix, and ending that declaration here
                    // is what would let the table be recorded as written short.
                    loadOver(pipelineId, plan, run, observedSnapshotCounts, load);
                }
                snapshotOnChain(plan.sourceId(), spec, run).ifPresent(snapshotTables::add);
            }
            pending.check();
            synchronized (startGate) {
                checkOpen();
                runsByPipeline.put(pipelineId, runs);
                loadsByPipeline.put(pipelineId, load);
                snapshotTablesByPipeline.put(pipelineId, List.copyOf(snapshotTables));
            }
        } catch (RuntimeException | Error failure) {
            if (liveSnapshot != null) {
                liveSnapshotsByPipeline.remove(pipelineId, liveSnapshot);
            }
            // A start that fell over releases what it took and nothing else. It is an abandoned attempt,
            // not somebody asking for the pipeline's position to be thrown away, and the sources that did
            // start may have advanced it before the one that failed.
            // Cancellation reaches this path with the worker's interrupt flag set. Let teardown finish
            // even when a connector or store uses interruptible waits, then restore the signal to its caller.
            boolean interrupted = Thread.interrupted();
            try {
                List<PipelineRun> aborted = List.copyOf(runs);
                if (!aborted.isEmpty()) { abortedRunsByPipeline.put(pipelineId, aborted); }
                RuntimeException cleanupFailure = closeRuns(runs, pipelineId, false, true);
                if (cleanupFailure != null && cleanupFailure != failure
                        && java.util.Arrays.stream(failure.getSuppressed()).noneMatch(suppressed -> suppressed == cleanupFailure)) {
                    failure.addSuppressed(cleanupFailure);
                }
                if (runs.stream().allMatch(run -> run.closeState.completed)) {
                    abortedRunsByPipeline.remove(pipelineId, aborted);
                }
                releaseUnopened(permits, failure);
            } finally {
                if (interrupted) {
                    Thread.currentThread().interrupt();
                }
            }
            throw failure;
        }

    }

    /**
     * Ends and reports every table of a run that came back with its load over, the way the run's own hand-off
     * would have as each table went through. Saying it twice changes nothing: an ended load stays ended, and a
     * table reported again is reported with what it had already read.
     */
    private void loadOver(
            String pipelineId,
            SourcePlan plan,
            CaptureRun run,
            Map<String, Long> observedSnapshotCounts,
            PipelineLoad load) {
        CaptureRunSpec spec = plan.spec();
        if (!CapturePlan.forReadMode(spec.readMode()).snapshot()) {
            return;
        }
        List<String> streams = spec.config().streams();
        Map<String, Long> counts = run.snapshotCounts().isEmpty() ? observedSnapshotCounts : run.snapshotCounts();
        for (String table : streams) {
            long count = streams.size() == 1
                    ? counts.getOrDefault(table, run.snapshotCount())
                    : counts.getOrDefault(table, 0L);
            saveLoadCountIfOwed(spec, run.chainId().map(MiningChainId::value), table, count);
            snapshotBuffer.endSnapshot(pipelineId, plan.resolution().ringName(table));
            load.loaded(plan.sourceId(), table, count, estimatedRows(plan.discovered(), table));
        }
    }

    /** Keep each measured load until its target confirms it, including across a replacement run. */
    private void saveLoadCountIfOwed(CaptureRunSpec spec, Optional<String> chainId, String table, long count) {
        chainId.ifPresent(chain -> {
            boolean completed = storePort.meta().read(chain)
                    .map(record -> record.snapshotCompletedTables(spec.consumerId()).contains(table))
                    .orElse(false);
            if (!completed) {
                SnapshotLoadCounts.save(storePort.keyedState(), spec.consumerId(), chain, table, count);
            }
        });
    }

    /** One source of a start as it was settled before anything was opened: what to open and for what. */
    private record SourcePlan(
            String sourceId,
            SourceModel discovered,
            SourceCaptureResolution resolution,
            CaptureRunSpec spec,
            CaptureId captureId) {
    }

    /**
     * Works out every source this start reads before taking any claim. The capture identities then choose
     * the locks under which every claim is settled before any source is opened.
     */
    private List<SourcePlan> plan(
            String pipelineId,
            PipelineResource pipeline,
            ArtifactStore captured,
            long snapshotEpoch) {
        List<SourcePlan> plans = new ArrayList<>();
        for (SourceRef ref : pipeline.sources()) {
            String sourceId = ref.id();
            SourceResource source = StoredArtifacts.requireSource(captured, sourceId);
            // Read once and used twice: it decides which streams this run reads, and it carries the
            // row count the last discovery took of each of them. Asking the store again for the second
            // use would pay for a second read per source on every start.
            SourceModel discovered = SourceDiscovery.model(storePort, source);
            Optional<SourceCaptureResolution> selected =
                    SourceCaptureResolution.forPipeline(pipeline, source, discovered);
            if (selected.isEmpty()) {
                continue;
            }
            SourceCaptureResolution resolution = selected.orElseThrow();
            CaptureRunSpec spec = deriveSpec(
                    pipelineId, pipeline.settings(), source, resolution, srsSwitchOf(pipelineId, ref),
                    snapshotEpoch);
            CaptureId captureId = CaptureId.of(spec);
            plans.add(new SourcePlan(sourceId, discovered, resolution, spec, captureId));
        }
        return plans;
    }

    private void settlePermits(
            String pipelineId, List<SourcePlan> plans,
            Map<CaptureId, OpeningClaim> permits, PendingStart pending) {
        for (SourcePlan plan : plans) {
            pending.check();
            CaptureId captureId = plan.captureId();
            if (managedOwnership && !ownedCaptures.containsKey(captureId) && !permits.containsKey(captureId)) {
                CaptureOwnership.Permit permit = permitOrNotYet(pipelineId, captureId, plan.spec());
                try {
                    permits.put(captureId, new OpeningClaim(captureId, permit, pending, null));
                } catch (RuntimeException | Error failure) {
                    if (permit.acquired()) {
                        try {
                            ownership.release(permit.claim());
                        } catch (RuntimeException unreleased) {
                            failure.addSuppressed(unreleased);
                        }
                    }
                    throw failure;
                }
            }
        }
    }

    /** Binds every source to the same prepared, bounded snapshot-buffer session. */
    private static List<SourcePlan> withSnapshotTokens(List<SourcePlan> plans, String snapshotWriterToken) {
        List<SourcePlan> bound = new ArrayList<>(plans.size());
        for (SourcePlan plan : plans) {
            bound.add(new SourcePlan(plan.sourceId(), plan.discovered(), plan.resolution(),
                    plan.spec().withSnapshotWriterToken(snapshotWriterToken), plan.captureId()));
        }
        return List.copyOf(bound);
    }

    private void requestSharedSelections(List<SourcePlan> plans) {
        Map<MiningChainId, Set<String>> tablesByChain = new LinkedHashMap<>();
        Map<MiningChainId, String> retentionByChain = new LinkedHashMap<>();
        for (SourcePlan plan : plans) {
            CaptureRunSpec spec = plan.spec();
            if (!spec.srsEnabled() || spec.readMode() == ReadMode.SNAPSHOT_ONLY) {
                continue;
            }
            MiningChainId chainId = spec.miningChainId();
            tablesByChain.computeIfAbsent(chainId, ignored -> new LinkedHashSet<>()).addAll(spec.config().streams());
            retentionByChain.putIfAbsent(chainId, spec.retention());
        }
        tablesByChain.forEach((chain, tables) -> {
            if (storePort.meta().read(chain.value()).isEmpty()) {
                // A chain with no record has no position, so connector notes still filed under its name are left
                // over from an earlier one whose clearing stopped part way -- after the record and the release,
                // before the notes (see purgeChain). Read as this chain's own they would name a slot that is
                // gone, and a connector that knows a slot's name creates it again only when its stream starts:
                // whatever is written to the source while the first load runs would never be read. They go
                // before the record is created, while nothing can have opened a connector over them yet; another
                // member seeding the same chain at the same moment is the one ordering this cannot give, as for
                // the record itself.
                dropChainNotes(chain.value());
                try {
                    storePort.meta().create(chain.value(), retentionByChain.get(chain));
                } catch (IllegalStateException alreadySeeded) {
                    // Another member can seed the same physical chain between the read and insert.
                    // Keep its accumulated state; an absent record is the original ordering failure.
                    if (storePort.meta().read(chain.value()).isEmpty()) {
                        throw alreadySeeded;
                    }
                }
            }
            storePort.meta().requestCaptureTables(chain.value(), List.copyOf(tables));
        });
    }

    private void requireRemoteSelections(
            String pipelineId, List<SourcePlan> plans, Map<CaptureId, OpeningClaim> permits) {
        if (!managedOwnership) {
            return;
        }
        Map<CaptureId, Set<String>> selectedByCapture = new LinkedHashMap<>();
        Map<CaptureId, MiningChainId> chains = new LinkedHashMap<>();
        for (SourcePlan plan : plans) {
            CaptureRunSpec spec = plan.spec();
            OpeningClaim opening = permits.get(plan.captureId());
            CaptureOwnership.Permit permit = opening == null ? null : opening.permit;
            if (!spec.srsEnabled() || spec.readMode() == ReadMode.SNAPSHOT_ONLY
                    || ownedCaptures.containsKey(plan.captureId()) || permit == null || permit.acquired()) {
                continue;
            }
            selectedByCapture.computeIfAbsent(plan.captureId(), ignored -> new LinkedHashSet<>())
                    .addAll(spec.config().streams());
            chains.put(plan.captureId(), spec.miningChainId());
        }
        selectedByCapture.forEach((capture, tables) -> {
            if (storePort.meta().captureServingTables(chains.get(capture).value()).containsAll(tables)) {
                servingWaits.remove(capture);
                return;
            }
            Duration bound = ownership.ttl();
            long now = System.nanoTime();
            RingWait prior = servingWaits.get(capture);
            RingWait wait = prior == null || now - prior.lastLooked() > bound.toNanos()
                    ? new RingWait(now, now) : prior;
            if (now - wait.since() >= bound.toNanos()) {
                servingWaits.remove(capture);
                throw new TapstateException(CaptureError.NO_RING_TO_ATTACH, Map.of(
                        "captureId", capture.value(), "seconds", bound.toSeconds()), null);
            }
            servingWaits.put(capture, new RingWait(wait.since(), now));
            LOG.info("Capture {} does not serve tables {} yet; pipeline {} starts once it does",
                    capture.value(), tables, pipelineId);
            throw new RingNotOpenYet(capture);
        });
    }

    /**
     * Lets go of the claims a start took for captures it never opened. One that cannot be let go of runs
     * out by its lease; saying so on the failure the start ends with is all that is left to do.
     */
    private void releaseUnopened(Map<CaptureId, OpeningClaim> permits, Throwable failure) {
        for (OpeningClaim opening : permits.values()) {
            try {
                opening.close();
            } catch (RuntimeException unreleased) {
                failure.addSuppressed(unreleased);
            }
        }
        permits.clear();
    }

    /**
     * One source run of a pipeline, with the spec it was started from: a clearing lets go of what the run's
     * source connector set up on the source through that spec's notes, as the run itself opened them.
     */
    private record PipelineRun(
            CaptureId captureId, CaptureRun run, boolean managed, boolean sharedTail, CaptureRunSpec spec, CloseState closeState) {

        static PipelineRun unmanaged(CaptureRun run, CaptureRunSpec spec) {
            return new PipelineRun(null, run, false, false, spec, new CloseState());
        }

        static PipelineRun managed(CaptureId captureId, CaptureRun run, boolean sharedTail, CaptureRunSpec spec) {
            return new PipelineRun(captureId, run, true, sharedTail, spec, new CloseState());
        }

        String consumerId() {
            return spec.consumerId();
        }
    }

    /** Holds a newly acquired claim from permit settlement through a possibly blocking source start. */
    private final class OpeningClaim implements AutoCloseable {
        private final CaptureId captureId;
        private final CaptureOwnership.Permit permit;
        private final CaptureClaimLease lease;
        private final Thread openingThread;
        private volatile PendingStart openingPending;
        private volatile OwnedCapture published;
        private volatile boolean lost;

        private OpeningClaim(CaptureId captureId, CaptureOwnership.Permit permit,
                PendingStart openingPending, Thread openingThread) {
            this.captureId = captureId;
            this.permit = permit;
            this.openingPending = openingPending;
            this.openingThread = openingThread;
            this.lease = !permit.acquired() || permit.claim() == null
                    ? CaptureClaimLease.unfenced()
                    : new CaptureClaimLease(ownership, permit.claim(), claimRenewInterval, this::lose);
        }

        private void lose() {
            lost = true;
            PendingStart pending = openingPending;
            if (pending != null) {
                pending.cancel();
            }
            cancelPendingStarts(captureId);
            OwnedCapture current = published;
            if (current != null && current.reconfiguring != null) {
                current.reconfiguring.interrupt();
            }
            if (current == null && openingThread != null) {
                openingThread.interrupt();
            }
            if (current != null) {
                captureClaimLost(captureId, current);
            }
        }

        private void check() {
            checkOpen();
            if (lost || Thread.currentThread().isInterrupted()) {
                throw new CancellationException("capture claim was lost during source start");
            }
        }

        private void publish(CaptureRun run, Set<String> pipelines,
                CaptureRunSpec spec, CaptureHandoff receive) {
            synchronized (startGate) {
                checkOpen();
                OwnedCapture owned = new OwnedCapture(run, permit, pipelines, spec, receive);
                owned.lease = lease;
                published = owned;
                ownedCaptures.put(captureId, owned);
                lookForCapturesNobodyTails();
                openingPending = null;
            }
        }

        @Override
        public void close() {
            lease.close();
        }
    }

    private static final class OwnedCapture {
        private volatile CaptureRun run;
        private final CaptureOwnership.Permit permit;
        private final Set<String> pipelines = new LinkedHashSet<>();
        private final CaptureRunSpec spec;
        private final CloseState nativeClose = new CloseState();
        private final CloseState leaseClose = new CloseState();
        private final CaptureHandoff receive;
        private CaptureClaimLease lease;
        private volatile Thread reconfiguring;

        private OwnedCapture(CaptureRun run, CaptureOwnership.Permit permit, Collection<String> pipelines,
                CaptureRunSpec spec, CaptureHandoff receive) {
            this.run = run;
            this.permit = permit;
            this.pipelines.addAll(pipelines);
            this.spec = spec;
            this.receive = receive;
        }
    }

    /**
     * A capture pipelines here read while another member tails it: those pipelines, and what this member
     * would open the tail with should nobody else be tailing it. That tail reads no load -- each of the
     * pipelines ran its own when it attached -- so it is opened as a change-only read of the same source,
     * which is the same capture.
     */
    private static final class JoinedCapture {
        private final boolean tails;
        private final CaptureRunSpec tailSpec;
        private final CaptureHandoff tailPassthrough;
        private final Set<String> pipelines = new LinkedHashSet<>();

        private JoinedCapture(CaptureRunSpec joinedWith, CaptureHandoff passthrough) {
            // A snapshot-only read has no tail for anybody to take over.
            this.tails = joinedWith.readMode() != ReadMode.SNAPSHOT_ONLY;
            this.tailSpec = new CaptureRunSpec(
                    joinedWith.config(), ReadMode.CDC_ONLY, joinedWith.srsKey(), joinedWith.srsEnabled(),
                    joinedWith.sourceId(), joinedWith.pipelineId(), joinedWith.startFrom(),
                    joinedWith.retention(), joinedWith.schemaVer(), joinedWith.snapshotEpoch())
                    .withConsumerId(joinedWith.consumerId());
            this.tailPassthrough = passthrough;
        }
    }





    /**
     * Opens, here, the tail of every capture pipelines here read that nobody tails any more.
     *
     * <p>A member lets a capture's claim go with the last of its own pipelines on it, and a pipeline on
     * another member reading the same capture goes on running over a ring nobody writes: healthy, and
     * receiving nothing because a different pipeline somewhere else was stopped. So each member that reads
     * a capture it does not hold keeps asking for its claim, and the first to get it opens the tail, from
     * where the durable record says the last one had got to. A holder that died leaves the claim to be
     * taken the same way once its lease has run out.
     */
    void tailWhatNobodyTails() {
        if (closing) { return; }
        for (CaptureId captureId : List.copyOf(joinedCaptures.keySet())) {
            // A slow start on one capture cannot hold the takeover pass or another capture behind it.
            KeyedLocks.Hold<CaptureId> hold = captureLocks.tryAcquire(captureId);
            if (hold == null) {
                continue;
            }
            try (hold) {
                JoinedCapture capture = joinedCaptures.get(captureId);
                if (capture == null || !capture.tails) {
                    continue;
                }
                if (stillLoading(capture.pipelines)) { continue; }
                CaptureOwnership.Permit permit = ownership.acquire(captureId);
                if (!permit.acquired()) {
                    continue;
                }
                OpeningClaim opening;
                try {
                    opening = new OpeningClaim(captureId, permit, null, Thread.currentThread());
                } catch (RuntimeException | Error failure) {
                    ownership.release(permit.claim());
                    throw failure;
                }
                CaptureRun tail = null;
                try {
                    opening.check();
                    tail = captureAttacher.start(
                            capture.tailSpec.withCaptureFence(permit.fence()), capture.tailPassthrough, true);
                    opening.check();
                    joinedCaptures.remove(captureId, capture);
                    opening.publish(tail, capture.pipelines,
                            capture.tailSpec.withCaptureFence(permit.fence()), capture.tailPassthrough);
                    if (opening.lost) {
                        captureClaimLost(captureId, opening.published);
                        throw new CancellationException("capture claim was lost during takeover");
                    }
                } catch (RuntimeException | Error failure) {
                    boolean interrupted = Thread.interrupted();
                    if (tail != null && opening.published == null) {
                        try {
                            tail.close();
                        } catch (RuntimeException unclosed) {
                            failure.addSuppressed(unclosed);
                        }
                    }
                    try {
                        opening.close();
                    } catch (RuntimeException unreleased) {
                        failure.addSuppressed(unreleased);
                    }
                    LOG.warn("Could not open the tail of capture {} for pipelines {} here; asking again later",
                            captureId.value(), capture.pipelines, failure);
                    if (interrupted && (takeovers == null || takeovers.isShutdown())) {
                        Thread.currentThread().interrupt();
                    }
                    continue;
                }
                LOG.info("Took over the tail of capture {} for pipelines {} here", captureId.value(),
                        capture.pipelines);
            }
        }
    }

    /** Whether any run of {@code pipelines} here is still reading its load. */
    private boolean stillLoading(Collection<String> pipelines) {
        return pipelines.stream()
                .map(runsByPipeline::get)
                .filter(Objects::nonNull)
                .flatMap(List::stream)
                .anyMatch(pipelineRun -> pipelineRun.run.loading());
    }

    private synchronized void lookForCapturesNobodyTails() {
        if (closing || takeovers != null) {
            return;
        }
        takeovers = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "tapstate-capture-takeover");
            thread.setDaemon(true);
            return thread;
        });
        // Shared readers retire confirmed history even when a single member has no claim to renew.
        long every = claimRenewInterval.isZero() ? TimeUnit.SECONDS.toMillis(1) : claimRenewInterval.toMillis();
        takeovers.scheduleWithFixedDelay(() -> {
            try {
                tailWhatNobodyTails();
                widenTheReadersHere();
            } catch (RuntimeException failure) {
                LOG.warn("Looking for captures nobody tails failed; asking again later", failure);
            }
        }, every, every, TimeUnit.MILLISECONDS);
    }

    /** Table requests are recorded before this pipeline attaches to the physical reader it joins. */
    private void serveFromTheReaderHere(OwnedCapture owned, CaptureRunSpec spec) {
        if (!spec.srsEnabled() || spec.readMode() == ReadMode.SNAPSHOT_ONLY) {
            return;
        }
        try {
            owned.reconfiguring = Thread.currentThread();
            captureAttacher.widen(owned.run);
        } catch (RuntimeException | Error failure) {
            owned.run.health().fail(failure);
            throw failure;
        } finally {
            owned.reconfiguring = null;
        }
    }

    /** Serves requested tables and retires confirmed history on every owner's maintenance schedule. */
    void widenTheReadersHere() {
        if (closing) { return; }
        for (CaptureId captureId : List.copyOf(ownedCaptures.keySet())) {
            KeyedLocks.Hold<CaptureId> hold = captureLocks.tryAcquire(captureId);
            if (hold == null) { continue; }
            try (hold) {
                OwnedCapture owned = ownedCaptures.get(captureId);
                if (owned == null) { continue; }
                try {
                    owned.reconfiguring = Thread.currentThread();
                    captureAttacher.widen(owned.run);
                } catch (RuntimeException failure) {
                    owned.run.health().fail(failure);
                    LOG.warn("Could not have a shared reader serve its requested tables", failure);
                } finally {
                    owned.reconfiguring = null;
                }
            }
        }
    }

    private record ShutdownWork(String resource, Runnable close) { }
    private record ClosingWork(ShutdownWork work, CompletableFuture<Void> completion) { }

    /** Whether every captured local resource returned from teardown without failure. */
    boolean shutdownComplete() { return shutdown.isDone() && !shutdown.isCompletedExceptionally(); }

    /** Captures exact local handles, cancels opens and closes them on the existing bounded worker pool. */
    public void close() {
        List<ShutdownWork> work = null;
        synchronized (startGate) {
            if (!closing) {
                closing = true;
                shutdownDeadline = System.nanoTime() + shutdownBudget.toNanos();
                work = new ArrayList<>();
                Set<String> pipelines = new java.util.TreeSet<>(runsByPipeline.keySet());
                pipelines.addAll(pendingStarts.keySet());
                pipelines.addAll(abortedRunsByPipeline.keySet());
                for (String pipeline : pipelines) {
                    List<PipelineRun> expected = runsByPipeline.get(pipeline);
                    PendingStart pending = pendingStarts.get(pipeline);
                    work.add(new ShutdownWork(pipeline, () -> closeExpectedPipeline(pipeline, expected, pending)));
                }
                for (var entry : ownedCaptures.entrySet()) {
                    CaptureId capture = entry.getKey(); OwnedCapture expected = entry.getValue();
                    boolean covered = java.util.stream.Stream.concat(runsByPipeline.values().stream(), abortedRunsByPipeline.values().stream())
                            .flatMap(List::stream)
                            .anyMatch(run -> run.managed && capture.equals(run.captureId));
                    if (!covered) { work.add(new ShutdownWork(capture.value(), () -> closeExpectedOrphan(capture, expected))); }
                }
            }
        }
        if (work != null) {
            try {
                closeOwnedWork(work);
                shutdown.complete(null);
            } catch (Throwable failure) {
                shutdown.completeExceptionally(failure);
                LOG.warn("Could not confirm local capture shutdown", failure);
            }
        }
        try {
            long remaining = Math.max(0L, shutdownDeadline - System.nanoTime());
            shutdown.get(remaining, TimeUnit.NANOSECONDS);
        } catch (ExecutionException failure) { throwShutdownFailure(failure.getCause()); }
        catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw incompleteShutdown(1, interrupted);
        } catch (TimeoutException unavailable) { throw incompleteShutdown(1, unavailable); }
    }

    private void closeExpectedPipeline(String pipeline, List<PipelineRun> expected, PendingStart pending) {
        try (KeyedLocks.Hold<String> ignored = pipelineLocks.acquireInterruptibly(pipeline)) {
            if (expected != null && runsByPipeline.get(pipeline) == expected) {
                List<KeyedLocks.Hold<CaptureId>> held = lockCaptures(expected.stream().filter(PipelineRun::managed)
                        .map(PipelineRun::captureId).toList(), true);
                try {
                    RuntimeException failed = closeRuns(expected, pipeline, false);
                    if (failed != null) { throw failed; }
                    runsByPipeline.remove(pipeline, expected);
                    loadsByPipeline.remove(pipeline); liveSnapshotsByPipeline.remove(pipeline);
                    snapshotTablesByPipeline.remove(pipeline);
                } finally { releaseCaptures(held); }
            }
            List<PipelineRun> aborted = abortedRunsByPipeline.get(pipeline);
            closeAbortedCapture(pipeline, aborted);
            if (pending != null && pending.failure != null && aborted == null) {
                Throwable failure = pending.failure;
                if (failure instanceof CancellationException) {
                    if (failure.getSuppressed().length == 0) { return; }
                    Throwable[] refused = failure.getSuppressed();
                    failure = refused[0];
                    for (int index = 1; index < refused.length; index++) {
                        failure = combineShutdownFailure(failure, refused[index]);
                    }
                }
                throwShutdownFailure(failure);
            }
        }
    }

    private void closeAbortedCapture(String pipeline, List<PipelineRun> expected) {
        if (expected == null || abortedRunsByPipeline.get(pipeline) != expected) { return; }
        List<KeyedLocks.Hold<CaptureId>> held = lockCaptures(expected.stream().filter(PipelineRun::managed)
                .map(PipelineRun::captureId).toList(), true);
        try {
            Throwable failure = null;
            for (PipelineRun run : expected) {
                try { closeRun(run, pipeline); }
                catch (RuntimeException | Error refused) { failure = combineShutdownFailure(failure, refused); }
            }
            if (failure != null) { throwShutdownFailure(failure); }
            snapshotBuffer.release(pipeline);
            abortedRunsByPipeline.remove(pipeline, expected);
        } finally { releaseCaptures(held); }
    }

    private void closeExpectedOrphan(CaptureId capture, OwnedCapture expected) {
        try (KeyedLocks.Hold<CaptureId> ignored = captureLocks.acquireInterruptibly(capture)) {
            if (ownedCaptures.get(capture) != expected
                    || expected.pipelines.stream().anyMatch(runsByPipeline::containsKey)) { return; }
            closeOwnedCapture(expected);
            ownedCaptures.remove(capture, expected);
        }
    }

    private void closeOwnedWork(List<ShutdownWork> work) {
        pendingStarts.values().forEach(PendingStart::cancel);
        ScheduledExecutorService maintenance;
        synchronized (this) {
            maintenance = takeovers; takeovers = null;
            if (maintenance != null) { maintenance.shutdownNow(); }
        }
        shutdownDispatcher.beginOwnedShutdown();
        var waiting = new ArrayDeque<ClosingWork>();
        var remainingWork = work.iterator();
        Throwable failure = null;
        int completed = 0;
        ShutdownWork next = null;
        try {
            while ((next != null || remainingWork.hasNext() || !waiting.isEmpty()) && System.nanoTime() < shutdownDeadline) {
                for (var iterator = waiting.iterator(); iterator.hasNext();) {
                    ClosingWork current = iterator.next();
                    if (!current.completion().isDone()) { continue; }
                    try { current.completion().get(); }
                    catch (ExecutionException refused) { failure = combineShutdownFailure(failure, refused.getCause()); }
                    catch (CancellationException refused) { failure = combineShutdownFailure(failure, refused); }
                    completed++; iterator.remove();
                }
                if ((next != null || remainingWork.hasNext()) && waiting.size() < shutdownDispatcher.ownedCleanupLimit()) {
                    if (next == null) { next = remainingWork.next(); }
                    var submitted = shutdownDispatcher.offerOwnedCleanup(next.close(), shutdownDeadline);
                    if (submitted.isPresent()) {
                        waiting.addLast(new ClosingWork(next, submitted.orElseThrow()));
                        next = null;
                        continue;
                    }
                    if (!shutdownDispatcher.ownedCleanupAccepting()) { break; }
                }
                if (!waiting.isEmpty()) {
                    long remaining = shutdownDeadline - System.nanoTime();
                    if (remaining <= 0) { break; }
                    try { waiting.getFirst().completion().get(Math.min(remaining, TimeUnit.MILLISECONDS.toNanos(100)), TimeUnit.NANOSECONDS); }
                    catch (ExecutionException | CancellationException ready) { /* Collected on the next pass. */ }
                    catch (TimeoutException stillClosing) { /* Waited on an actual bounded completion condition. */ }
                }
            }
            if (!shutdownDispatcher.awaitOwnedReconciliation(shutdownDeadline)) {
                failure = combineShutdownFailure(failure, incompleteShutdown(1, null));
            }
            if (maintenance != null && !maintenance.awaitTermination(Math.max(0L, shutdownDeadline - System.nanoTime()), TimeUnit.NANOSECONDS)) {
                failure = combineShutdownFailure(failure, incompleteShutdown(1, null));
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            failure = combineShutdownFailure(failure, incompleteShutdown(work.size() - completed, interrupted));
        }
        for (ClosingWork current : waiting) {
            if (!current.completion().isDone()) { continue; }
            try { current.completion().join(); }
            catch (java.util.concurrent.CompletionException refused) { failure = combineShutdownFailure(failure, refused.getCause()); }
            catch (CancellationException refused) { failure = combineShutdownFailure(failure, refused); }
            completed++;
        }
        if (completed < work.size()) {
            failure = combineShutdownFailure(failure, incompleteShutdown(work.size() - completed, null));
        }
        if (failure != null) { throwShutdownFailure(failure); }
    }

    private TapstateException incompleteShutdown(int resources, Throwable cause) {
        return new TapstateException(ActuationError.CAPTURE_SHUTDOWN_INCOMPLETE,
                Map.of("resources", resources, "timeout", shutdownBudget.toString()), cause);
    }
    private static Throwable combineShutdownFailure(Throwable first, Throwable next) {
        if (first == null) { return next; }
        if (first != next) { first.addSuppressed(next); }
        return first;
    }
    private static void throwShutdownFailure(Throwable failure) {
        if (failure instanceof RuntimeException runtime) { throw runtime; }
        if (failure instanceof Error defect) { throw defect; }
        throw new IllegalStateException(failure);
    }

    /**
     * This member's claim on {@code captureId}, or a refusal of it once there is a ring for a pipeline
     * attaching here to read -- or, while neither is so, the start given back with {@link RingNotOpenYet}.
     *
     * <p>A member takes a capture's claim before it opens the chain's ring, so a pipeline started elsewhere
     * in between finds the claim held and nothing yet to read under. Attaching then would put its load on
     * no generation at all; so the start is given back and the next pass looks again, taking the claim
     * itself should it have come free meanwhile -- a holder that died before opening anything leaves it free
     * once its lease runs out. A read with no tail has no ring to wait for.
     *
     * <p>Given back rather than waited for here because a held claim with no ring may outlive this pass.
     * How long a capture has been found this way is kept; once it exceeds a lease, the start is refused
     * with a code rather than retried over nothing forever. A wait not revisited for a lease starts afresh.
     *
     * <p>An open generation is all it waits for. It may be one a previous run of the chain opened rather
     * than the one the holder is about to open, and that is harmless: a ring numbers its changes on from
     * where it had reached, so reading under the older generation orders nothing backwards, and this
     * pipeline's load only sits lower -- beneath changes it would have sat beneath anyway.
     */
    private CaptureOwnership.Permit permitOrNotYet(String pipelineId, CaptureId captureId, CaptureRunSpec spec) {
        CaptureOwnership.Permit permit = ownership.acquire(captureId);
        if (permit.acquired() || spec.readMode() == ReadMode.SNAPSHOT_ONLY
                || aRingIsOpen(spec.miningChainId().value())) {
            ringWaits.remove(captureId);
            return permit;
        }
        Duration bound = ownership.ttl();
        long now = System.nanoTime();
        RingWait wait = ringWaits.get(captureId);
        if (wait == null || now - wait.lastLooked() > bound.toNanos()) {
            LOG.info("Capture {} is held by another member that has not opened its ring yet; pipeline {} "
                    + "starts once it has, or once the capture comes free", captureId.value(), pipelineId);
            wait = new RingWait(now, now);
        }
        if (now - wait.since() >= bound.toNanos()) {
            ringWaits.remove(captureId);
            throw new TapstateException(CaptureError.NO_RING_TO_ATTACH, Map.of(
                    "captureId", captureId.value(), "seconds", bound.toSeconds()), null);
        }
        ringWaits.put(captureId, new RingWait(wait.since(), now));
        throw new RingNotOpenYet(captureId);
    }

    /** When a capture was first found held elsewhere with no ring open, and when it was last looked at. */
    private record RingWait(long since, long lastLooked) {
    }

    private boolean aRingIsOpen(String chainId) {
        return storePort.meta().read(chainId).map(SrsMeta::epoch).orElse(0L) >= 1;
    }

    private void captureClaimLost(CaptureId captureId, OwnedCapture expected) {
        // A start may still be inside a connector snapshot while holding this capture's lock. Interrupt
        // it before waiting for that lock, or claim loss cannot end the blocked start.
        cancelPendingStarts(captureId);
        if (expected == null) {
            return;
        }
        try (KeyedLocks.Hold<CaptureId> ignored = captureLocks.acquire(captureId)) {
            if (ownedCaptures.get(captureId) != expected) {
                return;
            }
            TapstateException fenced = new TapstateException(
                    CaptureError.CLAIM_LOST, Map.of("captureId", captureId.value()), null);
            expected.run.health().fail(fenced);
            runsByPipeline.values().stream()
                    .flatMap(List::stream)
                    .filter(run -> run.managed && captureId.equals(run.captureId))
                    .forEach(run -> run.run.health().fail(fenced));
            // Readers no longer take the coordinator's global monitor. Publish failure to every local
            // attachment before removing the shared owner, so a read during a slow tail close cannot
            // mistake a lost claim for a healthy joined capture.
            closeOwnedCapture(expected);
            ownedCaptures.remove(captureId, expected);
        }
    }

    private void cancelPendingStarts(CaptureId captureId) {
        pendingStarts.values().stream().filter(pending -> pending.covers(captureId))
                .forEach(PendingStart::cancel);
    }

    /**
     * The highest chain generation whose durable consumer record says this pipeline reached it.
     *
     * <p>A stop that preserves state leaves that record beside the operator state it ordered. Looking
     * across every such chain also covers a pipeline whose source binding changed while stopped; using
     * only the source it names now would lose the generation of the state the earlier binding left behind.
     */
    private long retainedChainGeneration(String pipelineId) {
        return storePort.meta().miningChainIdsWithConsumer(pipelineId).stream()
                .map(storePort.meta()::read)
                .flatMap(Optional::stream)
                .mapToLong(SrsMeta::epoch)
                .max()
                .orElse(0L);
    }

    /** One source run's snapshot: its completion chain, if any, and the tables it covers. */
    private record SnapshotOnChain(
            String sourceId, String consumerId, Optional<String> chainId, List<String> tables) {
    }

    /**
     * What this run contributes to the delivered question, or empty when it contributes nothing.
     *
     * <p>A run whose read mode has no snapshot has no load to deliver. A run with no chain is a
     * snapshot-only read: it opens no tail, so nothing seeds a record and no completion is ever written
     * for it. Keep its tables with an absent chain so they remain owed: without delivery evidence, a
     * resume must re-read the load rather than restart a vertex over an empty snapshot hand-off.
     */
    private static Optional<SnapshotOnChain> snapshotOnChain(String sourceId, CaptureRunSpec spec, CaptureRun run) {
        if (!CapturePlan.forReadMode(spec.readMode()).snapshot()) {
            return Optional.empty();
        }
        return Optional.of(new SnapshotOnChain(
                sourceId, spec.consumerId(), run.chainId().map(MiningChainId::value), spec.config().streams()));
    }

    private record SnapshotTableKey(String sourceId, String table) {
    }

    /** One source's attributed snapshot reading, qualified when selected table names collide. */
    private record AttributedSnapshot(String sourceId, String table, TableSnapshot snapshot) {
    }

    private static SnapshotReading reading(List<AttributedSnapshot> attributed, Instant loadBegan) {
        if (attributed.isEmpty()) { return SnapshotReading.NONE; }
        return new SnapshotReading(keyByTableOrQualifyOnCollision(attributed), loadBegan);
    }

    private static Map<String, TableSnapshot> keyByTableOrQualifyOnCollision(List<AttributedSnapshot> attributed) {
        Map<String, Long> occurrences = attributed.stream()
                .collect(Collectors.groupingBy(AttributedSnapshot::table, Collectors.counting()));
        Map<String, TableSnapshot> loaded = new LinkedHashMap<>();
        for (AttributedSnapshot entry : attributed) {
            String key = occurrences.get(entry.table()) > 1 ? entry.sourceId() + "." + entry.table() : entry.table();
            loaded.put(key, entry.snapshot());
        }
        return loaded;
    }

    /** A live snapshot reading from connector callbacks, available before the bounded load finishes. */
    private static final class LiveSnapshot {
        private final Instant began;
        private final List<SnapshotTableKey> tables;
        private final Map<SnapshotTableKey, Long> totals;
        private final Map<SnapshotTableKey, AtomicLong> rows = new ConcurrentHashMap<>();

        private LiveSnapshot(Instant began, List<SnapshotTableKey> tables, Map<SnapshotTableKey, Long> totals) {
            this.began = began;
            this.tables = List.copyOf(tables);
            this.totals = Map.copyOf(totals);
        }

        private static LiveSnapshot of(List<SourcePlan> plans, Instant began) {
            List<SnapshotTableKey> tables = new ArrayList<>();
            Map<SnapshotTableKey, Long> totals = new LinkedHashMap<>();
            for (SourcePlan plan : plans) {
                if (!CapturePlan.forReadMode(plan.spec().readMode()).snapshot()) {
                    continue;
                }
                for (String table : plan.resolution().tables()) {
                    SnapshotTableKey key = new SnapshotTableKey(plan.sourceId(), table);
                    tables.add(key);
                    Long estimated = estimatedRows(plan.discovered(), table);
                    if (estimated != null) {
                        totals.put(key, estimated);
                    }
                }
            }
            return tables.isEmpty() ? null : new LiveSnapshot(began, tables, totals);
        }

        private void received(String sourceId, String table) {
            rows.computeIfAbsent(new SnapshotTableKey(sourceId, table), ignored -> new AtomicLong())
                    .incrementAndGet();
        }

        private SnapshotReading reading() {
            List<AttributedSnapshot> attributed = new ArrayList<>();
            for (SnapshotTableKey key : tables) {
                AtomicLong recorded = rows.get(key);
                long count = recorded == null ? 0L : recorded.get();
                Long total = totals.get(key);
                attributed.add(new AttributedSnapshot(key.sourceId(), key.table(),
                        new TableSnapshot(count, total, share(count, total))));
            }
            return StoreBackedPipelineCaptureCoordinator.reading(attributed, began);
        }
    }

    /**
     * One pipeline's load as the read face is told about it: every table it covers, each published once its
     * rows are all in, and the moment the load began.
     *
     * <p>A table is absent until then rather than shown at the rows read so far, which is what the read face
     * has always been handed: a count that exists only once a table is through. The load is read while the
     * pipeline runs, so a table now arrives the moment its own read is over instead of all of them arriving
     * together before the pipeline had a job; nothing about what a published entry means has moved.
     *
     * <p>The name each table is published under is settled when the start is planned. It is the bare table
     * name, unless more than one source in this pipeline reads a table of that same name (a normal shape: the
     * same table name in two different databases), in which case every entry for that name is qualified
     * {@code source_id.table}, the same addressing form `serve.from` and friends already use to disambiguate a
     * table reference. That depends on every source of the pipeline and not on which of their loads is through
     * first, so it cannot wait for the tables to arrive. A plain table-name key would otherwise have the last
     * source silently overwrite an earlier one's count and attribute it to the wrong source; qualifying only
     * the names that actually collide keeps the common single-source case unchanged.
     */
    private static final class PipelineLoad {

        private final Instant began;
        private final Map<CoveredTable, String> keys;
        private final Map<CoveredTable, Long> estimates;
        private final Map<String, TableSnapshot> through = new ConcurrentHashMap<>();

        private PipelineLoad(Instant began, Map<CoveredTable, String> keys, Map<CoveredTable, Long> estimates) {
            this.began = began;
            this.keys = keys;
            this.estimates = estimates;
        }

        /** The load of every source among {@code plans} that reads one; a cdc-only source covers nothing. */
        static PipelineLoad of(List<SourcePlan> plans, Instant began) {
            List<CoveredTable> covered = new ArrayList<>();
            Map<CoveredTable, Long> estimates = new LinkedHashMap<>();
            for (SourcePlan plan : plans) {
                if (CapturePlan.forReadMode(plan.spec().readMode()).snapshot()) {
                    for (String table : plan.spec().config().streams()) {
                        CoveredTable entry = new CoveredTable(plan.sourceId(), table);
                        covered.add(entry);
                        Long total = StoreBackedPipelineCaptureCoordinator.estimatedRows(plan.discovered(), table);
                        if (total != null) {
                            estimates.put(entry, total);
                        }
                    }
                }
            }
            Map<String, Long> occurrences = covered.stream()
                    .collect(Collectors.groupingBy(CoveredTable::table, Collectors.counting()));
            Map<CoveredTable, String> keys = new LinkedHashMap<>();
            for (CoveredTable entry : covered) {
                keys.put(entry, occurrences.get(entry.table()) > 1
                        ? entry.sourceId() + "." + entry.table() : entry.table());
            }
            return new PipelineLoad(began, Map.copyOf(keys), Map.copyOf(estimates));
        }

        /** {@code table} of {@code sourceId} is through, having read {@code rows} out of about {@code total}. */
        void loaded(String sourceId, String table, long rows, Long total) {
            String key = keys.get(new CoveredTable(sourceId, table));
            if (key != null) {
                through.put(key, new TableSnapshot(rows, total, share(rows, total)));
            }
        }

        /**
         * The tables through so far, counted from when the load began; nothing at all while none is. A
         * pipeline that ran no bounded load likewise reports nothing rather than a start with no rows: the
         * two are not the same claim, and a table through at zero rows already says "read, and empty".
         */
        SnapshotReading reading() {
            return through.isEmpty() ? SnapshotReading.NONE : new SnapshotReading(Map.copyOf(through), began);
        }

        Long estimatedRows(String sourceId, String table) {
            return estimates.get(new CoveredTable(sourceId, table));
        }

        String keyOf(String sourceId, String table) {
            return keys.get(new CoveredTable(sourceId, table));
        }
    }

    /** One table of one source, as a pipeline's load covers it. */
    private record CoveredTable(String sourceId, String table) {
    }

    /**
     * About how many rows {@code table} holds, as the last discovery of this source counted it, or null
     * where nothing counted it -- a connector that cannot count, a count that was not reached, a model
     * discovered before counting existed, or no discovered model at all.
     *
     * <p>It is an estimate and is treated as one everywhere it goes: it was taken at discovery time and
     * nothing has maintained it since, so a table that grew while nobody looked reports a load past its own
     * total. Null is not narrowed to zero on the way through, because a table nothing counted and a table
     * counted as empty are the two readings a progress figure must never confuse.
     */
    private static Long estimatedRows(SourceModel discovered, String table) {
        if (discovered == null) {
            return null;
        }
        return discovered.tables().stream()
                .filter(candidate -> candidate.name().equals(table))
                .findFirst()
                .map(SourceTable::approximateRowCount)
                .orElse(null);
    }

    /**
     * What share of {@code total} the {@code loaded} rows are, as whole percent, or null where the share is
     * not a number anybody could act on.
     *
     * <p>Two cases have no share rather than a computed one. Without a total there is nothing to take a
     * share of, which is the read face's standing rule -- progress with no total is honest partial data and
     * is never dressed up as a complete load. A total of zero is the sharper case: it is kept, because it
     * is what discovery counted, but a load that read rows out of a table counted as empty says the count
     * is stale, and no percentage describes that.
     *
     * <p>A share past a hundred is reported as a hundred. The table's load is finished by the time this is
     * asked, so overshooting a stale estimate means complete, and a progress figure above full is not a state
     * anything can be in -- publishing one would leave every reader to decide for themselves what it meant.
     */
    private static Integer share(long loaded, Long total) {
        if (total == null || total <= 0L) {
            return null;
        }
        return (int) Math.min(100L, loaded * 100L / total);
    }

    @Override
    public SnapshotReading snapshotProgress(String pipelineId) {
        PipelineLoad load = loadsByPipeline.get(pipelineId);
        if (load == null) {
            return SnapshotReading.NONE;
        }
        // A table this run skipped may not have been published by its hand-off yet, because another
        // table is still being read. Its sink's durable completion mark already answers for it. The
        // measured count was saved before that sink could confirm the load; discovery's estimate is the
        // fallback for an older completed record without one.
        SnapshotReading current = runSnapshotProgress(pipelineId);
        List<SnapshotOnChain> covered = snapshotTablesByPipeline.getOrDefault(pipelineId, List.of());
        if (covered.isEmpty()) {
            return current;
        }
        Map<String, TableSnapshot> completed = new LinkedHashMap<>(current.byTable());
        boolean changed = false;
        for (SnapshotOnChain source : covered) {
            if (source.chainId().isEmpty()) {
                continue;
            }
            List<String> confirmed = storePort.meta().read(source.chainId().orElseThrow())
                    .map(record -> record.snapshotCompletedTables(source.consumerId())).orElse(List.of());
            for (String table : source.tables()) {
                if (!confirmed.contains(table)) {
                    continue;
                }
                String key = load.keyOf(source.sourceId(), table);
                if (key == null) {
                    continue;
                }
                TableSnapshot reading = completed.get(key);
                OptionalLong saved = SnapshotLoadCounts.read(storePort.keyedState(), source.consumerId(),
                        source.chainId().orElseThrow(), table);
                Long rows = saved.isPresent() ? saved.getAsLong()
                        : reading != null && reading.rowsDone() > 0L ? reading.rowsDone()
                        : reading != null && reading.rowsTotal() != null ? reading.rowsTotal()
                        : load.estimatedRows(source.sourceId(), table);
                if (rows == null) {
                    continue;
                }
                completed.put(key, new TableSnapshot(rows, rows, 100));
                changed = true;
            }
        }
        return changed ? new SnapshotReading(completed, load.began) : current;
    }

    @Override
    public SnapshotReading runSnapshotProgress(String pipelineId) {
        PipelineLoad load = loadsByPipeline.get(pipelineId);
        SnapshotReading settled = load == null ? SnapshotReading.NONE : load.reading();
        LiveSnapshot live = liveSnapshotsByPipeline.get(pipelineId);
        if (live == null) { return settled; }
        Map<String, TableSnapshot> current = new LinkedHashMap<>(settled.byTable());
        live.reading().byTable().forEach((table, moving) -> current.merge(table, moving, (complete, now) -> {
            long rows = Math.max(complete.rowsDone(), now.rowsDone());
            Long total = complete.rowsTotal() != null ? complete.rowsTotal() : now.rowsTotal();
            return new TableSnapshot(rows, total, share(rows, total));
        }));
        return new SnapshotReading(current, live.began);
    }

    /**
     * What this pipeline's source runs have taken in, added together. A pipeline reads through one run per
     * source and each counts its own tables, so the sum is over runs that do not overlap -- except where two
     * sources name a table the same, and there the sum is still the answer: both arrivals are rows this
     * pipeline read.
     *
     * <p>The <strong>latest</strong> start among the runs, for the reason the target side takes the latest
     * of its own. A start is how a consumer is told the series began again, and a total that falls with no
     * such signal beside it is a counter going backwards; a run that is replaced resets its own count, and
     * moving this instant forward with it makes the fall read as the restart it is.
     */
    @Override
    public CaptureReading capturedRows(String pipelineId) {
        List<PipelineRun> runs = runsByPipeline.get(pipelineId);
        if (runs == null) {
            return CaptureReading.NONE;
        }
        // No second guard for a run list that is empty: nothing would be summed and no start taken, which
        // is what nothing reported already is. A guard for it would be a branch no case can enter, and a
        // branch nothing can enter is where a different answer hides.
        Map<String, Map<String, Long>> rows = new LinkedHashMap<>();
        Map<String, Long> bytes = new LinkedHashMap<>();
        Instant since = null;
        // This pipeline's own runs, not the captures behind them: a capture two pipelines share reads
        // once, and each attached pipeline is told what reached it. Counting the owning run for both would
        // report one read twice.
        for (PipelineRun pipelineRun : runs) {
            CaptureHealth health = pipelineRun.run.health();
            health.receivedRows().forEach((table, byOp) -> byOp.forEach((symbol, count) ->
                    rows.computeIfAbsent(table, ignored -> new LinkedHashMap<>())
                            .merge(symbol, count, Long::sum)));
            // Added across this pipeline's runs like the counts, and for the same reason: two sources
            // reading one table is twice the work and twice the payload, not one of them.
            health.receivedBytes().forEach((table, size) -> bytes.merge(table, size, Long::sum));
            Instant start = health.countingSince();
            since = since == null || start.isAfter(since) ? start : since;
        }
        return new CaptureReading(rows, bytes, since);
    }

    /**
     * A load is delivered when this pipeline's own record shows every table its snapshot covers as
     * written -- the sink's mark, made when its frontier confirms that table's rows.
     *
     * <p>Asked of the same reckoning a fresh start re-reads from, deliberately and not merely for tidiness:
     * the tables a rebuild would read again are exactly the ones this reports as not delivered. Two
     * readings of that one fact would eventually disagree, and both directions of the disagreement are
     * silent -- a resume that rebuilds and then reads nothing, or one that carries on over a load nobody
     * will finish.
     *
     * <p>Note what is deliberately not asked: whether the bounded read returned. It returned long before
     * anyone could hold the pipeline, so that question answers yes for the whole window this one exists
     * for.
     */
    @Override
    public boolean loadDelivered(String pipelineId) {
        for (SnapshotOnChain snapshot : snapshotTablesByPipeline.getOrDefault(pipelineId, List.of())) {
            if (!SnapshotPhase.stillOwed(snapshot.chainId().flatMap(storePort.meta()::read), snapshot.consumerId(),
                    snapshot.tables()).isEmpty()) {
                return false;
            }
        }
        return true;
    }

    @Override
    public void stopCapture(String pipelineId, boolean purgeState) {
        PendingStart pending = pendingStarts.get(pipelineId);
        if (pending != null) {
            pending.cancel();
        }
        try (KeyedLocks.Hold<String> ignored = pipelineLocks.acquire(pipelineId)) {
            stopCaptureLocked(pipelineId, purgeState);
        }
    }

    private void stopCaptureLocked(String pipelineId, boolean purgeState) {
        // The load belongs to the run being torn down: a stopped pipeline reports no snapshot rather than the
        // rows its previous run happened to load.

        // Holding no runs is not the same as having nothing to release. A pipeline whose start threw part
        // way, and one whose process was replaced, both arrive here with no handles and a record that is
        // still all there -- and a stop asked to clear the state has that record to clear. Returning on the
        // absent handle is what made the verb report success and take nothing, in the one state a caller
        // reaches for it most: after a run has died.
        List<PipelineRun> runs = Objects.requireNonNullElse(runsByPipeline.get(pipelineId), List.of());
        List<KeyedLocks.Hold<CaptureId>> captureHolds = lockCaptures(runs.stream()
                .filter(PipelineRun::managed).map(PipelineRun::captureId).toList(), false);
        try {
            closeAbortedCapture(pipelineId, abortedRunsByPipeline.get(pipelineId));
            RuntimeException cleanupFailure = closeRuns(runs, pipelineId, purgeState);
            if (cleanupFailure != null) { throw cleanupFailure; }
            runsByPipeline.remove(pipelineId, runs);
            loadsByPipeline.remove(pipelineId);
            liveSnapshotsByPipeline.remove(pipelineId);
            snapshotTablesByPipeline.remove(pipelineId);
        } finally {
            releaseCaptures(captureHolds);
        }
    }

    /**
     * Releases live runs after a stop, or after a later source prevents a multi-source start from completing.
     *
     * <p>{@code purgeState} decides only whether the source-side record is let go of as well; the hold on
     * the chain is given back either way, because holding it is what a running pipeline does and this one
     * has stopped.
     *
     * <p>Close this pipeline's attachment first. A shared capture tail remains while another local pipeline
     * references it and closes with the last attachment; an unshared run closes immediately. Then give back
     * this pipeline's hold on each chain it read -- a shared-ring run only; a run that opened no chain has
     * nothing to release. The chain itself closes when the pipeline giving it back was the last one on it,
     * which is the coordinator's
     * to decide: a stop tears nothing down, because a chain several pipelines read is not this one's to take
     * away. Stopping one used to remove it outright, and what that cost the others was measured -- their own
     * stop then threw, and a pipeline restarted afterwards read under a generation of its own.
     *
     * <p>Every step runs even when an earlier one throws, and the first failure carries the rest as
     * suppressed. A release abandoned half way is what leaves a chain nobody owns and a daemon nobody stops.
     *
     * <p>A clearing then sweeps whatever the record still holds for this pipeline beyond those runs, which
     * is the whole of it when there were no runs to hold anything.
     */
    private RuntimeException closeRuns(List<PipelineRun> runs, String pipelineId, boolean purgeState) {
        return closeRuns(runs, pipelineId, purgeState, false);
    }

    private RuntimeException closeRuns(List<PipelineRun> runs, String pipelineId, boolean purgeState, boolean abortedStart) {
        RuntimeException firstFailure = null;
        boolean capturesStopped = true;
        Map<MiningChainId, Set<String>> consumersByChain = new LinkedHashMap<>();
        Set<String> progressIds = new LinkedHashSet<>();
        progressIds.add(pipelineId);
        SourceReaders readers = new SourceReaders(pipelineId);
        for (PipelineRun pipelineRun : runs) {
            CaptureRun run = pipelineRun.run;
            try {
                // The inner managed-open refusal already tried this exact handle. Retain its
                // failure for rollback without repeating native close in the same failed attempt.
                if (abortedStart && pipelineRun.closeState.failure != null) {
                    throwShutdownFailure(pipelineRun.closeState.failure);
                }
                closeRun(pipelineRun, pipelineId);
            } catch (RuntimeException failure) {
                capturesStopped = false;
                if (firstFailure == null) {
                    firstFailure = failure;
                } else {
                    firstFailure.addSuppressed(failure);
                }
            }
            // Collected whether or not the close succeeded: a daemon that refused to stop does not make the
            // consumer membership this pipeline holds any less this pipeline's to give back.
            progressIds.add(pipelineRun.consumerId());
            run.chainId().ifPresent(chain -> consumersByChain.computeIfAbsent(chain, ignored -> new LinkedHashSet<>())
                    .add(pipelineRun.consumerId()));
            run.chainId().ifPresent(chain -> readers.ran(chain, pipelineRun.spec()));
        }
        // A live drain cannot detach an empty queue: capture may already hold that queue and append into it
        // after the drain returns. Lifecycle teardown has no such race once every capture close returned, so
        // this is where all of the pipeline's queues and their coordinate strings are released. Keep them when
        // a close failed because that capture may still be appending.
        if (capturesStopped) {
            snapshotBuffer.release(pipelineId);
        }
        if (!capturesStopped && !abortedStart) { return firstFailure; }
        // A whole-pipeline stop releases all of its source and legacy memberships atomically, then
        // decides whether the shared record can be removed after the final membership leaves.
        for (Map.Entry<MiningChainId, Set<String>> entry : consumersByChain.entrySet()) {
            MiningChainId chainId = entry.getKey();
            // Whether this pipeline was the last one on the chain, which decides how much of the chain's
            // record is this stop's to take. Read from the release itself rather than asked again after
            // it: a consumer attaching in between would make a second reading stale, and the two answers
            // would then disagree about a record one of them is about to delete.
            boolean chainClosed = false;
            try {
                chainClosed = srsCoordinator.releasePipelineConsumers(chainId, pipelineId);
            } catch (RuntimeException failure) {
                if (firstFailure == null) {
                    firstFailure = failure;
                } else {
                    firstFailure.addSuppressed(failure);
                }
            }
            if (!purgeState) {
                continue;
            }
            boolean nobodyLeft = false;
            if (chainClosed) {
                try {
                    nobodyLeft = nobodyElseOnTheRecord(chainId, pipelineId);
                } catch (RuntimeException failure) {
                    if (firstFailure == null) {
                        firstFailure = failure;
                    } else {
                        firstFailure.addSuppressed(failure);
                    }
                }
            }
            if (nobodyLeft) {
                // Nobody is left on it, so the whole record goes: the read offset, the seam the tail
                // resumes from, the schema history, and which tables finished their initial load. This
                // is what makes the next run of this pipeline read its source from the beginning, which
                // is what asking for the state to be cleared meant. What the chain's reader set up on the
                // source goes with it, after the record: see purgeChain.
                firstFailure = runCleanup(() -> purgeChain(chainId.value(), pipelineId, readers), firstFailure);
            } else {
                // Others are still reading it, so only this pipeline's own cursor is its to give back.
                // Run whether or not the release above succeeded, and safe to run twice: the detach
                // states the end condition "this consumer holds nothing here", which an absent chain and
                // an absent cursor already satisfy. Skipping it after one failure is what leaves a cursor
                // nobody will ever advance holding back every pipeline still on the chain.
                for (String consumerId : entry.getValue()) {
                    firstFailure = runCleanup(
                            () -> storePort.meta().detachConsumer(chainId.value(), consumerId), firstFailure);
                }
            }
        }
        if (purgeState) {
            // What is left to clear is asked of the record, because the handles above cannot answer it.
            // A pipeline holds no run here after a start that threw part way, and after a process came
            // up over an earlier one's work -- and in both, the cursor it left and everything the chain
            // accumulated for it are exactly what clearing the state was asked to take. Deriving the
            // work from the handles alone answers "nothing" for both while reporting that it worked,
            // which is the one state a caller most wants cleared: the one after a run died.
            //
            // Asked after the loop above rather than instead of it, so nothing is reached twice: a chain
            // that loop dropped has no record left to name, and one it detached no longer carries this
            // pipeline. The loop keeps deciding from the release itself for the chains it holds, where
            // that reading is the one that cannot go stale under a consumer attaching in between.
            for (String chainId : storePort.meta().miningChainIdsWithConsumer(pipelineId)) {
                storePort.meta().consumerOffsets(chainId).stream()
                        .map(offset -> offset.pipelineId())
                        .filter(consumer -> SrsConsumerId.belongsTo(consumer, pipelineId))
                        .forEach(progressIds::add);
                firstFailure = purgeWhatTheRecordStillHolds(chainId, pipelineId, readers, firstFailure);
            }
            for (String progressId : progressIds) {
                firstFailure = runCleanup(
                        () -> storePort.keyedState().dropNamespace(SnapshotLoadCounts.namespaceOf(progressId)),
                        firstFailure);
            }
        }
        return firstFailure;
    }

    /**
     * Whether the chain's record names nobody but {@code pipelineId}, the second half of "nobody is left
     * on it" where a chain can be read from more than one member.
     *
     * <p>This member's release answers for the pipelines it runs and for no others, and a pipeline another
     * member drives over the same chain keeps its own load and cursor in that record. Dropping the chain
     * because this member has nobody left would take them from it: its next start reads its whole source
     * again, and nothing says why. So the record is asked as well, and only the two together license the
     * drop. A single member runs every pipeline there is, and its own answer is the whole of it.
     */
    private boolean nobodyElseOnTheRecord(MiningChainId chainId, String pipelineId) {
        if (!managedOwnership) {
            return true;
        }
        return storePort.meta().consumerOffsets(chainId.value()).stream()
                .allMatch(offset -> SrsConsumerId.belongsTo(offset.pipelineId(), pipelineId));
    }

    private static final class CloseState {
        private boolean completed;
        private Throwable failure;
    }

    private static void closeOnce(CloseState state, Runnable action) {
        if (state.completed) { return; }
        try { action.run(); state.completed = true; state.failure = null; }
        catch (RuntimeException | Error refused) { state.failure = refused; throw refused; }
    }

    private void closeRun(PipelineRun pipelineRun, String pipelineId) {
        closeOnce(pipelineRun.closeState, () -> closeRunNow(pipelineRun, pipelineId));
    }

    private void closeCaptureRun(CaptureRun actual) {
        actual.close();
        if (actual.loadOverWhenHandedBack()) { return; }
        long remaining = closing ? Math.max(0L, shutdownDeadline - System.nanoTime()) : 0L;
        try {
            if (!actual.awaitLoaded(Duration.ofNanos(remaining))) { throw incompleteShutdown(1, null); }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw incompleteShutdown(1, interrupted);
        }
        // A legacy load may have opened its tail while cancellation waited for the real read to end.
        // Closing that same handle again after the actual worker exit cannot select a new owner.
        actual.close();
    }

    private void closeOwnedCapture(OwnedCapture expected) {
        Throwable failure = null;
        try { closeOnce(expected.nativeClose, () -> closeCaptureRun(expected.run)); }
        catch (RuntimeException | Error refused) { failure = refused; }
        try { closeOnce(expected.leaseClose, expected.lease::close); }
        catch (RuntimeException | Error refused) { failure = combineShutdownFailure(failure, refused); }
        if (failure != null) { throwShutdownFailure(failure); }
    }

    private void closeRunNow(PipelineRun pipelineRun, String pipelineId) {
        if (!pipelineRun.managed) {
            closeCaptureRun(pipelineRun.run);
            return;
        }
        OwnedCapture owned = ownedCaptures.get(pipelineRun.captureId);
        if (owned == null) {
            forgetJoined(pipelineRun.captureId, pipelineId);
            closeCaptureRun(pipelineRun.run);
            return;
        }
        owned.pipelines.remove(pipelineId);
        if (!owned.pipelines.isEmpty()) {
            if (pipelineRun.run != owned.run) {
                closeCaptureRun(pipelineRun.run);
            } else {
                // The run stays, because the pipelines still on the capture read what it goes on to do; the
                // load in it was this pipeline's alone, so that is let go of. Left reading, it would end on
                // the release of this pipeline's hand-off below, as a failure of the run the others read --
                // or, not waiting for room at that moment, go on handing rows to a queue nothing declares,
                // bounds or drains.
                owned.run.abandonLoad();
            }
            return;
        }
        Throwable failure = null;
        try { closeOwnedCapture(owned); }
        catch (RuntimeException | Error refused) { failure = refused; }
        if (pipelineRun.run != owned.run) {
            try { closeCaptureRun(pipelineRun.run); }
            catch (RuntimeException | Error refused) { failure = combineShutdownFailure(failure, refused); }
        }
        if (failure != null) { throwShutdownFailure(failure); }
        ownedCaptures.remove(pipelineRun.captureId, owned);
    }

    private void forgetJoined(CaptureId captureId, String pipelineId) {
        JoinedCapture joined = joinedCaptures.get(captureId);
        if (joined == null) {
            return;
        }
        joined.pipelines.remove(pipelineId);
        if (joined.pipelines.isEmpty()) {
            joinedCaptures.remove(captureId);
        }
    }

    /**
     * Clears one chain's record of a pipeline this coordinator holds no run for: the whole chain when
     * nobody else is on it, and only that pipeline's own cursor otherwise -- the same two branches a stop
     * takes for a chain it does hold, decided from the durable record instead of from the release.
     *
     * <p>"Nobody else" is asked of the record <em>and</em> of this process, because neither answers it
     * alone. The record does not name a consumer that has attached but not yet written anything of its
     * own; this process does not know a consumer running on any other member. Taking a chain away from a
     * pipeline still reading it is not an error that pipeline reports -- it reads its whole source again,
     * quietly -- so the two are read together and only their agreement licenses the drop.
     */
    private RuntimeException purgeWhatTheRecordStillHolds(
            String chainId, String pipelineId, SourceReaders readers, RuntimeException firstFailure) {
        boolean lastOneOff = storePort.meta().consumerOffsets(chainId).stream()
                        .allMatch(offset -> SrsConsumerId.belongsTo(offset.pipelineId(), pipelineId))
                && !srsCoordinator.isProvisioned(new MiningChainId(chainId));
        return runCleanup(
                () -> {
                    if (lastOneOff) {
                        purgeChain(chainId, pipelineId, readers);
                    } else {
                        storePort.meta().detachConsumer(chainId, pipelineId);
                    }
                },
                firstFailure);
    }

    /**
     * Lets go of everything a chain nobody reads any more accumulated: its record, then what its reader set up
     * on the source to read it -- a replication slot -- and then the connector notes that named it.
     *
     * <p>In that order because of what each way of stopping part way leaves behind. Stopped after the record,
     * a clearing leaves a slot nothing needs, or notes naming one that is gone, and the next run on the chain
     * starts from a record with no position: it reads its source as a first run does, and the notes left over
     * are dropped before that record is created (see requestSharedSelections). Dropped last, the record would
     * outlive a slot that is gone, and the next run would resume from a position the source no longer keeps --
     * quietly skipping whatever happened in between. The notes cannot go first either, because the release
     * needs them to find what it lets go of.
     *
     * <p>A source that refuses is said and left as it is. The state is cleared either way: a clearing that
     * failed over a source it cannot reach would leave a pipeline that can neither keep its state nor let go of
     * it. What cannot be let go of on this side -- the record, the notes -- fails the step like any other.
     */
    private void purgeChain(String chainId, String pipelineId, SourceReaders readers) {
        List<CaptureRunSpec> read = readers.of(new MiningChainId(chainId));
        // Read while the record still says who was on the chain: where no source defined now reads it, what the
        // notes name on the source is all there is to point at.
        List<String> named = read.isEmpty() ? namedInTheNotesOf(chainId) : List.of();
        storePort.meta().dropChain(chainId);
        releaseOnTheSource(pipelineId, chainId, read, named);
        dropChainNotes(chainId);
    }

    /** Drops the connector notes filed under a physical capture, and the markers their migration keeps. */
    private void dropChainNotes(String chainId) {
        String connectorNamespace = ConnectorStateNamespace.ofShared(chainId);
        storePort.keyedState().dropNamespace(connectorNamespace);
        storePort.keyedState().dropNamespace(ConnectorStateNamespace.migrationOf(connectorNamespace));
    }

    /** What the chain's notes, and the notes of each source recorded on it, name on the source. */
    private List<String> namedInTheNotesOf(String chainId) {
        List<String> namespaces = new ArrayList<>();
        namespaces.add(ConnectorStateNamespace.ofShared(chainId));
        for (ConsumerOffset consumer : storePort.meta().consumerOffsets(chainId)) {
            SrsConsumerId.sourceOf(consumer.pipelineId()).ifPresent(source -> namespaces.add(ConnectorStateNamespace.of(
                    new PipelineNode(SrsConsumerId.pipelineOf(consumer.pipelineId()), source))));
        }
        return PdkCapturePort.namedIn(storePort.keyedState(), namespaces);
    }

    /**
     * Has the source connector of one of {@code read} -- the specs this pipeline read the chain through -- let
     * go of what it set up on the source for the chain. Once per chain, never once per source: every source
     * on a chain reads it through the same notes, and a chain read directly has one source.
     */
    private void releaseOnTheSource(String pipelineId, String chainId, List<CaptureRunSpec> read, List<String> named) {
        Optional<CaptureRunSpec> reader = read.stream()
                .filter(spec -> CapturePlan.forReadMode(spec.readMode()).cdc())
                .findFirst();
        if (reader.isEmpty()) {
            LOG.warn("Cleared chain {} of pipeline {}, but no source the pipeline is defined to read reads it any "
                    + "more, so what its connector set up on the source to read it is left there; remove it on "
                    + "the source by hand: {}", chainId, pipelineId,
                    named.isEmpty() ? "nothing its notes name" : String.join(", ", named));
            return;
        }
        CaptureRunSpec spec = reader.get();
        starter().release(spec).ifPresent(refused -> LOG.warn(
                "Cleared pipeline {}, but source {} kept what its connector set up there to read changes; remove "
                        + "it on the source by hand: {}", pipelineId, spec.sourceId(), refused.getMessage()));
    }

    /** Whichever seam starts runs here: the attacher where captures are owned, the starter where they are not. */
    private CaptureStarter starter() {
        return managedOwnership ? captureAttacher : captureStarter;
    }

    /**
     * Which of a pipeline's sources read which chain, for a clearing to let go of what they set up there: the
     * specs its runs here were started from, and for a chain it holds no run of here, the specs its stored
     * definition gives now -- a clearing is as often asked of a pipeline whose runs are gone, after a start
     * that threw part way or a process that was replaced.
     *
     * <p>The definition is read once, and only if a chain turns out to need it. One that cannot be read now
     * releases nothing on the chains it would have answered for, and says so: the clearing goes on, and what
     * was set up on the source stays until somebody removes it there.
     */
    private final class SourceReaders {

        private final String pipelineId;
        private final Map<MiningChainId, List<CaptureRunSpec>> ran = new LinkedHashMap<>();
        private Map<MiningChainId, List<CaptureRunSpec>> defined;

        SourceReaders(String pipelineId) {
            this.pipelineId = pipelineId;
        }

        void ran(MiningChainId chainId, CaptureRunSpec spec) {
            ran.computeIfAbsent(chainId, ignored -> new ArrayList<>()).add(spec);
        }

        List<CaptureRunSpec> of(MiningChainId chainId) {
            List<CaptureRunSpec> held = ran.get(chainId);
            if (held != null) {
                return held;
            }
            if (defined == null) {
                defined = definedReaders();
            }
            return defined.getOrDefault(chainId, List.of());
        }

        private Map<MiningChainId, List<CaptureRunSpec>> definedReaders() {
            Map<MiningChainId, List<CaptureRunSpec>> byChain = new LinkedHashMap<>();
            try {
                ArtifactStore captured = artifacts();
                PipelineResource pipeline = StoredArtifacts.requirePipeline(captured, pipelineId);
                for (SourceRef ref : pipeline.sources()) {
                    SourceResource source = StoredArtifacts.requireSource(captured, ref.id());
                    Optional<SourceCaptureResolution> selected = SourceCaptureResolution.forPipeline(
                            pipeline, source, SourceDiscovery.model(storePort, source));
                    if (selected.isEmpty()) {
                        continue;
                    }
                    CaptureRunSpec spec = deriveSpec(pipelineId, pipeline.settings(), source, selected.get(),
                            srsSwitchOf(pipelineId, ref));
                    byChain.computeIfAbsent(spec.miningChainId(), ignored -> new ArrayList<>()).add(spec);
                }
            } catch (RuntimeException unreadable) {
                LOG.warn("Clearing pipeline {} could not read which sources it is defined to read, so nothing its "
                        + "connectors set up on them to read changes is let go of there", pipelineId, unreadable);
            }
            return byChain;
        }
    }

    /** Runs one release step, keeping the first failure and hanging any later one off it as suppressed. */
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
     * Derives one source run spec from the source, the pipeline settings, the shared resolution, and this
     * pipeline's own srs switch for that source. The read axis comes from settings (read mode defaulting to
     * snapshot-then-cdc, start position to latest); the srs switch is passed in rather than read off the
     * source, and there is deliberately no fallback to the source here -- one would put back exactly the
     * coupling that made an edit to a source re-route every pipeline reading it.
     *
     * <p>The start position defaults to latest because that is what the setting publishes as its default and
     * what the canonical form encodes by dropping an explicit {@code latest}. Filling in earliest instead
     * disagreed with both, and the disagreement is not cosmetic: for a tail that reads its source directly,
     * earliest is the oldest change the source still retains, so a first run replays the whole retention
     * window rather than picking up from now.
     */
    static CaptureRunSpec deriveSpec(
            String pipelineId, Settings settings, SourceResource source, SourceCaptureResolution resolution,
            boolean srsEnabled) {
        return deriveSpec(pipelineId, settings, source, resolution, srsEnabled, 1L);
    }

    private static CaptureRunSpec deriveSpec(
            String pipelineId, Settings settings, SourceResource source, SourceCaptureResolution resolution,
            boolean srsEnabled, long snapshotEpoch) {
        ReadMode readMode = readModeOf(settings);
        String startFromRaw = settings != null && settings.startFrom() != null
                ? settings.startFrom() : "latest";
        String retention = source.srs() != null ? source.srs().retention() : null;
        return new CaptureRunSpec(
                // Unscoped on purpose. The run scopes its own config to the pipeline and source it names,
                // so the node a connector files its notes under is worked out in one place rather than
                // here as well.
                resolution.config(),
                readMode,
                resolution.srsKey(),
                srsEnabled,
                resolution.sourceId(),
                pipelineId,
                StartFrom.parse(startFromRaw),
                retention,
                MOCK_SCHEMA_VER,
                snapshotEpoch).withConsumerId(SrsConsumerId.of(pipelineId, source.id()).value());
    }

    private static ReadMode readModeOf(Settings settings) {
        return settings != null && settings.readMode() != null
                ? settings.readMode() : ReadMode.SNAPSHOT_AND_CDC;
    }

    /**
     * This pipeline's own srs switch for that source. Apply records one on every reference it stores, so a
     * reference without one has never been through apply -- an invariant violation rather than anything an
     * author did, and so a bare crash naming both halves rather than a coded diagnostic. Guessing a value
     * here is the one thing this must not do: it would read as a working pipeline running the other way.
     */
    private static boolean srsSwitchOf(String pipelineId, SourceRef ref) {
        if (ref instanceof SourceRef.Spec spec) {
            return spec.srs();
        }
        throw new IllegalStateException(
                "pipeline '" + pipelineId + "' has no srs switch recorded for source '" + ref.id() + "'");
    }

    @Override
    public Optional<Throwable> captureFailure(String pipelineId) {
        List<PipelineRun> runs = runsByPipeline.get(pipelineId);
        if (runs == null) {
            return Optional.empty();
        }
        // Surface a failure of this pipeline's own run or a tail it shares with another pipeline. A
        // snapshot-only capture has no shared tail; its owner's load failure belongs to that owner alone.
        return runs.stream()
                .map(run -> {
                    Optional<Throwable> ownFailure = run.run.failure();
                    if (ownFailure.isPresent()) {
                        return ownFailure;
                    }
                    OwnedCapture shared = run.sharedTail ? ownedCaptures.get(run.captureId) : null;
                    return shared == null ? Optional.<Throwable>empty() : shared.run.failure();
                })
                .filter(Optional::isPresent)
                .map(Optional::get)
                .findFirst();
    }

    /** Whether this pipeline currently has a live capture -- a test-visible view of the retained handles. */
    boolean isActive(String pipelineId) {
        return runsByPipeline.containsKey(pipelineId);
    }

    @Override
    public boolean hasActiveCapture(String pipelineId) {
        return isActive(pipelineId) || abortedRunsByPipeline.containsKey(pipelineId);
    }

    @Override
    public void activateSnapshot(String pipelineId) {
        List<PipelineRun> runs = runsByPipeline.get(pipelineId);
        if (runs != null) {
            runs.forEach(pipelineRun -> pipelineRun.run.activateSnapshot());
        }
    }

    @Override
    public void activateSnapshot(String pipelineId, io.tapstate.core.logging.LogSink.Scope scope) {
        Objects.requireNonNull(scope, "scope");
        List<PipelineRun> runs = runsByPipeline.get(pipelineId);
        if (runs != null) {
            runs.forEach(pipelineRun -> pipelineRun.run.activateSnapshot(scope));
        }
    }

    private ArtifactStore artifacts() {
        return storePort.artifacts();
    }

    /**
     * The snapshot pass-through for one source: it appends each snapshot row to the shared buffer under this
     * consumer pipeline and the source's change-ring name. Only that pipeline's source vertex can drain the
     * rows, then emits them ahead of the cdc tail, so the snapshot flows through the same transform-to-sink
     * chain as cdc, strictly before it. A read mode that runs no snapshot never calls this, so the buffer for
     * that pipeline and ring stays empty and the source is a pure tail.
     *
     * <p>A declared load is held to a few thousand rows in that buffer, so an append waits whenever the
     * pipeline has not yet taken what is there -- which is how the pipeline's own pace reaches the source
     * read. When a table's load is through, its declaration is ended, which lets the source vertex move on
     * to the ring, and the table is published to the read face with what it read.
     */
    private CaptureHandoff snapshotPassthrough(
            String pipelineId, SourcePlan plan, Map<String, Long> observedSnapshotCounts, PipelineLoad load, LiveSnapshot liveSnapshot) {
        SourceCaptureResolution resolution = plan.resolution();
        Set<String> selectedTables = Set.copyOf(resolution.tables());
        CaptureRunSpec spec = plan.spec();
        CapturePlan phases = CapturePlan.forReadMode(spec.readMode());
        Optional<String> chainId = phases.snapshot() && phases.cdc()
                ? Optional.of(spec.miningChainId().value()) : Optional.empty();
        return new CaptureHandoff() {
            @Override
            public void accept(Envelope event) {
                if (!selectedTables.contains(event.src())) {
                    throw new TapstateException(
                            CaptureError.EVENT_TABLE_NOT_SELECTED, Map.of("table", event.src()), null);
                }
                String ring = resolution.ringName(event.src());
                if (event.op() == Op.READ) {
                    observedSnapshotCounts.merge(event.src(), 1L, Long::sum);
                    if (liveSnapshot != null) { liveSnapshot.received(plan.sourceId(), event.src()); }
                    if (snapshotBuffer.hasSnapshot(pipelineId, ring)) {
                        if (!snapshotBuffer.hasSnapshot(pipelineId, ring, spec.snapshotWriterToken())) {
                            throw new CancellationException("snapshot row belongs to an obsolete capture run");
                        }
                        // The deferred reader has already appended this row to its exact bounded session.
                        return;
                    }
                }
                snapshotBuffer.append(pipelineId, ring, event);
            }

            @Override
            public void loaded(String table) {
                String ring = resolution.ringName(table);
                if (snapshotBuffer.hasSnapshot(pipelineId, ring)
                        && !snapshotBuffer.hasSnapshot(pipelineId, ring, spec.snapshotWriterToken())) {
                    throw new CancellationException("snapshot completion belongs to an obsolete capture run");
                }
                saveLoadCountIfOwed(spec, chainId, table, observedSnapshotCounts.getOrDefault(table, 0L));
                snapshotBuffer.endSnapshot(pipelineId, resolution.ringName(table));
                load.loaded(plan.sourceId(), table, observedSnapshotCounts.getOrDefault(table, 0L),
                        estimatedRows(plan.discovered(), table));
            }
        };
    }

}
