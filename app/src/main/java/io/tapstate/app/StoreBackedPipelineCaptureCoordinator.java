package io.tapstate.app;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.Op;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.ReadMode;
import io.tapstate.core.model.Settings;
import io.tapstate.core.model.SourceRef;
import io.tapstate.core.model.SourceResource;
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
import io.tapstate.spi.store.SrsMeta;
import io.tapstate.spi.store.SourceModel;
import io.tapstate.spi.store.SourceTable;
import io.tapstate.spi.store.StorePort;
import io.tapstate.core.lifecycle.CaptureReading;
import io.tapstate.core.lifecycle.SnapshotReading;
import io.tapstate.core.lifecycle.TableSnapshot;

import java.time.Duration;
import java.time.Instant;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
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
 * In cluster mode, one CAPTURE claim owns the shared tail for a normalized source/read contract; later local
 * pipelines attach to that tail without opening the source again, and another member cannot cross the claim.
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

    /** One member-wide poll of durable expansion requests for captures this member owns. */
    private volatile ScheduledExecutorService physicalReconfigurations;

    /** What each running pipeline's load read, keyed by pipeline; dropped when it stops. */
    private final Map<String, SnapshotReading> snapshotsByPipeline = new ConcurrentHashMap<>();
    private final Map<String, LiveSnapshot> liveSnapshotsByPipeline = new ConcurrentHashMap<>();

    /**
     * The tables each running pipeline's snapshot covers, per chain; dropped when it stops. Only the
     * durable record can say whether a load reached the target, and it answers per pipeline, per chain,
     * per table -- so what is kept here is the question rather than the answer: which tables to ask about
     * and on which chain to ask. The map above cannot stand in for it. That one is keyed by table name
     * alone, qualified on collision, and carries no chain; and it holds an entry for every selected table
     * from the moment a run starts, so a set covering it is covered from the start.
     */
    private final Map<String, List<SnapshotOnChain>> snapshotTablesByPipeline = new ConcurrentHashMap<>();

    StoreBackedPipelineCaptureCoordinator(
            StorePort storePort, CaptureStarter captureStarter, SrsCoordinator srsCoordinator,
            SnapshotBuffer snapshotBuffer) {
        this.storePort = Objects.requireNonNull(storePort, "storePort");
        this.captureStarter = Objects.requireNonNull(captureStarter, "captureStarter");
        this.captureAttacher = null;
        this.ownership = CaptureOwnership.single();
        this.claimRenewInterval = Duration.ZERO;
        this.managedOwnership = false;
        this.srsCoordinator = Objects.requireNonNull(srsCoordinator, "srsCoordinator");
        this.snapshotBuffer = Objects.requireNonNull(snapshotBuffer, "snapshotBuffer");
    }

    StoreBackedPipelineCaptureCoordinator(
            StorePort storePort,
            CaptureAttacher captureAttacher,
            SrsCoordinator srsCoordinator,
            SnapshotBuffer snapshotBuffer,
            CaptureOwnership ownership,
            Duration claimRenewInterval) {
        this.storePort = Objects.requireNonNull(storePort, "storePort");
        this.captureStarter = null;
        this.captureAttacher = Objects.requireNonNull(captureAttacher, "captureAttacher");
        this.ownership = Objects.requireNonNull(ownership, "ownership");
        this.claimRenewInterval = Objects.requireNonNull(claimRenewInterval, "claimRenewInterval");
        this.managedOwnership = true;
        this.srsCoordinator = Objects.requireNonNull(srsCoordinator, "srsCoordinator");
        this.snapshotBuffer = Objects.requireNonNull(snapshotBuffer, "snapshotBuffer");
    }

    @Override
    public void startCapture(String pipelineId) {
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
            // A duplicate start observes the first one's published handles rather than opening its sources.
            if (runsByPipeline.containsKey(pipelineId)) {
                return;
            }
            PendingStart pending = new PendingStart(Thread.currentThread());
            pendingStarts.put(pipelineId, pending);
            try {
                startCaptureLocked(pipelineId, artifactSnapshot, cursorWriterToken, pending);
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
        List<SourcePlan> plans = withCompleteChainSelections(
                plan(pipelineId, pipeline, captured, snapshotEpoch),
                Objects.requireNonNull(cursorWriterToken, "cursorWriterToken"));
        pending.covers(plans.stream().map(SourcePlan::captureId).toList());
        // Capture locks are sorted so multi-source starts cannot deadlock. Holding one only blocks other
        // pipelines that actually share that capture; an unrelated snapshot never holds the whole member.
        List<KeyedLocks.Hold<CaptureId>> captureHolds = lockCaptures(plans.stream()
                .map(SourcePlan::captureId).distinct().toList(), true);
        try {
            ensurePhysicalSelections(plans);
            try {
                startPlannedCapture(pipelineId, plans, pending);
            } catch (TapstateException refused) {
                if (refused.code() != CaptureError.SHARED_SELECTION_RESTART_REQUIRED) {
                    throw refused;
                }
                // The first publication and a remote request can cross after preflight. Register the
                // demand again against the now-published selection and retry on a later pass.
                ensurePhysicalSelections(plans);
                throw new RingNotOpenYet(plans.getFirst().captureId());
            }
        } finally {
            releaseCaptures(captureHolds);
        }
    }

    /** Registers missing tables before any new snapshot or reader can rely on the physical tail. */
    private void ensurePhysicalSelections(List<SourcePlan> plans) {
        for (SourcePlan plan : plans) {
            CaptureRunSpec spec = plan.spec();
            if (!spec.srsEnabled() || spec.readMode() == ReadMode.SNAPSHOT_ONLY) {
                continue;
            }
            MiningChainId chain = MiningChainId.resolve(spec.config(), spec.srsKey());
            SrsMeta record = storePort.meta().read(chain.value()).orElse(null);
            if (record == null || record.epoch() < 1) {
                continue;
            }
            List<String> wanted = spec.selectedChainTables() == null
                    ? spec.config().streams() : spec.selectedChainTables();
            var selection = storePort.meta().physicalSelection(chain.value()).orElse(null);
            if (selection != null && selection.epoch() == record.epoch()
                    && selection.tables().containsAll(wanted)) {
                continue;
            }
            if (!storePort.meta().requestPhysicalTables(chain.value(), record.epoch(), wanted)) {
                throw new RingNotOpenYet(plan.captureId());
            }
            if (selection == null) {
                // A tail not yet published must include every request in its first subscription. Its
                // conditional publication refuses a union that raced this request.
                continue;
            }
            OwnedCapture owned = ownedCaptures.get(plan.captureId());
            if (owned != null && selection.epoch() == record.epoch()) {
                reopenOwnedPhysicalTail(plan.captureId(), owned);
                var expanded = storePort.meta().physicalSelection(chain.value()).orElse(null);
                if (expanded != null && expanded.epoch() == record.epoch()
                        && expanded.tables().containsAll(wanted)) {
                    continue;
                }
            }
            throw new RingNotOpenYet(plan.captureId());
        }
    }

    /** Called under this capture's lock; the old subscription must be closed before the new one opens. */
    private void reopenOwnedPhysicalTail(CaptureId captureId, OwnedCapture owned) {
        owned.reconfiguring = Thread.currentThread();
        try {
            CaptureRun replacement = captureAttacher.reopenPhysicalTail(owned.spec, owned.run);
            if (ownedCaptures.get(captureId) != owned) {
                replacement.close();
                throw new CancellationException("capture owner changed during physical expansion");
            }
            owned.run = replacement;
        } catch (RuntimeException | Error failure) {
            owned.run.health().fail(failure);
            throw failure;
        } finally {
            owned.reconfiguring = null;
        }
    }

    private void startPlannedCapture(String pipelineId, List<SourcePlan> plans, PendingStart pending) {
        Map<CaptureId, OpeningClaim> permits = new LinkedHashMap<>();
        List<PipelineRun> runs = new ArrayList<>();
        List<AttributedSnapshot> attributed = new ArrayList<>();
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
        try {
            settlePermits(pipelineId, plans, permits, pending);
            for (SourcePlan plan : plans) {
                pending.check();
                SourceCaptureResolution resolution = plan.resolution();
                CaptureRunSpec spec = plan.spec();
                CaptureId captureId = plan.captureId();
                Map<String, Long> observedSnapshotCounts = new ConcurrentHashMap<>();
                Consumer<Envelope> receive = snapshotPassthrough(pipelineId, plan.sourceId(), resolution,
                        spec.cursorWriterToken(), liveSnapshot, observedSnapshotCounts);
                CaptureRun run;
                if (!managedOwnership) {
                    run = captureStarter.start(spec, receive);
                    runs.add(PipelineRun.unmanaged(run));
                } else {
                    OwnedCapture existing = ownedCaptures.get(captureId);
                    if (existing != null) {
                        run = captureAttacher.start(
                                spec.withCaptureFence(existing.permit.fence()), receive, false);
                        existing.pipelines.add(pipelineId);
                    } else {
                        OpeningClaim opening = permits.get(captureId);
                        if (opening.permit.acquired()) {
                            // The lease already renews this claim before the source can block in snapshot.
                            permits.remove(captureId);
                            CaptureRun started = null;
                            try {
                                opening.check();
                                CaptureRunSpec ownedSpec = spec.withCaptureFence(opening.permit.fence());
                                started = captureAttacher.start(ownedSpec, receive, true);
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
                                opening.publish(run, pipelines, ownedSpec, receive);
                            } catch (RuntimeException | Error failure) {
                                boolean interrupted = Thread.interrupted();
                                try {
                                    if (started != null) {
                                        try {
                                            started.close();
                                        } catch (RuntimeException unclosed) {
                                            failure.addSuppressed(unclosed);
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
                            run = captureAttacher.start(spec, receive, false);
                            joinedCaptures.computeIfAbsent(captureId, ignored -> new JoinedCapture(spec, receive))
                                    .pipelines.add(pipelineId);
                            lookForCapturesNobodyTails();
                        }
                    }
                    runs.add(PipelineRun.managed(captureId, run));
                }
                pending.check();
                recordSnapshot(attributed, plan.sourceId(), spec, run, observedSnapshotCounts, plan.discovered());
                snapshotOnChain(spec, run).ifPresent(snapshotTables::add);
            }
            pending.check();
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
                RuntimeException cleanupFailure = closeRuns(runs, pipelineId, false);
                if (cleanupFailure != null) {
                    failure.addSuppressed(cleanupFailure);
                }
                releaseUnopened(permits, failure);
            } finally {
                if (interrupted) {
                    Thread.currentThread().interrupt();
                }
            }
            throw failure;
        }
        runsByPipeline.put(pipelineId, runs);
        snapshotsByPipeline.put(pipelineId, reading(attributed, loadBegan));
        snapshotTablesByPipeline.put(pipelineId, List.copyOf(snapshotTables));
    }

    /** One source of a start as resolved before any claim or source is opened. */
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

    /** Every source of one pipeline on a mining chain publishes the same complete table selection. */
    private static List<SourcePlan> withCompleteChainSelections(
            List<SourcePlan> plans, String cursorWriterToken) {
        Map<MiningChainId, LinkedHashSet<String>> selected = new LinkedHashMap<>();
        for (SourcePlan plan : plans) {
            CaptureRunSpec spec = plan.spec();
            if (spec.srsEnabled() && spec.readMode() != ReadMode.SNAPSHOT_ONLY) {
                selected.computeIfAbsent(MiningChainId.resolve(spec.config(), spec.srsKey()),
                        ignored -> new LinkedHashSet<>()).addAll(spec.config().streams());
            }
        }
        List<SourcePlan> complete = new ArrayList<>(plans.size());
        for (SourcePlan plan : plans) {
            CaptureRunSpec spec = plan.spec();
            LinkedHashSet<String> tables = spec.srsEnabled() && spec.readMode() != ReadMode.SNAPSHOT_ONLY
                    ? selected.get(MiningChainId.resolve(spec.config(), spec.srsKey())) : null;
            complete.add(tables == null ? plan : new SourcePlan(
                    plan.sourceId(), plan.discovered(), plan.resolution(),
                    spec.withChainSelection(List.copyOf(tables), cursorWriterToken), plan.captureId()));
        }
        return List.copyOf(complete);
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

    private record PipelineRun(CaptureId captureId, CaptureRun run, boolean managed) {

        static PipelineRun unmanaged(CaptureRun run) {
            return new PipelineRun(null, run, false);
        }

        static PipelineRun managed(CaptureId captureId, CaptureRun run) {
            return new PipelineRun(captureId, run, true);
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
            if (lost || Thread.currentThread().isInterrupted()) {
                throw new CancellationException("capture claim was lost during source start");
            }
        }

        private void publish(CaptureRun run, Set<String> pipelines,
                CaptureRunSpec spec, Consumer<Envelope> receive) {
            OwnedCapture owned = new OwnedCapture(run, permit, pipelines, spec, receive);
            owned.lease = lease;
            published = owned;
            ownedCaptures.put(captureId, owned);
            watchPhysicalRequests();
            openingPending = null;
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
        private final Consumer<Envelope> receive;
        private CaptureClaimLease lease;
        private volatile Thread reconfiguring;

        private OwnedCapture(CaptureRun run, CaptureOwnership.Permit permit, Collection<String> pipelines,
                CaptureRunSpec spec, Consumer<Envelope> receive) {
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
        private final Consumer<Envelope> tailPassthrough;
        private final Set<String> pipelines = new LinkedHashSet<>();

        private JoinedCapture(CaptureRunSpec joinedWith, Consumer<Envelope> passthrough) {
            // A snapshot-only read has no tail for anybody to take over.
            this.tails = joinedWith.readMode() != ReadMode.SNAPSHOT_ONLY;
            this.tailSpec = new CaptureRunSpec(
                    joinedWith.config(), ReadMode.CDC_ONLY, joinedWith.srsKey(), joinedWith.srsEnabled(),
                    joinedWith.sourceId(), joinedWith.pipelineId(), joinedWith.startFrom(),
                    joinedWith.retention(), joinedWith.schemaVer(), joinedWith.snapshotEpoch());
            this.tailPassthrough = passthrough;
        }
    }

    /** A remote attachment records its table demand in the chain document; only the owner replaces it. */
    void reconfigureRequestedCaptures() {
        for (CaptureId captureId : List.copyOf(ownedCaptures.keySet())) {
            KeyedLocks.Hold<CaptureId> hold = captureLocks.tryAcquire(captureId);
            if (hold == null) {
                continue;
            }
            try (hold) {
                OwnedCapture owned = ownedCaptures.get(captureId);
                if (owned == null || !owned.spec.srsEnabled()
                        || owned.spec.readMode() == ReadMode.SNAPSHOT_ONLY) {
                    continue;
                }
                MiningChainId chain = owned.run.chainId().orElse(null);
                if (chain == null) {
                    continue;
                }
                List<String> requested = storePort.meta().requestedPhysicalTables(chain.value());
                if (requested.isEmpty()) {
                    continue;
                }
                var selection = storePort.meta().physicalSelection(chain.value()).orElse(null);
                if (selection == null) {
                    // A reserved snapshot has not opened its tail yet. Its first subscription reads
                    // these durable requests, so there is no running subscription to replace.
                    continue;
                }
                if (selection.tables().containsAll(requested)) {
                    storePort.meta().clearPhysicalRequests(chain.value(), selection.epoch(), requested);
                    continue;
                }
                reopenOwnedPhysicalTail(captureId, owned);
            }
        }
    }

    private synchronized void watchPhysicalRequests() {
        if (physicalReconfigurations != null || claimRenewInterval.isZero()) {
            return;
        }
        physicalReconfigurations = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "tapstate-capture-selection");
            thread.setDaemon(true);
            return thread;
        });
        physicalReconfigurations.scheduleWithFixedDelay(() -> {
            try {
                reconfigureRequestedCaptures();
            } catch (RuntimeException failure) {
                LOG.warn("Could not expand a requested physical capture subscription", failure);
            }
        }, 1000, 1000, TimeUnit.MILLISECONDS);
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

    private synchronized void lookForCapturesNobodyTails() {
        if (takeovers != null || claimRenewInterval.isZero()) {
            return;
        }
        takeovers = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "tapstate-capture-takeover");
            thread.setDaemon(true);
            return thread;
        });
        long every = claimRenewInterval.toMillis();
        takeovers.scheduleWithFixedDelay(() -> {
            try {
                tailWhatNobodyTails();
            } catch (RuntimeException failure) {
                LOG.warn("Looking for captures nobody tails failed; asking again later", failure);
            }
        }, every, every, TimeUnit.MILLISECONDS);
    }

    /** Stops looking for captures to take over. What this member already tails is its stops' to close. */
    public synchronized void close() {
        if (takeovers != null) {
            takeovers.shutdownNow();
            takeovers = null;
        }
        if (physicalReconfigurations != null) {
            physicalReconfigurations.shutdownNow();
            physicalReconfigurations = null;
        }
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
                || aRingIsOpen(MiningChainId.resolve(spec.config(), spec.srsKey()).value())) {
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
            ownedCaptures.remove(captureId);
            expected.run.close();
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
    private record SnapshotOnChain(Optional<String> chainId, List<String> tables) {
    }

    /**
     * What this run contributes to the delivered question, or empty when it contributes nothing.
     *
     * <p>A run whose read mode has no snapshot has no load to deliver. A run with no chain is a
     * snapshot-only read: it opens no tail, so nothing seeds a record and no completion is ever written
     * for it. Keep its tables with an absent chain so they remain owed: without delivery evidence, a
     * resume must re-read the load rather than restart a vertex over an empty snapshot hand-off.
     */
    private static Optional<SnapshotOnChain> snapshotOnChain(CaptureRunSpec spec, CaptureRun run) {
        if (!CapturePlan.forReadMode(spec.readMode()).snapshot()) {
            return Optional.empty();
        }
        return Optional.of(new SnapshotOnChain(run.chainId().map(MiningChainId::value), spec.config().streams()));
    }

    /** One source's attributed snapshot load: which source, which table, and what it loaded. */
    private record AttributedSnapshot(String sourceId, String table, TableSnapshot snapshot) {
    }

    private record SnapshotTableKey(String sourceId, String table) {
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
     * Records what each selected stream's snapshot loaded, so {@link #startCapture} can key the pipeline's
     * published snapshot map once every source has run. A cdc-only run has no entries because it never ran a
     * bounded snapshot phase.
     */
    private static void recordSnapshot(
            List<AttributedSnapshot> attributed,
            String sourceId,
            CaptureRunSpec spec,
            CaptureRun run,
            Map<String, Long> observedSnapshotCounts,
            SourceModel discovered) {
        List<String> streams = spec.config().streams();
        if (!CapturePlan.forReadMode(spec.readMode()).snapshot()) {
            return;
        }
        for (String table : streams) {
            Map<String, Long> counts = run.snapshotCounts().isEmpty() ? observedSnapshotCounts : run.snapshotCounts();
            long count = streams.size() == 1
                    ? counts.getOrDefault(table, run.snapshotCount())
                    : counts.getOrDefault(table, 0L);
            Long total = estimatedRows(discovered, table);
            attributed.add(new AttributedSnapshot(sourceId, table, new TableSnapshot(count, total, share(count, total))));
        }
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
     * <p>A share past a hundred is reported as a hundred. The load is finished by the time this is asked,
     * so overshooting a stale estimate means complete, and a progress figure above full is not a state
     * anything can be in -- publishing one would leave every reader to decide for themselves what it meant.
     */
    private static Integer share(long loaded, Long total) {
        if (total == null || total <= 0L) {
            return null;
        }
        return (int) Math.min(100L, loaded * 100L / total);
    }

    /**
     * What {@code attributed} amounts to for the whole pipeline: the per-table loads keyed for the read
     * face, counted from {@code loadBegan}.
     *
     * <p>A pipeline that ran no bounded load reports nothing at all rather than a start with no rows. The
     * two are not the same claim: one says no load ran, and the other would say a load ran and read
     * nothing, which is a real and different state that a table entry at zero rows already expresses.
     */
    private static SnapshotReading reading(List<AttributedSnapshot> attributed, Instant loadBegan) {
        if (attributed.isEmpty()) {
            return SnapshotReading.NONE;
        }
        return new SnapshotReading(keyByTableOrQualifyOnCollision(attributed), loadBegan);
    }

    /**
     * Keys each attributed load by its bare table name, unless more than one source in this pipeline read a
     * table of that same name (a normal shape: the same table name in two different databases) — in that
     * case every entry for that name is instead qualified {@code source_id.table}, the same addressing form
     * `serve.from` and friends already use to disambiguate a table reference. A plain table-name key would
     * otherwise have the last source silently overwrite an earlier one's count and attribute it to the wrong
     * source; qualifying only the names that actually collide keeps the common single-source case unchanged.
     */
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

    @Override
    public SnapshotReading snapshotProgress(String pipelineId) {
        LiveSnapshot live = liveSnapshotsByPipeline.get(pipelineId);
        SnapshotReading settled = snapshotsByPipeline.getOrDefault(pipelineId, SnapshotReading.NONE);
        if (live == null) {
            return settled;
        }
        SnapshotReading moving = live.reading();
        Map<String, TableSnapshot> combined = new LinkedHashMap<>(settled.byTable());
        moving.byTable().forEach((table, current) -> combined.merge(table, current, (previous, now) -> {
            long rows = Math.max(previous.rowsDone(), now.rowsDone());
            Long total = previous.rowsTotal() != null ? previous.rowsTotal() : now.rowsTotal();
            return new TableSnapshot(rows, total, share(rows, total));
        }));
        return new SnapshotReading(combined, live.began);
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
            if (!SnapshotPhase.stillOwed(snapshot.chainId().flatMap(storePort.meta()::read), pipelineId,
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
        snapshotsByPipeline.remove(pipelineId);
        liveSnapshotsByPipeline.remove(pipelineId);
        snapshotTablesByPipeline.remove(pipelineId);
        // Holding no runs is not the same as having nothing to release. A pipeline whose start threw part
        // way, and one whose process was replaced, both arrive here with no handles and a record that is
        // still all there -- and a stop asked to clear the state has that record to clear. Returning on the
        // absent handle is what made the verb report success and take nothing, in the one state a caller
        // reaches for it most: after a run has died.
        List<PipelineRun> runs = Objects.requireNonNullElse(runsByPipeline.remove(pipelineId), List.of());
        List<KeyedLocks.Hold<CaptureId>> captureHolds = lockCaptures(runs.stream()
                .filter(PipelineRun::managed).map(PipelineRun::captureId).toList(), false);
        try {
            RuntimeException cleanupFailure = closeRuns(runs, pipelineId, purgeState);
            if (cleanupFailure != null) {
                throw cleanupFailure;
            }
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
        RuntimeException firstFailure = null;
        boolean capturesStopped = true;
        Set<MiningChainId> chains = new LinkedHashSet<>();
        for (PipelineRun pipelineRun : runs) {
            CaptureRun run = pipelineRun.run;
            try {
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
            run.chainId().ifPresent(chains::add);
        }
        // A live drain cannot detach an empty queue: capture may already hold that queue and append into it
        // after the drain returns. Lifecycle teardown has no such race once every capture close returned, so
        // this is where all of the pipeline's queues and their coordinate strings are released. Keep them when
        // a close failed because that capture may still be appending.
        if (capturesStopped) {
            snapshotBuffer.release(pipelineId);
        }
        // Once per chain, never once per run. Two sources reading one connection are one chain with a ring
        // per table, which is what a pipeline over a parent and a child table is; releasing it per run would
        // have the second source release a chain the first already closed, and the release refuses that.
        for (MiningChainId chainId : chains) {
            // Whether this pipeline was the last one on the chain, which decides how much of the chain's
            // record is this stop's to take. Read from the release itself rather than asked again after
            // it: a consumer attaching in between would make a second reading stale, and the two answers
            // would then disagree about a record one of them is about to delete.
            boolean chainClosed = false;
            try {
                chainClosed = srsCoordinator.releaseConsumer(chainId, pipelineId);
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
                // is what asking for the state to be cleared meant.
                firstFailure = runCleanup(() -> storePort.meta().dropChain(chainId.value()), firstFailure);
            } else {
                // Others are still reading it, so only this pipeline's own cursor is its to give back.
                // Run whether or not the release above succeeded, and safe to run twice: the detach
                // states the end condition "this consumer holds nothing here", which an absent chain and
                // an absent cursor already satisfy. Skipping it after one failure is what leaves a cursor
                // nobody will ever advance holding back every pipeline still on the chain.
                firstFailure = runCleanup(
                        () -> storePort.meta().detachConsumer(chainId.value(), pipelineId), firstFailure);
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
                firstFailure = purgeWhatTheRecordStillHolds(chainId, pipelineId, firstFailure);
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
                .allMatch(offset -> offset.pipelineId().equals(pipelineId));
    }

    private void closeRun(PipelineRun pipelineRun, String pipelineId) {
        if (!pipelineRun.managed) {
            pipelineRun.run.close();
            return;
        }
        OwnedCapture owned = ownedCaptures.get(pipelineRun.captureId);
        if (owned == null) {
            forgetJoined(pipelineRun.captureId, pipelineId);
            pipelineRun.run.close();
            return;
        }
        owned.pipelines.remove(pipelineId);
        if (!owned.pipelines.isEmpty()) {
            if (pipelineRun.run != owned.run) {
                pipelineRun.run.close();
            }
            return;
        }
        ownedCaptures.remove(pipelineRun.captureId);
        try {
            owned.run.close();
        } finally {
            if (pipelineRun.run != owned.run) {
                pipelineRun.run.close();
            }
            owned.lease.close();
        }
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
            String chainId, String pipelineId, RuntimeException firstFailure) {
        boolean lastOneOff = storePort.meta().consumerOffsets(chainId).stream()
                        .allMatch(offset -> offset.pipelineId().equals(pipelineId))
                && !srsCoordinator.isProvisioned(new MiningChainId(chainId));
        return runCleanup(
                () -> {
                    if (lastOneOff) {
                        storePort.meta().dropChain(chainId);
                    } else {
                        storePort.meta().detachConsumer(chainId, pipelineId);
                    }
                },
                firstFailure);
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
                snapshotEpoch);
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
        // The pipeline's cdc capture has failed if any of its source runs' tails died; surface the first, so a
        // dead tail becomes a failure the converge loop drives to the observable FAILED state rather than an
        // engine job that stays running over a ring gone quiet.
        return runs.stream()
                .map(run -> {
                    OwnedCapture owned = run.managed ? ownedCaptures.get(run.captureId) : null;
                    return owned == null ? run.run.failure() : owned.run.failure();
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
        return isActive(pipelineId);
    }

    @Override
    public void activateSnapshot(String pipelineId) {
        List<PipelineRun> runs = runsByPipeline.get(pipelineId);
        if (runs != null) {
            runs.forEach(pipelineRun -> pipelineRun.run.activateSnapshot());
        }
    }

    private ArtifactStore artifacts() {
        return storePort.artifacts();
    }

    /**
     * The callback for one source: counts snapshot rows as they arrive and hands a direct tail's CDC events
     * to the legacy member-local buffer. A deferred snapshot has already inserted its row into the bounded
     * session under the same token; appending it here again would deliver it twice. A stale token is refused
     * so an old callback cannot feed a replacement run's buffer.
     */
    private Consumer<Envelope> snapshotPassthrough(
            String pipelineId, String sourceId, SourceCaptureResolution resolution, String token,
            LiveSnapshot liveSnapshot,
            Map<String, Long> observedSnapshotCounts) {
        Set<String> selectedTables = Set.copyOf(resolution.tables());
        return event -> {
            if (!selectedTables.contains(event.src())) {
                throw new TapstateException(
                        CaptureError.EVENT_TABLE_NOT_SELECTED, Map.of("table", event.src()), null);
            }
            String ringName = resolution.ringName(event.src());
            if (event.op() == Op.READ) {
                observedSnapshotCounts.merge(event.src(), 1L, Long::sum);
                if (snapshotBuffer.hasSnapshot(pipelineId, ringName)) {
                    if (!snapshotBuffer.hasSnapshot(pipelineId, ringName, token)) {
                        throw new CancellationException("snapshot row belongs to an obsolete capture run");
                    }
                    // The deferred capture already wrote this row into its bounded session.
                    if (liveSnapshot != null) {
                        liveSnapshot.received(sourceId, event.src());
                    }
                    return;
                }
                if (liveSnapshot != null) {
                    liveSnapshot.received(sourceId, event.src());
                }
            }
            snapshotBuffer.append(pipelineId, ringName, event);
        };
    }

}
