package io.tapstate.app;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.Envelope;
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
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
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
    private final Map<CaptureId, OwnedCapture> ownedCaptures = new LinkedHashMap<>();

    /**
     * The pipelines here reading a capture another member holds, by capture. Kept for the moment this
     * member takes such a capture over, by a start of its own or because nobody tails it any more: its
     * tail then runs for these pipelines as well, and has to outlast the last of them rather than the
     * pipeline that happened to take the claim.
     */
    private final Map<CaptureId, JoinedCapture> joinedCaptures = new LinkedHashMap<>();

    /** The captures starts here were given back over, for as long as they are still not ready. */
    private final Map<CaptureId, RingWait> ringWaits = new LinkedHashMap<>();

    /** Looks for captures pipelines here read and nobody tails; started with the first capture joined. */
    private ScheduledExecutorService takeovers;

    /** How far each running pipeline's load has got, keyed by pipeline; dropped when it stops. */
    private final Map<String, PipelineLoad> loadsByPipeline = new ConcurrentHashMap<>();

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
    public synchronized void startCapture(String pipelineId) {
        startCapture(pipelineId, artifacts());
    }

    @Override
    public synchronized void startCapture(String pipelineId, ArtifactStore artifactSnapshot) {
        // Idempotent: a pipeline whose capture is already running is left running, so a repeated start does not
        // open a second capture behind the one already filling the ring.
        if (runsByPipeline.containsKey(pipelineId)) {
            return;
        }
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
        Map<CaptureId, CaptureOwnership.Permit> permits = new LinkedHashMap<>();
        List<SourcePlan> plans = plan(pipelineId, pipeline, captured, snapshotEpoch, permits);
        List<PipelineRun> runs = new ArrayList<>();
        List<SnapshotOnChain> snapshotTables = new ArrayList<>();
        // Taken before the first source is opened, because the load is what these totals accumulate from
        // and its first row is read inside the loop below. Stamping it afterwards would date the whole
        // load to the moment it ended, and a rate computed across the first two scrapes would divide by a
        // window that had already closed.
        Instant loadBegan = Instant.now();
        PipelineLoad load = PipelineLoad.of(plans, loadBegan);
        try {
            for (SourcePlan plan : plans) {
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
                CaptureHandoff handoff = snapshotPassthrough(pipelineId, plan, observedSnapshotCounts, load);
                CaptureRun run;
                if (!managedOwnership) {
                    run = captureStarter.start(spec, handoff);
                    runs.add(PipelineRun.unmanaged(run));
                } else {
                    OwnedCapture existing = ownedCaptures.get(captureId);
                    if (existing != null) {
                        run = captureAttacher.start(spec.withCaptureFence(existing.permit.fence()), handoff, false);
                        existing.pipelines.add(pipelineId);
                    } else {
                        CaptureOwnership.Permit permit = permits.get(captureId);
                        if (permit.acquired()) {
                            // From here the tail, or the release below, answers for this claim.
                            permits.remove(captureId);
                            try {
                                run = captureAttacher.start(spec.withCaptureFence(permit.fence()), handoff, true);
                            } catch (RuntimeException | Error failure) {
                                ownership.release(permit.claim());
                                throw failure;
                            }
                            // Pipelines here that joined this capture while another member held it read the
                            // tail this member now runs, so it runs for as long as any of them does.
                            Set<String> pipelines = new LinkedHashSet<>(List.of(pipelineId));
                            JoinedCapture joined = joinedCaptures.remove(captureId);
                            if (joined != null) {
                                pipelines.addAll(joined.pipelines);
                            }
                            own(captureId, run, permit, pipelines);
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
                    runs.add(PipelineRun.managed(captureId, run));
                }
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
        } catch (RuntimeException | Error failure) {
            // A start that fell over releases what it took and nothing else. It is an abandoned attempt,
            // not somebody asking for the pipeline's position to be thrown away, and the sources that did
            // start may have advanced it before the one that failed.
            RuntimeException cleanupFailure = closeRuns(runs, pipelineId, false);
            if (cleanupFailure != null) {
                failure.addSuppressed(cleanupFailure);
            }
            releaseUnopened(permits, failure);
            throw failure;
        }
        runsByPipeline.put(pipelineId, runs);
        loadsByPipeline.put(pipelineId, load);
        snapshotTablesByPipeline.put(pipelineId, List.copyOf(snapshotTables));
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
                    .map(record -> record.snapshotCompletedTables(spec.pipelineId()).contains(table))
                    .orElse(false);
            if (!completed) {
                SnapshotLoadCounts.save(storePort.keyedState(), spec.pipelineId(), chain, table, count);
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
     * Works out every source this start reads and settles the claim on each capture it does not already
     * tail here, before any of them is opened -- into {@code permits}, one per capture however many sources
     * share it. A capture held elsewhere whose ring is not open yet gives the whole start back, with the
     * claims taken so far let go.
     */
    private List<SourcePlan> plan(
            String pipelineId,
            PipelineResource pipeline,
            ArtifactStore captured,
            long snapshotEpoch,
            Map<CaptureId, CaptureOwnership.Permit> permits) {
        List<SourcePlan> plans = new ArrayList<>();
        try {
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
                if (managedOwnership && !ownedCaptures.containsKey(captureId) && !permits.containsKey(captureId)) {
                    permits.put(captureId, permitOrNotYet(pipelineId, captureId, spec));
                }
                plans.add(new SourcePlan(sourceId, discovered, resolution, spec, captureId));
            }
        } catch (RuntimeException | Error failure) {
            releaseUnopened(permits, failure);
            throw failure;
        }
        return plans;
    }

    /**
     * Lets go of the claims a start took for captures it never opened. One that cannot be let go of runs
     * out by its lease; saying so on the failure the start ends with is all that is left to do.
     */
    private void releaseUnopened(Map<CaptureId, CaptureOwnership.Permit> permits, Throwable failure) {
        for (CaptureOwnership.Permit permit : permits.values()) {
            if (!permit.acquired()) {
                continue;
            }
            try {
                ownership.release(permit.claim());
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

    private static final class OwnedCapture {
        private final CaptureRun run;
        private final CaptureOwnership.Permit permit;
        private final Set<String> pipelines = new LinkedHashSet<>();
        private CaptureClaimLease lease;

        private OwnedCapture(CaptureRun run, CaptureOwnership.Permit permit, Collection<String> pipelines) {
            this.run = run;
            this.permit = permit;
            this.pipelines.addAll(pipelines);
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
                    joinedWith.retention(), joinedWith.schemaVer(), joinedWith.snapshotEpoch());
            this.tailPassthrough = passthrough;
        }
    }

    private void own(CaptureId captureId, CaptureRun run, CaptureOwnership.Permit permit, Set<String> pipelines) {
        OwnedCapture owned = new OwnedCapture(run, permit, pipelines);
        ownedCaptures.put(captureId, owned);
        owned.lease = permit.claim() == null
                ? CaptureClaimLease.unfenced()
                : new CaptureClaimLease(
                        ownership, permit.claim(), claimRenewInterval,
                        () -> captureClaimLost(captureId, owned));
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
    synchronized void tailWhatNobodyTails() {
        Iterator<Map.Entry<CaptureId, JoinedCapture>> joined = joinedCaptures.entrySet().iterator();
        while (joined.hasNext()) {
            Map.Entry<CaptureId, JoinedCapture> entry = joined.next();
            JoinedCapture capture = entry.getValue();
            if (!capture.tails) {
                continue;
            }
            // Not while a pipeline here is still reading its own load of this capture: the tail would hand its
            // changes over while that load is still arriving -- through the very hand-off the load arrives by,
            // for a tail with no ring -- and a change ahead of a snapshot row of the same key is overwritten by
            // the older value. The tail resumes from where the durable record says the last one got to, so
            // asking again on a later pass costs a delay and nothing more.
            if (stillLoading(capture.pipelines)) {
                continue;
            }
            CaptureOwnership.Permit permit = ownership.acquire(entry.getKey());
            if (!permit.acquired()) {
                continue;
            }
            CaptureRun tail;
            try {
                tail = captureAttacher.start(
                        capture.tailSpec.withCaptureFence(permit.fence()), capture.tailPassthrough, true);
            } catch (RuntimeException failure) {
                ownership.release(permit.claim());
                LOG.warn("Could not open the tail of capture {} for pipelines {} here; asking again later",
                        entry.getKey().value(), capture.pipelines, failure);
                continue;
            }
            joined.remove();
            own(entry.getKey(), tail, permit, capture.pipelines);
            LOG.info("Took over the tail of capture {} for pipelines {} here", entry.getKey().value(),
                    capture.pipelines);
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

    private void lookForCapturesNobodyTails() {
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
     * <p>Given back rather than waited for here, because the start runs on the one thread that converges
     * every pipeline on this member, holding the lock its claim-loss handling, stops and takeovers take: a
     * wait here stalled all of them for as long as it lasted. How long a capture has been found this way is
     * kept, and once it is longer than a lease -- the longest a claim nobody renews can stay held -- the
     * start is refused with a code instead, rather than retried over nothing for ever. A wait nobody has
     * looked at for that long is one a stop abandoned, and starts afresh.
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

    private synchronized void captureClaimLost(CaptureId captureId, OwnedCapture expected) {
        if (ownedCaptures.get(captureId) != expected) {
            return;
        }
        ownedCaptures.remove(captureId);
        TapstateException fenced = new TapstateException(
                CaptureError.CLAIM_LOST, Map.of("captureId", captureId.value()), null);
        expected.run.health().fail(fenced);
        expected.run.close();
        runsByPipeline.values().stream()
                .flatMap(List::stream)
                .filter(run -> run.managed && captureId.equals(run.captureId))
                .forEach(run -> run.run.health().fail(fenced));
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
    private record SnapshotOnChain(String sourceId, Optional<String> chainId, List<String> tables) {
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
                sourceId, run.chainId().map(MiningChainId::value), spec.config().streams()));
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
        // fallback for an older completed record without one. The mark is also what says the load landed,
        // so a table read through reads as landed from the mark on, and never before it.
        SnapshotReading current = load.reading();
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
                    .map(record -> record.snapshotCompletedTables(pipelineId)).orElse(List.of());
            for (String table : source.tables()) {
                if (!confirmed.contains(table)) {
                    continue;
                }
                String key = load.keyOf(source.sourceId(), table);
                if (key == null) {
                    continue;
                }
                TableSnapshot reading = completed.get(key);
                OptionalLong saved = SnapshotLoadCounts.read(storePort.keyedState(), pipelineId,
                        source.chainId().orElseThrow(), table);
                // Boxed in every arm: one unboxed arm types the whole choice as a primitive, and a load nothing
                // counted anywhere would then throw here instead of reading as uncounted.
                Long rows = saved.isPresent() ? Long.valueOf(saved.getAsLong())
                        : reading != null && reading.rowsDone() > 0L ? Long.valueOf(reading.rowsDone())
                        : reading != null && reading.rowsTotal() != null ? reading.rowsTotal()
                        : load.estimatedRows(source.sourceId(), table);
                if (rows != null) {
                    completed.put(key, new TableSnapshot(rows, rows, 100, true));
                } else if (reading != null) {
                    // Nothing counted the table, but this run read it through: its reading stands, now landed.
                    completed.put(key, new TableSnapshot(
                            reading.rowsDone(), reading.rowsTotal(), reading.donePct(), true));
                } else {
                    continue;
                }
                changed = true;
            }
        }
        return changed ? new SnapshotReading(completed, load.began) : current;
    }

    @Override
    public SnapshotReading runSnapshotProgress(String pipelineId) {
        PipelineLoad load = loadsByPipeline.get(pipelineId);
        return load == null ? SnapshotReading.NONE : load.reading();
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
    public synchronized void stopCapture(String pipelineId, boolean purgeState) {
        // The load belongs to the run being torn down: a stopped pipeline reports no snapshot rather than the
        // rows its previous run happened to load.
        loadsByPipeline.remove(pipelineId);
        snapshotTablesByPipeline.remove(pipelineId);
        // Holding no runs is not the same as having nothing to release. A pipeline whose start threw part
        // way, and one whose process was replaced, both arrive here with no handles and a record that is
        // still all there -- and a stop asked to clear the state has that record to clear. Returning on the
        // absent handle is what made the verb report success and take nothing, in the one state a caller
        // reaches for it most: after a run has died.
        List<PipelineRun> runs = Objects.requireNonNullElse(runsByPipeline.remove(pipelineId), List.of());
        RuntimeException cleanupFailure = closeRuns(runs, pipelineId, purgeState);
        if (cleanupFailure != null) {
            throw cleanupFailure;
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
            firstFailure = runCleanup(
                    () -> storePort.keyedState().dropNamespace(SnapshotLoadCounts.namespaceOf(pipelineId)),
                    firstFailure);
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
    public synchronized Optional<Throwable> captureFailure(String pipelineId) {
        List<PipelineRun> runs = runsByPipeline.get(pipelineId);
        if (runs == null) {
            return Optional.empty();
        }
        // The pipeline's cdc capture has failed if any of its source runs' tails died; surface the first, so a
        // dead tail becomes a failure the converge loop drives to the observable FAILED state rather than an
        // engine job that stays running over a ring gone quiet.
        return runs.stream()
                .map(run -> run.managed && ownedCaptures.containsKey(run.captureId)
                        ? ownedCaptures.get(run.captureId).run.failure()
                        : run.run.failure())
                .filter(Optional::isPresent)
                .map(Optional::get)
                .findFirst();
    }

    /** Whether this pipeline currently has a live capture -- a test-visible view of the retained handles. */
    synchronized boolean isActive(String pipelineId) {
        return runsByPipeline.containsKey(pipelineId);
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
            String pipelineId, SourcePlan plan, Map<String, Long> observedSnapshotCounts, PipelineLoad load) {
        SourceCaptureResolution resolution = plan.resolution();
        Set<String> selectedTables = Set.copyOf(resolution.tables());
        CaptureRunSpec spec = plan.spec();
        CapturePlan phases = CapturePlan.forReadMode(spec.readMode());
        Optional<String> chainId = phases.snapshot() && phases.cdc()
                ? Optional.of(MiningChainId.resolve(spec.config(), spec.srsKey()).value()) : Optional.empty();
        return new CaptureHandoff() {
            @Override
            public void accept(Envelope event) {
                if (!selectedTables.contains(event.src())) {
                    throw new TapstateException(
                            CaptureError.EVENT_TABLE_NOT_SELECTED, Map.of("table", event.src()), null);
                }
                snapshotBuffer.append(pipelineId, resolution.ringName(event.src()), event);
                observedSnapshotCounts.merge(event.src(), 1L, Long::sum);
            }

            @Override
            public void loaded(String table) {
                saveLoadCountIfOwed(spec, chainId, table, observedSnapshotCounts.getOrDefault(table, 0L));
                snapshotBuffer.endSnapshot(pipelineId, resolution.ringName(table));
                load.loaded(plan.sourceId(), table, observedSnapshotCounts.getOrDefault(table, 0L),
                        estimatedRows(plan.discovered(), table));
            }
        };
    }

}
