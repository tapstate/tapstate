package io.tapstate.app;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.ReadMode;
import io.tapstate.core.model.Settings;
import io.tapstate.core.model.SourceRef;
import io.tapstate.core.model.SourceResource;
import io.tapstate.runtime.srs.CaptureRun;
import io.tapstate.runtime.srs.CaptureError;
import io.tapstate.runtime.srs.CaptureRunSpec;
import io.tapstate.runtime.srs.MiningChainId;
import io.tapstate.runtime.srs.SnapshotBuffer;
import io.tapstate.runtime.srs.SnapshotPhase;
import io.tapstate.runtime.srs.SrsCoordinator;
import io.tapstate.runtime.srs.StartFrom;
import io.tapstate.spi.capture.CapturePlan;
import io.tapstate.spi.store.ArtifactStore;
import io.tapstate.spi.store.StorePort;
import io.tapstate.core.lifecycle.TableSnapshot;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * The store-backed capture coordinator: it resolves a pipeline and the sources it reads from the store,
 * starts one cdc capture run per source through the capture seam, and holds the live handles so a stop can
 * tear them down. It derives each source run spec identically to how the topology builder derives the ring
 * the run fills, through the shared source resolution, so the capture and the reader agree on the ring.
 *
 * <p>No positions are supplied here. A run's seam and its per-change positions are the source's own and are
 * learned from it as the read happens, so there is nothing for this layer to stand in with. Snapshot rows
 * drain to a shared buffer keyed by the source's change-ring name; the source vertex reading that ring
 * drains the buffer and emits its rows through the same transform-to-sink chain as cdc, strictly before it.
 */
final class StoreBackedPipelineCaptureCoordinator implements PipelineCaptureCoordinator {

    /** The schema version stamped on ring items at L1 (schema evolution is a later increment). */
    private static final long MOCK_SCHEMA_VER = 0L;

    private final StorePort storePort;
    private final CaptureStarter captureStarter;
    private final SrsCoordinator srsCoordinator;
    private final SnapshotBuffer snapshotBuffer;
    private final Map<String, List<CaptureRun>> runsByPipeline = new ConcurrentHashMap<>();

    /** What each running pipeline's tables loaded, keyed by pipeline then table; dropped when it stops. */
    private final Map<String, Map<String, TableSnapshot>> snapshotsByPipeline = new ConcurrentHashMap<>();

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
        this.srsCoordinator = Objects.requireNonNull(srsCoordinator, "srsCoordinator");
        this.snapshotBuffer = Objects.requireNonNull(snapshotBuffer, "snapshotBuffer");
    }

    @Override
    public void startCapture(String pipelineId) {
        // Idempotent: a pipeline whose capture is already running is left running, so a repeated start does not
        // open a second capture behind the one already filling the ring.
        if (runsByPipeline.containsKey(pipelineId)) {
            return;
        }
        PipelineResource pipeline = StoredArtifacts.requirePipeline(artifacts(), pipelineId);
        List<CaptureRun> runs = new ArrayList<>();
        List<AttributedSnapshot> attributed = new ArrayList<>();
        List<SnapshotOnChain> snapshotTables = new ArrayList<>();
        try {
            for (SourceRef ref : pipeline.sources()) {
                String sourceId = ref.id();
                SourceResource source = StoredArtifacts.requireSource(artifacts(), sourceId);
                SourceCaptureResolution resolution = SourceCaptureResolution.of(source, SourceDiscovery.model(storePort, source));
                CaptureRunSpec spec = deriveSpec(
                        pipelineId, pipeline.settings(), source, resolution, srsSwitchOf(pipelineId, ref));
                Map<String, Long> observedSnapshotCounts = new LinkedHashMap<>();
                CaptureRun run = captureStarter.start(spec, snapshotPassthrough(resolution, observedSnapshotCounts));
                runs.add(run);
                recordSnapshot(attributed, sourceId, spec, run, observedSnapshotCounts);
                snapshotOnChain(spec, run).ifPresent(snapshotTables::add);
            }
        } catch (RuntimeException | Error failure) {
            // A start that fell over releases what it took and nothing else. It is an abandoned attempt,
            // not somebody asking for the pipeline's position to be thrown away, and the sources that did
            // start may have advanced it before the one that failed.
            RuntimeException cleanupFailure = closeRuns(runs, pipelineId, false);
            if (cleanupFailure != null) {
                failure.addSuppressed(cleanupFailure);
            }
            throw failure;
        }
        runsByPipeline.put(pipelineId, runs);
        snapshotsByPipeline.put(pipelineId, keyByTableOrQualifyOnCollision(attributed));
        snapshotTablesByPipeline.put(pipelineId, List.copyOf(snapshotTables));
    }

    /** One source run's snapshot: the chain that records its completion, and the tables it covers. */
    private record SnapshotOnChain(String chainId, List<String> tables) {
    }

    /**
     * What this run contributes to the delivered question, or empty when it contributes nothing.
     *
     * <p>A run whose read mode has no snapshot has no load to deliver. A run with no chain is a
     * snapshot-only read: it opens no tail, so nothing seeds a record and no completion is ever written
     * for it. That one contributes nothing rather than counting as never delivered -- a question the
     * record cannot answer must not be answered by guessing, and guessing that way round would re-read
     * the whole source on every resume with no state that could ever end it.
     */
    private static Optional<SnapshotOnChain> snapshotOnChain(CaptureRunSpec spec, CaptureRun run) {
        if (!CapturePlan.forReadMode(spec.readMode()).snapshot()) {
            return Optional.empty();
        }
        return run.chainId().map(chain -> new SnapshotOnChain(chain.value(), spec.config().streams()));
    }

    /** One source's attributed snapshot load: which source, which table, and what it loaded. */
    private record AttributedSnapshot(String sourceId, String table, TableSnapshot snapshot) {
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
            Map<String, Long> observedSnapshotCounts) {
        List<String> streams = spec.config().streams();
        if (!CapturePlan.forReadMode(spec.readMode()).snapshot()) {
            return;
        }
        for (String table : streams) {
            Map<String, Long> counts = run.snapshotCounts().isEmpty() ? observedSnapshotCounts : run.snapshotCounts();
            long count = streams.size() == 1
                    ? counts.getOrDefault(table, run.snapshotCount())
                    : counts.getOrDefault(table, 0L);
            // The total is reported by no source today, so it stays null and the percentage with it -- progress
            // with no total is honest partial data, never a faked complete load.
            attributed.add(new AttributedSnapshot(sourceId, table, new TableSnapshot(count, null, null)));
        }
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
    public Map<String, TableSnapshot> snapshotProgress(String pipelineId) {
        return snapshotsByPipeline.getOrDefault(pipelineId, Map.of());
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
            if (!SnapshotPhase.stillOwed(storePort.meta().read(snapshot.chainId()), pipelineId,
                    snapshot.tables()).isEmpty()) {
                return false;
            }
        }
        return true;
    }

    @Override
    public void stopCapture(String pipelineId, boolean purgeState) {
        // The load belongs to the run being torn down: a stopped pipeline reports no snapshot rather than the
        // rows its previous run happened to load.
        snapshotsByPipeline.remove(pipelineId);
        snapshotTablesByPipeline.remove(pipelineId);
        List<CaptureRun> runs = runsByPipeline.remove(pipelineId);
        if (runs == null) {
            return;
        }
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
     * <p>Close first: stops every capture daemon so no thread leaks. Then give back this pipeline's hold on
     * each chain it read -- a shared-ring run only; a run that opened no chain has nothing to release. The
     * chain itself closes when the pipeline giving it back was the last one on it, which is the coordinator's
     * to decide: a stop tears nothing down, because a chain several pipelines read is not this one's to take
     * away. Stopping one used to remove it outright, and what that cost the others was measured -- their own
     * stop then threw, and a pipeline restarted afterwards read under a generation of its own.
     *
     * <p>Every step runs even when an earlier one throws, and the first failure carries the rest as
     * suppressed. A release abandoned half way is what leaves a chain nobody owns and a daemon nobody stops.
     */
    private RuntimeException closeRuns(List<CaptureRun> runs, String pipelineId, boolean purgeState) {
        RuntimeException firstFailure = null;
        Set<MiningChainId> chains = new LinkedHashSet<>();
        for (CaptureRun run : runs) {
            firstFailure = runCleanup(run::close, firstFailure);
            // Collected whether or not the close succeeded: a daemon that refused to stop does not make the
            // consumer membership this pipeline holds any less this pipeline's to give back.
            run.chainId().ifPresent(chains::add);
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
            if (chainClosed) {
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
        return firstFailure;
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
        ReadMode readMode = settings != null && settings.readMode() != null
                ? settings.readMode() : ReadMode.SNAPSHOT_AND_CDC;
        String startFromRaw = settings != null && settings.startFrom() != null
                ? settings.startFrom() : "latest";
        String retention = source.srs() != null ? source.srs().retention() : null;
        return new CaptureRunSpec(
                resolution.config(),
                readMode,
                resolution.srsKey(),
                srsEnabled,
                resolution.sourceId(),
                pipelineId,
                StartFrom.parse(startFromRaw),
                retention,
                MOCK_SCHEMA_VER);
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
        List<CaptureRun> runs = runsByPipeline.get(pipelineId);
        if (runs == null) {
            return Optional.empty();
        }
        // The pipeline's cdc capture has failed if any of its source runs' tails died; surface the first, so a
        // dead tail becomes a failure the converge loop drives to the observable FAILED state rather than an
        // engine job that stays running over a ring gone quiet.
        return runs.stream()
                .map(CaptureRun::failure)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .findFirst();
    }

    /** Whether this pipeline currently has a live capture -- a test-visible view of the retained handles. */
    boolean isActive(String pipelineId) {
        return runsByPipeline.containsKey(pipelineId);
    }

    private ArtifactStore artifacts() {
        return storePort.artifacts();
    }

    /**
     * The snapshot pass-through for one source: it appends each snapshot row to the shared buffer under the
     * source's change-ring name. The source vertex reading that ring drains the buffer and emits its rows ahead
     * of the cdc tail, so the snapshot flows through the same transform-to-sink chain as cdc, strictly before
     * it. A read mode that runs no snapshot never calls this, so the buffer for that ring stays empty and the
     * source is a pure tail.
     */
    private Consumer<Envelope> snapshotPassthrough(
            SourceCaptureResolution resolution, Map<String, Long> observedSnapshotCounts) {
        Set<String> selectedTables = Set.copyOf(resolution.tables());
        return event -> {
            if (!selectedTables.contains(event.src())) {
                throw new TapstateException(
                        CaptureError.EVENT_TABLE_NOT_SELECTED, Map.of("table", event.src()), null);
            }
            observedSnapshotCounts.merge(event.src(), 1L, Long::sum);
            snapshotBuffer.append(resolution.ringName(event.src()), event);
        };
    }

}
