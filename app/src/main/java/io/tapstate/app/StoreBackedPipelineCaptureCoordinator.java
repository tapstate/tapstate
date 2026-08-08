package io.tapstate.app;

import io.tapstate.core.event.Envelope;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.ReadMode;
import io.tapstate.core.model.Settings;
import io.tapstate.core.model.SourceResource;
import io.tapstate.runtime.srs.CaptureRun;
import io.tapstate.runtime.srs.CaptureRunSpec;
import io.tapstate.runtime.srs.SnapshotBuffer;
import io.tapstate.runtime.srs.SrsCoordinator;
import io.tapstate.runtime.srs.StartFrom;
import io.tapstate.spi.capture.CapturePlan;
import io.tapstate.spi.capture.SourcePosition;
import io.tapstate.spi.store.ArtifactStore;
import io.tapstate.spi.store.StorePort;
import io.tapstate.spi.store.DiscoveredSourceModel;
import io.tapstate.spi.store.SourceModel;
import io.tapstate.core.lifecycle.TableSnapshot;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * The store-backed capture coordinator: it resolves a pipeline and the sources it reads from the store,
 * starts one cdc capture run per source through the capture seam, and holds the live handles so a stop can
 * tear them down. It derives each source run spec identically to how the topology builder derives the ring
 * the run fills, through the shared source resolution, so the capture and the reader agree on the ring.
 *
 * <p>The run spec carries L1 mock collaborators standing in for real connector machinery: a fixed cdc-start
 * token, a monotonic watermark generator, and a position order that ranks those watermark tokens by numeric
 * suffix (never lexically). The watermark token format and the position order are a matched pair. Snapshot
 * rows drain to a shared buffer keyed by the source's change-ring name; the source vertex reading that ring
 * drains the buffer and emits its rows through the same transform-to-sink chain as cdc, strictly before it.
 */
final class StoreBackedPipelineCaptureCoordinator implements PipelineCaptureCoordinator {

    /** The fixed cdc-start position for an L1 run: a mock stand-in for the position sampled at snapshot start. */
    private static final SourcePosition MOCK_CDC_START = new SourcePosition("cdc-start-0");

    /** The schema version stamped on ring items at L1 (schema evolution is a later increment). */
    private static final long MOCK_SCHEMA_VER = 0L;

    private final StorePort storePort;
    private final CaptureStarter captureStarter;
    private final SrsCoordinator srsCoordinator;
    private final SnapshotBuffer snapshotBuffer;
    private final Map<String, List<CaptureRun>> runsByPipeline = new ConcurrentHashMap<>();

    /** What each running pipeline's tables loaded, keyed by pipeline then table; dropped when it stops. */
    private final Map<String, Map<String, TableSnapshot>> snapshotsByPipeline = new ConcurrentHashMap<>();

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
        try {
            for (String sourceId : pipeline.sources()) {
                SourceResource source = StoredArtifacts.requireSource(artifacts(), sourceId);
                SourceCaptureResolution resolution = SourceCaptureResolution.of(source, discoveredModel(sourceId));
                CaptureRunSpec spec = deriveSpec(pipelineId, pipeline.settings(), source, resolution);
                Map<String, Long> observedSnapshotCounts = new LinkedHashMap<>();
                CaptureRun run = captureStarter.start(spec, snapshotPassthrough(resolution, observedSnapshotCounts));
                runs.add(run);
                recordSnapshot(attributed, sourceId, spec, run, observedSnapshotCounts);
            }
        } catch (RuntimeException | Error failure) {
            closeRuns(runs, pipelineId);
            throw failure;
        }
        runsByPipeline.put(pipelineId, runs);
        snapshotsByPipeline.put(pipelineId, keyByTableOrQualifyOnCollision(attributed));
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

    @Override
    public void stopCapture(String pipelineId) {
        // The load belongs to the run being torn down: a stopped pipeline reports no snapshot rather than the
        // rows its previous run happened to load.
        snapshotsByPipeline.remove(pipelineId);
        List<CaptureRun> runs = runsByPipeline.remove(pipelineId);
        if (runs == null) {
            return;
        }
        closeRuns(runs, pipelineId);
    }

    /** Releases live runs after a stop, or after a later source prevents a multi-source start from completing. */
    private void closeRuns(List<CaptureRun> runs, String pipelineId) {
        for (CaptureRun run : runs) {
            // Close first: stops the capture daemon so no thread leaks. Then release this pipeline's consumer
            // membership and tear the source chain down -- a shared-ring run only; a run that opened no chain
            // has nothing to release.
            run.close();
            run.chainId().ifPresent(chainId -> {
                srsCoordinator.detachConsumer(chainId, pipelineId);
                srsCoordinator.teardownSource(chainId);
            });
        }
    }

    /**
     * Derives one source run spec from the source, the pipeline settings, and the shared resolution. The read
     * axis comes from settings (read mode defaulting to snapshot-then-cdc, start position to earliest); srs is
     * on unless the source declares it off; the L1 mock collaborators are fresh per run.
     */
    static CaptureRunSpec deriveSpec(
            String pipelineId, Settings settings, SourceResource source, SourceCaptureResolution resolution) {
        ReadMode readMode = settings != null && settings.readMode() != null
                ? settings.readMode() : ReadMode.SNAPSHOT_AND_CDC;
        String startFromRaw = settings != null && settings.startFrom() != null
                ? settings.startFrom() : "earliest";
        String retention = source.srs() != null ? source.srs().retention() : null;
        return new CaptureRunSpec(
                resolution.config(),
                readMode,
                resolution.srsKey(),
                srsEnabled(source),
                resolution.sourceId(),
                pipelineId,
                StartFrom.parse(startFromRaw),
                MOCK_CDC_START,
                retention,
                MOCK_SCHEMA_VER,
                monotonicWatermark(),
                MockPositionOrder.INSTANCE);
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

    /** SRS is on unless the source declares an srs block that sets {@code enabled:false}. */
    private static boolean srsEnabled(SourceResource source) {
        if (source.srs() == null) {
            return true;
        }
        Boolean enabled = source.srs().enabled();
        return enabled == null || enabled;
    }

    /** A mock cdc watermark: a monotonic source-position generator (w1, w2, ...) standing in for the connector. */
    private static Supplier<SourcePosition> monotonicWatermark() {
        AtomicLong counter = new AtomicLong();
        return () -> new SourcePosition("w" + counter.incrementAndGet());
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
        return event -> {
            observedSnapshotCounts.merge(event.src(), 1L, Long::sum);
            snapshotBuffer.append(resolution.ringName(event.src()), event);
        };
    }

    private SourceModel discoveredModel(String sourceId) {
        return storePort.schemas().get(sourceId).map(DiscoveredSourceModel::model).orElse(null);
    }
}
