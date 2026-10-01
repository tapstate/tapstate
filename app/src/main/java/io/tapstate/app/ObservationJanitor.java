package io.tapstate.app;

import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.ArtifactStore;
import io.tapstate.spi.store.ExecutionGenerationStore;
import io.tapstate.spi.store.ObservationStore;
import io.tapstate.spi.store.ObservationStore.LatestSnapshot;
import io.tapstate.spi.store.StateStore;
import io.tapstate.spi.store.StopReservation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/** Cold-path, fixed-batch reclamation of latest documents no longer owned by their artifact and run. */
final class ObservationJanitor implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(ObservationJanitor.class);

    record Health(long scanned, long deleted, long failures, long maxDurationMillis,
            OptionalLong lastSuccessAgeMillis, boolean degraded) {
    }

    private final ObservationStore observations;
    private final ArtifactStore artifacts;
    private final ExecutionGenerationStore generations;
    private final StateStore states;
    private final String clusterId;
    private final int batchSize;
    private final ScheduledExecutorService worker;
    private final AtomicLong scanned = new AtomicLong();
    private final AtomicLong deleted = new AtomicLong();
    private final AtomicLong failures = new AtomicLong();
    private final AtomicLong maxDurationNanos = new AtomicLong();
    private volatile long lastSuccessNanos;
    private volatile int failedPhases;
    private Optional<String> after = Optional.empty();
    private Optional<String> manifestAfter = Optional.empty();
    private Phase phase = Phase.LEGACY;

    private enum Phase { LEGACY, MANIFEST, CHUNKS }

    ObservationJanitor(ObservationStore observations, ArtifactStore artifacts,
            ExecutionGenerationStore generations, String clusterId, int batchSize,
            Duration interval) {
        this(observations, artifacts, generations, clusterId, batchSize, interval, true);
    }

    ObservationJanitor(ObservationStore observations, ArtifactStore artifacts,
            ExecutionGenerationStore generations, String clusterId, int batchSize,
            Duration interval, boolean schedule) {
        this(observations, artifacts, generations, null, clusterId, batchSize, interval, schedule);
    }

    ObservationJanitor(ObservationStore observations, ArtifactStore artifacts,
            ExecutionGenerationStore generations, StateStore states, String clusterId, int batchSize,
            Duration interval, boolean schedule) {
        this.observations = Objects.requireNonNull(observations, "observations");
        this.artifacts = Objects.requireNonNull(artifacts, "artifacts");
        this.generations = Objects.requireNonNull(generations, "generations");
        this.states = states;
        this.clusterId = Objects.requireNonNull(clusterId, "clusterId");
        Objects.requireNonNull(interval, "interval");
        long intervalMillis;
        try {
            intervalMillis = interval.toMillis();
        } catch (ArithmeticException outOfRange) {
            throw new TapstateException(BootError.OBSERVABILITY_JANITOR_CONFIG_INVALID,
                    Map.of(), outOfRange);
        }
        if (clusterId.isBlank() || batchSize < 1 || batchSize > ObservationStore.MAX_LATEST_SCAN_BATCH
                || intervalMillis < 1) {
            throw new TapstateException(BootError.OBSERVABILITY_JANITOR_CONFIG_INVALID, Map.of(), null);
        }
        this.batchSize = batchSize;
        if (schedule) {
            worker = Executors.newSingleThreadScheduledExecutor(task -> {
                Thread thread = new Thread(task, "tapstate-observation-janitor");
                thread.setDaemon(true);
                return thread;
            });
            worker.scheduleWithFixedDelay(this::runOneBatch, intervalMillis,
                    intervalMillis, TimeUnit.MILLISECONDS);
        } else {
            worker = null;
        }
    }

    /** One cold scan batch; a failed owner lookup keeps its cursor so the next run retries that id. */
    synchronized void runOneBatch() {
        long started = System.nanoTime();
        Phase selected = Phase.LEGACY;
        try {
            if (observations.supportsManifestStorage()) {
                selected = phase;
                phase = switch (phase) {
                    case LEGACY -> Phase.MANIFEST;
                    case MANIFEST -> Phase.CHUNKS;
                    case CHUNKS -> Phase.LEGACY;
                };
            }
            switch (selected) {
                case LEGACY -> runLegacyBatch();
                case MANIFEST -> runManifestBatch();
                case CHUNKS -> runChunkBatch();
            }
            failedPhases &= ~(1 << selected.ordinal());
            lastSuccessNanos = System.nanoTime();
        } catch (RuntimeException failed) {
            failures.incrementAndGet();
            failedPhases |= 1 << selected.ordinal();
            LOG.warn("Could not complete a bounded observation cleanup batch; retrying", failed);
        } finally {
            maxDurationNanos.accumulateAndGet(System.nanoTime() - started, Math::max);
        }
    }

    private void runLegacyBatch() {
        List<LatestSnapshot> page = observations.scanLatestAfter(after, batchSize);
        if (page.size() > batchSize) {
            throw new IllegalStateException("observation store returned an unbounded cleanup page");
        }
        String previousId = after.orElse(null);
        for (LatestSnapshot snapshot : page) {
            if (previousId != null && snapshot.pipelineId().compareTo(previousId) <= 0) {
                throw new IllegalStateException("observation cleanup page is not in ascending id order");
            }
            scanned.incrementAndGet();
            boolean removed = orphan(snapshot) && (states != null && artifactOrphan(snapshot)
                    ? observations.deleteOrphanIfUnchanged(snapshot) : observations.deleteIfUnchanged(snapshot));
            if (removed) {
                deleted.incrementAndGet();
            }
            after = Optional.of(snapshot.pipelineId());
            previousId = snapshot.pipelineId();
        }
        if (page.size() < batchSize) {
            after = Optional.empty();
        }
    }

    private void runManifestBatch() {
        List<ObservationStore.ManifestSnapshot> page = observations.scanManifestsAfter(
                manifestAfter, batchSize);
        if (page.size() > batchSize) {
            throw new IllegalStateException("observation store returned an unbounded manifest page");
        }
        String previous = manifestAfter.orElse(null);
        for (ObservationStore.ManifestSnapshot snapshot : page) {
            if (snapshot.cursor().equals(previous)) {
                throw new IllegalStateException("observation manifest cleanup cursor did not advance");
            }
            scanned.incrementAndGet();
            Optional<String> livePipeline = snapshot.scopes().stream()
                    .map(scope -> artifacts.pipelineIdForIncarnation(scope.pipelineIncarnationId()))
                    .flatMap(Optional::stream).findFirst();
            boolean removed = manifestOrphan(snapshot) && (states != null && livePipeline.isPresent()
                    ? observations.deleteManifestIfUnchanged(snapshot, livePipeline.orElseThrow())
                    : observations.deleteManifestIfUnchanged(snapshot));
            if (removed) {
                deleted.incrementAndGet();
            }
            manifestAfter = Optional.of(snapshot.cursor());
            previous = snapshot.cursor();
        }
        if (page.size() < batchSize) {
            manifestAfter = Optional.empty();
        }
    }

    private void runChunkBatch() {
        ObservationStore.ReclaimResult result = observations.reclaimChunks(batchSize);
        if (result.scanned() > batchSize) {
            throw new IllegalStateException("observation store reclaimed an unbounded chunk page");
        }
        scanned.addAndGet(result.scanned());
        deleted.addAndGet(result.deleted());
    }

    private boolean orphan(LatestSnapshot snapshot) {
        String id = snapshot.pipelineId();
        if (states != null && artifactOrphan(snapshot)) { return true; }
        if (protectedContinuation(id)) { return false; }
        if (observations.hasCommittedManifest(id)) {
            return true;
        }
        Optional<String> incarnation = artifacts.pipelineIncarnationId(id);
        if (incarnation.isEmpty()) {
            boolean legacyPipeline = artifacts.get(id)
                    .map(artifact -> "pipeline".equals(artifact.kind())).orElse(false);
            return !legacyPipeline || snapshot.scope().isPresent();
        }
        if (snapshot.scope().isEmpty()
                || !incarnation.get().equals(snapshot.scope().orElseThrow().pipelineIncarnationId())) {
            return true;
        }
        OptionalLong current = generations.currentGeneration(clusterId, id);
        // A missing generation is insufficient evidence for deletion during startup or a mode switch.
        return current.isPresent()
                && current.getAsLong() != snapshot.scope().orElseThrow().executionGeneration();
    }

    private boolean manifestOrphan(ObservationStore.ManifestSnapshot snapshot) {
        if (snapshot.scopes().isEmpty()) {
            return true;
        }
        for (ObservationStore.Scope scope : snapshot.scopes()) {
            Optional<String> id = artifacts.pipelineIdForIncarnation(scope.pipelineIncarnationId());
            if (id.isEmpty()) {
                continue;
            }
            if (protectedContinuation(id.orElseThrow())) { return false; }
            OptionalLong current = generations.currentGeneration(clusterId, id.orElseThrow());
            // A missing generation is insufficient evidence during startup or a mode switch.
            if (current.isEmpty() || current.getAsLong() == scope.executionGeneration()) {
                return false;
            }
        }
        return true;
    }

    private boolean protectedContinuation(String id) {
        if (states == null || !states.supportsStopReservations()) { return false; }
        return states.readStopReservation(id).filter(marker -> marker.legacy()
                ? states.read(id).map(actual -> StopReservation.CounterPolicy.freeze(actual, marker.originalDesired())
                        == StopReservation.CounterPolicy.CONTINUE).orElse(true)
                : marker.counterPolicy() == StopReservation.CounterPolicy.CONTINUE).isPresent();
    }

    /** Missing or foreign artifact identity ends ownership; generation mismatch alone still needs the store guard. */
    private boolean artifactOrphan(LatestSnapshot snapshot) {
        Optional<String> incarnation = artifacts.pipelineIncarnationId(snapshot.pipelineId());
        if (incarnation.isPresent()) {
            return snapshot.scope().filter(scope -> incarnation.orElseThrow().equals(scope.pipelineIncarnationId())).isEmpty();
        }
        return artifacts.get(snapshot.pipelineId()).filter(resource -> "pipeline".equals(resource.kind())).isEmpty()
                || snapshot.scope().isPresent();
    }

    Health health() {
        long success = lastSuccessNanos;
        return new Health(scanned.get(), deleted.get(), failures.get(),
                TimeUnit.NANOSECONDS.toMillis(maxDurationNanos.get()),
                success == 0 ? OptionalLong.empty()
                        : OptionalLong.of(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - success)),
                failedPhases != 0);
    }

    @Override
    public void close() {
        if (worker != null) {
            worker.shutdownNow();
        }
    }
}
