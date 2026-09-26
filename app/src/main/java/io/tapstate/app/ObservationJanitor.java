package io.tapstate.app;

import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.ArtifactStore;
import io.tapstate.spi.store.ExecutionGenerationStore;
import io.tapstate.spi.store.ObservationStore;
import io.tapstate.spi.store.ObservationStore.LatestSnapshot;
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
    private final String clusterId;
    private final int batchSize;
    private final ScheduledExecutorService worker;
    private final AtomicLong scanned = new AtomicLong();
    private final AtomicLong deleted = new AtomicLong();
    private final AtomicLong failures = new AtomicLong();
    private final AtomicLong maxDurationNanos = new AtomicLong();
    private volatile long lastSuccessNanos;
    private volatile long lastFailureNanos;
    private Optional<String> after = Optional.empty();

    ObservationJanitor(ObservationStore observations, ArtifactStore artifacts,
            ExecutionGenerationStore generations, String clusterId, int batchSize,
            Duration interval) {
        this(observations, artifacts, generations, clusterId, batchSize, interval, true);
    }

    ObservationJanitor(ObservationStore observations, ArtifactStore artifacts,
            ExecutionGenerationStore generations, String clusterId, int batchSize,
            Duration interval, boolean schedule) {
        this.observations = Objects.requireNonNull(observations, "observations");
        this.artifacts = Objects.requireNonNull(artifacts, "artifacts");
        this.generations = Objects.requireNonNull(generations, "generations");
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
        try {
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
                if (orphan(snapshot) && observations.deleteIfUnchanged(snapshot)) {
                    deleted.incrementAndGet();
                }
                after = Optional.of(snapshot.pipelineId());
                previousId = snapshot.pipelineId();
            }
            if (page.size() < batchSize) {
                after = Optional.empty();
            }
            lastSuccessNanos = System.nanoTime();
        } catch (RuntimeException failed) {
            failures.incrementAndGet();
            lastFailureNanos = System.nanoTime();
            LOG.warn("Could not complete a bounded observation cleanup batch; retrying", failed);
        } finally {
            maxDurationNanos.accumulateAndGet(System.nanoTime() - started, Math::max);
        }
    }

    private boolean orphan(LatestSnapshot snapshot) {
        String id = snapshot.pipelineId();
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

    Health health() {
        long success = lastSuccessNanos;
        long failure = lastFailureNanos;
        return new Health(scanned.get(), deleted.get(), failures.get(),
                TimeUnit.NANOSECONDS.toMillis(maxDurationNanos.get()),
                success == 0 ? OptionalLong.empty()
                        : OptionalLong.of(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - success)),
                failure != 0 && (success == 0 || failure - success > 0));
    }

    @Override
    public void close() {
        if (worker != null) {
            worker.shutdownNow();
        }
    }
}
