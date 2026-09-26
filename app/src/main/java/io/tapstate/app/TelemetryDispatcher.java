package io.tapstate.app;

import io.tapstate.core.lifecycle.Observation;
import io.tapstate.runtime.scheduler.ObservationPublisher;
import io.tapstate.runtime.scheduler.RateSampler;
import io.tapstate.spi.metrics.MetricsExport;
import io.tapstate.spi.store.ObservationStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/** Fixed worker budgets keep slow telemetry stores away from convergence and data-plane calls. */
final class TelemetryDispatcher implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(TelemetryDispatcher.class);
    static final int DEFAULT_LATEST_WORKERS = 4;
    static final int DEFAULT_QUEUE_CAPACITY = 64;

    private sealed interface Frame permits ObservationFrame, ReconcileFailureFrame {
    }

    private record ObservationFrame(ObservationPublisher.Prepared prepared, ObservationStore.Scope scope)
            implements Frame {
    }

    private record ReconcileFailureFrame(String pipelineId, long failures, ObservationStore.Scope scope)
            implements Frame {
    }

    private final ObservationPublisher publisher;
    private final RateSampler sampler;
    private final MetricsExport export;
    private final ObservationScopeRegistry scopes;
    private final ThreadPoolExecutor latestWorkers;
    private final ThreadPoolExecutor historyWorker;
    private final ThreadPoolExecutor exportWorker;
    private final Semaphore latestCapacity;
    private final ConcurrentHashMap<String, LatestSlot> latestByPipeline = new ConcurrentHashMap<>();

    TelemetryDispatcher(ObservationPublisher publisher, RateSampler sampler, MetricsExport export,
            int latestConcurrency, int queueCapacity) {
        this(publisher, sampler, export, null, latestConcurrency, queueCapacity);
    }

    TelemetryDispatcher(ObservationPublisher publisher, RateSampler sampler, MetricsExport export,
            ObservationScopeRegistry scopes, int latestConcurrency, int queueCapacity) {
        this.publisher = Objects.requireNonNull(publisher, "publisher");
        this.sampler = sampler;
        this.export = Objects.requireNonNull(export, "export");
        this.scopes = scopes;
        if (latestConcurrency < 1 || queueCapacity < 1) {
            throw new IllegalArgumentException("telemetry worker and queue budgets must be positive");
        }
        latestCapacity = new Semaphore(Math.addExact(latestConcurrency, queueCapacity));
        latestWorkers = workers("latest", latestConcurrency, queueCapacity);
        historyWorker = workers("history", 1, queueCapacity);
        exportWorker = workers("export", 1, queueCapacity);
    }

    private static ThreadPoolExecutor workers(String sink, int count, int queueCapacity) {
        AtomicInteger next = new AtomicInteger();
        return new ThreadPoolExecutor(count, count, 0, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(queueCapacity), task -> {
                    Thread thread = new Thread(task, "tapstate-telemetry-" + sink + "-" + next.incrementAndGet());
                    thread.setDaemon(true);
                    return thread;
                }, new ThreadPoolExecutor.AbortPolicy());
    }

    void offer(ObservationPublisher.Prepared prepared, ObservationStore.Scope scope) {
        Objects.requireNonNull(prepared, "prepared");
        Observation observation = prepared.observation();
        offerLatest(observation.pipelineId(), new ObservationFrame(prepared, scope));
        if (sampler != null) {
            offerSide(historyWorker, observation.pipelineId(), "history", () -> {
                if (stillCurrent(observation.pipelineId(), scope)) {
                    sampler.offer(observation);
                }
            });
        }
        if (export != MetricsExport.none()) {
            offerSide(exportWorker, observation.pipelineId(), "export", () -> {
                if (stillCurrent(observation.pipelineId(), scope)) {
                    export.offer(observation.pipelineId(), observation.state(),
                            observation.observedAt(), observation.facts());
                }
            });
        }
    }

    private boolean stillCurrent(String pipelineId, ObservationStore.Scope scope) {
        return scopes == null || (scope != null && scopes.current(pipelineId).filter(scope::equals).isPresent());
    }

    void offerReconcileFailure(String pipelineId, long failures, ObservationStore.Scope scope) {
        offerLatest(pipelineId, new ReconcileFailureFrame(pipelineId, failures, scope));
    }

    private void offerLatest(String pipelineId, Frame frame) {
        while (true) {
            LatestSlot existing = latestByPipeline.get(pipelineId);
            if (existing != null) {
                synchronized (existing) {
                    if (existing.retired) {
                        continue;
                    }
                    existing.pending = frame;
                    return;
                }
            }
            if (!latestCapacity.tryAcquire()) {
                LOG.warn("Latest observation for pipeline {} was dropped: telemetry queue is full", pipelineId);
                return;
            }
            LatestSlot created = new LatestSlot(pipelineId, frame);
            if (latestByPipeline.putIfAbsent(pipelineId, created) != null) {
                latestCapacity.release();
                continue;
            }
            try {
                latestWorkers.execute(created);
            } catch (java.util.concurrent.RejectedExecutionException unavailable) {
                synchronized (created) {
                    created.retired = true;
                    created.pending = null;
                    latestByPipeline.remove(pipelineId, created);
                }
                latestCapacity.release();
                LOG.warn("Latest observation for pipeline {} was dropped: telemetry workers stopped", pipelineId);
            }
            return;
        }
    }

    private static void offerSide(ThreadPoolExecutor workers, String pipelineId, String sink, Runnable write) {
        try {
            workers.execute(() -> {
                try {
                    write.run();
                } catch (RuntimeException failed) {
                    LOG.warn("Could not write {} telemetry for pipeline {}", sink, pipelineId, failed);
                }
            });
        } catch (java.util.concurrent.RejectedExecutionException saturated) {
            LOG.warn("{} telemetry for pipeline {} was dropped: queue is full", sink, pipelineId);
        }
    }

    void retain(Collection<String> pipelineIds) {
        for (String id : latestByPipeline.keySet()) {
            if (!pipelineIds.contains(id)) {
                LatestSlot slot = latestByPipeline.get(id);
                if (slot != null) {
                    synchronized (slot) {
                        slot.pending = null;
                    }
                }
            }
        }
    }

    @Override
    public void close() {
        shutdown(latestWorkers);
        shutdown(historyWorker);
        shutdown(exportWorker);
    }

    private static void shutdown(ThreadPoolExecutor workers) {
        workers.shutdown();
        try {
            if (!workers.awaitTermination(Duration.ofSeconds(2).toNanos(), TimeUnit.NANOSECONDS)) {
                workers.shutdownNow();
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            workers.shutdownNow();
        }
    }

    private final class LatestSlot implements Runnable {
        private final String pipelineId;
        private Frame pending;
        private boolean retired;

        private LatestSlot(String pipelineId, Frame first) {
            this.pipelineId = pipelineId;
            this.pending = first;
        }

        @Override
        public void run() {
            while (true) {
                Frame frame;
                synchronized (this) {
                    frame = pending;
                    pending = null;
                    if (frame == null) {
                        retired = true;
                        latestByPipeline.remove(pipelineId, this);
                        latestCapacity.release();
                        return;
                    }
                }
                try {
                    if (frame instanceof ObservationFrame observation) {
                        publisher.commit(observation.prepared(), observation.scope());
                    } else if (frame instanceof ReconcileFailureFrame failure) {
                        if (failure.scope() == null) {
                            publisher.publishReconcileFailure(failure.pipelineId(), failure.failures());
                        } else {
                            publisher.publishReconcileFailureScoped(
                                    failure.pipelineId(), failure.failures(), failure.scope());
                        }
                    }
                } catch (RuntimeException failed) {
                    LOG.warn("Could not write latest observation for pipeline {}", pipelineId, failed);
                }
            }
        }
    }
}
