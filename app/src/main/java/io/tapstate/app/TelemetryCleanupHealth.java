package io.tapstate.app;

import io.tapstate.control.core.TelemetryCleanupReporter;
import io.tapstate.core.lifecycle.MetricFact;
import io.tapstate.core.lifecycle.MetricPoint;
import io.tapstate.core.lifecycle.MetricType;
import io.tapstate.core.lifecycle.PipelineEvent;
import io.tapstate.spi.metrics.MetricsExport;

import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/** Local evidence of cleanup residue, independent of the store whose cleanup may have failed. */
final class TelemetryCleanupHealth implements TelemetryCleanupReporter {

    record Health(long failures, long rejections, boolean degraded) {
    }

    private final Clock clock;
    private final Consumer<PipelineEvent> eventOffer;
    private final Instant startedAt;
    private final AtomicLong failures = new AtomicLong();
    private final AtomicLong rejections = new AtomicLong();

    TelemetryCleanupHealth(Clock clock, MetricsExport export, Consumer<PipelineEvent> eventOffer) {
        this.clock = Objects.requireNonNull(clock, "clock");
        this.eventOffer = Objects.requireNonNull(eventOffer, "eventOffer");
        this.startedAt = clock.instant();
        Objects.requireNonNull(export, "export").observeProcess("telemetry-cleanup", this::facts);
    }

    @Override
    public void failed(Failure failure) {
        Objects.requireNonNull(failure, "failure");
        failures.incrementAndGet();
        if (failure.rejected()) {
            rejections.incrementAndGet();
        }
        if (failure.incarnationId() == null || failure.executionGeneration().isEmpty()) {
            return;
        }
        eventOffer.accept(new PipelineEvent("cleanup-" + UUID.randomUUID(),
                failure.pipelineId(), failure.incarnationId(),
                failure.executionGeneration().getAsLong(), PipelineEvent.Kind.CLEANUP_INCOMPLETE,
                clock.instant(), null, null, null, failure.step(), null));
    }

    Health health() {
        long failed = failures.get();
        // A later successful cleanup cannot prove that earlier residue was reclaimed.
        return new Health(failed, rejections.get(), failed > 0);
    }

    List<MetricFact> facts() {
        Health health = health();
        if (health.failures() == 0) {
            return List.of();
        }
        Instant observedAt = clock.instant();
        return List.of(
                MetricFact.single("tapstate.process.telemetry.cleanup.failure", MetricType.COUNTER,
                        "{cleanup}", MetricPoint.accumulated(Map.of(), startedAt, observedAt,
                                health.failures())),
                MetricFact.single("tapstate.process.telemetry.cleanup.rejected", MetricType.COUNTER,
                        "{cleanup}", MetricPoint.accumulated(Map.of(), startedAt, observedAt,
                                health.rejections())),
                MetricFact.single("tapstate.process.telemetry.cleanup.degraded", MetricType.GAUGE,
                        "1", MetricPoint.reading(Map.of(), observedAt, health.degraded() ? 1 : 0)));
    }
}
