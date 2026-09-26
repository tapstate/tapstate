package io.tapstate.adapters.otel;

import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.common.export.MemoryMode;
import io.opentelemetry.sdk.metrics.Aggregation;
import io.opentelemetry.sdk.metrics.InstrumentType;
import io.opentelemetry.sdk.metrics.data.AggregationTemporality;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.export.MetricExporter;
import io.tapstate.core.lifecycle.MetricFact;
import io.tapstate.core.lifecycle.MetricPoint;
import io.tapstate.core.lifecycle.MetricType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/** Measures completed collector pushes without changing the SDK exporter's collection contract. */
final class ObservedMetricExporter implements MetricExporter {

    private static final Logger LOG = LoggerFactory.getLogger(ObservedMetricExporter.class);

    private final MetricExporter delegate;
    private final Instant startedAt = Instant.now();
    private final AtomicLong attemptSequence = new AtomicLong();
    private final AtomicLong successes = new AtomicLong();
    private final AtomicLong failures = new AtomicLong();
    private final AtomicLong maxDurationMillis = new AtomicLong();

    private long latestCompletedAttempt;
    private boolean degraded;
    private Instant lastSuccessAt;

    ObservedMetricExporter(MetricExporter delegate) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
    }

    @Override
    public CompletableResultCode export(Collection<MetricData> metrics) {
        long attempt = attemptSequence.incrementAndGet();
        long began = System.nanoTime();
        try {
            CompletableResultCode result = Objects.requireNonNull(delegate.export(metrics), "export result");
            return result.whenComplete(() -> complete(attempt, began, result.isSuccess()));
        } catch (RuntimeException failure) {
            complete(attempt, began, false);
            return CompletableResultCode.ofFailure();
        }
    }

    private synchronized void complete(long attempt, long began, boolean success) {
        long duration = TimeUnit.NANOSECONDS.toMillis(Math.max(0L, System.nanoTime() - began));
        maxDurationMillis.accumulateAndGet(duration, Math::max);
        if (success) {
            successes.incrementAndGet();
            lastSuccessAt = Instant.now();
        } else {
            failures.incrementAndGet();
        }
        if (attempt >= latestCompletedAttempt) {
            latestCompletedAttempt = attempt;
            if (degraded != !success) {
                degraded = !success;
                if (degraded) {
                    LOG.warn("OTLP metrics export failed; process export health is degraded");
                } else {
                    LOG.info("OTLP metrics export recovered");
                }
            }
        }
    }

    synchronized List<MetricFact> healthFacts() {
        Instant now = Instant.now();
        List<MetricFact> facts = new ArrayList<>();
        facts.add(counter("tapstate.process.otlp.export.success", successes.get(), now));
        facts.add(counter("tapstate.process.otlp.export.failure", failures.get(), now));
        facts.add(gauge("tapstate.process.otlp.export.duration.max", "ms", maxDurationMillis.get(), now));
        if (lastSuccessAt != null) {
            facts.add(gauge("tapstate.process.otlp.export.last_success.age", "ms",
                    Math.max(0L, Duration.between(lastSuccessAt, now).toMillis()), now));
        }
        facts.add(gauge("tapstate.process.otlp.export.degraded", "1", degraded ? 1L : 0L, now));
        return List.copyOf(facts);
    }

    private MetricFact counter(String name, long value, Instant at) {
        return MetricFact.single(name, MetricType.COUNTER, "{export}",
                MetricPoint.accumulated(Map.of(), startedAt, at, value));
    }

    private static MetricFact gauge(String name, String unit, long value, Instant at) {
        return MetricFact.single(name, MetricType.GAUGE, unit, MetricPoint.reading(Map.of(), at, value));
    }

    @Override
    public AggregationTemporality getAggregationTemporality(InstrumentType instrumentType) {
        return delegate.getAggregationTemporality(instrumentType);
    }

    @Override
    public Aggregation getDefaultAggregation(InstrumentType instrumentType) {
        return delegate.getDefaultAggregation(instrumentType);
    }

    @Override
    public MemoryMode getMemoryMode() {
        return delegate.getMemoryMode();
    }

    @Override
    public CompletableResultCode flush() {
        return delegate.flush();
    }

    @Override
    public CompletableResultCode shutdown() {
        return delegate.shutdown();
    }
}
