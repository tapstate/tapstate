package io.tapstate.runtime.scheduler;

import io.tapstate.core.lifecycle.CdcDeliveryTotals;
import io.tapstate.core.lifecycle.MetricAttributes;
import io.tapstate.core.lifecycle.MetricFact;
import io.tapstate.core.lifecycle.MetricPoint;
import io.tapstate.core.lifecycle.Observation;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.lifecycle.RateSample;
import io.tapstate.spi.store.RateHistoryStore;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Keeps a history of a pipeline's movement by taking a sample off the observation it just published,
 * once per interval. The observation is published every convergence pass; a history at that cadence is
 * over a million documents per pipeline for the retention, and a line drawn over fifteen days needs
 * nothing like it. So the sampler is offered every observation and keeps one per interval.
 *
 * <p><strong>A sample is the observation's own numbers at the observation's own time.</strong> Nothing is
 * re-measured and nothing is re-timed: the same pass produced the latest state and the sample, so the
 * two can never describe different moments of the run. What is kept is the subset a line is drawn from —
 * the pipeline-level counters and the per-table delay — and what the counters accumulate from, read off
 * the facts beside the flat map.
 *
 * <p>An observation with nothing to draw a line from is not sampled. A pipeline with no live job publishes
 * no counters, and a history of empty documents, one a minute for fifteen days per stopped pipeline, would
 * be the cost of a stop that nobody reads back. The samples that pipeline took while running stay where
 * they are: stopping is not what removes a history, deleting the pipeline is. A completed pipeline retains
 * its final metrics for display, but later state publications are not new measurements of those metrics.
 */
public final class RateSampler {

    /** The flat keys a line can be drawn from: the pipeline-level counters, by prefix or by name. */
    private static final List<String> COUNTER_PREFIXES = List.of("records.", "bytes.", "errors.");
    private static final String DRIVEN = "recordCount";
    private static final String LAG_PREFIX = "lag.";
    private static final String RECORDS_FACT = "tapstate.pipeline.records";

    private final RateHistoryStore history;
    private final Duration interval;
    private record Cadence(Object scope, Instant at) {}
    private final Map<String, Cadence> lastSampledAt = new ConcurrentHashMap<>();
    private record Pending(Object scope, RateSample sample, RateHistoryStore destination, long epoch) {}
    private static final class Flight {
        private Pending pending;
        private long epoch;
    }
    private final Map<String, Flight> appending = new java.util.HashMap<>();

    public RateSampler(RateHistoryStore history, Duration interval) {
        this.history = Objects.requireNonNull(history, "history");
        this.interval = Objects.requireNonNull(interval, "interval");
        if (interval.isNegative() || interval.isZero()) {
            throw new IllegalArgumentException("a sampling interval is a positive length of time: " + interval);
        }
    }

    /**
     * Offers {@code observation}; a sample is appended when the pipeline's last sample is at least an
     * interval older than this observation and the observation carries something to draw a line from.
     */
    public void offer(Observation observation) {
        Objects.requireNonNull(observation, "observation"); offer(observation.pipelineId(), observation, history);
    }

    /** The original Pipeline scope owns both the cadence and this invocation's destination. */
    public void offer(Observation observation, io.tapstate.spi.store.PipelineIntentScope scope, RateHistoryStore destination) {
        Objects.requireNonNull(observation, "observation"); Objects.requireNonNull(scope, "scope"); Objects.requireNonNull(destination, "destination");
        requirePipeline(observation.pipelineId(), scope);
        offer(scope, observation, destination);
    }

    /** Offers a sample prepared from an already published observation. */
    public void offerPrepared(RateSample sample, io.tapstate.spi.store.PipelineIntentScope scope, RateHistoryStore destination) {
        requirePipeline(sample.pipelineId(), scope);
        enqueue(scope, sample, destination);
    }

    private static void requirePipeline(String pipeline, io.tapstate.spi.store.PipelineIntentScope scope) {
        if (!scope.pipelineId().equals(pipeline)) throw new io.tapstate.core.common.TapstateException(
                io.tapstate.spi.store.IoError.WORKLOAD_CLAIM_FENCED, Map.of(), null);
    }

    private void offer(Object key, Observation observation, RateHistoryStore destination) {
        RateSample sample = sampleOf(observation);
        if (sample != null) enqueue(key, sample, destination);
    }

    private void enqueue(Object key, RateSample sample, RateHistoryStore destination) {
        String pipelineId = sample.pipelineId();
        Flight flight;
        Pending next;
        synchronized (appending) {
            flight = appending.get(pipelineId);
            if (flight != null) {
                var pending = flight.pending;
                if (pending == null || !sample.observedAt().isBefore(pending.sample().observedAt()))
                    flight.pending = new Pending(key, sample, destination, flight.epoch);
                return;
            }
            flight = new Flight();
            appending.put(pipelineId, flight);
            next = new Pending(key, sample, destination, flight.epoch);
        }
        drain(next, flight);
    }

    private void drain(Pending next, Flight flight) {
        String pipelineId = next.sample().pipelineId();
        Throwable failure = null;
        while (next != null) {
            try { append(next, flight); }
            catch (RuntimeException | Error problem) {
                if (failure == null) failure = problem;
                else if (problem instanceof Error && !(failure instanceof Error)) {
                    problem.addSuppressed(failure);
                    failure = problem;
                } else if (failure != problem) failure.addSuppressed(problem);
            }
            synchronized (appending) {
                next = flight.pending;
                flight.pending = null;
                if (next == null) appending.remove(pipelineId, flight);
            }
        }
        if (failure instanceof Error fatal) throw fatal;
        if (failure instanceof RuntimeException refused) throw refused;
    }

    private void append(Pending offer, Flight flight) {
        var sample = offer.sample();
        var pipelineId = sample.pipelineId();
        synchronized (appending) {
            if (flight.epoch != offer.epoch()) return;
            var last = lastSampledAt.get(pipelineId);
            if (last != null && offer.scope().equals(last.scope()) && sample.observedAt().isBefore(last.at().plus(interval)))
                return;
        }
        offer.destination().append(sample);
        synchronized (appending) {
            if (flight.epoch == offer.epoch()) lastSampledAt.put(pipelineId, new Cadence(offer.scope(), sample.observedAt()));
        }
    }

    /** A final publication waits for every retained append of this Pipeline, including an older scope. */
    public boolean idle(io.tapstate.spi.store.PipelineIntentScope scope) {
        Objects.requireNonNull(scope, "scope");
        synchronized (appending) { return !appending.containsKey(scope.pipelineId()); }
    }

    /** Drops cadence and pending work for deleted Pipelines without waiting for their active append. */
    public void forgetPipelinesOutside(Collection<String> live) {
        var kept = Set.copyOf(live);
        synchronized (appending) {
            lastSampledAt.keySet().retainAll(kept);
            appending.forEach((id, flight) -> {
                if (!kept.contains(id)) {
                    flight.pending = null;
                    flight.epoch++;
                }
            });
        }
    }

    /** The sample {@code observation} yields, or {@code null} when it carries nothing to draw a line from. */
    public static RateSample sampleOf(Observation observation) {
        if (observation.observedAt() == null || observation.state() == PipelineState.COMPLETED) {
            return null;
        }
        Map<String, Long> counters = new LinkedHashMap<>();
        Map<String, Long> lag = new LinkedHashMap<>();
        observation.metrics().forEach((key, value) -> {
            if (DRIVEN.equals(key) || COUNTER_PREFIXES.stream().anyMatch(key::startsWith)) {
                counters.put(key, value);
            } else if (key.startsWith(LAG_PREFIX)) {
                lag.put(key.substring(LAG_PREFIX.length()), value);
            }
        });
        for (MetricFact fact : observation.facts()) {
            if (CdcDeliveryTotals.COUNT_METRIC.equals(fact.name()) || CdcDeliveryTotals.TOTAL_MILLIS_METRIC.equals(fact.name())) {
                counters.put(fact.name(), fact.points().getFirst().value());
            }
        }
        if (counters.isEmpty() && lag.isEmpty()) {
            return null;
        }
        return new RateSample(observation.pipelineId(), observation.observedAt(), counters, lag,
                countingSince(observation.facts()));
    }

    /**
     * What the counters accumulate from: the latest start among the confirmed-delivery points, which is
     * the start the engine already chose for the pipeline's totals; absent when nothing accumulates.
     */
    private static Instant countingSince(List<MetricFact> facts) {
        Instant latest = null;
        for (MetricFact fact : facts) {
            if (!RECORDS_FACT.equals(fact.name())) {
                continue;
            }
            for (MetricPoint point : fact.points()) {
                if (!"out".equals(point.attributes().get(MetricAttributes.DIRECTION)) || point.startTime() == null) {
                    continue;
                }
                latest = latest == null || point.startTime().isAfter(latest) ? point.startTime() : latest;
            }
        }
        return latest;
    }
}
