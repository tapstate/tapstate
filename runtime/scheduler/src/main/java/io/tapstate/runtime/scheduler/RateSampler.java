package io.tapstate.runtime.scheduler;

import io.tapstate.core.lifecycle.MetricAttributes;
import io.tapstate.core.lifecycle.MetricFact;
import io.tapstate.core.lifecycle.MetricPoint;
import io.tapstate.core.lifecycle.Observation;
import io.tapstate.core.lifecycle.RateSample;
import io.tapstate.spi.store.RateHistoryStore;
import io.tapstate.spi.store.ObservationStore;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Keeps a history of a pipeline's movement by taking a sample off one measured observation frame,
 * once per interval. A frame is measured every convergence pass; a history at that cadence is
 * over a million documents per pipeline for the retention, and a line drawn over fifteen days needs
 * nothing like it. So the sampler is offered every observation and keeps one per interval.
 *
 * <p><strong>A sample is the observation's own numbers at the observation's own time.</strong> Nothing is
 * re-measured and nothing is re-timed: the latest store and history sink receive the same frame even if
 * either sink is slow or unavailable. What is kept is the subset a line is drawn from —
 * the pipeline-level counters and the per-table delay — and what the counters accumulate from, read off
 * the facts beside the flat map.
 *
 * <p>An observation with nothing to draw a line from is not sampled. A pipeline with no live job publishes
 * no counters, and a history of empty documents, one a minute for fifteen days per stopped pipeline, would
 * be the cost of a stop that nobody reads back. The samples that pipeline took while running stay where
 * they are: stopping is not what removes a history, deleting the pipeline is.
 */
public final class RateSampler {

    /** The flat keys a line can be drawn from: the pipeline-level counters, by prefix or by name. */
    private static final List<String> COUNTER_PREFIXES = List.of("records.", "bytes.", "errors.");
    private static final String DRIVEN = "recordCount";
    private static final String LAG_PREFIX = "lag.";
    private static final String RECORDS_FACT = "tapstate.pipeline.records";

    private final RateHistoryStore history;
    private final Duration interval;
    private record Cadence(ObservationStore.Scope scope, Instant lastWrittenAt, Instant firstFailedAt) { }

    private final Map<String, Cadence> cadence = new ConcurrentHashMap<>();
    private final AtomicInteger openGaps = new AtomicInteger();
    private final AtomicLong gapsOpened = new AtomicLong();
    private final AtomicLong gapsClosed = new AtomicLong();
    private final Instant startedAt = Instant.now();

    /** Local sampling loss evidence; deletion or identity replacement is not a successful recovery. */
    public record GapHealth(int open, long opened, long closed, Instant startedAt) { }

    public GapHealth gapHealth() {
        return new GapHealth(openGaps.get(), gapsOpened.get(), gapsClosed.get(), startedAt);
    }

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
        appendIfDue(observation);
    }

    /** Returns whether this frame appended a retained sample. */
    public boolean appendIfDue(Observation observation) {
        return appendIfDue(observation, null);
    }

    /** Samples one execution without letting the preceding execution suppress its first frame. */
    public boolean appendIfDue(Observation observation, ObservationStore.Scope scope) {
        Objects.requireNonNull(observation, "observation");
        if (observation.observedAt() == null) {
            return false;
        }
        RateSample sample = sampleOf(observation);
        if (sample == null) {
            return false;
        }
        Cadence seen = cadence.get(observation.pipelineId());
        Cadence prior = seen;
        if (prior != null && !Objects.equals(prior.scope(), scope)) {
            if (olderThan(scope, prior.scope())) {
                return false;
            }
            prior = null;
        }
        if (prior != null && prior.lastWrittenAt() != null
                && observation.observedAt().isBefore(prior.lastWrittenAt().plus(interval))) {
            return false;
        }
        Instant gapFrom = prior != null && prior.firstFailedAt() != null
                && prior.firstFailedAt().isBefore(sample.observedAt()) ? prior.firstFailedAt() : null;
        try {
            if (scope == null) {
                history.append(sample, gapFrom);
            } else {
                history.appendScoped(sample, scope, gapFrom);
            }
        } catch (RuntimeException failed) {
            cadence.compute(observation.pipelineId(), (id, current) -> {
                if (current != null && !Objects.equals(current.scope(), scope) && current != seen) {
                    return current;
                }
                Cadence same = current == null || !Objects.equals(current.scope(), scope) ? null : current;
                Instant first = same == null || same.firstFailedAt() == null
                        ? sample.observedAt() : min(same.firstFailedAt(), sample.observedAt());
                return trackGapChange(current,
                        new Cadence(scope, same == null ? null : same.lastWrittenAt(), first), false);
            });
            throw failed;
        }
        cadence.compute(observation.pipelineId(), (id, current) -> {
            if (current != null && !Objects.equals(current.scope(), scope) && current != seen) {
                return current;
            }
            Cadence same = current == null || !Objects.equals(current.scope(), scope) ? null : current;
            Instant last = same == null || same.lastWrittenAt() == null
                    ? sample.observedAt() : max(same.lastWrittenAt(), sample.observedAt());
            // A loss observed during the store call was not carried by this document. Retain its
            // boundary for a later recovery sample rather than erasing it with this completion.
            Instant unresolved = same == null ? null : same == seen
                    ? same.firstFailedAt() != null && same.firstFailedAt().isAfter(sample.observedAt())
                            ? same.firstFailedAt() : null
                    : same.firstFailedAt();
            return trackGapChange(current, new Cadence(scope, last, unresolved), true);
        });
        return true;
    }

    /** Remembers a due history frame rejected before it reached the append worker. No store IO occurs. */
    public void markDropped(Observation observation, ObservationStore.Scope scope) {
        Objects.requireNonNull(observation, "observation");
        if (observation.observedAt() == null || sampleOf(observation) == null) {
            return;
        }
        cadence.compute(observation.pipelineId(), (id, current) -> {
            Cadence original = current;
            if (current != null && !Objects.equals(current.scope(), scope)) {
                if (olderThan(scope, current.scope())) {
                    return current;
                }
                current = null;
            }
            if (current != null && current.lastWrittenAt() != null
                    && observation.observedAt().isBefore(current.lastWrittenAt().plus(interval))) {
                return current;
            }
            Instant first = current == null || current.firstFailedAt() == null
                    ? observation.observedAt() : min(current.firstFailedAt(), observation.observedAt());
            return trackGapChange(original,
                    new Cadence(scope, current == null ? null : current.lastWrittenAt(), first), false);
        });
    }

    /** Drops the cadence bookkeeping of every pipeline outside {@code live}, which is the set that still exists. */
    public void forgetPipelinesOutside(Collection<String> live) {
        Objects.requireNonNull(live, "live");
        cadence.forEach((id, ignored) -> {
            if (!live.contains(id)) {
                cadence.computeIfPresent(id, (key, current) -> trackGapChange(current, null, false));
            }
        });
    }

    private Cadence trackGapChange(Cadence previous, Cadence next, boolean written) {
        boolean wasOpen = previous != null && previous.firstFailedAt() != null;
        boolean nowOpen = next != null && next.firstFailedAt() != null;
        boolean sameScope = previous != null && next != null
                && Objects.equals(previous.scope(), next.scope());
        if (wasOpen != nowOpen) {
            openGaps.addAndGet(nowOpen ? 1 : -1);
        }
        if (nowOpen && (!wasOpen || !sameScope)) {
            gapsOpened.incrementAndGet();
        }
        if (wasOpen && !nowOpen && sameScope && written) {
            gapsClosed.incrementAndGet();
        }
        return next;
    }

    private static Instant min(Instant left, Instant right) {
        return left.isBefore(right) ? left : right;
    }

    private static Instant max(Instant left, Instant right) {
        return left.isAfter(right) ? left : right;
    }

    private static boolean olderThan(ObservationStore.Scope candidate, ObservationStore.Scope current) {
        return current != null && (candidate == null
                || candidate.executionGeneration() <= current.executionGeneration());
    }

    /** The sample {@code observation} yields, or {@code null} when it carries nothing to draw a line from. */
    static RateSample sampleOf(Observation observation) {
        Map<String, Long> counters = new LinkedHashMap<>();
        Map<String, Long> lag = new LinkedHashMap<>();
        observation.metrics().forEach((key, value) -> {
            if (DRIVEN.equals(key) || COUNTER_PREFIXES.stream().anyMatch(key::startsWith)) {
                counters.put(key, value);
            } else if (key.startsWith(LAG_PREFIX)) {
                lag.put(key.substring(LAG_PREFIX.length()), value);
            }
        });
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
