package io.tapstate.core.alerting;

import io.tapstate.core.lifecycle.MetricAttributes;
import io.tapstate.core.lifecycle.MetricFact;
import io.tapstate.core.lifecycle.MetricPoint;
import io.tapstate.core.lifecycle.Observation;
import java.time.Instant;
import java.util.List;
import java.util.Objects;

/**
 * Bounded selection from a published observation, before any export projection. This has no queue,
 * worker, storage or durable-acceptance semantics. The host supplies and authenticates the binding.
 */
public final class ObservationMetricInput {
    public static final int CONTRACT_VERSION = 1;
    public static final int MAX_FACTS = 4096;
    public static final int MAX_POINTS = 65536;
    private ObservationMetricInput() { }

    public enum Status {
        READY, BINDING_MISMATCH, FACT_LIMIT, POINT_LIMIT, MISSING_OBSERVATION_TIME, INVALID_OBSERVATION_TIME,
        EXPORT_PROJECTION
    }

    /** Internal work limits supplied by the host, separate from metric-export cardinality limits. */
    public record Budget(int maxFacts, int maxPoints) {
        public Budget {
            AlertValidation.require(maxFacts > 0 && maxFacts <= MAX_FACTS, "inputMaxFacts", "outside-range");
            AlertValidation.require(maxPoints > 0 && maxPoints <= MAX_POINTS, "inputMaxPoints", "outside-range");
        }
    }

    /**
     * The original publication tagged with its trusted evaluation binding and producer sequence.
     * Observation alone cannot establish scope, resource incarnation or a current producer generation.
     */
    public record Publication(AlertRule binding, long sequence, Observation observation) {
        public Publication {
            Objects.requireNonNull(binding, "binding");
            Objects.requireNonNull(observation, "observation");
            AlertValidation.require(sequence >= 0, "sequence", "must-be-nonnegative");
        }
    }

    /**
     * READY can carry an UNKNOWN conclusion. Other statuses provide no signal or accepted frontier.
     * Point is only the selected original measurement, not proof the lifecycle accepted it.
     */
    public record Reading(Status status, AlertSignal signal, MetricAlertEvaluator.Result evaluation,
            MetricPoint point) { }

    /**
     * Computes a candidate only. Neither this result nor an export offer licenses a state write or send.
     * Previous must belong to this exact bound metric series, as required by the metric evaluator.
     */
    public static Reading evaluate(AlertRule expected, MetricAlertEvaluator.Rule metric,
            Publication publication, Budget budget, MetricPoint previous, boolean firing,
            AlarmSeverity severity, Instant now) {
        Objects.requireNonNull(expected, "expected");
        Objects.requireNonNull(metric, "metric");
        Objects.requireNonNull(publication, "publication");
        Objects.requireNonNull(budget, "budget");
        Objects.requireNonNull(severity, "severity");
        Objects.requireNonNull(now, "now");
        Observation observation = publication.observation();
        if (!expected.equals(publication.binding()) || !expected.key().resource().kind().equals("pipeline")
                || !expected.key().resource().id().equals(observation.pipelineId())) {
            return refused(Status.BINDING_MISMATCH);
        }
        if (observation.observedAt() == null) { return refused(Status.MISSING_OBSERVATION_TIME); }
        if (observation.observedAt().isAfter(now)) { return refused(Status.INVALID_OBSERVATION_TIME); }
        if (observation.facts().size() > budget.maxFacts()) { return refused(Status.FACT_LIMIT); }
        int points = 0;
        MetricFact selected = null;
        // Check the whole publication before accepting any prefix, even when the matching fact is first.
        for (MetricFact fact : observation.facts()) {
            if (fact.points().size() > budget.maxPoints() - points) { return refused(Status.POINT_LIMIT); }
            points += fact.points().size();
            if (metric.name().equals(fact.name())) { selected = fact; }
        }
        MetricPoint point = null;
        if (selected != null) {
            for (MetricPoint candidate : selected.points()) {
                if (metric.attributes().equals(candidate.attributes())) {
                    point = candidate;
                    break;
                }
            }
        }
        if (point != null && point.attributes().containsKey(MetricAttributes.OVERFLOW)) {
            return refused(Status.EXPORT_PROJECTION);
        }
        if (point != null && point.observedAt().isAfter(observation.observedAt())) {
            return refused(Status.INVALID_OBSERVATION_TIME);
        }
        // Keep the original point, including its accumulation start and measurement time. Never use the
        // lossy numeric map or an export overflow aggregate to invent a missing resource measurement.
        MetricFact exact = selected == null ? null
                : new MetricFact(selected.name(), selected.type(), selected.unit(),
                        point == null ? List.of() : List.of(point));
        MetricAlertEvaluator.Result measured = MetricAlertEvaluator.evaluate(metric, exact, previous,
                firing, now, expected.maxAge(), expected.maxGap());
        Instant observedAt = point == null ? observation.observedAt() : point.observedAt();
        AlertSignal signal = new AlertSignal(expected.key(), expected.revision(), expected.generation(),
                expected.producerIncarnation(), publication.sequence(), observedAt, measured.condition(), severity);
        return new Reading(Status.READY, signal, measured, point);
    }

    private static Reading refused(Status status) { return new Reading(status, null, null, null); }
}
