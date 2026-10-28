package io.tapstate.core.alerting;

import io.tapstate.core.lifecycle.Observation;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;

/** Original typed evidence. None of these values authenticates a producer or grants a store write. */
public sealed interface AlertInput permits AlertInput.Conditions, AlertInput.Metric, AlertInput.Freshness,
        AlertInput.Discontinuity {
    Instant observedAt();

    record Conditions(Instant observedAt, AlertConditionGroup.Operator operator,
            List<AlertSignal.Condition> values, AlarmDiagnostics.SafeTrace trace) implements AlertInput {
        public Conditions {
            Objects.requireNonNull(observedAt); values = List.copyOf(values);
            AlertConditionGroup.evaluate(operator, values);
        }
        public Conditions(Instant at, AlertSignal.Condition condition, AlarmDiagnostics.SafeTrace trace) {
            this(at, AlertConditionGroup.Operator.ALL, List.of(condition), trace);
        }
    }
    record Metric(Observation observation) implements AlertInput {
        public Metric { Objects.requireNonNull(observation); }
        @Override public Instant observedAt() { return observation.observedAt(); }
    }
    record Freshness(Instant observedAt, boolean expected, Instant expectedSince, Instant lastPublication,
            Duration startupGrace, Duration maxAge) implements AlertInput {
        public Freshness { Objects.requireNonNull(observedAt); }
    }
    record Discontinuity(Instant observedAt) implements AlertInput {
        public Discontinuity { Objects.requireNonNull(observedAt); }
    }
}
