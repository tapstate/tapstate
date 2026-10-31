package io.tapstate.core.alerting;

import io.tapstate.core.lifecycle.MetricFact;
import io.tapstate.core.lifecycle.MetricPoint;
import io.tapstate.core.lifecycle.MetricType;
import java.math.BigDecimal;
import java.math.MathContext;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;

/** Consumes original measurements. No exporter, exported labels or scrape result is an input. */
public final class MetricAlertEvaluator {
    private MetricAlertEvaluator() { }
    public enum Direction { ABOVE, BELOW }
    public enum Quality { VALID, MISSING, STALE, CLOCK_INVALID, UNSUPPORTED, RESET, GAP, OUT_OF_ORDER }
    public record Rule(String name, MetricType type, String unit, Map<String, String> attributes,
            Direction direction, BigDecimal trigger, BigDecimal recover) {
        public Rule {
            name = AlertValidation.text(name, "metricName", 256);
            unit = AlertValidation.text(unit, "metricUnit", 64);
            Objects.requireNonNull(type, "type");
            Objects.requireNonNull(direction, "direction");
            Objects.requireNonNull(trigger, "trigger");
            Objects.requireNonNull(recover, "recover");
            attributes = Map.copyOf(attributes);
            AlertValidation.require(attributes.size() <= 16, "metricAttributes", "too-many");
            attributes.forEach((key, value) -> {
                AlertValidation.text(key, "metricAttributeKey", 128);
                AlertValidation.text(value, "metricAttributeValue", 256);
            });
            AlertValidation.require(direction == Direction.ABOVE ? trigger.compareTo(recover) >= 0
                    : trigger.compareTo(recover) <= 0, "thresholds", "invalid-hysteresis");
        }
    }
    public record Result(AlertSignal.Condition condition, Quality quality, BigDecimal value) { }

    /** Previous must be the last accepted point of this exact bound series. */
    public static Result evaluate(Rule rule, MetricFact fact, MetricPoint previous, boolean firing,
            Instant now, Duration maxAge, Duration maxGap) {
        Objects.requireNonNull(now, "now");
        AlertValidation.duration(maxAge, "maxAge", false, Duration.ofHours(1));
        AlertValidation.duration(maxGap, "maxGap", false, maxAge);
        if (fact == null || !rule.name().equals(fact.name())) { return unknown(Quality.MISSING); }
        if (rule.type() != fact.type() || !rule.unit().equals(fact.unit()) || fact.type() == MetricType.HISTOGRAM) {
            return unknown(Quality.UNSUPPORTED);
        }
        MetricPoint point = fact.points().stream().filter(p -> p.attributes().equals(rule.attributes())).findFirst().orElse(null);
        if (point == null) { return unknown(Quality.MISSING); }
        if (point.observedAt().isAfter(now)) { return unknown(Quality.CLOCK_INVALID); }
        if (point.observedAt().plus(maxAge).isBefore(now)) { return unknown(Quality.STALE); }
        if (previous != null && (!previous.attributes().equals(point.attributes())
                || !point.observedAt().isAfter(previous.observedAt()))) { return unknown(Quality.OUT_OF_ORDER); }
        BigDecimal value = BigDecimal.valueOf(point.value());
        if (fact.type() == MetricType.COUNTER) {
            if (previous == null || previous.value() == null || point.value() < 0 || previous.value() < 0
                    || !Objects.equals(point.startTime(), previous.startTime())
                    || point.value() < previous.value()) { return unknown(Quality.RESET); }
            if (point.startTime().isAfter(previous.observedAt())
                    || previous.observedAt().plus(maxGap).isBefore(point.observedAt())) { return unknown(Quality.GAP); }
            Duration elapsed = Duration.between(previous.observedAt(), point.observedAt());
            BigDecimal seconds = BigDecimal.valueOf(elapsed.getSeconds()).add(BigDecimal.valueOf(elapsed.getNano(), 9));
            value = value.subtract(BigDecimal.valueOf(previous.value())).divide(seconds, MathContext.DECIMAL128);
        }
        BigDecimal threshold = firing ? rule.recover() : rule.trigger();
        int comparison = value.compareTo(threshold);
        // Trigger equality is healthy; recovery equality remains firing until it crosses the lower/upper boundary.
        boolean bad = rule.direction() == Direction.ABOVE ? comparison > 0 || firing && comparison == 0
                : comparison < 0 || firing && comparison == 0;
        return new Result(bad ? AlertSignal.Condition.BAD : AlertSignal.Condition.GOOD, Quality.VALID, value);
    }
    private static Result unknown(Quality quality) { return new Result(AlertSignal.Condition.UNKNOWN, quality, null); }
}
