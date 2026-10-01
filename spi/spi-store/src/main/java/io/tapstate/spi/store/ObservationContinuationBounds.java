package io.tapstate.spi.store;

import io.tapstate.core.lifecycle.CardinalityBudget;
import io.tapstate.core.lifecycle.MetricAttributes;
import io.tapstate.core.lifecycle.MetricPoint;

import java.util.EnumMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/** Private snapshots retain one owner's declared series, with counts derived from the shared dimensions. */
public final class ObservationContinuationBounds {
    private static final Map<CardinalityBudget, Set<String>> ATTRIBUTES = attributes();
    private ObservationContinuationBounds() { }

    public static int maximumFacts() { return CardinalityBudget.values().length; }

    public static Set<String> allowedAttributes(String instrument) { return ATTRIBUTES.get(budget(instrument)); }

    public static int maximumProducerGroups() {
        int groups = 0;
        for (CardinalityBudget budget : CardinalityBudget.values()) {
            Set<String> attributes = ATTRIBUTES.get(budget);
            groups += (attributes.contains(MetricAttributes.DIRECTION) ? MetricAttributes.DIRECTIONS.size() + 1 : 1)
                    * (attributes.contains(MetricAttributes.STAGE)
                            ? MetricAttributes.closedDomain(MetricAttributes.STAGE).orElseThrow().size() + 1 : 1);
        }
        return groups;
    }

    public static int maximumPoints(String instrument) {
        CardinalityBudget budget = budget(instrument);
        // A missing owner is a permitted unlabelled point; a labelled point belongs to the one encoded owner.
        int points = 2 * (budget.openDimension().isPresent() ? budget.distinctValues() + 2 : 2);
        for (String attribute : ATTRIBUTES.get(budget)) {
            Set<String> values = MetricAttributes.closedDomain(attribute).orElse(null);
            if (values != null) { points = Math.multiplyExact(points, values.size() + 1); }
        }
        return points;
    }

    public static void validatePoint(String instrument, MetricPoint point) {
        CardinalityBudget budget = budget(instrument);
        Set<String> declared = ATTRIBUTES.get(budget);
        for (var attribute : point.attributes().entrySet()) {
            if (!declared.contains(attribute.getKey())) {
                throw new IllegalArgumentException("a private continuation carries only its instrument's declared attributes");
            }
            Set<String> domain = MetricAttributes.closedDomain(attribute.getKey()).orElse(null);
            if (domain != null && !domain.contains(attribute.getValue())) {
                throw new IllegalArgumentException("a private continuation respects closed attribute values");
            }
        }
        if (point.attributes().containsKey(MetricAttributes.OVERFLOW)
                && (!"true".equals(point.attributes().get(MetricAttributes.OVERFLOW))
                        || budget.openDimension().filter(point.attributes()::containsKey).isPresent())) {
            throw new IllegalArgumentException("a private overflow marker replaces its folded open dimension");
        }
    }

    public static void validateGroup(String instrument, String direction, String stage) {
        Set<String> declared = ATTRIBUTES.get(budget(instrument));
        if (!direction.isEmpty() && !declared.contains(MetricAttributes.DIRECTION)
                || !stage.isEmpty() && !declared.contains(MetricAttributes.STAGE)) {
            throw new IllegalArgumentException("a private native group uses only its instrument's declared dimensions");
        }
    }

    private static CardinalityBudget budget(String instrument) {
        return CardinalityBudget.forInstrument(instrument).orElseThrow(() ->
                new IllegalArgumentException("a private continuation instrument declares its cardinality budget"));
    }

    private static Map<CardinalityBudget, Set<String>> attributes() {
        Map<CardinalityBudget, Set<String>> declared = new EnumMap<>(CardinalityBudget.class);
        for (CardinalityBudget budget : CardinalityBudget.values()) {
            Set<String> keys = new HashSet<>(Set.of(MetricAttributes.PIPELINE_ID, MetricAttributes.OVERFLOW));
            budget.openDimension().ifPresent(keys::add);
            switch (budget) {
                case RECORDS -> { keys.add(MetricAttributes.DIRECTION); keys.add(MetricAttributes.OP); }
                case BYTES -> keys.add(MetricAttributes.DIRECTION);
                case PROCESS_DURATION, PROCESS_ACTIVE, STAGE_QUEUE_DEPTH, STAGE_QUEUE_CAPACITY,
                        STAGE_QUEUE_HIGH_WATER, STAGE_OUTPUT_REFUSED, STAGE_OUTPUT_RETRY_DURATION -> keys.add(MetricAttributes.STAGE);
                case STATE_OPERATION_COUNT -> {
                    keys.add(MetricAttributes.STATE_OPERATION); keys.add(MetricAttributes.STATE_OUTCOME);
                }
                case STATE_OPERATION_DURATION, STATE_OPERATION_BYTES -> keys.add(MetricAttributes.STATE_OPERATION);
                case STATE_SERIALIZATION_COUNT, STATE_SERIALIZATION_BYTES -> keys.add(MetricAttributes.STATE_CODEC);
                case ROLLUP_CLOSED_THROUGH_AGE, ROLLUP_BUCKET_COMPUTED, ROLLUP_BUCKET_RETRIED, ROLLUP_BUCKET_FAILED,
                        ROLLUP_BUILD_RAW_FALLBACK, ROLLUP_QUERY_RAW_FALLBACK, ROLLUP_QUERY_FULL_RAW_FALLBACK,
                        ROLLUP_QUERY_BUCKET_DOWN_DRILLED -> keys.add(MetricAttributes.ROLLUP_RESOLUTION);
                case LIFECYCLE_WORK_DURATION -> keys.add(MetricAttributes.LIFECYCLE_VERB);
                case CONNECTOR_EXTERNAL_CALL_COUNT, CONNECTOR_EXTERNAL_CALL_DURATION -> {
                    keys.add(MetricAttributes.CONNECTOR_CALL); keys.add(MetricAttributes.CONNECTOR_OUTCOME);
                }
                default -> {
                    if (budget.instrument().startsWith("tapstate.process.telemetry.")) {
                        keys.add(MetricAttributes.TELEMETRY_SINK);
                    }
                }
            }
            declared.put(budget, Set.copyOf(keys));
        }
        return Map.copyOf(declared);
    }
}
