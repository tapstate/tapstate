package io.tapstate.core.lifecycle;

import java.io.Serializable;

/** Computed upper bounds one execution reserves on one stable cluster member. */
public record ClusterCapacityDemand(
        long processors,
        long blockingProcessors,
        long writers,
        long connectorInstances,
        long bufferedRecords,
        long edgeQueueRecords) implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Known absence of occupancy; an unknown execution must not be represented by this value. */
    public static final ClusterCapacityDemand ZERO = new ClusterCapacityDemand(0, 0, 0, 0, 0, 0);

    public ClusterCapacityDemand {
        if (processors < 0 || blockingProcessors < 0 || writers < 0 || connectorInstances < 0
                || bufferedRecords < 0 || edgeQueueRecords < 0) {
            throw new IllegalArgumentException("capacity demand must not be negative");
        }
    }

    /** Adds known occupancy without silently wrapping a count into a smaller reservation. */
    public ClusterCapacityDemand plus(ClusterCapacityDemand other) {
        return new ClusterCapacityDemand(
                Math.addExact(processors, other.processors),
                Math.addExact(blockingProcessors, other.blockingProcessors),
                Math.addExact(writers, other.writers),
                Math.addExact(connectorInstances, other.connectorInstances),
                Math.addExact(bufferedRecords, other.bufferedRecords),
                Math.addExact(edgeQueueRecords, other.edgeQueueRecords));
    }
}
