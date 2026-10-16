package io.tapstate.core.lifecycle;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** Deployment ceilings shared by ordinary starts and recovery reservations on one member. */
public record ClusterCapacityLimits(
        long processors,
        long blockingProcessors,
        long writers,
        long connectorInstances,
        long bufferedRecords,
        long edgeQueueRecords) implements Serializable {

    private static final long serialVersionUID = 1L;

    public ClusterCapacityLimits {
        if (processors < 1 || blockingProcessors < 1 || writers < 1 || connectorInstances < 1
                || bufferedRecords < 1 || edgeQueueRecords < 1) {
            throw new IllegalArgumentException("every cluster capacity ceiling must be positive");
        }
    }

    /** Every ceiling exceeded by existing executions plus the proposed reservation, in stable order. */
    public List<Violation> violations(ClusterCapacityDemand occupied, ClusterCapacityDemand requested) {
        Objects.requireNonNull(occupied, "occupied");
        Objects.requireNonNull(requested, "requested");
        List<Violation> refused = new ArrayList<>();
        check(refused, "processors", occupied.processors(), requested.processors(), processors);
        check(refused, "blocking-processors", occupied.blockingProcessors(), requested.blockingProcessors(),
                blockingProcessors);
        check(refused, "writers", occupied.writers(), requested.writers(), writers);
        check(refused, "connector-instances", occupied.connectorInstances(), requested.connectorInstances(),
                connectorInstances);
        check(refused, "buffered-records", occupied.bufferedRecords(), requested.bufferedRecords(), bufferedRecords);
        check(refused, "edge-queue-records", occupied.edgeQueueRecords(), requested.edgeQueueRecords(), edgeQueueRecords);
        return List.copyOf(refused);
    }

    private static void check(List<Violation> refused, String resource, long occupied, long requested, long limit) {
        // Subtract only after proving occupied fits, so even a sum greater than Long.MAX_VALUE is refused.
        if (occupied > limit || requested > limit - occupied) {
            refused.add(new Violation(resource, occupied, requested, limit));
        }
    }

    /** A concrete admission refusal; its canonical error code is supplied by the actuation surface. */
    public record Violation(String resource, long occupied, long requested, long limit) implements Serializable {
        private static final long serialVersionUID = 1L;
    }
}
