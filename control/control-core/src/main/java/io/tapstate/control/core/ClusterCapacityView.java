package io.tapstate.control.core;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/** Occupancy and its exact profile snapshot; unavailable demand is never represented as zero. */
public record ClusterCapacityView(String availability, String provenance,
        ClusterRecoveryItemView.Profile profile, ClusterResourceCounts configuredLimits,
        Map<String, ClusterResourceCounts> occupiedByNode, ClusterRecoveryReadFailure unavailable) {
    public ClusterCapacityView {
        if (occupiedByNode != null) {
            occupiedByNode = Collections.unmodifiableMap(new LinkedHashMap<>(occupiedByNode));
        }
    }
}
