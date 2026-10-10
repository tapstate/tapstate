package io.tapstate.control.core;

import java.util.List;

/** The same item projection narrowed to the pipeline's current incarnation. */
public record ClusterPipelineRecoveryView(String pipelineId, String currentIncarnation,
        String recoveryState, List<String> causes, List<ClusterRecoveryItemView> items,
        ClusterRecoveryReadFailure unavailable, ClusterCapacityView capacity) {
    public ClusterPipelineRecoveryView {
        causes = List.copyOf(causes);
        items = List.copyOf(items);
    }
}
