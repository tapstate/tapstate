package io.tapstate.control.core;

import java.util.List;

/** One control-core recovery reading, shared by cluster readers without client-side state inference. */
public record ClusterRecoveryView(String clusterId, Boolean quorumReady, String recoveryState,
        List<String> causes, List<ClusterRecoveryItemView> items,
        ClusterRecoveryItemView.Profile currentProfile, ClusterRecoveryReadFailure profileUnavailable,
        ClusterRecoveryItemView.ClaimReading coordinatorClaim, ClusterRecoveryReadFailure claimUnavailable,
        ClusterCapacityView capacity, ClusterRecoveryReadFailure queueUnavailable) {
    public ClusterRecoveryView {
        causes = List.copyOf(causes);
        items = List.copyOf(items);
    }
}
