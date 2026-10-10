package io.tapstate.control.core;

/** The same cluster recovery projection as cluster and pipeline readers consume it. */
public interface ClusterRecoveryQueries {
    ClusterRecoveryQueries NONE = new ClusterRecoveryQueries() {
        @Override public ClusterRecoveryView cluster() { return null; }
        @Override public ClusterPipelineRecoveryView pipeline(String pipelineId) { return null; }
    };

    ClusterRecoveryView cluster();
    ClusterPipelineRecoveryView pipeline(String pipelineId);
}
