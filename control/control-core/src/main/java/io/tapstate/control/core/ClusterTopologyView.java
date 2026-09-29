package io.tapstate.control.core;

import java.util.List;
import java.util.Objects;

/**
 * What one control-plane node answers when asked what the cluster looks like.
 *
 * <p>Every node answers the same, because everything in here is either durable or asked of the engine,
 * which every member can ask equally. That is a promise the read face has to keep rather than a happy
 * accident: a reader who has to know which node they reached in order to read the answer has not been
 * given a cluster, they have been given a node.
 *
 * @param clusterId         the stable cluster identity; null on a build with no cluster behind it
 * @param topologyRevision  the committed membership revision these members were judged against, or null
 *                          when nothing is committed -- a single-node build, or a cluster whose
 *                          coordination store has not answered yet. Null is "cannot say", and a reader
 *                          must not print it as zero
 * @param members           every member the answering node sees, ordered by stable id
 * @param pipelines         every pipeline the cluster has been asked to run, ordered by id
 */
public record ClusterTopologyView(
        String clusterId,
        Long topologyRevision,
        List<ClusterMemberView> members,
        List<ClusterPipelineView> pipelines) {

    public ClusterTopologyView {
        members = List.copyOf(Objects.requireNonNull(members, "members"));
        pipelines = List.copyOf(Objects.requireNonNull(pipelines, "pipelines"));
    }
}
