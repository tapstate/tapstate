package io.tapstate.cli;

import java.util.List;

/**
 * The outcome of a remote {@code GET /api/cluster/members}. Either the cluster answered with its members,
 * the read was refused with a coded reason, or the server could not be reached. Sealed so the caller
 * renders each branch without try/catch, mirroring the never-throw seam.
 */
sealed interface ClusterMembersOutcome {

    /**
     * The cluster as the answering member sees it.
     *
     * @param clusterId        the stable cluster identity, or null when the server did not say
     * @param topologyRevision the committed membership revision, or null when nothing is committed --
     *                         which is not the same as revision zero and must not be printed as one
     */
    record Listed(String clusterId, Long topologyRevision, List<RemoteClusterMember> members)
            implements ClusterMembersOutcome {
        public Listed {
            members = List.copyOf(members);
        }
    }

    /** The server refused the read with a coded reason already rendered to a message. */
    record Rejected(String code, String message) implements ClusterMembersOutcome {
    }

    /** The server could not be reached (connection refused, timeout, or a malformed target). */
    record Unreachable() implements ClusterMembersOutcome {
    }
}
