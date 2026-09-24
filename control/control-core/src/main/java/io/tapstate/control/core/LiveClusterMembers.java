package io.tapstate.control.core;

import java.util.List;

/**
 * The cluster's live member list. A port, because the member list is the one part of the topology that
 * only the engine layer can see, and this ring does not link the engine.
 *
 * <p>Live, and therefore never stored: who is up right now is answered by asking, and a copy of it in a
 * database is a copy of who was up when somebody last wrote. What is durable about a node -- its stable
 * id, its advertised control URL, the claims it holds -- is durable elsewhere and joined onto this.
 */
@FunctionalInterface
public interface LiveClusterMembers {

    /** Every member this node currently sees, in whatever order the engine reports them. */
    List<LiveClusterMember> members();

    /** A build with no cluster behind it, which sees nobody rather than pretending to see itself. */
    static LiveClusterMembers none() {
        return List::of;
    }
}
