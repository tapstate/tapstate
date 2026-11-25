package io.tapstate.adapters.pdk;

import io.tapstate.core.model.PipelineNode;
import io.tapstate.spi.store.RequiredNoteOrigin;
import io.tapstate.spi.store.RequiredNoteScope;
import io.tapstate.spi.store.WorkloadClaimFence;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * The sink connectors a member shares, one per sink node and artifact: the first writer of a sink to open
 * starts the instance, every later writer of that sink on this member uses it too, and the last one to close
 * stops it.
 *
 * <p>Only an artifact certified for it is shared - one whose single instance was shown to serve several writers
 * at once, through the whole of its lifecycle. Every other artifact runs an instance per writer, and so does a
 * write that names no sink. Nothing is shared across members: each member keeps its own.
 */
public final class SharedSinkConnectors {

    private final Map<Key, Lease> leases = new HashMap<>();

    /**
     * The connector for {@code node}'s writers of {@code ref}'s artifact, started by {@code start} if no writer
     * of that sink holds one on this member yet. Every call is matched by one {@link #release}.
     */
    synchronized PdkConnector acquire(PipelineNode node, String connectorId, ConnectorRef ref,
            Supplier<PdkConnector> start) {
        return acquire(node, PipelineNode.Purpose.SYNC, connectorId, ref, start);
    }

    synchronized PdkConnector acquire(PipelineNode node, PipelineNode.Purpose purpose, String connectorId,
            ConnectorRef ref, Supplier<PdkConnector> start) {
        return acquire(node, purpose, connectorId, ref, null, start);
    }

    synchronized PdkConnector acquire(PipelineNode node, PipelineNode.Purpose purpose, String connectorId,
            ConnectorRef ref, NoteIdentity notes, Supplier<PdkConnector> start) {
        Key key = new Key(node, purpose, connectorId, ref.contentHash(), notes);
        Lease lease = leases.get(key);
        if (lease == null) {
            lease = new Lease(start.get());
            leases.put(key, lease);
        }
        lease.holders++;
        return lease.connector;
    }

    /** Lets go of one hold on {@code node}'s connector, stopping and closing it with the last. */
    synchronized void release(PipelineNode node, String connectorId, ConnectorRef ref) {
        release(node, PipelineNode.Purpose.SYNC, connectorId, ref);
    }

    synchronized void release(PipelineNode node, PipelineNode.Purpose purpose, String connectorId, ConnectorRef ref) {
        release(node, purpose, connectorId, ref, null);
    }

    synchronized void release(PipelineNode node, PipelineNode.Purpose purpose, String connectorId, ConnectorRef ref,
            NoteIdentity notes) {
        Key key = new Key(node, purpose, connectorId, ref.contentHash(), notes);
        Lease lease = leases.get(key);
        if (lease == null) {
            throw new IllegalStateException("released a shared connector of " + node + " that nothing holds");
        }
        if (--lease.holders == 0) {
            leases.remove(key);
            lease.connector.stopQuietly();
            lease.connector.close();
        }
    }

    /** How many writers hold {@code node}'s shared connector of {@code ref}'s artifact on this member. */
    synchronized int holders(PipelineNode node, String connectorId, ConnectorRef ref) {
        return holders(node, PipelineNode.Purpose.SYNC, connectorId, ref);
    }

    synchronized int holders(PipelineNode node, PipelineNode.Purpose purpose, String connectorId, ConnectorRef ref) {
        Lease lease = leases.get(new Key(node, purpose, connectorId, ref.contentHash(), null));
        return lease == null ? 0 : lease.holders;
    }

    /** Existing authority and exact configured facts, never a new lease or a latest-context lookup. */
    record NoteIdentity(WorkloadClaimFence execution, RequiredNoteScope primary, RequiredNoteScope global,
            RequiredNoteOrigin origin) {
        NoteIdentity {
            Objects.requireNonNull(execution, "execution");
            Objects.requireNonNull(primary, "primary");
            Objects.requireNonNull(global, "global");
            Objects.requireNonNull(origin, "origin");
        }
    }

    private record Key(PipelineNode node, PipelineNode.Purpose purpose, String connectorId, String contentHash,
            NoteIdentity notes) {
        Key {
            Objects.requireNonNull(node, "node");
            if (purpose != PipelineNode.Purpose.VIEW && purpose != PipelineNode.Purpose.SYNC) {
                throw new IllegalArgumentException("sink purpose must be view or sync");
            }
            Objects.requireNonNull(connectorId, "connectorId");
            Objects.requireNonNull(contentHash, "contentHash");
        }
    }

    private static final class Lease {

        private final PdkConnector connector;
        private int holders;

        Lease(PdkConnector connector) {
            this.connector = connector;
        }
    }
}
