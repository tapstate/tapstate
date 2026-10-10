package io.tapstate.adapters.pdk;

import io.tapstate.core.logging.LogSink;
import io.tapstate.core.model.PipelineNode;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * The sink connectors a member shares, one per sink node, artifact and frozen execution owner: the first
 * writer to open starts the instance, every later writer with those same coordinates uses it too, and the
 * last one to close stops it.
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
        return acquire(node, connectorId, ref, null, start);
    }

    /** Acquires only the connector belonging to this exact captured owner; null keeps legacy sharing. */
    synchronized PdkConnector acquire(PipelineNode node, String connectorId, ConnectorRef ref,
            LogSink.Scope scope, Supplier<PdkConnector> start) {
        Key key = new Key(node, connectorId, ref.contentHash(), scope);
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
        release(node, connectorId, ref, null);
    }

    /** Releases the same owner's hold acquired earlier, without touching a successor's connector. */
    synchronized void release(PipelineNode node, String connectorId, ConnectorRef ref, LogSink.Scope scope) {
        Key key = new Key(node, connectorId, ref.contentHash(), scope);
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
        return holders(node, connectorId, ref, null);
    }

    synchronized int holders(PipelineNode node, String connectorId, ConnectorRef ref, LogSink.Scope scope) {
        Lease lease = leases.get(new Key(node, connectorId, ref.contentHash(), scope));
        return lease == null ? 0 : lease.holders;
    }

    private record Key(PipelineNode node, String connectorId, String contentHash, LogSink.Scope scope) {
        Key {
            Objects.requireNonNull(node, "node");
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
