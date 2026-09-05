package io.tapstate.app;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.function.SupplierEx;
import io.tapstate.adapters.pdk.ConnectorProvisioner;
import io.tapstate.adapters.pdk.PdkSinkPort;
import io.tapstate.core.model.PipelineNode;
import io.tapstate.spi.sink.DdlPolicy;
import io.tapstate.spi.sink.SinkConfig;
import io.tapstate.spi.sink.SinkWriter;
import io.tapstate.spi.sink.TargetTable;
import io.tapstate.spi.sink.WriteMode;
import io.tapstate.spi.store.KeyedStateStore;
import java.util.Map;
import java.util.Set;

/**
 * A serializable sink-writer factory carried onto the DAG: it holds only the resolved connector coordinates
 * of one serve.sync target and opens the connector on the member that runs the sink vertex. It exists
 * because the DAG builder wraps a bare {@link SupplierEx}, invoked member-side with no Jet context, yet the
 * connector provisioner it needs is not serializable and lives on the member - so this carries the
 * serializable coordinates and resolves the provisioner from the local member's user context, mirroring how
 * the SRS source resolves its coordination store member-side.
 *
 * <p>The pipeline node it writes for travels with those coordinates, so the connector opened member-side
 * files what it keeps for itself under the node that asked for the write rather than under nothing. It is
 * one of the serializable coordinates for that reason: it is resolved where the topology is built and there
 * is nothing member-side to re-derive it from.
 *
 * <p>The provisioner is expected under {@link #CONNECTOR_PROVISIONER_USER_CONTEXT_KEY}; a member with none
 * bound is not sink-capable and the open fails rather than silently dropping writes. Binding the provisioner
 * into the member user context is the assembly root's job when it makes the member sink-capable.
 */
final class PdkSinkWriterFactory implements SupplierEx<SinkWriter> {

    private static final long serialVersionUID = 1L;

    /**
     * The member user-context key under which the connector provisioner is bound, so a sink factory shipped
     * onto the DAG can resolve it member-side. The assembly layer binds the provisioner under this key when
     * it makes the member sink-capable.
     */
    static final String CONNECTOR_PROVISIONER_USER_CONTEXT_KEY = "tapstate.pdk.connector-provisioner";

    /**
     * The member user-context key under which the layer a connector's own notes are kept in is bound. It is
     * reached the same way and for the same reason as the provisioner: the factory crosses to another member
     * and a live store does not survive the crossing, so what travels is the node it writes for and the store
     * is picked up where the connector is actually opened. A member with none bound keeps a connector's notes
     * only for the life of the open, which is what every sink got before there was anywhere to file them.
     */
    static final String CONNECTOR_STATE_STORE_USER_CONTEXT_KEY = "tapstate.pdk.connector-state-store";

    private final String connectorId;
    private final Map<String, Object> settings;
    private final WriteMode writeMode;
    private final DdlPolicy ddl;
    private final Map<String, TargetTable> targets;
    private final PipelineNode node;

    PdkSinkWriterFactory(
            String connectorId, Map<String, Object> settings, WriteMode writeMode, DdlPolicy ddl, TargetTable target,
            PipelineNode node) {
        this(connectorId, settings, writeMode, ddl,
                target == null ? Map.<String, TargetTable>of() : Map.of(target.name(), target), node);
    }

    PdkSinkWriterFactory(
            String connectorId, Map<String, Object> settings, WriteMode writeMode, DdlPolicy ddl,
            Map<String, TargetTable> targets, PipelineNode node) {
        this.connectorId = connectorId;
        this.settings = settings;
        this.writeMode = writeMode;
        this.ddl = ddl;
        this.targets = targets == null ? Map.of() : Map.copyOf(targets);
        this.node = node;
    }

    /** The write-side models this factory will hand the sink, keyed by the stream each answers for. */
    Map<String, TargetTable> targets() {
        return targets;
    }

    /** The pipeline node this factory writes for, which scopes what the opened connector keeps for itself. */
    PipelineNode node() {
        return node;
    }

    @Override
    public SinkWriter getEx() {
        HazelcastInstance member = localMember();
        return new PdkSinkPort(provisioner(member), stateStore(member))
                .open(new SinkConfig(connectorId, settings, writeMode, ddl, null, node), targets);
    }

    /**
     * The layer this member keeps connector notes in, or null when none is bound. Null is a real answer and
     * not a failure: it is a run with no store behind it, and the connector then keeps its notes for the life
     * of the open. Unlike the provisioner, whose absence means the member cannot write at all, a missing store
     * costs only durability - so this does not refuse.
     */
    private static KeyedStateStore stateStore(HazelcastInstance member) {
        Object bound = member.getUserContext().get(CONNECTOR_STATE_STORE_USER_CONTEXT_KEY);
        return bound instanceof KeyedStateStore store ? store : null;
    }

    /** The connector provisioner bound onto the local member, or a bare failure when the member has none. */
    private static ConnectorProvisioner provisioner(HazelcastInstance member) {
        Object bound = member.getUserContext().get(CONNECTOR_PROVISIONER_USER_CONTEXT_KEY);
        if (!(bound instanceof ConnectorProvisioner provisioner)) {
            throw new IllegalStateException(
                    "no connector provisioner is bound in the member user context; the member is not sink-capable");
        }
        return provisioner;
    }

    /** The single embedded member in this process; more than one breaks the single-member run invariant. */
    private static HazelcastInstance localMember() {
        Set<HazelcastInstance> instances = Hazelcast.getAllHazelcastInstances();
        if (instances.size() != 1) {
            throw new IllegalStateException(
                    "expected exactly one local Hazelcast member on the sink member, found " + instances.size());
        }
        return instances.iterator().next();
    }
}
