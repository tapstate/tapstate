package io.tapstate.app;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.function.SupplierEx;
import io.tapstate.adapters.pdk.ConnectorProvisioner;
import io.tapstate.adapters.pdk.PdkSinkPort;
import io.tapstate.spi.sink.DdlPolicy;
import io.tapstate.spi.sink.SinkConfig;
import io.tapstate.spi.sink.SinkWriter;
import io.tapstate.spi.sink.TargetTable;
import io.tapstate.spi.sink.WriteMode;
import java.util.LinkedHashMap;
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

    private final String connectorId;
    private final Map<String, Object> settings;
    private final WriteMode writeMode;
    private final DdlPolicy ddl;
    private final Map<String, TargetTable> targets;

    PdkSinkWriterFactory(
            String connectorId, Map<String, Object> settings, WriteMode writeMode, DdlPolicy ddl, TargetTable target) {
        this.connectorId = connectorId;
        this.settings = settings;
        this.writeMode = writeMode;
        this.ddl = ddl;
        this.targets = target == null ? Map.of() : Map.of(target.name(), target);
    }

    PdkSinkWriterFactory(
            String connectorId, Map<String, Object> settings, WriteMode writeMode, DdlPolicy ddl,
            Map<String, TargetTable> targets) {
        this.connectorId = connectorId;
        this.settings = settings;
        this.writeMode = writeMode;
        this.ddl = ddl;
        this.targets = targets == null ? Map.of() : Map.copyOf(new LinkedHashMap<>(targets));
    }

    @Override
    public SinkWriter getEx() {
        ConnectorProvisioner provisioner = provisioner(localMember());
        return new PdkSinkPort(provisioner).open(new SinkConfig(connectorId, settings, writeMode, ddl), targets);
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
