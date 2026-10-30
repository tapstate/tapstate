package io.tapstate.adapters.pdk;

import io.tapstate.spi.store.CapabilityDeriver;
import io.tapstate.spi.store.ConnectorCapabilities;
import io.tapdata.pdk.apis.entity.Capability;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;

import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * The PDK implementation of the capability-derivation port: it provisions a connector, refuses it with
 * a code if it will not load or its declared API level is incompatible, and reads back the capabilities
 * its {@code registerCapabilities} registered. Opening a connector already runs registerCapabilities,
 * which only stores function references — so the deriver reads the registered ids straight off the
 * opened handle and never inits the connector or opens a connection. The PDK types stay inside this
 * class and neither the port nor the model carries any of them.
 */
public final class PdkCapabilityDeriver implements CapabilityDeriver {

    private final ConnectorProvisioner provisioner;

    public PdkCapabilityDeriver(ConnectorProvisioner provisioner) {
        this.provisioner = provisioner;
    }

    @Override
    public ConnectorCapabilities derive(String connectorId) {
        return derive(connectorId,provisioner.resolve(connectorId));
    }

    /** Recovery preflight inspects the exact registered artifact without opening a source or target connection. */
    public ConnectorCapabilities derive(String connectorId,String contentHash) {
        return derive(connectorId,provisioner.resolve(connectorId,contentHash));
    }

    private ConnectorCapabilities derive(String connectorId,ConnectorRef original) {
        PdkConnector connector = PdkConnector.open(connectorId,original,Map.of());
        try {
            return toCapabilities(connector.functions());
        } finally {
            connector.close();
        }
    }

    private static ConnectorCapabilities toCapabilities(ConnectorFunctions functions) {
        Set<String> ids = new TreeSet<>();
        for (Capability capability : functions.getCapabilities()) {
            ids.add(capability.getId());
        }
        return new ConnectorCapabilities(ids);
    }
}
