package io.tapstate.adapters.pdk;

import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.pdk.apis.TapConnector;
import io.tapdata.pdk.apis.entity.Capability;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;

import java.util.Set;
import java.util.TreeSet;

/**
 * Runs a connector's {@code registerCapabilities} live, inside its isolated loader, and reads back
 * the capability ids it registers (the snake_case function field names, e.g. {@code
 * batch_read_function}).
 *
 * <p>This is the runtime form of the offline catalog build: the connector is loaded through a
 * {@link ConnectorClassLoader}, so its implementation classes stay isolated while the frozen PDK
 * contract ({@code ConnectorFunctions} / {@code Capability}) is the shared host type the harness
 * constructs and reads. {@code registerCapabilities} only stores function references — the reads and
 * writes those functions perform run later, during a real task — so probing a just-constructed
 * connector opens no connection and starts no thread. Only the registered ids are read; the
 * connector's lifecycle (init / stop / discover / read / write) is never invoked.
 *
 * <p>The connector's own loader is installed as the thread context loader for the probe, so PDK
 * lookups that consult the context loader resolve against the connector's classpath rather than the
 * host. Constructing a real connector can also initialize the PDK runtime, which is not part of the
 * frozen host contract; the caller is responsible for making that runtime reachable, since a
 * connector dist jar is a thin plugin that does not bundle it.
 *
 * <p>The ids are the raw PDK vocabulary; mapping them to source modes and sink capability is the
 * catalog's job, not the bridge's — the bridge reports what the connector registered and invents
 * nothing.
 */
public final class CapabilityHarness {

    private CapabilityHarness() {
    }

    /**
     * Loads {@code connectorClassName} through {@code loader}, runs its {@code registerCapabilities}
     * against a fresh {@code ConnectorFunctions}, and returns the registered capability ids, sorted.
     */
    public static Set<String> deriveCapabilities(ConnectorClassLoader loader, String connectorClassName) {
        ClassLoader restore = Thread.currentThread().getContextClassLoader();
        try {
            Class<? extends TapConnector> connectorClass = loader.loadConnectorClass(connectorClassName);
            // Instantiation runs the connector's base-class initialization; run it under the
            // connector's own loader so any context-loader-based PDK lookup resolves against the
            // connector's classpath rather than the host.
            Thread.currentThread().setContextClassLoader(connectorClass.getClassLoader());
            TapConnector connector = connectorClass.getDeclaredConstructor().newInstance();

            ConnectorFunctions functions = new ConnectorFunctions();
            // Discarded on purpose, unlike where rows are read: this asks the connector only which
            // capabilities it offers, and no value ever passes through here to be converted.
            connector.registerCapabilities(functions, new TapCodecsRegistry());

            Set<String> capabilities = new TreeSet<>();
            for (Capability capability : functions.getCapabilities()) {
                capabilities.add(capability.getId());
            }
            return capabilities;
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("deriving capabilities of " + connectorClassName, e);
        } finally {
            Thread.currentThread().setContextClassLoader(restore);
        }
    }
}
