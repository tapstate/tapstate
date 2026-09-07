package io.tapstate.tools.catalog.derive;

import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.pdk.apis.TapConnector;
import io.tapdata.pdk.apis.entity.Capability;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Path;
import java.util.Set;
import java.util.TreeSet;

/**
 * Derives a connector's capability bitmap by classloading it from a jar and running
 * {@code registerCapabilities} against a fresh {@code ConnectorFunctions}, offline and without
 * opening any connection. The result is the set of registered capability ids (the snake_case
 * function field names, e.g. {@code batch_read_function}).
 *
 * <p>registerCapabilities only stores function references — the actual reads/writes those
 * functions perform run later, during a real task — so calling it on a just-constructed connector
 * is side-effect free.
 */
public final class ConnectorCapabilityProbe {

    private ConnectorCapabilityProbe() {
    }

    /**
     * Classloads {@code connectorClassName} from {@code connectorJar} and returns the capability
     * ids it registers in {@code registerCapabilities}.
     */
    public static Set<String> probe(Path connectorJar, String connectorClassName) {
        URL jarUrl = toUrl(connectorJar);
        // Parent-first URLClassLoader: PDK API types (TapConnector, ConnectorFunctions, ...) resolve
        // from this host classloader, so the connector binds to OUR ConnectorFunctions instance and
        // the capabilities it registers are visible here; only the connector impl classes (and its
        // bundled libraries) come from the jar. This also transparently follows the inheritance chain
        // for cloud-variant connectors that extend a concrete connector without overriding the method.
        try (URLClassLoader loader = new URLClassLoader(new URL[]{jarUrl}, ConnectorCapabilityProbe.class.getClassLoader())) {
            Class<?> connectorClass = Class.forName(connectorClassName, true, loader);
            TapConnector connector = (TapConnector) connectorClass.getDeclaredConstructor().newInstance();

            ConnectorFunctions functions = new ConnectorFunctions();
            // Discarded on purpose, unlike where rows are read: this asks the connector only which
            // capabilities it offers, and no value ever passes through here to be converted.
            connector.registerCapabilities(functions, new TapCodecsRegistry());

            Set<String> capabilities = new TreeSet<>();
            for (Capability capability : functions.getCapabilities()) {
                capabilities.add(capability.getId());
            }
            return capabilities;
        } catch (ReflectiveOperationException | IOException e) {
            throw new IllegalStateException(
                    "probing connector " + connectorClassName + " in " + connectorJar, e);
        }
    }

    private static URL toUrl(Path jar) {
        try {
            return jar.toUri().toURL();
        } catch (java.net.MalformedURLException e) {
            throw new IllegalStateException("bad connector jar path " + jar, e);
        }
    }
}
