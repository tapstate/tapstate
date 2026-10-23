package io.tapstate.adapters.pdk;

/**
 * Resolves a connector id to the {@link ConnectorRef} that says where to load it from. The capture and
 * sink ports are constructed with one; the resolution itself — the seed directory today, the connector
 * distribution store later — is out of this module's scope.
 */
@FunctionalInterface
public interface ConnectorProvisioner {

    /** The ref for {@code connectorId}, or throws if the id resolves to no connector. */
    ConnectorRef resolve(String connectorId);

    /** Internal retained-context resolution; never substitute the currently registered artifact. */
    default ConnectorRef resolve(String connectorId, String contentHash) {
        if (contentHash == null || !contentHash.matches("[a-f0-9]{64}")) {
            throw new io.tapstate.core.common.TapstateException(ConnectorError.LOAD_FAILED,
                    java.util.Map.of("connector", connectorId), null);
        }
        ConnectorRef ref = resolve(connectorId);
        if (!java.util.Objects.equals(contentHash, ref.contentHash())) {
            throw new io.tapstate.core.common.TapstateException(ConnectorError.LOAD_FAILED,
                    java.util.Map.of("connector", connectorId), null);
        }
        return ref;
    }

    /** A stable, non-secret version identity for cache keys; implementations may avoid loading the connector. */
    default String cacheIdentity(String connectorId) {
        ConnectorRef connector = resolve(connectorId);
        return String.join("\n",
                connector.className(),
                connector.pdkApiVersion() == null ? "" : connector.pdkApiVersion(),
                connector.requiredLevel() == null ? "" : connector.requiredLevel(),
                connector.spec() == null ? "" : connector.spec(),
                connector.classpath().stream().map(path -> path.toAbsolutePath().normalize().toString())
                        .sorted().collect(java.util.stream.Collectors.joining("\n")));
    }
}
