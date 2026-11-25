package io.tapstate.adapters.pdk;

import io.tapstate.spi.store.RequiredNoteOrigin;
import io.tapstate.spi.store.RequiredNoteScope;
import io.tapstate.spi.store.RequiredNoteStore;

/**
 * Resolves a connector id to the {@link ConnectorRef} that says where to load it from. The capture and
 * sink ports are constructed with one; the resolution itself — the seed directory today, the connector
 * distribution store later — is out of this module's scope.
 */
@FunctionalInterface
public interface ConnectorProvisioner {

    /** The ref for {@code connectorId}, or throws if the id resolves to no connector. */
    ConnectorRef resolve(String connectorId);

    /** Narrow default-Source resolution preserves implementation defects for UNKNOWN diagnostics. */
    default ConnectorRef resolveDefaultTest(String connectorId) { return resolve(connectorId); }

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

    /** Checks a member's current readiness, optionally requiring the exact retained hash. */
    default ConnectorRef resolveForReadiness(String connectorId, String retainedHash) {
        return retainedHash == null ? resolve(connectorId) : resolve(connectorId, retainedHash);
    }

    /** Resolves one startup artifact while retaining ordinary selection and exact-hash rules. */
    default ConnectorRef resolveStartup(String connectorId) { return resolve(connectorId); }

    default ConnectorRef resolveStartup(String connectorId, String contentHash) { return resolve(connectorId, contentHash); }

    /** Opens the original artifact authorized by this durable note origin. */
    default ConnectorRef resolveRetained(RequiredNoteStore notes, RequiredNoteScope scope, RequiredNoteOrigin origin) {
        return resolve(origin.connectorId(), origin.artifactHash());
    }

    /** Reads metadata for an exact locally installed artifact without opening its classloader. */
    default java.util.Optional<String> artifactVersion(String connectorId, String sha256) {
        return java.util.Optional.empty();
    }

    /** Serializes retaining exact local bytes and publishing a durable reference against reclamation. */
    default <T> T withArtifactRetention(java.util.function.Supplier<T> publication) {
        return publication.get();
    }

    /** Keeps a persistent copy before publishing a new durable binding. */
    default void retain(ConnectorRef ref) {}

    /** Records that the authorized binding can now recover from its local artifact. */
    default void markBound(RequiredNoteStore notes, RequiredNoteScope scope, RequiredNoteOrigin origin) {}

    /** Resolves only the connector needed by the current Capture startup. */
    default ConnectorRef resolveCaptureStartup(String connectorId) { return resolve(connectorId); }

    /** Rechecks a captured registration at physical open; other implementations retain full resolution. */
    default ConnectorRef revalidateCaptureStartup(String connectorId, ConnectorRef captured) {
        return resolve(connectorId, captured.contentHash());
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
