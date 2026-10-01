package io.tapstate.adapters.pdk;

import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.ConnectorRegistration;
import io.tapstate.spi.store.ConnectorRegistry;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Resolves a connector id against the distribution store and materializes it for loading: it finds the
 * id's registration, stages the registered artifact bytes into a content-addressed on-disk cache — a
 * file named for the content hash, written once and reused on every later resolve — and introspects the
 * staged jar into the {@link ConnectorRef} the bridge loads from. Staging is content-addressed, so the
 * bytes are fetched from the store only the first time a given artifact is seen.
 *
 * <p>An id that resolves to no registered artifact, or to more than one, is refused with a coded
 * connector-domain exception rather than loaded blindly: selecting among connector versions is out of
 * scope, and a silent wrong-version load is never taken. A registration whose bytes the store cannot
 * produce is a load failure. A raw I/O failure staging the artifact is not a connector defect and
 * surfaces as an unchecked I/O exception.
 */
public final class RegistryConnectorProvisioner implements ConnectorProvisioner {

    private final ConnectorRegistry registry;
    private final ConnectorIntrospector introspector;
    private final Path cacheDir;

    public RegistryConnectorProvisioner(
            ConnectorRegistry registry, ConnectorIntrospector introspector, Path cacheDir) {
        this.registry = Objects.requireNonNull(registry, "registry");
        this.introspector = Objects.requireNonNull(introspector, "introspector");
        this.cacheDir = Objects.requireNonNull(cacheDir, "cacheDir");
    }

    @Override
    public ConnectorRef resolve(String connectorId) {
        ConnectorRegistration registration = resolveRegistration(connectorId);
        Path staged = stage(connectorId, registration.contentHash());
        IntrospectedConnector introspected = introspector.introspect(List.of(staged));
        boolean shareSafe = registry.shareSafeCertification(connectorId, registration.contentHash()).isPresent();
        return new ConnectorRef(List.of(staged), introspected.className(), introspected.pdkApiVersion(), null,
                introspected.spec(), registration.contentHash(), shareSafe);
    }

    /** The single registration for the id, refusing with a code when none or more than one matches. */
    private ConnectorRegistration resolveRegistration(String connectorId) {
        List<ConnectorRegistration> matches = registry.list().stream()
                .filter(registration -> registration.connectorId().equals(connectorId))
                .toList();
        if (matches.isEmpty()) {
            throw new TapstateException(ConnectorError.NOT_REGISTERED, Map.of("connector", connectorId), null);
        }
        if (matches.size() > 1) {
            throw new TapstateException(ConnectorError.AMBIGUOUS_REGISTRATION,
                    Map.of("connector", connectorId, "artifacts", contentHashes(matches)), null);
        }
        return matches.get(0);
    }

    /**
     * The path to the artifact staged under its content hash: it writes the store's bytes into the cache
     * the first time the hash is seen and reuses the file every time after, so an already-staged hash is
     * never re-fetched. The write goes through a temp file moved into place, so a reader never sees a
     * half-written jar.
     */
    private Path stage(String connectorId, String contentHash) {
        Path target = cacheDir.resolve(contentHash + ".jar");
        if (Files.exists(target)) {
            return target;
        }
        byte[] bytes = registry.artifact(contentHash).orElseThrow(() ->
                new TapstateException(ConnectorError.LOAD_FAILED, Map.of("connector", connectorId), null));
        try {
            Files.createDirectories(cacheDir);
            Path tmp = Files.createTempFile(cacheDir, contentHash, ".tmp");
            try {
                Files.write(tmp, bytes);
                // Content-addressed: any concurrently-staged file under this hash holds identical bytes,
                // so replacing it is safe.
                Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            } finally {
                Files.deleteIfExists(tmp);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("staging connector artifact " + target, e);
        }
        return target;
    }

    private static String contentHashes(List<ConnectorRegistration> registrations) {
        return registrations.stream()
                .map(ConnectorRegistration::contentHash).sorted().collect(Collectors.joining(", "));
    }
}
