package io.tapstate.app;

import io.tapstate.adapters.pdk.ConnectorError;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.ConnectorCatalogStore;
import io.tapstate.spi.store.ConnectorRegistration;
import io.tapstate.spi.store.ConnectorRegistry;
import io.tapstate.spi.store.RegistrationOutcome;
import io.tapstate.spi.store.RegistrationSource;

import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

/** Cloud registration is a CACHE catalog projection; executable bytes stay in the image. */
final class BundledConnectorRegistry implements ConnectorRegistry {
    private final ConnectorCatalogStore catalog;
    private final Path seedDir;

    BundledConnectorRegistry(ConnectorCatalogStore catalog, Path seedDir) {
        this.catalog = catalog;
        this.seedDir = seedDir;
    }

    @Override public RegistrationOutcome register(String id, String api, RegistrationSource source, byte[] artifact) {
        throw new TapstateException(ConnectorError.CLOUD_UPLOAD_DISABLED, java.util.Map.of(), null);
    }

    @Override public List<ConnectorRegistration> list() {
        return catalog.bundledRegistrations();
    }

    @Override public List<ConnectorRegistration> findAll(String connectorId) {
        return catalog.bundledRegistration(connectorId).stream().toList();
    }

    @Override public Optional<byte[]> artifact(String hash) {
        return Optional.empty();
    }

    @Override public boolean hasArtifact(String hash) {
        return catalog.bundledRegistrations().stream().anyMatch(registration ->
                registration.contentHash().equals(hash) && Files.isRegularFile(
                        seedDir.resolve(registration.connectorId() + "-connector.jar"), LinkOption.NOFOLLOW_LINKS));
    }
}
