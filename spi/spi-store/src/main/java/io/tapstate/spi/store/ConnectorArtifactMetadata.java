package io.tapstate.spi.store;

import io.tapstate.core.catalog.ConnectorCatalogEntry;

import java.util.Objects;
import java.util.Optional;

/** Catalog data belonging to one exact connector artifact. */
public record ConnectorArtifactMetadata(ConnectorCatalogEntry catalog, String sha256, String version,
                                        String pdkApiVersion, Origin origin, long sizeBytes,
                                        Optional<BundledConnectorIcon> icon) {
    public enum Origin {
        DOWNLOAD, UPLOAD, DROP_DIRECTORY, LEGACY_GRIDFS, BUNDLE
    }

    public ConnectorArtifactMetadata {
        Objects.requireNonNull(catalog, "catalog");
        Objects.requireNonNull(origin, "origin");
        Objects.requireNonNull(icon, "icon");
        if (!sha256.matches("[0-9a-f]{64}") || version.isBlank() || pdkApiVersion.isBlank() || sizeBytes < 1) {
            throw new IllegalArgumentException("Invalid connector artifact metadata");
        }
    }
}
