package io.tapstate.control.core;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.SourceResource;
import io.tapstate.spi.store.ArtifactStore;
import io.tapstate.spi.store.SchemaStore;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * The Source-scoped read side of schema discovery. It reads the Source's latest matching connection
 * discovery, then projects it through the tables that Source declares. Connection schema reads remain
 * separate because they intentionally report every table discovery found.
 */
public final class SourceSchemaQueryService {

    private final ArtifactStore artifacts;
    private final SchemaStore schemas;

    public SourceSchemaQueryService(ArtifactStore artifacts, SchemaStore schemas) {
        this.artifacts = Objects.requireNonNull(artifacts, "artifacts");
        this.schemas = Objects.requireNonNull(schemas, "schemas");
    }

    /**
     * Returns the Source's latest selected discovery, or empty when it has never been discovered.
     * A missing Source is a coded Source-not-found error rather than an indistinguishable empty read.
     */
    public Optional<SchemaReport> find(String sourceId) {
        Objects.requireNonNull(sourceId, "sourceId");
        SourceResource source = artifacts.get(sourceId)
                .filter(SourceResource.class::isInstance)
                .map(SourceResource.class::cast)
                .orElseThrow(() -> new TapstateException(SourceError.NOT_FOUND, Map.of("id", sourceId), null));
        return schemas.get(source.id())
                .filter(discovered -> discovered.connectorId().equals(source.connector()))
                .map(discovered -> new SchemaReport(
                        discovered.connectionId(),
                        discovered.connectorId(),
                        SourceTableScope.select(source, discovered.model().tables()).stream()
                                .map(SchemaReport.Table::from)
                                .toList(),
                        discovered.discoveredAt()));
    }
}
