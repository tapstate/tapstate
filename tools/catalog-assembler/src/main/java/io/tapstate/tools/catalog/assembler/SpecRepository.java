package io.tapstate.tools.catalog.assembler;

import java.util.Set;

import io.tapstate.core.catalog.ConnectorCatalogEntry;

/** Repository ownership for bundled rows whose provenance records only a relative spec path. */
enum SpecRepository {
    OSS,
    ENTERPRISE;

    // These bundled ids originate in the enterprise checkout, including the source-only preview.
    private static final Set<String> ENTERPRISE_IDS = Set.of("oracle", "sqlserver", "db2");

    static SpecRepository of(ConnectorCatalogEntry row) {
        return ENTERPRISE_IDS.contains(row.id()) ? ENTERPRISE : OSS;
    }
}
