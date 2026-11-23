package io.tapstate.messages;

import io.tapstate.core.common.DiagnosticDefinitionLookup;
import io.tapstate.core.common.ConnectorCatalogErrorDefinitions;
import io.tapstate.core.common.ProductErrorModule;

/** Explicit runtime contracts; lifecycle behavior remains with its owner. */
public final class ConnectorCatalogDiagnosticCatalog {
    private ConnectorCatalogDiagnosticCatalog() {}

    private static final class Bundled {
        static final ProductDiagnosticCatalog INSTANCE = ProductDiagnosticCatalog.load(
                ProductErrorModule.CONNECTOR_CATALOG.number(), ConnectorCatalogErrorDefinitions.registered().stream()
                        .map(definition -> new ProductDiagnosticCatalog.Definition(definition.code(), definition.parameters()))
                        .toList(), language -> MessageCatalog.read("/messages/connector-catalog-diagnostic-" + language + ".json"));
    }

    public static DiagnosticDefinitionLookup bundled() { return Bundled.INSTANCE; }
}
