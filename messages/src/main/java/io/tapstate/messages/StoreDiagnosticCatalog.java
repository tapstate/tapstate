package io.tapstate.messages;

import io.tapstate.core.common.DiagnosticDefinitionLookup;
import io.tapstate.core.common.StoreErrorDefinitions;
import io.tapstate.core.common.ProductErrorModule;

/** Explicit runtime contracts; lifecycle behavior remains with its owner. */
public final class StoreDiagnosticCatalog {
    private StoreDiagnosticCatalog() {}

    private static final class Bundled {
        static final ProductDiagnosticCatalog INSTANCE = ProductDiagnosticCatalog.load(
                ProductErrorModule.STORE.number(), StoreErrorDefinitions.registered().stream()
                        .map(definition -> new ProductDiagnosticCatalog.Definition(definition.code(), definition.parameters()))
                        .toList(), language -> MessageCatalog.read("/messages/store-diagnostic-" + language + ".json"));
    }

    public static DiagnosticDefinitionLookup bundled() { return Bundled.INSTANCE; }
}
