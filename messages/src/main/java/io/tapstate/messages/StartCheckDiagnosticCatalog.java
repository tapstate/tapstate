package io.tapstate.messages;

import io.tapstate.core.common.DiagnosticDefinitionLookup;
import io.tapstate.core.common.StartCheckDefinitions;
import io.tapstate.core.common.ProductErrorModule;

/** Explicit start-check text; report behavior remains authoritative. */
public final class StartCheckDiagnosticCatalog {
    private StartCheckDiagnosticCatalog() {}

    private static final class Bundled {
        static final ProductDiagnosticCatalog INSTANCE = ProductDiagnosticCatalog.load(
                ProductErrorModule.START_CHECK.number(), StartCheckDefinitions.registered().stream()
                        .map(definition -> new ProductDiagnosticCatalog.Definition(definition.code(), definition.parameters()))
                        .toList(), language -> MessageCatalog.read("/messages/start-check-diagnostic-" + language + ".json"));
    }

    public static DiagnosticDefinitionLookup bundled() { return Bundled.INSTANCE; }
}
