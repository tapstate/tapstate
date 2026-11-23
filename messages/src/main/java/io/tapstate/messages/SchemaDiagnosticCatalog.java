package io.tapstate.messages;

import io.tapstate.core.common.DiagnosticDefinitionLookup;
import io.tapstate.core.common.SchemaErrorDefinitions;
import io.tapstate.core.common.ProductErrorModule;

/** Explicit schema advisory definitions, resolved without legacy identity translation. */
public final class SchemaDiagnosticCatalog {
    private SchemaDiagnosticCatalog() {}

    private static final class Bundled {
        static final ProductDiagnosticCatalog INSTANCE = ProductDiagnosticCatalog.load(
                ProductErrorModule.SCHEMA.number(), SchemaErrorDefinitions.registered().stream()
                        .map(definition -> new ProductDiagnosticCatalog.Definition(definition.code(), definition.parameters()))
                        .toList(), language -> MessageCatalog.read("/messages/schema-diagnostic-" + language + ".json"));
    }

    public static DiagnosticDefinitionLookup bundled() { return Bundled.INSTANCE; }
}
