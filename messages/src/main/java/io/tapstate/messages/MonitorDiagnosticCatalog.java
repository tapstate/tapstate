package io.tapstate.messages;

import io.tapstate.core.common.DiagnosticDefinitionLookup;
import io.tapstate.core.common.MonitorErrorDefinitions;
import io.tapstate.core.common.ProductErrorModule;

/** Explicit runtime contracts; lifecycle behavior remains with its owner. */
public final class MonitorDiagnosticCatalog {
    private MonitorDiagnosticCatalog() {}

    private static final class Bundled {
        static final ProductDiagnosticCatalog INSTANCE = ProductDiagnosticCatalog.load(
                ProductErrorModule.MONITOR.number(), MonitorErrorDefinitions.registered().stream()
                        .map(definition -> new ProductDiagnosticCatalog.Definition(definition.code(), definition.parameters()))
                        .toList(), language -> MessageCatalog.read("/messages/monitor-diagnostic-" + language + ".json"));
    }

    public static DiagnosticDefinitionLookup bundled() { return Bundled.INSTANCE; }
}
