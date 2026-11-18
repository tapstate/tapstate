package io.tapstate.messages;

import io.tapstate.core.common.DiagnosticCodeSource;
import io.tapstate.core.common.DiagnosticDefinitionLookup;
import io.tapstate.core.common.ProductErrorModule;
import io.tapstate.core.common.TapstateException;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

/** Product definitions used by the connection diagnostic projection. */
public final class ConnectionDiagnosticCatalog implements DiagnosticDefinitionLookup {
    private static final String MESSAGE = "message";
    private static final String SOLUTION = "solution";

    public enum Definition {
        TEST_EXECUTION_FAILED(io.tapstate.core.common.ConnectorErrorDefinitions.TEST_FAILED);

        private final io.tapstate.core.common.ProductErrorDefinition definition;

        Definition(io.tapstate.core.common.ProductErrorDefinition definition) {
            this.definition = definition;
        }

        public String code() { return definition.code(); }
        public Set<String> parameters() { return definition.parameters(); }
    }

    private final ProductDiagnosticCatalog catalog;

    private ConnectionDiagnosticCatalog(ProductDiagnosticCatalog catalog) {
        this.catalog = catalog;
    }

    private static final class Bundled {
        static final ConnectionDiagnosticCatalog INSTANCE = load(MessageCatalog::read);
    }

    public static ConnectionDiagnosticCatalog bundled() {
        return Bundled.INSTANCE;
    }

    public Map<String, Object> snapshot(TapstateException failure, Locale locale) {
        String code = failure.code().code();
        return resolve(code, failure.args(), List.of(), locale)
                .<Map<String, Object>>map(rendered -> Map.of("code", rendered.code(), "params", failure.args(),
                        DiagnosticCodeSource.FIELD, DiagnosticCodeSource.TAPSTATE_STANDARD,
                        MESSAGE, rendered.message(), SOLUTION, rendered.solution()))
                .orElseGet(() -> Map.of("code", code, DiagnosticCodeSource.FIELD, ProductErrorModule.isRegistered(code)
                        ? DiagnosticCodeSource.TAPSTATE_STANDARD : DiagnosticCodeSource.TAPSTATE_LEGACY));
    }

    @Override public Optional<Resolved> resolve(String code, Map<String, Object> named, List<String> positional, Locale locale) {
        return catalog.resolve(code, named, positional, locale);
    }

    static String language(Locale locale) {
        return ProductDiagnosticCatalog.language(locale);
    }

    static ConnectionDiagnosticCatalog load(Function<String, String> readResource) {
        var registered = io.tapstate.core.common.ConnectorErrorDefinitions.registered().stream()
                .map(definition -> new ProductDiagnosticCatalog.Definition(definition.code(), definition.parameters()))
                .toList();
        var catalog = ProductDiagnosticCatalog.load("006", registered,
                language -> readResource.apply("/messages/connector-diagnostic-" + language + ".json"));
        return new ConnectionDiagnosticCatalog(catalog);
    }
}
