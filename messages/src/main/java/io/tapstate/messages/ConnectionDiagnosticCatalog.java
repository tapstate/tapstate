package io.tapstate.messages;

import io.tapstate.core.common.DiagnosticCodeSource;
import io.tapstate.core.common.DiagnosticDefinitionLookup;
import io.tapstate.core.common.TapstateException;

import java.util.LinkedHashMap;
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
        TEST_EXECUTION_FAILED("TS0060001", "connector.test-failed", Set.of("connector", "detail"));

        private final String code;
        private final String legacy;
        private final Set<String> parameters;

        Definition(String code, String legacy, Set<String> parameters) {
            this.code = code;
            this.legacy = legacy;
            this.parameters = parameters;
        }

        public String code() { return code; }
        public String legacy() { return legacy; }
        public Set<String> parameters() { return parameters; }
    }

    private final Map<String, Definition> byLegacy;
    private final ProductDiagnosticCatalog catalog;

    private ConnectionDiagnosticCatalog(
            Map<String, Definition> byLegacy, ProductDiagnosticCatalog catalog) {
        this.byLegacy = byLegacy;
        this.catalog = catalog;
    }

    private static final class Bundled {
        static final ConnectionDiagnosticCatalog INSTANCE = load(MessageCatalog::read);
    }

    public static ConnectionDiagnosticCatalog bundled() {
        return Bundled.INSTANCE;
    }

    public Map<String, Object> snapshot(TapstateException failure, Locale locale) {
        String legacy = failure.code().code();
        return resolve(legacy, failure.args(), List.of(), locale)
                .<Map<String, Object>>map(rendered -> Map.of("code", rendered.code(), "params", failure.args(),
                        DiagnosticCodeSource.FIELD, DiagnosticCodeSource.TAPSTATE_STANDARD,
                        MESSAGE, rendered.message(), SOLUTION, rendered.solution()))
                .orElseGet(() -> Map.of("code", legacy, DiagnosticCodeSource.FIELD, DiagnosticCodeSource.TAPSTATE_LEGACY));
    }

    @Override public Optional<Resolved> resolve(String code, Map<String, Object> named, List<String> positional, Locale locale) {
        Definition definition = byLegacy.get(code);
        return definition == null ? Optional.empty()
                : catalog.resolve(definition.code(), named, positional, locale);
    }

    static String language(Locale locale) {
        return ProductDiagnosticCatalog.language(locale);
    }

    static ConnectionDiagnosticCatalog load(Function<String, String> readResource) {
        Map<String, Definition> byLegacy = new LinkedHashMap<>();
        for (Definition definition : Definition.values()) {
            if (byLegacy.putIfAbsent(definition.legacy(), definition) != null) {
                throw new IllegalStateException("invalid or repeated connection diagnostic definition " + definition.code());
            }
        }
        var registered = java.util.Arrays.stream(Definition.values())
                .map(definition -> new ProductDiagnosticCatalog.Definition(definition.code(), definition.parameters()))
                .toList();
        var catalog = ProductDiagnosticCatalog.load("006", registered,
                language -> readResource.apply("/messages/connection-diagnostic-" + language + ".json"));
        return new ConnectionDiagnosticCatalog(Map.copyOf(byLegacy), catalog);
    }
}
