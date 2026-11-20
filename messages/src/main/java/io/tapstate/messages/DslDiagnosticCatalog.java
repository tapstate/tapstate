package io.tapstate.messages;

import io.tapstate.core.common.DiagnosticDefinitionLookup;
import io.tapstate.core.common.DslErrorDefinitions;
import io.tapstate.core.common.ProductErrorModule;
import io.tapstate.core.common.JsonReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Explicit DSL definition registration, independent of parser and service implementations. */
public final class DslDiagnosticCatalog {
    private DslDiagnosticCatalog() {}

    private static final class Bundled {
        static final ProductDiagnosticCatalog CATALOG = ProductDiagnosticCatalog.load(
                ProductErrorModule.DSL.number(), DslErrorDefinitions.registered().stream()
                        .map(definition -> new ProductDiagnosticCatalog.Definition(definition.code(), definition.parameters()))
                        .toList(), language -> MessageCatalog.read("/messages/dsl-diagnostic-" + language + ".json"));
    }

    private static final class DisplayNames {
        static final Map<String, Map<String, String>> BY_LANGUAGE = load();

        private static Map<String, Map<String, String>> load() {
            var languages = new HashMap<String, Map<String, String>>();
            for (var language : List.of("en", "zh-CN", "zh-TW")) {
                var parsed = JsonReader.parseUnique(MessageCatalog.read(
                        "/messages/diagnostic-display-names-" + language + ".json"));
                if (!(parsed instanceof Map<?, ?> entries))
                    throw new IllegalStateException("invalid diagnostic display names");
                var names = new HashMap<String, String>();
                for (var entry : entries.entrySet()) {
                    if (!(entry.getKey() instanceof String key) || !(entry.getValue() instanceof String value)
                            || value.isBlank()) throw new IllegalStateException("invalid diagnostic display name");
                    names.put(key, value);
                }
                languages.put(language, Map.copyOf(names));
            }
            return Map.copyOf(languages);
        }
    }

    public static DiagnosticDefinitionLookup bundled() {
        return (code, named, positional, locale) -> {
            if (!(named.get("path") instanceof String path))
                return Bundled.CATALOG.resolve(code, named, positional, locale);
            var label = DisplayNames.BY_LANGUAGE.get(ProductDiagnosticCatalog.language(locale)).get(path);
            if (label == null) return Bundled.CATALOG.resolve(code, named, positional, locale);
            var display = new HashMap<>(named);
            display.put("path", label);
            return Bundled.CATALOG.resolve(code, display, positional, locale);
        };
    }
}
