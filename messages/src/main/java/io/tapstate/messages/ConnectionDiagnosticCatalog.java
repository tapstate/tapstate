package io.tapstate.messages;

import io.tapstate.core.common.JsonReader;
import io.tapstate.core.common.DiagnosticDefinitionLookup;
import io.tapstate.core.common.TapstateException;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Pattern;

/** Product definitions used by the connection diagnostic projection. */
public final class ConnectionDiagnosticCatalog implements DiagnosticDefinitionLookup {
    private static final Pattern PARAMETER = Pattern.compile("\\{([a-z][A-Za-z0-9]*)}");
    private static final String ENGLISH = "en";
    private static final String MESSAGE = "message";
    private static final String SOLUTION = "solution";
    private static final List<String> LANGUAGES = List.of(ENGLISH, "zh-CN", "zh-TW");

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
    private final Map<String, Map<String, MessageCatalog.Rendered>> text;

    private ConnectionDiagnosticCatalog(
            Map<String, Definition> byLegacy, Map<String, Map<String, MessageCatalog.Rendered>> text) {
        this.byLegacy = byLegacy;
        this.text = text;
    }

    private static final class Bundled {
        static final ConnectionDiagnosticCatalog INSTANCE = load(MessageCatalog::read);
    }

    public static ConnectionDiagnosticCatalog bundled() {
        return Bundled.INSTANCE;
    }

    public Map<String, Object> snapshot(TapstateException failure, Locale locale) {
        return resolve(failure.code().code(), failure.args(), List.of(), locale)
                .<Map<String, Object>>map(rendered -> Map.of("code", rendered.code(), "params", failure.args(),
                        MESSAGE, rendered.message(), SOLUTION, rendered.solution()))
                .orElseGet(Map::of);
    }

    @Override public Optional<Resolved> resolve(String code, Map<String, Object> named, List<String> positional, Locale locale) {
        Definition definition = byLegacy.get(code);
        if (definition == null || !positional.isEmpty() || !named.keySet().equals(definition.parameters())) return Optional.empty();
        var template = text.get(language(locale)).get(definition.code());
        return Optional.of(new Resolved(definition.code(),
                MessageCatalog.substitute(template.message(), named), MessageCatalog.substitute(template.solution(), named)));
    }

    static String language(Locale locale) {
        if (!"zh".equals(locale.getLanguage())) return ENGLISH;
        return "Hant".equals(locale.getScript()) || Set.of("HK", "MO", "TW").contains(locale.getCountry())
                ? "zh-TW" : "zh-CN";
    }

    static ConnectionDiagnosticCatalog load(Function<String, String> readResource) {
        Map<String, Definition> byLegacy = new LinkedHashMap<>();
        Map<String, Definition> byCode = new LinkedHashMap<>();
        for (Definition definition : Definition.values()) {
            if (!definition.code().matches("TS006(?!0000)[0-9]{4}")
                    || byCode.putIfAbsent(definition.code(), definition) != null
                    || byLegacy.putIfAbsent(definition.legacy(), definition) != null) {
                throw new IllegalStateException("invalid or repeated connection diagnostic definition " + definition.code());
            }
        }
        Map<String, Map<String, MessageCatalog.Rendered>> languages = new LinkedHashMap<>();
        for (String language : LANGUAGES) {
            Object parsed = JsonReader.parse(readResource.apply("/messages/connection-diagnostic-" + language + ".json"));
            if (!(parsed instanceof Map<?, ?> entries) || !entries.keySet().equals(byCode.keySet())) {
                throw new IllegalStateException("incomplete connection diagnostic catalog " + language);
            }
            Map<String, MessageCatalog.Rendered> templates = new LinkedHashMap<>();
            byCode.forEach((code, definition) -> {
                if (!(entries.get(code) instanceof Map<?, ?> entry)
                        || !entry.keySet().equals(Set.of(MESSAGE, SOLUTION))
                        || !(entry.get(MESSAGE) instanceof String message) || message.isBlank()
                        || !(entry.get(SOLUTION) instanceof String solution) || solution.isBlank()) {
                    throw new IllegalStateException("invalid language entry " + language + "/" + code);
                }
                Set<String> used = new java.util.HashSet<>();
                PARAMETER.matcher(message).results().forEach(token -> used.add(token.group(1)));
                PARAMETER.matcher(solution).results().forEach(token -> used.add(token.group(1)));
                if (!used.equals(definition.parameters())) {
                    throw new IllegalStateException("incorrect diagnostic parameters " + language + "/" + code);
                }
                templates.put(code, new MessageCatalog.Rendered(message, solution));
            });
            languages.put(language, Map.copyOf(templates));
        }
        return new ConnectionDiagnosticCatalog(Map.copyOf(byLegacy), Map.copyOf(languages));
    }
}
