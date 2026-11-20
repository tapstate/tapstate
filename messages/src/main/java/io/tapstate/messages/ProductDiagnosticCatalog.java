package io.tapstate.messages;

import io.tapstate.core.common.DiagnosticDefinitionLookup;
import io.tapstate.core.common.JsonReader;
import io.tapstate.core.common.TapstateErrorCode;
import io.tapstate.core.common.ProductErrorModule;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Pattern;

/** Explicitly registered product definitions and their bundled, named-parameter translations. */
public final class ProductDiagnosticCatalog implements DiagnosticDefinitionLookup {
    private static final List<String> LANGUAGES = List.of("en", "zh-CN", "zh-TW");
    private static final Pattern NAME = Pattern.compile("[a-z][A-Za-z0-9]*");

    /** A projection of an owner's definition, not a second throwable error-code family. */
    public record Definition(String code, Set<String> parameters) {
        public Definition {
            java.util.Objects.requireNonNull(code, "code");
            parameters = Set.copyOf(parameters);
            if (parameters.stream().anyMatch(name -> !NAME.matcher(name).matches()))
                throw new IllegalArgumentException("invalid named diagnostic parameter");
        }

        public static Definition of(TapstateErrorCode code) {
            return new Definition(code.code(), code.placeholders());
        }
    }

    private final Map<String, Definition> definitions;
    private final Map<String, Map<String, MessageCatalog.Rendered>> text;

    private ProductDiagnosticCatalog(Map<String, Definition> definitions,
            Map<String, Map<String, MessageCatalog.Rendered>> text) {
        this.definitions = Map.copyOf(definitions);
        this.text = Map.copyOf(text);
    }

    /**
     * Registers one explicitly selected module. The owner supplies definitions and locale resources;
     * no classpath scan, enum reconstruction, legacy alias or upstream-code interpretation is performed.
     */
    public static ProductDiagnosticCatalog load(String module, List<Definition> registered,
            Function<String, String> readLocale) {
        if (module == null || java.util.Arrays.stream(ProductErrorModule.values())
                .noneMatch(owner -> owner.number().equals(module)) || registered.isEmpty())
            throw new IllegalStateException("invalid or empty diagnostic module registration");
        Map<String, Definition> definitions = new LinkedHashMap<>();
        for (Definition definition : registered) {
            if (!definition.code().matches("TS" + module + "(?!0000)[0-9]{4}")
                    || definitions.putIfAbsent(definition.code(), definition) != null)
                throw new IllegalStateException("invalid or repeated diagnostic definition " + definition.code());
        }
        Map<String, Map<String, MessageCatalog.Rendered>> languages = new LinkedHashMap<>();
        for (String language : LANGUAGES) {
            Object parsed = JsonReader.parseUnique(readLocale.apply(language));
            if (!(parsed instanceof Map<?, ?> entries) || !entries.keySet().equals(definitions.keySet()))
                throw new IllegalStateException("incomplete diagnostic catalog " + language);
            Map<String, MessageCatalog.Rendered> templates = new LinkedHashMap<>();
            definitions.forEach((code, definition) -> {
                if (!(entries.get(code) instanceof Map<?, ?> entry)
                        || !entry.keySet().equals(entry.containsKey("technicalParams")
                                ? Set.of("message", "solution", "technicalParams") : Set.of("message", "solution"))
                        || !(entry.get("message") instanceof String message) || message.isBlank()
                        || !(entry.get("solution") instanceof String solution) || solution.isBlank())
                    throw new IllegalStateException("invalid language entry " + language + "/" + code);
                Set<String> used = new HashSet<>();
                collectParameters(message, used);
                collectParameters(solution, used);
                // Technical arguments remain mandatory wire facts but never enter the human summary.
                Set<String> technical = new HashSet<>();
                if (entry.containsKey("technicalParams")) {
                    if (!(entry.get("technicalParams") instanceof List<?> names))
                        throw new IllegalStateException("invalid technical diagnostic parameters " + language + "/" + code);
                    for (Object name : names) {
                        if (!(name instanceof String text) || !NAME.matcher(text).matches() || !technical.add(text))
                            throw new IllegalStateException("invalid technical diagnostic parameter " + language + "/" + code);
                    }
                }
                if (!java.util.Collections.disjoint(used, technical))
                    throw new IllegalStateException("technical diagnostic parameter occurs in summary " + language + "/" + code);
                used.addAll(technical);
                if (!used.equals(definition.parameters()))
                    throw new IllegalStateException("incorrect diagnostic parameters " + language + "/" + code);
                templates.put(code, new MessageCatalog.Rendered(message, solution));
            });
            languages.put(language, Map.copyOf(templates));
        }
        return new ProductDiagnosticCatalog(definitions, languages);
    }

    private static void collectParameters(String template, Set<String> used) {
        processTemplate(template, used, null);
    }

    /** Backslash escapes a literal brace or backslash; inserted argument text is never re-parsed. */
    private static String processTemplate(String template, Set<String> used, Map<String, Object> args) {
        StringBuilder rendered = new StringBuilder();
        for (int offset = 0; offset < template.length(); offset++) {
            char character = template.charAt(offset);
            if (character == '\\') {
                if (++offset == template.length() || "{}\\".indexOf(template.charAt(offset)) < 0)
                    throw new IllegalStateException("invalid diagnostic template escape");
                rendered.append(template.charAt(offset));
                continue;
            }
            if (character == '}')
                throw new IllegalStateException("unmatched diagnostic parameter delimiter");
            if (character != '{') {
                rendered.append(character);
                continue;
            }
            int close = template.indexOf('}', offset + 1);
            if (close < 0)
                throw new IllegalStateException("unmatched diagnostic parameter delimiter");
            String name = template.substring(offset + 1, close);
            if (!NAME.matcher(name).matches())
                throw new IllegalStateException("diagnostic templates require named parameters");
            if (used != null) used.add(name);
            if (args != null) rendered.append(String.valueOf(args.get(name)));
            offset = close;
        }
        return rendered.toString();
    }

    /** The registered identities, available for composition and independent completeness gates. */
    public Set<String> codes() { return definitions.keySet(); }

    @Override
    public Optional<Resolved> resolve(String code, Map<String, Object> named,
            List<String> positional, Locale locale) {
        Definition definition = definitions.get(code);
        if (definition == null || !positional.isEmpty() || !named.keySet().equals(definition.parameters()))
            return Optional.empty();
        var template = text.get(language(locale)).get(code);
        return Optional.of(new Resolved(code, processTemplate(template.message(), null, named),
                processTemplate(template.solution(), null, named)));
    }

    static String language(Locale locale) {
        if (!"zh".equals(locale.getLanguage())) return "en";
        return "Hant".equals(locale.getScript()) || Set.of("HK", "MO", "TW").contains(locale.getCountry())
                ? "zh-TW" : "zh-CN";
    }
}
