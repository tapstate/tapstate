package io.tapstate.adapters.pdk;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.core.StreamReadFeature;
import io.tapstate.core.common.JsonReader;
import io.tapstate.core.common.DiagnosticDefinitionLookup;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.jar.JarFile;

/** A connector's optional data catalog, read only from the artifact selected by registration. */
final class PdkDiagnosticCatalog implements DiagnosticDefinitionLookup {
    static final String RESOURCE = "META-INF/tapstate/connection-diagnostics.json";
    private static final int MAX_BYTES = 256 * 1024;
    private static final int MAX_DEFINITIONS = 1024;
    private static final JsonFactory JSON = JsonFactory.builder()
            .enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION)
            .streamReadConstraints(StreamReadConstraints.builder().maxNestingDepth(12).build()).build();
    private static final PdkDiagnosticCatalog UNAVAILABLE = new PdkDiagnosticCatalog(Map.of(), Map.of());

    record Definition(String code, Map<String, String> english, Map<String, String> simplifiedChinese,
                      Map<String, String> traditionalChinese) {
        Map<String, String> text(Locale locale) {
            if (!"zh".equals(locale.getLanguage())) return english;
            if (("Hant".equals(locale.getScript()) || Set.of("TW", "HK", "MO").contains(locale.getCountry()))
                    && !traditionalChinese.isEmpty()) return traditionalChinese;
            return simplifiedChinese.isEmpty() ? english : simplifiedChinese;
        }
    }

    private final Map<String, Definition> byFullCode;
    private final Map<String, Definition> byRawCode;

    private PdkDiagnosticCatalog(Map<String, Definition> byFullCode, Map<String, Definition> byRawCode) {
        this.byFullCode = Map.copyOf(byFullCode);
        this.byRawCode = Map.copyOf(byRawCode);
    }

    static PdkDiagnosticCatalog load(String connectorId, ConnectorRef ref) {
        if (ref.contentHash() == null || !ref.contentHash().matches("[0-9a-f]{64}") || ref.classpath().size() != 1) {
            return UNAVAILABLE;
        }
        try (var jar = new JarFile(ref.classpath().getFirst().toFile(), false)) {
            var entry = jar.getJarEntry(RESOURCE);
            if (entry == null || entry.isDirectory() || entry.getSize() > MAX_BYTES
                    || jar.stream().filter(candidate -> candidate.getName().equals(RESOURCE)).limit(2).count() != 1) {
                return UNAVAILABLE;
            }
            try (var input = jar.getInputStream(entry)) {
                byte[] data = input.readNBytes(MAX_BYTES + 1);
                return data.length <= MAX_BYTES ? read(connectorId, data) : UNAVAILABLE;
            }
        } catch (IOException | IllegalArgumentException invalid) {
            return UNAVAILABLE;
        }
    }

    static PdkDiagnosticCatalog read(String connectorId, byte[] data) throws IOException {
        try (var parser = JSON.createParser(data)) {
            while (parser.nextToken() != null) {
                // The streaming parser refuses duplicate fields before map materialization.
            }
        }
        Object tree = JsonReader.parse(StandardCharsets.UTF_8.newDecoder().decode(ByteBuffer.wrap(data)).toString());
        if (!(tree instanceof Map<?, ?> root) || !root.keySet().equals(Set.of("formatVersion", "connectorId", "definitions"))
                || !Long.valueOf(1L).equals(root.get("formatVersion")) || !connectorId.equals(root.get("connectorId"))
                || !(root.get("definitions") instanceof List<?> definitions) || definitions.size() > MAX_DEFINITIONS) {
            throw new IllegalArgumentException("invalid connector diagnostic catalog");
        }
        Map<String, Definition> full = new HashMap<>();
        Map<String, Definition> raw = new HashMap<>();
        Set<String> ambiguous = new HashSet<>();
        for (Object value : definitions) {
            if (!(value instanceof Map<?, ?> item)
                    || !(item.get("rawCode") instanceof String original) || original.isBlank()
                    || !(item.get("code") instanceof String code) || code.isBlank() || !code.endsWith(original)) {
                throw new IllegalArgumentException("invalid connector diagnostic identity");
            }
            var entry = new Definition(code, readText(item.get("en")), readText(item.get("zh-CN")), readText(item.get("zh-TW")));
            if (entry.english().isEmpty() || full.putIfAbsent(code, entry) != null) {
                throw new IllegalArgumentException("incomplete or repeated full connector diagnostic code");
            }
            if (raw.putIfAbsent(original, entry) != null) ambiguous.add(original);
        }
        ambiguous.forEach(raw::remove);
        return new PdkDiagnosticCatalog(full, raw);
    }

    private static Map<String, String> readText(Object value) {
        if (value == null) return Map.of();
        if (!(value instanceof Map<?, ?> text)) throw new IllegalArgumentException("invalid connector diagnostic text");
        Map<String, String> entries = new HashMap<>();
        for (String field : List.of("message", "solution", "dynamic")) {
            if (text.get(field) instanceof String content && !content.isBlank()) entries.put(field, content);
            else if (text.containsKey(field)) throw new IllegalArgumentException("invalid connector diagnostic text");
        }
        if (!entries.keySet().equals(text.keySet())) throw new IllegalArgumentException("unknown connector diagnostic text field");
        return Map.copyOf(entries);
    }

    Definition definition(String code) {
        if (code == null) return null;
        Definition exact = byFullCode.get(code);
        Definition raw = byRawCode.get(code);
        return exact != null && raw != null && exact != raw ? null : exact == null ? raw : exact;
    }

    @Override public Optional<Resolved> resolve(String code, Map<String, Object> named, List<String> positional, Locale locale) {
        Definition definition = definition(code);
        if (definition == null || !named.isEmpty()) return Optional.empty();
        var text = definition.text(locale);
        String message = text.get("message");
        if (text.get("dynamic") != null) {
            String substitution = renderPositional(text.get("dynamic"), positional);
            if (substitution != null) message = message == null ? substitution : message + "\n" + substitution;
        }
        return Optional.of(new Resolved(definition.code(), message, text.get("solution")));
    }

    private static String renderPositional(String template, List<String> args) {
        StringBuilder rendered = new StringBuilder();
        int from = 0;
        int index = 0;
        int token;
        while ((token = template.indexOf("{}", from)) >= 0) {
            if (index >= args.size()) return null;
            rendered.append(template, from, token).append(args.get(index++));
            from = token + 2;
        }
        if (index != args.size()) return null;
        return rendered.append(template, from, template.length()).toString();
    }
}
