package io.tapstate.adapters.pdk;

import io.tapstate.core.common.JsonReader;
import io.tapstate.core.common.JsonWriter;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.RequiredNoteOrigin;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/** Complete, detached settings with stable values and runtime types, without secrets in diagnostics. */
public final class ConnectorNoteOrigin {
    private static final String FORMAT = "connector-settings-v1";
    private ConnectorNoteOrigin() {}

    public static RequiredNoteOrigin capture(String connectorId, String artifactHash, Map<String, Object> settings) {
        try {
            String json = JsonWriter.write(List.of(FORMAT, encode(settings)));
            return new RequiredNoteOrigin(connectorId, artifactHash, json.getBytes(StandardCharsets.UTF_8));
        } catch (IllegalArgumentException | NullPointerException invalid) {
            throw new TapstateException(IoError.NOTE_ORIGIN_MISMATCH, Map.of(), null);
        }
    }

    @SuppressWarnings("unchecked")
    static Map<String, Object> settings(RequiredNoteOrigin origin) {
        try {
            Object parsed = JsonReader.parse(new String(origin.settings(), StandardCharsets.UTF_8));
            if (parsed instanceof List<?> root && root.size() == 2 && FORMAT.equals(root.getFirst())
                    && decode(root.get(1)) instanceof Map<?, ?> object) return (Map<String, Object>) object;
        } catch (IllegalArgumentException | NullPointerException | ClassCastException invalid) {
            // Parser and numeric diagnostics may include secret input; retain neither message nor cause.
        }
        throw new TapstateException(IoError.DOCUMENT_UNREADABLE, Map.of("id", "required-note", "field", "origin"), null);
    }

    private static Object encode(Object value) {
        if (value == null) return List.of("null");
        if (value instanceof Map<?, ?> map) {
            Map<String, Object> sorted = new TreeMap<>();
            for (var entry : map.entrySet()) {
                if (!(entry.getKey() instanceof String key)) throw invalid();
                sorted.put(key, encode(entry.getValue()));
            }
            return List.of("map", sorted);
        }
        if (value instanceof List<?> list) return List.of("list", list.stream().map(ConnectorNoteOrigin::encode).toList());
        if (value instanceof byte[] bytes) return List.of("bytes", Base64.getUrlEncoder().withoutPadding().encodeToString(bytes));
        if (value instanceof String text) return List.of("string", text);
        if (value instanceof Boolean flag) return List.of("boolean", flag);
        String tag = switch (value) {
            case Integer ignored -> "integer";
            case Long ignored -> "long";
            case Short ignored -> "short";
            case Byte ignored -> "byte";
            case Double number when Double.isFinite(number) -> "double";
            case Float number when Float.isFinite(number) -> "float";
            case java.math.BigInteger ignored -> "big-integer";
            case java.math.BigDecimal ignored -> "big-decimal";
            default -> throw invalid();
        };
        return List.of(tag, value.toString());
    }

    private static Object decode(Object encoded) {
        if (!(encoded instanceof List<?> row) || row.isEmpty() || !(row.getFirst() instanceof String tag)) throw invalid();
        if (tag.equals("null") && row.size() == 1) return null;
        if (row.size() != 2) throw invalid();
        Object payload = row.get(1);
        return switch (tag) {
            case "map" -> {
                if (!(payload instanceof Map<?, ?> map)) throw invalid();
                Map<String, Object> result = new LinkedHashMap<>();
                for (var entry : map.entrySet()) {
                    if (!(entry.getKey() instanceof String key)) throw invalid();
                    result.put(key, decode(entry.getValue()));
                }
                yield result;
            }
            case "list" -> {
                if (!(payload instanceof List<?> list)) throw invalid();
                List<Object> result = new ArrayList<>();
                for (Object element : list) result.add(decode(element));
                yield result;
            }
            case "string" -> (String) payload;
            case "boolean" -> (Boolean) payload;
            case "bytes" -> Base64.getUrlDecoder().decode((String) payload);
            case "integer" -> Integer.valueOf((String) payload);
            case "long" -> Long.valueOf((String) payload);
            case "short" -> Short.valueOf((String) payload);
            case "byte" -> Byte.valueOf((String) payload);
            case "double" -> Double.valueOf((String) payload);
            case "float" -> Float.valueOf((String) payload);
            case "big-integer" -> new java.math.BigInteger((String) payload);
            case "big-decimal" -> new java.math.BigDecimal((String) payload);
            default -> throw invalid();
        };
    }

    private static IllegalArgumentException invalid() { return new IllegalArgumentException("invalid retained settings"); }
}
