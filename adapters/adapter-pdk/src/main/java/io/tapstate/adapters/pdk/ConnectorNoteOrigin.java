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
    private static final String PIPELINE_FORMAT = "pipeline-source-settings-v2";
    private ConnectorNoteOrigin() {}

    public static RequiredNoteOrigin capture(String connectorId, String artifactHash, Map<String, Object> settings) {
        return capture(connectorId, artifactHash, FORMAT, settings);
    }

    /** Records the exact non-materialized settings supplied to a new Pipeline source drive. */
    public static RequiredNoteOrigin capturePipeline(String connectorId, ConnectorRef ref,
            Map<String, Object> settings, Map<String, Object> nodeConfig) {
        var effective = PdkConnector.pipelineSettings(connectorId, ref.spec(), settings, nodeConfig);
        return capture(connectorId, ref.contentHash(), PIPELINE_FORMAT,
                settings, nodeConfig, effective.connection(), effective.node());
    }

    @SafeVarargs
    private static RequiredNoteOrigin capture(String connectorId, String artifactHash, String format,
            Map<String, Object>... settings) {
        try {
            List<Object> fields = new ArrayList<>();
            fields.add(format);
            for (var field : settings) fields.add(encode(field));
            String json = JsonWriter.write(fields);
            return new RequiredNoteOrigin(connectorId, artifactHash, json.getBytes(StandardCharsets.UTF_8));
        } catch (IllegalArgumentException | NullPointerException invalid) {
            throw new TapstateException(IoError.NOTE_ORIGIN_MISMATCH, Map.of(), null);
        }
    }
    /** Validates retained settings without exposing their decoded credentials to the caller. */
    public static void requireReadable(RequiredNoteOrigin origin) { payload(origin); }

    /** Compares authored settings; a legacy origin has no separately recorded node map. */
    public static boolean matches(RequiredNoteOrigin origin, String connectorId, String artifactHash,
            Map<String, Object> settings, Map<String, Object> nodeConfig) {
        if (origin == null || !origin.connectorId().equals(connectorId)
                || !origin.artifactHash().equals(artifactHash)) return false;
        var payload = payload(origin);
        if (payload.nodeConfig() == null) return origin.equals(capture(connectorId, artifactHash, settings));
        return sameValues(payload.authored(), settings)
                && (nodeConfig == null || sameValues(payload.nodeConfig(), nodeConfig));
    }

    private static boolean sameValues(Map<String, Object> left, Map<String, Object> right) {
        // Encoding preserves numeric types and compares byte arrays by value.
        return encode(left).equals(encode(right));
    }

    private record Payload(Map<String, Object> authored, Map<String, Object> nodeConfig,
            PdkConnector.PipelineSettings effective) {}

    static Map<String, Object> settings(RequiredNoteOrigin origin) { return payload(origin).authored(); }

    static PdkConnector.PipelineSettings pipelineSettings(RequiredNoteOrigin origin) { return payload(origin).effective(); }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> decodedMap(Object encoded) {
        if (decode(encoded) instanceof Map<?, ?> map) return (Map<String, Object>) map;
        throw invalid();
    }

    private static Payload payload(RequiredNoteOrigin origin) {
        try {
            Object parsed = JsonReader.parse(new String(origin.settings(), StandardCharsets.UTF_8));
            if (parsed instanceof List<?> root) {
                if (root.size() == 2 && FORMAT.equals(root.getFirst())) {
                    return new Payload(decodedMap(root.get(1)), null, null);
                }
                if (root.size() == 5 && PIPELINE_FORMAT.equals(root.getFirst())) {
                    return new Payload(decodedMap(root.get(1)), decodedMap(root.get(2)),
                            new PdkConnector.PipelineSettings(decodedMap(root.get(3)), decodedMap(root.get(4))));
                }
            }
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
            case Double number -> {
                if (!Double.isFinite(number)) throw invalid();
                yield "double";
            }
            case Float number -> {
                if (!Float.isFinite(number)) throw invalid();
                yield "float";
            }
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
