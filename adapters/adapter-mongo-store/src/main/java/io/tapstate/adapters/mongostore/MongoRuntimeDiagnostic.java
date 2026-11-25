package io.tapstate.adapters.mongostore;

import io.tapstate.core.common.RuntimeDiagnosticProjection;
import org.bson.Document;

import java.util.List;
import java.util.Map;

/** Optional bounded attachment to the existing scoped runtime observation. */
final class MongoRuntimeDiagnostic {
    static final int MAX_BYTES = 256 * 1024;
    private MongoRuntimeDiagnostic() {}

    static Document document(Map<String, Object> projection) {
        try {
            var bounded = RuntimeDiagnosticProjection.fit(projection);
            var document = encode(bounded);
            if (MongoConnectionDiagnostic.bsonSize(document) <= MAX_BYTES) return document;
        } catch (RuntimeException | LinkageError unavailable) {
            // An attachment must never prevent publication of the original failure.
        }
        return encode(Map.of("truncated", true));
    }

    static Map<String, Object> read(Object input) {
        if (!(input instanceof Document stored) || !Integer.valueOf(2).equals(stored.get("formatVersion"))
                || MongoConnectionDiagnostic.bsonSize(stored) > MAX_BYTES) {
            throw new IllegalArgumentException("unsupported or oversized runtime diagnostic");
        }
        var projection = RuntimeDiagnosticProjection.read(stored.get("projection"));
        if (projection.isEmpty() || !stored.equals(encode(projection))) {
            throw new IllegalArgumentException("noncanonical runtime diagnostic");
        }
        return projection;
    }

    private static Document encode(Map<String, Object> projection) {
        return new Document("formatVersion", 2).append("projection", bson(projection));
    }

    private static Object bson(Object value) {
        if (value instanceof Map<?, ?> map) {
            Document document = new Document();
            map.forEach((key, child) -> document.put((String) key, bson(child)));
            return document;
        }
        if (value instanceof List<?> list) return list.stream().map(MongoRuntimeDiagnostic::bson).toList();
        return value;
    }
}
