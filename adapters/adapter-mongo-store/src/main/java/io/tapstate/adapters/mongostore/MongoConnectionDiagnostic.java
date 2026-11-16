package io.tapstate.adapters.mongostore;

import io.tapstate.core.common.DiagnosticProjection;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.ConnectionTestItem;
import io.tapstate.spi.store.ConnectionTestResult;
import io.tapstate.spi.store.IoError;
import org.bson.BsonBinaryWriter;
import org.bson.Document;
import org.bson.codecs.DocumentCodec;
import org.bson.codecs.EncoderContext;
import org.bson.io.BasicOutputBuffer;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

/** Versioned diagnostic subdocuments inside the existing CACHE result layouts. */
final class MongoConnectionDiagnostic {
    private static final int VERSION = 1;
    private static final int MAX_BSON_BYTES = 256 * 1024;
    private static final int MAX_REFERENCES = 128;
    private static final int COMPACT_TEXT_LENGTH = 1024;

    private MongoConnectionDiagnostic() {
    }

    static Document document(Map<String, Object> diagnostic) {
        return new Document("formatVersion", VERSION).append("projection", bsonValue(diagnostic));
    }

    private static Object bsonValue(Object value) {
        if (value instanceof Map<?, ?> map) {
            Document document = new Document();
            map.forEach((key, child) -> document.put((String) key, bsonValue(child)));
            return document;
        }
        if (value instanceof List<?> list) return list.stream().map(MongoConnectionDiagnostic::bsonValue).toList();
        return value;
    }

    static Map<String, Object> read(Object stored) {
        if (!(stored instanceof Document document) || !Integer.valueOf(VERSION).equals(document.get("formatVersion"))) {
            throw new IllegalArgumentException("unsupported connection diagnostic format");
        }
        Map<String, Object> diagnostic = DiagnosticProjection.read(document.get("projection"));
        if (diagnostic.isEmpty() || !document.equals(document(diagnostic))) {
            throw new IllegalArgumentException("noncanonical connection diagnostic");
        }
        return diagnostic;
    }

    static void requireBounded(ConnectionTestResult result) {
        if (!withinBudget(result)) throw new IllegalArgumentException("connection diagnostic exceeds its budget");
    }

    static ConnectionTestResult fit(ConnectionTestResult result, Predicate<ConnectionTestResult> wholeRowFits) {
        if (withinBudget(result) && wholeRowFits.test(result)) return result;
        List<ConnectionTestItem> compact = new ArrayList<>();
        for (var item : result.items()) {
            compact.add(withDiagnostic(item, compact(item.diagnostic())));
        }
        ConnectionTestResult shorter = withItems(result, compact);
        if (withinBudget(shorter) && wholeRowFits.test(shorter)) return shorter;

        List<ConnectionTestItem> minimal = new ArrayList<>();
        boolean marked = false;
        for (var item : result.items()) {
            boolean mark = !marked && !item.diagnostic().isEmpty();
            minimal.add(withDiagnostic(item, mark ? Map.of("truncated", true) : Map.of()));
            marked |= mark;
        }
        ConnectionTestResult truncated = withItems(result, minimal);
        if (wholeRowFits.test(truncated)) return truncated;
        throw new TapstateException(IoError.DOCUMENT_TOO_LARGE, Map.of("id", result.connectionId()), null);
    }

    private static Map<String, Object> compact(Map<String, Object> diagnostic) {
        return diagnostic.isEmpty() ? diagnostic : compactNode(diagnostic);
    }

    private static Map<String, Object> compactNode(Map<?, ?> diagnostic) {
        Map<String, Object> result = new LinkedHashMap<>();
        for (var entry : diagnostic.entrySet()) {
            String field = (String) entry.getKey();
            Object value = entry.getValue();
            switch (field) {
                case "stackTrace", "rawMessage" -> { }
                case "cause" -> result.put(field, compactNode((Map<?, ?>) value));
                case "causes" -> result.put(field, ((List<?>) value).stream()
                        .map(child -> compactNode((Map<?, ?>) child)).toList());
                case "message", "solution" -> result.put(field, compactText(value));
                case "params" -> {
                    Map<String, Object> params = new LinkedHashMap<>();
                    ((Map<?, ?>) value).forEach((name, arg) -> params.put((String) name, compactText(arg)));
                    result.put(field, params);
                }
                case "arguments" -> result.put(field, ((List<?>) value).stream()
                        .map(MongoConnectionDiagnostic::compactText).toList());
                default -> result.put(field, value);
            }
        }
        if (!result.equals(diagnostic)) result.put("truncated", true);
        return result;
    }

    private static Object compactText(Object value) {
        if (value instanceof String text && text.length() > COMPACT_TEXT_LENGTH) {
            int end = Character.isHighSurrogate(text.charAt(COMPACT_TEXT_LENGTH - 1))
                    ? COMPACT_TEXT_LENGTH - 1 : COMPACT_TEXT_LENGTH;
            return text.substring(0, end) + "\u2026";
        }
        return value;
    }

    private static ConnectionTestItem withDiagnostic(ConnectionTestItem item, Map<String, Object> diagnostic) {
        return new ConnectionTestItem(item.name(), item.status(), item.message(), item.reason(), item.solution(),
                item.connectorErrorCode(), diagnostic);
    }

    private static ConnectionTestResult withItems(ConnectionTestResult result, List<ConnectionTestItem> items) {
        return new ConnectionTestResult(result.connectionId(), result.connectorId(), result.outcome(), items,
                result.testedAt(), result.diagnosticsRedacted());
    }

    private static boolean withinBudget(ConnectionTestResult result) {
        long bytes = 0;
        long references = 0;
        for (var item : result.items()) {
            if (item.diagnostic().isEmpty()) continue;
            if (!result.diagnosticsRedacted()) throw new IllegalArgumentException("unredacted connection diagnostic");
            bytes += bsonSize(document(item.diagnostic()));
            references += referenceCount(item.diagnostic());
            if (bytes > MAX_BSON_BYTES || references > MAX_REFERENCES) return false;
        }
        return true;
    }

    static int bsonSize(Document document) {
        try (var output = new BasicOutputBuffer(); var writer = new BsonBinaryWriter(output)) {
            new DocumentCodec().encode(writer, document, EncoderContext.builder().build());
            return output.getSize();
        }
    }

    private static long referenceCount(Map<String, Object> diagnostic) {
        Deque<Map<?, ?>> pending = new ArrayDeque<>();
        pending.push(diagnostic);
        long count = 0;
        while (!pending.isEmpty()) {
            Map<?, ?> node = pending.pop();
            if (node.get("references") instanceof List<?> references) count += references.size();
            if (node.get("cause") instanceof Map<?, ?> cause) pending.push(cause);
            if (node.get("causes") instanceof List<?> causes) {
                for (Object cause : causes) pending.push((Map<?, ?>) cause);
            }
        }
        return count;
    }
}
