package io.tapstate.adapters.mongostore;

import io.tapstate.core.event.Bytes;
import io.tapstate.core.event.ConvertedValue;
import org.bson.Document;
import org.bson.types.Binary;

import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Typed values and ordinary maps have disjoint encodings, including maps containing the escape tag itself. */
final class RequiredDeadLetterRows {
    private static final String TAG = "__tapstate_dead_letter_value";
    private RequiredDeadLetterRows() {}

    static Document encode(Map<String, Object> row) { return plainMap(row); }
    static Map<String, Object> decode(Document row) { return plainRow(row); }

    private static Document plainMap(Map<?, ?> map) {
        Document encoded = new Document();
        map.forEach((key, value) -> {
            if (!(key instanceof String name)) throw RequiredDeadLetterCodec.unreadable();
            encoded.append(name, encodeValue(value));
        });
        return encoded;
    }
    private static Object encodeValue(Object value) {
        if (value instanceof ConvertedValue carrier) {
            return new Document(TAG, "converted").append("value", encodeValue(carrier.value())).append("originType", carrier.originType());
        }
        if (value instanceof Bytes bytes) return new Document(TAG, "bytes").append("tag", Byte.toUnsignedInt(bytes.tag())).append("value", new Binary(bytes.value()));
        if (value instanceof Map<?, ?> map) {
            Document document = plainMap(map);
            return map.containsKey(TAG) ? new Document(TAG, "map").append("value", document) : document;
        }
        if (value instanceof List<?> list) {
            var encoded = new ArrayList<Object>(list.size()); list.forEach(item -> encoded.add(encodeValue(item))); return encoded;
        }
        if (!scalar(value)) throw RequiredDeadLetterCodec.unreadable();
        return value;
    }
    private static Map<String, Object> plainRow(Document row) {
        var decoded = new LinkedHashMap<String, Object>(); row.forEach((key, value) -> decoded.put(key, decodeValue(value))); return decoded;
    }
    private static Object decodeValue(Object value) {
        if (value instanceof Document document) {
            if (!document.containsKey(TAG)) return plainRow(document);
            if (!(document.get(TAG) instanceof String kind)) throw RequiredDeadLetterCodec.unreadable();
            return switch (kind) {
                case "map" -> {
                    if (!document.keySet().equals(Set.of(TAG, "value")) || !(document.get("value") instanceof Document map)) throw RequiredDeadLetterCodec.unreadable();
                    yield plainRow(map);
                }
                case "converted" -> {
                    if (!document.keySet().equals(Set.of(TAG, "value", "originType"))
                            || (document.get("originType") != null && !(document.get("originType") instanceof String))) throw RequiredDeadLetterCodec.unreadable();
                    yield new ConvertedValue(decodeValue(document.get("value")), document.getString("originType"));
                }
                case "bytes" -> {
                    if (!document.keySet().equals(Set.of(TAG, "tag", "value")) || !(document.get("tag") instanceof Integer tag) || tag < 0 || tag > 255
                            || !(document.get("value") instanceof Binary bytes) || bytes.getType() != 0) throw RequiredDeadLetterCodec.unreadable();
                    yield new Bytes((byte) tag.intValue(), bytes.getData());
                }
                default -> throw RequiredDeadLetterCodec.unreadable();
            };
        }
        if (value instanceof List<?> list) {
            var decoded = new ArrayList<Object>(list.size()); list.forEach(item -> decoded.add(decodeValue(item))); return decoded;
        }
        if (value instanceof Binary bytes) {
            if (bytes.getType() != 0) throw RequiredDeadLetterCodec.unreadable();
            return bytes.getData();
        }
        // Retain the existing scalar decimal and bare-binary normalization without its ambiguous map markers.
        Object portable = RowImages.toRow(new Document("value", value)).get("value");
        if (!scalar(portable)) throw RequiredDeadLetterCodec.unreadable();
        return portable;
    }
    private static boolean scalar(Object value) {
        return value == null || value instanceof String || value instanceof Boolean || value instanceof Number
                || value instanceof Date || value instanceof byte[];
    }
}
