package io.tapstate.app;

import io.tapstate.adapters.pdk.PdkPreviewDateTimes;
import io.tapstate.core.common.JsonReader;
import io.tapstate.core.common.JsonWriter;
import io.tapstate.core.event.Bytes;
import io.tapstate.core.event.ConvertedValue;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Stores preview rows as logical JSON plus private type metadata needed by target codecs. */
final class PreviewDocumentStorage {

    private PreviewDocumentStorage() {
    }

    static String encode(Map<String, Object> document) {
        Object logicalDocument = PreviewJsonValues.normalize(document);
        List<Map<String, Object>> carriers = new ArrayList<>();
        collect(document, new ArrayList<>(), carriers);
        Map<String, Object> stored = new LinkedHashMap<>();
        stored.put("document", logicalDocument);
        stored.put("carriers", carriers);
        return JsonWriter.write(stored);
    }

    static long encodedSize(String encoded) {
        return encoded.getBytes(StandardCharsets.UTF_8).length;
    }

    static Map<String, Object> decode(String encoded) {
        Object parsed = JsonReader.parse(encoded);
        if (!(parsed instanceof Map<?, ?> envelope)
                || !(envelope.get("document") instanceof Map<?, ?> rawDocument)
                || !(envelope.get("carriers") instanceof List<?> rawCarriers)) {
            throw new IllegalStateException("preview materializer stored an invalid document envelope");
        }
        Map<String, Object> document = stringKeyedMap(rawDocument);
        Map<String, Carrier> carriers = new LinkedHashMap<>();
        for (Object item : rawCarriers) {
            if (!(item instanceof Map<?, ?> raw)) {
                throw new IllegalStateException("preview materializer stored invalid type metadata");
            }
            Object path = raw.get("path");
            Object kind = raw.get("kind");
            if (!(path instanceof String pointer) || !(kind instanceof String type)) {
                throw new IllegalStateException("preview materializer stored incomplete type metadata");
            }
            Carrier carrier = new Carrier(type, raw.get("originType"), raw.get("value"));
            if (carriers.put(pointer, carrier) != null) {
                throw new IllegalStateException("preview materializer stored duplicate type metadata");
            }
        }
        Object restored = restore(document, "", carriers);
        return stringKeyedMap((Map<?, ?>) restored);
    }

    private static void collect(Object value, List<String> path, List<Map<String, Object>> carriers) {
        if (value instanceof ConvertedValue carrier) {
            Map<String, Object> metadata = new LinkedHashMap<>();
            metadata.put("path", pointer(path));
            metadata.put("kind", kind(carrier.value()));
            metadata.put("originType", carrier.originType());
            metadata.put("value", wireValue(carrier.value()));
            carriers.add(metadata);
            return;
        }
        if (value instanceof Map<?, ?> map) {
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                if (!(entry.getKey() instanceof String key)) {
                    throw new IllegalArgumentException("preview document field names must be strings");
                }
                path.add(key);
                collect(entry.getValue(), path, carriers);
                path.removeLast();
            }
            return;
        }
        if (value instanceof Collection<?> values) {
            int index = 0;
            for (Object item : values) {
                path.add(Integer.toString(index++));
                collect(item, path, carriers);
                path.removeLast();
            }
            return;
        }
        if (value != null && value.getClass().isArray()) {
            for (int index = 0; index < Array.getLength(value); index++) {
                path.add(Integer.toString(index));
                collect(Array.get(value, index), path, carriers);
                path.removeLast();
            }
        }
    }

    private static Object restore(Object value, String pointer, Map<String, Carrier> carriers) {
        Carrier carrier = carriers.get(pointer);
        if (carrier != null) {
            Object restoredValue = readWireValue(carrier.kind(), carrier.value());
            return new ConvertedValue(restoredValue, carrier.originType() instanceof String origin ? origin : null);
        }
        if (value instanceof Map<?, ?> map) {
            Map<String, Object> restored = new LinkedHashMap<>();
            map.forEach((key, item) -> {
                String field = (String) key;
                restored.put(field, restore(item, child(pointer, field), carriers));
            });
            return restored;
        }
        if (value instanceof List<?> list) {
            List<Object> restored = new ArrayList<>(list.size());
            for (int index = 0; index < list.size(); index++) {
                restored.add(restore(list.get(index), child(pointer, Integer.toString(index)), carriers));
            }
            return restored;
        }
        return value;
    }

    private static String kind(Object value) {
        if (PdkPreviewDateTimes.isDateTime(value)) {
            return "datetime";
        }
        return switch (value) {
            case Bytes ignored -> "bytes";
            case String ignored -> "string";
            case Double ignored -> "double";
            case BigDecimal ignored -> "big-decimal";
            case BigInteger ignored -> "big-integer";
            case Integer ignored -> "integer";
            case Long ignored -> "long";
            case Short ignored -> "short";
            case Byte ignored -> "byte";
            case Float ignored -> "float";
            case Date ignored -> "date";
            case byte[] ignored -> "byte-array";
            case Boolean ignored -> "boolean";
            case Character ignored -> "character";
            default -> "json";
        };
    }

    private static Object wireValue(Object value) {
        if (PdkPreviewDateTimes.isDateTime(value)) {
            return PdkPreviewDateTimes.toIsoString(value);
        }
        return switch (value) {
            case Bytes bytes -> Map.of("tag", bytes.tag(), "base64", Base64.getEncoder().encodeToString(bytes.value()));
            case BigInteger integer -> integer.toString();
            case BigDecimal decimal -> decimal.toPlainString();
            case Date date -> date.getTime();
            case byte[] bytes -> Base64.getEncoder().encodeToString(bytes);
            case Character character -> character.toString();
            default -> PreviewJsonValues.normalize(value);
        };
    }

    private static Object readWireValue(String kind, Object value) {
        return switch (kind) {
            case "bytes" -> {
                Map<?, ?> bytes = requireMap(value);
                yield new Bytes(number(bytes.get("tag")).byteValue(),
                        Base64.getDecoder().decode(requireString(bytes.get("base64"))));
            }
            case "datetime" -> PdkPreviewDateTimes.fromIsoString(requireString(value));
            case "string" -> requireString(value);
            case "double" -> number(value).doubleValue();
            case "big-decimal" -> new BigDecimal(requireString(value));
            case "big-integer" -> new BigInteger(requireString(value));
            case "integer" -> number(value).intValueExact();
            case "long" -> number(value).longValueExact();
            case "short" -> number(value).shortValueExact();
            case "byte" -> number(value).byteValueExact();
            case "float" -> number(value).floatValue();
            case "date" -> new Date(number(value).longValueExact());
            case "byte-array" -> Base64.getDecoder().decode(requireString(value));
            case "boolean" -> {
                if (!(value instanceof Boolean bool)) {
                    throw new IllegalStateException("preview materializer stored invalid boolean metadata");
                }
                yield bool;
            }
            case "character" -> {
                String character = requireString(value);
                if (character.length() != 1) {
                    throw new IllegalStateException("preview materializer stored invalid character metadata");
                }
                yield character.charAt(0);
            }
            case "json" -> value;
            default -> throw new IllegalStateException("preview materializer stored an unknown value type");
        };
    }

    private static String pointer(List<String> path) {
        StringBuilder pointer = new StringBuilder();
        for (String token : path) {
            pointer.append('/').append(token.replace("~", "~0").replace("/", "~1"));
        }
        return pointer.toString();
    }

    private static String child(String pointer, String token) {
        return pointer + "/" + token.replace("~", "~0").replace("/", "~1");
    }

    private static Map<String, Object> stringKeyedMap(Map<?, ?> source) {
        Map<String, Object> result = new LinkedHashMap<>();
        source.forEach((key, value) -> {
            if (!(key instanceof String field)) {
                throw new IllegalStateException("preview materializer stored a non-string field name");
            }
            result.put(field, value);
        });
        return result;
    }

    private static Map<?, ?> requireMap(Object value) {
        if (value instanceof Map<?, ?> map) {
            return map;
        }
        throw new IllegalStateException("preview materializer stored invalid map metadata");
    }

    private static String requireString(Object value) {
        if (value instanceof String text) {
            return text;
        }
        throw new IllegalStateException("preview materializer stored invalid string metadata");
    }

    private static BigDecimal number(Object value) {
        if (value instanceof BigDecimal decimal) {
            return decimal;
        }
        if (value instanceof BigInteger integer) {
            return new BigDecimal(integer);
        }
        if (value instanceof Number number) {
            return new BigDecimal(number.toString());
        }
        throw new IllegalStateException("preview materializer stored invalid numeric metadata");
    }

    private record Carrier(String kind, Object originType, Object value) {
    }
}
