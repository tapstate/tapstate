package io.tapstate.app;

import io.tapstate.core.event.EventJsonValues;
import io.tapstate.core.event.ConvertedValue;
import io.tapdata.entity.schema.value.DateTime;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

/** Converts connector values to a bounded JSON-safe logical preview representation. */
final class PreviewJsonValues {

    private static final int MAX_DEPTH = 128;

    private PreviewJsonValues() {
    }

    static Object normalize(Object value) {
        return normalize(value, 0);
    }

    private static Object normalize(Object value, int depth) {
        if (depth > MAX_DEPTH) {
            throw new IllegalArgumentException("event value exceeds the maximum JSON nesting depth");
        }
        return switch (value) {
            case null -> null;
            case ConvertedValue converted -> normalize(converted.value(), depth + 1);
            case DateTime dateTime -> dateTime.toInstant().toString();
            case Map<?, ?> map -> normalizeMap(map, depth);
            case Collection<?> collection -> collection.stream()
                    .map(item -> normalize(item, depth + 1)).toList();
            default -> value.getClass().isArray()
                    ? normalizeArray(value, depth)
                    : EventJsonValues.normalize(value);
        };
    }

    private static Map<String, Object> normalizeMap(Map<?, ?> source, int depth) {
        Map<String, Object> result = new LinkedHashMap<>(source.size());
        source.forEach((key, value) -> {
            if (!(key instanceof String field)) {
                throw new IllegalArgumentException("event object field names must be strings");
            }
            result.put(field, normalize(value, depth + 1));
        });
        return result;
    }

    private static Object normalizeArray(Object source, int depth) {
        int length = Array.getLength(source);
        ArrayList<Object> result = new ArrayList<>(length);
        for (int index = 0; index < length; index++) {
            result.add(normalize(Array.get(source, index), depth + 1));
        }
        return result;
    }
}
