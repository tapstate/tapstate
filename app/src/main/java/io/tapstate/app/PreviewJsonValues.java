package io.tapstate.app;

import io.tapstate.core.event.EventJsonValues;
import io.tapstate.core.event.ConvertedValue;
import io.tapstate.core.event.RowMapKeys;
import io.tapstate.adapters.pdk.PdkPreviewDateTimes;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/** Converts connector values to a bounded JSON-safe logical preview representation. */
final class PreviewJsonValues {

    private static final int MAX_DEPTH = 128;

    private PreviewJsonValues() {
    }

    static Object normalize(Object value) {
        return normalize(value, 0, "$");
    }

    private static Object normalize(Object value, int depth, String path) {
        if (depth > MAX_DEPTH) {
            throw new IllegalArgumentException("event value exceeds the maximum JSON nesting depth");
        }
        if (PdkPreviewDateTimes.isDateTime(value)) {
            return PdkPreviewDateTimes.toIsoString(value);
        }
        return switch (value) {
            case null -> null;
            case ConvertedValue converted -> normalize(converted.value(), depth + 1, path);
            case Map<?, ?> map -> normalizeMap(map, depth, path);
            case Collection<?> collection -> {
                ArrayList<Object> result = new ArrayList<>(collection.size());
                int index = 0;
                for (Object item : collection) {
                    result.add(normalize(item, depth + 1, path + "/" + index++));
                }
                yield Collections.unmodifiableList(result);
            }
            default -> value.getClass().isArray()
                    ? normalizeArray(value, depth, path)
                    : EventJsonValues.normalize(value);
        };
    }

    private static Map<String, Object> normalizeMap(Map<?, ?> source, int depth, String path) {
        Map<String, Object> result = new LinkedHashMap<>(source.size());
        source.forEach((key, value) -> {
            String field = RowMapKeys.requireString(key, path);
            result.put(field, normalize(value, depth + 1, RowMapKeys.childPath(path, field)));
        });
        return result;
    }

    private static Object normalizeArray(Object source, int depth, String path) {
        int length = Array.getLength(source);
        ArrayList<Object> result = new ArrayList<>(length);
        for (int index = 0; index < length; index++) {
            result.add(normalize(Array.get(source, index), depth + 1, path + "/" + index));
        }
        return result;
    }
}
