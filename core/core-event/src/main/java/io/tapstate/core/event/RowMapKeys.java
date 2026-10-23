package io.tapstate.core.event;

import io.tapstate.core.common.TapstateException;
import java.util.Collection;
import java.util.IdentityHashMap;
import java.util.Map;

/** Checks map keys without normalizing values, invoking key conversions, or changing the input. */
public final class RowMapKeys {
    private RowMapKeys() { }

    /** Paths start at {@code $}; subsequent segments use JSON Pointer escaping. */
    public static void validate(Object value) {
        validate(value, "$", new IdentityHashMap<>(), 0, Integer.MAX_VALUE);
    }

    public static void validate(Object value, String path) {
        validate(value, path, new IdentityHashMap<>(), 0, Integer.MAX_VALUE);
    }

    /** Keeps preview validation inside the renderer's existing nesting boundary. */
    public static void validate(Object value, int maxDepth) {
        validate(value, "$", new IdentityHashMap<>(), 0, maxDepth);
    }

    /** The path identifies the containing map; key values are never included in the diagnostic. */
    public static String requireString(Object key, String path) {
        if (key instanceof String field) {
            return field;
        }
        throw new TapstateException(RowValueError.NON_STRING_MAP_KEY,
                Map.of("path", path, "keyType", key == null ? "null" : key.getClass().getName()), null);
    }

    public static String childPath(String path, String field) {
        return path + "/" + field.replace("~", "~0").replace("/", "~1");
    }

    private static void validate(Object value, String path, IdentityHashMap<Object, Boolean> seen,
            int depth, int maxDepth) {
        if (depth > maxDepth) {
            throw new IllegalArgumentException("row value exceeds the nesting limit");
        }
        if (value instanceof ConvertedValue converted) {
            validate(converted.value(), path, seen, depth + 1, maxDepth);
        } else if (value instanceof Map<?, ?> map && seen.put(map, Boolean.TRUE) == null) {
            map.forEach((key, child) -> validate(child, childPath(path, requireString(key, path)),
                    seen, depth + 1, maxDepth));
        } else if (value instanceof Collection<?> values && seen.put(values, Boolean.TRUE) == null) {
            int index = 0;
            for (Object child : values) {
                validate(child, path + "/" + index++, seen, depth + 1, maxDepth);
            }
        } else if (value instanceof Object[] values && seen.put(values, Boolean.TRUE) == null) {
            for (int index = 0; index < values.length; index++) {
                validate(values[index], path + "/" + index, seen, depth + 1, maxDepth);
            }
        }
    }
}
