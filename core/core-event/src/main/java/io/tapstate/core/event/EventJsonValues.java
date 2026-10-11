package io.tapstate.core.event;

import io.tapstate.core.common.JsonValues;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Date;

/** Converts event values, including their portable type carriers, to a logical JSON representation. */
public final class EventJsonValues {

    private static final int MAX_DEPTH = 128;

    private EventJsonValues() {
    }

    public static Object normalize(Object value) {
        return normalize(value, 0);
    }

    /** Measures the compact JSON UTF-8 representation, stopping once the supplied limit is exceeded. */
    public static long encodedSize(Object value, long limit) {
        if (limit < 0) {
            throw new IllegalArgumentException("limit must not be negative");
        }
        Meter meter = new Meter(limit);
        measure(value, meter, 0);
        return meter.size;
    }

    private static Object normalize(Object value, int depth) {
        if (depth > MAX_DEPTH) {
            throw new IllegalArgumentException("event value exceeds the maximum JSON nesting depth");
        }
        return switch (value) {
            case null -> null;
            case ConvertedValue converted -> normalize(converted.value(), depth + 1);
            case Bytes bytes -> Map.of(
                    "$binary", Base64.getEncoder().encodeToString(bytes.value()),
                    "$tag", bytes.tag());
            case Map<?, ?> map -> normalizeMap(map, depth);
            case Collection<?> collection -> {
                ArrayList<Object> result = new ArrayList<>(collection.size());
                for (Object item : collection) {
                    result.add(normalize(item, depth + 1));
                }
                yield result;
            }
            default -> value != null && value.getClass().isArray()
                    ? normalizeArray(value, depth)
                    : JsonValues.normalize(value);
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

    private static void measure(Object value, Meter meter, int depth) {
        if (meter.exceeded()) {
            return;
        }
        if (depth > MAX_DEPTH) {
            throw new IllegalArgumentException("event value exceeds the maximum JSON nesting depth");
        }
        switch (value) {
            case null -> meter.add(4);
            case ConvertedValue converted -> measure(converted.value(), meter, depth + 1);
            case Bytes bytes -> {
                meter.add(22L + 4L * ((bytes.value().length + 2L) / 3L)
                        + JsonValues.encodedSize(bytes.tag(), Long.MAX_VALUE));
            }
            case Map<?, ?> map -> {
                meter.add(2);
                boolean first = true;
                for (Map.Entry<?, ?> entry : map.entrySet()) {
                    if (meter.exceeded()) {
                        return;
                    }
                    if (!(entry.getKey() instanceof String field)) {
                        throw new IllegalArgumentException("event object field names must be strings");
                    }
                    if (!first) {
                        meter.add(1);
                    }
                    first = false;
                    meter.add(JsonValues.encodedSize(field, meter.remaining()));
                    meter.add(1);
                    measure(entry.getValue(), meter, depth + 1);
                }
            }
            case Collection<?> collection -> {
                meter.add(2);
                boolean first = true;
                for (Object item : collection) {
                    if (meter.exceeded()) {
                        return;
                    }
                    if (!first) {
                        meter.add(1);
                    }
                    first = false;
                    measure(item, meter, depth + 1);
                }
            }
            default -> {
                if (value.getClass().isArray()) {
                    meter.add(2);
                    int length = Array.getLength(value);
                    for (int index = 0; index < length && !meter.exceeded(); index++) {
                        if (index > 0) {
                            meter.add(1);
                        }
                        measure(Array.get(value, index), meter, depth + 1);
                    }
                } else if (value instanceof Date || value instanceof java.time.temporal.TemporalAccessor) {
                    meter.add(JsonValues.encodedSize(value, meter.remaining()));
                } else if (value instanceof Number || value instanceof String
                        || value instanceof Boolean || value instanceof Character || value instanceof byte[]) {
                    meter.add(JsonValues.encodedSize(value, meter.remaining()));
                } else {
                    throw new IllegalArgumentException(
                            "value is not JSON-compatible: " + value.getClass().getName());
                }
            }
        }
    }

    private static final class Meter {
        private final long limit;
        private long size;

        private Meter(long limit) {
            this.limit = limit;
        }

        private void add(long amount) {
            if (size > limit || amount > limit - size) {
                size = limit == Long.MAX_VALUE ? Long.MAX_VALUE : limit + 1;
            } else {
                size += amount;
            }
        }

        private boolean exceeded() {
            return size > limit;
        }

        private long remaining() {
            return size > limit ? 0 : limit - size;
        }
    }
}
