package io.tapstate.core.common;

import java.lang.reflect.Array;
import java.time.temporal.TemporalAccessor;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;

/** Converts common connector values to JSON primitives and measures their encoded size. */
public final class JsonValues {

    private static final int MAX_DEPTH = 128;

    private JsonValues() {
    }

    public static Object normalize(Object value) {
        return normalize(value, 0);
    }

    /** Returns the exact compact JSON UTF-8 size, or {@code limit + 1} as soon as it is exceeded. */
    public static long encodedSize(Object value, long limit) {
        if (limit < 0) {
            throw new IllegalArgumentException("limit must not be negative");
        }
        Meter meter = new Meter(limit);
        measure(value, meter, 0);
        return meter.size;
    }

    private static Object normalize(Object value, int depth) {
        checkDepth(depth);
        return switch (value) {
            case null -> null;
            case String ignored -> value;
            case Boolean ignored -> value;
            case Number number -> normalizeNumber(number);
            case Character character -> character.toString();
            case Date date -> date.toInstant().toString();
            case TemporalAccessor temporal -> temporal.toString();
            case byte[] bytes -> Map.of("$binary", Base64.getEncoder().encodeToString(bytes));
            case Map<?, ?> map -> normalizeMap(map, depth);
            case Collection<?> collection -> {
                ArrayList<Object> result = new ArrayList<>(collection.size());
                for (Object item : collection) {
                    result.add(normalize(item, depth + 1));
                }
                yield result;
            }
            default -> {
                if (!value.getClass().isArray()) {
                    throw new IllegalArgumentException(
                            "value is not JSON-compatible: " + value.getClass().getName());
                }
                yield normalizeArray(value, depth);
            }
        };
    }

    private static Number normalizeNumber(Number number) {
        if ((number instanceof Double doubleValue && !Double.isFinite(doubleValue))
                || (number instanceof Float floatValue && !Float.isFinite(floatValue))) {
            throw new IllegalArgumentException("non-finite numbers are not valid JSON values");
        }
        return number;
    }

    private static Map<String, Object> normalizeMap(Map<?, ?> map, int depth) {
        checkDepth(depth + 1);
        Map<String, Object> result = new LinkedHashMap<>();
        map.forEach((key, value) -> {
            if (!(key instanceof String field)) {
                throw new IllegalArgumentException("JSON object field names must be strings");
            }
            result.put(field, normalize(value, depth + 1));
        });
        return result;
    }

    private static Object normalizeArray(Object values, int depth) {
        checkDepth(depth + 1);
        int length = Array.getLength(values);
        ArrayList<Object> result = new ArrayList<>(length);
        for (int index = 0; index < length; index++) {
            result.add(normalize(Array.get(values, index), depth + 1));
        }
        return result;
    }

    private static void measure(Object value, Meter meter, int depth) {
        if (meter.exceeded()) {
            return;
        }
        checkDepth(depth);
        switch (value) {
            case null -> meter.add(4);
            case String string -> measureString(string, meter);
            case Boolean bool -> meter.add(bool ? 4 : 5);
            case Number number -> {
                normalizeNumber(number);
                meter.add(number.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8).length);
            }
            case Character character -> measureString(character.toString(), meter);
            case Date date -> measureString(date.toInstant().toString(), meter);
            case TemporalAccessor temporal -> measureString(temporal.toString(), meter);
            case byte[] bytes -> {
                long encoded = 4L * ((bytes.length + 2L) / 3L);
                meter.add(14L + encoded);
            }
            case Map<?, ?> map -> measureMap(map, meter, depth);
            case Collection<?> collection -> measureCollection(collection, meter, depth);
            default -> {
                if (!value.getClass().isArray()) {
                    throw new IllegalArgumentException(
                            "value is not JSON-compatible: " + value.getClass().getName());
                }
                measureArray(value, meter, depth);
            }
        }
    }

    private static void measureMap(Map<?, ?> map, Meter meter, int depth) {
        meter.add(2);
        boolean first = true;
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            if (meter.exceeded()) {
                return;
            }
            if (!(entry.getKey() instanceof String key)) {
                throw new IllegalArgumentException("JSON object field names must be strings");
            }
            if (!first) {
                meter.add(1);
            }
            first = false;
            measureString(key, meter);
            meter.add(1);
            measure(entry.getValue(), meter, depth + 1);
        }
    }

    private static void measureCollection(Collection<?> values, Meter meter, int depth) {
        meter.add(2);
        boolean first = true;
        for (Object item : values) {
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

    private static void measureArray(Object values, Meter meter, int depth) {
        meter.add(2);
        int length = Array.getLength(values);
        for (int index = 0; index < length && !meter.exceeded(); index++) {
            if (index > 0) {
                meter.add(1);
            }
            measure(Array.get(values, index), meter, depth + 1);
        }
    }

    private static void measureString(String value, Meter meter) {
        meter.add(2);
        for (int index = 0; index < value.length() && !meter.exceeded(); index++) {
            char current = value.charAt(index);
            switch (current) {
                case '"', '\\', '\b', '\f', '\n', '\r', '\t' -> meter.add(2);
                default -> {
                    if (current < 0x20) {
                        meter.add(6);
                    } else if (Character.isHighSurrogate(current)
                            && index + 1 < value.length()
                            && Character.isLowSurrogate(value.charAt(index + 1))) {
                        meter.add(4);
                        index++;
                    } else if (Character.isSurrogate(current)) {
                        meter.add(1);
                    } else if (current <= 0x7f) {
                        meter.add(1);
                    } else if (current <= 0x7ff) {
                        meter.add(2);
                    } else {
                        meter.add(3);
                    }
                }
            }
        }
    }

    private static void checkDepth(int depth) {
        if (depth > MAX_DEPTH) {
            throw new IllegalArgumentException("JSON value exceeds the maximum nesting depth");
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
    }
}
