package io.tapstate.adapters.pdk;

import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.schema.value.DateTime;
import io.tapstate.core.common.JsonReader;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.ConvertedValue;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.EventJsonValues;
import io.tapstate.core.model.SourceResource;
import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Renders preview rows with target codecs when the target representation is available. */
public final class PdkTargetPreviewRenderer {

    public static final String LOGICAL_JSON = "logical-json";
    public static final String MONGO_EXTENDED_JSON = "mongo-extended-json";

    private static final String PREVIEW_STREAM = "__preview.result";
    private static final int MAX_NESTING = 128;

    private final ConnectorProvisioner connectors;

    public PdkTargetPreviewRenderer(ConnectorProvisioner connectors) {
        this.connectors = Objects.requireNonNull(connectors, "connectors");
    }

    /**
     * Rehydrates portable values with the selected target's codecs, then renders Mongo targets as
     * canonical Extended JSON. Opening a connector here constructs it and registers codecs only; it
     * does not initialize, start, or write to the target.
     */
    public Result render(SourceResource target, List<Map<String, Object>> documents, Runnable checkpoint) {
        Objects.requireNonNull(documents, "documents");
        Objects.requireNonNull(checkpoint, "checkpoint");
        if (target == null || !isMongoConnector(target.connector())) {
            return logicalJson(documents, checkpoint);
        }

        ConnectorRef ref;
        try {
            ref = connectors.resolve(target.connector());
        } catch (TapstateException unresolvedTarget) {
            return logicalJson(documents, checkpoint);
        }
        PdkConnector connector = PdkConnector.open(target.connector(), ref, target.config());

        try (connector) {
            List<Map<String, Object>> rendered = connector.underLoader(() -> {
                ClassLoader loader = connector.connector().getClass().getClassLoader();
                if (!hasBsonJsonSupport(loader)) {
                    return null;
                }
                List<Map<String, Object>> output = new ArrayList<>(documents.size());
                for (Map<String, Object> document : documents) {
                    checkpoint.run();
                    TapEvent targetEvent = TapEventCodec.encode(
                            Envelope.read(0L, PREVIEW_STREAM, document, null), connector.codecs());
                    Map<String, Object> targetValues = ((TapInsertRecordEvent) targetEvent).getAfter();
                    String extendedJson = writeExtendedJson(targetValues, loader);
                    Object parsed = JsonReader.parse(extendedJson);
                    if (!(parsed instanceof Map<?, ?> map)) {
                        throw new IllegalStateException("MongoDB preview renderer returned a non-document value");
                    }
                    output.add(stringKeyedMap(map));
                }
                return List.copyOf(output);
            });
            if (rendered == null) {
                return logicalJson(documents, checkpoint);
            }
            return new Result(MONGO_EXTENDED_JSON, rendered);
        } catch (Throwable failure) {
            if (failure instanceof InterruptedException) {
                Thread.currentThread().interrupt();
                throw new java.util.concurrent.CancellationException("preview target rendering was interrupted");
            }
            if (failure instanceof RuntimeException runtime) {
                throw runtime;
            }
            if (failure instanceof Error error) {
                throw error;
            }
            throw new IllegalStateException("MongoDB target preview rendering failed", failure);
        }
    }

    private static Result logicalJson(List<Map<String, Object>> documents, Runnable checkpoint) {
        List<Map<String, Object>> normalized = new ArrayList<>(documents.size());
        for (Map<String, Object> document : documents) {
            checkpoint.run();
            Object value = logicalValue(document);
            if (!(value instanceof Map<?, ?> map)) {
                throw new IllegalStateException("preview result is not a JSON document");
            }
            normalized.add(stringKeyedMap(map));
        }
        return new Result(LOGICAL_JSON, normalized);
    }

    private static Object logicalValue(Object value) {
        return logicalValue(value, 0);
    }

    private static Object logicalValue(Object value, int depth) {
        if (depth > MAX_NESTING) {
            throw new IllegalArgumentException("preview document exceeds the JSON nesting limit");
        }
        return switch (value) {
            case null -> null;
            case ConvertedValue converted -> logicalValue(converted.value(), depth + 1);
            case DateTime dateTime -> dateTime.toInstant().toString();
            case Map<?, ?> map -> {
                Map<String, Object> normalized = new LinkedHashMap<>();
                map.forEach((key, item) -> {
                    if (!(key instanceof String field)) {
                        throw new IllegalArgumentException("preview document field names must be strings");
                    }
                    normalized.put(field, logicalValue(item, depth + 1));
                });
                yield normalized;
            }
            case Collection<?> collection -> collection.stream()
                    .map(item -> logicalValue(item, depth + 1)).toList();
            case byte[] bytes -> EventJsonValues.normalize(bytes);
            default -> value != null && value.getClass().isArray()
                    ? logicalArray(value, depth)
                    : EventJsonValues.normalize(value);
        };
    }

    private static List<Object> logicalArray(Object array, int depth) {
        List<Object> values = new ArrayList<>(Array.getLength(array));
        for (int index = 0; index < Array.getLength(array); index++) {
            values.add(logicalValue(Array.get(array, index), depth + 1));
        }
        return values;
    }

    private static boolean isMongoConnector(String connectorId) {
        String normalized = connectorId.toLowerCase(java.util.Locale.ROOT);
        return normalized.equals("mongo") || normalized.contains("mongodb");
    }

    private static boolean hasBsonJsonSupport(ClassLoader loader) {
        try {
            Class.forName("org.bson.Document", false, loader);
            Class.forName("org.bson.json.JsonWriterSettings", false, loader);
            Class.forName("org.bson.json.JsonMode", false, loader);
            return true;
        } catch (ClassNotFoundException missingBson) {
            return false;
        }
    }

    private static String writeExtendedJson(Map<String, Object> document, ClassLoader loader) throws Exception {
        Class<?> documentType = Class.forName("org.bson.Document", true, loader);
        Map<String, Object> bsonValues = new LinkedHashMap<>();
        for (Map.Entry<String, Object> entry : document.entrySet()) {
            bsonValues.put(entry.getKey(), bsonValue(entry.getValue(), documentType, loader, 0));
        }
        Constructor<?> constructor = documentType.getConstructor(Map.class);
        Object bsonDocument = constructor.newInstance(bsonValues);

        Class<?> settingsType = Class.forName("org.bson.json.JsonWriterSettings", true, loader);
        Object builder = settingsType.getMethod("builder").invoke(null);
        Class<?> modeType = Class.forName("org.bson.json.JsonMode", true, loader);
        @SuppressWarnings({"rawtypes", "unchecked"})
        Object extendedMode = Enum.valueOf((Class<? extends Enum>) modeType.asSubclass(Enum.class), "EXTENDED");
        Method outputMode = builder.getClass().getMethod("outputMode", modeType);
        outputMode.invoke(builder, extendedMode);
        Object settings = builder.getClass().getMethod("build").invoke(builder);
        Method toJson = documentType.getMethod("toJson", settingsType);
        try {
            return (String) toJson.invoke(bsonDocument, settings);
        } catch (InvocationTargetException failure) {
            Throwable cause = failure.getCause();
            if (cause instanceof Exception exception) {
                throw exception;
            }
            if (cause instanceof Error error) {
                throw error;
            }
            throw failure;
        }
    }

    private static Object bsonValue(Object value, Class<?> documentType, ClassLoader loader, int depth)
            throws ReflectiveOperationException {
        if (depth > MAX_NESTING) {
            throw new IllegalArgumentException("preview document exceeds the BSON nesting limit");
        }
        if (value instanceof Map<?, ?> map && !documentType.isInstance(value)) {
            Map<String, Object> nested = new LinkedHashMap<>();
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                if (!(entry.getKey() instanceof String key)) {
                    throw new IllegalArgumentException("preview document field names must be strings");
                }
                nested.put(key, bsonValue(entry.getValue(), documentType, loader, depth + 1));
            }
            return documentType.getConstructor(Map.class).newInstance(nested);
        }
        if (value instanceof Collection<?> collection) {
            List<Object> nested = new ArrayList<>(collection.size());
            for (Object item : collection) {
                nested.add(bsonValue(item, documentType, loader, depth + 1));
            }
            return nested;
        }
        if (value != null && value.getClass().isArray() && !(value instanceof byte[])) {
            List<Object> nested = new ArrayList<>(Array.getLength(value));
            for (int index = 0; index < Array.getLength(value); index++) {
                nested.add(bsonValue(Array.get(value, index), documentType, loader, depth + 1));
            }
            return nested;
        }
        if (value instanceof BigDecimal decimal) {
            try {
                Class<?> decimalType = Class.forName("org.bson.types.Decimal128", true, loader);
                return decimalType.getConstructor(BigDecimal.class).newInstance(decimal);
            } catch (ClassNotFoundException missingDecimal128) {
                return value;
            }
        }
        if (value instanceof Date date) {
            return date;
        }
        return value;
    }

    private static Map<String, Object> stringKeyedMap(Map<?, ?> source) {
        Map<String, Object> result = new LinkedHashMap<>();
        source.forEach((key, value) -> {
            if (!(key instanceof String field)) {
                throw new IllegalStateException("preview result contains a non-string field name");
            }
            result.put(field, value);
        });
        return Collections.unmodifiableMap(result);
    }

    public record Result(String format, List<Map<String, Object>> documents) {
        public Result {
            Objects.requireNonNull(format, "format");
            documents = documents.stream()
                    .map(document -> Collections.unmodifiableMap(new LinkedHashMap<>(document)))
                    .toList();
        }
    }
}
