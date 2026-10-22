package io.tapstate.adapters.pdk;

import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.codec.FromTapValueCodec;
import io.tapdata.entity.codec.filter.TapCodecsFilterManager;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.value.*;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.JsonParser;
import io.tapstate.core.event.Bytes;
import io.tapstate.core.event.ConvertedValue;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BinaryOperator;
import java.util.function.BiFunction;
import java.util.function.Supplier;

/** Runs the PDK conversion contract while keeping its objects inside the adapter. */
final class PdkValueConversions {
    private static final Map<Integer, java.util.function.IntFunction<DateTime>> TIME_ORIGINS = timeOrigins();
    private static final Map<String, Supplier<TapValue<?, ?>>> VALUES = Map.ofEntries(
            Map.entry("TapArrayValue", TapArrayValue::new), Map.entry("TapBinaryValue", TapBinaryValue::new),
            Map.entry("TapBooleanValue", TapBooleanValue::new), Map.entry("TapDateValue", TapDateValue::new),
            Map.entry("TapDateTimeValue", TapDateTimeValue::new), Map.entry("TapMapValue", TapMapValue::new),
            Map.entry("TapNumberValue", TapNumberValue::new), Map.entry("TapStringValue", TapStringValue::new),
            Map.entry("TapTimeValue", TapTimeValue::new), Map.entry("TapYearValue", TapYearValue::new),
            Map.entry("TapMoneyValue", TapMoneyValue::new), Map.entry("TapJsonValue", TapJsonValue::new),
            Map.entry("TapXmlValue", TapXmlValue::new), Map.entry("TapRawValue", TapRawValue::new),
            Map.entry("TapInputStreamValue", TapInputStreamValue::new));

    private static final Map<Class<?>, String> VALUE_KINDS = valueKinds();
    private static final Map<Class<?>, String> TYPE_KINDS = typeKinds();
    private static final ClassValue<String> CANONICAL_KINDS = new ClassValue<>() {
        @Override
        protected String computeValue(Class<?> type) {
            for (Class<?> base = type; base != null; base = base.getSuperclass()) {
                String kind = VALUE_KINDS.get(base);
                if (kind != null) return kind;
            }
            return "";
        }
    };
    private final TapCodecsRegistry codecs;
    private final PdkConversionManager source;
    private final PdkConversionManager target;

    PdkValueConversions(TapCodecsRegistry codecs) {
        this.codecs = java.util.Objects.requireNonNull(codecs, "codecs");
        source = new PdkConversionManager(codecs);
        target = new PdkConversionManager(new RetainingTargetCodecs(codecs));
    }

    TapCodecsRegistry codecs() {
        return codecs;
    }

    /** An absent row image stays absent rather than becoming an empty document. */
    Optional<Map<String, Object>> read(Map<String, Object> row, PdkRowSchema schema, BiFunction<Object, String, String> origin,
            BinaryOperator<Object> compatibility) {
        if (row == null) {
            return Optional.empty();
        }
        PdkSourceRow converted = new PdkSourceRow(row);
        source.transformToTapValueMap(converted, schema.fields());
        ReadContext context = new ReadContext(schema, origin, compatibility, converted);
        converted.replaceAll((key, value) -> carry(value, row.get(key), key, key, context));
        return Optional.of(converted);
    }

    private record ReadContext(PdkRowSchema schema, BiFunction<Object, String, String> origin,
            BinaryOperator<Object> compatibility, PdkSourceRow originals) { }

    private static Object carry(Object value, Object original, String path, String schemaPath, ReadContext context) {
        if (value instanceof TapValue<?, ?> tap) {
            return carryTapValue(tap, original, path, schemaPath, context);
        }
        if (value instanceof Map<?, ?> map) {
            return carryMap(map, original, path, schemaPath, context);
        }
        if (value instanceof Collection<?> collection) {
            return carryList(collection, original, schemaPath, context);
        }
        return portableScalar(value);
    }

    private static Object carryTapValue(TapValue<?, ?> tap, Object original, String path,
            String schemaPath, ReadContext context) {
        original = context.originals().original(tap, original);
        Object payload = tap.getValue();
        if (payload == null) {
            Object fallback = original instanceof TapValue<?, ?> previous ? previous.getOriginValue() : original;
            return fallback == null ? null : carry(fallback, fallback, path, schemaPath, context);
        }
        if (original != null) {
            payload = context.compatibility().apply(original, payload);
        }
        Object originalPayload = original instanceof TapValue<?, ?> previous ? previous.getValue() : original;
        String temporal = payload instanceof DateTime time ? describeTime(time) : null;
        Object portable = portablePayload(payload, originalPayload, path, schemaPath, context);
        String kind = CANONICAL_KINDS.get(tap.getClass());
        if (kind.isEmpty()) kind = TYPE_KINDS.get(tap.tapTypeClass());
        // The PDK flattens array indexes when finding fields. An element must not inherit
        // the container's declared database type when the schema cannot name the element.
        TapField field = positionalElement(schemaPath) ? null
                : context.schema().fields().get(TapCodecsFilterManager.fieldName(schemaPath));
        boolean matches = field != null && tap.tapTypeClass().isInstance(field.getTapType());
        var type = tap.getTapType();
        if (type == null && matches) {
            // Already-converted values are skipped by the PDK filter and may inherit their
            // type from the table. Read that metadata without modifying a connector-owned value.
            type = field.getTapType();
        }
        String declared = declaredType(tap, matches ? field : null, original, path, context);
        return new ConvertedValue(portable, declared, kind,
                context.schema().description(type), temporal);
    }

    private static Object portablePayload(Object payload, Object original, String path,
            String schemaPath, ReadContext context) {
        if (payload instanceof DateTime time) {
            return time.isContainsIllegal() ? time.getIllegalDate() : time.toInstant();
        }
        if (payload instanceof ByteData bytes) {
            return new Bytes(bytes.getType(), bytes.getValue());
        }
        return carry(payload, original, path, schemaPath, context);
    }

    private static String declaredType(TapValue<?, ?> tap, TapField field, Object original,
            String path, ReadContext context) {
        if (!context.originals().wasConverted(tap) && tap.getOriginType() != null) {
            return tap.getOriginType();
        }
        if (path != null && tap.getOriginType() != null) {
            return tap.getOriginType();
        }
        if (field != null) {
            return field.getDataType();
        }
        return original == null ? null : context.origin().apply(original, path);
    }

    private static Map<?, ?> carryMap(Map<?, ?> map, Object original, String path,
            String schemaPath, ReadContext context) {
        Map<?, ?> source = original instanceof Map<?, ?> raw ? raw : Map.of();
        Map<?, ?> result = PdkContainerValues.map(map, (key, index, element) -> carry(element, source.get(key),
                path == null || !(key instanceof String) ? null : path + "." + key,
                PdkContainerValues.childPath(schemaPath, key, index), context));
        // Driver container classes are needed by source codecs, but cannot cross loader boundaries.
        return result.getClass().getClassLoader() == null ? result : new LinkedHashMap<>(result);
    }

    private static Collection<?> carryList(Collection<?> collection, Object original,
            String schemaPath, ReadContext context) {
        List<?> source = original instanceof List<?> raw ? raw : List.of();
        Collection<?> result = PdkContainerValues.collection(collection, (key, index, element) ->
                carry(element, index < source.size() ? source.get(index) : null,
                        null, schemaPath + ".#" + index, context));
        return result.getClass().getClassLoader() == null ? result : new ArrayList<>(result);
    }

    private static Object portableScalar(Object value) {
        if (value instanceof Integer || value instanceof Short || value instanceof Byte) {
            return ((Number) value).longValue();
        }
        if (value instanceof Float number) {
            return number.doubleValue();
        }
        if (value instanceof java.math.BigInteger number && number.bitLength() < Long.SIZE) {
            return number.longValue();
        }
        if (value instanceof java.time.ZonedDateTime time) {
            return time.toInstant();
        }
        if (value instanceof java.time.OffsetDateTime time) {
            return time.toInstant();
        }
        return value;
    }

    /** Preserve the distinction between an absent image and a present, empty document. */
    Optional<Map<String, Object>> write(Map<String, Object> row,
            java.util.function.Function<ConvertedValue, Object> legacy) {
        if (row == null) {
            return Optional.empty();
        }
        Map<String, Object> restored = new LinkedHashMap<>(row.size());
        row.forEach((key, value) -> restored.put(key, restore(value, legacy)));
        target.transformFromTapValueMap(restored);
        return Optional.of(restored);
    }

    private static final class RetainingTargetCodecs extends TapCodecsRegistry {
        private final TapCodecsRegistry delegate;

        private RetainingTargetCodecs(TapCodecsRegistry delegate) {
            this.delegate = delegate;
        }

        @Override
        public <T extends TapValue<?, ?>> FromTapValueCodec<T> getCustomFromTapValueCodec(Class<T> type) {
            return retaining(delegate.getCustomFromTapValueCodec(type));
        }

        @Override
        public <T extends TapValue<?, ?>> FromTapValueCodec<T> getDefaultFromTapValueCodec(Class<T> type) {
            return retaining(delegate.getDefaultFromTapValueCodec(type));
        }

        private static <T extends TapValue<?, ?>> FromTapValueCodec<T> retaining(FromTapValueCodec<T> codec) {
            if (codec == null) {
                return null;
            }
            return value -> {
                Object result = codec.fromTapValue(value);
                // Retain before the PDK iterator acts: for an array element, returning null can
                // remove a whole container rather than leave a box for a later pass to unwrap.
                return result == null ? value.getValue() : result;
            };
        }
    }

    private static Object restore(Object value, java.util.function.Function<ConvertedValue, Object> legacy) {
        if (value instanceof ConvertedValue carrier) {
            return restoreCarrier(carrier, legacy);
        }
        if (value instanceof Map<?, ?> map) {
            return new TapMapValue(restoreMap(map, legacy));
        }
        if (value instanceof Collection<?> collection) {
            return new TapArrayValue(restoreList(collection, legacy));
        }
        return value;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private static Object restoreCarrier(ConvertedValue carrier,
            java.util.function.Function<ConvertedValue, Object> legacy) {
        if (carrier.valueKind() == null) {
            return legacy.apply(carrier);
        }
        Supplier<TapValue<?, ?>> factory = VALUES.get(carrier.valueKind());
        if (factory == null) {
            // A newer producer may know a kind this runtime does not. Keep the portable payload
            // usable without loading source-side classes or guessing a target driver type.
            return restore(carrier.value(), legacy);
        }
        TapValue tap = factory.get();
        // A compatibility correction can preserve an exact numeric value wider than the PDK box.
        if (tap instanceof TapNumberValue && !(carrier.value() instanceof Double)) {
            return carrier.value();
        }
        tap.setValue(restorePayload(carrier, legacy));
        tap.setOriginType(carrier.originType());
        if (carrier.typeDescription() != null) {
            tap.setTapType((io.tapdata.entity.schema.type.TapType)
                    parser().fromJson(carrier.typeDescription(), tap.tapTypeClass()));
        }
        return tap;
    }

    private static Object restorePayload(ConvertedValue carrier,
            java.util.function.Function<ConvertedValue, Object> legacy) {
        if (carrier.temporalDescription() != null) {
            return restoreTime(carrier.temporalDescription(), carrier.valueKind());
        }
        Object payload = carrier.value();
        if (payload instanceof Bytes bytes) {
            return new ByteData(bytes.tag(), bytes.value());
        }
        if (payload instanceof Map<?, ?> map) {
            return restoreMap(map, legacy);
        }
        if (payload instanceof Collection<?> collection) {
            return restoreList(collection, legacy);
        }
        return payload;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private static Map<String, Object> restoreMap(Map<?, ?> map,
            java.util.function.Function<ConvertedValue, Object> legacy) {
        Map<Object, Object> result = new LinkedHashMap<>(map.size());
        map.forEach((key, value) -> result.put(key, restore(value, legacy)));
        // TapMapValue's generic signature is narrower than portable nested maps. The host's
        // iterator retains these key objects without casting or changing their representation.
        return (Map) result;
    }

    private static List<Object> restoreList(Collection<?> collection,
            java.util.function.Function<ConvertedValue, Object> legacy) {
        List<Object> result = new ArrayList<>(collection.size());
        collection.forEach(value -> result.add(restore(value, legacy)));
        return result;
    }

    private static Map<Class<?>, String> valueKinds() {
        Map<Class<?>, String> result = new LinkedHashMap<>();
        VALUES.forEach((kind, factory) -> result.put(factory.get().getClass(), kind));
        return Map.copyOf(result);
    }

    private static Map<Class<?>, String> typeKinds() {
        Map<Class<?>, String> result = new LinkedHashMap<>();
        VALUES.forEach((kind, factory) -> result.put(factory.get().tapTypeClass(), kind));
        return Map.copyOf(result);
    }

    private static JsonParser parser() {
        return java.util.Objects.requireNonNull(InstanceFactory.instance(JsonParser.class), "PDK JSON provider");
    }

    private static boolean positionalElement(String path) {
        String last = path.substring(path.lastIndexOf('.') + 1);
        return last.length() > 1 && last.charAt(0) == '#'
                && last.substring(1).chars().allMatch(Character::isDigit);
    }

    private static String describeTime(DateTime time) {
        // DateTime also exposes mutable JavaScript Date properties. Serializing all its bean
        // getters and invoking their setters on read rounds nanoseconds through getTime().
        Map<String, Object> state = new LinkedHashMap<>();
        state.put("seconds", time.getSeconds());
        state.put("nano", time.getNano());
        state.put("fraction", time.getFraction());
        state.put("origin", time.getOriginType());
        state.put("zone", time.getTimeZone() == null ? null : time.getTimeZone().getID());
        state.put("illegal", time.isContainsIllegal() ? time.getIllegalDate() : null);
        state.put("bytes", time.getOriginBytes() == null ? null
                : java.util.Base64.getEncoder().encodeToString(time.getOriginBytes()));
        return parser().toJson(state);
    }

    private static DateTime restoreTime(String description, String valueKind) {
        Map<String, Object> state = parser().fromJsonObject(description);
        DateTime result;
        if (state.get("illegal") instanceof String illegal) {
            String kind = switch (valueKind) {
                case "TapDateValue" -> DateTime.DATE_TYPE;
                case "TapTimeValue" -> DateTime.TIME_TYPE;
                case "TapYearValue" -> DateTime.YEAR_TYPE;
                default -> DateTime.DATETIME_TYPE;
            };
            // The constructor consumes raw component separators, while getIllegalDate exposes
            // formatted text. Invert those separators and verify the representation is unchanged.
            result = new DateTime(illegal.replace(' ', '-').replace(':', '-').replace('.', '-'), kind);
            if (!illegal.equals(result.getIllegalDate())) {
                throw new IllegalArgumentException("unsupported stored illegal temporal representation");
            }
        } else {
            var constructor = TIME_ORIGINS.get(((Number) state.get("origin")).intValue());
            int fraction = ((Number) state.get("fraction")).intValue();
            // Unknown origin representations still have an exact portable clock. Use a known
            // fractional representation rather than ask the PDK to reconstruct an unknown driver.
            result = constructor == null ? new DateTime(java.math.BigDecimal.ZERO, fraction)
                    : constructor.apply(fraction);
            result.setSeconds(((Number) state.get("seconds")).longValue());
            result.setNano(((Number) state.get("nano")).intValue());
        }
        if (state.get("zone") instanceof String zone) {
            result.setTimeZone(java.util.TimeZone.getTimeZone(zone));
        }
        if (state.get("bytes") instanceof String bytes) {
            result.setOriginBytes(java.util.Base64.getDecoder().decode(bytes));
        }
        return result;
    }

    private static Map<Integer, java.util.function.IntFunction<DateTime>> timeOrigins() {
        // Read origin identifiers from the PDK constructors rather than duplicating its private
        // constants. Rebuild the original representation, then restore its exact clock fields.
        List<java.util.function.IntFunction<DateTime>> constructors = List.of(
                fraction -> new DateTime(), fraction -> new DateTime(Instant.EPOCH),
                fraction -> new DateTime(Instant.EPOCH.atZone(java.time.ZoneOffset.UTC)),
                fraction -> new DateTime(new java.util.Date(0)),
                fraction -> new DateTime(new java.sql.Date(0)),
                fraction -> new DateTime(new java.sql.Time(0)),
                fraction -> new DateTime(java.sql.Timestamp.from(Instant.EPOCH)),
                fraction -> new DateTime(java.math.BigDecimal.ZERO, fraction),
                fraction -> new DateTime(0L, fraction), fraction -> new DateTime(0L),
                fraction -> new DateTime(java.time.LocalDateTime.ofInstant(Instant.EPOCH, java.time.ZoneOffset.UTC)));
        Map<Integer, java.util.function.IntFunction<DateTime>> result = new LinkedHashMap<>();
        constructors.forEach(constructor -> result.put(constructor.apply(3).getOriginType(), constructor));
        return Map.copyOf(result);
    }
}
