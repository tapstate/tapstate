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

    private PdkValueConversions() { }

    static Map<String, Object> read(Map<String, Object> row, TapCodecsRegistry codecs,
            Map<String, TapField> fields, BiFunction<Object, String, String> origin,
            BiFunction<Object, Object, Object> compatibility) {
        if (row == null) {
            return null;
        }
        Map<String, Object> converted = copyRow(row);
        new TapCodecsFilterManager(codecs).transformToTapValueMap(converted, fields);
        Map<String, Object> result = new LinkedHashMap<>(converted.size());
        ReadContext context = new ReadContext(fields, origin, compatibility);
        converted.forEach((key, value) -> result.put(key,
                carry(value, row.get(key), key, key, context)));
        return result;
    }

    private record ReadContext(Map<String, TapField> fields, BiFunction<Object, String, String> origin,
            BiFunction<Object, Object, Object> compatibility) { }

    private static Object carry(Object value, Object original, String path, String schemaPath, ReadContext context) {
        if (value instanceof TapValue<?, ?> tap) {
            Object payload = tap.getValue();
            if (payload == null) {
                return null;
            }
            if (original != null) {
                payload = context.compatibility().apply(original, payload);
            }
            Object originalPayload = original instanceof TapValue<?, ?> previous ? previous.getValue() : original;
            String temporal = payload instanceof DateTime time ? describeTime(time) : null;
            Object portable = payload instanceof DateTime time
                    ? time.isContainsIllegal() ? time.getIllegalDate() : time.toInstant()
                    : payload instanceof ByteData bytes ? new Bytes(bytes.getType(), bytes.getValue())
                    : carry(payload, originalPayload, path, schemaPath, context);
            String kind = tap.getClass().getSimpleName();
            if (!VALUES.containsKey(kind)) {
                throw new IllegalArgumentException("unsupported PDK value kind: " + kind);
            }
            // The PDK flattens array indexes when finding fields. An element must not inherit
            // the container's declared database type when the schema cannot name the element.
            TapField field = positionalElement(schemaPath) ? null
                    : context.fields().get(TapCodecsFilterManager.fieldName(schemaPath));
            boolean matches = field != null && tap.tapTypeClass().isInstance(field.getTapType());
            var type = tap.getTapType();
            if (type == null && matches) {
                // Already-converted values are skipped by the PDK filter and may inherit their
                // type from the table. Read that metadata without modifying a connector-owned value.
                type = field.getTapType();
            }
            String declared = path == null || tap.getOriginType() == null
                    ? matches ? field.getDataType()
                            : original == null ? null : context.origin().apply(original, path)
                    : tap.getOriginType();
            return new ConvertedValue(portable, declared, kind,
                    type == null ? null : parser().toJson(type), temporal);
        }
        if (value instanceof Map<?, ?> map) {
            Map<String, Object> result = new LinkedHashMap<>();
            Map<?, ?> source = original instanceof Map<?, ?> raw ? raw : Map.of();
            map.forEach((key, element) -> result.put((String) key,
                    carry(element, source.get(key), path == null ? null : path + "." + key,
                            schemaPath + "." + key, context)));
            return result;
        }
        if (value instanceof Collection<?> collection) {
            List<?> source = original instanceof List<?> raw ? raw : List.of();
            List<Object> result = new ArrayList<>(collection.size());
            int index = 0;
            for (Object element : collection) {
                result.add(carry(element, index < source.size() ? source.get(index) : null,
                        null, schemaPath + ".#" + index, context));
                index++;
            }
            return result;
        }
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

    static Map<String, Object> write(Map<String, Object> row, TapCodecsRegistry codecs,
            java.util.function.Function<ConvertedValue, Object> legacy) {
        if (row == null) {
            return null;
        }
        Map<String, Object> restored = new LinkedHashMap<>(row.size());
        row.forEach((key, value) -> restored.put(key, restore(value, legacy)));
        new TapCodecsFilterManager(new RetainingTargetCodecs(codecs)).transformFromTapValueMap(restored);
        return restored;
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

    @SuppressWarnings({"rawtypes", "unchecked"})
    private static Object restore(Object value, java.util.function.Function<ConvertedValue, Object> legacy) {
        if (value instanceof ConvertedValue carrier) {
            if (carrier.valueKind() == null) {
                return legacy.apply(carrier);
            }
            Supplier<TapValue<?, ?>> factory = VALUES.get(carrier.valueKind());
            if (factory == null) {
                throw new IllegalArgumentException("unsupported stored PDK value kind: " + carrier.valueKind());
            }
            TapValue tap = factory.get();
            Object payload = carrier.value();
            // A compatibility correction can preserve an exact numeric value wider than the PDK box.
            if (tap instanceof TapNumberValue && !(payload instanceof Double)) {
                return payload;
            }
            if (carrier.temporalDescription() != null) {
                payload = restoreTime(carrier.temporalDescription(), carrier.valueKind());
            } else if (payload instanceof Bytes bytes) {
                payload = new ByteData(bytes.tag(), bytes.value());
            } else if (payload instanceof Map<?, ?> map) {
                payload = restoreMap(map, legacy);
            } else if (payload instanceof Collection<?> collection) {
                payload = restoreList(collection, legacy);
            }
            tap.setValue(payload);
            tap.setOriginType(carrier.originType());
            if (carrier.typeDescription() != null) {
                tap.setTapType((io.tapdata.entity.schema.type.TapType)
                        parser().fromJson(carrier.typeDescription(), tap.tapTypeClass()));
            }
            return tap;
        }
        if (value instanceof Map<?, ?> map) {
            return new TapMapValue(restoreMap(map, legacy));
        }
        if (value instanceof Collection<?> collection) {
            return new TapArrayValue(restoreList(collection, legacy));
        }
        return value;
    }

    private static Map<String, Object> restoreMap(Map<?, ?> map,
            java.util.function.Function<ConvertedValue, Object> legacy) {
        Map<String, Object> result = new LinkedHashMap<>(map.size());
        map.forEach((key, value) -> result.put((String) key, restore(value, legacy)));
        return result;
    }

    private static List<Object> restoreList(Collection<?> collection,
            java.util.function.Function<ConvertedValue, Object> legacy) {
        List<Object> result = new ArrayList<>(collection.size());
        collection.forEach(value -> result.add(restore(value, legacy)));
        return result;
    }

    private static Map<String, Object> copyRow(Map<String, Object> row) {
        Map<String, Object> copy = new LinkedHashMap<>(row.size());
        row.forEach((key, value) -> copy.put(key, copyValue(value)));
        return copy;
    }

    private static Object copyValue(Object value) {
        if (value instanceof Map<?, ?> map) {
            Map<String, Object> copy = new LinkedHashMap<>(map.size());
            map.forEach((key, element) -> copy.put((String) key, copyValue(element)));
            return copy;
        }
        if (value instanceof Collection<?> collection) {
            List<Object> copy = new ArrayList<>(collection.size());
            collection.forEach(element -> copy.add(copyValue(element)));
            return copy;
        }
        return value;
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
            if (constructor == null) {
                throw new IllegalArgumentException("unsupported temporal origin representation");
            }
            result = constructor.apply(((Number) state.get("fraction")).intValue());
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
