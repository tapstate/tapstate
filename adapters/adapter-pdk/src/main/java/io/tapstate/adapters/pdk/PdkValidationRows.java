package io.tapstate.adapters.pdk;

import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.type.*;
import io.tapdata.entity.schema.value.TapValue;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.ConvertedValue;
import io.tapstate.core.validation.ValidationError;
import io.tapstate.core.validation.ValidationValues;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Composes independent field codecs into an owned portable row, including nested container values.
 * Rules describe the admitted physical representation, not a desired target type to coerce into.
 * The caller separately admits the driver and codec allocation bounds, model and six row-sized work
 * reservations plus a fixed parser allowance before invoking this decoder. Local charges prevent
 * unbounded traversal and copies; they are not a measurement of heap or a driver admission proof.
 */
final class PdkValidationRows implements PdkValidationCursor.Decoder {
    enum Shape { AUTO, EXACT_NUMBER, FLOAT32, FLOAT64, TEXT, OBJECT_ID, BOOLEAN, BINARY, JSON, OBJECT, ARRAY,
        DATE, TIME, LOCAL_DATE_TIME, INSTANT }
    record Rule(Shape shape, PdkValidationTemporal.LegacyEncoding temporalEncoding) {
        static final Rule AUTO = new Rule(Shape.AUTO, null);
        Rule { Objects.requireNonNull(shape, "shape"); }
        static Rule of(Shape shape) { return new Rule(shape, null); }
    }
    /** String field and integer ordinal segments remain distinct; this is not a dotted-name grammar. */
    @FunctionalInterface interface Rules { Rule at(List<Object> path); }
    private record Model(TapType type, String nativeType) {}
    private record Payload(Object value, Class<? extends TapType> wrapper, String nativeType, boolean rawChildren) {}

    private final PdkValidationCodecs codecs;
    private final Map<String, Model> model;
    private final Rules rules;
    private final ValidationValues.Limits limits;
    private final boolean bsonEpochDates;

    PdkValidationRows(TapTable table, PdkValidationCodecs codecs, Rules rules, ValidationValues.Limits limits) {
        this(table,codecs,rules,limits,false);
    }
    PdkValidationRows(TapTable table, PdkValidationCodecs codecs, Rules rules, ValidationValues.Limits limits,boolean bsonEpochDates) {
        this.codecs = Objects.requireNonNull(codecs, "codecs");
        this.rules = Objects.requireNonNull(rules, "rules");
        this.limits = Objects.requireNonNull(limits, "limits");
        this.bsonEpochDates=bsonEpochDates;
        Objects.requireNonNull(table, "table");
        if (table.getNameFieldMap() == null || table.getNameFieldMap().size() > 4096) throw invalid("model.fields");
        Map<String, Model> fields = new LinkedHashMap<>();
        table.getNameFieldMap().forEach((name, field) -> {
            if (name == null || field == null) throw invalid("model.field");
            // Do not clone PDK types: some standard clones omit declared attributes. This independent
            // reader owns its discovered model; codecs receive that exact read-only model contract.
            fields.put(name, new Model(field.getTapType(), field.getDataType()));
        });
        model = Map.copyOf(fields);
    }

    static long workReservationBytes(ValidationValues.Limits limits) {
        return Math.addExact(64L * 1024, Math.multiplyExact(6, limits.bytes()));
    }

    @Override public ValidationValues.Row decode(Map<String, Object> input) {
        Objects.requireNonNull(input, "input");
        var meter = new Meter(limits);
        var active = new IdentityHashMap<Object, Boolean>();
        var path = new ArrayList<Object>();
        Map<String, Object> fields = object(input, 1, meter, active, path, true, true);
        return ValidationValues.ownRow(fields, limits);
    }

    private Object value(Object raw, Model fieldModel, boolean rawValue, int depth, Meter meter,
            IdentityHashMap<Object, Boolean> active, ArrayList<Object> path) {
        meter.ensureNode(depth);
        String field = diagnostic(path);
        // Date is mutable. A codec may change its input, so its fidelity reference must be fixed
        // before conversion. This reference only verifies the selected output; it never replaces it.
        Instant bsonInstant=bsonEpochDates&&rawValue&&raw!=null&&raw.getClass()==java.util.Date.class
                ?((java.util.Date)raw).toInstant():null;
        var selected = rawValue ? codecs.select(field, raw, fieldModel == null ? null : fieldModel.type())
                : new PdkValidationCodecs.Selected(raw, PdkValidationCodecs.Origin.ALREADY_CONVERTED, codecs.binding());
        Rule rule = Objects.requireNonNull(rules.at(List.copyOf(path)), "admitted field rule");
        boolean bsonDate=bsonInstant!=null;
        if(bsonDate){
            // This document protocol's DateCodec supplies epoch milliseconds. The selected codec
            // must retain that instant; a model's timezone flag cannot establish this encoding.
            if(rule.shape()!=Shape.AUTO&&rule.shape()!=Shape.INSTANT
                    ||rule.temporalEncoding()!=null&&rule.temporalEncoding()!=PdkValidationTemporal.LegacyEncoding.EPOCH_SECONDS)
                throw codecs.unavailable(field);
            rule=new Rule(Shape.INSTANT,PdkValidationTemporal.LegacyEncoding.EPOCH_SECONDS);
        }
        Payload payload = payload(selected, fieldModel == null ? null : fieldModel.nativeType(), field, rule);
        boolean objectId=rawValue&&raw!=null&&raw.getClass().getName().equals("org.bson.types.ObjectId");
        if(objectId){
            // The runtime class establishes the domain. The chosen codec must retain its exact
            // canonical identity; never classify a business string by its printed appearance.
            String canonical=objectId(raw,field);
            Object converted=payload.value() instanceof ValidationValues.Typed typed?typed.value():payload.value();
            if(!(converted instanceof String text)||!canonical.equals(text)
                    ||rule.shape()!=Shape.AUTO&&rule.shape()!=Shape.OBJECT_ID)throw codecs.unavailable(field);
            if(payload.value() instanceof ValidationValues.Typed typed&&!typed.logicalType().equals("object-id"))throw codecs.unavailable(field);
        }
        if (payload.value() instanceof ValidationValues.Typed typed) {
            validateTyped(typed, rule, payload.wrapper(), field);
            if(bsonDate&&!sameInstant(typed,bsonInstant))throw codecs.unavailable(field);
            return portable(typed, depth, meter, active, path);
        }
        if (payload.value() == null && rule.shape() == Shape.AUTO) return portable(null, depth, meter, active, path);
        Shape shape = objectId?Shape.OBJECT_ID:rule.shape() == Shape.AUTO ? infer(payload, field) : rule.shape();
        if (payload.wrapper() != null && !wrapperMatches(shape, payload.wrapper())) throw codecs.unavailable(field);
        if (!rawValue && shape == Shape.JSON && payload.wrapper() == null && payload.value() != null)
            throw codecs.unavailable(field);
        String logical = logical(shape);
        String origin = selected.origin().name().toLowerCase(java.util.Locale.ROOT).replace('_', '-');
        meter.node(depth);
        meter.charge(64L + 2L * logical.length() + 2L * codecs.binding().length() + 2L * origin.length()
                + (payload.nativeType() == null ? 0 : 2L * payload.nativeType().length()));
        meter.ensureNode(depth + 1);
        Object decoded;
        var leaf = new PdkValidationCodecs.Selected(payload.value(), PdkValidationCodecs.Origin.ALREADY_CONVERTED, codecs.binding());
        if (payload.value() == null) {
            decoded = portable(null, depth + 1, meter, active, path);
        } else {
            decoded = switch (shape) {
                case EXACT_NUMBER -> portable(codecs.exactNumber(field, leaf), depth + 1, meter, active, path);
                case FLOAT32, FLOAT64 -> portable(floating(payload.value(), shape, field), depth + 1, meter, active, path);
                case TEXT, OBJECT_ID -> portable(require(payload.value(), String.class, field), depth + 1, meter, active, path);
                case BOOLEAN -> portable(require(payload.value(), Boolean.class, field), depth + 1, meter, active, path);
                case BINARY -> portable(codecs.binary(field, leaf, Math.max(1, meter.remainingBytes())), depth + 1, meter, active, path);
                case JSON -> {
                    var available = new ValidationValues.Limits(Math.max(1, meter.remainingBytes()),
                            Math.max(1, limits.nodes() - meter.nodes), Math.max(1, limits.depth() - depth));
                    yield portable(codecs.jsonText(field, leaf, available), depth + 1, meter, active, path);
                }
                case OBJECT -> {
                    if (!(payload.value() instanceof Map<?, ?> map)) throw codecs.unavailable(field);
                    yield object(map, depth + 1, meter, active, path, payload.rawChildren(), false);
                }
                case ARRAY -> {
                    if (!(payload.value() instanceof List<?> list)) throw codecs.unavailable(field);
                    yield array(list, depth + 1, meter, active, path, payload.rawChildren());
                }
                case DATE, TIME, LOCAL_DATE_TIME, INSTANT -> portable(codecs.temporal(field, leaf,
                        PdkValidationTemporal.Kind.valueOf(shape.name()), rule.temporalEncoding()), depth + 1, meter, active, path);
                case AUTO -> throw new IllegalStateException("automatic value shape was not resolved");
            };
        }
        if(bsonDate&&!sameInstant(decoded,bsonInstant))throw codecs.unavailable(field);
        return new ValidationValues.Typed(logical, payload.nativeType(), codecs.binding(), origin, decoded);
    }
    private static boolean sameInstant(Object decoded,Instant expected){
        int wrappers=0;
        while(decoded instanceof ValidationValues.Typed typed){if(++wrappers>64)return false;decoded=typed.value();}
        return decoded instanceof Instant instant&&instant.equals(expected);
    }

    private Payload payload(PdkValidationCodecs.Selected selected, String nativeType, String field, Rule rule) {
        Object value = selected.value();
        Class<? extends TapType> wrapper = null;
        boolean rawChildren = selected.origin() == PdkValidationCodecs.Origin.BUILTIN;
        for (int count = 0; count < 64; count++) {
            if (value instanceof ConvertedValue converted) {
                if (nativeType == null) nativeType = converted.originType();
                value = converted.value();
                rawChildren = false;
            } else if (value instanceof TapValue<?, ?> converted) {
                Class<? extends TapType> next;
                try { next = converted.tapTypeClass(); }
                catch (RuntimeException failure) { throw codecs.failed(field, failure); }
                if (!supportedWrapper(next) || wrapper != null && wrapper != next
                        || rule.shape() != Shape.AUTO && !wrapperMatches(rule.shape(), next)) throw codecs.unavailable(field);
                try { value = converted.getValue(); }
                catch (RuntimeException failure) { throw codecs.failed(field, failure); }
                wrapper = next;
                rawChildren = false;
            } else return new Payload(value, wrapper, nativeType, rawChildren);
        }
        throw limit("codecWrappers");
    }

    private Shape infer(Payload payload, String field) {
        Object value = payload.value();
        Class<? extends TapType> wrapper = payload.wrapper();
        if (wrapper == TapString.class) return Shape.TEXT;
        if (wrapper == TapBoolean.class) return Shape.BOOLEAN;
        if (wrapper == TapBinary.class) return Shape.BINARY;
        if (wrapper == TapJson.class) return Shape.JSON;
        if (wrapper == TapMap.class) return Shape.OBJECT;
        if (wrapper == TapArray.class) return Shape.ARRAY;
        if (wrapper == TapNumber.class) {
            // A framework Double may have come from an exact native number. Only an explicit physical
            // floating rule can establish that the conversion retains this field's necessary facts.
            if (value == null || exact(value)) return Shape.EXACT_NUMBER;
            throw codecs.unavailable(field);
        }
        if (wrapper != null) throw codecs.unavailable(field);
        if (value == null) throw new IllegalStateException("untyped null must remain a presence fact");
        if (exact(value)) return Shape.EXACT_NUMBER;
        if (value instanceof Float) return Shape.FLOAT32;
        if (value instanceof Double) return Shape.FLOAT64;
        if (value instanceof String) return Shape.TEXT;
        if (value instanceof Boolean) return Shape.BOOLEAN;
        if (value instanceof byte[] || value instanceof ByteBuffer || value instanceof ValidationValues.Binary
                || value instanceof io.tapdata.entity.schema.value.ByteData) return Shape.BINARY;
        if (value instanceof Map<?, ?>) return Shape.OBJECT;
        if (value instanceof List<?>) return Shape.ARRAY;
        if (value instanceof LocalDate) return Shape.DATE;
        if (value instanceof LocalTime) return Shape.TIME;
        if (value instanceof LocalDateTime) return Shape.LOCAL_DATE_TIME;
        if (value instanceof Instant || value instanceof OffsetDateTime || value instanceof ZonedDateTime) return Shape.INSTANT;
        throw codecs.unavailable(field);
    }

    private String objectId(Object raw,String field){
        try{
            Object value=raw.getClass().getMethod("toHexString").invoke(raw);
            if(value instanceof String text&&text.matches("[0-9a-f]{24}"))return text;
            throw codecs.unavailable(field);
        }catch(java.lang.reflect.InvocationTargetException failed){
            if(failed.getCause() instanceof Error fatal)throw fatal;
            throw codecs.failed(field,new IllegalStateException("native identity accessor failed",failed.getCause()));
        }catch(ReflectiveOperationException missing){throw codecs.unavailable(field);}
    }

    private Map<String, Object> object(Map<?, ?> raw, int depth, Meter meter, IdentityHashMap<Object, Boolean> active,
            ArrayList<Object> path, boolean rawChildren, boolean root) {
        meter.node(depth); meter.charge(64); meter.children(raw.size());
        enter(raw, active);
        try {
            Map<String, Object> output = new LinkedHashMap<>();
            for (var entry : raw.entrySet()) {
                if (!(entry.getKey() instanceof String name) || output.containsKey(name)) throw invalid("value.fieldName");
                meter.charge(72L + 2L * name.length());
                path.add(name);
                try { output.put(name, value(entry.getValue(), root ? model.get(name) : null, rawChildren,
                        depth + 1, meter, active, path)); }
                finally { path.removeLast(); }
            }
            return output;
        } finally { active.remove(raw); }
    }

    private List<Object> array(List<?> raw, int depth, Meter meter, IdentityHashMap<Object, Boolean> active,
            ArrayList<Object> path, boolean rawChildren) {
        meter.node(depth); meter.charge(32); meter.children(raw.size());
        enter(raw, active);
        try {
            List<Object> output = new ArrayList<>();
            int ordinal = 0;
            for (Object item : raw) {
                meter.charge(8);
                path.add(ordinal++);
                try { output.add(value(item, null, rawChildren, depth + 1, meter, active, path)); }
                finally { path.removeLast(); }
            }
            return output;
        } finally { active.remove(raw); }
    }

    /** Own only already-portable shapes here: no codec, JSON reparse or temporal reinterpretation. */
    private Object portable(Object value, int depth, Meter meter, IdentityHashMap<Object, Boolean> active,
            ArrayList<Object> path) {
        return portable(value, depth, meter, active, path, false);
    }

    private Object portable(Object value, int depth, Meter meter, IdentityHashMap<Object, Boolean> active,
            ArrayList<Object> path, boolean json) {
        meter.ensureNode(depth);
        String field = diagnostic(path);
        Rule rule = Objects.requireNonNull(rules.at(List.copyOf(path)), "admitted field rule");
        if (value instanceof ValidationValues.Typed typed) {
            Shape shape = validateTyped(typed, rule, null, field);
            if (json && !jsonShape(shape)) throw codecs.unavailable(field);
            json |= shape == Shape.JSON;
        } else {
            if (rule.shape() != Shape.AUTO && !portableShape(rule.shape(), value)) throw codecs.unavailable(field);
            json |= rule.shape() == Shape.JSON;
            if (json && !jsonValue(value)) throw codecs.unavailable(field);
        }
        meter.node(depth);
        if (value == null) return null;
        if (value instanceof String text) { meter.charge(24L + 2L * text.length()); return text; }
        if (value.getClass() == BigInteger.class) {
            meter.charge(32L + ((long)((BigInteger)value).bitLength() + 7) / 8); return value;
        }
        if (value.getClass() == BigDecimal.class) {
            meter.charge(48L + ((long)((BigDecimal)value).unscaledValue().bitLength() + 7) / 8); return value;
        }
        if (value instanceof Byte || value instanceof Short || value instanceof Integer || value instanceof Long
                || value instanceof Float || value instanceof Double || value instanceof Boolean || value instanceof LocalDate
                || value instanceof LocalTime || value instanceof LocalDateTime || value instanceof Instant) {
            meter.charge(48); return value;
        }
        if (value instanceof ValidationValues.Binary binary) { meter.charge(48L + binary.length()); return binary; }
        enter(value, active);
        try {
            if (value instanceof ValidationValues.Typed typed) {
                if (!codecs.binding().equals(typed.codecBinding())) throw codecs.unavailable(diagnostic(path));
                meter.charge(64L + 2L * typed.logicalType().length() + 2L * typed.codecBinding().length()
                        + 2L * typed.conversionOrigin().length() + (typed.nativeType() == null ? 0 : 2L * typed.nativeType().length()));
                return new ValidationValues.Typed(typed.logicalType(), typed.nativeType(), typed.codecBinding(), typed.conversionOrigin(),
                        portable(typed.value(), depth + 1, meter, active, path, json));
            }
            if (value instanceof Map<?, ?> map) {
                meter.charge(64); meter.children(map.size());
                Map<String, Object> copy = new LinkedHashMap<>();
                for (var entry : map.entrySet()) {
                    if (!(entry.getKey() instanceof String name) || copy.containsKey(name)) throw invalid("value.fieldName");
                    meter.charge(72L + 2L * name.length());
                    path.add(name);
                    try { copy.put(name, portable(entry.getValue(), depth + 1, meter, active, path, json)); }
                    finally { path.removeLast(); }
                }
                return copy;
            }
            if (value instanceof List<?> list) {
                meter.charge(32); meter.children(list.size());
                List<Object> copy = new ArrayList<>();
                int ordinal = 0;
                for (Object child : list) {
                    meter.charge(8);
                    path.add(ordinal++);
                    try { copy.add(portable(child, depth + 1, meter, active, path, json)); }
                    finally { path.removeLast(); }
                }
                return copy;
            }
            throw codecs.unavailable(diagnostic(path));
        } finally { active.remove(value); }
    }

    private Shape validateTyped(ValidationValues.Typed typed, Rule rule,
            Class<? extends TapType> wrapper, String field) {
        if (!codecs.binding().equals(typed.codecBinding())) throw codecs.unavailable(field);
        Shape shape = shape(typed.logicalType());
        if (shape == Shape.AUTO || rule.shape() != Shape.AUTO && shape != rule.shape()
                || wrapper != null && !wrapperMatches(shape, wrapper)
                || shape != Shape.AUTO && !portableShape(shape, typed.value())) throw codecs.unavailable(field);
        return shape;
    }

    private static Shape shape(String logicalType) {
        for (Shape shape : Shape.values()) {
            if (shape != Shape.AUTO && logical(shape).equals(logicalType)) return shape;
        }
        return Shape.AUTO;
    }

    private static boolean portableShape(Shape shape, Object value) {
        int wrappers = 0;
        while (value instanceof ValidationValues.Typed typed) {
            if (++wrappers > 64) throw limit("codecWrappers");
            Shape nested = shape(typed.logicalType());
            if (shape == Shape.JSON ? !jsonShape(nested) : nested != shape) return false;
            value = typed.value();
        }
        if (value == null) return true;
        return switch (shape) {
            case EXACT_NUMBER -> exact(value);
            case FLOAT32 -> value instanceof Float;
            case FLOAT64 -> value instanceof Double;
            case TEXT -> value instanceof String;
            case OBJECT_ID -> value instanceof String text&&text.matches("[0-9a-f]{24}");
            case BOOLEAN -> value instanceof Boolean;
            case BINARY -> value instanceof ValidationValues.Binary;
            case DATE -> value instanceof LocalDate;
            case TIME -> value instanceof LocalTime;
            case LOCAL_DATE_TIME -> value instanceof LocalDateTime;
            case INSTANT -> value instanceof Instant;
            case OBJECT -> value instanceof Map<?, ?>;
            case ARRAY -> value instanceof List<?>;
            case JSON -> jsonValue(value);
            case AUTO -> true;
        };
    }

    private static boolean jsonShape(Shape shape) {
        return shape == Shape.JSON || shape == Shape.EXACT_NUMBER || shape == Shape.TEXT
                || shape == Shape.BOOLEAN || shape == Shape.OBJECT || shape == Shape.ARRAY;
    }
    private static boolean jsonValue(Object value) {
        return value == null || exact(value) || value instanceof String || value instanceof Boolean
                || value instanceof Map<?, ?> || value instanceof List<?>;
    }

    private Object floating(Object value, Shape shape, String field) {
        if (!(value instanceof Float || value instanceof Double)) throw codecs.unavailable(field);
        double number = ((Number)value).doubleValue();
        if (shape == Shape.FLOAT64) return number;
        float narrowed = (float)number;
        if (!Double.isNaN(number) && (double)narrowed != number) throw codecs.unavailable(field);
        return narrowed;
    }
    private Object require(Object value, Class<?> type, String field) {
        if (!type.isInstance(value)) throw codecs.unavailable(field);
        return value;
    }
    private static boolean exact(Object value) {
        return value instanceof Byte || value instanceof Short || value instanceof Integer || value instanceof Long
                || value != null && (value.getClass() == BigInteger.class || value.getClass() == BigDecimal.class);
    }
    private static boolean supportedWrapper(Class<? extends TapType> wrapper) {
        return wrapper == TapNumber.class || wrapper == TapString.class || wrapper == TapBoolean.class
                || wrapper == TapBinary.class || wrapper == TapJson.class || wrapper == TapMap.class
                || wrapper == TapArray.class || wrapper == TapDate.class || wrapper == TapTime.class
                || wrapper == TapDateTime.class;
    }
    private static boolean wrapperMatches(Shape shape, Class<? extends TapType> wrapper) {
        return switch (shape) {
            case EXACT_NUMBER, FLOAT32, FLOAT64 -> wrapper == TapNumber.class;
            case TEXT, OBJECT_ID -> wrapper == TapString.class;
            case BOOLEAN -> wrapper == TapBoolean.class;
            case BINARY -> wrapper == TapBinary.class;
            case JSON -> wrapper == TapJson.class;
            case OBJECT -> wrapper == TapMap.class;
            case ARRAY -> wrapper == TapArray.class;
            case DATE -> wrapper == TapDate.class;
            case TIME -> wrapper == TapTime.class;
            case LOCAL_DATE_TIME, INSTANT -> wrapper == TapDateTime.class;
            case AUTO -> false;
        };
    }
    private static String logical(Shape shape) { return shape.name().toLowerCase(java.util.Locale.ROOT).replace('_', '-'); }
    private static void enter(Object value, IdentityHashMap<Object, Boolean> active) {
        if (active.put(value, Boolean.TRUE) != null) throw invalid("value.cycle");
    }
    private static String diagnostic(List<Object> path) {
        StringBuilder result = new StringBuilder();
        for (Object segment : path) {
            if (!result.isEmpty()) result.append('/');
            String text = segment.toString();
            int remaining = 512 - result.length();
            if (remaining <= 0) break;
            result.append(text, 0, Math.min(text.length(), remaining));
        }
        return result.toString();
    }
    private static TapstateException invalid(String field) {
        return new TapstateException(ValidationError.INVALID_CONTRACT, Map.of("field", field), null);
    }
    private static TapstateException limit(String dimension) {
        return new TapstateException(ValidationError.VALUE_LIMIT_EXCEEDED, Map.of("limit", dimension), null);
    }
    private static final class Meter {
        private final ValidationValues.Limits limits;
        private long bytes;
        private int nodes;
        private Meter(ValidationValues.Limits limits) { this.limits = limits; }
        private void ensureNode(int depth) {
            if (depth > limits.depth()) throw limit("depth");
            if (nodes >= limits.nodes()) throw limit("nodes");
        }
        private void node(int depth) { ensureNode(depth); nodes++; charge(16); }
        private void children(int count) { if (count > limits.nodes() - nodes) throw limit("nodes"); }
        private void charge(long amount) { if (amount > remainingBytes()) throw limit("bytes"); bytes += amount; }
        private long remainingBytes() { return limits.bytes() - bytes; }
    }
}
