package io.tapstate.e2e;

import com.sun.jdi.ArrayReference;
import com.sun.jdi.BooleanValue;
import com.sun.jdi.Field;
import com.sun.jdi.IntegerValue;
import com.sun.jdi.LongValue;
import com.sun.jdi.ObjectReference;
import com.sun.jdi.PrimitiveValue;
import com.sun.jdi.ReferenceType;
import com.sun.jdi.StringReference;
import com.sun.jdi.Value;

import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Bounded mirror reads; no target method is invoked and no partial container is accepted. */
final class NativeTelemetryMirror {
    interface Validator { void validate(ReferenceType type) throws Exception; }
    static final class Unavailable extends Exception {
        Unavailable(String reason) { super(reason); }
    }
    private static final int MAX_ITEMS = 2048;
    private static final int MAX_ARRAY = 16384;
    private static final int MAX_TEXT = 4096;
    private static final int MAX_READS = 8192;
    private static final int MAP_READ_ATTEMPTS = 3;
    private static final int MAX_TEXT_TOTAL = 65536;
    private final Validator validator;
    private final Set<String> layouts;
    private int reads;
    private int textTotal;

    NativeTelemetryMirror(Validator validator, Set<String> layouts) {
        this.validator = validator;
        this.layouts = layouts;
    }

    Value field(ObjectReference object, String name, String signature) throws Exception {
        if (++reads > MAX_READS) { throw unavailable("MIRROR_READ_BUDGET"); }
        if (object == null) { throw unavailable("NULL_OBJECT"); }
        ReferenceType type = object.referenceType();
        if (!type.name().startsWith("java.")) { validator.validate(type); }
        Field field = type.fieldByName(name);
        if (field == null || field.isStatic() || !field.signature().equals(signature)) {
            throw unavailable("FIELD_LAYOUT:" + type.name() + "." + name);
        }
        layouts.add(type.name() + "." + name + ":" + signature);
        if (layouts.size() > 256) { throw unavailable("LAYOUT_BUDGET"); }
        return object.getValue(field);
    }

    ObjectReference object(Value value) throws Unavailable {
        if (!(value instanceof ObjectReference result)) {
            throw unavailable("OBJECT_REQUIRED:" + (value == null ? "NULL" : value.type().name()));
        }
        return result;
    }

    String text(Value value) throws Unavailable {
        if (!(value instanceof StringReference string)) { throw unavailable("STRING_REQUIRED"); }
        String result = string.value();
        textTotal += result.length();
        if (result.length() > MAX_TEXT || textTotal > MAX_TEXT_TOTAL) { throw unavailable("TEXT_BUDGET"); }
        return result;
    }

    long integral(Value value) throws Exception {
        if (value instanceof LongValue number) { return number.value(); }
        if (value instanceof IntegerValue number) { return number.value(); }
        ObjectReference boxed = object(value);
        return switch (boxed.referenceType().name()) {
            case "java.lang.Long" -> integral(field(boxed, "value", "J"));
            case "java.lang.Integer" -> integral(field(boxed, "value", "I"));
            default -> throw unavailable("INTEGRAL_LAYOUT");
        };
    }

    Object scalar(Value value) throws Exception {
        if (value == null) { return null; }
        if (value instanceof StringReference) { return text(value); }
        if (value instanceof BooleanValue number) { return number.value(); }
        if (value instanceof LongValue || value instanceof IntegerValue) { return integral(value); }
        if (value instanceof PrimitiveValue primitive) {
            if (value.type().name().equals("double")) { return primitive.doubleValue(); }
            if (value.type().name().equals("float")) { return primitive.floatValue(); }
            throw unavailable("PRIMITIVE_LAYOUT");
        }
        ObjectReference reference = object(value);
        return switch (reference.referenceType().name()) {
            case "java.lang.Long", "java.lang.Integer" -> integral(reference);
            case "java.lang.Double" -> scalar(field(reference, "value", "D"));
            case "java.lang.Boolean" -> scalar(field(reference, "value", "Z"));
            case "java.time.Instant" -> Instant.ofEpochSecond(
                    integral(field(reference, "seconds", "J")),
                    integral(field(reference, "nanos", "I"))).toString();
            default -> throw unavailable("SCALAR_LAYOUT:" + reference.referenceType().name());
        };
    }

    String enumName(Value value) throws Exception {
        ObjectReference object = object(value);
        return text(field(object, "name", "Ljava/lang/String;"));
    }

    Map<String, Object> scope(Value value) throws Exception {
        ObjectReference object = object(value);
        String type = object.referenceType().name();
        String key = switch (type) {
            case "io.tapstate.spi.metrics.MetricsExport$ScopeToken" -> "incarnationId";
            case "io.tapstate.spi.store.ObservationStore$Scope",
                    "io.tapstate.core.logging.LogSink$Scope" -> "pipelineIncarnationId";
            default -> throw unavailable("SCOPE_LAYOUT");
        };
        String incarnation = text(field(object, key, "Ljava/lang/String;"));
        long generation = integral(field(object, "executionGeneration", "J"));
        if (incarnation.isBlank() || generation < 1) { throw unavailable("UNFENCED_SCOPE"); }
        return Map.of("incarnation", incarnation, "generation", generation);
    }

    Map<String, Object> job(Value value) throws Exception {
        ObjectReference identity = object(value);
        requireType(identity, "io.tapstate.spi.store.StopReservation$JobIdentity");
        return Map.of("clusterId", text(field(identity, "clusterId", "Ljava/lang/String;")),
                "bootId", text(field(identity, "bootId", "Ljava/lang/String;")),
                "jobId", integral(field(identity, "jobId", "J")));
    }

    private void requireType(ObjectReference object, String expected) throws Exception {
        if (!object.referenceType().name().equals(expected)) { throw unavailable("OBJECT_TYPE:" + expected); }
        validator.validate(object.referenceType());
    }

    List<Value> sequence(Value value) throws Exception { return sequence(value, 0); }
    private List<Value> sequence(Value value, int depth) throws Exception {
        if (depth > 8) { throw unavailable("CONTAINER_DEPTH"); }
        ObjectReference object = object(value);
        String type = object.referenceType().name();
        layouts.add(type);
        return switch (type) {
            case "java.util.ArrayList" -> {
                long size = integral(field(object, "size", "I"));
                long version = integral(field(object, "modCount", "I"));
                ArrayReference array = array(field(object, "elementData", "[Ljava/lang/Object;"));
                if (size < 0 || size > MAX_ITEMS || size > array.length()) { throw unavailable("LIST_SIZE"); }
                List<Value> elements = new ArrayList<>(array.getValues(0, (int) size));
                if (size != integral(field(object, "size", "I"))
                        || version != integral(field(object, "modCount", "I"))) {
                    throw unavailable("INCOHERENT_LIST");
                }
                yield elements;
            }
            case "java.util.ImmutableCollections$ListN" -> values(
                    array(field(object, "elements", "[Ljava/lang/Object;")), false);
            case "java.util.ImmutableCollections$List12" -> {
                Value first = field(object, "e0", "Ljava/lang/Object;");
                Value second = field(object, "e1", "Ljava/lang/Object;");
                List<Value> elements = new ArrayList<>();
                elements.add(first);
                if (!jdkEmptySentinel(second, object)) { elements.add(second); }
                yield elements;
            }
            case "java.util.Arrays$ArrayList" -> values(array(field(object, "a", "[Ljava/lang/Object;")), false);
            case "java.util.Collections$EmptyList", "java.util.Collections$EmptySet" -> List.of();
            case "java.util.Collections$SingletonList", "java.util.Collections$SingletonSet" ->
                    List.of(field(object, "element", "Ljava/lang/Object;"));
            case "java.util.Collections$UnmodifiableRandomAccessList", "java.util.Collections$UnmodifiableList" ->
                    sequence(field(object, "list", "Ljava/util/List;"), depth + 1);
            case "java.util.Collections$UnmodifiableCollection", "java.util.Collections$UnmodifiableSet" ->
                    sequence(field(object, "c", "Ljava/util/Collection;"), depth + 1);
            case "java.util.HashSet", "java.util.LinkedHashSet" ->
                    map(field(object, "map", "Ljava/util/HashMap;")).stream().map(Pair::key).toList();
            case "java.util.ImmutableCollections$SetN" ->
                    values(array(field(object, "elements", "[Ljava/lang/Object;")), true);
            case "java.util.ImmutableCollections$Set12" -> {
                List<Value> elements = new ArrayList<>();
                elements.add(field(object, "e0", "Ljava/lang/Object;"));
                Value second = field(object, "e1", "Ljava/lang/Object;");
                if (!jdkEmptySentinel(second, object)) { elements.add(second); }
                yield elements;
            }
            default -> throw unavailable("SEQUENCE_LAYOUT:" + type);
        };
    }

    record Pair(Value key, Value value) { }
    List<Pair> map(Value value) throws Exception { return map(value, 0); }
    private List<Pair> map(Value value, int depth) throws Exception {
        if (depth > 8) { throw unavailable("CONTAINER_DEPTH"); }
        ObjectReference object = object(value);
        String type = object.referenceType().name();
        layouts.add(type);
        List<Pair> result = new ArrayList<>();
        switch (type) {
            case "java.util.Collections$EmptyMap" -> { return List.of(); }
            case "java.util.Collections$SingletonMap" -> {
                return List.of(new Pair(field(object, "k", "Ljava/lang/Object;"),
                        field(object, "v", "Ljava/lang/Object;")));
            }
            case "java.util.ImmutableCollections$Map1" -> {
                return List.of(new Pair(field(object, "k0", "Ljava/lang/Object;"),
                        field(object, "v0", "Ljava/lang/Object;")));
            }
            case "java.util.ImmutableCollections$MapN" -> {
                ArrayReference array = array(field(object, "table", "[Ljava/lang/Object;"));
                List<Value> slots = array.getValues();
                if ((slots.size() & 1) != 0) { throw unavailable("MAP_ARRAY_LAYOUT"); }
                for (int index = 0; index < slots.size(); index += 2) {
                    if (slots.get(index) != null) { result.add(new Pair(slots.get(index), slots.get(index + 1))); }
                }
                if (result.size() != integral(field(object, "size", "I"))) { throw unavailable("MAP_SIZE"); }
            }
            case "java.util.Collections$UnmodifiableMap" -> {
                return map(field(object, "m", "Ljava/util/Map;"), depth + 1);
            }
            case "java.util.Collections$UnmodifiableSortedMap" -> {
                Value underlying = field(object, "m", "Ljava/util/Map;");
                Value sorted = field(object, "sm", "Ljava/util/SortedMap;");
                if (!Objects.equals(underlying, sorted)) { throw unavailable("INCOHERENT_SORTED_WRAPPER"); }
                return map(underlying, depth + 1);
            }
            case "java.util.TreeMap" -> { return treeMap(object); }
            case "java.util.HashMap", "java.util.LinkedHashMap", "java.util.concurrent.ConcurrentHashMap" -> {
                return coherentHashMap(object, type.endsWith("ConcurrentHashMap"));
            }
            default -> throw unavailable("MAP_LAYOUT:" + type);
        }
        if (result.size() > MAX_ITEMS) { throw unavailable("MAP_ITEM_BUDGET"); }
        return List.copyOf(result);
    }

    private List<Pair> coherentHashMap(ObjectReference object, boolean concurrent) throws Exception {
        for (int attempt = 1; attempt <= MAP_READ_ATTEMPTS; attempt++) {
            try { return hashMapOnce(object, concurrent); }
            catch (Unavailable changed) {
                // Other target threads can change a map during this event-thread suspension. Retry
                // only a complete incoherent read; this mirror's read/text/layout budgets remain spent.
                if (!"INCOHERENT_MAP".equals(changed.getMessage()) || attempt == MAP_READ_ATTEMPTS) { throw changed; }
            }
        }
        throw new AssertionError("a bounded map read ended without a result or failure");
    }

    private List<Pair> hashMapOnce(ObjectReference object, boolean concurrent) throws Exception {
        List<Pair> result = new ArrayList<>();
        long size = concurrent ? concurrentSize(object) : integral(field(object, "size", "I"));
        if (size < 0 || size > MAX_ITEMS) { throw unavailable("MAP_SIZE_BUDGET"); }
        long version = concurrent ? size : integral(field(object, "modCount", "I"));
        String descriptor = concurrent ? "[Ljava/util/concurrent/ConcurrentHashMap$Node;"
                : "[Ljava/util/HashMap$Node;";
        Value tableValue = field(object, "table", descriptor);
        long tableId = tableValue == null ? 0 : object(tableValue).uniqueID();
        Set<Long> visited = new HashSet<>();
        if (tableValue != null) {
            for (Value bucket : array(tableValue).getValues()) {
                if (bucket == null) { continue; }
                ObjectReference node = object(bucket);
                if (concurrent && node.referenceType().name().equals("java.util.concurrent.ConcurrentHashMap$TreeBin")) {
                    node = nullableObject(field(node, "first", "Ljava/util/concurrent/ConcurrentHashMap$TreeNode;"));
                }
                while (node != null) {
                    if (!visited.add(node.uniqueID()) || result.size() >= MAX_ITEMS) {
                        throw unavailable("MAP_NODE_BUDGET_OR_CYCLE");
                    }
                    String nodeType = node.referenceType().name();
                    if (!Set.of("java.util.HashMap$Node", "java.util.HashMap$TreeNode",
                            "java.util.LinkedHashMap$Entry", "java.util.concurrent.ConcurrentHashMap$Node",
                            "java.util.concurrent.ConcurrentHashMap$TreeNode").contains(nodeType)) {
                        throw unavailable("MAP_NODE_LAYOUT:" + nodeType);
                    }
                    result.add(new Pair(field(node, "key", "Ljava/lang/Object;"),
                            field(node, concurrent ? "val" : "value", "Ljava/lang/Object;")));
                    node = nullableObject(field(node, "next", concurrent
                            ? "Ljava/util/concurrent/ConcurrentHashMap$Node;" : "Ljava/util/HashMap$Node;"));
                }
            }
        }
        Value afterTable = field(object, "table", descriptor);
        long afterId = afterTable == null ? 0 : object(afterTable).uniqueID();
        long afterSize = concurrent ? concurrentSize(object) : integral(field(object, "size", "I"));
        if (afterSize < 0 || afterSize > MAX_ITEMS) { throw unavailable("MAP_SIZE_BUDGET"); }
        long afterVersion = concurrent ? afterSize : integral(field(object, "modCount", "I"));
        if (size != result.size() || afterSize != size || version != afterVersion || tableId != afterId) {
            throw unavailable("INCOHERENT_MAP");
        }
        return List.copyOf(result);
    }

    private record TreeEntry(ObjectReference node, Value key, Value value, Value left, Value right) { }

    private List<Pair> treeMap(ObjectReference map) throws Exception {
        long size = integral(field(map, "size", "I"));
        long version = integral(field(map, "modCount", "I"));
        if (size < 0 || size > MAX_ITEMS) { throw unavailable("TREE_MAP_SIZE_BUDGET"); }
        Value root = field(map, "root", "Ljava/util/TreeMap$Entry;");
        Deque<ObjectReference> pending = new ArrayDeque<>();
        if (root != null) { pending.push(object(root)); }
        Set<Long> visited = new HashSet<>();
        List<TreeEntry> entries = new ArrayList<>();
        List<Pair> result = new ArrayList<>();
        while (!pending.isEmpty()) {
            ObjectReference node = pending.pop();
            if (!node.referenceType().name().equals("java.util.TreeMap$Entry")) {
                throw unavailable("TREE_MAP_NODE_LAYOUT:" + node.referenceType().name());
            }
            if (!visited.add(node.uniqueID()) || entries.size() >= MAX_ITEMS) {
                throw unavailable("TREE_MAP_NODE_BUDGET_OR_CYCLE");
            }
            Value key = field(node, "key", "Ljava/lang/Object;");
            Value value = field(node, "value", "Ljava/lang/Object;");
            Value left = field(node, "left", "Ljava/util/TreeMap$Entry;");
            Value right = field(node, "right", "Ljava/util/TreeMap$Entry;");
            entries.add(new TreeEntry(node, key, value, left, right));
            result.add(new Pair(key, value));
            if (entries.size() > size) { throw unavailable("INCOHERENT_TREE_MAP"); }
            if (right != null) { pending.push(object(right)); }
            if (left != null) { pending.push(object(left)); }
        }
        if (result.size() != size) { throw unavailable("INCOHERENT_TREE_MAP"); }
        // Value replacement need not change modCount, so reread every observed entry as well.
        for (TreeEntry entry : entries) {
            if (!Objects.equals(entry.key(), field(entry.node(), "key", "Ljava/lang/Object;"))
                    || !Objects.equals(entry.value(), field(entry.node(), "value", "Ljava/lang/Object;"))
                    || !Objects.equals(entry.left(), field(entry.node(), "left", "Ljava/util/TreeMap$Entry;"))
                    || !Objects.equals(entry.right(), field(entry.node(), "right", "Ljava/util/TreeMap$Entry;"))) {
                throw unavailable("INCOHERENT_TREE_MAP_ENTRY");
            }
        }
        if (size != integral(field(map, "size", "I")) || version != integral(field(map, "modCount", "I"))
                || !Objects.equals(root, field(map, "root", "Ljava/util/TreeMap$Entry;"))) {
            throw unavailable("INCOHERENT_TREE_MAP");
        }
        return List.copyOf(result);
    }

    private long concurrentSize(ObjectReference map) throws Exception {
        long count = integral(field(map, "baseCount", "J"));
        Value cells = field(map, "counterCells", "[Ljava/util/concurrent/ConcurrentHashMap$CounterCell;");
        if (cells != null) {
            ArrayReference array = array(cells);
            if (array.length() > 128) { throw unavailable("MAP_COUNTER_BUDGET"); }
            for (Value cell : array.getValues()) {
                if (cell != null) { count += integral(field(object(cell), "value", "J")); }
            }
        }
        if (count < 0 || count > MAX_ITEMS) { throw unavailable("MAP_SIZE_BUDGET"); }
        return count;
    }

    Value lookup(Value map, String key) throws Exception {
        Value found = null;
        boolean matched = false;
        for (Pair entry : map(map)) {
            if (text(entry.key()).equals(key)) {
                if (matched) { throw unavailable("DUPLICATE_MAP_KEY"); }
                found = entry.value(); matched = true;
            }
        }
        return found;
    }

    Map<String, Object> strings(Value map) throws Exception {
        Map<String, Object> result = new LinkedHashMap<>();
        for (Pair entry : map(map)) {
            String key = text(entry.key());
            if (result.putIfAbsent(key, scalar(entry.value())) != null) { throw unavailable("DUPLICATE_MAP_KEY"); }
        }
        return java.util.Collections.unmodifiableMap(result);
    }

    List<Object> scalars(Value list) throws Exception {
        List<Object> result = new ArrayList<>();
        for (Value value : sequence(list)) { result.add(scalar(value)); }
        return java.util.Collections.unmodifiableList(result);
    }

    private ArrayReference array(Value value) throws Unavailable {
        if (!(value instanceof ArrayReference array) || array.length() > MAX_ARRAY) {
            throw unavailable("ARRAY_LAYOUT_OR_BUDGET");
        }
        return array;
    }
    private boolean jdkEmptySentinel(Value candidate, ObjectReference container) throws Exception {
        List<ReferenceType> matches = container.virtualMachine().classesByName("java.util.ImmutableCollections")
                .stream().filter(type -> type.classLoader() == null).toList();
        if (matches.size() != 1) { throw unavailable("JDK_IMMUTABLE_SENTINEL_CLASS"); }
        ReferenceType type = matches.getFirst();
        Field empty = type.fieldByName("EMPTY");
        if (empty == null || !empty.isStatic() || !empty.signature().equals("Ljava/lang/Object;")) {
            throw unavailable("JDK_IMMUTABLE_SENTINEL_LAYOUT");
        }
        Value actual = type.getValue(empty);
        if (!(actual instanceof ObjectReference sentinel)) { throw unavailable("JDK_IMMUTABLE_SENTINEL_VALUE"); }
        if (!(candidate instanceof ObjectReference reference)) { throw unavailable("JDK_IMMUTABLE_SECOND_ELEMENT"); }
        layouts.add("java.util.ImmutableCollections.EMPTY:Ljava/lang/Object;@bootstrap");
        return reference.uniqueID() == sentinel.uniqueID();
    }
    private List<Value> values(ArrayReference array, boolean omitNulls) throws Unavailable {
        List<Value> values = new ArrayList<>();
        for (Value value : array.getValues()) { if (!omitNulls || value != null) { values.add(value); } }
        if (values.size() > MAX_ITEMS) { throw unavailable("SEQUENCE_ITEM_BUDGET"); }
        return java.util.Collections.unmodifiableList(values);
    }
    private ObjectReference nullableObject(Value value) throws Unavailable { return value == null ? null : object(value); }
    private static Unavailable unavailable(String reason) { return new Unavailable(reason); }
}
