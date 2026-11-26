package io.tapstate.runtime.engine;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.common.TapstateType;
import io.tapstate.core.event.ConvertedValue;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.Op;
import io.tapstate.spi.sink.TargetKeyConstraints;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** All values a row releases or acquires in its physical target, under proven simple equality. */
final class SinkLaneClaims implements java.io.Serializable {
    private static final long serialVersionUID = 1L;
    record Key(String table, List<String> columns, List<Object> value) {}
    record Row(Envelope event, Set<Key> keys, List<String> fences, String destination, boolean reserved, boolean tableOrdered) {}

    private final String node;
    private final SinkTarget target;
    private final TargetKeyConstraints constraints;
    private final List<List<String>> secondary;

    SinkLaneClaims(String node, SinkTarget target, TargetKeyConstraints constraints) {
        this.node = node;
        this.target = target;
        this.constraints = constraints;
        var keys = new ArrayList<>(constraints.uniqueKeys());
        keys.removeIf(k -> k.equals(target.keyColumns()));
        secondary = List.copyOf(keys);
    }

    boolean requiresReservation(Envelope e) {
        if (!constraints.exactScalarEquality()) {
            // This table cannot admit a move. Rows that stay on their existing key can still use its native writer.
            if (e.op() == Op.UPDATE) {
                for (var column : target.keyColumns()) {
                    Object oldKey = e.before() == null ? null : ConvertedValue.unwrap(e.before().get(column));
                    Object newKey = e.after() == null ? null : ConvertedValue.unwrap(e.after().get(column));
                    if (oldKey == null || !java.util.Objects.deepEquals(oldKey, newKey)) throw unsupported(e);
                }
            }
            return false;
        }
        for (var column : target.keyColumns()) {
            if (e.op() != Op.INSERT && !known(e.before(), column, false)
                    || e.op() != Op.DELETE && !known(e.after(), column, false)) throw unsupported(e);
        }
        if (e.op() == Op.UPDATE && differs(e, target.keyColumns())) return true;
        for (var columns : secondary) {
            if (generated(columns, e)) continue;
            if (e.op() != Op.UPDATE || differs(e, columns)) return true;
            // A partial or non-scalar key cannot prove a secondary value was preserved.
            for (String column : columns) {
                if (!known(e.before(), column, true) || !known(e.after(), column, true)) return true;
            }
        }
        return false;
    }

    private static boolean differs(Envelope row, List<String> columns) {
        if (row.before() == null || row.after() == null) return false;
        for (var col : columns) {
            Object oldValue = ConvertedValue.unwrap(row.before().get(col));
            Object newValue = ConvertedValue.unwrap(row.after().get(col));
            if (!java.util.Objects.deepEquals(oldValue, newValue)) return true;
        }
        return false;
    }

    /**
     * A stable target primary key still identifies a valid update when a connector emits only its
     * replica identity in the before image. An unknown release cannot be ordered by tuple, so the
     * router must order the row against every reserved write for this table.
     */
    private boolean ordersByTable(Envelope e) {
        if (e.op() != Op.UPDATE || e.before() == null || e.after() == null
                || differs(e, target.keyColumns())) return false;
        for (var column : target.keyColumns()) {
            if (!known(e.before(), column, false) || !known(e.after(), column, false)) return false;
        }
        boolean omitted = false;
        for (var columns : secondary) {
            if (generated(columns, e)) continue;
            for (var column : columns) {
                if (!known(e.after(), column, true)) return false;
                if (!e.before().containsKey(column) && !column.equals("_id")) omitted = true;
                else if (!known(e.before(), column, true)) return false;
            }
        }
        return omitted;
    }

    Row describe(Envelope e, boolean reserved) {
        boolean tableOrdered = reserved && ordersByTable(e);
        var keys = new HashSet<Key>();
        var fences = new LinkedHashSet<String>();
        var last = e.op() == Op.DELETE ? e.before() : e.after();
        for (var image : images(e)) {
            claim(keys, target.keyColumns(), image, e, false);
            fences.add(RoutingKeys.writerLane(node, target, e, image));
            for (var columns : secondary) {
                if (generated(columns, e)) continue;
                if (tableOrdered && image == e.before()
                        && columns.stream().anyMatch(column -> !image.containsKey(column))) continue;
                claim(keys, columns, image, e, true);
            }
        }
        if (e.op() == Op.UPDATE && (hasId(e.before()) || hasId(e.after()))
                && !java.util.Objects.equals(e.before().get("_id"), e.after().get("_id"))) throw unsupported(e);
        return new Row(e, keys, List.copyOf(fences), RoutingKeys.writerLane(node, target, e, last), reserved, tableOrdered);
    }

    private Collection<Map<String, Object>> images(Envelope e) {
        return switch (e.op()) {
            case UPDATE -> {
                if (e.before() == null || e.after() == null) throw unsupported(e);
                yield List.of(e.before(), e.after());
            }
            case INSERT -> List.of(e.after());
            case DELETE -> List.of(e.before());
            default -> throw new IllegalArgumentException("expected a row mutation");
        };
    }

    private void claim(Set<Key> keys, List<String> columns, Map<String, Object> image, Envelope e, boolean allowExplicitNull) {
        var tuple = new ArrayList<>();
        for (var column : columns) {
            if (!known(image, column, allowExplicitNull)) throw unsupported(e);
            Object value = ConvertedValue.unwrap(image.get(column));
            tuple.add(value instanceof Number n ? n.longValue() : value);
        }
        keys.add(new Key(target.table(), columns, Collections.unmodifiableList(tuple)));
    }

    private boolean known(Map<String, Object> image, String column, boolean allowExplicitNull) {
        if (image == null) return false;
        Object v = ConvertedValue.unwrap(image.get(column));
        var type = constraints.fieldTypes().get(column);
        // An explicit null occupies a unique index entry. An omitted column has no known value.
        if (v == null) return allowExplicitNull && !column.equals("_id") && image.containsKey(column)
                && (type == TapstateType.STRING || type == TapstateType.INT64);
        return type == TapstateType.STRING && v instanceof String || type == TapstateType.INT64
                && (v instanceof Long || v instanceof Integer || v instanceof Short || v instanceof Byte);
    }

    private static boolean hasId(Map<String, Object> image) {
        return image != null && image.containsKey("_id");
    }

    private boolean generated(List<String> columns, Envelope e) {
        return constraints.generatedDocumentIdentity() && columns.equals(List.of("_id"))
                && !hasId(e.before()) && !hasId(e.after());
    }

    private TapstateException unsupported(Envelope row) {
        return new TapstateException(EngineError.TARGET_KEY_ORDER_UNKNOWN,
                Map.of("node", node, "stream", row.src()), null);
    }
}
