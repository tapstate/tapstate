package io.tapstate.adapters.transform;

import io.tapstate.core.dsl.UnwindRules;

import io.tapstate.core.event.ConvertedValue;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.Op;
import io.tapstate.spi.transform.TransformPort;
import java.util.ArrayList;
import java.util.AbstractList;
import java.util.Objects;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The {@code unwind} port: one row carrying a list becomes one row per element, the list's field
 * holding a single element in each. The parent's other columns travel on every one of them.
 *
 * <p>Two columns are written beside the element and no others: the ordinal where one was asked for,
 * and the element's own identifying field where the declaration named one. Both are there because a
 * target addresses a row by column and neither is reachable as one otherwise. Lifting the rest of an
 * element's fields is a following {@code map} rather than a second job here.
 *
 * <p>This is the only stateless operator that changes how many rows there are, and everything below
 * follows from that. It is still a pure function of the one event it is handed - an update is paired
 * within itself, never against anything remembered - so nothing here holds state between events.
 *
 * <p><b>An update is paired by key, not by element value.</b> The keys the earlier row expands to,
 * minus the keys the new row expands to, are the rows that have to go; everything the new row
 * expands to is written. Comparing elements instead is wrong in two ways that both leave rows
 * behind for good and report nothing: a parent whose key changes while its list does not looks like
 * nothing happened, and removing an element from the middle of a list numbered by position makes
 * every later element look changed while the last position is left with nobody to delete it.
 *
 * <p><b>Keys are compared unwrapped.</b> A value a source connector converted for travel arrives
 * inside a carrier, and a carrier never equals the plain value inside it - so a key that met a
 * conversion on one side and not the other never matches, and the difference is taken as the whole
 * list having been replaced. Every row that came out is still correct, which is exactly why nothing
 * catches it. Moving the parent's other columns along is the opposite case and deliberately does
 * not unwrap: the write side is owed what the source declared.
 */
final class UnwindPort implements TransformPort {

    private final UnwindSpec spec;
    private final UnwindAlert alert;

    UnwindPort(UnwindSpec spec, UnwindAlert alert) {
        this.spec = spec;
        this.alert = alert;
    }

    @Override
    public List<Envelope> transform(Envelope event) {
        return switch (event.op()) {
            // A schema change carries no row, so there is nothing to expand. Dropping it would break
            // the downstream evolution chain, exactly as it would through a map or a filter.
            case DDL -> List.of(event);
            case INSERT, READ -> expandOneSide(event, event.after(), event.op());
            case DELETE -> {
                UnwindBeforeImage.require(event, spec.path(), spec.parentKey());
                yield expandOneSide(event, event.before(), Op.DELETE);
            }
            case UPDATE -> {
                UnwindBeforeImage.require(event, spec.path(), spec.parentKey());
                yield pairUpdate(event);
            }
        };
    }

    /** Every element of one row's list, as events of {@code op} carrying the row on the right side. */
    private List<Envelope> expandOneSide(Envelope event, Map<String, Object> row, Op op) {
        Expansion expansion = expand(row);
        // Ordinals cannot collide inside one input row. Element-key collisions are reported before the
        // lazy outputs leave, retaining the same alert order without retaining expanded row copies.
        if (spec.elementKey() != null) {
            Set<Object> seen = new LinkedHashSet<>();
            for (int index = 0; index < expansion.size(); index++) {
                Object key = expansion.keyAt(index);
                if (!seen.add(key)) {
                    alert.rowsShareAKey(TransformErrors.unwindRowsShareAKey(spec.path(), key));
                }
            }
        }
        return new AbstractList<>() {
            @Override public int size() { return expansion.size(); }

            @Override public Envelope get(int index) {
                Map<String, Object> expanded = expansion.rowAt(index);
                return op == Op.DELETE ? asDelete(event, expanded) : asRow(event, op, expanded);
            }
        };
    }

    /** Deletes for vanished keys precede the new rows; duplicate new rows retain their original order. */
    private List<Envelope> pairUpdate(Envelope event) {
        Expansion before = expand(event.before());
        Expansion after = expand(event.after());
        Map<Object, Integer> was = keyed(before);
        Map<Object, Integer> now = keyed(after);
        List<Integer> removed = new ArrayList<>();
        was.forEach((key, ordinal) -> {
            if (!now.containsKey(key)) {
                removed.add(ordinal);
            }
        });
        int size = Math.addExact(removed.size(), after.size());
        return new AbstractList<>() {
            @Override public int size() { return size; }

            @Override public Envelope get(int index) {
                Objects.checkIndex(index, size);
                if (index < removed.size()) {
                    return asDelete(event, before.rowAt(removed.get(index)));
                }
                int ordinal = index - removed.size();
                Map<String, Object> row = after.rowAt(ordinal);
                Integer earlier = was.get(after.keyAt(ordinal));
                return earlier == null ? asRow(event, Op.INSERT, row)
                        : new Envelope(Op.UPDATE, event.ts(), event.src(), before.rowAt(earlier), row, event.schema())
                                .withRemoved(event.removed());
            }
        };
    }

    /** Only keys and input ordinals are indexed; expanded rows are created when their output is consumed. */
    private Map<Object, Integer> keyed(Expansion expansion) {
        Map<Object, Integer> byKey = new LinkedHashMap<>();
        for (int index = 0; index < expansion.size(); index++) {
            Object key = expansion.keyAt(index);
            if (byKey.put(key, index) != null) {
                alert.rowsShareAKey(TransformErrors.unwindRowsShareAKey(spec.path(), key));
            }
        }
        return byKey;
    }

    private Expansion expand(Map<String, Object> row) {
        if (row == null) {
            return new Expansion(null, List.of(), false);
        }
        UnwindRules.refuseColumnCollisions(spec.path(), spec.includeArrayIndex(),
                spec.elementKey(), row.keySet());
        Object original = row.get(spec.path());
        Object value = containerValue(original);
        if (value == null || value instanceof List<?> list && list.isEmpty()) {
            return new Expansion(row, List.of(), spec.preserveNullAndEmptyArrays());
        }
        return new Expansion(row, value instanceof List<?> list ? list : List.of(original), false);
    }

    private final class Expansion {
        private final Map<String, Object> row;
        private final List<?> elements;
        private final boolean preservedEmpty;
        private final int size;
        private final List<Object> parent;

        private Expansion(Map<String, Object> row, List<?> elements, boolean preservedEmpty) {
            this.row = row;
            this.elements = elements;
            this.preservedEmpty = preservedEmpty;
            this.size = preservedEmpty ? 1 : elements.size();
            this.parent = new ArrayList<>(spec.parentKey().size());
            if (row != null) {
                spec.parentKey().forEach(column -> parent.add(ConvertedValue.unwrap(row.get(column))));
            }
        }

        private int size() { return size; }

        private Map<String, Object> rowAt(int index) {
            Objects.checkIndex(index, size);
            return rowWith(row, preservedEmpty ? null : elements.get(index), preservedEmpty ? null : (long) index);
        }

        private Object keyAt(int index) {
            Objects.checkIndex(index, size);
            List<Object> key = new ArrayList<>(parent);
            if (spec.elementKey() == null) {
                key.add(preservedEmpty ? null : (long) index);
            } else {
                Object value = preservedEmpty ? null : containerValue(elements.get(index));
                key.add(ConvertedValue.unwrap(value instanceof Map<?, ?> map ? map.get(spec.elementKey()) : null));
            }
            return key;
        }
    }

    /**
     * The parent's row with the list's field replaced by one element, and whatever this expansion
     * writes beside it: the ordinal where one was asked for, and the element's own identifying
     * field where the declaration named one.
     *
     * <p><b>Lifting that field is what makes the row addressable anywhere but here.</b> Inside the
     * element it is reachable by this port and by nothing downstream - a key is matched at the
     * target by column, and no store builds a column for a name that reaches into a value. Left
     * there, every expanded row of one parent arrives keyed on the parent alone and the target
     * keeps the last of them, which is the one failure this whole expansion has to not have.
     *
     * <p>The column is written whether or not the element carries the field, and whether or not
     * there is an element at all. A row short of a column the published model declares is a write
     * that fails on some stores and quietly takes a default on others, and neither is the answer
     * for a row whose identity is simply empty.
     */
    private Map<String, Object> rowWith(Map<String, Object> row, Object element, Long ordinal) {
        Map<String, Object> out = new LinkedHashMap<>(row);
        out.put(spec.path(), element);
        if (spec.includeArrayIndex() != null) {
            out.put(spec.includeArrayIndex(), ordinal);
        }
        if (spec.elementKey() != null) {
            Object value = containerValue(element);
            out.put(spec.elementKey(),
                    value instanceof Map<?, ?> map ? map.get(spec.elementKey()) : null);
        }
        return out;
    }

    /**
     * Reads a container's shape without stripping the carriers of its members. The source may
     * convert an entire array or document; its outer carrier is not itself a scalar element.
     * Nested values and unchanged row fields retain the type names the sink needs.
     */
    private static Object containerValue(Object value) {
        while (value instanceof ConvertedValue carrier) {
            value = carrier.value();
        }
        return value;
    }

    // What an event covers is stamped onto every output by the runtime that drives this port, so
    // the port neither reads nor sets it and stays a function of the row it was handed.
    private Envelope asRow(Envelope event, Op op, Map<String, Object> row) {
        return new Envelope(op, event.ts(), event.src(), null, row, event.schema())
                .withRemoved(event.removed());
    }

    private Envelope asDelete(Envelope event, Map<String, Object> row) {
        return new Envelope(Op.DELETE, event.ts(), event.src(), row, null, event.schema())
                .withRemoved(event.removed());
    }
}
