package io.tapstate.core.event;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A row value the source connector converted for travel, carried together with the connector's own
 * object that produced it.
 *
 * <p>A connector declares how its driver's own types — a document store's key, a binary column, a
 * decimal the driver has its own class for — become values anything can read. The conversion's result
 * is what every reader downstream wants: it compares, it renders, it joins, it goes into an expression.
 * The object it was converted from is what the write side wants, and only the write side: a target of
 * the same kind puts the value back the way it arrived by reading it, so a key that travelled as text
 * is written as a key rather than as text. Carrying both is what lets each side have the one it needs
 * without asking the other to give its up.
 *
 * <p><b>{@code origin} is deliberately untyped.</b> The type it holds belongs to the connector contract,
 * which one module owns and no other may name; a field declared as that type here would pull the whole
 * contract into the kernel and into every ring above it. What is in there is only ever read back by the
 * module that put it in, which knows what it is.
 *
 * <p><b>Every boundary that uses a row value <i>as a value</i> unwraps first</b>, through
 * {@link #unwrap} — comparing, keying, rendering, binding into an expression. Nothing warns when one
 * does not: this carrier has no {@code equals} worth the name for the value inside it, so a join keyed
 * on an unwrapped column simply never matches and an expression comparing one simply never holds, both
 * without an error. Pass-through paths — anything moving a whole row map along — need no unwrapping and
 * must not do it, or the write side loses what it is owed.
 */
public record ConvertedValue(Object value, Object origin) implements Serializable {

    private static final long serialVersionUID = 1L;

    public ConvertedValue {
        Objects.requireNonNull(origin, "origin");
    }

    /**
     * The value inside, with maps and lists unwrapped through to their leaves; any other value is
     * returned as it is. A container with nothing to unwrap is returned uncopied, so a row that never
     * met a connector conversion costs nothing to pass through this.
     */
    public static Object unwrap(Object value) {
        if (value instanceof ConvertedValue carried) {
            return unwrap(carried.value());
        }
        if (value instanceof Map<?, ?> map) {
            Map<Object, Object> unwrapped = new LinkedHashMap<>(map.size());
            boolean changed = false;
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                Object element = unwrap(entry.getValue());
                changed |= element != entry.getValue();
                unwrapped.put(entry.getKey(), element);
            }
            return changed ? unwrapped : map;
        }
        if (value instanceof List<?> list) {
            List<Object> unwrapped = new ArrayList<>(list.size());
            boolean changed = false;
            for (Object element : list) {
                Object next = unwrap(element);
                changed |= next != element;
                unwrapped.add(next);
            }
            return changed ? unwrapped : list;
        }
        return value;
    }

    /** One row with every value unwrapped, or {@code null} when the map is absent. */
    @SuppressWarnings("unchecked")
    public static Map<String, Object> unwrapRow(Map<String, Object> row) {
        return row == null ? null : (Map<String, Object>) unwrap(row);
    }
}
