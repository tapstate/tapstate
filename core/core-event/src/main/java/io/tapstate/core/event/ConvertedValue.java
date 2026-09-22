package io.tapstate.core.event;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

/**
 * A row value the source connector converted for travel, carried together with the name the source's
 * own schema gives it.
 *
 * <p>A connector declares how its driver's own types — a document store's key, a binary column, a
 * decimal the driver has its own class for — become values anything can read. The conversion's result
 * is what every reader downstream wants: it compares, it renders, it joins, it goes into an expression.
 * The declared name is what the write side wants, and only the write side: a connector's own way back
 * from a converted value reads it to decide what to rebuild, so a key that travelled as text is written
 * as a key rather than as text. Carrying both is what lets each side have the one it needs.
 *
 * <p>That name is what the source's schema calls the value: the declared type of the place it came out
 * of — a column, or a field inside a document, which discovery spells as the dotted path that reaches
 * it. An array's elements are the one thing that schema has no place for at all, being positional and
 * free to differ from each other, so for those it is the name that same schema gives the element's own
 * driver type where it does name a place holding one. Never the array's own declared name: that would
 * rebuild every element as whatever the array is declared to be, and succeed.
 *
 * <p><b>What is carried is a name, never the driver's object.</b> Two connectors on one pipeline are two
 * isolated class loaders, so the source's object is a type the target cannot read even when both speak
 * the same driver — the classes share a name and nothing else. Handing it over threw a cast error that
 * named one class twice and took the whole run down on the first row. A name crosses both a loader
 * boundary and a serializer, which is why the object is not here and the object's *declared type* is.
 *
 * <p><b>{@code originType} is null where the source's schema names neither</b> — a column or a nested
 * field discovery could not describe, or an array element of a driver type that schema names nowhere in
 * the row. Null rather than empty on purpose: a connector's way back tests it, and "no declared type"
 * and "a type spelled with no characters" must not arrive as the same answer.
 *
 * <p><b>Every boundary that uses a row value <i>as a value</i> exposes the value first</b> — through
 * {@link #unwrap} when comparing, keying, rendering or binding into an expression, and through a tracked
 * proxy when a script reads it. Nothing warns when one does not, and the way it goes wrong is quiet: a
 * carrier never equals the plain value inside it, so a join between a side that met a conversion and a
 * side that did not simply never matches, and an expression comparing one simply never holds, both
 * without an error. Two carriers do compare by their parts, which makes the failure worse rather than
 * better — a join with conversions on both sides works until the two schemas spell the column
 * differently, and then stops matching for a reason nothing on that path names. Pass-through paths —
 * anything moving a whole row map along — preserve the carrier, as does an untouched script slot, or the
 * write side loses what it is owed.
 */
public record ConvertedValue(Object value, String originType) implements Serializable {

    private static final long serialVersionUID = 1L;

    public ConvertedValue {
        Objects.requireNonNull(value, "value");
    }

    /**
     * The value inside, with maps and lists unwrapped through to their leaves; any other value is
     * returned as it is. A container with nothing to unwrap is returned uncopied, so a row that never
     * met a connector conversion costs nothing to pass through this.
     */
    public static Object unwrap(Object value) {
        return unwrap(value, ConvertedValue::value);
    }

    /**
     * The same walk, with each carrier replaced by what {@code carried} makes of it rather than by the
     * value inside — for the one side that rebuilds the driver's own type from it. Every other
     * caller wants {@link #unwrap(Object)}, which is this with the value.
     */
    public static Object unwrap(Object value, Function<ConvertedValue, Object> carried) {
        if (value instanceof ConvertedValue carrier) {
            return unwrap(carried.apply(carrier), carried);
        }
        if (value instanceof Map<?, ?> map) {
            Map<Object, Object> unwrapped = new LinkedHashMap<>(map.size());
            boolean changed = false;
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                Object element = unwrap(entry.getValue(), carried);
                changed |= element != entry.getValue();
                unwrapped.put(entry.getKey(), element);
            }
            return changed ? unwrapped : map;
        }
        if (value instanceof List<?> list) {
            List<Object> unwrapped = new ArrayList<>(list.size());
            boolean changed = false;
            for (Object element : list) {
                Object next = unwrap(element, carried);
                changed |= next != element;
                unwrapped.add(next);
            }
            return changed ? unwrapped : list;
        }
        return value;
    }

    /** One row with every value unwrapped, or {@code null} when the map is absent. */
    public static Map<String, Object> unwrapRow(Map<String, Object> row) {
        return unwrapRow(row, ConvertedValue::value);
    }

    /** One row with each carrier replaced by what {@code carried} makes of it. */
    @SuppressWarnings("unchecked")
    public static Map<String, Object> unwrapRow(Map<String, Object> row, Function<ConvertedValue, Object> carried) {
        return row == null ? null : (Map<String, Object>) unwrap(row, carried);
    }
}
