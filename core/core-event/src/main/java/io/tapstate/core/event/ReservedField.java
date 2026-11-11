package io.tapstate.core.event;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * The one top-level field of a row that belongs to the product rather than to the customer, and the
 * registry of who may put something under it.
 *
 * <p>Everything the product adds to a row that also carries customer columns goes under this single
 * name, one sub-key per owner. That makes "is this ours?" one exact comparison of a top-level name,
 * never a prefix test, and it makes one place enough to stop product data from leaving: whatever
 * serves a row to a target calls {@link #served} and trusts nothing upstream to have cleaned up.
 *
 * <p>Only the top-level name is reserved. A field of the same name nested inside a customer's own
 * sub-document is ordinary data and is never touched here.
 *
 * <p>This class is the only code that spells the name. Everything else reads and writes through it,
 * and a build-time rule refuses the literal anywhere else, so a new kind of product data cannot
 * appear in a row without first appearing in {@link Owner}.
 */
public final class ReservedField {

    /** The reserved top-level field name. */
    public static final String NAME = "__tap";

    /**
     * What a source column that already carries the reserved name is called from capture onwards,
     * unless the source's configuration chooses another name.
     */
    public static final String DEFAULT_SOURCE_RENAME = NAME + "_src";

    /** Whether an owner's data may be written to a target. */
    public enum Delivery {
        /** Bookkeeping. Never written to a target, whatever the target asks for. */
        INTERNAL,
        /** Meant for whoever reads the target. Written unless the target opts out. */
        DELIVERABLE
    }

    /**
     * Every owner that may keep data under the reserved field. Adding one changes what can appear in
     * a customer's target, so it is a reviewed decision and not a code-only change.
     */
    public enum Owner {
        /** Order and model version of a replicated source row. */
        SRS("srs", Delivery.INTERNAL),
        /**
         * The marker of a group whose totals are withheld. Keyed by step id below this owner, because
         * two steps can mark one row once their outputs are embedded into the same document.
         */
        AGGREGATE("aggregate", Delivery.DELIVERABLE);

        private final String key;
        private final Delivery delivery;

        Owner(String key, Delivery delivery) {
            this.key = key;
            this.delivery = delivery;
        }

        /** The sub-key this owner's data sits under. */
        public String key() {
            return key;
        }

        public Delivery delivery() {
            return delivery;
        }
    }

    private ReservedField() {
    }

    /** Whether {@code field}, a top-level field name, is the reserved one. */
    public static boolean is(String field) {
        return NAME.equals(field);
    }

    /** What {@code owner} keeps on {@code row}, or null when it keeps nothing there. */
    public static Object read(Map<String, ?> row, Owner owner) {
        Objects.requireNonNull(owner, "owner");
        return row != null && row.get(NAME) instanceof Map<?, ?> owners ? owners.get(owner.key()) : null;
    }

    /**
     * A copy of {@code row} on which {@code owner} keeps {@code value}; a null value removes what the
     * owner kept, and the reserved field itself once no owner keeps anything. Other owners' data and
     * every customer column are carried over unchanged. The input is not modified.
     */
    public static Map<String, Object> with(Map<String, ?> row, Owner owner, Object value) {
        Objects.requireNonNull(row, "row");
        Objects.requireNonNull(owner, "owner");
        Map<String, Object> owners = new LinkedHashMap<>();
        if (row.get(NAME) instanceof Map<?, ?> present) {
            present.forEach((key, kept) -> owners.put(String.valueOf(key), kept));
        }
        if (value == null) {
            owners.remove(owner.key());
        } else {
            owners.put(owner.key(), value);
        }
        Map<String, Object> copy = new LinkedHashMap<>(row);
        if (owners.isEmpty()) {
            copy.remove(NAME);
        } else {
            copy.put(NAME, Collections.unmodifiableMap(owners));
        }
        return copy;
    }

    /**
     * {@code row} as a target may receive it. The reserved field is dropped whole, and then only the
     * entries of {@link Delivery#DELIVERABLE} owners are written back, and only when {@code deliver}
     * is true. An entry under a sub-key no owner registered is dropped like an internal one: nothing
     * unregistered can be meant for a consumer.
     *
     * <p>Returns {@code row} itself when it carries nothing reserved, which is nearly every row, so the
     * common path allocates nothing.
     */
    public static Map<String, Object> served(Map<String, Object> row, boolean deliver) {
        if (row == null || !row.containsKey(NAME)) {
            return row;
        }
        Map<String, Object> kept = deliver ? deliverable(row.get(NAME)) : Map.of();
        Map<String, Object> copy = new LinkedHashMap<>(row);
        if (kept.isEmpty()) {
            copy.remove(NAME);
        } else {
            copy.put(NAME, kept);
        }
        return copy;
    }

    /**
     * As {@link #served(Map, boolean)}, also applied to the rows embedded at {@code embeddedAt}. Each
     * entry is the list of field names leading from the top of the document to a place where a whole
     * row was embedded, as an object or as the elements of an array. The places are known when a
     * pipeline is compiled; a document is never searched for the name, because below the top level
     * the same name inside a customer's own data is theirs.
     */
    public static Map<String, Object> served(
            Map<String, Object> row, boolean deliver, List<List<String>> embeddedAt) {
        Map<String, Object> top = served(row, deliver);
        if (top == null || embeddedAt == null || embeddedAt.isEmpty()) {
            return top;
        }
        Map<String, Object> result = top;
        for (List<String> path : embeddedAt) {
            if (!path.isEmpty()) {
                result = servedBelow(result, path, 0, deliver);
            }
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> servedBelow(
            Map<String, Object> level, List<String> path, int depth, boolean deliver) {
        String field = path.get(depth);
        Object child = level.get(field);
        Object replaced = child;
        boolean last = depth == path.size() - 1;
        if (child instanceof Map<?, ?> map) {
            Map<String, Object> embedded = (Map<String, Object>) map;
            replaced = last ? served(embedded, deliver) : servedBelow(embedded, path, depth + 1, deliver);
        } else if (child instanceof List<?> elements) {
            List<Object> copies = null;
            for (int index = 0; index < elements.size(); index++) {
                Object element = elements.get(index);
                Object served = element;
                if (element instanceof Map<?, ?> map) {
                    Map<String, Object> embedded = (Map<String, Object>) map;
                    served = last ? served(embedded, deliver) : servedBelow(embedded, path, depth + 1, deliver);
                }
                if (served != element && copies == null) {
                    copies = new ArrayList<>(elements);
                }
                if (copies != null) {
                    copies.set(index, served);
                }
            }
            replaced = copies == null ? child : copies;
        }
        if (replaced == child) {
            return level;
        }
        Map<String, Object> copy = new LinkedHashMap<>(level);
        copy.put(field, replaced);
        return copy;
    }

    private static Map<String, Object> deliverable(Object owners) {
        if (!(owners instanceof Map<?, ?> present)) {
            return Map.of();
        }
        Map<String, Object> kept = new LinkedHashMap<>();
        for (Owner owner : Owner.values()) {
            if (owner.delivery() == Delivery.DELIVERABLE && present.get(owner.key()) != null) {
                kept.put(owner.key(), present.get(owner.key()));
            }
        }
        return kept.isEmpty() ? Map.of() : Collections.unmodifiableMap(kept);
    }
}
