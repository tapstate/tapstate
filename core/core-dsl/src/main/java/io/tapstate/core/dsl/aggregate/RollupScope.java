package io.tapstate.core.dsl.aggregate;

import java.io.Serializable;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * What one element of a level looks like once rendered, as far as its rollup fields may read it: its own
 * columns (with the fields its flat embeds merge in and the rollup fields its own child levels write), the
 * objects and arrays its references resolve to, and the gathered child embeds it carries.
 *
 * <p>{@code columnsKnown} says whether {@code columns} came from a discovered model. A model is a sample for
 * some sources, so a column it does not list is read as an untyped value rather than refused; a reference's
 * model, where one was discovered, is closed instead, so a misspelt reference column is refused when the
 * pipeline is applied rather than read as an absent value forever after.
 */
public record RollupScope(
        Map<String, ValueType> columns,
        boolean columnsKnown,
        Map<String, RollupScope> referenceObjects,
        Map<String, RollupScope> referenceArrays,
        Map<String, Boolean> gathered) implements Serializable {

    private static final long serialVersionUID = 1L;

    public RollupScope {
        columns = copy(columns);
        referenceObjects = copy(referenceObjects);
        referenceArrays = copy(referenceArrays);
        gathered = copy(gathered);
    }

    /** A scope with only columns, which is what a referenced row is. */
    public static RollupScope ofColumns(Map<String, ValueType> columns, boolean known) {
        return new RollupScope(columns, known, Map.of(), Map.of(), Map.of());
    }

    /** Whether {@code path} is where one of this element's references renders. */
    public boolean isReference(String path) {
        return referenceObjects.containsKey(path) || referenceArrays.containsKey(path);
    }

    private static <V> Map<String, V> copy(Map<String, V> map) {
        return map == null ? Map.of() : Collections.unmodifiableMap(new LinkedHashMap<>(map));
    }
}
