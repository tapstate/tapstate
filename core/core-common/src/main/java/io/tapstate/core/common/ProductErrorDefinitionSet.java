package io.tapstate.core.common;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** A module-local definition collection populated while its owner class initializes. */
final class ProductErrorDefinitionSet {
    private final ProductErrorModule module;
    private final Map<Integer, ProductErrorDefinition> definitions = new LinkedHashMap<>();
    private List<ProductErrorDefinition> sealed;

    ProductErrorDefinitionSet(ProductErrorModule module) {
        this.module = java.util.Objects.requireNonNull(module);
    }

    synchronized ProductErrorDefinition define(int serial, Severity severity, Set<String> parameters) {
        if (sealed != null) throw new IllegalStateException("diagnostic definitions are already sealed");
        var definition = new ProductErrorDefinition(module, serial, severity, parameters);
        if (definitions.putIfAbsent(serial, definition) != null)
            throw new IllegalStateException("duplicate diagnostic serial " + serial);
        return definition;
    }

    synchronized List<ProductErrorDefinition> registered() {
        if (sealed == null) sealed = List.copyOf(definitions.values());
        return sealed;
    }
}
