package io.tapstate.core.common;

import java.util.List;
import java.util.Set;

/** Schema advisories describe successful writes; they do not refuse an operation. */
public final class SchemaErrorDefinitions {
    private static final ProductErrorDefinitionSet DEFINITIONS = new ProductErrorDefinitionSet(ProductErrorModule.SCHEMA);

    public static final ProductErrorDefinition COLUMN_NAME_READS_AS_A_PATH =
            DEFINITIONS.define(1, Severity.WARNING,
                    Set.of("pipeline", "source", "table", "column", "target"));

    private SchemaErrorDefinitions() {}

    public static List<ProductErrorDefinition> registered() {
        return DEFINITIONS.registered();
    }
}
