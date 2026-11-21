package io.tapstate.core.common;

import java.util.List;
import java.util.Set;

/**
 * Schema advisories describe successful writes and do not refuse an operation. The one error here is
 * a source column that leaves no name to read it under.
 */
public final class SchemaErrorDefinitions {
    private static final ProductErrorDefinitionSet DEFINITIONS = new ProductErrorDefinitionSet(ProductErrorModule.SCHEMA);

    public static final ProductErrorDefinition COLUMN_NAME_READS_AS_A_PATH =
            DEFINITIONS.define(1, Severity.WARNING,
                    Set.of("pipeline", "source", "table", "column", "target"));

    public static final ProductErrorDefinition RESERVED_FIELD_RENAMED =
            DEFINITIONS.define(2, Severity.WARNING, Set.of("source", "table", "column", "renamed"));

    public static final ProductErrorDefinition RESERVED_FIELD_RENAME_CONFLICT =
            DEFINITIONS.define(3, Severity.ERROR, Set.of("source", "table", "renamed"));

    private SchemaErrorDefinitions() {}

    public static List<ProductErrorDefinition> registered() {
        return DEFINITIONS.registered();
    }
}
