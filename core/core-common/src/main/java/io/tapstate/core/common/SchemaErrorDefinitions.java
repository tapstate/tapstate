package io.tapstate.core.common;

import java.util.List;
import java.util.Set;

/** Schema advisories describe successful writes; they do not refuse an operation. */
public final class SchemaErrorDefinitions {
    public static final ProductErrorDefinition COLUMN_NAME_READS_AS_A_PATH =
            new ProductErrorDefinition(ProductErrorModule.SCHEMA, 1, Severity.WARNING,
                    Set.of("pipeline", "source", "table", "column", "target"));

    private SchemaErrorDefinitions() {}

    public static List<ProductErrorDefinition> registered() {
        return List.of(COLUMN_NAME_READS_AS_A_PATH);
    }
}
