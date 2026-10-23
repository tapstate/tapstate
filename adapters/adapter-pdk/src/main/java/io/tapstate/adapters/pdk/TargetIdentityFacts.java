package io.tapstate.adapters.pdk;

import java.util.List;
import java.util.Set;

/** Target constraints read through the connector, separate from the source-derived write model. */
public record TargetIdentityFacts(boolean exists, boolean flexibleColumns, List<String> primaryKey,
        Set<String> generatedColumns, Set<String> columns, List<List<String>> uniqueKeys,
        Set<String> nullableColumns, boolean nullsCompareEqual, boolean logicalKeyWrites,
        boolean mayPrepareLogicalIndex, boolean opaqueUniqueConstraints, Set<String> signedInt64Columns) {
    public TargetIdentityFacts {
        primaryKey = List.copyOf(primaryKey);
        generatedColumns = Set.copyOf(generatedColumns);
        columns = Set.copyOf(columns);
        uniqueKeys = uniqueKeys.stream().map(List::copyOf).toList();
        nullableColumns = Set.copyOf(nullableColumns);
        signedInt64Columns = Set.copyOf(signedInt64Columns);
    }
}
