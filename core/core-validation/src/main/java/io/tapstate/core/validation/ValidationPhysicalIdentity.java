package io.tapstate.core.validation;

import java.util.List;
import java.util.Objects;

/** Actual fields read from one physical table, separate from the comparison key. */
public record ValidationPhysicalIdentity(String sourceId, String database, String schema, String table,
                                         List<List<String>> keyFields, ValidationKey actualKey) {
    public ValidationPhysicalIdentity {
        sourceId = ValidationError.identifier(sourceId, "sourceId");
        if (database != null) database = ValidationError.identifier(database, "database");
        if (schema != null) schema = ValidationError.identifier(schema, "schema");
        table = ValidationError.identifier(table, "table");
        Objects.requireNonNull(actualKey, "actualKey");
        if (keyFields == null || keyFields.size() != actualKey.parts().size()) {
            throw ValidationError.invalid("physicalKey.fields");
        }
        for (var field : keyFields) {
            if (field == null || field.isEmpty() || field.size() > 32) {
                throw ValidationError.invalid("physicalKey.fieldPath");
            }
            for (String segment : field) ValidationError.identifier(segment, "physicalKey.fieldPath");
        }
        keyFields = keyFields.stream().map(List::copyOf).toList();
        if (keyFields.stream().distinct().count() != keyFields.size()) {
            throw ValidationError.invalid("physicalKey.duplicateField");
        }
    }
}
