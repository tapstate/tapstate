package io.tapstate.spi.sink;

import java.io.Serializable;
import io.tapstate.core.common.TapstateType;
import io.tapstate.core.common.NumericType;
import io.tapstate.core.common.StringType;

/**
 * One field of the resolved target table a sink writes to: its name, its type, and whether it is part
 * of the primary key. An immutable value.
 *
 * <p>{@code type} is the target store's own type token for the field — the string a connector reads to
 * build the column and to coerce each row value to it. It may be null when the type could not be
 * resolved; a null type leaves the connector to infer one. When {@code inferredType} is present it
 * takes precedence: {@code type} then preserves the source spelling for diagnosis, and the adapter
 * converts the inferred portable type using the target connector's own mapping. {@code primaryKey} marks a field as part of
 * the key an upsert matches on; the key's column order follows the field order in the {@link
 * TargetTable}.
 *
 * <p>Serializable so a resolved model travels with the sink factory the engine ships onto the DAG.
 */
public record TargetField(String name, String type, boolean primaryKey, TapstateType inferredType, NumericType numericType, StringType stringType) implements Serializable {

    /** Compatibility constructor for observations without string attributes. */
    public TargetField(String name, String type, boolean primaryKey, TapstateType inferredType, NumericType numericType) {
        this(name, type, primaryKey, inferredType, numericType, null);
    }

    /** Compatibility constructor for target fields without declared numeric attributes. */
    public TargetField(String name, String type, boolean primaryKey, TapstateType inferredType) {
        this(name, type, primaryKey, inferredType, null);
    }

    /** A field with an explicit target type token and no source inference to translate. */
    public TargetField(String name, String type, boolean primaryKey) {
        this(name, type, primaryKey, null);
    }

    public TargetField {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("target field name must be non-blank");
        }
    }
}
