package io.tapstate.spi.sink;

import io.tapstate.core.common.TapstateType;
import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * Actual target uniqueness used to order conflicting writes within one execution.
 * A scalar equality proof covers non-null strings and signed integral values. Consumers must
 * reject other value representations before relying on these keys to release conflicting writes.
 * Generated document identity may be omitted only when both event images omit that field.
 */
public record TargetKeyConstraints(List<List<String>> uniqueKeys, boolean exactScalarEquality,
        boolean generatedDocumentIdentity, Map<String, TapstateType> fieldTypes) implements Serializable {

    public TargetKeyConstraints {
        fieldTypes = Map.copyOf(fieldTypes);
        uniqueKeys = uniqueKeys.stream().map(List::copyOf).toList();
    }

    /** No claim can be made about the target's complete equality constraints. */
    public static TargetKeyConstraints unknown() {
        return new TargetKeyConstraints(List.of(), false, false, Map.of());
    }
}
