package io.tapstate.core.validation;

import java.util.List;
import java.util.Objects;

/** Root and ancestor scope remain explicit even when sibling arrays reuse the same element key. */
public record ValidationRoot(String mappingId, String outputId, ValidationKey rootKey,
                             List<Ancestor> ancestors) {
    public ValidationRoot {
        mappingId = ValidationError.identifier(mappingId, "mappingId");
        outputId = ValidationError.identifier(outputId, "outputId");
        Objects.requireNonNull(rootKey, "rootKey");
        if (ancestors == null || ancestors.size() > 32) throw ValidationError.invalid("ancestors");
        ancestors = List.copyOf(ancestors);
    }

    public record Ancestor(List<String> fieldPath, ValidationKey key) {
        public Ancestor {
            if (fieldPath == null || fieldPath.isEmpty() || fieldPath.size() > 32) {
                throw ValidationError.invalid("ancestor.fieldPath");
            }
            for (String segment : fieldPath) ValidationError.identifier(segment, "ancestor.fieldPath");
            fieldPath = List.copyOf(fieldPath);
            Objects.requireNonNull(key, "key");
        }
    }
}
