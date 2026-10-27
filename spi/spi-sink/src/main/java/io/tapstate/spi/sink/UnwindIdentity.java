package io.tapstate.spi.sink;

import java.io.Serializable;
import java.util.List;

/** Parent identity retained through expansion, before any target-specific projection. */
public record UnwindIdentity(List<String> parentKey, List<String> sourceIdFields) implements Serializable {
    public UnwindIdentity {
        parentKey = List.copyOf(parentKey);
        sourceIdFields = sourceIdFields.stream().distinct().toList();
    }
    public UnwindIdentity(List<String> parentKey, String sourceIdField) {
        this(parentKey, List.of(sourceIdField));
    }

    /** Null means the actual target must decide whether ambiguous names are relevant. */
    public String sourceIdField() {
        return sourceIdFields.size() == 1 ? sourceIdFields.getFirst() : null;
    }
}
