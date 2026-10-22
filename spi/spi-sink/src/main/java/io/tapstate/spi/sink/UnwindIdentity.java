package io.tapstate.spi.sink;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

/** Parent identity retained through expansion, before any target-specific projection. */
public record UnwindIdentity(List<String> parentKey, String sourceIdField) implements Serializable {
    public UnwindIdentity {
        parentKey = List.copyOf(parentKey);
        Objects.requireNonNull(sourceIdField, "sourceIdField");
    }
}
