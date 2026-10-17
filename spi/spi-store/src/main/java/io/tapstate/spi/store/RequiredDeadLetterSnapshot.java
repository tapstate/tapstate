package io.tapstate.spi.store;

import java.util.Objects;

/** A retained element revision; a later observation must explicitly compare against this revision. */
public record RequiredDeadLetterSnapshot(long revision, RequiredDeadLetterFact fact) {
    public RequiredDeadLetterSnapshot {
        Objects.requireNonNull(fact, "fact");
        if (revision < 1 || revision == Long.MAX_VALUE) throw new IllegalArgumentException("invalid dead-letter revision");
    }
}
