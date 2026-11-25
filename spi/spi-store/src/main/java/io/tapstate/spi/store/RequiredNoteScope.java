package io.tapstate.spi.store;

import java.util.Objects;

/** One captured namespace clear generation; an old command must never resolve a replacement token. */
public record RequiredNoteScope(RequiredNoteOwner owner, long generation) {
    public RequiredNoteScope {
        Objects.requireNonNull(owner, "owner");
        if (generation < 1) throw new IllegalArgumentException("required-note generation must be positive");
    }
}
