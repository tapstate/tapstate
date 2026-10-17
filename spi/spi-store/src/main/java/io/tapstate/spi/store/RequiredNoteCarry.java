package io.tapstate.spi.store;

import java.util.Objects;

/** A retained, read-only dependency on one exact private note scope, never a runtime lease. */
public record RequiredNoteCarry(RequiredNoteScope shared, RequiredNoteScope source) {
    public RequiredNoteCarry {
        Objects.requireNonNull(shared, "shared");
        Objects.requireNonNull(source, "source");
        if (shared.owner().kind() != RequiredNoteOwner.Kind.CHAIN
                || source.owner().kind() != RequiredNoteOwner.Kind.PIPELINE
                || shared.owner().bindingVersion() != source.owner().bindingVersion()) {
            throw new IllegalArgumentException("carry requires a shared chain and private scope in the same binding");
        }
    }
}
