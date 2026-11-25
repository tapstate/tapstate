package io.tapstate.spi.store;

import java.util.Objects;
import java.util.Optional;

/** Read-only original private context under an existing lifecycle operation; this grants no release authority. */
public record RequiredNoteContext(RequiredNoteScope scope,Optional<RequiredNoteOrigin> origin,boolean completed) {
    public RequiredNoteContext {
        Objects.requireNonNull(scope);origin=Objects.requireNonNull(origin);
        if(scope.owner().kind()!=RequiredNoteOwner.Kind.PIPELINE || completed && origin.isPresent())throw new IllegalArgumentException("invalid private context");
    }
}
