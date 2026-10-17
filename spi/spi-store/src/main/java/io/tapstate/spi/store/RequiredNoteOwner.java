package io.tapstate.spi.store;

import java.util.Objects;

/** Immutable logical identity of necessary connector notes inside one storage binding. */
public record RequiredNoteOwner(long bindingVersion, String namespace, Kind kind, String ownerId,
        long stateGeneration) {
    public enum Kind { PIPELINE, CHAIN, GLOBAL }

    public RequiredNoteOwner {
        Objects.requireNonNull(namespace, "namespace");
        Objects.requireNonNull(kind, "kind");
        Objects.requireNonNull(ownerId, "ownerId");
        if (bindingVersion < 0 || namespace.isBlank() || ownerId.isBlank()
                || (kind == Kind.PIPELINE ? stateGeneration < 1 : stateGeneration != 0)) {
            throw new IllegalArgumentException("invalid required-note owner");
        }
        boolean validNamespace = switch (kind) {
            case PIPELINE -> namespace.startsWith("pdk.state." + ownerId + ".")
                    && namespace.length() > ("pdk.state." + ownerId + ".").length();
            case CHAIN -> namespace.equals("pdk.chain." + ownerId);
            case GLOBAL -> namespace.equals("pdk.global-state");
        };
        if (!validNamespace) throw new IllegalArgumentException("namespace does not belong to required-note owner");
    }
}
