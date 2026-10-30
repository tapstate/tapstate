package io.tapstate.adapters.pdk;

import io.tapstate.spi.store.RequiredNoteScope;
import io.tapstate.spi.store.RequiredNoteStore;
import java.util.Objects;
import java.util.function.Function;

/** Keeps each original guard around an access without rewrapping already guarded stores and openers. */
final class BoundConnectorNoteScopes implements ConnectorNoteScopes {
    private final RequiredNoteStore originalStore;
    private final RequiredNoteStore guardedStore;
    private final Function<String, RequiredNoteScope> opener;
    private final Runnable active;

    BoundConnectorNoteScopes(RequiredNoteStore store, Function<String, RequiredNoteScope> opener, Runnable active) {
        this.originalStore = Objects.requireNonNull(store, "store");
        this.opener = Objects.requireNonNull(opener, "opener");
        this.active = Objects.requireNonNull(active, "active");
        this.guardedStore = store.guardedBy(active);
    }

    @Override public RequiredNoteStore store() { return guardedStore; }
    @Override public void requireActive() { active.run(); }
    @Override public RequiredNoteScope open(String namespace) {
        active.run();
        var scope = Objects.requireNonNull(opener.apply(namespace), "scope");
        if (!scope.owner().namespace().equals(namespace))
            throw new IllegalStateException("the note opener returned another namespace");
        active.run();
        return scope;
    }
    @Override public ConnectorNoteScopes guardedBy(Runnable additionalGuard) {
        Objects.requireNonNull(additionalGuard, "additionalGuard");
        return new BoundConnectorNoteScopes(originalStore, opener, () -> {
            active.run(); additionalGuard.run();
        });
    }

    /** A release must retain the exact primary scope captured with its connector origin. */
    static ConnectorNoteScopes pin(ConnectorNoteScopes scopes, RequiredNoteScope original) {
        Objects.requireNonNull(scopes, "scopes"); Objects.requireNonNull(original, "original");
        String namespace = original.owner().namespace();
        if (scopes instanceof BoundConnectorNoteScopes bound) {
            return new BoundConnectorNoteScopes(bound.originalStore,
                    requested -> namespace.equals(requested) ? original : bound.opener.apply(requested), bound.active);
        }
        // A custom provider owns its own checks; preserve its public contract when it cannot be unwrapped.
        return ConnectorNoteScopes.of(scopes.store(),
                requested -> namespace.equals(requested) ? original : scopes.open(requested), scopes::requireActive);
    }
}
