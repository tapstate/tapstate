package io.tapstate.adapters.pdk;

import io.tapstate.spi.store.RequiredNoteScope;
import io.tapstate.spi.store.RequiredNoteStore;

import java.util.Objects;
import java.util.function.Function;

/** Opens authorized configuration note scopes before plugin code and preserves the drive's existing guard. */
public interface ConnectorNoteScopes {
    RequiredNoteStore store();
    RequiredNoteScope open(String namespace);
    void requireActive();

    /** The opener loads previously prepared scopes; it must not initialize or repair scopes on a restart. */
    static ConnectorNoteScopes of(RequiredNoteStore store, Function<String, RequiredNoteScope> opener,
            Runnable requireActive) {
        return new BoundConnectorNoteScopes(store, opener, requireActive);
    }

    default ConnectorNoteScopes guardedBy(Runnable additionalGuard) {
        Objects.requireNonNull(additionalGuard, "additionalGuard");
        return of(store(), this::open, () -> {
            requireActive();
            additionalGuard.run();
        });
    }
}
