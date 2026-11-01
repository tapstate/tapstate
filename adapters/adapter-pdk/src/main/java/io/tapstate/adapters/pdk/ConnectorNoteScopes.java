package io.tapstate.adapters.pdk;

import io.tapstate.spi.store.RequiredNoteScope;
import io.tapstate.spi.store.RequiredNoteStore;

import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;
import io.tapstate.spi.store.CaptureOpeningJournal;

/** Opens authorized configuration note scopes before plugin code and preserves the drive's existing guard. */
public interface ConnectorNoteScopes {
    RequiredNoteStore store();
    RequiredNoteScope open(String namespace);
    void requireActive();

    /** Called before constructing a source plugin; legacy/read-only scopes have no capture journal. */
    default CaptureOpeningJournal.Opening newOpening() { return null; }

    /** The opener loads previously prepared scopes; it must not initialize or repair scopes on a restart. */
    static ConnectorNoteScopes of(RequiredNoteStore store, Function<String, RequiredNoteScope> opener,
            Runnable requireActive) {
        return new BoundConnectorNoteScopes(store, opener, requireActive);
    }

    static ConnectorNoteScopes journaled(RequiredNoteStore store, Function<String, RequiredNoteScope> opener,
            Runnable requireActive, Supplier<CaptureOpeningJournal.Opening> openings) {
        return new BoundConnectorNoteScopes(store, opener, requireActive, openings);
    }

    default ConnectorNoteScopes guardedBy(Runnable additionalGuard) {
        Objects.requireNonNull(additionalGuard, "additionalGuard");
        return journaled(store(), this::open, () -> {
            requireActive();
            additionalGuard.run();
        }, this::newOpening);
    }
}
