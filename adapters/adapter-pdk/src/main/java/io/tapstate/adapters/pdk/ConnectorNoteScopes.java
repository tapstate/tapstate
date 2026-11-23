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
    default boolean batchSharedStartup() { return false; }

    /** For shared captures, the note owner is the chain but connector logs still belong to the opening Pipeline. */
    default long logStateGeneration() { return 0; }

    /** The opener loads previously prepared scopes; it must not initialize or repair scopes on a restart. */
    static ConnectorNoteScopes of(RequiredNoteStore store, Function<String, RequiredNoteScope> opener,
            Runnable requireActive) {
        return new BoundConnectorNoteScopes(store, opener, requireActive);
    }

    static ConnectorNoteScopes forBoundRun(RequiredNoteStore store, String logicalPrimary,
            RequiredNoteScope primary, RequiredNoteScope global, Runnable requireActive) {
        return forBoundRun(store, logicalPrimary, primary, global, requireActive, primary.owner().stateGeneration());
    }

    static ConnectorNoteScopes forBoundRun(RequiredNoteStore store, String logicalPrimary,
            RequiredNoteScope primary, RequiredNoteScope global, Runnable requireActive, long logStateGeneration) {
        Objects.requireNonNull(logicalPrimary, "logicalPrimary");
        String physical = primary.owner().namespace();
        return new BoundConnectorNoteScopes(store, namespace -> {
            if (namespace.equals(physical)) return primary;
            if (namespace.equals(global.owner().namespace())) return global;
            throw new io.tapstate.core.common.TapstateException(io.tapstate.spi.store.IoError.NOTE_SCOPE_UNAVAILABLE,
                    java.util.Map.of(), null);
        }, requireActive, false, logicalPrimary, physical, logStateGeneration);
    }

    static ConnectorNoteScopes forSharedStartup(RequiredNoteStore store, Function<String, RequiredNoteScope> opener,
            Runnable requireActive) {
        return new BoundConnectorNoteScopes(store, opener, requireActive, true);
    }

    static ConnectorNoteScopes forCapture(RequiredNoteStore store, Function<String, RequiredNoteScope> opener,
            Runnable requireActive, boolean batchSharedStartup, long logStateGeneration) {
        return new BoundConnectorNoteScopes(store, opener, requireActive, batchSharedStartup, logStateGeneration);
    }

    default ConnectorNoteScopes guardedBy(Runnable additionalGuard) {
        Objects.requireNonNull(additionalGuard, "additionalGuard");
        return forCapture(store(), this::open, () -> {
            requireActive();
            additionalGuard.run();
        }, batchSharedStartup(), logStateGeneration());
    }
}
