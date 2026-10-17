package io.tapstate.spi.store;

import io.tapstate.core.model.SourceResource;

import java.util.Optional;

/** One configuration-client transaction publishes the first cache binding and server-owned default Source. */
public interface StorageBindingStore {
    /** Existing request authorization and expiry must be rechecked on each transaction callback attempt. */
    StorageBindingStore guardedBy(Runnable requireActive);

    /** Load-only: missing or inconsistent committed facts are never repaired by a normal restart. */
    Optional<StorageBinding> load();

    /**
     * Called only after the operation's formal connection tests and identity checks. The exact setup
     * generation is checked inside the write; a committed retry must match the same original candidate.
     * Publication does not authorize business assembly or declare cache readiness.
     */
    StorageBinding commit(StorageSetupScope expected, StorageBinding.Mode mode,
            StorageBinding.Connection cache, SourceResource defaultSource);

    /**
     * Publish setup completion only after the caller verifies the saved clients, default Source and
     * formal authentication readiness. OP first-admin pending is created in this same transaction;
     * Cloud never creates a local first-admin record. The original guard remains required.
     */
    StorageBinding complete(StorageSetupScope expected);
}
