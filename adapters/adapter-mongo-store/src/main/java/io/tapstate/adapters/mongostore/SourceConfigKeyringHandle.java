package io.tapstate.adapters.mongostore;

import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.WorkloadClaim;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

/** Process-local, refreshable view of the metadata-backed Cluster keyring. */
public final class SourceConfigKeyringHandle implements SourceConfigCipherProvider, SourceConfigKeyringSession {

    private final SourceConfigKeyringStore store;
    private final AtomicReference<SourceConfigKeyringStore.Loaded> loaded;

    SourceConfigKeyringHandle(SourceConfigKeyringStore store) {
        this.store = Objects.requireNonNull(store, "store");
        this.loaded = new AtomicReference<>(store.loadExisting());
    }

    @Override
    public SourceConfigCipher current() {
        return loaded.get().cipher();
    }

    @Override
    public SourceConfigCipher refresh() {
        SourceConfigKeyringStore.Loaded next = store.loadExisting();
        loaded.set(next);
        return next.cipher();
    }

    @Override
    public boolean requiresWriteFence() {
        return true;
    }

    @Override
    public boolean fenceWrite(SourceConfigWriteScope scope, String keyId) {
        return scope.fence(store, keyId);
    }

    public long epoch() {
        return loaded.get().epoch();
    }

    /** Persists the next key without changing the active writer; safe to retry after a racing caller. */
    public long prepareRotation() {
        SourceConfigKeyringStore.Loaded next = store.prepareRotation();
        loaded.set(next);
        return next.epoch();
    }

    /** Activates a prepared key only after every live node session loaded it, then rewrites old envelopes. */
    public long activatePrepared() {
        SourceConfigKeyringStore.Loaded next = store.activatePrepared();
        loaded.set(next);
        return next.epoch();
    }

    /** Reloads every key for the current epoch, then binds this exact live node-session to it. */
    @Override
    public void acknowledge(WorkloadClaim session, Duration ttl) {
        for (int attempt = 0; attempt < 3; attempt++) {
            SourceConfigKeyringStore.Loaded next = store.loadExisting();
            if (store.acknowledge(session, next.epoch(), ttl)) {
                loaded.set(next);
                return;
            }
        }
        throw new TapstateException(StoreError.SOURCE_CONFIG_KEYRING_NOT_READY, Map.of(), null);
    }

    /** Ends only this exact boot/generation acknowledgement; a replacement node remains untouched. */
    @Override
    public void release(WorkloadClaim session) {
        store.release(session);
    }
}
