package io.tapstate.control.core;

import io.tapstate.spi.store.ClusterIdentity;
import io.tapstate.spi.store.ClusterIdentityStore;

import java.util.Objects;
import java.util.UUID;
import java.util.function.Supplier;

/** Initializes once and thereafter reads the stable identity persisted for this cluster. */
public final class ClusterIdentityService {

    private final ClusterIdentityStore store;
    private final Supplier<String> ids;

    public ClusterIdentityService(ClusterIdentityStore store) {
        this(store, () -> UUID.randomUUID().toString());
    }

    public ClusterIdentityService(ClusterIdentityStore store, Supplier<String> ids) {
        this.store = Objects.requireNonNull(store, "store");
        this.ids = Objects.requireNonNull(ids, "ids");
    }

    /** Returns the storage identity inside control-core; presentation layers use {@link #identityView()}. */
    ClusterIdentity identity() {
        return store.find().orElseGet(() -> store.createIfAbsent(new ClusterIdentity(ids.get())));
    }

    /** Projects the stable identity without exposing the storage port type to presentation layers. */
    public ClusterIdentityView identityView() {
        ClusterIdentity identity = identity();
        return new ClusterIdentityView(identity.issuer(), identity.clusterId());
    }
}
