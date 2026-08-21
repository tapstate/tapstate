package io.tapstate.spi.store;

import java.util.Optional;

/** Persistence port for the singleton cluster identity shared by every server node. */
public interface ClusterIdentityStore {

    /** Returns the initialized identity, or empty before the cluster's first server startup. */
    Optional<ClusterIdentity> find();

    /** Atomically inserts {@code proposed} only when absent and returns the stored winner. */
    ClusterIdentity createIfAbsent(ClusterIdentity proposed);
}
