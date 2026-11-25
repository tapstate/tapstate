package io.tapstate.app;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import io.tapstate.spi.store.WorkloadClaim;

/** Installs one admitted storage owner's typed member providers before joining. */
interface MemberStorage {
    void prepare(Config config, WorkloadClaim originalNode, long askedAt);
    default void prepare(Config config, WorkloadClaim originalNode, long askedAt, NodeSessionLease startupLease) {
        prepare(config, originalNode, askedAt);
    }
    default boolean processOwnsNode() { return false; }
    void joined(HazelcastInstance member);
}
