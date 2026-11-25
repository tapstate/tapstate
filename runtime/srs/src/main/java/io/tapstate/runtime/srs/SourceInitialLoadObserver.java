package io.tapstate.runtime.srs;

import com.hazelcast.core.HazelcastInstance;
import java.io.Serializable;

/** Optional member-local metadata observer. Implementations enqueue bounded facts without IO or waiting. */
@FunctionalInterface
public interface SourceInitialLoadObserver {
    String USER_CONTEXT_KEY = "tapstate.source-initial-load-observer";
    /** Actual producer startup, not an initial target commit, capture barrier or source acknowledgement. */
    void started(HazelcastInstance member, Serializable executionIdentity, long jobId, long executionId,
                 String stream, int processorIndex, String initialMode, long snapshotEpoch);
    default void ended(long jobId, long executionId, String stream, int processorIndex, boolean completedInput) {}
}
