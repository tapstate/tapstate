package io.tapstate.runtime.engine.nest;

/**
 * Where a nest namespace's readings are left for the outside to find. The readings are of a run in flight
 * and of nothing else, so they are published as statistics of the job they belong to: the job is what
 * scopes them to a pipeline and what makes them disappear when it does. A reading kept anywhere else would
 * have to be told when the run ended, and would answer with the last run's numbers until it was.
 */
interface NestStateGauge {

    /** A gauge for a store driven outside a running job, where there are no job statistics to leave any in. */
    NestStateGauge NONE = (namespace, entries, accesses, backfills, backfillMillis) -> {
    };

    /**
     * Leaves one namespace's current readings. Called on the thread that reads the state, at a cadence of
     * the caller's choosing rather than per access - one of the four costs a round trip to work out.
     */
    void reading(String namespace, long entries, long accesses, long backfills, long backfillMillis);
}
