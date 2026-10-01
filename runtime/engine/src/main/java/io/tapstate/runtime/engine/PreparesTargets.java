package io.tapstate.runtime.engine;

import com.hazelcast.core.HazelcastInstance;

/**
 * A sink's writer factory whose writers write only tables prepared for them before they open: created where
 * missing, cleared or checked for a full load, indexed. The tables are prepared once for each execution of the
 * graph, by {@link #prepareTargets}, and every writer the factory opens then only checks that they were.
 *
 * <p>Once, and not by each writer: a sink runs several, and a table cleared by one after another has started
 * writing into it loses what the other wrote.
 */
public interface PreparesTargets {

    /**
     * Prepares every table this factory's writers will write, on the member coordinating an execution and before
     * any writer of that execution exists.
     */
    void prepareTargets(HazelcastInstance coordinator);
}
