package io.tapstate.runtime.engine.nest;

import com.hazelcast.core.HazelcastInstance;

import java.io.Serializable;

/**
 * Where an explicit abandonment decision places changes that cannot be handed on normally.
 *
 * <p>An ordinary missing or deleted parent is not such a decision: its children wait in durable state
 * for a later declaration. Separate relocation ownership rules can still abandon an unclaimed
 * handover. Whoever owns that decision must write the changes here before reclaiming their last copy.
 */
@FunctionalInterface
public interface NestDeadLetter extends Serializable {

    /** Hands over a change that can never reach a document, from the level that was holding it. */
    void unassemblable(NestVertex from, ReleasedChild released);

    /**
     * Binds this to the member whose vertices it is about to serve, returning what to hand changes to from
     * there on. It exists for the reason the stores have the same hook: this is serialized when the job is
     * submitted, while somewhere durable to put a change is reached through a handle that is not — only
     * coordinates travel, and the live handle is picked up once the vertex is where it will run. Somewhere
     * that needs nothing from the member answers itself, which is why the default is to do so.
     */
    default NestDeadLetter bind(HazelcastInstance member) {
        return this;
    }
}
