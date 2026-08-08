package io.tapstate.runtime.engine;

import com.hazelcast.core.HazelcastInstance;
import java.io.Serializable;

/**
 * Resolves, on the member it runs on, the {@link ReplayFloor} an operator reads a chain's durable resume
 * position back through. It exists for the same reason the sink-side ack factory does: the vertex is
 * serialized when the job is submitted, yet the durable coordination store is not serializable and lives on
 * the member. So this factory - holding only serializable coordinates, never the store - is carried onto the
 * vertex and binds the real store once it is there. It is the read-side mirror of the sink's ack seam.
 *
 * <p>A member with no store bound resolves to a floor that knows nothing, so an operator runs unchanged
 * before the assembly layer populates the member's user context and simply forgets nothing while it waits.
 */
@FunctionalInterface
public interface ReplayFloorFactory extends Serializable {

    /** A factory that resolves to a floor that knows nothing - the default when no store is bound. */
    ReplayFloorFactory NONE = member -> ReplayFloor.NONE;

    /**
     * Resolves the {@link ReplayFloor} on {@code member}: the seam an operator reads durable resume
     * positions through, bound to the store the member holds.
     */
    ReplayFloor resolve(HazelcastInstance member);
}
