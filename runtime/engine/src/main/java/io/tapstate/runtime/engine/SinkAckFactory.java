package io.tapstate.runtime.engine;

import com.hazelcast.core.HazelcastInstance;
import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * Resolves, on the member it runs on, the {@link SinkAck} a sink advances its durable watermark through. It
 * exists because the sink vertex is serialized on job submit, yet the durable coordination store the ack
 * writes is not serializable and lives on the member. So this factory - holding only serializable coordinates,
 * never the store - is carried onto the sink and, once on the member, resolves the actual store (from the
 * member's user context) and binds the ack. It is the sink-side mirror of the source's read-cursor publisher.
 *
 * <p>A member that has no store bound yet resolves to a no-op ack, so a sink still runs before the assembly
 * layer populates the member's user context.
 */
@FunctionalInterface
public interface SinkAckFactory extends Serializable {

    /** A factory that resolves to an ack that records nothing - the default when no store is bound. */
    SinkAckFactory NONE = member -> (chain, srcpos) -> { };

    /**
     * Resolves the {@link SinkAck} on {@code member}: the seam the sink advances one chain's durable
     * watermark through, bound to the store the member holds.
     */
    SinkAck resolve(HazelcastInstance member);

    /**
     * Gives the factory the complete writer plan while the DAG is being assembled, before it can be
     * submitted. A durable implementation uses this hook to reject retained aggregate progress that
     * cannot truthfully be assigned to several writers. The default has no preparation to perform.
     */
    default void prepareWriterPlan(Map<String, List<String>> writerIdsByStream) {
    }

    /**
     * Binds this factory to one stable sink writer and the complete writer plan for every source stream.
     * A store-backed implementation uses the plan to keep each writer's progress apart and expose only
     * the minimum as the pipeline's durable position. Factories that do not persist progress need no
     * additional binding and retain their existing behaviour.
     */
    default SinkAckFactory forWriter(
            String writerId, List<String> streams, Map<String, List<String>> writerIdsByStream) {
        return this;
    }
}
