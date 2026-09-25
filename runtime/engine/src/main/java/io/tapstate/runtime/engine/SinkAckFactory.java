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
     * Starts the accounting one execution of the graph lands its progress under, on the member coordinating
     * that execution and before any writer of it exists: for each chain, every writer the graph routes that
     * chain's changes to, named as {@link SinkProcessor#writerId} names them.
     *
     * <p>How far a pipeline has landed a chain is the lowest of what those writers have landed, so the set
     * comes first: progress from a writer nobody knew to wait for would be progress nobody waits on. A factory
     * whose acks do not keep writers apart has nothing to start, which is the default.
     */
    default void beginRun(HazelcastInstance coordinator, Map<String, List<String>> writersByChain) {
    }
}
