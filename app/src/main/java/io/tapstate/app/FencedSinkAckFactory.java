package io.tapstate.app;

import com.hazelcast.core.HazelcastInstance;
import io.tapstate.runtime.engine.SinkAck;
import io.tapstate.runtime.engine.SinkAckFactory;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Holds one run's durable acknowledgements to that run. A position advanced by a superseded run is worse
 * than a write it makes: the write can be replayed, while a watermark moved past changes where every later
 * run resumes from. So the member's own guard is asked before each advance, on the member where the sink
 * actually runs, exactly as it is asked before the batch that earned the position.
 *
 * <p>The local answer and the durable boundary are separate halves of the fence. The guard returns the
 * exact live workload claim behind its answer, and the acknowledgement carries that claim to the store;
 * an advance already in flight when ownership changes is therefore conditionally ignored there rather
 * than moving the position after the run has been superseded.
 */
final class FencedSinkAckFactory implements SinkAckFactory {

    private static final long serialVersionUID = 1L;

    private final SinkAckFactory delegate;
    private final ExecutionFence fence;

    private FencedSinkAckFactory(SinkAckFactory delegate, ExecutionFence fence) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.fence = Objects.requireNonNull(fence, "fence");
    }

    /** {@code factory} held to {@code fence}'s run, or {@code factory} itself where there is none. */
    static SinkAckFactory heldTo(SinkAckFactory factory, ExecutionFence fence) {
        return fence == null ? factory : new FencedSinkAckFactory(factory, fence);
    }

    @Override
    public SinkAck resolve(HazelcastInstance member) {
        return guarded(delegate.resolve(member), fence, ExecutionAuthorization.of(member));
    }

    @Override
    public void prepareWriterPlan(Map<String, List<String>> writerIdsByStream) {
        delegate.prepareWriterPlan(writerIdsByStream);
    }

    @Override
    public SinkAckFactory forWriter(
            String writerId, List<String> streams, Map<String, List<String>> writerIdsByStream) {
        return new FencedSinkAckFactory(delegate.forWriter(writerId, streams, writerIdsByStream), fence);
    }

    /** {@code ack}, asking {@code authorization} for {@code fence}'s run before each advance. */
    static SinkAck guarded(SinkAck ack, ExecutionFence fence, ExecutionAuthorization authorization) {
        return (chain, position) -> {
            ack.advance(chain, position, authorization.require(fence));
        };
    }
}
