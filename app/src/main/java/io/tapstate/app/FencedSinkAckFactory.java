package io.tapstate.app;

import com.hazelcast.core.HazelcastInstance;
import io.tapstate.runtime.engine.SinkAck;
import io.tapstate.runtime.engine.SinkAckFactory;

import java.util.Objects;

/**
 * Holds one run's durable acknowledgements to that run. A position advanced by a superseded run is worse
 * than a write it makes: the write can be replayed, while a watermark moved past changes where every later
 * run resumes from. So the member's own guard is asked before each advance, on the member where the sink
 * actually runs, exactly as it is asked before the batch that earned the position.
 *
 * <p>The store fences the same thing on its own side, by the generations the record carries. This is the
 * near end of that: it stops the call rather than having it rejected, which is what keeps a superseded
 * member from spending its time being turned away.
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

    /** {@code ack}, asking {@code authorization} for {@code fence}'s run before each advance. */
    static SinkAck guarded(SinkAck ack, ExecutionFence fence, ExecutionAuthorization authorization) {
        return (chain, position) -> {
            authorization.require(fence);
            ack.advance(chain, position);
        };
    }
}
