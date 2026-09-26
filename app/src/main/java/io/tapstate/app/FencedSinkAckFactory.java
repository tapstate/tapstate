package io.tapstate.app;

import com.hazelcast.core.HazelcastInstance;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.core.lifecycle.LoadLandings;
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
 * <p>Starting the run's accounting is held to the run the same way, on the member that starts it: a
 * superseded run starting its accounting again would take it over from the current run, whose writers would
 * then have nothing they say land.
 *
 * <p>On this path that guard is most of the fence, and the limit is worth stating where it is felt. A
 * writer's own progress is refused at the store once a later run has started its accounting, but what the
 * pipeline's record says is written after that, and that record carries no generation of its own, so nothing
 * on the far side turns a late write of it away. The capture side does carry one and is refused at the store.
 * Until the same holds here, a write that leaves a member after its guard last said yes -- one already in
 * flight, or one riding the slack between the two clocks -- is not caught a second time.
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
    public void beginRun(HazelcastInstance coordinator, Map<String, List<String>> writersByChain) {
        ExecutionAuthorization.of(coordinator).require(fence);
        delegate.beginRun(coordinator, writersByChain);
    }

    /**
     * Where the loads stand, read as the wrapped factory reads it. Reading moves nothing, and what it reads
     * already answers only for the run the factory was made for.
     */
    @Override
    public LoadLandings loadLandings(HazelcastInstance member) {
        return delegate.loadLandings(member);
    }

    /**
     * {@code ack}, asking {@code authorization} for {@code fence}'s run before everything it lands - and so
     * is every ack bound from it to a writer, which is the ack a sink actually reports through.
     */
    static SinkAck guarded(SinkAck ack, ExecutionFence fence, ExecutionAuthorization authorization) {
        return new Guarded(ack, fence, authorization);
    }

    private static final class Guarded implements SinkAck {

        private static final long serialVersionUID = 1L;

        private final SinkAck ack;
        private final ExecutionFence fence;
        private final transient ExecutionAuthorization authorization;

        Guarded(SinkAck ack, ExecutionFence fence, ExecutionAuthorization authorization) {
            this.ack = ack;
            this.fence = fence;
            this.authorization = authorization;
        }

        @Override
        public void advance(String chain, ChainPosition position) {
            authorization.require(fence);
            ack.advance(chain, position);
        }

        @Override
        public void bounded(String chain, SourceOrder through) {
            authorization.require(fence);
            ack.bounded(chain, through);
        }

        @Override
        public SinkAck forWriter(String writerId) {
            return new Guarded(ack.forWriter(writerId), fence, authorization);
        }
    }
}
