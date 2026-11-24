package io.tapstate.app;

import com.hazelcast.core.HazelcastInstance;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.core.lifecycle.LoadLandings;
import io.tapstate.runtime.engine.SinkAck;
import io.tapstate.runtime.engine.SinkAckFactory;
import io.tapstate.spi.store.WorkloadClaimFence;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Holds one run's durable acknowledgements to that run. A position advanced by a superseded run is worse
 * than a write it makes: the write can be replayed, while a watermark moved past changes where every later
 * run resumes from. A synchronous report checks the guard before advancing. A buffered report carries the
 * claim admitted when the writer resolved and checks it on the checkpoint worker before persistence.
 *
 * <p>Starting the run's accounting is held to the run the same way, on the member that starts it: a
 * superseded run starting its accounting again would take it over from the current run, whose writers would
 * then have nothing they say land.
 *
 * <p>The local answer and the durable boundary are separate halves of the fence. The guard returns the
 * exact live workload claim behind its answer, and the acknowledgement carries that claim to the store,
 * which proves it in the same operation as the progress it records - the run's start, each writer's
 * progress and the pipeline's record alike. An advance already in flight when ownership changes is
 * therefore ignored there rather than moving the position after the run has been superseded.
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
        ExecutionAuthorization authorization = ExecutionAuthorization.of(member);
        boolean deferred = ValidationPipelineInitials.underlying(delegate) instanceof ScopedSrsSinkAckFactory;
        SinkAck resolved = delegate.resolve(member);
        WorkloadClaimFence original = deferred && CheckpointProgressWrites.of(member) != null ? authorization.require(fence) : null;
        SinkAck guarded = new Guarded(resolved, fence, authorization, original);
        return deferred
                ? new DeferredSinkAck(guarded, () -> authorization.authorized(fence))
                : guarded;
    }

    @Override
    public void beginRun(HazelcastInstance coordinator, Map<String, List<String>> writersByChain) {
        delegate.beginRun(coordinator, writersByChain, ExecutionAuthorization.of(coordinator).require(fence));
    }
    @Override public void beginRun(HazelcastInstance coordinator, RunWriters writers) {
        delegate.beginRun(coordinator, writers, ExecutionAuthorization.of(coordinator).require(fence));
    }
    @Override public void beginRun(HazelcastInstance coordinator, ExecutionWriters execution) {
        delegate.beginRun(coordinator, execution, ExecutionAuthorization.of(coordinator).require(fence));
    }

    /**
     * Where the loads stand, read as the wrapped factory reads it. Reading moves nothing, and what it reads
     * already answers only for the run the factory was made for.
     */
    @Override
    public LoadLandings loadLandings(HazelcastInstance member) {
        return delegate.loadLandings(member);
    }

    @Override
    public Set<String> completedSnapshotInputs(HazelcastInstance member, Set<String> streams) {
        ExecutionAuthorization.of(member).require(fence);
        var completed = delegate.completedSnapshotInputs(member, streams);
        ExecutionAuthorization.of(member).require(fence);
        return completed;
    }

    /**
     * {@code ack}, asking {@code authorization} for {@code fence}'s run before everything it lands - and so
     * is every ack bound from it to a writer, which is the ack a sink actually reports through.
     */
    static SinkAck guarded(SinkAck ack, ExecutionFence fence, ExecutionAuthorization authorization) {
        return new Guarded(ack, fence, authorization, null);
    }

    private static final class Guarded implements SinkAck {

        private static final long serialVersionUID = 1L;

        private final SinkAck ack;
        private final ExecutionFence fence;
        private final transient ExecutionAuthorization authorization;
        private final WorkloadClaimFence original;

        Guarded(SinkAck ack, ExecutionFence fence, ExecutionAuthorization authorization, WorkloadClaimFence original) {
            this.ack = ack;
            this.fence = fence;
            this.authorization = authorization;
            this.original = original;
        }

        @Override
        public void advance(String chain, ChainPosition position) {
            ack.advance(chain, position, claim(position.order()));
        }

        @Override
        public void bounded(String chain, SourceOrder through) {
            ack.bounded(chain, through, claim(through));
        }
        private WorkloadClaimFence claim(SourceOrder through) {
            // The async writer rechecks this exact claim before and after persistence.
            if (original != null && through.seq() != SourceOrder.SNAPSHOT_SEQ) return original;
            return authorization.require(fence);
        }
        @Override public void flush(boolean complete) { ack.flush(complete); }
        @Override public void flushOnClose() { if (authorization.authorized(fence)) ack.flushOnClose(); }
        @Override public void started(long jobId, long executionId) {
            try { ack.started(jobId, executionId); }
            catch (RuntimeException failedObservation) { System.getLogger(FencedSinkAckFactory.class.getName()).log(System.Logger.Level.WARNING, "writer initialization observation failed", failedObservation); }
        }

        @Override
        public SinkAck forWriter(String writerId) {
            return new Guarded(ack.forWriter(writerId), fence, authorization, original);
        }
    }
}
