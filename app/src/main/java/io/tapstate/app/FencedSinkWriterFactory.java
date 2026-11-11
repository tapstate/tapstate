package io.tapstate.app;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.function.SupplierEx;
import io.tapstate.core.event.Envelope;
import io.tapstate.runtime.engine.PreparesTargets;
import io.tapstate.spi.sink.SinkWriter;
import io.tapstate.spi.sink.WriteResult;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletionStage;

/**
 * Holds one run's writes to that run. The factory crosses to whichever member runs the sink vertex, and
 * the writer it opens there asks that member's own guard before each batch — so a member still carrying a
 * piece of a superseded run stops writing to the target instead of writing into it beside the current run.
 *
 * <p>Per batch, not per record: the guard is answered locally from a reading the member refreshes on a
 * bounded schedule. Scoped PDK writers check the same proof at the connector call.
 *
 * <p>Refusing throws rather than dropping the batch silently. The records are not lost — nothing has
 * acknowledged them, and the run that is current re-reads them from the durable position — and a sink that
 * quietly wrote nothing would leave a pipeline reporting healthy over a target that had stopped moving.
 *
 * <p>Where the writers write tables prepared before they open, preparing them is held to the run too: a clear
 * is a write, and a superseded run clearing a table after the current run has started writing into it would
 * take the current run's rows with it.
 */
final class FencedSinkWriterFactory implements io.tapstate.runtime.engine.IndexedSinkWriterFactory {

    private static final long serialVersionUID = 1L;

    private final SupplierEx<? extends SinkWriter> delegate;
    private final ExecutionFence fence;

    private FencedSinkWriterFactory(SupplierEx<? extends SinkWriter> delegate, ExecutionFence fence) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.fence = Objects.requireNonNull(fence, "fence");
    }

    /** {@code factory} held to {@code fence}'s run, or {@code factory} itself where there is none. */
    static SupplierEx<? extends SinkWriter> heldTo(
            SupplierEx<? extends SinkWriter> factory, ExecutionFence fence) {
        if (fence == null) {
            return factory;
        }
        FencedSinkWriterFactory writers = new FencedSinkWriterFactory(factory, fence);
        return factory instanceof PreparesTargets targets ? new Preparing(writers, targets, fence) : writers;
    }

    @Override
    public SinkWriter getEx() throws Exception {return forWriter(0,0);}
    @Override public SinkWriter forWriter(int memberIndex,int localIndex)throws Exception {
        return open((member, local) -> delegate instanceof io.tapstate.runtime.engine.IndexedSinkWriterFactory indexed
                ? indexed.forWriter(member, local) : delegate.get(), memberIndex, localIndex);
    }

    @Override public Opening opening() {
        Opening batch = delegate instanceof io.tapstate.runtime.engine.IndexedSinkWriterFactory indexed
                ? indexed.opening() : (member, local) -> delegate.get();
        return (member, local) -> open(batch, member, local);
    }

    private SinkWriter open(Opening opening, int memberIndex, int localIndex) throws Exception {
        ExecutionAuthorization authorization = ExecutionAuthorization.local();
        authorization.require(fence);
        SinkWriter opened = opening.open(memberIndex, localIndex);
        try {
            authorization.require(fence);
            boolean checkBatch = !(delegate instanceof PdkSinkWriterFactory pdk && pdk.guardsWritesFor(fence));
            return new FencedSinkWriter(opened, fence, authorization, checkBatch);
        } catch (RuntimeException | Error failure) {
            try {
                opened.close();
            } catch (RuntimeException | Error cleanup) {
                if (cleanup != failure) failure.addSuppressed(cleanup);
            }
            throw failure;
        }
    }

    /** {@code writer}, asking {@code authorization} for {@code fence}'s run before each batch. */
    static SinkWriter guarded(
            SinkWriter writer, ExecutionFence fence, ExecutionAuthorization authorization) {
        return new FencedSinkWriter(writer, fence, authorization, true);
    }

    /** Writers held to the run, over tables whose preparing is held to it too. */
    private record Preparing(FencedSinkWriterFactory writers, PreparesTargets targets, ExecutionFence fence)
            implements io.tapstate.runtime.engine.IndexedSinkWriterFactory, PreparesTargets {

        @Override
        public SinkWriter getEx() throws Exception {
            return writers.getEx();
        }
        @Override public SinkWriter forWriter(int memberIndex,int localIndex)throws Exception{return writers.forWriter(memberIndex,localIndex);}
        @Override public Opening opening() { return writers.opening(); }

        @Override
        public void prepareTargets(HazelcastInstance coordinator) {
            ExecutionAuthorization.of(coordinator).require(fence);
            targets.prepareTargets(coordinator);
        }
    }

    private static final class FencedSinkWriter implements SinkWriter {

        private final SinkWriter delegate;
        private final ExecutionFence fence;
        private final ExecutionAuthorization authorization;
        private final boolean checkBatch;
        /**
         * Set as closing starts, before the delegate is let go. A write still under way then is ended by the
         * closing - a connector let go under a write fails it - and the run is closing because something
         * else ended it, which is for whoever drives the run to judge. Filed as the sink's own failure, that
         * write would outrank the reason the run actually ended: a member leaving would read as the
         * pipeline's own death, and the pipeline would stay failed instead of being rebuilt.
         */
        private volatile boolean closing;

        private FencedSinkWriter(SinkWriter delegate, ExecutionFence fence, ExecutionAuthorization authorization,
                boolean checkBatch) {
            this.delegate = delegate;
            this.fence = fence;
            this.authorization = authorization;
            this.checkBatch = checkBatch;
        }

        @Override
        public CompletionStage<WriteResult> write(List<Envelope> records) {
            if (checkBatch) authorization.require(fence);
            try {
                return delegate.write(records).whenComplete((ignored, failure) -> fileIfTheSinks(failure));
            } catch (RuntimeException failure) {
                fileIfTheSinks(failure);
                throw failure;
            }
        }

        private void fileIfTheSinks(Throwable failure) {
            if (!closing && PipelineFailures.isSinkWriteFailure(fence.pipelineId(), failure)) {
                authorization.recordSinkWriteFailure(fence);
            }
        }

        @Override
        public void close() {
            closing = true;
            delegate.close();
        }
    }
}
