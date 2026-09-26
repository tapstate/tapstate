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
 * bounded schedule, and asking it is a comparison of two numbers.
 *
 * <p>Refusing throws rather than dropping the batch silently. The records are not lost — nothing has
 * acknowledged them, and the run that is current re-reads them from the durable position — and a sink that
 * quietly wrote nothing would leave a pipeline reporting healthy over a target that had stopped moving.
 *
 * <p>Where the writers write tables prepared before they open, preparing them is held to the run too: a clear
 * is a write, and a superseded run clearing a table after the current run has started writing into it would
 * take the current run's rows with it.
 */
final class FencedSinkWriterFactory implements SupplierEx<SinkWriter> {

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
    public SinkWriter getEx() throws Exception {
        return guarded(delegate.get(), fence, ExecutionAuthorization.local());
    }

    /** {@code writer}, asking {@code authorization} for {@code fence}'s run before each batch. */
    static SinkWriter guarded(
            SinkWriter writer, ExecutionFence fence, ExecutionAuthorization authorization) {
        return new FencedSinkWriter(writer, fence, authorization);
    }

    /** Writers held to the run, over tables whose preparing is held to it too. */
    private record Preparing(FencedSinkWriterFactory writers, PreparesTargets targets, ExecutionFence fence)
            implements SupplierEx<SinkWriter>, PreparesTargets {

        @Override
        public SinkWriter getEx() throws Exception {
            return writers.getEx();
        }

        @Override
        public void prepareTargets(HazelcastInstance coordinator) {
            ExecutionAuthorization.of(coordinator).require(fence);
            targets.prepareTargets(coordinator);
        }
    }

    private record FencedSinkWriter(
            SinkWriter delegate, ExecutionFence fence, ExecutionAuthorization authorization)
            implements SinkWriter {

        @Override
        public CompletionStage<WriteResult> write(List<Envelope> records) {
            authorization.require(fence);
            return delegate.write(records);
        }

        @Override
        public void close() {
            delegate.close();
        }
    }
}
