package io.tapstate.app;

import com.hazelcast.function.SupplierEx;
import io.tapstate.core.event.Envelope;
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
        return fence == null ? factory : new FencedSinkWriterFactory(factory, fence);
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

    private record FencedSinkWriter(
            SinkWriter delegate, ExecutionFence fence, ExecutionAuthorization authorization)
            implements SinkWriter {

        @Override
        public CompletionStage<WriteResult> write(List<Envelope> records) {
            authorization.require(fence);
            try {
                return delegate.write(records).whenComplete((ignored, failure) -> {
                    if (PipelineFailures.isSinkWriteFailure(fence.pipelineId(), failure)) {
                        authorization.recordSinkWriteFailure(fence);
                    }
                });
            } catch (RuntimeException failure) {
                if (PipelineFailures.isSinkWriteFailure(fence.pipelineId(), failure)) {
                    authorization.recordSinkWriteFailure(fence);
                }
                throw failure;
            }
        }

        @Override
        public void close() {
            delegate.close();
        }
    }
}
