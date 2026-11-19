package io.tapstate.spi.sink;

import io.tapstate.core.event.Envelope;
import java.util.List;
import java.util.concurrent.CompletionStage;

/**
 * A write session bound to one sink target. Accepts batches of events and holds the target resource
 * until closed.
 *
 * <p>{@link #write} is asynchronous: it does not block the caller, and the returned stage completes
 * once the target has accepted the batch (or completes exceptionally if the write fails). The
 * writer spans many batches; the order in which in-flight writes complete, the backpressure that
 * bounds how many are outstanding, and the delivery guarantee the target makes belong to the
 * runtime, not to this contract.
 */
public interface SinkWriter extends AutoCloseable {

    /**
     * Writes a batch of events asynchronously. The returned stage completes with the number of
     * events the target accepted once it has taken the batch. Ownership of {@code records} passes to
     * the writer for the duration of the write; the caller must not mutate the list until the
     * returned stage completes.
     */
    CompletionStage<WriteResult> write(List<Envelope> records);

    /** The runtime has delivered every snapshot row on this input, including an empty snapshot. */
    default void snapshotDelivered(String stream) { }

    /** Closes the writer, releasing the target resource. Idempotent. */
    @Override
    void close();
}
